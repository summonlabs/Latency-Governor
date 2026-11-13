#include "latency_governor/clock.hpp"
#include "latency_governor/request.hpp"
#include "latency_governor/policy.hpp"
#include "latency_governor/governor.hpp"
#include "latency_governor/persistence.hpp"
#include "latency_governor/serialization.hpp"
#include "latency_governor/enums.hpp"
#include "latency_governor/identifiers.hpp"
#include "test_framework.hpp"

#include <filesystem>
#include <fstream>
#include <string>

using namespace latency_governor;

namespace {
Policy make_policy() {
    Policy p; p.name = "persist";
    SloClassSpec s; s.name = "REALTIME"; s.priority = 5; s.default_e2e_target = ms(200); s.default_ttf_target = ms(20);
    s.phase_weights = {}; s.phase_weights[enum_index(Phase::PREFILL)] = 0.25; s.phase_weights[enum_index(Phase::DECODE)] = 0.40;
    s.phase_weights[enum_index(Phase::QUEUEING)] = 0.05; s.phase_weights[enum_index(Phase::BATCH_WAIT)] = 0.05;
    s.phase_weights[enum_index(Phase::SCHEDULING)] = 0.05; s.phase_weights[enum_index(Phase::TRANSFER)] = 0.05;
    s.phase_weights[enum_index(Phase::SPEC_PROPOSAL)] = 0.03; s.phase_weights[enum_index(Phase::SPEC_VERIFY)] = 0.02;
    s.phase_weights[enum_index(Phase::RETRY_BACKOFF)] = 0.02; s.phase_weights[enum_index(Phase::RECOVERY)] = 0.02;
    s.phase_weights[enum_index(Phase::COMPLETION)] = 0.01; s.phase_weights[enum_index(Phase::UNCLASSIFIED)] = 0.0;
    s.reserve_fraction = 0.05; p.classes.push_back(s);
    p.retry.max_retries = 2;
    return p;
}
RequestDescriptor mk(RequestId id, Duration e2e) {
    RequestDescriptor d; d.request_id = id; d.tenant_id = TenantId(7); d.model_id = ModelId(3);
    d.model_revision = ModelRevision(2); d.slo_class = SloClass::REALTIME;
    d.contract.e2e_target = e2e; d.contract.ttf_target = ms(20); d.contract.hard_deadline = e2e;
    d.prompt_tokens = 128; d.max_tokens = 64; d.remaining_tokens = 64;
    return d;
}
std::string read_all(const std::string& path) {
    std::ifstream is(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(is)), std::istreambuf_iterator<char>());
}
void write_all(const std::string& path, const std::string& data) {
    std::ofstream os(path, std::ios::binary | std::ios::trunc); os.write(data.data(), data.size());
}
} // namespace

int main() {
    const std::string path = "latency_governor_test_persist.bin";
    std::filesystem::remove(path);

    // Build state, persist.
    {
        ManualClock clk;
        Governor g(GovernorConfig{}, clk);
        std::string err; g.policy_store().add(make_policy(), err);
        auto ar = g.admit(mk(RequestId(42), ms(200)));
        CHECK(ar.accepted);
        CHECK_EQ(ar.request_id.value(), 42ull);
        // Some observations with correct authority.
        for (int i = 0; i < 3; ++i) {
            Observation o; o.request_id = RequestId(42); o.attempt_id = ar.attempt_id;
            o.generation = ar.generation; o.epoch = ar.epoch; o.phase = Phase::PREFILL;
            o.type = ObservationType::PREFILL_CHUNK_COMPLETED; o.elapsed = ms(10); o.at = clk.now();
            CHECK(g.observe(o).accepted); clk.advance_ms(10);
        }
        ReservationId rid; std::string rerr;
        CHECK(g.reserve(RequestId(42), BackendId(1), 2048, rid, rerr));
        CHECK(persist_to_file(g, path, err));
        CHECK(err.empty());
    }
    CHECK(std::filesystem::exists(path));
    // No leftover temp file.
    CHECK_FALSE(std::filesystem::exists(path + ".tmp"));

    // Load into a fresh governor.
    {
        ManualClock clk;
        Governor g(GovernorConfig{}, clk);
        std::string err;
        CHECK(load_from_file(g, path, err));
        CHECK(err.empty());
        // The request must be restored with its identity.
        auto ra = g.assess(RequestId(42));
        CHECK(ra.budget_used_fraction > 0.0);
        auto snap = g.snapshot();
        bool found = false;
        for (const auto& r : snap.requests) if (r.request_id == RequestId(42)) found = true;
        CHECK(found);
        // Policy restored.
        CHECK(g.policy_store().current() != nullptr);
        // The persisted generation/epoch are preserved (epoch >= 1).
        CHECK(g.coordinator_epoch().value() >= 1);
    }

    // Deterministic round-trip of encode_state.
    {
        ManualClock clk;
        Governor g(GovernorConfig{}, clk);
        std::string err; g.policy_store().add(make_policy(), err);
        g.admit(mk(RequestId(9), ms(100)));
        std::string a, b;
        a = g.encode_state(err);
        b = g.encode_state(err);
        CHECK_EQ(a, b);   // deterministic serialization
    }

    // Corruption: flip a payload byte.
    {
        auto blob = read_all(path);
        CHECK_FALSE(blob.empty());
        blob[blob.size() / 2] = static_cast<char>(blob[blob.size()/2] ^ 0x5A);
        const std::string bad = "latency_governor_test_bad.bin";
        write_all(bad, blob);
        ManualClock clk; Governor g(GovernorConfig{}, clk);
        std::string err;
        CHECK_FALSE(load_from_file(g, bad, err));
        CHECK(!err.empty());
        std::filesystem::remove(bad);
    }

    // Truncation.
    {
        auto blob = read_all(path);
        CHECK_FALSE(blob.empty());
        const std::string bad = "latency_governor_test_trunc.bin";
        write_all(bad, blob.substr(0, blob.size() / 3));
        ManualClock clk; Governor g(GovernorConfig{}, clk);
        std::string err;
        CHECK_FALSE(load_from_file(g, bad, err));
        std::filesystem::remove(bad);
    }

    // Bad magic.
    {
        auto blob = read_all(path);
        blob[0] = 'X';
        const std::string bad = "latency_governor_test_magic.bin";
        write_all(bad, blob);
        ManualClock clk; Governor g(GovernorConfig{}, clk);
        std::string err;
        CHECK_FALSE(load_from_file(g, bad, err));
        std::filesystem::remove(bad);
    }

    // Unsupported version.
    {
        auto blob = read_all(path);
        // version is bytes [8..11]; overwrite with a huge version.
        blob[8] = static_cast<char>(0xFF); blob[9] = static_cast<char>(0xFF); blob[10] = static_cast<char>(0xFF); blob[11] = static_cast<char>(0xFF);
        const std::string bad = "latency_governor_test_ver.bin";
        write_all(bad, blob);
        ManualClock clk; Governor g(GovernorConfig{}, clk);
        std::string err;
        CHECK_FALSE(load_from_file(g, bad, err));
        std::filesystem::remove(bad);
    }

    // Trailing data appended after checksum.
    {
        auto blob = read_all(path);
        blob.push_back('!');
        const std::string bad = "latency_governor_test_trail.bin";
        write_all(bad, blob);
        ManualClock clk; Governor g(GovernorConfig{}, clk);
        std::string err;
        CHECK_FALSE(load_from_file(g, bad, err));
        std::filesystem::remove(bad);
    }

    std::filesystem::remove(path);
    test::report("test_persistence");
    return test::exit_code();
}