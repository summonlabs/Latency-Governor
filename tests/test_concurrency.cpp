#include "latency_governor/clock.hpp"
#include "latency_governor/request.hpp"
#include "latency_governor/policy.hpp"
#include "latency_governor/governor.hpp"
#include "latency_governor/enums.hpp"
#include "latency_governor/identifiers.hpp"
#include "test_framework.hpp"

#include <atomic>
#include <thread>
#include <string>
#include <vector>

using namespace latency_governor;

namespace {
Policy make_policy() {
    Policy p; p.name = "conc";
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
RequestDescriptor mk(RequestId id) {
    RequestDescriptor d; d.request_id = id; d.tenant_id = TenantId(1); d.model_id = ModelId(1);
    d.model_revision = ModelRevision(1); d.slo_class = SloClass::REALTIME;
    d.contract.e2e_target = ms(200); d.contract.ttf_target = ms(20); d.contract.hard_deadline = ms(200);
    d.prompt_tokens = 128; d.max_tokens = 64; d.remaining_tokens = 64;
    return d;
}
} // namespace

int main() {
    SystemClock clk;
    Governor g(GovernorConfig{}, clk);
    std::string err; g.policy_store().add(make_policy(), err);

    const int kThreads = 8;
    const int kPerThread = 1500;
    std::atomic<std::size_t> accepted{0};
    std::atomic<std::size_t> observed_ok{0};
    std::atomic<std::size_t> plan_ok{0};
    std::atomic<bool> failed{false};

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&](int tid) {
            for (int i = 0; i < kPerThread; ++i) {
                const RequestId id(static_cast<std::uint64_t>(tid) * 100000ull + static_cast<std::uint64_t>(i) + 1);
                auto ar = g.admit(mk(id));
                if (!ar.accepted) { failed = true; continue; }
                ++accepted;
                for (int k = 0; k < 3; ++k) {
                    Observation o; o.request_id = id; o.attempt_id = ar.attempt_id;
                    o.generation = ar.generation; o.epoch = ar.epoch;
                    o.type = ObservationType::PREFILL_CHUNK_COMPLETED; o.phase = Phase::PREFILL;
                    o.elapsed = ms(1); o.at = clk.now();
                    if (g.observe(o).accepted) ++observed_ok; else failed = true;
                }
                if (g.plan(id).items.size() > 1) { /* just exercise */ }
                ++plan_ok;
                // snapshot occasionally
                if ((i & 127) == 0) { auto s = g.snapshot(); if (s.json.empty()) failed = true; }
            }
        }, t);
    }
    // Reader threads doing snapshot/explain/metrics concurrently.
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < 2000; ++i) {
                auto s = g.snapshot();
                auto m = g.metrics();
                (void)m;
                if (s.summary.active > kThreads * kPerThread) failed = true;
            }
        });
    }
    for (auto& th : threads) th.join();
    CHECK_FALSE(failed.load());
    CHECK(accepted.load() > 0);
    CHECK(observed_ok.load() > 0);
    // All active requests must eventually be bounded; no overflow of active count.
    CHECK(g.metrics().active <= kThreads * kPerThread);

    test::report("test_concurrency");
    return test::exit_code();
}
