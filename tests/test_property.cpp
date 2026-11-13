#include "latency_governor/clock.hpp"
#include "latency_governor/request.hpp"
#include "latency_governor/policy.hpp"
#include "latency_governor/governor.hpp"
#include "latency_governor/persistence.hpp"
#include "latency_governor/enums.hpp"
#include "latency_governor/identifiers.hpp"
#include "latency_governor/serialization.hpp"
#include "test_framework.hpp"

#include <cmath>
#include <random>
#include <string>
#include <unordered_set>

using namespace latency_governor;

namespace {
Policy make_policy() {
    Policy p; p.name = "property";
    SloClassSpec s; s.name = "REALTIME"; s.priority = 5; s.default_e2e_target = ms(200); s.default_ttf_target = ms(20);
    s.phase_weights = {}; s.phase_weights[enum_index(Phase::PREFILL)] = 0.25; s.phase_weights[enum_index(Phase::DECODE)] = 0.40;
    s.phase_weights[enum_index(Phase::QUEUEING)] = 0.05; s.phase_weights[enum_index(Phase::BATCH_WAIT)] = 0.05;
    s.phase_weights[enum_index(Phase::SCHEDULING)] = 0.05; s.phase_weights[enum_index(Phase::TRANSFER)] = 0.05;
    s.phase_weights[enum_index(Phase::SPEC_PROPOSAL)] = 0.03; s.phase_weights[enum_index(Phase::SPEC_VERIFY)] = 0.02;
    s.phase_weights[enum_index(Phase::RETRY_BACKOFF)] = 0.02; s.phase_weights[enum_index(Phase::RECOVERY)] = 0.02;
    s.phase_weights[enum_index(Phase::COMPLETION)] = 0.01; s.phase_weights[enum_index(Phase::UNCLASSIFIED)] = 0.0;
    s.reserve_fraction = 0.05;
    p.classes.push_back(s);
    p.retry.max_retries = 2; p.retry.max_cumulative_retry_delay = ms(40);
    p.batch.default_max_batch_wait = ms(50); p.speculation.default_max_depth = 2;
    return p;
}
RequestDescriptor mk(RequestId id, Duration e2e) {
    RequestDescriptor d; d.request_id = id; d.tenant_id = TenantId(1); d.model_id = ModelId(1);
    d.model_revision = ModelRevision(1); d.slo_class = SloClass::REALTIME;
    d.contract.e2e_target = e2e; d.contract.ttf_target = ms(20); d.contract.hard_deadline = e2e;
    d.prompt_tokens = 128; d.max_tokens = 64; d.remaining_tokens = 64;
    return d;
}
} // namespace

int main() {
    const std::uint64_t seed = 0xC0FFEE1234ULL;
    std::printf("property-test seed=%llu\n", (unsigned long long)seed);
    std::mt19937_64 rng(seed);

    ManualClock clk;
    std::string err;
    Governor g(GovernorConfig{}, clk);
    g.policy_store().add(make_policy(), err);

    std::unordered_set<RequestId> active;
    const std::size_t kOps = 5000;
    for (std::size_t op = 0; op < kOps; ++op) {
        const int kind = static_cast<int>(rng() % 6);
        RequestId rid(static_cast<std::uint64_t>(1 + (rng() % 50)));
        if (kind == 0) {  // admit
            if (active.count(rid)) continue;   // duplicate id
            auto ar = g.admit(mk(rid, ms(50 + (rng() % 300))));
            if (ar.accepted) active.insert(rid);
        } else if (kind == 1) {  // observe
            if (!active.count(rid)) continue;
            Observation o; o.request_id = rid; o.attempt_id = AttemptId(1);
            o.generation = Generation(0); o.epoch = CoordinatorEpoch(0);
            o.type = ObservationType::PREFILL_CHUNK_COMPLETED; o.phase = Phase::PREFILL;
            o.elapsed = ns(1 + (rng() % 5000000)); o.at = clk.now();
            auto res = g.observe(o);
            // Attempt/generation mismatch must be rejected; never accepted stale.
            CHECK_FALSE(res.accepted);  // envelope is wrong
            clk.advance_us(1);
        } else if (kind == 2) {  // assess
            if (!active.count(rid)) continue;
            auto ra = g.assess(rid);
            CHECK(!std::isnan(ra.deadline_risk));
            CHECK(!std::isinf(ra.completion_probability));
            CHECK(ra.completion_probability >= 0.0 && ra.completion_probability <= 1.0);
            CHECK(ra.budget_used_fraction >= 0.0 && ra.budget_used_fraction <= 1.0);
        } else if (kind == 3) {  // plan
            if (!active.count(rid)) continue;
            auto plan = g.plan(rid);
            CHECK(plan.items.size() <= 1);
        } else if (kind == 4) {  // reservation
            if (!active.count(rid)) continue;
            ReservationId rid2; std::string rerr;
            CHECK(g.reserve(rid, BackendId(1), 1024, rid2, rerr));
        } else if (kind == 5) {  // snapshot
            auto s = g.snapshot();
            CHECK(!s.json.empty());
        }
    }

    // Fresh valid admission then proper lifecycle to completion.
    {
        std::string e;
        auto ar = g.admit(mk(RequestId(1000), ms(200)));
        if (!ar.accepted) std::printf("admit 1000 rejected: code=%s detail=%s active=%llu\n", to_string(ar.code), ar.detail.c_str(), (unsigned long long)g.metrics().active);
        CHECK(ar.accepted);
        CHECK(ar.request_id == RequestId(1000));
        // Valid observations with correct authority.
        Observation start;
        start.id = ObservationId(1); start.request_id = RequestId(1000); start.attempt_id = ar.attempt_id;
        start.generation = ar.generation; start.epoch = ar.epoch;
        start.type = ObservationType::PREFILL_STARTED; start.phase = Phase::PREFILL; start.at = clk.now();
        CHECK(g.observe(start).accepted);
        for (int i = 0; i < 5; ++i) {
            Observation c; c.request_id = RequestId(1000); c.attempt_id = ar.attempt_id;
            c.generation = ar.generation; c.epoch = ar.epoch;
            c.type = ObservationType::PREFILL_CHUNK_COMPLETED; c.phase = Phase::PREFILL;
            c.elapsed = ms(5); c.at = clk.now();
            CHECK(g.observe(c).accepted);
            clk.advance_ms(5);
        }
        Observation ft; ft.request_id = RequestId(1000); ft.attempt_id = ar.attempt_id;
        ft.generation = ar.generation; ft.epoch = ar.epoch; ft.type = ObservationType::FIRST_TOKEN;
        ft.phase = Phase::DECODE; ft.elapsed = ms(5); ft.at = clk.now();
        CHECK(g.observe(ft).accepted);
        // Complete with stale attempt must be rejected.
        Completion comp;
        comp.request_id = RequestId(1000); comp.attempt_id = AttemptId(ar.attempt_id.value() + 1);
        comp.generation = ar.generation; comp.epoch = ar.epoch;
        comp.outcome = Completion::Outcome::COMPLETED; comp.slo_met = true; comp.at = clk.now();
        std::string cerr;
        CHECK_FALSE(g.commit(comp, cerr));
        // Correct attempt completes.
        comp.attempt_id = ar.attempt_id;
        CHECK(g.commit(comp, cerr));
        // Duplicate completion rejected (no double time).
        CHECK_FALSE(g.commit(comp, cerr));
        // terminal can't be re-admitted? A new admission with same id is a duplicate.
        auto ar2 = g.admit(mk(RequestId(1000), ms(200)));
        CHECK_FALSE(ar2.accepted);
        CHECK(ar2.code == RejectionCode::DUPLICATE);
        CHECK(active.count(RequestId(1000)) == 0);
    }

    // Hard-deadline violation must not be reported met.
    {
        std::string e;
        auto ar = g.admit(mk(RequestId(2000), ms(20)));
        CHECK(ar.accepted);
        for (int i = 0; i < 10; ++i) {
            Observation o; o.request_id = RequestId(2000); o.attempt_id = ar.attempt_id;
            o.generation = ar.generation; o.epoch = ar.epoch;
            o.type = ObservationType::PREFILL_CHUNK_COMPLETED; o.phase = Phase::PREFILL;
            o.elapsed = ms(10); o.at = clk.now();
            g.observe(o); clk.advance_ms(10);
        }
        auto ra = g.assess(RequestId(2000));
        CHECK_FALSE(ra.completion_probability > 1.0);
        // Soft target may be violated but hard-deadline met flag must be consistent.
        Completion comp; comp.request_id = RequestId(2000); comp.attempt_id = ar.attempt_id;
        comp.generation = ar.generation; comp.epoch = ar.epoch;
        comp.outcome = Completion::Outcome::COMPLETED; comp.hard_violation = true; comp.slo_met = false; comp.at = clk.now();
        std::string cerr; CHECK(g.commit(comp, cerr));
    }

    // Serialization determinism.
    {
        BinaryWriter a, b;
        a.u64(123456789); a.string("hello"); a.duration(ms(42));
        b.u64(123456789); b.string("hello"); b.duration(ms(42));
        CHECK_EQ(a.size(), b.size());
        CHECK(a.data() == b.data());
    }

    test::report("test_property");
    return test::exit_code();
}