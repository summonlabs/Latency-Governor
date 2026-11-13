// Regression checks: terminal requests are never mutated or revived by late
// observations, policy history keeps each generation's policy, recovery keeps
// policy generations, and ADMIT carries the request's latency contract.
#include "latency_governor/clock.hpp"
#include "latency_governor/governor.hpp"
#include "latency_governor/policy.hpp"
#include "latency_governor/protocol.hpp"
#include "latency_governor/serialization.hpp"
#include "test_framework.hpp"

#include <string>

using namespace latency_governor;

namespace {
Policy make_policy(const std::string& name) {
    Policy p; p.name = name;
    SloClassSpec s; s.name = "REALTIME"; s.priority = 5; s.default_e2e_target = ms(200); s.default_ttf_target = ms(20);
    s.phase_weights = {}; s.phase_weights[enum_index(Phase::PREFILL)] = 0.25; s.phase_weights[enum_index(Phase::DECODE)] = 0.40;
    s.reserve_fraction = 0.05; p.classes.push_back(s);
    return p;
}
RequestDescriptor mk(RequestId id) {
    RequestDescriptor d; d.request_id = id; d.tenant_id = TenantId(7); d.model_id = ModelId(3);
    d.model_revision = ModelRevision(2); d.slo_class = SloClass::REALTIME;
    d.contract.e2e_target = ms(200); d.contract.ttf_target = ms(20); d.contract.hard_deadline = ms(200);
    d.prompt_tokens = 128; d.max_tokens = 64; d.remaining_tokens = 64;
    return d;
}
} // namespace

// A late or replayed observation with a valid envelope never mutates or
// revives a terminal request, so its completion is never counted twice.
void terminal_request_rejects_observation() {
    ManualClock clk; Governor g(GovernorConfig{}, clk);
    std::string err; CHECK(g.policy_store().add(make_policy("p"), err).has_value());
    const auto ar = g.admit(mk(RequestId(1)));
    CHECK(ar.accepted);
    Completion c; c.request_id = RequestId(1); c.attempt_id = ar.attempt_id; c.generation = ar.generation; c.epoch = ar.epoch;
    c.outcome = Completion::Outcome::COMPLETED; c.slo_met = true; c.at = clk.now();
    CHECK(g.commit(c, err));
    for (ObservationType t : {ObservationType::QUEUE_ENTERED, ObservationType::FIRST_TOKEN}) {
        Observation o; o.request_id = RequestId(1); o.attempt_id = ar.attempt_id; o.generation = ar.generation; o.epoch = ar.epoch;
        o.type = t; o.phase = Phase::DECODE; o.elapsed = ms(1); o.at = clk.now();
        const auto r = g.observe(o);
        CHECK_FALSE(r.accepted);
        CHECK(r.code == RejectionCode::STATE_INELIGIBLE);
    }
    CHECK(g.snapshot().requests.empty());
    CHECK_FALSE(g.commit(c, err));
    CHECK_EQ(g.metrics().completed, 1ull);
    CHECK_EQ(g.metrics().active, 0ull);
}

// Bounded policy history: every retained generation still resolves to its own policy.
void policy_eviction_keeps_generations() {
    PolicyStore ps; ps.set_max_retained(3);
    std::string err;
    for (int i = 1; i <= 6; ++i) CHECK(ps.add(make_policy("p" + std::to_string(i)), err).has_value());
    CHECK_EQ(ps.count(), 3u);
    for (std::uint64_t gen = 4; gen <= 6; ++gen) {
        const Policy* p = ps.get(gen);
        CHECK(p != nullptr);
        if (p != nullptr) { CHECK_EQ(p->generation, gen); CHECK_EQ(p->name, "p" + std::to_string(gen)); }
    }
    const auto all = ps.all();
    CHECK_EQ(all.size(), 3u);
    for (std::size_t i = 0; i < all.size(); ++i) CHECK_EQ(all[i]->generation, 4u + i);
}

// Recovery keeps policy generations, and later policies continue after them.
void recovery_keeps_policy_generations() {
    ManualClock clk;
    std::string blob, err;
    {
        Governor g(GovernorConfig{}, clk);
        CHECK(g.policy_store().add(make_policy("old"), err).has_value());
        CHECK(g.policy_store().add(make_policy("new"), err).has_value());
        g.policy_store().compact(err);   // retains generation 2 only
        blob = g.encode_state(err);
    }
    Governor r(GovernorConfig{}, clk);
    CHECK(r.decode_state(blob, err));
    const auto gen = r.policy_store().current_generation();
    CHECK(gen.has_value() && *gen == 2u);
    const Policy* cur = r.policy_store().current();
    CHECK(cur != nullptr && cur->name == "new");
    const auto next = r.policy_store().add(make_policy("after"), err);
    CHECK(next.has_value() && *next == 3u);
}

// ADMIT carries the request's latency contract to the coordinator.
void admit_codec_carries_contract() {
    RequestDescriptor d = mk(RequestId(5));
    d.contract.e2e_target = ms(1000); d.contract.hard_deadline = ms(1500); d.contract.ttf_target = ms(100);
    d.contract.prefill_target = ms(300); d.contract.decode_step_target = ms(7); d.contract.max_queue_residence = ms(50);
    d.contract.deadline_risk_threshold = 0.25; d.contract.fairness_weight = 2.0;
    d.contract.cancellation_policy = CancellationPolicy::ALLOW_COMPLETE; d.contract.degradation_allowed = false;
    BinaryWriter w; net::encode_admit(w, d);
    BinaryReader rd(w.data());
    RequestDescriptor out;
    CHECK(net::decode_admit(rd, out));
    CHECK(rd.at_end());
    CHECK(out.contract.e2e_target == ms(1000));
    CHECK(out.contract.hard_deadline.has_value() && *out.contract.hard_deadline == ms(1500));
    CHECK(out.contract.ttf_target == ms(100));
    CHECK(out.contract.prefill_target == ms(300));
    CHECK(out.contract.decode_step_target == ms(7));
    CHECK(out.contract.max_queue_residence == ms(50));
    CHECK(out.contract.deadline_risk_threshold == 0.25);
    CHECK(out.contract.fairness_weight == 2.0);
    CHECK(out.contract.cancellation_policy == CancellationPolicy::ALLOW_COMPLETE);
    CHECK_FALSE(out.contract.degradation_allowed);
}

int main() {
    terminal_request_rejects_observation();
    policy_eviction_keeps_generations();
    recovery_keeps_policy_generations();
    admit_codec_carries_contract();
    test::report("test_regression");
    return test::exit_code();
}
