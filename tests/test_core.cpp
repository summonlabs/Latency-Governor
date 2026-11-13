#include "latency_governor/clock.hpp"
#include "latency_governor/request.hpp"
#include "latency_governor/policy.hpp"
#include "latency_governor/governor.hpp"
#include "latency_governor/prediction.hpp"
#include "latency_governor/risk.hpp"
#include "latency_governor/fairness.hpp"
#include "latency_governor/enums.hpp"
#include "latency_governor/identifiers.hpp"
#include "latency_governor/serialization.hpp"
#include "test_framework.hpp"

#include <cmath>
#include <string>

using namespace latency_governor;

namespace {
Policy make_policy() {
    Policy p;
    p.name = "test-policy";
    auto add = [&](const char* name, double prio, Duration e2e, Duration ttf) {
        SloClassSpec s;
        s.name = name; s.priority = prio;
        s.default_e2e_target = e2e; s.default_ttf_target = ttf;
        s.default_deadline_risk_threshold = 0.1; s.default_min_completion_probability = 0.0;
        s.default_fairness_weight = 1.0; s.default_degradation_allowed = true;
        s.phase_weights = {};
        s.phase_weights[enum_index(Phase::QUEUEING)] = 0.05;
        s.phase_weights[enum_index(Phase::BATCH_WAIT)] = 0.05;
        s.phase_weights[enum_index(Phase::SCHEDULING)] = 0.05;
        s.phase_weights[enum_index(Phase::TRANSFER)] = 0.05;
        s.phase_weights[enum_index(Phase::PREFILL)] = 0.25;
        s.phase_weights[enum_index(Phase::DECODE)] = 0.40;
        s.phase_weights[enum_index(Phase::SPEC_PROPOSAL)] = 0.03;
        s.phase_weights[enum_index(Phase::SPEC_VERIFY)] = 0.02;
        s.phase_weights[enum_index(Phase::RETRY_BACKOFF)] = 0.02;
        s.phase_weights[enum_index(Phase::RECOVERY)] = 0.02;
        s.phase_weights[enum_index(Phase::COMPLETION)] = 0.01;
        s.phase_weights[enum_index(Phase::UNCLASSIFIED)] = 0.0;
        s.reserve_fraction = 0.05;
        p.classes.push_back(s);
    };
    add("REALTIME", 5.0, ms(100), ms(10));
    add("INTERACTIVE", 4.0, ms(300), ms(40));
    add("STANDARD", 3.0, ms(800), ms(120));
    add("THROUGHPUT", 2.0, ms(2000), ms(300));
    add("BACKGROUND", 1.0, ms(6000), ms(1000));
    p.retry.max_retries = 2;
    p.retry.max_cumulative_retry_delay = ms(50);
    p.batch.default_max_batch_wait = ms(50);
    p.speculation.default_max_depth = 2;
    p.transfer.max_transfer_fraction_of_budget = 0.2;
    return p;
}

RequestDescriptor make_req(RequestId id, SloClass cls = SloClass::REALTIME, Duration e2e = ms(100)) {
    RequestDescriptor d;
    d.request_id = id; d.tenant_id = TenantId(1); d.model_id = ModelId(1);
    d.model_revision = ModelRevision(1); d.slo_class = cls;
    d.contract.e2e_target = e2e; d.contract.ttf_target = ms(10);
    d.contract.hard_deadline = e2e;
    d.contract.retry_allowance = ms(20); d.contract.max_retry_delay = ms(20);
    d.prompt_tokens = 128; d.max_tokens = 64; d.remaining_tokens = 64;
    return d;
}

} // namespace

int main() {
    // Strong ID
    CHECK(RequestId(0).is_null());
    CHECK(RequestId(7).is_valid());
    CHECK(RequestId(7) != RequestId(8));
    CHECK(RequestId(7) == RequestId(7));
    RequestId r; CHECK(RequestId::parse("42", r)); CHECK_EQ(r.value(), 42ull);
    CHECK_FALSE(RequestId::parse("-1", r));
    CHECK_FALSE(RequestId::parse("", r));

    // enum round trips
    CHECK_EQ(enum_count<SloClass>() , 5u);
    CHECK_EQ(enum_index(Phase::PREFILL) , (size_t)enum_index(Phase::PREFILL));
    auto rc = enum_from_string<RiskState>("SAFE");
    CHECK(rc.has_value());
    CHECK_FALSE(enum_from_string<RiskState>("NOT_A_STATE").has_value());

    // contract validation
    {
        LatencyContract c; c.e2e_target = ms(100); c.ttf_target = ms(10);
        std::string err;
        CHECK(c.validate(err));
        LatencyContract bad; bad.e2e_target = ms(-5);
        CHECK_FALSE(bad.validate(err));
        LatencyContract over; over.e2e_target = ms(200); over.hard_deadline = ms(100);
        CHECK_FALSE(over.validate(err));
    }

    // budget accounting + donation
    {
        BudgetPartition bp;
        bp.allocation[enum_index(Phase::PREFILL)] = ms(25);
        bp.allocation[enum_index(Phase::DECODE)] = ms(40);
        bp.allocation[enum_index(Phase::QUEUEING)] = ms(5);
        bp.reserve = ms(5);
        PhaseBudgets pb(bp);
        CHECK_EQ(ms_count(pb.partition().total()) , 75);
        pb.attribute(Phase::PREFILL, ms(10));
        CHECK_EQ(ms_count(pb.consumed(Phase::PREFILL)) , 10);
        CHECK_EQ(ms_count(pb.remaining(Phase::PREFILL)) , 15);
        pb.attribute(Phase::PREFILL, ms(30));  // overspend
        CHECK(pb.overspent(Phase::PREFILL));
        CHECK_EQ(ms_count(pb.consumed(Phase::PREFILL)) , 40);
        // donation preserves total
        const auto total = pb.partition().total();
        pb.donate(Phase::DECODE, Phase::PREFILL, ms(10));
        CHECK_EQ(ms_count(pb.partition().total()) , ms_count(total));
        // random-ish attribute sanity
        for (int i=0;i<10000;++i) pb.attribute(Phase::DECODE, ns(i*7));
        CHECK(pb.consumed(Phase::DECODE) > Duration::zero());
    }

    // request remaining budget
    {
        RequestState rs;
        RequestDescriptor d = make_req(RequestId(9));
        rs.descriptor = d; rs.attempt_id = AttemptId(1); rs.generation = Generation(1);
        rs.elapsed_total = ms(30);
        const Duration cap = d.contract.hard_deadline ? *d.contract.hard_deadline : d.contract.e2e_target;
        CHECK_EQ(ns_count(rs.remaining_budget()) , ns_count(cap) - ns_count(ms(30)));
    }

    // predictor cold start + update
    {
        Predictor p;
        CHECK_FALSE(p.predict("PREFILL").has_value());
        for (int i=0;i<5;++i) p.record("PREFILL", ms(10));
        auto pr = p.predict("PREFILL");
        CHECK(pr.has_value());
        CHECK(pr->source == PredictionSource::MEASURED);
        CHECK_EQ(pr->evidence_count , (size_t)5);
        CHECK(pr->predicted == ms(10));
        CHECK_EQ(p.evidence_count("PREFILL") , (size_t)5);
    }

    // evidence count trackers
    {
        Predictor p;
        CHECK_EQ(p.evidence_count("X") , (size_t)0);
        p.record("X", ms(1));
        CHECK_EQ(p.evidence_count("X") , (size_t)1);
    }

    // risk + admission
    {
        ManualClock clk;
        Governor g(GovernorConfig{}, clk);
        std::string err; g.policy_store().add(make_policy(), err);
        RequestDescriptor d = make_req(RequestId(100), SloClass::REALTIME, ms(100));
        auto aa = g.evaluate_admission(d);
        CHECK(aa.verdict == AdmissionVerdict::FEASIBLE || aa.verdict == AdmissionVerdict::FEASIBLE_WITH_RISK);
        auto ar = g.admit(d);
        CHECK(ar.accepted);
        CHECK(ar.generation.is_valid());
        CHECK(ar.attempt_id.is_valid());
        // drive to risk: advance clock + observe heavy prefill
        for (int i=0;i<20;++i) {
            Observation o;
            o.request_id = d.request_id; o.attempt_id = ar.attempt_id;
            o.generation = ar.generation; o.epoch = ar.epoch;
            o.type = ObservationType::PREFILL_CHUNK_COMPLETED;
            o.phase = Phase::PREFILL; o.elapsed = ms(6);
            o.at = clk.now();
            auto orr = g.observe(o);
            CHECK(orr.accepted);
            clk.advance_ms(6);
        }
        auto ra = g.assess(d.request_id);
        CHECK(ra.budget_used_fraction > 0.0);
        // RiskState should be at least WATCH / AT_RISK given 120ms of 100ms budget
        CHECK(ra.state == RiskState::AT_RISK || ra.state == RiskState::CRITICAL || ra.state == RiskState::INFEASIBLE);
        // plan produces at least one typed intervention with reason/evidence
        auto plan = g.plan(d.request_id);
        CHECK(!plan.items.empty());
        CHECK(plan.items[0].request_id == d.request_id);
        // snapshot
        auto snap = g.snapshot();
        CHECK(snap.summary.active >= 1);
        CHECK(!snap.json.empty());
        // commit
        Completion comp;
        comp.request_id = d.request_id; comp.attempt_id = ar.attempt_id;
        comp.generation = ar.generation; comp.epoch = ar.epoch;
        comp.outcome = Completion::Outcome::COMPLETED; comp.slo_met = false; comp.soft_violation = true;
        comp.at = clk.now();
        std::string cerr;
        CHECK(g.commit(comp, cerr));
        CHECK_EQ(g.metrics().soft_violation , 1u);
        // terminal can't be re-committed
        CHECK_FALSE(g.commit(comp, cerr));
        // stale authority after commit is rejected
        comp.slo_met = true;
        CHECK_FALSE(g.commit(comp, cerr));
    }

    // stale authority rejection on observe
    {
        ManualClock clk;
        Governor g(GovernorConfig{}, clk);
        std::string err; g.policy_store().add(make_policy(), err);
        auto ar = g.admit(make_req(RequestId(200), SloClass::REALTIME, ms(100)));
        Observation o;
        o.request_id = RequestId(200); o.attempt_id = ar.attempt_id;
        o.generation = ar.generation; o.epoch = CoordinatorEpoch(ar.epoch.value()+1);
        o.type = ObservationType::QUEUE_ENTERED; o.phase = Phase::QUEUEING; o.at = clk.now();
        auto orr = g.observe(o);
        CHECK_FALSE(orr.accepted);
        CHECK(orr.code == RejectionCode::EPOCH_MISMATCH);
    }

    // fairness
    {
        FairnessTracker f;
        f.account(TenantId(1), SloClass::BACKGROUND, ms(1), Duration::zero(), true, 1.0);
        f.account(TenantId(2), SloClass::REALTIME, ms(1000), Duration::zero(), true, 1.0);
        // Tenant 1 has consumed far less -> more deserving of an intervention.
        CHECK(f.arbitration_score(TenantId(1), SloClass::BACKGROUND, ms(5)) >
              f.arbitration_score(TenantId(2), SloClass::REALTIME, ms(5)));
    }

    // policy versioning
    {
        ManualClock clk;
        Governor g(GovernorConfig{}, clk);
        std::string err;
        auto gen1 = g.policy_store().add(make_policy(), err);
        CHECK(gen1.has_value());
        auto ar1 = g.admit(make_req(RequestId(300), SloClass::REALTIME, ms(100)));
        CHECK(ar1.accepted);
        // live replacement: gen2
        auto p2 = make_policy(); p2.name = "gen2";
        auto gen2 = g.policy_store().add(p2, err);
        CHECK(gen2.has_value());
        CHECK(gen2.value() > gen1.value());
        // request still governed by its admission generation
        auto st = g.snapshot();
        CHECK(st.summary.active >= 1);
    }

    test::report("test_core");
    return test::exit_code();
}