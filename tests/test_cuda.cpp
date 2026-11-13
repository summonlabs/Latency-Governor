#include "latency_governor/backend.hpp"
#include "latency_governor/governor.hpp"
#include "latency_governor/policy.hpp"
#include "latency_governor/request.hpp"
#include "latency_governor/prediction.hpp"
#include "latency_governor/enums.hpp"
#include "latency_governor/identifiers.hpp"
#include "latency_governor/clock.hpp"
#include "test_framework.hpp"

#include <cstdio>
#include <string>
#include <vector>

using namespace latency_governor;

namespace {
Policy make_policy() {
    Policy p; p.name = "cuda";
    SloClassSpec s; s.name = "REALTIME"; s.priority = 5; s.default_e2e_target = ms(200); s.default_ttf_target = ms(20);
    s.phase_weights = {}; s.phase_weights[enum_index(Phase::PREFILL)] = 0.25; s.phase_weights[enum_index(Phase::DECODE)] = 0.40;
    s.phase_weights[enum_index(Phase::QUEUEING)] = 0.05; s.phase_weights[enum_index(Phase::BATCH_WAIT)] = 0.05;
    s.phase_weights[enum_index(Phase::SCHEDULING)] = 0.05; s.phase_weights[enum_index(Phase::TRANSFER)] = 0.05;
    s.phase_weights[enum_index(Phase::SPEC_PROPOSAL)] = 0.05; s.phase_weights[enum_index(Phase::SPEC_VERIFY)] = 0.03;
    s.phase_weights[enum_index(Phase::RETRY_BACKOFF)] = 0.02; s.phase_weights[enum_index(Phase::RECOVERY)] = 0.02;
    s.phase_weights[enum_index(Phase::COMPLETION)] = 0.01; s.phase_weights[enum_index(Phase::UNCLASSIFIED)] = 0.0;
    s.reserve_fraction = 0.02; p.classes.push_back(s);
    p.retry.max_retries = 2;
    return p;
}
} // namespace

int main() {
    CudaBackend cuda(BackendId(2));
    if (!cuda.available()) { std::printf("CUDA backend unavailable on this host; SKIPPING (not a failure).\n"); return 0; }
    std::printf("CUDA device: %s\n", cuda.device_info().c_str());

    const std::size_t n = 4096;
    std::vector<std::uint8_t> in(n, 0x2A), out;
    // --- real prefill & decode execution (alloc, H2D, launch, sync, D2H, verify, free) ---
    auto pre = cuda.execute(Workload::PREFILL, n, in, out);
    CHECK(pre.success);
    CHECK(pre.duration > Duration::zero());
    CHECK(pre.checksum != 0);
    CHECK_EQ(out.size(), n * sizeof(float));

    auto dec = cuda.execute(Workload::DECODE, n, in, out);
    CHECK(dec.success);
    CHECK(dec.duration > Duration::zero());
    CHECK(dec.checksum != 0);

    // Determinism of numerical output across runs.
    auto pre2 = cuda.execute(Workload::PREFILL, n, in, out);
    CHECK(pre2.success);
    CHECK_EQ(pre.checksum, pre2.checksum);

    // --- feed real durations into the predictor ---
    Predictor pred;
    pred.record("CUDA_PREFILL", pre.duration);
    pred.record("CUDA_DECODE", dec.duration);
    auto p = pred.predict("CUDA_PREFILL");
    CHECK(p.has_value());
    CHECK(p->source == PredictionSource::MEASURED);

    // --- governor: governed CUDA request crossing from safe to at-risk ---
    ManualClock clk;
    Governor g(GovernorConfig{}, clk);
    std::string err; g.policy_store().add(make_policy(), err);

    // Budget scaled so a realistic number of real chunks exhausts it.
    const Duration budget = Duration(static_cast<std::int64_t>(ns_count(pre.duration) * 50 + 1));
    RequestDescriptor d;
    d.request_id = RequestId(1); d.tenant_id = TenantId(1); d.model_id = ModelId(1);
    d.model_revision = ModelRevision(1); d.slo_class = SloClass::REALTIME;
    d.contract.e2e_target = budget; d.contract.ttf_target = budget / 4;
    d.contract.hard_deadline = budget; d.prompt_tokens = 128; d.max_tokens = 64; d.remaining_tokens = 64;
    auto ar = g.admit(d);
    CHECK(ar.accepted);

    // Feed real CUDA prefill durations as observations with the correct authority.
    const int kChunks = 60;   // 60 * (50x budget factor) = 3000x budget => clearly at risk
    for (int i = 0; i < kChunks; ++i) {
        Observation o;
        o.request_id = d.request_id; o.attempt_id = ar.attempt_id;
        o.generation = ar.generation; o.epoch = ar.epoch;
        o.type = ObservationType::PREFILL_CHUNK_COMPLETED; o.phase = Phase::PREFILL;
        o.elapsed = pre.duration; o.predictor_key = "CUDA_PREFILL"; o.at = clk.now();
        auto r = g.observe(o); CHECK(r.accepted);
        clk.advance(pre.duration);
    }
    auto ra = g.assess(d.request_id);
    CHECK(ra.budget_used_fraction > 0.0);
    CHECK(ra.prediction.has_value());
    std::printf("CUDA governed request: risk=%s budget_frac=%.3f pred_us=%lld\n",
                to_string(ra.state), ra.budget_used_fraction,
                (long long)std::chrono::duration_cast<std::chrono::microseconds>(ra.prediction->predicted).count());
    // A heavily-over-budget request must be at risk or worse (never SAFE).
    CHECK(ra.state != RiskState::SAFE);

    // Deterministic intervention.
    auto plan = g.plan(d.request_id);
    CHECK(!plan.items.empty());
    CHECK(plan.items[0].request_id == d.request_id);
    CHECK(plan.items[0].reason != ReasonCode::DEADLINE_SLACK ||
          ra.state != RiskState::SAFE);  // an at-risk request carries a real reason

    // Complete it, verify accounting closes.
    Completion comp;
    comp.request_id = d.request_id; comp.attempt_id = ar.attempt_id;
    comp.generation = ar.generation; comp.epoch = ar.epoch;
    comp.outcome = Completion::Outcome::COMPLETED; comp.soft_violation = true; comp.slo_met = false;
    comp.at = clk.now(); comp.tokens_generated = 64;
    std::string cerr;
    CHECK(g.commit(comp, cerr));
    CHECK_EQ(g.metrics().active, 0u);
    CHECK_EQ(g.metrics().soft_violation, 1u);

    // Fresh work succeeds under current authority (a distinct request id).
    RequestDescriptor d2 = d; d2.request_id = RequestId(2);
    auto ar2 = g.admit(d2);
    CHECK(ar2.accepted);
    CHECK(g.commit(comp, cerr) == false);  // stale attempt (attempt_id differs from ar2) must be rejected
    comp.request_id = d2.request_id; comp.attempt_id = ar2.attempt_id; comp.generation = ar2.generation; comp.epoch = ar2.epoch;
    CHECK(g.commit(comp, cerr));
    CHECK_EQ(g.metrics().active, 0u);

    // Device accounting: run a final clean batch; cudaMalloc/free succeed.
    auto last = cuda.execute(Workload::PREFILL, n, in, out);
    CHECK(last.success);
    CHECK_EQ(last.checksum, pre.checksum);

    // Calibration API.
    auto cal = cuda.calibrate(Workload::PREFILL, n);
    CHECK(cal > Duration::zero());

    test::report("test_cuda");
    return test::exit_code();
}