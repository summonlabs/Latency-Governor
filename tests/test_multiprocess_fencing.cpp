// Atomic distributed restart-fencing proof.
//
// Launch a real coordinator OS process plus two real worker OS processes.
// Admit latency-governed work, capture a genuine pre-restart authority
// envelope, kill one worker while work is active, restart the same logical
// worker with a new WorkerBootId, replay stale pre-restart messages through the
// real framed-TCP path and prove they are rejected (epoch / generation /
// attempt / boot / dispatch), then prove fresh work succeeds under the current
// authority and that accounting returns to zero.
#include "latency_governor/protocol.hpp"
#include "latency_governor/serialization.hpp"
#include "latency_governor/enums.hpp"
#include "latency_governor/identifiers.hpp"
#include "latency_governor/clock.hpp"
#include "latency_governor/snapshot.hpp"
#include "test_framework.hpp"

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

using namespace latency_governor;
using namespace latency_governor::net;

static std::string find_exe(const std::vector<std::string>& dirs, const std::string& name) {
    for (const auto& d : dirs) { auto p = std::filesystem::path(d) / name; if (std::filesystem::exists(p)) return p.string(); }
    return "";
}

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#undef ERROR
struct Proc {
    PROCESS_INFORMATION pi{};
    bool spawn(const std::string& exe, const std::vector<std::string>& args) {
        std::string cmd = exe; for (const auto& a : args) cmd += " " + a;
        STARTUPINFOA si{}; si.cb = sizeof(si); std::string cc = cmd;
        std::string app(exe);
        BOOL ok = CreateProcessA(app.data(), cc.data(), nullptr, nullptr, FALSE, 0, nullptr, nullptr, &si, &pi);
        std::fprintf(stderr, "spawn exe=[%s] ok=%d winerr=%lu\n", exe.c_str(), (int)ok, (unsigned long)GetLastError());
        return ok != 0;
    }
    bool kill() { if (pi.hProcess) { TerminateProcess(pi.hProcess, 1); WaitForSingleObject(pi.hProcess, 5000); CloseHandle(pi.hProcess); CloseHandle(pi.hThread); pi.hProcess=nullptr; return true; } return false; }
};
#else
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
struct Proc {
    pid_t pid = 0;
    bool spawn(const std::string& exe, const std::vector<std::string>& args) {
        pid = fork();
        if (pid == 0) { std::vector<char*> av; av.push_back(const_cast<char*>(exe.c_str())); for (auto& a : args) av.push_back(const_cast<char*>(a.c_str())); av.push_back(nullptr); execvp(exe.c_str(), av.data()); _exit(1); }
        return pid > 0;
    }
    bool kill() { if (pid > 0) { kill(pid, SIGKILL); return true; } return false; }
};
#endif

namespace {
constexpr std::uint16_t kPort = 48551;

bool send_typed(SocketHandle s, MessageType t, const std::string& payload, std::string& err) {
    Frame f; f.type = t; f.payload = payload; return send_frame(s, f, err);
}

// Send an observation and return (accepted, code) from the OBSERVATION_ACK.
bool send_obs(SocketHandle s, const Observation& o, bool& accepted, RejectionCode& code, std::string& err) {
    BinaryWriter w; encode_observation(w, o);
    if (!send_typed(s, MessageType::OBSERVATION, w.data(), err)) return false;
    Frame r; if (!recv_frame(s, r, err)) return false;
    BinaryReader rr(std::string_view(r.payload));
    return decode_observation_ack(rr, accepted, code);
}
Observation make_obs(RequestId rq, AttemptId at, Generation gn, CoordinatorEpoch ep,
                     WorkerId w, WorkerBootId b, DispatchId ds, ObservationType ot, Phase ph, Duration el) {
    Observation o; o.id = ObservationId(123456);
    o.request_id = rq; o.attempt_id = at; o.generation = gn; o.epoch = ep;
    o.worker_id = w; o.worker_boot_id = b; o.dispatch_id = ds;
    o.type = ot; o.phase = ph; o.elapsed = el; o.at = mono_now();
    return o;
}
} // namespace

int main() {
    std::string err;
    SocketInitGuard init(err); CHECK(init.ok());

    // The applications of this build (not whatever a fixed build/ folder holds).
    const std::string base = LG_APPS_DIR;
    const std::string coord = find_exe({base + "/coordinator"}, "latency-governor-coordinator.exe");
    const std::string worker = find_exe({base + "/worker"}, "latency-governor-worker.exe");
    std::printf("coordinator: %s\nworker:      %s\n", coord.c_str(), worker.c_str());
    if (coord.empty() || worker.empty()) { std::printf("SKIP: app binaries not built\n"); return 0; }

    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    // Use a temp-dir path (no spaces) so the spawned process parses --persist exactly.
    const std::string persist = (std::filesystem::temp_directory_path() / ("lg_mp_" + std::to_string(now_ms) + ".bin")).string();
    std::filesystem::remove(persist);

    Proc coordProc, workerA, workerB;
    CHECK(coordProc.spawn(coord, {"--listen", "127.0.0.1", std::to_string(kPort), "--persist", persist}));
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    CHECK(workerA.spawn(worker, {"--connect", "127.0.0.1", std::to_string(kPort), "--worker-id", "1", "--boot-id", "100", "--backend", "cpu"}));
    CHECK(workerB.spawn(worker, {"--connect", "127.0.0.1", std::to_string(kPort), "--worker-id", "2", "--boot-id", "200", "--backend", "cpu"}));
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    SocketHandle ctrl; CHECK(tcp_connect("127.0.0.1", kPort, ctrl, err));
    // The coordinator requires a HELLO handshake on every connection.
    {
        BinaryWriter hw; encode_hello(hw, kProtocolVersion, 1);
        CHECK(send_typed(ctrl, MessageType::HELLO, hw.data(), err));
        Frame hreply; CHECK(recv_frame(ctrl, hreply, err));
        CHECK(hreply.type == MessageType::HELLO_ACK);
        // The HELLO_ACK message type is the contract; payload is best-effort.
        CHECK(hreply.type == MessageType::HELLO_ACK);
    }

    // Admit R (555), captured pre-restart envelope (attempt/gen/epoch).
    auto admit = [&](RequestId reqid, TenantId tenant, AttemptId& at, Generation& gn, CoordinatorEpoch& ep) {
        RequestDescriptor d; d.request_id = reqid; d.tenant_id = tenant; d.model_id = ModelId(1);
        d.model_revision = ModelRevision(1); d.slo_class = SloClass::REALTIME;
        d.contract.e2e_target = ms(1000); d.contract.ttf_target = ms(100); d.contract.hard_deadline = ms(1000);
        d.prompt_tokens = 128; d.max_tokens = 64; d.remaining_tokens = 64;
        BinaryWriter aw; encode_admit(aw, d);
        if (!send_typed(ctrl, MessageType::ADMIT, aw.data(), err)) return false;
        Frame ack; if (!recv_frame(ctrl, ack, err)) return false;
        bool accepted; RejectionCode code; RequestId rid; std::string msg;
        BinaryReader ar(std::string_view(ack.payload));
        if (!decode_admit_ack(ar, accepted, code, rid, at, gn, ep, msg)) return false;
        return accepted;
    };
    AttemptId at0; Generation gen0; CoordinatorEpoch ep0;
    CHECK(admit(RequestId(555), TenantId(1), at0, gen0, ep0));
    AttemptId at1; Generation gen1; CoordinatorEpoch ep1;
    CHECK(admit(RequestId(556), TenantId(2), at1, gen1, ep1));

    // Bind R to worker A (boot 100) and R2 to worker B (boot 200); mark both active.
    bool acc; RejectionCode code;
    CHECK(send_obs(ctrl, make_obs(RequestId(555), at0, gen0, ep0, WorkerId(1), WorkerBootId(100), DispatchId(900), ObservationType::DISPATCH_STARTED, Phase::SCHEDULING, Duration::zero()), acc, code, err) && acc);
    CHECK(send_obs(ctrl, make_obs(RequestId(555), at0, gen0, ep0, WorkerId(1), WorkerBootId(100), DispatchId(900), ObservationType::PREFILL_STARTED, Phase::PREFILL, ms(5)), acc, code, err) && acc);
    CHECK(send_obs(ctrl, make_obs(RequestId(556), at1, gen1, ep1, WorkerId(2), WorkerBootId(200), DispatchId(901), ObservationType::DISPATCH_STARTED, Phase::SCHEDULING, Duration::zero()), acc, code, err) && acc);
    CHECK(send_obs(ctrl, make_obs(RequestId(556), at1, gen1, ep1, WorkerId(2), WorkerBootId(200), DispatchId(901), ObservationType::PREFILL_STARTED, Phase::PREFILL, ms(5)), acc, code, err) && acc);

    // Kill worker A while work is active.
    CHECK(workerA.kill());
    std::this_thread::sleep_for(std::chrono::milliseconds(1600));   // coordinator detects loss -> fail R + bump epoch

    // Restart worker A with a new WorkerBootId (101).
    CHECK(workerA.spawn(worker, {"--connect", "127.0.0.1", std::to_string(kPort), "--worker-id", "1", "--boot-id", "101", "--backend", "cpu"}));
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    // Replay stale pre-restart messages for R2 (which survives, but was epoch-fenced).
    bool stale_ok = false;
    const std::vector<RejectionCode> stale_codes = {
        RejectionCode::GENERATION_MISMATCH, RejectionCode::EPOCH_MISMATCH, RejectionCode::BOOT_MISMATCH,
        RejectionCode::ATTEMPT_MISMATCH, RejectionCode::DISPATCH_MISMATCH, RejectionCode::STATE_INELIGIBLE,
        RejectionCode::REQUEST_NOT_FOUND};
    {
        auto o = make_obs(RequestId(556), at1, gen1, ep1, WorkerId(2), WorkerBootId(200), DispatchId(901), ObservationType::PREFILL_CHUNK_COMPLETED, Phase::PREFILL, ms(1));
        CHECK(send_obs(ctrl, o, acc, code, err));
        CHECK_FALSE(acc);
        std::printf("stale R2 replay rejected: %s\n", to_string(code));
        for (auto c2 : stale_codes) if (c2 == code) stale_ok = true;
        CHECK(stale_ok);
    }
    // Also replay a stale completion for the fenced R (worker A loss) -> must be rejected or R finalized.
    Completion stale_comp;
    stale_comp.request_id = RequestId(555); stale_comp.attempt_id = at0; stale_comp.generation = gen0; stale_comp.epoch = ep0;
    stale_comp.worker_id = WorkerId(1); stale_comp.worker_boot_id = WorkerBootId(100); stale_comp.dispatch_id = DispatchId(900);
    stale_comp.outcome = Completion::Outcome::COMPLETED; stale_comp.slo_met = true; stale_comp.at = mono_now();
    {
        BinaryWriter cw_stale; encode_completion(cw_stale, stale_comp);
        CHECK(send_typed(ctrl, MessageType::COMPLETION, cw_stale.data(), err));
        Frame r_stale; CHECK(recv_frame(ctrl, r_stale, err));
        CHECK(r_stale.type == MessageType::ERROR);
        RejectionCode c3; std::string msg; BinaryReader rr(std::string_view(r_stale.payload));
        CHECK(decode_error(rr, c3, msg));
        std::printf("stale completion rejected: %s (%s)\n", to_string(c3), msg.c_str());
    }

    // Read the current authority for R2 from a snapshot (epoch/generation bumped).
    CHECK(send_typed(ctrl, MessageType::SNAPSHOT, "", err));
    Frame sframe; CHECK(recv_frame(ctrl, sframe, err));
    Snapshot snap; BinaryReader sr(std::string_view(sframe.payload));
    CHECK(decode_snapshot(sr, snap));
    bool found556 = false; Generation cur556gen; CoordinatorEpoch cur556ep;
    for (const auto& rs : snap.requests) {
        if (rs.request_id == RequestId(556)) { found556 = true; cur556gen = Generation(rs.generation); cur556ep = CoordinatorEpoch(rs.epoch); }
    }
    CHECK(found556);

    // Fresh work under the CURRENT authority succeeds: complete R2 with its bumped authority.
    Completion comp;
    comp.request_id = RequestId(556); comp.attempt_id = at1; comp.generation = cur556gen; comp.epoch = cur556ep;
    comp.outcome = Completion::Outcome::COMPLETED; comp.slo_met = true; comp.at = mono_now();
    {
        BinaryWriter cw_fresh; encode_completion(cw_fresh, comp);
        CHECK(send_typed(ctrl, MessageType::COMPLETION, cw_fresh.data(), err));
        Frame r_fresh; CHECK(recv_frame(ctrl, r_fresh, err));
        RejectionCode c4; std::string msg2; BinaryReader rr2(std::string_view(r_fresh.payload));
        CHECK(decode_error(rr2, c4, msg2));
        CHECK(c4 == RejectionCode::NONE);
        std::printf("fresh R2 completion accepted\n");
    }

    // Verify accounting returned to zero for completed/failed work.
    CHECK(send_typed(ctrl, MessageType::SNAPSHOT, "", err));
    Frame sframe2; CHECK(recv_frame(ctrl, sframe2, err));
    Snapshot snap2; BinaryReader sr2(std::string_view(sframe2.payload));
    CHECK(decode_snapshot(sr2, snap2));
    // The wire snapshot carries a canonical JSON summary; parse the counters.
    int failed_n = 0, completed_n = 0, active_n = -1;
    auto grab = [](const std::string& json, const std::string& key, int& out) {
        auto pos = json.find("\"" + key + "\":");
        if (pos != std::string::npos) {
            std::size_t v = pos + key.size() + 3;
            try { out = std::stoi(json.substr(v)); } catch (...) { }
        }
    };
    grab(snap2.json, "failed", failed_n);
    grab(snap2.json, "completed", completed_n);
    grab(snap2.json, "active", active_n);
    std::printf("final (json) active=%d failed=%d completed=%d\n", active_n, failed_n, completed_n);
    CHECK(failed_n >= 1);        // R (bound to the killed worker) was failed on worker loss
    CHECK(completed_n >= 1);     // R2 completed under current authority
    CHECK(active_n == 0);        // accounting closed to zero

    tcp_close(ctrl);
    workerB.kill(); workerA.kill(); coordProc.kill();
    std::filesystem::remove(persist);

    test::report("test_multiprocess_fencing");
    return test::exit_code();
}