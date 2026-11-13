#include "latency_governor/protocol.hpp"
#include "latency_governor/serialization.hpp"
#include "latency_governor/enums.hpp"
#include "latency_governor/identifiers.hpp"
#include "latency_governor/clock.hpp"
#include "test_framework.hpp"

#include <atomic>
#include <string>
#include <thread>
#include <chrono>

using namespace latency_governor;
using namespace latency_governor::net;

namespace {
constexpr std::uint16_t kPort = 48221;
} // namespace

int main() {
    std::string err;
    SocketInitGuard init(err);
    CHECK(init.ok());

    // ---- Success round trip: server accepts, reads HELLO, replies HELLO_ACK.
    SocketHandle listen;
    CHECK(tcp_listen("127.0.0.1", kPort, listen, err));
    std::atomic<bool> server_ok{false};
    std::string server_err;
    std::thread server([&]() {
        SocketHandle conn;
        if (!tcp_accept(listen, conn, server_err)) return;
        Frame f;
        if (!recv_frame(conn, f, server_err)) { server_err += " recv:" + server_err; tcp_close(conn); return; }
        if (f.type != MessageType::HELLO) { server_err = "wrong msg type"; tcp_close(conn); return; }
        std::uint32_t v, abi;
        BinaryReader hr(std::string_view(f.payload));
        if (!decode_hello(hr, v, abi)) { server_err = "bad hello"; tcp_close(conn); return; }
        // Reply HELLO_ACK.
        BinaryWriter w; encode_hello(w, kProtocolVersion, 1);
        Frame reply; reply.type = MessageType::HELLO_ACK; reply.payload = w.data();
        if (!send_frame(conn, reply, server_err)) { server_err = "send:" + server_err; tcp_close(conn); return; }
        server_ok = true;
        tcp_close(conn);
    });

    SocketHandle client;
    CHECK(tcp_connect("127.0.0.1", kPort, client, err));
    {
        BinaryWriter w; encode_hello(w, kProtocolVersion, 1);
        Frame hello; hello.type = MessageType::HELLO; hello.payload = w.data();
        CHECK(send_frame(client, hello, err));
        Frame reply;
        CHECK(recv_frame(client, reply, err));
        CHECK(reply.type == MessageType::HELLO_ACK);
        std::uint32_t v, abi;
        BinaryReader rr2(std::string_view(reply.payload));
        CHECK(decode_hello(rr2, v, abi));
        CHECK_EQ(v, kProtocolVersion);
    }
    tcp_close(client);
    tcp_close(listen);
    if (server.joinable()) server.join();
    CHECK(server_ok);

    // ---- Malformed decoder checks (non-blocking, deterministic).
    {
        std::uint32_t v, abi;
        BinaryReader short1(std::string_view("\x01"));
        CHECK_FALSE(decode_hello(short1, v, abi));   // truncated
    }
    {
        // Observation codec must reject a truncated payload.
        BinaryReader short2(std::string_view("\x00\x00"));
        Observation o;
        CHECK_FALSE(decode_observation(short2, o));
    }
    {
        // Admit codec rejects an out-of-range enum.
        BinaryWriter w;
        w.id(RequestId(1)); w.id(TenantId(1)); w.id(ModelId(1)); w.id(ModelRevision(1));
        w.bool_(false); w.enums_raw(99 /* out of range SloClass */);
        BinaryReader r_enc(w.data());
        RequestDescriptor d;
        CHECK_FALSE(decode_admit(r_enc, d));
    }

    // Frame header parsing via raw bytes (big-endian).
    {
        std::string hdr;
        auto put = [&](std::uint32_t v) { for (int s=24;s>=0;s-=8) hdr.push_back(static_cast<char>((v>>s)&0xFF)); };
        put(kFrameMagic); put(kProtocolVersion); put(static_cast<std::uint32_t>(MessageType::PING)); put(0u);
        BinaryReader r(hdr);
        std::uint32_t m, ver, ty, len;
        CHECK(r.u32(m) && m == kFrameMagic);
        CHECK(r.u32(ver) && ver == kProtocolVersion);
        CHECK(r.u32(ty) && ty == static_cast<std::uint32_t>(MessageType::PING));
        CHECK(r.u32(len) && len == 0u);
        CHECK(r.at_end());
    }

    // Round trip of an observation payload through encode/decode.
    {
        Observation o;
        o.id = ObservationId(3); o.request_id = RequestId(100); o.attempt_id = AttemptId(1);
        o.generation = Generation(2); o.epoch = CoordinatorEpoch(1);
        o.type = ObservationType::DECODE_STEP; o.phase = Phase::DECODE; o.elapsed = ms(3);
        o.value = 3.5; o.tokens = 1; o.predictor_key = "DECODE";
        o.at = mono_now();
        BinaryWriter w; encode_observation(w, o);
        BinaryReader r(w.data());
        Observation o2;
        CHECK(decode_observation(r, o2));
        CHECK(o2.request_id == o.request_id);
        CHECK(o2.attempt_id == o.attempt_id);
        CHECK(o2.generation == o.generation);
        CHECK(o2.epoch == o.epoch);
        CHECK(o2.type == o.type);
        CHECK(o2.phase == o.phase);
        CHECK(o2.elapsed == o.elapsed);
        CHECK(o2.value.has_value() && *o2.value == 3.5);
        CHECK(o2.tokens == 1);
        CHECK(o2.predictor_key == "DECODE");
        CHECK(r.at_end());
    }

    test::report("test_protocol");
    return test::exit_code();
}