#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>

namespace test {
inline int g_failures = 0;
inline int g_checks = 0;
inline int g_expected = -1;

inline void fail(const char* expr, const char* file, int line) {
    ++g_failures;
    std::printf("  [FAIL] %s:%d  %s\n", file, line, expr);
}

inline void report(const char* name) {
    std::printf("[%s] checks=%d failures=%d\n", name, g_checks, g_failures);
    if (g_failures == 0) std::printf("PASS: %s\n", name);
    else std::printf("FAIL: %s (%d failures)\n", name, g_failures);
}

inline int exit_code() { return g_failures == 0 ? 0 : 1; }
} // namespace test

#define CHECK(cond) do { ++test::g_checks; if (!(cond)) test::fail(#cond, __FILE__, __LINE__); } while (0)
#define CHECK_EQ(a, b) do { ++test::g_checks; if (!((a) == (b))) test::fail(#a " == " #b, __FILE__, __LINE__); } while (0)
#define CHECK_TRUE(cond) CHECK(cond)
#define CHECK_FALSE(cond) do { ++test::g_checks; if ((cond)) test::fail("!" #cond, __FILE__, __LINE__); } while (0)
#define CHECK_NEAR(a, b, eps) do { ++test::g_checks; if (std::abs((a) - (b)) > (eps)) test::fail(#a " ~= " #b, __FILE__, __LINE__); } while (0)
