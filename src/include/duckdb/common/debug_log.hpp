#pragma once

#include <cstdio>

// Debug logging helper.
//
// Why this exists:
// - Performance measurements should not be perturbed by ad-hoc logging in release binaries.
// - Debug diagnostics are still useful while developing hash join and planner changes.
//
// Behavior:
// - In DEBUG builds, logs are emitted to stderr using fprintf semantics.
// - In non-DEBUG builds, calls compile to a no-op (arguments are not evaluated).
#ifdef DEBUG
#define DEBUG_LOG(...)                                                                                           \
	do {                                                                                                               \
		std::fprintf(stderr, __VA_ARGS__);                                                                              \
		std::fflush(stderr);                                                                                             \
	} while (0)
#else
#define DEBUG_LOG(...)                                                                                           \
	do {                                                                                                               \
	} while (0)
#endif