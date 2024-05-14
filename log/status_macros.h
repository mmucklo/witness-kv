// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd

// From: util/task/contrib/status_macros/status_macros.h
// Also From: src/google/protobuf/port_def.inc
// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/stubs/status_macros.h
// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/port_def.inc

// MODIFIED to 

#ifndef STATUS_MACROS_H
#define STATUS_MACROS_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#if defined(STATUS_MACROS_PREDICT_TRUE) || defined(STATUS_MACROS_PREDICT_FALSE)
#error PREDICT_(TRUE|FALSE) was previously defined
#endif
#if defined(__GNUC__)
# define STATUS_MACROS_PREDICT_TRUE(x) (__builtin_expect(false || (x), true))
# define STATUS_MACROS_PREDICT_FALSE(x) (__builtin_expect(false || (x), false))
#else
# define STATUS_MACROS_PREDICT_TRUE(x) (x)
# define STATUS_MACROS_PREDICT_FALSE(x) (x)
#endif
// Run a command that returns a util::Status.  If the called code returns an
// error status, return that status up out of this method too.
//
// Example:
//   RETURN_IF_ERROR(DoThings(4));
#define RETURN_IF_ERROR(expr)                                                \
  do {                                                                       \
    /* Using _status below to avoid capture problems if expr is "status". */ \
    const absl::Status _status = (expr);                                     \
    if (STATUS_MACROS_PREDICT_FALSE(!_status.ok())) return _status;               \
  } while (0)

// Executes an expression that returns a absl::StatusOr, extracting its value
// into the variable defined by lhs (or returning on error).
//
// Example: Assigning to an existing value
//   ValueType value;
//   ASSIGN_OR_RETURN(value, MaybeGetValue(arg));
//
// MODIFIED and SIMPLIFIED by mmucklo 2024/05/14
#define ASSIGN_OR_RETURN(lhs, rexpr) \
  if (STATUS_MACROS_PREDICT_FALSE(!(rexpr).ok())) { return (rexpr).status(); } \
  lhs = (rexpr).value();

#endif  // STATUS_MACROS_H
