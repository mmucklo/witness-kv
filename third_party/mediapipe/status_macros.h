// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file or at
// https://developers.google.com/open-source/licenses/bsd
// https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/stubs/status_macros.h

// Copyright 2019 The MediaPipe Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// MODIFIED by mmucklo some.

#ifndef STATUS_MACROS_H
#define STATUS_MACROS_H

#include <cstdint>

#include "mediapipe_status_builder.h"

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

// Run a command that returns a util::Status.  If the called code returns an
// error status, return that status up out of this method too.
//
// Example:
//   RETURN_IF_ERROR(DoThings(4));
#define RETURN_IF_ERROR(expr)                                                \
  do {                                                                       \
    /* Using _status below to avoid capture problems if expr is "status". */ \
    const absl::Status _status = (expr);                                     \
    if (ABSL_PREDICT_FALSE(!_status.ok())) return _status;          \
  } while (0)

// Executes an expression `rexpr` that returns a `absl::StatusOr<T>`. On
// OK, extracts its value into the variable defined by `lhs`, otherwise returns
// from the current function. By default the error status is returned
// unchanged, but it may be modified by an `error_expression`. If there is an
// error, `lhs` is not evaluated; thus any side effects that `lhs` may have
// only occur in the success case.
//
// Interface:
//
//   ASSIGN_OR_RETURN(lhs, rexpr)
//   ASSIGN_OR_RETURN(lhs, rexpr, error_expression);
//
// WARNING: expands into multiple statements; it cannot be used in a single
// statement (e.g. as the body of an if statement without {})!
//
// Example: Declaring and initializing a new variable (ValueType can be anything
//          that can be initialized with assignment, including references):
//   ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(arg));
//
// Example: Assigning to an existing variable:
//   ValueType value;
//   ASSIGN_OR_RETURN(value, MaybeGetValue(arg));
//
// Example: Assigning to an expression with side effects:
//   MyProto data;
//   ASSIGN_OR_RETURN(*data.mutable_str(), MaybeGetValue(arg));
//   // No field "str" is added on error.
//
// Example: Assigning to a std::unique_ptr.
//   ASSIGN_OR_RETURN(std::unique_ptr<T> ptr, MaybeGetPtr(arg));
//
// If passed, the `error_expression` is evaluated to produce the return
// value. The expression may reference any variable visible in scope, as
// well as a `mediapipe::StatusBuilder` object populated with the error and
// named by a single underscore `_`. The expression typically uses the
// builder to modify the status and is returned directly in manner similar
// to RETURN_IF_ERROR. The expression may, however, evaluate to any type
// returnable by the function, including (void). For example:
//
// Example: Adjusting the error message.
//   ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(query),
//                    _ << "while processing query " << query.DebugString());
//
// Example: Logging the error on failure.
//   ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(query), _.LogError());
//
#define ASSIGN_OR_RETURN(...)                                     \
  MP_STATUS_MACROS_IMPL_GET_VARIADIC_(                            \
      (__VA_ARGS__, MP_STATUS_MACROS_IMPL_MP_ASSIGN_OR_RETURN_3_, \
       MP_STATUS_MACROS_IMPL_MP_ASSIGN_OR_RETURN_2_))             \
  (__VA_ARGS__)

// =================================================================
// == Implementation details, do not rely on anything below here. ==
// =================================================================

// MSVC incorrectly expands variadic macros, splice together a macro call to
// work around the bug.
#define MP_STATUS_MACROS_IMPL_GET_VARIADIC_HELPER_(_1, _2, _3, NAME, ...) NAME
#define MP_STATUS_MACROS_IMPL_GET_VARIADIC_(args) \
  MP_STATUS_MACROS_IMPL_GET_VARIADIC_HELPER_ args

#define MP_STATUS_MACROS_IMPL_MP_ASSIGN_OR_RETURN_2_(lhs, rexpr)               \
  MP_STATUS_MACROS_IMPL_MP_ASSIGN_OR_RETURN_(                                  \
      MP_STATUS_MACROS_IMPL_CONCAT_(_status_or_value, __LINE__), lhs, rexpr,   \
      return mediapipe::StatusBuilder(                                         \
          std::move(MP_STATUS_MACROS_IMPL_CONCAT_(_status_or_value, __LINE__)) \
              .status(),                                                       \
          MEDIAPIPE_LOC))
#define MP_STATUS_MACROS_IMPL_MP_ASSIGN_OR_RETURN_3_(lhs, rexpr,               \
                                                     error_expression)         \
  MP_STATUS_MACROS_IMPL_MP_ASSIGN_OR_RETURN_(                                  \
      MP_STATUS_MACROS_IMPL_CONCAT_(_status_or_value, __LINE__), lhs, rexpr,   \
      mediapipe::StatusBuilder _(                                              \
          std::move(MP_STATUS_MACROS_IMPL_CONCAT_(_status_or_value, __LINE__)) \
              .status(),                                                       \
          MEDIAPIPE_LOC);                                                      \
      (void)_; /* error_expression is allowed to not use this variable */      \
      return (error_expression))
#define MP_STATUS_MACROS_IMPL_MP_ASSIGN_OR_RETURN_(statusor, lhs, rexpr, \
                                                   error_expression)     \
  auto statusor = (rexpr);                                               \
  if (ABSL_PREDICT_FALSE(!statusor.ok())) {                              \
    error_expression;                                                    \
  }                                                                      \
  lhs = std::move(statusor).value()

// Internal helper for concatenating macro values.
#define MP_STATUS_MACROS_IMPL_CONCAT_INNER_(x, y) x##y
#define MP_STATUS_MACROS_IMPL_CONCAT_(x, y) \
  MP_STATUS_MACROS_IMPL_CONCAT_INNER_(x, y)

// The GNU compiler emits a warning for code like:
//
//   if (foo)
//     if (bar) { } else baz;
//
// because it thinks you might want the else to bind to the first if.  This
// leads to problems with code like:
//
//   if (do_expr) RETURN_IF_ERROR(expr) << "Some message";
//
// The "switch (0) case 0:" idiom is used to suppress this.
#define MP_STATUS_MACROS_IMPL_ELSE_BLOCKER_ \
  switch (0)                                \
  case 0:                                   \
  default:  // NOLINT

#endif
