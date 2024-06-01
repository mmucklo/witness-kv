#ifndef LOG_LOGS_TRUNCATOR_H
#define LOG_LOGS_TRUNCATOR_H

#include <cstdint>
#include <functional>
#include <queue>
#include <stop_token>
#include <thread>
#include <variant>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "log.pb.h"

namespace witnesskvs::log {

// LogsTruncator, will periodically truncate logs
// IMPORTANT: must be instantiated before LogWriter and destroyed after the
// same.
class LogsTruncator {
 public:
  LogsTruncator(std::string dir, std::string prefix,
                std::function<uint64_t(const Log::Message&)> idxfn);

  using TruncationIdx = uint64_t;
  struct TruncationFileInfo {
    uint64_t min_idx;
    uint64_t max_idx;
    std::optional<std::string> filename;
  };

  // Truncates log files up to max_idx.
  void Truncate(TruncationIdx max_idx);

  absl::flat_hash_map<std::string, TruncationFileInfo> filename_max_idx()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

  // Returns a callback function primarily for use in LogWriter during log
  // rotations.
  absl::AnyInvocable<void(std::string, uint64_t, uint64_t)> GetCallbackFn();

 private:
  // TODO(mmucklo): method comments.
  void Init();
  void Run(std::stop_token& stop_token);
  void DoTruncation(uint64_t max_idx);
  void DoSingleFileTruncation(absl::string_view filename, uint64_t max_idx);

  // Registers a set of max_idx and min_idx against a filename.
  void Register(TruncationFileInfo truncation_file_info);

  // Reads a header and updates filename_max_idx_ hash.
  //
  // insert signals whether we expect an insert or update.
  absl::Status ReadHeader(const std::filesystem::path& path,
                          bool insert = true);

  // The truncation queue is a lightweight place to enqueue requests into
  // the truncator without the need to hold locks for an extended period of
  // time (such as waiting for the truncator to finish a truncation which
  // could involve I/O).
  absl::Mutex queue_lock_;
  std::queue<std::variant<TruncationIdx, TruncationFileInfo>> queue_
      ABSL_GUARDED_BY(queue_lock_);

  absl::Mutex lock_;

  // Map of filename to max_idx.
  // This is a list of rotatable filenames, and their min/max idx values.
  absl::flat_hash_map<std::string, TruncationFileInfo> filename_max_idx_
      ABSL_GUARDED_BY(lock_);

  // TODO(mmucklo) if there are a lot of log files, the map could get a bit big
  // in which case we can just iterate over the directory and directly read each
  // file header one by one when we want to truncate.
  // bool filename_max_idx_in_use_;

  const std::string dir_;
  const std::string prefix_;
  std::jthread worker_;
  const std::function<uint64_t(const Log::Message&)> idxfn_;
};

}  // namespace witnesskvs::log

#endif
