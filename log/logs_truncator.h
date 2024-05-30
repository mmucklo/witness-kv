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
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "log.pb.h"

namespace witnesskvs::log {

// TODO(mmucklo) - feedback loop from rotation.
// TODO(mmucklo) - truncate from filesystem rather than memory hash.
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

  // Registers a set of max_idx and min_idx against a filename.
  void Register(TruncationFileInfo truncation_file_info);

  absl::flat_hash_map<std::string, TruncationFileInfo> filename_max_idx()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(lock_);

 private:
  void Init();
  void Run(std::stop_token& stop_token);
  void DoTruncation(uint64_t max_idx);
  void DoSingleFileTruncation(absl::string_view filename, uint64_t max_idx);

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
      ABSL_GUARDED_BY(truncation_queue_lock_);

  // Map of filename to max_idx.
  absl::Mutex lock_;
  absl::flat_hash_map<std::string, TruncationFileInfo> filename_max_idx_
      ABSL_GUARDED_BY(lock_);

  // Whether the map is in use, or not (memory constraints).
  bool filename_max_idx_in_use_;

  const std::string dir_;
  const std::string prefix_;
  std::jthread worker_;
  const std::function<uint64_t(const Log::Message&)> idxfn_;
};

}  // namespace witnesskvs::log

#endif
