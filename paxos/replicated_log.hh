#ifndef REPLICATED_LOG_H_
#define REPLICATED_LOG_H_

#include "common.hh"
#include "log/log_writer.h"
#include "log/logs_truncator.h"

namespace witnesskvs::paxos {

struct ReplicatedLogEntry {
  uint64_t idx_{};
  uint64_t min_proposal_{};
  uint64_t accepted_proposal_{};
  std::string accepted_value_{};
  bool is_chosen_{};
};

class ReplicatedLog {
 private:
  uint8_t node_id_;

  mutable absl::Mutex lock_;
  uint64_t first_unchosen_index_ ABSL_GUARDED_BY(lock_);
  uint64_t proposal_number_ ABSL_GUARDED_BY(lock_);

  std::map<uint64_t, ReplicatedLogEntry> log_entries_ ABSL_GUARDED_BY(lock_);

  static constexpr uint8_t num_bits_for_node_id_ = 3;
  static constexpr uint8_t max_node_id_ = (1ull << num_bits_for_node_id_) - 1;
  static constexpr uint64_t mask_ = ~(max_node_id_);

  std::unique_ptr<witnesskvs::log::LogsTruncator> logs_truncator_;
  std::unique_ptr<witnesskvs::log::LogWriter> log_writer_;

  void UpdateFirstUnchosenIdx();

  void MakeLogEntryStable(const ReplicatedLogEntry &entry);

  std::function<void(std::string)> app_registered_callback_;

 public:
  ReplicatedLog(uint8_t node_id,
                std::function<void(std::string)> callback = nullptr);
  ~ReplicatedLog();

  uint64_t GetFirstUnchosenIdx();
  uint64_t GetNextProposalNumber();
  void UpdateProposalNumber(uint64_t prop_num);
  void MarkLogEntryChosen(uint64_t idx);
  void SetLogEntryAtIdx(uint64_t idx, std::string value);

  uint64_t GetMinProposalForIdx(uint64_t idx);
  void UpdateMinProposalForIdx(uint64_t idx, uint64_t new_min_proposal);
  ReplicatedLogEntry GetLogEntryAtIdx(uint64_t idx);

  // Updates the log entry if the existing entry has a lower min_proposal than
  // new_entry. Regardless returns the proposal number needed for this entry to
  // be updated.
  uint64_t UpdateLogEntry(const ReplicatedLogEntry &new_entry);

  // Useful for unit testing.
  std::map<uint64_t, ReplicatedLogEntry> GetLogEntries() const {
    absl::ReaderMutexLock l(&lock_);
    return log_entries_;
  }
};

}  // namespace witnesskvs::paxos
#endif  // REPLICATED_LOG_H_
