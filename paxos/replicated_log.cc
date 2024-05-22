#include "replicated_log.hh"

ReplicatedLog::ReplicatedLog(uint8_t node_id) : node_id_{node_id} {
  CHECK_LT(node_id, max_node_id_) << "Node initialization has gone wrong.";
  // TODO[V]: Read `first_unchosen_index_` and `log_entries_` from the actual
  // log.
  first_unchosen_index_ = 0;
  proposal_number_ = 0;
}

ReplicatedLog::~ReplicatedLog() {}

uint64_t ReplicatedLog::GetFirstUnchosenIdx() {
  absl::MutexLock l(&log_mutex_);
  return first_unchosen_index_;
}

uint64_t ReplicatedLog::GetNextProposalNumber() {
  absl::MutexLock l(&log_mutex_);
  proposal_number_ =
      ((proposal_number_ & mask_) + (1ull << num_bits_for_node_id_)) |
      (uint64_t)node_id_;
  LOG(INFO) << "Generated proposal number: " << proposal_number_;
  return proposal_number_;
}

void ReplicatedLog::UpdateProposalNumber(uint64_t prop_num) {
  absl::MutexLock l(&log_mutex_);
  if (prop_num > proposal_number_) {
    proposal_number_ = prop_num;
  }
}

void ReplicatedLog::UpdateFirstUnchosenIdx() {
  log_mutex_.AssertHeld();
  for (uint64_t i = first_unchosen_index_; i <= log_entries_.rbegin()->first;
       i++) {
    auto it = log_entries_.find(i);
    if (it == log_entries_.end()) {
      break;
    }
    if (!it->second.is_chosen_) {
      break;
    }
    first_unchosen_index_++;
  }
  LOG(INFO) << "First unchosen index after UpdateFirstUnchosenIdx: "
            << first_unchosen_index_;
}

void ReplicatedLog::MarkLogEntryChosen(uint64_t idx) {
  absl::MutexLock l(&log_mutex_);
  ReplicatedLogEntry &entry = log_entries_[idx];
  CHECK(!entry.is_chosen_);
  entry.is_chosen_ = true;

  UpdateFirstUnchosenIdx();
}

void ReplicatedLog::SetLogEntryAtIdx(uint64_t idx, std::string value) {
  absl::MutexLock l(&log_mutex_);
  ReplicatedLogEntry &entry = log_entries_[idx];
  if (entry.accepted_value_ != value) {
    // This is fine, as it is possible we may be the only node that accepted a
    // value but that value never got quorum, some other value won and now we
    // are learning about it.
    LOG(INFO) << "Choosing a different value (" << value
              << ") than what was previously accepted ("
              << entry.accepted_value_ << ")";
  }

  entry.accepted_value_ = value;
  entry.is_chosen_ = true;

  UpdateFirstUnchosenIdx();
}

uint64_t ReplicatedLog::GetMinProposalForIdx(uint64_t idx) {
  absl::MutexLock l(&log_mutex_);
  ReplicatedLogEntry &entry = log_entries_[idx];
  return entry.min_proposal_;
}

void ReplicatedLog::UpdateMinProposalForIdx(uint64_t idx,
                                            uint64_t new_min_proposal) {
  absl::MutexLock l(&log_mutex_);
  auto it = log_entries_.find(idx);
  CHECK(it != log_entries_.end()) << "Attempting to update min proposal for "
                                     "a log entry that does not exist.";

  CHECK(new_min_proposal > it->second.min_proposal_)
      << "Cannot attempt to make an update to min proposal with a lower value.";

  it->second.min_proposal_ = new_min_proposal;
}

ReplicatedLogEntry ReplicatedLog::GetLogEntryAtIdx(uint64_t idx) {
  absl::MutexLock l(&log_mutex_);
  auto it = log_entries_.find(idx);
  CHECK(it != log_entries_.end());
  return it->second;
}

uint64_t ReplicatedLog::UpdateLogEntryAtIdx(uint64_t idx,
                                            ReplicatedLogEntry new_entry) {
  absl::MutexLock l(&log_mutex_);
  ReplicatedLogEntry &current_entry = log_entries_[idx];
  if (new_entry.min_proposal_ >= current_entry.min_proposal_) {
    current_entry.accepted_proposal_ = new_entry.accepted_proposal_;
    current_entry.min_proposal_ = new_entry.min_proposal_;
    current_entry.accepted_value_ = new_entry.accepted_value_;
  }
  return current_entry.min_proposal_;
}
