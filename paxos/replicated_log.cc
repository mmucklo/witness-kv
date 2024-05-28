#include "replicated_log.hh"

#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "log/logs_loader.h"

ABSL_FLAG(std::string, paxos_log_directory, "/tmp", "Paxos Log directory");

ABSL_FLAG(std::string, paxos_log_file_prefix, "replicated_log",
          "Paxos log file prefix");

namespace witnesskvs::paxoslibrary {

ReplicatedLog::ReplicatedLog(uint8_t node_id)
    : node_id_{node_id}, first_unchosen_index_{0}, proposal_number_{0} {
  CHECK_LT(node_id, max_node_id_) << "Node initialization has gone wrong.";

  bool found_unchosen_idx = false;

  const std::string prefix =
      absl::GetFlag(FLAGS_paxos_log_file_prefix) + std::to_string(node_id);

  witnesskvs::log::LogsLoader log_loader{
      absl::GetFlag(FLAGS_paxos_log_directory), prefix};
  for (auto &log_msg : log_loader) {
    ReplicatedLogEntry &entry = log_entries_[log_msg.paxos().idx()];
    entry.idx_ = log_msg.paxos().idx();
    entry.min_proposal_ = log_msg.paxos().min_proposal();
    entry.accepted_proposal_ = log_msg.paxos().accepted_proposal();
    entry.accepted_value_ = log_msg.paxos().accepted_value();
    entry.is_chosen_ = log_msg.paxos().is_chosen();

    if (!found_unchosen_idx) {
      if (entry.is_chosen_) {
        first_unchosen_index_ = log_msg.paxos().idx() + 1;
      } else {
        first_unchosen_index_ = log_msg.paxos().idx();
        found_unchosen_idx = true;
      }
    }

    proposal_number_ =
        std::max(proposal_number_, log_msg.paxos().min_proposal());
  }

  VLOG(1) << "NODE: [" << static_cast<uint32_t>(node_id_)
          << "] Constructed Replicated log with first unchosen index : "
          << first_unchosen_index_ << " and proposal number "
          << proposal_number_;

  log_writer_ = std::make_unique<witnesskvs::log::LogWriter>(
      absl::GetFlag(FLAGS_paxos_log_directory), prefix);
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
  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] Generated proposal number: " << proposal_number_;
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
  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] updated First unchosen index: " << first_unchosen_index_;
}

void ReplicatedLog::MakeLogEntryStable(const ReplicatedLogEntry &entry) {
  Log::Message log_message;
  log_message.mutable_paxos()->set_idx(entry.idx_);
  log_message.mutable_paxos()->set_min_proposal(entry.min_proposal_);
  log_message.mutable_paxos()->set_accepted_proposal(entry.accepted_proposal_);
  log_message.mutable_paxos()->set_accepted_value(entry.accepted_value_);
  log_message.mutable_paxos()->set_is_chosen(entry.is_chosen_);

  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] stable entry at idx: " << entry.idx_
            << " with value: " << entry.accepted_value_
            << " with chosenness: " << entry.is_chosen_;
  absl::Status status = log_writer_->Log(log_message);
  CHECK_EQ(status, absl::OkStatus());
}

void ReplicatedLog::MarkLogEntryChosen(uint64_t idx) {
  absl::MutexLock l(&log_mutex_);
  ReplicatedLogEntry &entry = log_entries_[idx];
  entry.is_chosen_ = true;

  MakeLogEntryStable(entry);
  UpdateFirstUnchosenIdx();
}

void ReplicatedLog::SetLogEntryAtIdx(uint64_t idx, std::string value) {
  absl::MutexLock l(&log_mutex_);
  ReplicatedLogEntry &entry = log_entries_[idx];
  if (entry.accepted_value_ != value) {
    // This is fine, as it is possible we may be the only node that accepted a
    // value but that value never got quorum, some other value won and now we
    // are learning about it.
    LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
              << "] Choosing a different value (" << value
              << ") than what was previously accepted ("
              << entry.accepted_value_ << ")";
  }

  entry.idx_ = idx;
  entry.accepted_value_ = value;
  entry.is_chosen_ = true;

  MakeLogEntryStable(entry);
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

  it->second.idx_ = idx;
  it->second.min_proposal_ = new_min_proposal;
  MakeLogEntryStable(it->second);
}

ReplicatedLogEntry ReplicatedLog::GetLogEntryAtIdx(uint64_t idx) {
  absl::MutexLock l(&log_mutex_);
  auto it = log_entries_.find(idx);
  CHECK(it != log_entries_.end());
  return it->second;
}

uint64_t ReplicatedLog::UpdateLogEntry(const ReplicatedLogEntry &new_entry) {
  absl::MutexLock l(&log_mutex_);
  ReplicatedLogEntry &current_entry = log_entries_[new_entry.idx_];
  if (new_entry.min_proposal_ >= current_entry.min_proposal_) {
    current_entry.min_proposal_ = new_entry.min_proposal_;
    current_entry.accepted_proposal_ = new_entry.accepted_proposal_;
    current_entry.accepted_value_ = new_entry.accepted_value_;

    if (!current_entry.is_chosen_) {
      current_entry.is_chosen_ = new_entry.is_chosen_;
    }
    MakeLogEntryStable(current_entry);
  }
  return current_entry.min_proposal_;
}

}  // namespace witnesskvs::paxoslibrary