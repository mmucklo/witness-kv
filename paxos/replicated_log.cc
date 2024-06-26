#include "replicated_log.h"

#include <google/protobuf/util/message_differencer.h>

#include "log/logs_loader.h"

ABSL_FLAG(std::string, paxos_log_directory, "/var/tmp", "Paxos Log directory");

ABSL_FLAG(std::string, paxos_log_file_prefix, "replicated_log",
          "Paxos log file prefix");

namespace witnesskvs::paxos {

std::function<bool(const Log::Message &a, const Log::Message &b)>
GetLogSortFn() {
  static auto fn = [](const Log::Message &a, const Log::Message &b) {
    if (a.paxos().idx() < b.paxos().idx()) {
      return true;
    } else if (a.paxos().idx() == b.paxos().idx()) {
      if (a.paxos().is_chosen() == b.paxos().is_chosen()) {
        return false;  // for strict weak ordering criteria.
      }
      if (!a.paxos().is_chosen()) {
        return true;
      }
      return false;
    }
    return false;
  };
  return fn;
}

ReplicatedLog::ReplicatedLog(uint8_t node_id)
    : node_id_{node_id}, first_unchosen_index_{0}, proposal_number_{0} {
  CHECK_LT(node_id, max_node_id_) << "Node initialization has gone wrong.";

  const std::string prefix =
      absl::GetFlag(FLAGS_paxos_log_file_prefix) + std::to_string(node_id);

  witnesskvs::log::SortingLogsLoader log_loader{
      absl::GetFlag(FLAGS_paxos_log_directory), prefix, GetLogSortFn()};
  for (auto &log_msg : log_loader) {
    ReplicatedLogEntry &entry = log_entries_[log_msg.paxos().idx()];
    entry.idx_ = log_msg.paxos().idx();
    entry.min_proposal_ = log_msg.paxos().min_proposal();
    entry.accepted_proposal_ = log_msg.paxos().accepted_proposal();
    entry.accepted_value_ = log_msg.paxos().accepted_value();
    entry.is_chosen_ = log_msg.paxos().is_chosen();

    proposal_number_ =
        std::max(proposal_number_, log_msg.paxos().min_proposal());
  }

  for (const auto &[key, value] : log_entries_) {
    if (value.is_chosen_) {
      first_unchosen_index_ = value.idx_ + 1;
    } else {
      first_unchosen_index_ = value.idx_;
      break;
    }
  }

  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] Constructed Replicated log with first unchosen index : "
            << first_unchosen_index_ << " and proposal number "
            << proposal_number_;

  logs_truncator_ = std::make_unique<log::LogsTruncator>(
      absl::GetFlag(FLAGS_paxos_log_directory), prefix,
      [](const Log::Message &msg) { return msg.paxos().idx(); });
  log_writer_ = std::make_unique<witnesskvs::log::LogWriter>(
      absl::GetFlag(FLAGS_paxos_log_directory), prefix,
      [](const Log::Message &msg) { return msg.paxos().idx(); });
  log_writer_->RegisterRotateCallback(logs_truncator_->GetCallbackFn());
}

ReplicatedLog::~ReplicatedLog() {}

uint64_t ReplicatedLog::GetFirstUnchosenIdx() {
  absl::MutexLock l(&lock_);
  return first_unchosen_index_;
}

uint64_t ReplicatedLog::GetNextProposalNumber() {
  absl::MutexLock l(&lock_);
  proposal_number_ =
      ((proposal_number_ & mask_) + (1ull << num_bits_for_node_id_)) |
      (uint64_t)node_id_;
  LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
            << "] Generated proposal number: " << proposal_number_;
  return proposal_number_;
}

void ReplicatedLog::UpdateProposalNumber(uint64_t prop_num) {
  absl::MutexLock l(&lock_);
  if (prop_num > proposal_number_) {
    proposal_number_ = prop_num;
  }
}

void ReplicatedLog::UpdateFirstUnchosenIdx() {
  lock_.AssertHeld();
  for (uint64_t i = first_unchosen_index_; i <= log_entries_.rbegin()->first;
       i++) {
    auto it = log_entries_.find(i);
    if (it == log_entries_.end()) {
      break;
    }
    if (!it->second.is_chosen_) {
      break;
    }

    // This entry is chosen and it is now safe to be applied to application
    // state. The update to the application state happens as the
    // `first_unchosen_index_` is incremented. This ensures that even if the
    // paxos log has holes, the callback is invoked in log order instead of
    // commit order.
    if (this->app_callback_) {
      LOG(INFO) << "NODE: [" << static_cast<uint32_t>(node_id_)
                << "] Calling App registered callback";
      this->app_callback_(it->second.accepted_value_);
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
  absl::MutexLock l(&lock_);
  ReplicatedLogEntry &entry = log_entries_[idx];
  entry.idx_ = idx;
  entry.is_chosen_ = true;
  CHECK_EQ(entry.idx_, idx);

  MakeLogEntryStable(entry);

  UpdateFirstUnchosenIdx();
}

void ReplicatedLog::SetLogEntryAtIdx(uint64_t idx, std::string value) {
  absl::MutexLock l(&lock_);
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
  absl::MutexLock l(&lock_);
  ReplicatedLogEntry &entry = log_entries_[idx];
  entry.idx_ = idx;
  return entry.min_proposal_;
}

void ReplicatedLog::UpdateMinProposalForIdx(uint64_t idx,
                                            uint64_t new_min_proposal) {
  absl::MutexLock l(&lock_);
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
  absl::MutexLock l(&lock_);
  auto it = log_entries_.find(idx);
  it->second.idx_ = idx;
  return it->second;
}

uint64_t ReplicatedLog::UpdateLogEntry(const ReplicatedLogEntry &new_entry) {
  absl::MutexLock l(&lock_);
  ReplicatedLogEntry &current_entry = log_entries_[new_entry.idx_];
  if (new_entry.min_proposal_ >= current_entry.min_proposal_) {
    current_entry.idx_ = new_entry.idx_;
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

void ReplicatedLog::Truncate(uint64_t index) {
  // Note maybe we should put this under a lock and make sure
  // we're not shutting down / going through destruction.
  CHECK(logs_truncator_ != nullptr);
  log_writer_->MaybeForceRotate();
  logs_truncator_->Truncate(index);
  absl::MutexLock l(&lock_);
  std::vector<Index> erase;
  for (const auto &[cur_index, _] : log_entries_) {
    if (cur_index < index) {
      erase.push_back(cur_index);
    }
  }
  for (const Index index : erase) {
    log_entries_.erase(index);
  }
}

}  // namespace witnesskvs::paxos
