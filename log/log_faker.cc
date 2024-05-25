#include "log_writer.h"

// Creates fake log entries.
#include <string>

#include "log.pb.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/check.h"
#include "absl/log/initialize.h"
#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

ABSL_FLAG(uint16_t, num, 1, "Number of entries to create.");
ABSL_FLAG(absl::Duration, pause, absl::Seconds(1),
          "Duration to pause between entries.");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();
  if (argc <= 2) {
    absl::PrintF("usage: %s <dir> <prefix>\n", argv[0]);
    return -1;
  }
  std::string dir(argv[1]);
  std::string prefix(argv[2]);
  witnesskvs::log::LogWriter writer(dir, prefix);
  std::string filename;
  for (uint16_t i = 0; i < absl::GetFlag(FLAGS_num); i++) {
    Log::Message log_message;
    log_message.mutable_paxos()->set_round(i);
    log_message.mutable_paxos()->set_proposal_id(i * i / 2);
    log_message.mutable_paxos()->set_value(
        absl::StrFormat("test value %d", i + 1));
    absl::Status status = writer.Log(log_message);
    CHECK_OK(status) << status.message();
    absl::PrintF("Msg %d written.\n", i + 1);
    if (writer.filename() != filename) {
      filename = writer.filename();
      absl::PrintF("%s\n", filename);
    }
    if (i + 1 < absl::GetFlag(FLAGS_num)) {
      absl::SleepFor(absl::GetFlag(FLAGS_pause));
    }
  }
  return 0;
}