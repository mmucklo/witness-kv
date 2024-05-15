#include "log_reader.h"

#include "log.pb.h"
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/initialize.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

ABSL_FLAG(bool, tail, false, "Sits and tails on the end of the log until killed.");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();
  if (argc == 1) {
    absl::PrintF("usage: %s <filename>\n", argv[0]);
    return -1;
  }
  std::string filename(argv[1]);
  witnesskvs::log::LogReader reader(filename);
  for (const auto& msg : reader) {
    absl::PrintF("%s", msg.DebugString());
  }
  if (!absl::GetFlag(FLAGS_tail)) {
    return 0;
  }

  // Tail the log file.
  while (true) {
    absl::StatusOr<Log::Message> msg_or = reader.next();
    if (msg_or.ok()) {
      absl::PrintF("%s", msg_or->DebugString());
      continue;
    }
    absl::SleepFor(absl::Seconds(1));
  }
  return 0;
}