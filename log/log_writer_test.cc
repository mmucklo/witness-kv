#include "log_writer.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/log.h"
#include "absl/random/random.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_join.h"
#include "absl/time/time.h"
#include "log.pb.h"

TEST(LogWriterTest, Basic) {
  LogWriter log_writer("/tmp", "log_writer_test");
  Log::Message log_message;
  log_message.mutable_paxos()->set_round(4);
  log_message.mutable_paxos()->set_proposal_id(9);
  log_message.mutable_paxos()->set_value("test1234");
  log_writer.Log(log_message);
}
