#include "file_writer.h"

#include <filesystem>
#include <iostream>

#include <gtest/gtest.h>
#include "absl/strings/cord.h"
#include "absl/strings/str_join.h"
#include "absl/time/time.h"

class FileWriterTest
    : public ::testing::Test
{
    virtual void SetUp() override {
        std::cout << "test SetUp called" << std::endl;
    }

    virtual void TearDown() override {
        std::cout << "test TearDown called" << std::endl;
    }
};

std::string getTempFilename() {
    absl::Time now = absl::Now();
    std::string filename = "/tmp/file_writer_test.";
    filename.append(absl::StrCat(absl::ToUnixMicros(now)));
    return filename;
}

TEST(FileWriterTest, Smoke) {
    absl::Time now = absl::Now();
    std::string filename = getTempFilename();
    filename.append(absl::StrCat(absl::ToUnixMicros(now)));
    FileWriter file_writer(filename);
    absl::Cord cord;
    cord.Append("Some message!");
    cord.Append("1234SecondMessage in cord.");
    file_writer.Write(cord);
    file_writer.Flush();
    ASSERT_TRUE(std::filesystem::remove(std::filesystem::path(filename)));    
}

// TODO write large cord test
// TODO write large multi-cord test
// TODO verify contents of file afterwards.

// TODO microbenchmark to watch flush cycles and timing.