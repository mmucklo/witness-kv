#include "file_writer.h"

#include <filesystem>
#include <iterator>
#include <iostream>
#include <fstream>

#include <gtest/gtest.h>
#include "absl/log/log.h"
#include "absl/random/random.h"
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
    std::string filename = getTempFilename();
    FileWriter file_writer(filename);
    absl::Cord cord;
    cord.Append("Some message!");
    cord.Append("1234SecondMessage in cord.");
    file_writer.Write(cord);
    file_writer.Flush();
    ASSERT_TRUE(std::filesystem::remove(std::filesystem::path(filename)));    
}

TEST(FileWriterTest, Large) {
    std::string filename = getTempFilename();
    absl::Cord cord;
    {
        FileWriter file_writer(filename);
        absl::BitGen gen;
        for (int i = 0 ; i < 128 ; i++) {
            std::string tempstr;
            uint32_t len = absl::Uniform(absl::IntervalClosed, gen, 1, 4096);
            for (uint32_t j = 0; j < len ; j++) {
                uint32_t c = absl::Uniform(absl::IntervalClosed, gen, 32, 126);
                tempstr.append(1, (char) c);
            }
            cord.Append(tempstr);
        }
        cord.Append("Some message!");
        cord.Append("1234SecondMessage in cord.asdf");
        file_writer.Write(cord);
        file_writer.Flush();
        std::string str;
        {
            std::ifstream fs(filename.c_str());
            ASSERT_TRUE(fs.good());
            str = std::string(std::istreambuf_iterator<char>{fs}, {});
        }
        EXPECT_EQ(cord, str);
        EXPECT_EQ(cord.size(), file_writer.bytes_written());
    }
    ASSERT_TRUE(std::filesystem::remove(std::filesystem::path(filename)));    
}

// TODO write large cord test
// TODO write large multi-cord test
// TODO verify contents of file afterwards.

// TODO microbenchmark to watch flush cycles and timing.