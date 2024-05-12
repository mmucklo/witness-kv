#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iostream>
#include <sstream>
#include <string>
#include "paxos.hh"

// Mock class for Paxos
class MockPaxos : public Paxos {
public:
    MockPaxos(const std::string& config_file, uint8_t node_id)
            : Paxos(config_file, node_id) {}

    MOCK_METHOD(void, Replicate, (const std::string&, uint64_t));
};

// Test fixture for ClientTest
class ClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a mock Paxos object
        paxos = std::make_unique<MockPaxos>("paxos/nodes_config.txt", 1);
    }

    void TearDown() override {
        // Clean up
        paxos.reset();
    }

    std::unique_ptr<MockPaxos> paxos;
};

// Test case to test the main function
TEST_F(ClientTest, MainFunctionTest) {
    // Set up input stream for user input
    std::istringstream input_stream("Hello\n1\nquit\n");
    std::streambuf* old_cin = std::cin.rdbuf(input_stream.rdbuf());

    // Set up output stream for capturing cout
    #include <iostream> // Include the header file for 'main' function

    std::stringstream output_stream;
    std::streambuf* old_cout = std::cout.rdbuf(output_stream.rdbuf());

    // Call the main function
    int argc = 2;
    char* argv[] = {const_cast<char*>("test_client"), const_cast<char*>("1")};
    int main(int argc, char** argv); // Declare the 'main' function
    main(argc, argv);

    // Restore cin and cout
    std::cin.rdbuf(old_cin);
    std::cout.rdbuf(old_cout);

    // Verify the expected output
    std::string expected_output = "Enter something to replicate (or 'quit' to exit): \n"
                                                    "Enter index to replicate on (or 'quit' to exit): \n";
    EXPECT_EQ(output_stream.str(), expected_output);
}

// Test case to test the Replicate function
TEST_F(ClientTest, ReplicateTest) {
    // Set up input stream for user input
    std::istringstream input_stream("Hello\n1\nquit\n");
    std::streambuf* old_cin = std::cin.rdbuf(input_stream.rdbuf());

    // Call the Replicate function
    EXPECT_CALL(*paxos, Replicate("Hello", 1));
    paxos->Replicate("Hello", 1);

    // Restore cin
    std::cin.rdbuf(old_cin);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}