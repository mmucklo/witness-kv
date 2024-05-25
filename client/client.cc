#include <cstdlib>
#include <iostream>
#include <memory>

#include "paxos.hh"

int main(int argc, char* argv[]) {
  using namespace std::chrono_literals;

  if (argc != 2) {
    std::cout << "Invalid argc\n";
    exit(EXIT_FAILURE);
  }

  Paxos p{static_cast<uint8_t>(std::atoi(argv[1]))};

  std::string input;
  while (true) {
    std::cout << "Enter something to replicate (or 'quit' to exit): \n";
    std::getline(std::cin, input);

    if (input == "quit") {
      break;
    }

    p.Propose(input);
  }

  return 0;
}