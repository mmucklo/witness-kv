#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/log/check.h"
#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "util/node.h"

int PutHelper(const std::vector<std::unique_ptr<Node>>& nodes,
              const std::string& key, const std::string& value);

std::string GetHelper(const std::vector<std::unique_ptr<Node>>& nodes,
                      const std::string& key, int* return_code);

int DeleteHelper(const std::vector<std::unique_ptr<Node>>& nodes,
                 const std::string& key);

int LinearizabilityCheckerInitHelper(
    const std::vector<std::unique_ptr<Node>>& nodes);

int LinearizabilityCheckerDeinitHelper(
    const std::vector<std::unique_ptr<Node>>& nodes);