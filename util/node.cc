#include "node.h"

#include <fstream>
#include <memory>
#include <vector>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"

bool IsValidNodeId(uint8_t node_id) { return (node_id != INVALID_NODE_ID); }

std::vector<std::unique_ptr<Node>> ParseNodesList(
    const std::vector<std::string>& node_list) {
  std::vector<std::unique_ptr<Node>> nodes;
  for (const auto& node_str : node_list) {
    std::string ip_address, port_str;
    int port;

    std::vector<absl::string_view> parts = absl::StrSplit(node_str, ":");
    if (!absl::SimpleAtoi(parts[parts.size() - 1], &port)) {
      LOG(FATAL) << "Could not convert port to int full str: " << node_str
                 << " port: " << parts[parts.size() - 1];
    }
    parts.pop_back();
    nodes.push_back(std::make_unique<Node>(absl::StrJoin(parts, ":"), port));
  }
  return nodes;
}

std::vector<std::unique_ptr<Node>> ParseNodesConfig(
    std::string config_file_name) {
  std::vector<std::unique_ptr<Node>> nodes{};
  std::ifstream config_file(config_file_name);

  CHECK(config_file.is_open()) << "Failed to open nodes configuration file";

  std::string line;
  while (std::getline(config_file, line)) {
    if (line.empty()) {
      continue;
    }
    std::string ip_address, port_str;
    int port;
    std::vector<absl::string_view> parts = absl::StrSplit(line, ":");
    if (!absl::SimpleAtoi(parts[parts.size() - 1], &port)) {
      LOG(FATAL) << "Could not convert port to int full str: " << line
                 << " port: " << parts[parts.size() - 1];
    }
    parts.pop_back();
    nodes.push_back(std::make_unique<Node>(absl::StrJoin(parts, ":"), port));
  }

  config_file.close();

  return nodes;
}
