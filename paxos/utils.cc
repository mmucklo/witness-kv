#include "utils.hh"

bool IsValidNodeId(uint8_t node_id) { return (node_id != INVALID_NODE_ID); }

std::vector<Node> ParseNodesConfig(std::string config_file_name) {
  std::vector<Node> nodes{};
  std::ifstream config_file(config_file_name);

  CHECK(config_file.is_open()) << "Failed to open nodes configuration file";

  std::string line;
  while (std::getline(config_file, line)) {
    std::stringstream ss(line);
    std::string ip_address, port_str;
    int port;
    if (std::getline(ss, ip_address, ':') && std::getline(ss, port_str)) {
      try {
        port = std::stoi(port_str);
      } catch (const std::invalid_argument& e) {
        throw std::runtime_error("Invalid port number in config file");
      }
      nodes.push_back({ip_address, port});
    }
  }

  config_file.close();

  return nodes;
}