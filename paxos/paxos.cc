#include "paxos.hh"

#include <fstream>
#include <sstream>

void
Paxos::parseNodesConfig(const std::string& configFileName)
{
    std::vector<Node> nodes{};
    std::ifstream configFile(configFileName);

    if (!configFile.is_open()) {
        throw std::runtime_error("Failed to open nodes configuration file");
    }

    std::string line;
    while (std::getline(configFile, line)) {
        std::stringstream ss(line);
        std::string ipAddress, portStr;
        int port;
        if (std::getline(ss, ipAddress, ':') && std::getline(ss, portStr)) {
            try {
                port = std::stoi(portStr);
            } catch (const std::invalid_argument& e) {
                throw std::runtime_error("Invalid port number in config file");
            }
            nodes.push_back({ipAddress, port});
        }
    }

    configFile.close();
}

Paxos::Paxos(const std::string& configFileName)
{
    parseNodesConfig(configFileName);
}