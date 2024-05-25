#ifndef LOG_CRC32_H
#define LOG_CRC32_H

#include <cstdint>

#include <sys/types.h>
namespace witnesskv::log {

// Returns a crc32 of the buffer passed in.
uint32_t crc32(const char *buf, size_t bufLen);

} // namespace witnesskvs::log
#endif
