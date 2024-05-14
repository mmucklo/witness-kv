#ifndef LOG_BYTE_CONVERSION_H
#define LOG_BYTE_CONVERSION_H

#include <bit>
#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>

namespace witnesskvs::log {

using enable_if_not_mixed =
    std::enable_if_t<std::endian::native == std::endian::little ||
                         std::endian::native == std::endian::big,
                     bool>;

template <typename T, std::endian to_endianness = std::endian::big,
          std::enable_if_t<std::is_fundamental_v<T>, bool> = false,
          enable_if_not_mixed = false>
std::vector<uint8_t> toBytes(T value) {
  std::vector<uint8_t> buffer(sizeof(T));

  std::copy_n(reinterpret_cast<uint8_t*>(&value), sizeof(T), buffer.begin());

  if constexpr (std::endian::native != to_endianness)
    std::reverse(buffer.begin(), buffer.end());

  return buffer;
}

template <typename T, std::endian from_endianness = std::endian::big,
          std::enable_if_t<std::is_fundamental_v<T>, bool> = false,
          enable_if_not_mixed = false>
T fromBytes(std::vector<uint8_t> bytes) {
  if constexpr (std::endian::native != from_endianness)
    std::reverse(bytes.begin(), bytes.end());

  T* buffer = reinterpret_cast<T*>(bytes.data());

  return *buffer;
}

/*
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 * For more information, please refer to <http://unlicense.org/>
 */

// Source:
// https://gist.github.com/PanForPancakes/3989f91371661dc76d4e87263d6da3e8


// New Code goes here:

// Helper function to convert funamental type bytes into a little-endian ordered
// string
template <typename T, std::enable_if_t<std::is_fundamental_v<T>, bool> = false>
std::string byte_str(T val) {
  std::vector<uint8_t> val_bytes = toBytes<T, std::endian::little>(val);
  std::basic_string<unsigned char> bytes_str(val_bytes.begin(),
                                             val_bytes.end());
  return std::string(bytes_str.begin(), bytes_str.end());
}

}  // namespace witnesskvs::log
#endif
