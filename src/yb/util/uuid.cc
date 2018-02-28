// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include <boost/uuid/random_generator.hpp>
#include "yb/util/uuid.h"
#include "yb/util/random_util.h"

namespace yb {

Uuid::Uuid() {
}

Uuid::Uuid(const Uuid& other) {
  boost_uuid_ = other.boost_uuid_;
}

Uuid::Uuid(const uuid_t copy) {
  for(int it = 0; it < 16; it ++) {
    boost_uuid_.data[it] = copy[it];
  }
}

boost::uuids::uuid Uuid::Generate() {
  boost::uuids::basic_random_generator<std::mt19937_64> generator(&ThreadLocalRandom());
  return generator();
}

CHECKED_STATUS Uuid::FromString(const std::string& strval) {
  try {
    boost_uuid_ = boost::lexical_cast<boost::uuids::uuid>(strval);
  } catch (std::exception& e) {
    return STATUS(Corruption, "Couldn't read Uuid from string!");
  }
  return Status::OK();
}

std::string Uuid::ToString() const {
  std::string strval;
  CHECK_OK(ToString(&strval));
  return strval;
}

CHECKED_STATUS Uuid::ToString(std::string *strval) const {
  *strval = boost::uuids::to_string(boost_uuid_);
  return Status::OK();
}

CHECKED_STATUS Uuid::EncodeToComparable(std::string* bytes) const {
  try {
    uint8_t output[kUuidSize];
    if (boost_uuid_.version() == boost::uuids::uuid::version_time_based) {
      // Take the MSB of the UUID and get the timestamp ordered bytes.
      toTimestampBytes(output);
    } else {
      toVersionFirstBytes(output);
    }
    memcpy(output + kUuidMsbSize, boost_uuid_.data + kUuidMsbSize, kUuidLsbSize);
    bytes->assign(reinterpret_cast<char *>(output), kUuidSize);
  } catch (std::exception& e) {
    return STATUS(Corruption, "Couldn't serialize Uuid to raw bytes!");
  }
  return Status::OK();
}

CHECKED_STATUS Uuid::ToBytes(std::string* bytes) const {
  try {
    (*bytes).assign(boost_uuid_.begin(), boost_uuid_.end());
  } catch (std::exception& e) {
    return STATUS(Corruption, "Couldn't serialize Uuid to raw bytes!");
  }
  return Status::OK();
}

CHECKED_STATUS Uuid::FromSlice(const Slice& slice, size_t size_hint) {
  size_t expected_size = (size_hint == 0) ? slice.size() : size_hint;
  if (expected_size > slice.size()) {
    return STATUS_SUBSTITUTE(InvalidArgument, "Size of slice: $0 is smaller than provided "
        "size_hint: $1", slice.size(), expected_size);
  }
  if (expected_size != kUuidSize) {
    return STATUS_SUBSTITUTE(InvalidArgument, "Size of slice is invalid: $0", expected_size);
  }
  memcpy(boost_uuid_.begin(), slice.data(), kUuidSize);
  return Status::OK();
}

CHECKED_STATUS Uuid::FromBytes(const std::string& bytes) {
  Slice slice (bytes.data(), bytes.size());
  return FromSlice(slice);
}

CHECKED_STATUS Uuid::FromHexString(const std::string& hex_string) {
  if (hex_string.size() != kUuidSize * 2) {
    return STATUS_SUBSTITUTE(InvalidArgument, "Size of hex_string is invalid: $0, expected: $1",
                             hex_string.size(), kUuidSize * 2);
  }
  std::string bytes;
  for (int i = 0; i < hex_string.size(); i+=2) {
    string byte = hex_string.substr(i, 2);
    int64_t byte_val = -1;
    try {
      byte_val = std::stol(byte.c_str(), NULL, 16);
      // Verify the value fits within a byte.
      if (byte_val > std::numeric_limits<uint8_t>::max() || byte_val < 0) {
        return STATUS_SUBSTITUTE(InvalidArgument, "$0 is not a valid uuid", hex_string);
      }
    } catch (std::invalid_argument& ia) {
      return STATUS_SUBSTITUTE(InvalidArgument, "$0 is not a valid uuid", hex_string);
    }
    bytes.insert(bytes.end(), static_cast<char>(byte_val));
  }
  // We have been provided a string in host byte order and we need to convert to network byte
  // order for FromBytes().
  std::reverse(bytes.begin(), bytes.end());
  DCHECK_EQ(kUuidSize, bytes.size());
  return FromBytes(bytes);
}

CHECKED_STATUS Uuid::DecodeFromComparableSlice(const Slice& slice, size_t size_hint) {
  size_t expected_size = (size_hint == 0) ? slice.size() : size_hint;
  if (expected_size > slice.size()) {
    return STATUS_SUBSTITUTE(InvalidArgument, "Size of slice: $0 is smaller than provided "
        "size_hint: $1", slice.size(), expected_size);
  }
  if (expected_size != kUuidSize) {
    return STATUS_SUBSTITUTE(InvalidArgument,
                             "Decode error: Size of slice is invalid: $0", expected_size);
  }
  const uint8_t* bytes = slice.data();
  if ((bytes[0] & 0xF0) == 0x10) {
    // Check the first byte to see if it is version 1.
    fromTimestampBytes(bytes);
  } else {
    fromVersionFirstBytes(bytes);
  }
  memcpy(boost_uuid_.data + kUuidMsbSize, bytes + kUuidMsbSize, kUuidLsbSize);
  return Status::OK();
}

CHECKED_STATUS Uuid::DecodeFromComparable(const std::string& bytes) {
  Slice slice (bytes.data(), bytes.size());
  return DecodeFromComparableSlice(slice);
}

CHECKED_STATUS Uuid::HashMACAddress() const {
  RETURN_NOT_OK(IsTimeUuid());
  boost::uuids::detail::sha1 sha1;
  unsigned int hash[kShaDigestSize];
  sha1.process_bytes(boost_uuid_.begin() + kTimeUUIDMacOffset, kTimeUUIDTotalMacBytes);
  uint8_t tmp[kTimeUUIDTotalMacBytes];
  sha1.get_digest(hash);
  for (int i = 0; i < kTimeUUIDTotalMacBytes; i ++) {
    tmp[i] = (hash[i % kShaDigestSize] & 255);
    hash[i % kShaDigestSize] = hash[i % kShaDigestSize] >> 8;
  }
  memcpy((void *) (boost_uuid_.begin() + kTimeUUIDMacOffset), tmp, kTimeUUIDTotalMacBytes);
  return Status::OK();
}

CHECKED_STATUS Uuid::toUnixTimestamp(int64_t* timestamp_ms) {
  RETURN_NOT_OK(IsTimeUuid());
  uint8_t output[kUuidMsbSize];
  toTimestampBytes(output);
  output[0] = (output[0] & 0x0f);
  *timestamp_ms = 0;
  for (int i = 0; i < kUuidMsbSize; i++) {
    *timestamp_ms = (*timestamp_ms << 8) | (output[i] & 0xff);
  }
  // Convert from nano seconds since Gregorian calendar start to millis since unix epoch.
  *timestamp_ms = (*timestamp_ms / kMillisPerHundredNanos) + kGregorianOffsetMillis;
  return Status::OK();
}

} // namespace yb
