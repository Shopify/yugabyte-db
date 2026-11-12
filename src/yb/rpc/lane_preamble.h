// Copyright (c) YugabyteDB, Inc.
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

#pragma once

#include <cstddef>

#include "yb/util/slice.h"

namespace yb::rpc {

// A lane preamble is "YL" followed by one lane byte. A laned tablet server writes it before any
// other byte of a lane-keyed connection, ahead of the TLS ClientHello, the compression header and
// the YB connection header, so that a laned receiver can pick a reactor of that lane at accept,
// before the TLS handshake. It is plaintext because nothing else is readable by then; a wrong lane
// costs locality only. No YB RPC stream starts with "YL" otherwise: TLS starts with 0x16, a
// compressed stream with "YB" and an algorithm id, a plain one with "YB\1".
inline constexpr char kLanePreambleMagic[] = {'Y', 'L'};
inline constexpr size_t kLanePreambleSize = sizeof(kLanePreambleMagic) + 1;

enum class LanePreambleMatch {
  // The bytes do not start a preamble; nothing should be consumed.
  kNone,
  // The bytes, fewer than kLanePreambleSize, are a prefix of a preamble.
  kPrefix,
  // The first kLanePreambleSize bytes are a preamble, whose lane byte is the last of them.
  kPreamble,
};

// Matches the first bytes received on a connection, deciding as early as the bytes allow: a first
// byte other than 'Y' decides at once, "YB" at two bytes.
inline LanePreambleMatch MatchLanePreamble(Slice first_bytes) {
  for (size_t i = 0; i != sizeof(kLanePreambleMagic); ++i) {
    if (i == first_bytes.size()) {
      return LanePreambleMatch::kPrefix;
    }
    if (first_bytes[i] != static_cast<uint8_t>(kLanePreambleMagic[i])) {
      return LanePreambleMatch::kNone;
    }
  }
  return first_bytes.size() < kLanePreambleSize ? LanePreambleMatch::kPrefix
                                                : LanePreambleMatch::kPreamble;
}

} // namespace yb::rpc
