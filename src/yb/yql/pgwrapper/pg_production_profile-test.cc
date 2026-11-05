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

// Runs the production gflags profile, checked in as production_profile/yb-{master,tserver}.flags,
// through the real --flagfile path, and checks that every flag is accepted (known to the binary,
// not deprecated, passes preview gating and validators) and took effect. gflags silently ignores
// unknown names inside a flagfile, so a renamed or removed flag never stops startup; every flag has
// to be looked up by name instead.
//
// Flagfile grammar. The active lines are byte-for-byte what the deployment renders, so a fresh
// render diffs cleanly against the files. Lines the test cannot apply are kept as markers, and a
// line starting with "# PROD " that is not one of the two marker forms fails the test, so a typo
// cannot silently drop an assertion:
//   --<key>=<value>, or bare --<key> meaning true
//       Active: applied and asserted.
//   # PROD --<key>=<per-host> [DROPPED: <reason>]
//       Host-specific production line. Not applied and not asserted; the value is never recorded.
//   # PROD --<key>=<value> [SUBSTITUTED: harness]
//       Production value the harness overrides. Not applied; the live value is expected to differ.
//   Any other line starting with '#': free-form comment.
//   Blank line: ignored.
// The value is everything after the first '=', verbatim, quotes included. Any other line is an
// error: in a gflags flagfile, a non-blank line starting with neither '-' nor '#' is a
// program-name filter that silently disables every line after it. gflags has no inline trailing
// comments either; one would become part of the value.
//
// Value comparison rule, file value against a value read back from a daemon:
//   - booleans compare case-insensitively across true/false/1/0/on/off (gflags and PG spellings);
//   - integers compare exactly; other numbers compare as doubles with a tiny relative tolerance,
//     since gflags prints doubles with 17 significant digits (0.67 reads back as
//     0.67000000000000004);
//   - anything else compares byte for byte. gflags keeps the quotes of a value such as
//     'read committed', so flag readback compares quotes too; stripping PG quoting is opt-in, for
//     comparisons against PG's SHOW and pg_settings.
//
// Refresh procedure: render a production host's flagfile, diff it against the checked-in file,
// and carry the changes over. Active lines stay verbatim and in rendered order; host-specific
// lines become DROPPED markers without their values, lines the harness overrides become
// SUBSTITUTED markers, and a host-specific token inside a value is replaced and explained in a
// '# NOTE:' comment. No host name, address, path or placement value goes into the files.
//
// The flagfiles hold all the expectations. The only other data are the three lists below, each
// carrying facts a flagfile cannot: kBootstrapDeferredFlags, kKnownUnrecognizedFlags and
// kOtherDaemonOnlyFlags. When the production profile is corrected, the files change by diff and
// the lists shrink; a kKnownUnrecognizedFlags or kOtherDaemonOnlyFlags row whose key leaves the
// profile fails the test.

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <map>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "yb/gutil/strings/join.h"

#include "yb/util/env.h"
#include "yb/util/env_util.h"
#include "yb/util/enums.h"
#include "yb/util/faststring.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/path_util.h"
#include "yb/util/result.h"
#include "yb/util/status.h"
#include "yb/util/strongly_typed_bool.h"
#include "yb/util/subprocess.h"
#include "yb/util/test_macros.h"

#include "yb/yql/pgwrapper/libpq_test_base.h"

DECLARE_string(dynamically_linked_exe_suffix);

namespace yb::pgwrapper {

namespace {

constexpr std::string_view kProfileDir = "src/yb/yql/pgwrapper/production_profile";
const std::string kMasterDaemon = "yb-master";
const std::string kTServerDaemon = "yb-tserver";

// master_join_existing_universe=true makes a fresh master refuse to create a universe, so a
// cluster can only boot without it. It takes effect on a master restart against the existing
// universe, which is how the flag behaves on every version, not a version-specific exception.
const std::vector<std::string> kBootstrapDeferredFlags = {"master_join_existing_universe"};

// Tserver keys that appear in the profile but are not gflags, so production drops them silently.
// Each row records the PG GUC the line was meant for and the value that GUC has in production.
struct KnownUnrecognizedFlag {
  std::string key;
  std::string show_guc;
  std::string expected_show;
  std::string reason;
};

const std::vector<KnownUnrecognizedFlag> kKnownUnrecognizedFlags = {
  {
    "yb_enable_parallel_append", "yb_enable_parallel_append", "on",
    "Not a gflag (GUC only), so production drops it silently. With CBO=on the GUC ends up on "
    "(CBO dynamic default at 2025.2.1; on master, an old-name alias of enable_parallel_append, "
    "default on). Shows current production behavior; flip when the profile is corrected."
  },
  {
    "yb_parallel_range_rows", "yb_parallel_range_rows", "10000",
    "The gflag is ysql_yb_parallel_range_rows (a preview flag), so this line is dropped. The "
    "CBO=on dynamic default applies. Shows current production behavior; flip when the profile "
    "is corrected."
  },
  {
    "yb_walsender_poll_sleep_duration_empty_ms", "yb_walsender_poll_sleep_duration_empty_ms",
    "10ms",
    "The gflag is ysql_yb_walsender_poll_sleep_duration_empty_ms, so this line is dropped and "
    "the built-in default applies. Shows current production behavior; flip when the profile is "
    "corrected."
  },
};

// Keys in one daemon's profile that are defined only in the other daemon's code. The dynamically
// linked binary of the profile's daemon does not link that code and drops them silently; a merged
// LTO yb-server binary accepts them, still with no effect.
struct OtherDaemonOnlyFlag {
  std::string key;
  std::string defined_in_daemon;
  std::string reason;
};

const std::vector<OtherDaemonOnlyFlag> kOtherDaemonOnlyFlags = {
  {
    "enable_heartbeat_pg_catalog_versions_cache", kMasterDaemon,
    "Defined only in master code (catalog manager), so the tserver has no use for it. Shows "
    "current production behavior; remove when the profile drops it."
  },
  {
    "enable_truncate_cdcsdk_table", kMasterDaemon,
    "Defined only in master code (xrepl catalog manager), so the tserver has no use for it. "
    "Shows current production behavior; remove when the profile drops it."
  },
};

const KnownUnrecognizedFlag* FindKnownUnrecognized(const std::string& key) {
  for (const auto& row : kKnownUnrecognizedFlags) {
    if (row.key == key) {
      return &row;
    }
  }
  return nullptr;
}

const OtherDaemonOnlyFlag* FindOtherDaemonOnly(const std::string& key) {
  for (const auto& row : kOtherDaemonOnlyFlags) {
    if (row.key == key) {
      return &row;
    }
  }
  return nullptr;
}

bool IsBootstrapDeferred(const std::string& key) {
  return std::find(kBootstrapDeferredFlags.begin(), kBootstrapDeferredFlags.end(), key) !=
         kBootstrapDeferredFlags.end();
}

YB_DEFINE_ENUM(ProfileEntryKind, (kActive)(kDropped)(kSubstituted));

struct ProfileEntry {
  ProfileEntryKind kind;
  std::string key;
  // Verbatim text after the first '='. "true" for a bare active line, "<per-host>" for a dropped
  // marker.
  std::string value;
  bool bare = false;
};

struct DaemonProfile {
  std::string daemon;
  std::string source_path;
  // File contents, verbatim. The copy writers work from these so copies keep the file's bytes.
  std::string contents;
  // In file order. Comments and blank lines are not entries.
  std::vector<ProfileEntry> entries;

  const ProfileEntry* FindActive(const std::string& key) const {
    for (const auto& entry : entries) {
      if (entry.kind == ProfileEntryKind::kActive && entry.key == key) {
        return &entry;
      }
    }
    return nullptr;
  }
};

std::vector<std::string_view> SplitLines(std::string_view contents) {
  std::vector<std::string_view> lines;
  size_t pos = 0;
  while (pos < contents.size()) {
    auto end = contents.find('\n', pos);
    if (end == std::string_view::npos) {
      end = contents.size();
    }
    lines.push_back(contents.substr(pos, end - pos));
    pos = end + 1;
  }
  return lines;
}

// Returns std::nullopt for a comment or blank line.
Result<std::optional<ProfileEntry>> ParseProfileLine(std::string_view line) {
  static const std::regex kActiveRe("--([A-Za-z0-9_]+)(=(.*))?");
  // DROPPED markers must carry the literal <per-host> placeholder so a real host value can never
  // be checked in by accident.
  static const std::regex kDroppedRe(R"(# PROD --([A-Za-z0-9_]+)=<per-host> \[DROPPED: [^\]]+\])");
  static const std::regex kSubstitutedRe(
      R"(# PROD --([A-Za-z0-9_]+)=(.+) \[SUBSTITUTED: harness\])");

  const std::string line_str(line);
  std::smatch match;
  if (line.empty()) {
    return std::nullopt;
  }
  if (line.starts_with("--")) {
    if (!std::regex_match(line_str, match, kActiveRe)) {
      return STATUS_FORMAT(InvalidArgument, "Malformed flag line: '$0'", line);
    }
    if (match[2].matched) {
      return ProfileEntry{ProfileEntryKind::kActive, match[1].str(), match[3].str(), false};
    }
    return ProfileEntry{ProfileEntryKind::kActive, match[1].str(), "true", true};
  }
  if (line.starts_with("# PROD ")) {
    if (std::regex_match(line_str, match, kDroppedRe)) {
      return ProfileEntry{ProfileEntryKind::kDropped, match[1].str(), "<per-host>", false};
    }
    if (std::regex_match(line_str, match, kSubstitutedRe)) {
      return ProfileEntry{ProfileEntryKind::kSubstituted, match[1].str(), match[2].str(), false};
    }
    return STATUS_FORMAT(
        InvalidArgument,
        "Malformed '# PROD' marker (expected '[DROPPED: <reason>]' with value <per-host>, or "
        "'[SUBSTITUTED: harness]'): '$0'", line);
  }
  if (line.starts_with("#")) {
    return std::nullopt;
  }
  return STATUS_FORMAT(
      InvalidArgument,
      "Line starts with neither '--' nor '#'; gflags would read it as a program-name filter and "
      "skip every line after it: '$0'", line);
}

Result<std::vector<ProfileEntry>> ParseProfile(std::string_view contents) {
  std::vector<ProfileEntry> entries;
  std::unordered_map<std::string, size_t> line_of_key;
  size_t line_no = 0;
  for (auto line : SplitLines(contents)) {
    ++line_no;
    auto entry = VERIFY_RESULT_PREPEND(ParseProfileLine(line), Format("Line $0", line_no));
    if (!entry) {
      continue;
    }
    // gflags lets the last duplicate win, which would make per-key assertions ambiguous.
    auto [it, inserted] = line_of_key.emplace(entry->key, line_no);
    if (!inserted) {
      return STATUS_FORMAT(
          InvalidArgument, "Line $0: key '$1' already appears on line $2", line_no, entry->key,
          it->second);
    }
    entries.push_back(std::move(*entry));
  }
  return entries;
}

// Reads production_profile/<daemon>.flags from the source tree.
Result<DaemonProfile> LoadProfile(const std::string& daemon) {
  // GetRootDirResult returns the directory that contains kProfileDir, which is the repo root. If
  // YB_HOME is set it returns that unchecked, hence the existence check below.
  const auto root = VERIFY_RESULT_PREPEND(
      env_util::GetRootDirResult(std::string(kProfileDir)),
      Format("Cannot find $0 above the test binary", kProfileDir));
  const auto dir = JoinPathSegments(root, kProfileDir);
  if (!Env::Default()->DirExists(dir)) {
    return STATUS_FORMAT(
        NotFound, "Production profile directory $0 does not exist (YB_HOME set elsewhere?)", dir);
  }
  DaemonProfile profile;
  profile.daemon = daemon;
  profile.source_path = JoinPathSegments(dir, daemon + ".flags");
  faststring contents;
  RETURN_NOT_OK(ReadFileToString(Env::Default(), profile.source_path, &contents));
  profile.contents = contents.ToString();
  profile.entries = VERIFY_RESULT_PREPEND(
      ParseProfile(profile.contents), Format("Parsing $0", profile.source_path));
  return profile;
}

// Writes the profile verbatim to dest.
Status WriteFullCopy(const DaemonProfile& profile, const std::string& dest) {
  return WriteStringToFile(Env::Default(), profile.contents, dest);
}

std::string ToLower(std::string_view s) {
  std::string result(s);
  for (auto& c : result) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return result;
}

std::optional<bool> ParseBool(std::string_view s) {
  const auto lower = ToLower(s);
  if (lower == "true" || lower == "1" || lower == "on") {
    return true;
  }
  if (lower == "false" || lower == "0" || lower == "off") {
    return false;
  }
  return std::nullopt;
}

std::optional<int64_t> ParseInteger(const std::string& s) {
  static const std::regex kIntegerRe("[-+]?[0-9]+");
  if (!std::regex_match(s, kIntegerRe)) {
    return std::nullopt;
  }
  errno = 0;
  char* end = nullptr;
  const auto value = std::strtoll(s.c_str(), &end, 10);
  if (errno != 0 || *end != '\0') {
    return std::nullopt;
  }
  return value;
}

std::optional<double> ParseDecimal(const std::string& s) {
  static const std::regex kDecimalRe(R"([-+]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][-+]?[0-9]+)?)");
  if (!std::regex_match(s, kDecimalRe)) {
    return std::nullopt;
  }
  const auto value = std::strtod(s.c_str(), nullptr);
  if (!std::isfinite(value)) {
    return std::nullopt;
  }
  return value;
}

// PG quoting: 'it''s' -> it's.
std::string StripPgQuotes(const std::string& s) {
  if (s.size() < 2 || s.front() != '\'' || s.back() != '\'') {
    return s;
  }
  std::string result;
  const auto inner = std::string_view(s).substr(1, s.size() - 2);
  for (size_t i = 0; i < inner.size(); ++i) {
    result.push_back(inner[i]);
    if (inner[i] == '\'' && i + 1 < inner.size() && inner[i + 1] == '\'') {
      ++i;
    }
  }
  return result;
}

YB_STRONGLY_TYPED_BOOL(StripQuotes);

// The value comparison rule from the top-of-file comment.
bool ProfileValuesMatch(
    const std::string& file_value, const std::string& live_value,
    StripQuotes strip_quotes = StripQuotes::kFalse) {
  const auto expected = strip_quotes ? StripPgQuotes(file_value) : file_value;
  const auto actual = strip_quotes ? StripPgQuotes(live_value) : live_value;
  const auto expected_bool = ParseBool(expected);
  const auto actual_bool = ParseBool(actual);
  if (expected_bool && actual_bool) {
    return *expected_bool == *actual_bool;
  }
  // Integers compare exactly: a relative tolerance would equate large byte counts that differ.
  const auto expected_int = ParseInteger(expected);
  const auto actual_int = ParseInteger(actual);
  if (expected_int && actual_int) {
    return *expected_int == *actual_int;
  }
  const auto expected_num = ParseDecimal(expected);
  const auto actual_num = ParseDecimal(actual);
  if (expected_num && actual_num) {
    return std::abs(*expected_num - *actual_num) <=
           1e-12 * std::max(std::abs(*expected_num), std::abs(*actual_num));
  }
  return expected == actual;
}

// Same as ProfileValuesMatch, but returns an error naming `what` and both values on mismatch.
Status CheckProfileValue(
    const std::string& what, const std::string& file_value, const std::string& live_value,
    StripQuotes strip_quotes = StripQuotes::kFalse) {
  if (ProfileValuesMatch(file_value, live_value, strip_quotes)) {
    return Status::OK();
  }
  return STATUS_FORMAT(
      IllegalState, "$0: file value '$1' does not match daemon value '$2'", what, file_value,
      live_value);
}

struct HelpXmlFlag {
  std::string current;
  std::string tags;

  bool deprecated() const {
    size_t pos = 0;
    while (pos <= tags.size()) {
      auto end = tags.find(',', pos);
      if (end == std::string::npos) {
        end = tags.size();
      }
      if (std::string_view(tags).substr(pos, end - pos) == "deprecated") {
        return true;
      }
      pos = end + 1;
    }
    return false;
  }
};

using HelpXmlFlags = std::unordered_map<std::string, HelpXmlFlag>;

// Reverses EscapeForHtml (util/url-coding.h). "<br/>" goes first because it is the only escape
// containing a raw '<'; "&amp;" goes last so "&amp;lt;" becomes "&lt;", not "<".
std::string UnescapeHelpXml(std::string_view s) {
  std::string result(s);
  static const std::vector<std::pair<std::string_view, std::string_view>> kEntities = {
      {"<br/>", "\n"}, {"&lt;", "<"}, {"&gt;", ">"}, {"&quot;", "\""}, {"&amp;", "&"}};
  for (const auto& [from, to] : kEntities) {
    std::string next;
    size_t pos = 0;
    for (;;) {
      auto found = result.find(from, pos);
      if (found == std::string::npos) {
        next.append(result, pos, std::string::npos);
        break;
      }
      next.append(result, pos, found - pos).append(to);
      pos = found + from.size();
    }
    result = std::move(next);
  }
  return result;
}

Result<std::string> ExtractXmlElement(std::string_view flag_xml, std::string_view tag) {
  const auto open = Format("<$0>", tag);
  const auto close = Format("</$0>", tag);
  const auto begin = flag_xml.find(open);
  if (begin == std::string_view::npos) {
    return STATUS_FORMAT(NotFound, "No <$0> in '$1'", tag, flag_xml);
  }
  const auto value_begin = begin + open.size();
  const auto end = flag_xml.find(close, value_begin);
  if (end == std::string_view::npos) {
    return STATUS_FORMAT(Corruption, "Unterminated <$0> in '$1'", tag, flag_xml);
  }
  return UnescapeHelpXml(flag_xml.substr(value_begin, end - value_begin));
}

// Scans the whole buffer rather than line by line, so the parse does not depend on how the
// output is wrapped.
Result<HelpXmlFlags> ParseHelpXml(std::string_view xml) {
  static constexpr std::string_view kOpen = "<flag>";
  static constexpr std::string_view kClose = "</flag>";
  HelpXmlFlags flags;
  size_t pos = 0;
  for (;;) {
    const auto begin = xml.find(kOpen, pos);
    if (begin == std::string_view::npos) {
      break;
    }
    const auto end = xml.find(kClose, begin);
    if (end == std::string_view::npos) {
      return STATUS(Corruption, "Unterminated <flag> in --helpxml output");
    }
    const auto flag_xml = xml.substr(begin + kOpen.size(), end - begin - kOpen.size());
    auto name = VERIFY_RESULT(ExtractXmlElement(flag_xml, "name"));
    HelpXmlFlag flag{
      .current = VERIFY_RESULT(ExtractXmlElement(flag_xml, "current")),
      .tags = VERIFY_RESULT(ExtractXmlElement(flag_xml, "tags")),
    };
    if (!flags.emplace(name, std::move(flag)).second) {
      return STATUS_FORMAT(Corruption, "Flag $0 listed twice in --helpxml output", name);
    }
    pos = end + kClose.size();
  }
  if (flags.empty()) {
    return STATUS(Corruption, "No <flag> elements in --helpxml output");
  }
  return flags;
}

// Runs `<daemon> --flagfile=<flagfile> --helpxml`. gflags fully parses, validates and
// preview-gates the flags before printing them, so a zero exit proves the file is accepted.
Result<HelpXmlFlags> RunHelpXml(const std::string& daemon, const std::string& flagfile) {
  const auto binary = VERIFY_RESULT(
      path_utils::GetToolPath(daemon + FLAGS_dynamically_linked_exe_suffix));
  std::string output;
  std::string error;
  const auto status =
      Subprocess::Call({binary, "--flagfile=" + flagfile, "--helpxml"}, &output, &error);
  if (!status.ok()) {
    return status.CloneAndAppend(Format("$0 rejected the profile; stderr:\n$1", daemon, error));
  }
  return VERIFY_RESULT_PREPEND(ParseHelpXml(output), Format("$0 --helpxml", daemon));
}

}  // namespace

class PgProductionProfileTest : public LibPqTestBase {
 protected:
  void SetUp() override {
    if (std::string_view(CURRENT_TEST_NAME()) == "FlagfilesParseCleanly") {
      // Only the test directory is needed; LibPqTestBase::SetUp would start a cluster.
      YBMiniClusterTestBase<ExternalMiniCluster>::SetUp();
      return;
    }
    LibPqTestBase::SetUp();
  }

  // Writes the profile to dest without the active lines of kBootstrapDeferredFlags, for booting a
  // fresh cluster.
  static Status WriteCopyWithoutDeferredFlags(
      const DaemonProfile& profile, const std::string& dest) {
    std::string out;
    for (auto line : SplitLines(profile.contents)) {
      auto entry = VERIFY_RESULT(ParseProfileLine(line));
      if (entry && entry->kind == ProfileEntryKind::kActive && IsBootstrapDeferred(entry->key)) {
        continue;
      }
      out.append(line).append("\n");
    }
    return WriteStringToFile(Env::Default(), out, dest);
  }
};

// For each daemon, runs the binary on the full profile with --helpxml and checks that every
// active key is a known, non-deprecated flag whose parsed value matches the file.
TEST_F(PgProductionProfileTest, FlagfilesParseCleanly) {
  std::map<std::string, DaemonProfile> profiles;
  std::map<std::string, HelpXmlFlags> xml;
  for (const auto& daemon : {kMasterDaemon, kTServerDaemon}) {
    auto profile = ASSERT_RESULT(LoadProfile(daemon));
    const auto copy = GetTestPath(daemon + ".flags");
    ASSERT_OK(WriteFullCopy(profile, copy));
    xml[daemon] = ASSERT_RESULT(RunHelpXml(daemon, copy));
    profiles[daemon] = std::move(profile);
  }

  std::vector<std::string> failures;
  auto& tserver_xml = xml[kTServerDaemon];
  const auto& tserver_profile = profiles[kTServerDaemon];

  for (const auto& [daemon, profile] : profiles) {
    const auto& flags = xml[daemon];
    for (const auto& entry : profile.entries) {
      if (entry.kind != ProfileEntryKind::kActive) {
        continue;
      }
      // Rows of the two lists are checked separately below.
      if (daemon == kTServerDaemon && FindKnownUnrecognized(entry.key)) {
        continue;
      }
      const auto* other_daemon_only = FindOtherDaemonOnly(entry.key);
      if (other_daemon_only && other_daemon_only->defined_in_daemon != daemon) {
        continue;
      }
      auto it = flags.find(entry.key);
      if (it == flags.end()) {
        failures.push_back(Format(
            "$0: --$1 (file value '$2') is not a flag of this binary; gflags drops it silently",
            daemon, entry.key, entry.value));
        continue;
      }
      if (it->second.deprecated()) {
        failures.push_back(Format(
            "$0: --$1 is deprecated (tags '$2'); file value '$3', parsed value '$4'", daemon,
            entry.key, it->second.tags, entry.value, it->second.current));
      }
      auto status = CheckProfileValue(
          Format("$0: --$1", daemon, entry.key), entry.value, it->second.current);
      if (!status.ok()) {
        failures.push_back(status.message().ToBuffer());
      }
    }
  }

  // A row whose key left the profile, or that became a gflag, must be removed or revisited.
  for (const auto& row : kKnownUnrecognizedFlags) {
    const auto* entry = tserver_profile.FindActive(row.key);
    if (!entry) {
      failures.push_back(Format(
          "$0: kKnownUnrecognizedFlags row $1 has no active line in the profile; remove the row "
          "if the profile was corrected", kTServerDaemon, row.key));
    }
    auto it = tserver_xml.find(row.key);
    if (it != tserver_xml.end()) {
      failures.push_back(Format(
          "$0: kKnownUnrecognizedFlags row $1 is now a flag of this binary (file value '$2', "
          "parsed value '$3'); revisit the row: $4", kTServerDaemon, row.key,
          entry ? entry->value : "<none>", it->second.current, row.reason));
    }
  }

  for (const auto& row : kOtherDaemonOnlyFlags) {
    const auto& defining_daemon = row.defined_in_daemon;
    const auto& profile_daemon =
        defining_daemon == kMasterDaemon ? kTServerDaemon : kMasterDaemon;
    const auto* entry = profiles[profile_daemon].FindActive(row.key);
    if (!entry) {
      failures.push_back(Format(
          "$0: kOtherDaemonOnlyFlags row $1 has no active line in the profile; remove the row "
          "if the profile was corrected", profile_daemon, row.key));
      continue;
    }
    // Checked against the defining binary, so a rename or removal still fails by name.
    auto defining_it = xml[defining_daemon].find(row.key);
    if (defining_it == xml[defining_daemon].end()) {
      failures.push_back(Format(
          "$0: kOtherDaemonOnlyFlags row $1 (in the $2 profile, file value '$3') is not a flag "
          "of $0 either; revisit the row: $4", defining_daemon, row.key, profile_daemon,
          entry->value, row.reason));
    } else if (defining_it->second.deprecated()) {
      failures.push_back(Format(
          "$0: kOtherDaemonOnlyFlags row $1 (in the $2 profile) is deprecated (tags '$3')",
          defining_daemon, row.key, profile_daemon, defining_it->second.tags));
    }
    // Absent from a dynamically linked binary that does not link the defining code; present in
    // a merged LTO yb-server, where the value must still parse to the file value.
    auto it = xml[profile_daemon].find(row.key);
    if (it != xml[profile_daemon].end()) {
      auto status = CheckProfileValue(
          Format("$0: --$1", profile_daemon, row.key), entry->value, it->second.current);
      if (!status.ok()) {
        failures.push_back(status.message().ToBuffer());
      }
    }
  }

  ASSERT_TRUE(failures.empty()) << "Production profile problems:\n" << JoinStrings(failures, "\n");
}

}  // namespace yb::pgwrapper
