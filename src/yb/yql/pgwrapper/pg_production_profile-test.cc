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
// to be looked up by name instead. FlagfilesParseCleanly checks acceptance with --helpxml, without
// a cluster. ClusterRunsProductionProfile boots a cluster with the files and checks the effect:
// gflag values read back from every daemon, PG GUCs and their provenance, and the hba rules.
//
// Flagfile grammar. The active lines are byte-for-byte what the deployment renders, so a fresh
// render diffs cleanly against the files. Lines the test cannot apply are kept as markers:
//   --<key>=<value>, or bare --<key> meaning true
//       Active: applied and asserted.
//   # PROD --<key>=<per-host> [DROPPED: <reason>]
//       Host-specific production line. The value is never recorded, applied or asserted.
//   # PROD --<key>=<value> [SUBSTITUTED: harness]
//       Production value the harness overrides. Not applied; the live value is expected to differ.
//   Production still sets the key of either marker, so it must remain a known, non-deprecated
//   flag of the daemon.
//   Any other line starting with '#': free-form comment.
//   Blank line: ignored.
// A line starting with "# PROD " that is not one of the two marker forms fails the test, and so
// does a comment that looks like a mistyped marker: '#', optional whitespace, then "prod" in any
// case followed by whitespace or '-' ("#PROD --", "# prod --"; "# Production ..." stays a
// comment). A typo'd marker would otherwise be a plain comment whose key silently leaves both the
// expected and the read-back sets.
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
//     comparisons against PG's settings views.
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
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "yb/gutil/strings/join.h"

#include "yb/integration-tests/external_mini_cluster.h"

#include "yb/util/csv_util.h"
#include "yb/util/env.h"
#include "yb/util/env_util.h"
#include "yb/util/enums.h"
#include "yb/util/faststring.h"
#include "yb/util/flags.h"
#include "yb/util/format.h"
#include "yb/util/net/net_util.h"
#include "yb/util/path_util.h"
#include "yb/util/pg_util.h"
#include "yb/util/result.h"
#include "yb/util/status.h"
#include "yb/util/strongly_typed_bool.h"
#include "yb/util/subprocess.h"
#include "yb/util/test_macros.h"

#include "yb/yql/pgwrapper/libpq_test_base.h"
#include "yb/yql/pgwrapper/libpq_utils.h"

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

std::string ToLower(std::string_view s) {
  std::string result(s);
  for (auto& c : result) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return result;
}

bool IsSpace(char c) {
  return std::isspace(static_cast<unsigned char>(c));
}

// True for a comment that looks like a mistyped marker ("#PROD --", "# prod --", "#  PROD --").
// Such a line would otherwise be a plain comment: its key would leave both the expected and the
// read-back sets, so no assertion would notice the marker is gone.
bool IsNearMissMarker(std::string_view line) {
  auto rest = line.substr(1);
  while (!rest.empty() && IsSpace(rest.front())) {
    rest.remove_prefix(1);
  }
  return rest.size() > 4 && ToLower(rest.substr(0, 4)) == "prod" &&
         (IsSpace(rest[4]) || rest[4] == '-');
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
    if (IsNearMissMarker(line)) {
      return STATUS_FORMAT(
          InvalidArgument,
          "Comment looks like a mistyped marker (markers start with exactly '# PROD '): '$0'",
          line);
    }
    return std::nullopt;
  }
  // gflags applies "-key=value" and indented flag lines, but rendered flagfiles never have them.
  if (line.starts_with("-") || IsSpace(line.front())) {
    return STATUS_FORMAT(
        InvalidArgument, "Not in the profile grammar (flags start with '--' at column 0): '$0'",
        line);
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
    // Checked here because a skip inside UpdateMiniClusterOptions would not stop the cluster from
    // starting. Production connects clients to PG directly, and the hba checks assume that.
    ExternalMiniClusterOptions conn_mgr_probe;
    conn_mgr_probe.enable_ysql = true;
    if (conn_mgr_probe.IsYsqlConnMgrEnabled()) {
      GTEST_SKIP() << "Production does not run the YSQL connection manager";
    }
    LibPqTestBase::SetUp();
  }

  void UpdateMiniClusterOptions(ExternalMiniClusterOptions* opts) override {
    opts->num_masters = 1;
    opts->num_tablet_servers = 3;
    opts->replication_factor = 3;
    opts->enable_ysql_auth = true;
    // Makes the harness pass --alsologtostderr instead of --logtostderr, so the profile's
    // logtostderr=false holds and PG writes its own log files.
    opts->log_to_file = true;

    master_profile_ = ASSERT_RESULT(LoadProfile(kMasterDaemon));
    tserver_profile_ = ASSERT_RESULT(LoadProfile(kTServerDaemon));
    master_flagfile_ = GetTestPath(kMasterDaemon + ".flags");
    tserver_flagfile_ = GetTestPath(kTServerDaemon + ".flags");
    ASSERT_OK(WriteCopyWithoutDeferredFlags(master_profile_, master_flagfile_));
    ASSERT_OK(WriteFullCopy(tserver_profile_, tserver_flagfile_));

    // gflags applies --flagfile at its position in argv, so last in extra_*_flags it beats every
    // fixture default (e.g. --timestamp_history_retention_interval_sec=0, --ysql_num_tablets=-1)
    // and loses only to the flags the harness pushes after opts (replication_factor,
    // transaction_table_num_tablets, enable_ysql, ysql_enable_auth) and to YB_EXTRA_*_FLAGS.
    // The snapshotter knob goes first so that if production ever sets it, the profile wins and
    // readback stays consistent.
    opts->extra_tserver_flags.push_back("--metrics_snapshotter_interval_ms=1000");
    opts->extra_tserver_flags.push_back("--flagfile=" + tserver_flagfile_);
    opts->extra_master_flags.push_back("--flagfile=" + master_flagfile_);
  }

  // With auth on, LibPqTestBase::Connect's default user postgres cannot log in over TCP.
  static Result<PGConn> ConnectTcp(
      const ExternalTabletServer& ts, const std::string& user, const std::string& password,
      const std::string& dbname = kDefaultDb, size_t connect_timeout_sec = 0) {
    return PGConnBuilder({
      .host = ts.bind_host(),
      .port = ts.ysql_port(),
      .dbname = dbname,
      .user = user,
      .password = password,
      .connect_timeout = connect_timeout_sec,
    }).Connect();
  }

  static Result<PGConn> ConnectUnixSocket(
      const ExternalTabletServer& ts, const std::string& user,
      const std::string& dbname = kDefaultDb, size_t connect_timeout_sec = 0) {
    return PGConnBuilder({
      .host = PgDeriveSocketDir(HostPort(ts.bind_host(), ts.ysql_port())),
      .port = ts.ysql_port(),
      .dbname = dbname,
      .user = user,
      .connect_timeout = connect_timeout_sec,
    }).Connect();
  }

  // LibPqTestBase::ConnectToDBWithReplication, authenticated as the superuser.
  static Result<PGConn> ConnectReplication(
      const ExternalTabletServer& ts, const std::string& dbname) {
    return PGConnBuilder({
      .host = ts.bind_host(),
      .port = ts.pgsql_rpc_port(),
      .dbname = dbname,
      .user = kSuperuser,
      .password = kSuperuserPassword,
      .replication = "database",
    }).Connect(/* simple_query_protocol= */ true);
  }

  Result<PGConn> ConnectAsSuperuser() {
    return ConnectTcp(*pg_ts, kSuperuser, kSuperuserPassword);
  }

  std::vector<std::pair<std::string, ExternalDaemon*>> DaemonsOf(const std::string& daemon) {
    std::vector<std::pair<std::string, ExternalDaemon*>> result;
    if (daemon == kMasterDaemon) {
      for (size_t i = 0; i < cluster_->num_masters(); ++i) {
        result.emplace_back(Format("$0-$1", daemon, i), cluster_->master(i));
      }
    } else {
      for (size_t i = 0; i < cluster_->num_tablet_servers(); ++i) {
        result.emplace_back(Format("$0-$1", daemon, i), cluster_->tablet_server(i));
      }
    }
    return result;
  }

  const DaemonProfile& ProfileOf(const std::string& daemon) const {
    return daemon == kMasterDaemon ? master_profile_ : tserver_profile_;
  }

  void CheckFlagReadback();
  void CheckOtherDaemonOnlyFlags();
  void CheckPgConfCsv(PGConn* conn);
  void CheckPgTaggedFlags(PGConn* conn);
  void CheckHbaRules(PGConn* conn);
  void CheckHbaBehavior(PGConn* conn);

  static constexpr auto kDefaultDb = "yugabyte";
  static constexpr auto kSuperuser = "yugabyte";
  static constexpr auto kSuperuserPassword = "yugabyte";
  // Bounds a login that is expected to be rejected: PGConn::Connect retries most rejections,
  // including "no password supplied", until its deadline (60 s by default).
  static constexpr size_t kRejectedConnectTimeoutSec = 3;

  DaemonProfile master_profile_;
  DaemonProfile tserver_profile_;
  // Paths passed to --flagfile. The master copy leaves out kBootstrapDeferredFlags.
  std::string master_flagfile_;
  std::string tserver_flagfile_;

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
      auto it = flags.find(entry.key);
      if (entry.kind != ProfileEntryKind::kActive) {
        // Production sets this key even though the test cannot apply its value.
        const auto* marker = entry.kind == ProfileEntryKind::kDropped ? "DROPPED" : "SUBSTITUTED";
        if (it == flags.end()) {
          failures.push_back(Format(
              "$0: --$1 ($2 marker) is not a flag of this binary; production's line for it is "
              "dropped silently", daemon, entry.key, marker));
        } else if (it->second.deprecated()) {
          failures.push_back(Format(
              "$0: --$1 ($2 marker) is deprecated (tags '$3')", daemon, entry.key, marker,
              it->second.tags));
        }
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

namespace {

// (daemon, key)
using DaemonKey = std::pair<std::string, std::string>;

std::string DaemonKeysToString(const std::set<DaemonKey>& keys) {
  std::vector<std::string> parts;
  for (const auto& [daemon, key] : keys) {
    parts.push_back(Format("$0 --$1", daemon, key));
  }
  return "{" + JoinStrings(parts, ", ") + "}";
}

// ExternalDaemon::GetFlag reports a name the daemon does not know with this status; an RPC
// failure comes back as some other status.
bool IsUnknownFlagError(const Status& status) {
  return status.IsRemoteError() &&
         status.message().ToBuffer().find("Failed to get gflag") != std::string::npos;
}

Result<std::vector<std::string>> ReadProfileCsv(
    const DaemonProfile& profile, const std::string& key) {
  const auto* entry = profile.FindActive(key);
  if (!entry) {
    return STATUS_FORMAT(NotFound, "$0: no active --$1 line", profile.daemon, key);
  }
  std::vector<std::string> values;
  RETURN_NOT_OK_PREPEND(ReadCSVValues(entry->value, &values), Format("Parsing --$0", key));
  if (values.empty()) {
    return STATUS_FORMAT(InvalidArgument, "$0: --$1 has no elements", profile.daemon, key);
  }
  return values;
}

std::vector<std::string> SplitWhitespace(const std::string& s) {
  std::istringstream in(s);
  std::vector<std::string> fields;
  for (std::string field; in >> field;) {
    fields.push_back(std::move(field));
  }
  return fields;
}

std::string OrNull(const std::optional<std::string>& value) {
  return value ? Format("'$0'", *value) : "NULL";
}

}  // namespace

// Reads back every value the profile sets from every live daemon. The set M of (daemon, key)
// pairs whose live value differs from the file must be exactly the SUBSTITUTED markers. A value
// that silently did not apply (gflags ignores an unknown name in a flagfile) or a new harness
// override then fails by name, and so does a SUBSTITUTED key the harness stopped overriding, whose
// marker should become an active line.
void PgProductionProfileTest::CheckFlagReadback() {
  std::set<DaemonKey> expected_m;
  std::set<DaemonKey> m;
  std::map<DaemonKey, std::string> file_values;
  std::map<DaemonKey, std::vector<std::string>> live_values;
  // M is keyed by daemon type, so also require all daemons of a type to agree.
  std::vector<std::string> inconsistent;
  for (const auto& daemon : {kMasterDaemon, kTServerDaemon}) {
    const auto daemons = DaemonsOf(daemon);
    for (const auto& entry : ProfileOf(daemon).entries) {
      // Skipped here, and what covers them:
      // - DROPPED markers carry no value; T1 checks the key is still a known, non-deprecated flag.
      // - kBootstrapDeferredFlags are not in the master's boot copy, so the master runs the
      //   default; T1 checks the full file's value against --helpxml.
      // - kKnownUnrecognizedFlags are not gflags; CheckPgTaggedFlags checks the GUCs they were
      //   meant for.
      // - kOtherDaemonOnlyFlags keys in the profile of the daemon that does not define them:
      //   CheckOtherDaemonOnlyFlags.
      if (entry.kind == ProfileEntryKind::kDropped) {
        continue;
      }
      if (entry.kind == ProfileEntryKind::kActive) {
        if (daemon == kMasterDaemon && IsBootstrapDeferred(entry.key)) {
          continue;
        }
        if (daemon == kTServerDaemon && FindKnownUnrecognized(entry.key)) {
          continue;
        }
        const auto* other_daemon_only = FindOtherDaemonOnly(entry.key);
        if (other_daemon_only && other_daemon_only->defined_in_daemon != daemon) {
          continue;
        }
      }
      const DaemonKey daemon_key{daemon, entry.key};
      if (entry.kind == ProfileEntryKind::kSubstituted) {
        expected_m.insert(daemon_key);
      }
      file_values[daemon_key] = entry.value;
      size_t num_differ = 0;
      for (const auto& [name, process] : daemons) {
        auto live = process->GetFlag(entry.key);
        if (live.ok()) {
          num_differ += !ProfileValuesMatch(entry.value, *live);
          live_values[daemon_key].push_back(Format("$0 '$1'", name, *live));
        } else {
          ++num_differ;
          live_values[daemon_key].push_back(Format("$0 <$1>", name, live.status()));
        }
      }
      if (num_differ > 0) {
        m.insert(daemon_key);
      }
      if (num_differ != 0 && num_differ != daemons.size()) {
        inconsistent.push_back(Format(
            "$0 --$1: daemons disagree (file '$2'; live $3)", daemon, entry.key, entry.value,
            JoinStrings(live_values[daemon_key], ", ")));
      }
    }
  }
  LOG(INFO) << "Read back " << file_values.size() << " keys; those whose live value differs "
            << "from the profile (M): " << DaemonKeysToString(m);

  std::set<DaemonKey> reported(m);
  reported.insert(expected_m.begin(), expected_m.end());
  std::vector<std::string> details;
  for (const auto& daemon_key : reported) {
    const auto* verdict = !expected_m.contains(daemon_key)
        ? "UNEXPECTED: active value did not apply"
        : !m.contains(daemon_key) ? "UNEXPECTED: SUBSTITUTED value is no longer overridden"
                                  : "SUBSTITUTED, differs as expected";
    details.push_back(Format(
        "$0 --$1 [$2]: file '$3'; live $4", daemon_key.first, daemon_key.second, verdict,
        file_values[daemon_key], JoinStrings(live_values[daemon_key], ", ")));
  }
  ASSERT_TRUE(m == expected_m && inconsistent.empty())
      << "M:           " << DaemonKeysToString(m) << "\n"
      << "SUBSTITUTED: " << DaemonKeysToString(expected_m) << "\n"
      << JoinStrings(details, "\n") << "\n" << JoinStrings(inconsistent, "\n");

  for (const auto& [name, tserver] : DaemonsOf(kTServerDaemon)) {
    // Also covered by M. log_to_file makes the harness pass --alsologtostderr instead of
    // --logtostderr, the only harness flag that touches logtostderr; PG writes its own log files
    // only while logtostderr is false.
    const auto logtostderr = ASSERT_RESULT(tserver->GetFlag("logtostderr"));
    ASSERT_EQ(logtostderr, "false") << name;
    // use_memory_defaults_optimized_for_ysql raises the tserver's block cache default to 32%
    // (AdjustMemoryLimitsIfNeeded in tserver/server_main_util.cc).
    const auto block_cache = ASSERT_RESULT(tserver->GetFlag("db_block_cache_size_percentage"));
    ASSERT_EQ(block_cache, "32") << name;
  }
}

// kOtherDaemonOnlyFlags keys, in the profile of the daemon that does not define them. A
// dynamically linked binary does not know the key; a merged LTO yb-server accepts the file value,
// with no effect either way.
void PgProductionProfileTest::CheckOtherDaemonOnlyFlags() {
  std::vector<std::string> failures;
  for (const auto& row : kOtherDaemonOnlyFlags) {
    const auto& profile_daemon =
        row.defined_in_daemon == kMasterDaemon ? kTServerDaemon : kMasterDaemon;
    const auto* entry = ProfileOf(profile_daemon).FindActive(row.key);
    if (!entry) {
      failures.push_back(Format(
          "$0: kOtherDaemonOnlyFlags row $1 has no active line in the profile", profile_daemon,
          row.key));
      continue;
    }
    for (const auto& [name, process] : DaemonsOf(profile_daemon)) {
      auto live = process->GetFlag(row.key);
      if (live.ok()) {
        auto status = CheckProfileValue(Format("$0: --$1", name, row.key), entry->value, *live);
        if (!status.ok()) {
          failures.push_back(status.message().ToBuffer());
        }
      } else if (!IsUnknownFlagError(live.status())) {
        failures.push_back(Format("$0: GetFlag($1) failed: $2", name, row.key, live.status()));
      }
    }

    // When the defining daemon's own profile sets the key, CheckFlagReadback reads it there.
    // Otherwise production's value never reaches the daemon where the flag acts, and the live
    // value there is expected to differ from it. This fails if the default comes to equal the
    // file value; revisit the row then.
    if (ProfileOf(row.defined_in_daemon).FindActive(row.key)) {
      continue;
    }
    for (const auto& [name, process] : DaemonsOf(row.defined_in_daemon)) {
      auto live = process->GetFlag(row.key);
      if (!live.ok()) {
        failures.push_back(Format("$0: GetFlag($1) failed: $2", name, row.key, live.status()));
      } else if (ProfileValuesMatch(entry->value, *live)) {
        failures.push_back(Format(
            "$0: --$1 is '$2', the value only the $3 profile sets; $0 was expected to run "
            "without it. Revisit the row: $4", name, row.key, *live, profile_daemon, row.reason));
      } else {
        LOG(INFO) << name << " runs --" << row.key << "=" << *live << "; only the "
                  << profile_daemon << " profile sets " << entry->value;
      }
    }
  }
  ASSERT_TRUE(failures.empty()) << JoinStrings(failures, "\n");
}

// Every ysql_pg_conf_csv element arrived through ysql_pg.conf. Provenance rather than behavior is
// the proof: CBO=on turns some of the same GUCs on by itself (yb_enable_bitmapscan,
// yb_enable_update_reltuples_after_create_index), and the profile's explicit
// ysql_enable_auto_analyze=false beats CBO's default for it, which CheckFlagReadback pins.
void PgProductionProfileTest::CheckPgConfCsv(PGConn* conn) {
  const auto elements = ASSERT_RESULT(ReadProfileCsv(tserver_profile_, "ysql_pg_conf_csv"));
  std::vector<std::string> failures;
  for (const auto& element : elements) {
    const auto eq = element.find('=');
    if (eq == std::string::npos) {
      failures.push_back(Format("ysql_pg_conf_csv element without '=': '$0'", element));
      continue;
    }
    const auto name = element.substr(0, eq);
    const auto file_value = element.substr(eq + 1);
    auto show = conn->FetchRow<std::string>("SHOW " + name);
    if (!show.ok()) {
      failures.push_back(Format("SHOW $0: $1", name, show.status()));
    }
    // ysql_pg.conf can hold several lines for one name (initdb defaults first, then the CSV, then
    // PG-tagged gflags); PG applies the last.
    auto rows = conn->FetchRows<
        std::optional<std::string>, bool, std::optional<std::string>, std::optional<std::string>>(
        Format(
            "SELECT setting, applied, sourcefile, error FROM pg_file_settings WHERE name = '$0' "
            "ORDER BY seqno", name));
    if (!rows.ok()) {
      failures.push_back(Format("pg_file_settings $0: $1", name, rows.status()));
    } else if (rows->empty()) {
      failures.push_back(Format("pg_file_settings has no row for $0", name));
    } else {
      for (const auto& [setting, applied, sourcefile, error] : *rows) {
        if (error) {
          failures.push_back(Format(
              "pg_file_settings $0 (setting $1, file $2) has error '$3'", name, OrNull(setting),
              OrNull(sourcefile), *error));
        }
      }
      const auto& [setting, applied, sourcefile, error] = rows->back();
      if (!applied) {
        failures.push_back(Format("pg_file_settings: last row for $0 is not applied", name));
      }
      if (!sourcefile || !sourcefile->ends_with("/ysql_pg.conf")) {
        failures.push_back(Format(
            "pg_file_settings: last row for $0 comes from $1, not ysql_pg.conf", name,
            OrNull(sourcefile)));
      }
      auto status = CheckProfileValue(
          Format("pg_file_settings $0", name), file_value, setting.value_or("<NULL>"),
          StripQuotes::kTrue);
      if (!status.ok()) {
        failures.push_back(status.message().ToBuffer());
      }
    }
    auto source = conn->FetchRow<std::string>(
        Format("SELECT source FROM pg_settings WHERE name = '$0'", name));
    if (!source.ok()) {
      failures.push_back(Format("pg_settings $0: $1", name, source.status()));
    } else if (*source != "configuration file") {
      failures.push_back(Format("pg_settings $0: source is '$1'", name, *source));
    }
  }
  ASSERT_TRUE(failures.empty()) << JoinStrings(failures, "\n");
}

// PG-tagged gflags (DEFINE_*_PG_FLAG) are named ysql_<GUC>, and the tserver writes each
// non-default one into ysql_pg.conf. The rule "active tserver key ysql_X where pg_settings has a
// row named X" finds them without a list. A non-PG-tagged flag whose suffix is a GUC name would be
// a false positive; every match today is a PG-tagged flag, so no exclusion list is needed.
void PgProductionProfileTest::CheckPgTaggedFlags(PGConn* conn) {
  static constexpr std::string_view kPgFlagPrefix = "ysql_";
  const auto settings = ASSERT_RESULT((conn->FetchRows<
      std::string, std::optional<std::string>, std::string>(
      "SELECT name, setting, source FROM pg_settings")));
  std::unordered_map<std::string, std::pair<std::optional<std::string>, std::string>> by_name;
  for (const auto& [name, setting, source] : settings) {
    by_name.emplace(name, std::make_pair(setting, source));
  }

  std::vector<std::string> matched;
  std::vector<std::string> failures;
  for (const auto& entry : tserver_profile_.entries) {
    if (entry.kind != ProfileEntryKind::kActive || !entry.key.starts_with(kPgFlagPrefix)) {
      continue;
    }
    const auto guc = entry.key.substr(kPgFlagPrefix.size());
    auto it = by_name.find(guc);
    if (it == by_name.end()) {
      continue;
    }
    matched.push_back(guc);
    const auto& [setting, source] = it->second;
    if (source != "configuration file") {
      failures.push_back(Format("--$0: pg_settings $1 source is '$2'", entry.key, guc, source));
    }
    auto status = CheckProfileValue(
        Format("--$0 against pg_settings $1", entry.key, guc), entry.value,
        setting.value_or("<NULL>"), StripQuotes::kTrue);
    if (!status.ok()) {
      failures.push_back(status.message().ToBuffer());
    }
  }
  LOG(INFO) << "PG-tagged flags found as GUCs: " << JoinStrings(matched, ", ");
  ASSERT_FALSE(matched.empty()) << "No active ysql_<GUC> key matched a pg_settings row";

  // The profile's line for each of these never reaches PG, so the GUC shows what production runs
  // with. "Not from the config file" is checked as zero matching rows, not as one row's source:
  // yb_enable_parallel_append is an old-name alias of enable_parallel_append here, with no
  // pg_settings row of its own.
  for (const auto& row : kKnownUnrecognizedFlags) {
    auto show = conn->FetchRow<std::string>("SHOW " + row.show_guc);
    if (!show.ok()) {
      failures.push_back(Format("SHOW $0 (row $1): $2", row.show_guc, row.key, show.status()));
    } else if (*show != row.expected_show) {
      failures.push_back(Format(
          "kKnownUnrecognizedFlags row $0: SHOW $1 is '$2', expected '$3'; revisit the row: $4",
          row.key, row.show_guc, *show, row.expected_show, row.reason));
    }
    for (const auto& query : {
        Format(
            "SELECT count(*) FROM pg_settings WHERE name = '$0' AND source = 'configuration file'",
            row.show_guc),
        Format("SELECT count(*) FROM pg_file_settings WHERE name = '$0'", row.show_guc)}) {
      auto count = conn->FetchRow<int64_t>(query);
      if (!count.ok()) {
        failures.push_back(Format("$0: $1", query, count.status()));
      } else if (*count != 0) {
        failures.push_back(Format(
            "kKnownUnrecognizedFlags row $0: GUC $1 is set from a config file ($2 returned $3); "
            "revisit the row: $4", row.key, row.show_guc, query, *count, row.reason));
      }
    }
  }
  ASSERT_TRUE(failures.empty()) << JoinStrings(failures, "\n");
}

// ysql_hba.conf starts with the ysql_hba_conf_csv lines; WritePgHbaConfig appends its own after
// them.
void PgProductionProfileTest::CheckHbaRules(PGConn* conn) {
  const auto lines = ASSERT_RESULT(ReadProfileCsv(tserver_profile_, "ysql_hba_conf_csv"));
  using OptString = std::optional<std::string>;
  const auto rows = ASSERT_RESULT((conn->FetchRows<
      int32_t, OptString, OptString, OptString, OptString, OptString, OptString, OptString>(
      "SELECT line_number, type, array_to_string(database, ','), "
      "array_to_string(user_name, ','), address, netmask, auth_method, error "
      "FROM pg_hba_file_rules ORDER BY line_number")));
  std::vector<std::string> failures;
  for (const auto& row : rows) {
    if (const auto& error = std::get<7>(row)) {
      failures.push_back(Format("ysql_hba.conf line $0: $1", std::get<0>(row), *error));
    }
  }
  ASSERT_GE(rows.size(), lines.size()) << "pg_hba_file_rules has fewer rules than the CSV";

  for (size_t i = 0; i < lines.size(); ++i) {
    const auto fields = SplitWhitespace(lines[i]);
    const auto& [line_number, type, database, user_name, address, netmask, auth_method, error] =
        rows[i];
    if (fields.size() < 4) {
      failures.push_back(Format("hba line '$0' has an unsupported form", lines[i]));
      continue;
    }
    OptString expected_address;
    OptString expected_netmask;
    size_t method_index = 3;
    if (fields[0] != "local") {
      const auto& cidr = fields[3];
      if (cidr.find('/') == std::string::npos) {
        failures.push_back(Format(
            "hba line '$0': only the CIDR address form is supported by this check", lines[i]));
        continue;
      }
      // PG reports the address and mask it parsed from the CIDR (0.0.0.0/0 -> 0.0.0.0 and
      // 0.0.0.0, ::0/0 -> :: and ::), so let PG normalize the expectation the same way.
      const auto normalized = ASSERT_RESULT((conn->FetchRow<std::string, std::string>(Format(
          "SELECT host('$0'::inet), host(netmask('$0'::inet))", cidr))));
      expected_address = std::get<0>(normalized);
      expected_netmask = std::get<1>(normalized);
      method_index = 4;
    }
    if (fields.size() != method_index + 1) {
      failures.push_back(Format(
          "hba line '$0': auth options are not supported by this check", lines[i]));
      continue;
    }
    const std::vector<std::tuple<std::string_view, OptString, OptString>> columns = {
      {"type", fields[0], type},
      {"database", fields[1], database},
      {"user_name", fields[2], user_name},
      {"address", expected_address, address},
      {"netmask", expected_netmask, netmask},
      {"auth_method", fields[method_index], auth_method},
    };
    for (const auto& [column, expected, actual] : columns) {
      if (expected != actual) {
        failures.push_back(Format(
            "hba line '$0' vs pg_hba_file_rules line $1: $2 is $3, expected $4", lines[i],
            line_number, column, OrNull(actual), OrNull(expected)));
      }
    }
  }
  ASSERT_TRUE(failures.empty()) << JoinStrings(failures, "\n");
}

// conn is already logged in to pg_ts, so a rejected login below cannot be PG still starting.
void PgProductionProfileTest::CheckHbaBehavior(PGConn* conn) {
  // Not discriminating on its own: WritePgHbaConfig (pg_wrapper.cc) appends
  // "local all yugabyte trust" whatever the profile says, while
  // ysql_trust_local_yugabyte_connections is at its default (true).
  ASSERT_OK(ConnectUnixSocket(*pg_ts, kSuperuser));
  ASSERT_NOK_STR_CONTAINS(
      ConnectTcp(*pg_ts, kSuperuser, /* password= */ "", kDefaultDb, kRejectedConnectTimeoutSec),
      "no password supplied");

  // A passwordless non-superuser: only the profile's "local all all trust" lets carol in over the
  // socket, and over TCP the md5 lines ask for a password she does not have.
  ASSERT_OK(conn->Execute("CREATE ROLE carol LOGIN"));
  auto carol = ASSERT_RESULT(ConnectUnixSocket(*pg_ts, "carol"));
  ASSERT_FALSE(ASSERT_RESULT(
      carol.FetchRow<bool>("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")));
  ASSERT_NOK_STR_CONTAINS(
      ConnectTcp(*pg_ts, "carol", /* password= */ "", kDefaultDb, kRejectedConnectTimeoutSec),
      "no password supplied");
}

// Boots the cluster with the profile (UpdateMiniClusterOptions passes the copies by --flagfile)
// and checks that every flag took effect: gflag readback, PG GUC provenance and the hba rules.
TEST_F(PgProductionProfileTest, ClusterRunsProductionProfile) {
  ASSERT_NO_FATALS(CheckFlagReadback());
  ASSERT_NO_FATALS(CheckOtherDaemonOnlyFlags());
  auto conn = ASSERT_RESULT(ConnectAsSuperuser());
  ASSERT_NO_FATALS(CheckPgConfCsv(&conn));
  ASSERT_NO_FATALS(CheckPgTaggedFlags(&conn));
  ASSERT_NO_FATALS(CheckHbaRules(&conn));
  ASSERT_NO_FATALS(CheckHbaBehavior(&conn));
}

}  // namespace yb::pgwrapper
