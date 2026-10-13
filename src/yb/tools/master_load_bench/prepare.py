#!/usr/bin/env python3
"""Prepares production YSQL dumps for master-load-bench.

The bench runs SQL files exactly as given; everything that has to be derived from or changed
in a dump happens here, so the result can be inspected before a load.

  prepare.py --out-dir DIR [--globaldb-repo DIR] [--build-dir build/latest]
             [--drop-stats] [--scale-splits N:PROD] TABLESPACES.sql DUMP.sql ...

Writes into DIR:
  topology.txt   one 'cloud region zone percent' line per zone named by the replica_placement
                 blocks; percentages follow the production tserver counts in --globaldb-repo's
                 cluster_configs/gdb.yml, equal split without it
  roles.sql      CREATE ROLE for every role the dumps reference, so their own "role exists"
                 guards take the production branch instead of skipping ownership
  <basename>     a copy of each input with the requested edits plus FUNCTION_SEARCH_PATHS, the
                 search_path the dump's empty one breaks for some SQL function bodies

Text search dictionaries in the dump name rules files that postgres opens from the build's
postgres/share/tsearch_data; every one missing there is copied from --globaldb-repo's ansible
node role files, and the tool fails if it cannot find one.

Prints the --topology and --sql_file flags to pass to the bench.
"""

import argparse
import os
import re
import shutil
import sys

PLACEMENT_FIELD_RE = re.compile(r'"(cloud|region|zone)"\s*:\s*"([^"]*)"')
SPLIT_RE = re.compile(r"SPLIT INTO (\d+) TABLETS")
ROLNAME_RE = re.compile(r"rolname = '([^']+)'")
OWNER_RE = re.compile(r'\bOWNER(?: TO)? "?([A-Za-z_][A-Za-z0-9_]*)"?')
STATS_PREFIX = "SELECT * FROM pg_catalog.pg_restore_"
RULES_RE = re.compile(r"\brules\s*=\s*'([^']+)'", re.IGNORECASE)
# SQL function bodies are copied verbatim by pg_dump and parsed only when first used, under the
# dump's empty search_path; unqualified names in them fail at the first expression index. Same
# effect as ALTER FUNCTION ... SET search_path in production, which pg_dump then emits itself.
FUNCTION_SEARCH_PATHS = {
    "core.shopify_string_normalization(input text)": "'public', 'pg_catalog'",
}
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
# Inside a globaldb-yugabyte checkout: production tserver counts, and the files ansible installs
# on every node.
GLOBALDB_CLUSTER_CONFIG = "tools/ansible/inventories/production/cluster_configs/gdb.yml"
GLOBALDB_NODE_FILES = "tools/ansible/collections/ansible_collections/shopify/yugabyte/roles/node/files"


def read_lines(path):
    """Lines without their trailing newline, byte-exact (latin-1 round-trips any byte, and
    newline="" keeps str.splitlines from cutting on U+2028 and friends)."""
    with open(path, encoding="latin-1", newline="") as f:
        text = f.read()
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()  # the file ended with a newline; the writer puts it back
    return lines


def open_out(path):
    return open(path, "w", encoding="latin-1", newline="")


def collect_zones(paths):
    """Returns ([(cloud, region, zone)], {(cloud, region) seen only with zone '*'})."""
    zones = []
    wildcard = set()
    for path in paths:
        in_placement = in_read_replica = False
        cloud = region = zone = ""
        for line in read_lines(path):
            if "read_replica_placement='" in line:
                in_read_replica = True  # observer zones: not live-tserver zones
            if "replica_placement='" in line:
                in_placement = True
            if not in_placement:
                continue
            for key, value in PLACEMENT_FIELD_RE.findall(line):
                if key == "cloud":
                    cloud = value
                elif key == "region":
                    region = value
                else:
                    zone = value
            if "min_num_replicas" in line and not in_read_replica:
                if not (cloud and region and zone):
                    sys.exit(f"{path}: placement block without cloud/region/zone")
                if zone == "*":
                    wildcard.add((cloud, region))
                elif (cloud, region, zone) not in zones:
                    zones.append((cloud, region, zone))
                cloud = region = zone = ""
            if "')" in line:
                in_placement = in_read_replica = False
    return zones, wildcard


def read_zone_counts(path):
    """{zone: tserver count} from the tservers: section of a cluster_configs/*.yml. Only
    'zones:' maps with per-zone 'count:' entries are read, so no YAML library is needed."""
    counts = {}
    tservers_indent = None  # indent of the 'tservers:' key while inside its section
    pending = None
    for line in read_lines(path):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip())
        if tservers_indent is not None and indent <= tservers_indent:
            tservers_indent = None
        if tservers_indent is None:
            if line.strip() == "tservers:":
                tservers_indent = indent
            continue
        key_match = re.match(r"\s*([A-Za-z0-9-]+):\s*$", line)
        count_match = re.match(r"\s*count:\s*(\d+)\s*$", line)
        if count_match and pending:
            counts[pending] = int(count_match.group(1))
            pending = None
        elif key_match:
            pending = key_match.group(1)
    if not counts:
        sys.exit(f"{path}: no 'count:' entries under tservers:")
    return counts


def write_topology(paths, out_path, cluster_config):
    zones, wildcard = collect_zones(paths)
    if not zones:
        sys.exit("no replica_placement blocks found")
    known_regions = {(c, r) for c, r, _ in zones}
    for cloud, region in sorted(wildcard - known_regions):
        sys.exit(f"region {cloud}/{region} appears only as zone '*'; add its zones by hand")
    if cluster_config:
        counts = read_zone_counts(cluster_config)
        missing = [z for _, _, z in zones if z not in counts]
        if missing:
            sys.exit(f"{cluster_config}: no tserver count for {', '.join(missing)}")
        unused = sorted(set(counts) - {z for _, _, z in zones})
        if unused:
            print(f"warning: {cluster_config} zones not in any tablespace: {', '.join(unused)}")
        total = sum(counts[z] for _, _, z in zones)
        shares = [100.0 * counts[z] / total for _, _, z in zones]
        source = f"{cluster_config} ({total} tservers)"
    else:
        shares = [100.0 / len(zones)] * len(zones)
        source = "equal split"
    with open(out_path, "w") as out:
        out.write("# Generated by master_load_bench/prepare.py from:\n")
        for path in paths:
            out.write(f"#   {path}\n")
        out.write(f"# cloud region zone percent   ({source}; adds up to 100)\n")
        for (cloud, region, zone), share in zip(zones, shares):
            out.write(f"{cloud} {region} {zone} {share:.4f}\n")
    print(f"{len(zones)} zones -> {out_path}")


def collect_roles(paths):
    roles = set()
    for path in paths:
        for line in read_lines(path):
            if "rolname" in line:
                roles.update(ROLNAME_RE.findall(line))
            if "OWNER" in line:
                roles.update(OWNER_RE.findall(line))
    roles.discard("yugabyte")
    roles.discard("postgres")
    return sorted(roles)


def write_roles(paths, out_path):
    roles = collect_roles(paths)
    with open(out_path, "w") as out:
        out.write("-- Generated by master_load_bench/prepare.py\n")
        for role in roles:
            out.write(
                "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = "
                f"'{role}') THEN CREATE ROLE \"{role}\"; END IF; END $$;\n")
    print(f"{len(roles)} roles -> {out_path}")


def install_tsearch_rules(paths, build_dir, rules_dir):
    tsearch = os.path.join(build_dir, "postgres", "share", "tsearch_data")
    if not os.path.isdir(tsearch):
        sys.exit(f"{tsearch} not found; build first or pass --build-dir")
    needed = []
    for path in paths:
        for line in read_lines(path):
            for name in RULES_RE.findall(line):
                if name not in needed:
                    needed.append(name)
    missing = []
    for name in needed:
        dest = os.path.join(tsearch, name + ".rules")
        if os.path.exists(dest):
            continue
        src = os.path.join(rules_dir, name + ".rules") if rules_dir else None
        if src and os.path.exists(src):
            shutil.copyfile(src, dest)
            print(f"{src} -> {dest}")
        else:
            missing.append(name + ".rules")
    if missing:
        sys.exit(f"rules files not in {tsearch}: {', '.join(missing)}; "
                 "pass --globaldb-repo to copy them in")


def rewrite_file(path, out_path, args):
    num, prod = args.scale_splits if args.scale_splits else (0, 0)
    dropping_stats = False
    counts = {"stats": 0, "splits": 0, "search_paths": 0}
    pending_search_path = None  # set after a CREATE FUNCTION line, emitted after its LANGUAGE line
    with open_out(out_path) as out:
        for line in read_lines(path):
            if dropping_stats:
                if line.strip() == ");":
                    dropping_stats = False
                continue
            if args.drop_stats and line.startswith(STATS_PREFIX):
                dropping_stats = True
                counts["stats"] += 1
                continue
            if num and "SPLIT INTO" in line:
                def scale(m):
                    n = int(m.group(1))
                    return f"SPLIT INTO {max(1, round(n * num / prod))} TABLETS"
                line, n = SPLIT_RE.subn(scale, line)
                counts["splits"] += n
            if line.startswith("CREATE FUNCTION "):
                signature = line[len("CREATE FUNCTION "):].split(" RETURNS")[0]
                pending_search_path = FUNCTION_SEARCH_PATHS.get(signature)
            elif pending_search_path and line.startswith("    LANGUAGE "):
                line += f"\n    SET search_path TO {pending_search_path}"
                counts["search_paths"] += 1
                pending_search_path = None
            out.write(line + "\n")
    return counts


def parse_ratio(text):
    try:
        num, prod = (int(x) for x in text.split(":"))
    except ValueError:
        raise argparse.ArgumentTypeError("expected N:PROD, e.g. 100:154")
    if num <= 0 or prod <= 0:
        raise argparse.ArgumentTypeError("both counts must be positive")
    return num, prod


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--globaldb-repo", metavar="DIR",
                        help="globaldb-yugabyte checkout: its production cluster config sets "
                             "the zone percentages and its node files supply missing rules files")
    parser.add_argument("--build-dir", default=os.path.join(REPO_ROOT, "build", "latest"),
                        help="YugabyteDB build whose postgres/share/tsearch_data gets the rules "
                             "files (default: %(default)s)")
    parser.add_argument("--drop-stats", action="store_true",
                        help="remove pg_restore_*_stats calls")
    parser.add_argument("--scale-splits", type=parse_ratio, metavar="N:PROD",
                        help="scale SPLIT INTO counts by N/PROD tservers (min 1)")
    parser.add_argument("files", nargs="+", help="tablespaces dump, then database dumps")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    topology = os.path.join(args.out_dir, "topology.txt")
    roles = os.path.join(args.out_dir, "roles.sql")
    cluster_config = rules_dir = None
    if args.globaldb_repo:
        cluster_config = os.path.join(args.globaldb_repo, GLOBALDB_CLUSTER_CONFIG)
        rules_dir = os.path.join(args.globaldb_repo, GLOBALDB_NODE_FILES)
        for path in (cluster_config, rules_dir):
            if not os.path.exists(path):
                sys.exit(f"{path} not found; is --globaldb-repo a globaldb-yugabyte checkout?")
    write_topology(args.files, topology, cluster_config)
    write_roles(args.files, roles)
    install_tsearch_rules(args.files, args.build_dir, rules_dir)
    outputs = [roles]
    for path in args.files:
        out_path = os.path.join(args.out_dir, os.path.basename(path))
        if os.path.abspath(out_path) == os.path.abspath(path):
            sys.exit(f"{path} would overwrite itself; use another --out-dir")
        counts = rewrite_file(path, out_path, args)
        outputs.append(out_path)
        print(f"{path} -> {out_path}: dropped {counts['stats']} stats calls, scaled "
              f"{counts['splits']} SPLIT INTO, set {counts['search_paths']} function search_paths")
    print()
    print(f"--topology={topology} --sql_file={','.join(outputs)}")


if __name__ == "__main__":
    main()
