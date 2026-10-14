#!/usr/bin/env python3
"""Per-window summary of master-load-bench CSVs.

    report.py run.csv              one run, one column per window between marks
    report.py base.csv new.csv     two runs side by side, with deltas, for windows both have

A window is a stretch of rows with the same phase (the label of the last `mark`). Counters
are reported as rates over the window, latency sums as means, gauges as mean/last.
"""

import argparse
import csv
import statistics
import sys
from collections import OrderedDict, defaultdict


def load(path):
    """phase -> {metric: [(elapsed_sec, value)]}; sql latency rows keep every value.

    The bench appends to an existing CSV, so a file may hold several runs; only the last one is
    reported. A window's counters start from the previous window's last sample, so sends between
    two samples are charged to the window they happened in.
    """
    windows = OrderedDict()
    last = {}
    prev_elapsed = -1.0
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            elapsed = float(row["elapsed_sec"])
            if elapsed < prev_elapsed:
                print("note: %s holds more than one run; reporting the last" % path,
                      file=sys.stderr)
                windows = OrderedDict()
                last = {}
            prev_elapsed = elapsed
            phase = row["phase"]
            metric = row["metric"]
            if metric == "mark":
                windows.setdefault(row["value"], defaultdict(list))
                continue
            try:
                value = float(row["value"])
            except ValueError:
                continue
            series = windows.setdefault(phase, defaultdict(list))[metric]
            if not series and metric in last and not metric.startswith("bench_sql_"):
                series.append(last[metric])
            series.append((elapsed, value))
            last[metric] = (elapsed, value)
    return windows


def delta(series):
    return series[-1][1] - series[0][1] if len(series) > 1 else 0.0


def span(series):
    return series[-1][0] - series[0][0] if len(series) > 1 else 0.0


def summarize(metrics):
    """metric name -> (value, unit) for one window."""
    out = OrderedDict()
    cpu = metrics.get("master_cpu_seconds_total")
    if cpu and span(cpu) > 0:
        out["window_sec"] = (span(cpu), "s")
        out["master_cpu"] = (100.0 * delta(cpu) / span(cpu), "%")
    rss = metrics.get("master_rss_bytes")
    if rss:
        out["master_rss"] = (rss[-1][1] / 2**30, "GiB")
    for name, series in sorted(metrics.items()):
        if name.startswith("bench_") and name.endswith("_ok_total"):
            base = name[len("bench_"):-len("_ok_total")]
            window = span(series)
            if window > 0 and delta(series) > 0:
                out[base + " ok/s"] = (delta(series) / window, "/s")
            failed = metrics.get("bench_%s_failed_total" % base)
            if failed and delta(failed) > 0:
                out[base + " failed"] = (delta(failed), "")
            lat_sum = metrics.get("bench_%s_latency_us_sum" % base)
            lat_count = metrics.get("bench_%s_latency_count" % base)
            if lat_sum and lat_count and delta(lat_count) > 0:
                out[base + " latency"] = (delta(lat_sum) / delta(lat_count) / 1000.0, "ms")
        elif name.startswith("bench_sql_") and name.endswith("_latency_ms"):
            values = [v for _, v in series]
            label = name[len("bench_sql_"):-len("_latency_ms")]
            if len(values) == 1:
                out["sql %s" % label] = (values[0], "ms")
            else:
                out["sql %s n" % label] = (len(values), "")
                out["sql %s mean" % label] = (statistics.fmean(values), "ms")
                out["sql %s p50" % label] = (statistics.median(values), "ms")
                out["sql %s max" % label] = (max(values), "ms")
        elif name.startswith("rpcs_in_queue_"):
            values = [v for _, v in series]
            if any(values):
                out[name + " mean"] = (statistics.fmean(values), "")
                out[name + " max"] = (max(values), "")
        elif name.startswith("rpcs_queue_overflow") or name.startswith("rpcs_timed_out"):
            if delta(series) > 0:
                out[name] = (delta(series), "")
        elif name.startswith("handler_latency_yb_master_") and name.endswith("_sum"):
            count = metrics.get(name[:-len("_sum")] + "_count")
            if count and delta(count) > 0:
                method = name[len("handler_latency_yb_master_"):-len("_sum")].split("_", 1)[-1]
                window = span(series)
                out["master %s /s" % method] = (delta(count) / window, "/s") if window else (0, "/s")
                out["master %s latency" % method] = (delta(series) / delta(count) / 1000.0, "ms")
    return out


def fmt(value, unit):
    if isinstance(value, float) and abs(value) >= 1000:
        text = "%.0f" % value
    elif isinstance(value, float):
        text = "%.2f" % value
    else:
        text = str(value)
    return text + (" " + unit if unit else "")


def row_order(key):
    """Master process, bench senders, master handlers, queues, then sql by name."""
    groups = ["window_sec", "master_cpu", "master_rss", "rpc_", "ysql_connect", "master ",
              "rpcs_", "sql "]
    suffixes = [" ok/s", " /s", " failed", " latency", " n", " mean", " p50", " max"]
    sub = next((j for j, suffix in enumerate(suffixes) if key.endswith(suffix)), -1)
    stem = key[:-len(suffixes[sub])] if sub >= 0 else key
    for i, prefix in enumerate(groups):
        if key.startswith(prefix):
            return (i, stem, sub)
    return (len(groups), stem, sub)


def print_table(rows):
    """First column left-aligned, the rest right-aligned, each as wide as its content."""
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for r in rows:
        cells = ["%-*s" % (widths[0], r[0])] + [
            "%*s" % (widths[i], r[i]) for i in range(1, len(r))]
        print("  ".join(cells).rstrip())


def print_single(windows):
    summaries = OrderedDict((phase, summarize(m)) for phase, m in windows.items() if m)
    rows = sorted({k for s in summaries.values() for k in s}, key=row_order)
    table = [[""] + list(summaries)]
    for key in rows:
        table.append([key] + [fmt(*s[key]) if key in s else "-" for s in summaries.values()])
    print_table(table)


def print_pair(base, new, base_name, new_name):
    base_s = OrderedDict((p, summarize(m)) for p, m in base.items() if m)
    new_s = OrderedDict((p, summarize(m)) for p, m in new.items() if m)
    for phase in base_s:
        if phase not in new_s:
            continue
        print("== %s ==" % phase)
        keys = sorted(set(base_s[phase]) | set(new_s[phase]), key=row_order)
        table = [["", base_name, new_name, "delta"]]
        for key in keys:
            a = base_s[phase].get(key)
            b = new_s[phase].get(key)
            d = ""
            if a and b and isinstance(a[0], (int, float)) and isinstance(b[0], (int, float)):
                d = "%+.2f" % (b[0] - a[0])
                if a[0]:
                    d += " (%+.0f%%)" % (100.0 * (b[0] - a[0]) / a[0])
            table.append([key, fmt(*a) if a else "-", fmt(*b) if b else "-", d])
        print_table(table)
        print()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv", nargs="+", help="one CSV, or base and new")
    args = parser.parse_args()
    if len(args.csv) > 2:
        parser.error("at most two CSVs")
    if len(args.csv) == 1:
        print_single(load(args.csv[0]))
    else:
        print_pair(load(args.csv[0]), load(args.csv[1]), args.csv[0], args.csv[1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
