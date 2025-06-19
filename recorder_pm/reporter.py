#!/usr/bin/env python
# encoding: utf-8
from __future__ import absolute_import
import math, os
import numpy as np


from .creader_wrapper import RecorderReader
from .html_writer import HTMLWriter
from .build_offset_intervals import *
from .metrics import MetricObject



# For local test
"""
from creader_wrapper import RecorderReader
from html_writer import HTMLWriter
from build_offset_intervals import ignore_files
from build_offset_intervals import build_offset_intervals
"""
def filter_intervals(intervals, operations: list[str]):
    return_intervals = {}
    for filename in intervals:
        return_intervals[filename] = []
        for interval in intervals[filename]:
            for op in operations:
                if op == interval[3]:
                    return_intervals[filename].append(interval)
    return return_intervals


def get_duration_sum(intervals):
    duration_sum = 0.0
    for interval in intervals:
        duration_sum += interval[2] - interval[1]
    return duration_sum


def assign_metaops(ioops, opens, closes, seeks, syncs, writeOps: bool):
    
    def get_last_before(ioop, metaops):
        start = ioop[1]
        before_ops = [x for x in metaops if x[2] < start]
        return max(before_ops, key=lambda x: x[1], default=[])
    

    def get_first_after(ioop, metaops):
        end = ioop[2]
        after_ops = [x for x in metaops if x[1] > end]
        return min(after_ops, key=lambda x: x[1], default=[])


    assigned_opens = []
    assigned_closes = []
    assigned_other = []
    for op in ioops:
        last_open = get_last_before(op, opens)
        last_seek = get_last_before(op, seeks)
        first_close = get_first_after(op, closes)

        if last_open and last_open not in assigned_opens:
            assigned_opens.append(last_open)
        if last_seek and last_seek not in assigned_other:
            assigned_other.append(last_seek)
        if first_close and first_close not in assigned_closes:
            assigned_closes.append(first_close)
        if writeOps:
            first_sync = get_first_after(op, syncs)
            if first_sync and first_sync not in assigned_other:
                assigned_other.append(first_sync)

    return {"open": assigned_opens, "close": assigned_closes, "other": assigned_other}

     
def posix_op_time_pure_bw(intervals, ranks, metricObj: MetricObject):
    total_write_size = 0
    total_read_size = 0
    for filename in intervals:
        if filename not in metricObj.unique_files: continue

        sum_write_size = 0
        write_times = [0.0] * ranks
        sum_read_size = 0
        read_times = [0.0] * ranks

        # aggregate all bytes written in file filename
        # aggregate write / read durations for each rank seperately
        # so that only the maximum aggregate duration gets used for bw
        for interval in intervals[filename]:
            rank, io_size , operation = interval[0], interval[4], interval[3]
            duration = float(interval[2]) - float(interval[1])

            if operation == "read":
                sum_read_size  += io_size
                read_times[rank]  += duration
            elif operation == "write":
                sum_write_size += io_size
                write_times[rank] += duration

        total_write_size += sum_write_size
        total_read_size += sum_read_size
        
        # bandwidth has MiB/s as unit
        max_read_time = max(read_times)
        max_write_time = max(write_times)
        if operation == "read":
            if sum_read_size == 0 or max_read_time == 0: continue

            metricObj.metrics["read"]["bytes_per_file"][filename] = sum_read_size
            metricObj.metrics["read"]["posix_op_time"][filename] = max_read_time
            metricObj.metrics["read"]["posix_pure_bw"][filename] = sum_read_size / max_read_time / (1024*1024)
        elif operation == "write":
            if sum_write_size == 0 or max_write_time == 0: continue

            metricObj.metrics["write"]["bytes_per_file"][filename] = sum_write_size
            metricObj.metrics["write"]["posix_op_time"][filename] = max_write_time
            metricObj.metrics["write"]["posix_pure_bw"][filename] = sum_write_size / max_write_time / (1024*1024)

    metricObj.metrics["write"]["bytes_total"] = total_write_size
    metricObj.metrics["read"]["bytes_total"] = total_read_size
        
    return write_times, read_times


def posix_meta_time_e2e_bw(intervals, ranks, metricObj: MetricObject, write_times, read_times):
    write_intervals = filter_intervals(intervals, ["write"])
    read_intervals  = filter_intervals(intervals, ["read"])
    open_intervals  = filter_intervals(intervals, ["open"])
    close_intervals = filter_intervals(intervals, ["close"])
    seek_intervals  = filter_intervals(intervals, ["seek"])
    sync_intervals  = filter_intervals(intervals, ["sync"])

    for filename in intervals:
        if filename not in metricObj.unique_files: continue
        meta_w_times  = [0.0] * ranks
        open_w_times  = [0.0] * ranks
        close_w_times = [0.0] * ranks
        e2e_w_times   = [0.0] * ranks

        meta_r_times  = [0.0] * ranks
        open_r_times  = [0.0] * ranks
        close_r_times = [0.0] * ranks
        e2e_r_times   = [0.0] * ranks
        for rank in ranks:
            # the meta time gets partitioned more than necessary for the overall meta time
            # however for possible future metrics (i.e. max open / close time) it stays that way for now
            writes = sorted([x for x in write_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            reads  = sorted([x for x in read_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            opens  = sorted([x for x in open_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            closes = sorted([x for x in close_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            seeks  = sorted([x for x in seek_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            syncs  = sorted([x for x in sync_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            
            write_metaops = assign_metaops(writes, opens, closes, seeks, syncs, True)
            read_metaops = assign_metaops(reads, opens, closes, seeks, syncs, False)

            open_w_times[rank]  = get_duration_sum(write_metaops["open"])
            close_w_times[rank] = get_duration_sum(write_metaops["close"])
            meta_w_times[rank]  = get_duration_sum(write_metaops["other"]) + open_w_times[rank] + close_w_times[rank]
            e2e_w_times[rank]   = write_times[rank] + meta_w_times[rank]

            open_r_times[rank]  = get_duration_sum(read_metaops["open"])
            close_r_times[rank] = get_duration_sum(read_metaops["close"])
            meta_r_times[rank]  = get_duration_sum(read_metaops["other"]) + open_r_times[rank] + close_r_times[rank]
            e2e_r_times[rank]   = read_times[rank] + meta_r_times[rank]

        max_e2e_write = max(e2e_w_times)
        max_e2e_read = max(e2e_r_times)
        bytes_written = metricObj.metrics["write"]["bytes_per_file"][filename]
        bytes_read = metricObj.metrics["read"]["bytes_per_file"][filename]

        if max_e2e_write != 0 and bytes_written != 0:
            metricObj.metrics["write"]["posix_meta_time"][filename] = max_e2e_write
            metricObj.metrics["write"]["posix_e2e_bw"][filename] = bytes_written / max_e2e_write / (1024 * 1024)
        else:
            metricObj.metrics["write"]["posix_meta_time"][filename] = 0.0
            metricObj.metrics["write"]["posix_e2e_bw"][filename] = 0.0

        if max_e2e_read != 0 and bytes_read != 0:
            metricObj.metrics["read"]["posix_meta_time"][filename] = max_e2e_read
            metricObj.metrics["read"]["posix_e2e_bw"][filename] = bytes_written / max_e2e_read / (1024 * 1024)
        else:
            metricObj.metrics["read"]["posix_meta_time"][filename] = 0.0
            metricObj.metrics["read"]["posix_e2e_bw"][filename] = 0.0


def mpiio_op_time_pure_bw(intervals, ranks, metricObj: MetricObject):
    
    for filename in intervals:
        if filename not in metricObj.unique_files: continue

        write_times = [0.0] * ranks
        read_times = [0.0] * ranks

        # aggregate all bytes written in file filename
        # aggregate write / read durations for each rank seperately
        # so that only the maximum aggregate duration gets used for bw
        for interval in intervals[filename]:
            rank, operation = interval[0], interval[3]
            duration = float(interval[2]) - float(interval[1])

            if operation == "read":
                read_times[rank]  += duration
            elif operation == "write":
                write_times[rank] += duration
        
        # bandwidth has MiB/s as unit
        max_read_time = max(read_times)
        max_write_time = max(write_times)
        if operation == "read":
            if max_read_time == 0: continue

            read_size = metricObj.metrics["read"]["bytes_per_file"][filename]
            metricObj.metrics["read"]["mpiio_op_time"][filename] = max_read_time
            metricObj.metrics["read"]["mpiio_pure_bw"][filename] = read_size / max_read_time / (1024*1024)
        elif operation == "write":
            if max_write_time == 0: continue

            write_size = metricObj.metrics["write"]["bytes_per_file"][filename]
            metricObj.metrics["write"]["mpiio_op_time"][filename] = max_write_time
            metricObj.metrics["write"]["mpiio_pure_bw"][filename] = write_size / max_write_time / (1024*1024)

    return write_times, read_times


def mpiio_meta_time_e2e_bw(intervals, ranks, metricObj: MetricObject, write_times, read_times):
    write_intervals = filter_intervals(intervals, ["write"])
    read_intervals  = filter_intervals(intervals, ["read"])
    open_intervals  = filter_intervals(intervals, ["open"])
    close_intervals = filter_intervals(intervals, ["close"])

    for filename in intervals:
        if filename not in metricObj.unique_files: continue
        meta_w_times  = [0.0] * ranks
        open_w_times  = [0.0] * ranks
        close_w_times = [0.0] * ranks
        e2e_w_times   = [0.0] * ranks

        meta_r_times  = [0.0] * ranks
        open_r_times  = [0.0] * ranks
        close_r_times = [0.0] * ranks
        e2e_r_times   = [0.0] * ranks
        for rank in ranks:
            # the meta time gets partitioned more than necessary for the overall meta time
            # however for possible future metrics (i.e. max open / close time) it stays that way for now
            writes = sorted([x for x in write_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            reads  = sorted([x for x in read_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            opens  = sorted([x for x in open_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            closes = sorted([x for x in close_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            
            write_metaops = assign_metaops(writes, opens, closes, [], [], True)
            read_metaops = assign_metaops(reads, opens, closes, [], [], False)

            open_w_times[rank]  = get_duration_sum(write_metaops["open"])
            close_w_times[rank] = get_duration_sum(write_metaops["close"])
            meta_w_times[rank]  = open_w_times[rank] + close_w_times[rank]
            e2e_w_times[rank]   = write_times[rank] + meta_w_times[rank]

            open_r_times[rank]  = get_duration_sum(read_metaops["open"])
            close_r_times[rank] = get_duration_sum(read_metaops["close"])
            meta_r_times[rank]  = open_r_times[rank] + close_r_times[rank]
            e2e_r_times[rank]   = read_times[rank] + meta_r_times[rank]

        max_e2e_write = max(e2e_w_times)
        max_e2e_read = max(e2e_r_times)
        bytes_written = metricObj.metrics["write"]["bytes_per_file"][filename]
        bytes_read = metricObj.metrics["read"]["bytes_per_file"][filename]

        if max_e2e_write != 0 and bytes_written != 0:
            metricObj.metrics["write"]["mpiio_meta_time"][filename] = max_e2e_write
            metricObj.metrics["write"]["mpiio_e2e_bw"][filename] = bytes_written / max_e2e_write / (1024 * 1024)
        else:
            metricObj.metrics["write"]["mpiio_meta_time"][filename] = 0.0
            metricObj.metrics["write"]["mpiio_e2e_bw"][filename] = 0.0

        if max_e2e_read != 0 and bytes_read != 0:
            metricObj.metrics["read"]["mpiio_meta_time"][filename] = max_e2e_read
            metricObj.metrics["read"]["mpiio_e2e_bw"][filename] = bytes_written / max_e2e_read / (1024 * 1024)
        else:
            metricObj.metrics["read"]["mpiio_meta_time"][filename] = 0.0
            metricObj.metrics["read"]["mpiio_e2e_bw"][filename] = 0.0


def print_metrics(reader, output_path):
    metrics = MetricObject()
    ranks = reader.GM.total_ranks

    posix_intervals = build_intervals(reader, True)
    mpiio_intervals = build_intervals(reader, False)

    posix_write_times, posix_read_times = posix_op_time_pure_bw(posix_intervals, ranks, metrics)
    posix_meta_time_e2e_bw(posix_intervals, ranks, metrics, posix_write_times, posix_read_times)

    mpiio_write_times, mpiio_read_times = mpiio_op_time_pure_bw(mpiio_intervals, ranks, metrics)
    mpiio_meta_time_e2e_bw(mpiio_intervals, ranks, metrics, mpiio_write_times, mpiio_read_times)


    with open(output_path, "w") as f:

        f.write("Overall Benchmark MetricObject: \n\n")

        f.write("Write MetricObject:\n")
        f.write(f"Total Bytes:  \n\n")

        f.write(f"POSIX Level: (min / max / avg) \n")
        f.write(f"\tBW:  \n")
        f.write(f"\tE2E BW:  \n")
        f.write(f"\twrite time:  \n")
        f.write(f"\tmetadata operations time:  \n")
        f.write(f"\tfile open time (w & r):  \n")
        f.write(f"\tfile close time (w & r):  \n\n")

        f.write(f"mpiio Level: (min / max / avg) \n")
        f.write(f"\tBW:  \n")
        f.write(f"\tE2E BW:  \n")
        f.write(f"\twrite time:  \n")
        f.write(f"\tmetadata operations time:  \n")
        f.write(f"\tfile open time (w & r):  \n")
        f.write(f"\tfile close time (w & r):  \n\n")
        

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process trace data and generate a report.")
    parser.add_argument(
        "-i", "--input_path",
        required=True,
        type=str,
        help="Path to the trace file to be processed."
    )
    parser.add_argument(
        "-o", "--output_path",
        required=True,
        type=str,
        help="Path to save the generated report."
    )

    args = parser.parse_args()

    reader = RecorderReader(args.input_path)
    print_metrics(reader, args.output_path)
