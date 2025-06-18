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

     
def posix_op_time_pure_bw(intervals, ranks, metricObj: MetricObject):

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


def posix_meta_time_e2e_bw(intervals, ranks, metricObj: MetricObject):
    write_intervals = filter_intervals(intervals, ["write"])
    read_intervals  = filter_intervals(intervals, ["read"])
    open_intervals  = filter_intervals(intervals, ["open"])
    close_intervals = filter_intervals(intervals, ["close"])
    seek_intervals  = filter_intervals(intervals, ["seek"])
    sync_intervals  = filter_intervals(intervals, ["sync"])
    # pro Rank schauen!
    for filename in intervals:
        meta_w_times  = [0.0] * ranks
        open_w_times  = [0.0] * ranks
        close_w_times = [0.0] * ranks
        meta_r_times  = [0.0] * ranks
        open_r_times  = [0.0] * ranks
        close_r_times = [0.0] * ranks
        for rank in ranks:
            writes = sorted([x for x in write_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            reads  = sorted([x for x in read_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            opens  = sorted([x for x in open_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            closes = sorted([x for x in close_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            seeks  = sorted([x for x in seek_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            syncs  = sorted([x for x in sync_intervals[filename] if x[0] == rank], key=lambda x: x.tstart)
            
            write_metaops = assign_metaops(writes, opens, closes, seeks, syncs, True)
            read_metaops = assign_metaops(reads, opens, closes, seeks, syncs, False)

            


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


def e2e_file_metrics(reader, metrics: MetricObject):

    posix_meta_write_records, posix_meta_read_records, posix_open_records, posix_close_records = file_open_close_records(reader, True)
    mpiio_meta_write_records, mpiio_meta_read_records, mpiio_open_records, mpiio_close_records = file_open_close_records(reader, False)

    for filename in metrics.files_bytes_written:
        metrics.files_posix_e2e_write_bw[filename] = 0
        metrics.files_posix_e2e_read_bw[filename] = 0
        metrics.files_mpiio_e2e_write_bw[filename] = 0
        metrics.files_mpiio_e2e_read_bw[filename] = 0

        sum_posix_meta_write_time = 0
        sum_posix_meta_read_time = 0
        sum_posix_open_time = 0
        sum_posix_close_time = 0
        sum_mpiio_meta_write_time = 0
        sum_mpiio_meta_read_time = 0
        sum_mpiio_open_time = 0
        sum_mpiio_close_time = 0

        for record in posix_meta_write_records[filename]:
            sum_posix_meta_write_time = record.tend - record.tstart
        metrics.files_posix_meta_write_time[filename] = sum_posix_meta_write_time
        
        for record in posix_meta_read_records[filename]:
            sum_posix_meta_read_time = record.tend - record.tstart
        metrics.files_posix_meta_read_time[filename] = sum_posix_meta_read_time

        for record in posix_open_records[filename]:
            sum_posix_open_time = record.tend - record.tstart
        metrics.files_posix_open_time[filename] = sum_posix_open_time
        
        for record in posix_close_records[filename]:
            sum_posix_close_time = record.tend - record.tstart
        metrics.files_posix_close_time[filename] = sum_posix_close_time

        for record in mpiio_meta_write_records[filename]:
            sum_mpiio_meta_write_time = record.tend - record.tstart
        metrics.files_mpiio_meta_write_time[filename] = sum_mpiio_meta_write_time
        
        for record in mpiio_meta_read_records[filename]:
            sum_mpiio_meta_read_time = record.tend - record.tstart
        metrics.files_mpiio_meta_read_time[filename] = sum_mpiio_meta_read_time

        for record in mpiio_open_records[filename]:
            sum_mpiio_open_time = record.tend - record.tstart
        metrics.files_mpiio_open_time[filename] = sum_mpiio_open_time
        
        for record in mpiio_close_records[filename]:
            sum_mpiio_close_time = record.tend - record.tstart
        metrics.files_mpiio_close_time[filename] = sum_mpiio_close_time

        
        if sum_posix_meta_write_time != 0:
            e2e_time = metrics.files_posix_write_time.get(filename, 0) + sum_posix_meta_write_time
            metrics.files_posix_e2e_write_bw[filename] = metrics.files_bytes_written[filename] / e2e_time / (1024*1024)

        if sum_posix_meta_read_time != 0:
            e2e_time = metrics.files_posix_read_time.get(filename, 0) + sum_posix_meta_read_time
            metrics.files_posix_e2e_read_bw[filename] = metrics.files_bytes_read[filename] / e2e_time / (1024*1024)

        if sum_mpiio_meta_write_time != 0:
            e2e_time = metrics.files_mpiio_write_time.get(filename, 0) + metrics.files_posix_write_time.get(filename, 0) + sum_mpiio_meta_write_time
            metrics.files_mpiio_e2e_write_bw[filename] = metrics.files_bytes_written[filename] / e2e_time / (1024*1024)

        if sum_mpiio_meta_read_time != 0:
            e2e_time = metrics.files_mpiio_read_time.get(filename, 0) + metrics.files_posix_read_time.get(filename, 0) + sum_mpiio_meta_read_time
            metrics.files_mpiio_e2e_read_bw[filename] = metrics.files_bytes_read[filename] / e2e_time / (1024*1024)
        

# for each rank, gets the tstart of the first open before write / read respectively
# and the tend of the last close after write / read respectively
# only_posix determines, if its the timestamps of posix file open / close or mpi file open / close
# the timestamps are then used to determine which file operations of each rank belong to
# e2e_write_bw / e2e_read_bw
def file_open_close_records(reader, only_posix: bool):
    func_list = reader.funcs
    ranks = reader.GM.total_ranks
    meta_read_records = {}
    meta_write_records = {}
    all_open_records = {}
    all_close_records = {}

    for rank in range(ranks):
        open_records = {}
        close_records = {}
        write_records = {}
        read_records = {}
        other_meta_records = {}
        for i in range(reader.LMs[rank].total_records):
            record = reader.records[rank][i]
            record.rank = rank

            # ignore user functions
            if record.func_id >= len(func_list): continue

            func = func_list[record.func_id]

            # either get only get posix open / close or only mpi open / close
            if only_posix:
                if ignore_funcs(func): continue
            else:
                if not "MPI" in func and not "H5" in func: continue
            
            args = record.args_to_strs()
            filename, record_type = get_record_filename_type(func, args)
            if not filename: continue
            
            if record_type == "close":
                if filename not in close_records:
                    close_records[filename] = []
                close_records[filename].append(record)
            elif record_type == "open":
                if filename not in open_records:
                    open_records[filename] = []
                open_records[filename].append(record)
            elif record_type == "read":
                if filename not in read_records:
                    read_records[filename] = []
                read_records[filename].append(record)
            elif record_type == "write":
                if filename not in write_records:
                    write_records[filename] = []
                write_records[filename].append(record)
            else:
                if filename not in other_meta_records:
                    other_meta_records[filename] = []
                other_meta_records[filename].append(record)
        
        for filename in open_records:
            open_records[filename] = sorted(open_records[filename] , key=lambda x: x.tstart)
            close_records[filename] = sorted(close_records[filename], key=lambda x: x.tstart)
            write_records[filename] = sorted(write_records[filename], key=lambda x: x.tstart)
            read_records[filename] = sorted(read_records[filename] , key=lambda x: x.tstart)

            write_start, write_end, read_start, read_end = get_rank_timestamps(open_records[filename],
                                                                               close_records[filename],
                                                                               write_records[filename],
                                                                               read_records[filename])
            # TODO: this approach only works if there are seperate write / read phases
            # --> is it even possible to clearly assign for each file open / close if it belong to a write / read?
            meta_records = open_records.get(filename, []) + close_records.get(filename, []) + other_meta_records.get(filename, [])
            meta_write = [x for x in meta_records if x.tstart >= write_start and x.tend <= write_end]
            meta_read = [x for x in meta_records if x.tstart >= read_start and x.tend <= read_end]

            meta_write_records[filename] = meta_write_records.get(filename, []) + meta_write
            meta_read_records[filename] = meta_read_records.get(filename, []) + meta_read

            all_open_records[filename] = all_open_records.get(filename, []) + open_records[filename]
            all_close_records[filename] = all_close_records.get(filename, []) + close_records[filename]
    
    return meta_write_records, meta_read_records, all_open_records, all_close_records
        

def get_record_filename_type(func, args):

    if "fwrite" in func:
        return args[3], "write"
    if "fread" in func:
        return args[3], "read"
    if "write" in func:
        return args[0], "write"
    if "read" in func:
        return args[0], "read"
    if "open" in func:
        return args[0], "open"
    if "close" in func:
        return args[0], "close"
    if "sync" in func or "seek" in func:
        return args[0], "other"
    return "", ""


def get_rank_timestamps(open, close, write, read):
    last_write = write[-1]
    first_read = read[0]

    first_write_open = open[0]
    last_write_close = next(filter(lambda x: x.tstart > last_write.tend and x.tend < first_read.tstart, close), close[0])

    tmp_opens = [x for x in open if x.tstart > last_write_close.tend and x.tend < first_read.tstart]

    first_read_open = max(tmp_opens, key=lambda x: x.tstart, default=open[-1])
    last_read_close = close[-1]

    return first_write_open.tstart, last_write_close.tend, first_read_open.tstart, last_read_close.tend


def print_metrics(reader, output_path):
    metrics = MetricObject()
    # TODO: maybe change back 
    posix_intervals = build_intervals(reader, True)
    mpiio_intervals = build_intervals(reader, False)
    ranks = reader.GM.total_ranks
    posix_op_time_pure_bw(posix_intervals, ranks, metrics)
    mpiio_op_time_pure_bw(mpiio_intervals, ranks, metrics)
    #e2e_file_metrics(reader, metrics)

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
