#!/usr/bin/env python
# encoding: utf-8
from __future__ import absolute_import
from .creader_wrapper import RecorderReader
from .build_intervals import *
from .metrics import MetricObject
from datetime import datetime
from bisect import bisect_left, bisect_right


def filter_intervals(intervals, op):
    return {
        filename: [interval for interval in file_intervals if interval[3] == op]
        for filename, file_intervals in intervals.items()
    }


def get_duration_sum(intervals):
    duration_sum = 0.0
    for interval in intervals:
        duration_sum += interval[2] - interval[1]
    return duration_sum


def assign_metaops(intervals, ioop, metaops_strs):

    def get_last_before(ioop, metaops, starts, is_fcntl: bool):
        if not metaops: return []
        
        op_start = ioop[1]
        # bisect_left returns index of first starts element e with e >= op_start
        # since realistically no two timestamps are the same, the relevant index
        # is always one before e
        pos = bisect_left(starts, op_start) - 1

        if pos < 0:
            return []

        # fcntl sometimes starts before pwrite / pread but ends after
        # to avoid unassigned fcntl calls, only look at start of fcntl
        if is_fcntl:
            return metaops[pos]

        while(pos > 0 and metaops[pos][2] >= op_start):
            pos -= 1
        
        if pos > 0:
            return metaops[pos]
        elif pos == 0:
            if metaops[pos][2] < op_start:
                return metaops[pos]
        return []

    
    def get_first_after(ioop, metaops, starts, is_fcntl: bool):
        if not metaops: return []
        # in some cases fcntl starts after pwrite / pread started but before pwrite / pread ended
        end = ioop[1] if is_fcntl else ioop[2]
        pos = bisect_right(starts, end)
        if pos >= len(starts):
            return []
        return metaops[pos]


    def add_metaop(metaop, assigned_metaops, mop_key):
        if metaop and metaop not in assigned_metaops[mop_key]:
            assigned_metaops[mop_key].append(metaop)


    assigned_mops = {"open": [], "close": [], "other": []}

    starts = {}
    for mop in metaops_strs:
        starts[mop] = [x[1] for x in intervals[mop]]

    for op in intervals[ioop]:

        for mop in ("open", "seek", "fcntl"):
            last_before = get_last_before(op, intervals[mop], starts[mop], mop == "fcntl")
            mop_key = mop if mop == "open" else "other"
            add_metaop(last_before, assigned_mops, mop_key)
        
        for mop in ("close", "fcntl"):
            first_after = get_first_after(op, intervals[mop], starts[mop], mop == "fcntl")
            mop_key = mop if mop == "close" else "other"
            add_metaop(first_after, assigned_mops, mop_key)

        if ioop == "write":
            first_sync = get_first_after(op, intervals["sync"], starts["sync"], False)
            add_metaop(first_sync, assigned_mops, "other")

            for mop in ("set_size", "ftruncate"):
                last_before = get_last_before(op, intervals[mop], starts[mop], False)
                add_metaop(last_before, assigned_mops, "other")

                if last_before:
                    last_open = get_last_before(last_before, intervals["open"], starts["open"], False)
                    add_metaop(last_open, assigned_mops, "open")
                    first_close = get_first_after(last_before, intervals["close"], starts["close"], False)
                    add_metaop(first_close, assigned_mops, "close")

    return assigned_mops


def get_file_bytes(intervals, byte_dict, posix: bool):
    level = "posix" if posix else "mpiio"

    for filename in intervals:
        if filename not in byte_dict:
            byte_dict[filename] = {
                "write": {
                    "posix": 0.0,
                    "mpiio": 0.0},
                "read": {
                    "posix": 0.0,
                    "mpiio": 0.0}
                }
        sum_write_size = 0
        sum_read_size = 0

        for interval in intervals[filename]:
            operation, io_size = interval[3], interval[4]

            if operation == "read":
                sum_read_size  += io_size
            elif operation == "write":
                sum_write_size += io_size

        byte_dict[filename]["write"][level] = sum_write_size
        byte_dict[filename]["read"][level] = sum_read_size


def set_byte_counts(file_bytes, metricObj: MetricObject):
    
    def get_max_bytes(op_dict):
        return max(op_dict["posix"], op_dict["mpiio"])
    
    total_write_bytes = 0
    total_read_bytes = 0

    for filename in file_bytes:
        max_write_bytes = get_max_bytes(file_bytes[filename]["write"])
        max_read_bytes = get_max_bytes(file_bytes[filename]["read"])

        total_write_bytes += max_write_bytes
        total_read_bytes += max_read_bytes

        if filename not in metricObj.metrics: metricObj.add_filename(filename)

        metricObj.metrics[filename]["write"]["bytes"] = max_write_bytes
        metricObj.metrics[filename]["read"]["bytes"] = max_read_bytes

    metricObj.metrics["overall"]["write"]["total_bytes"] = total_write_bytes
    metricObj.metrics["overall"]["read"]["total_bytes"] = total_read_bytes

     
def op_time_pure_bw(intervals, ranks, metricObj: MetricObject, posix: bool):
    op_time_key = "posix_op_time" if posix else "mpiio_op_time"
    pure_bw_key = "posix_pure_bw" if posix else "mpiio_pure_bw"
    files_pure_times = {}
    
    for filename in intervals:

        files_pure_times[filename] = {}
        write_times = [0.0] * ranks
        read_times = [0.0] * ranks

        # aggregate write / read durations for each rank seperately
        # so that only the maximum aggregate duration gets used for bw
        for interval in intervals[filename]:
            rank, operation = interval[0], interval[3]
            duration = float(interval[2]) - float(interval[1])

            if operation == "read":
                read_times[rank]  += duration
            elif operation == "write":
                write_times[rank] += duration

        files_pure_times[filename]["write"] = write_times
        files_pure_times[filename]["read"] = read_times
        
        max_read_time = max(read_times)
        max_write_time = max(write_times)

        # bandwidth has MiB/s as unit
        if max_read_time != 0:
            metricObj.metrics[filename]['read'][op_time_key] = max_read_time
            metricObj.metrics[filename]['read'][pure_bw_key] = metricObj.metrics[filename]['read']['bytes'] / max_read_time / (1024*1024)

        if max_write_time != 0:
            metricObj.metrics[filename]['write'][op_time_key] = max_write_time
            metricObj.metrics[filename]['write'][pure_bw_key] = metricObj.metrics[filename]['write']['bytes'] / max_write_time / (1024*1024)
        
    return files_pure_times


def meta_time_e2e_bw(intervals, ranks, metricObj: MetricObject, files_pure_times, posix: bool):
    
    def debug_not_assigned(mop_list, mop_type, metaops, rank):
        if mop_list:   
            for x in mop_list:
                if x not in metaops["write"][mop_type] and x not in metaops["read"][mop_type]:
                    print(f"Rank {rank} unassigned MOp: {x}")
    

    filtered_intervals = {}
    op_strs = ("write", "read", "open", "close", "seek", "sync", "set_size", "ftruncate", "fcntl")
    for op in op_strs:
        filtered_intervals[op] = filter_intervals(intervals, op)

    meta_time_key = "posix_meta_time" if posix else "mpiio_meta_time"
    e2e_bw_key = "posix_e2e_bw" if posix else "mpiio_e2e_bw"

    for filename in intervals:

        file_times = {"write": {}, "read": {}}
        for op in ("write", "read"):
            file_times[op]["pure"] = files_pure_times[filename][op]
            for time in ("open", "close", "all_meta", "e2e"):
                file_times[op][time] = [0.0] * ranks
        
        for rank in range(ranks):
            # get only intervals of current rank and sort them by tstart
            rank_intervals = {}
            for op in op_strs:
                rank_intervals[op] = sorted([x for x in filtered_intervals[op][filename] if x[0] == rank], key=lambda x: x[1])

            metaops_strs = ("open", "close", "seek", "sync", "set_size", "ftruncate", "fcntl")

            metaops = {"write": {}, "read": {}}
            for op in ("write", "read"):
                metaops[op] = assign_metaops(rank_intervals, op, metaops_strs)
                file_times[op]["open"][rank] = get_duration_sum(metaops[op]["open"])
                file_times[op]["close"][rank] = get_duration_sum(metaops[op]["close"])
                file_times[op]["all_meta"][rank] = get_duration_sum(metaops[op]["other"]) + file_times[op]["open"][rank] + file_times[op]["close"][rank]
                file_times[op]["e2e"][rank] = file_times[op]["pure"][rank] + file_times[op]["all_meta"][rank]

            # debug, to print metaop intervals that were not assigned
            #for mop_type in metaops_strs:
            #    metaops_type = mop_type if mop_type == "open" or mop_type == "close" else "other"
            #    debug_not_assigned(rank_intervals[mop_type], metaops_type, metaops, rank)

        max_e2e_write = max(file_times["write"]["e2e"])
        max_e2e_read = max(file_times["read"]["e2e"])
        bytes_written = metricObj.metrics[filename]['write']['bytes']
        bytes_read = metricObj.metrics[filename]['read']['bytes']

        if filename not in metricObj.metrics: metricObj.add_filename(filename)

        if max_e2e_write != 0 and bytes_written != 0:
            metricObj.metrics[filename]['write'][meta_time_key] = max_e2e_write
            metricObj.metrics[filename]['write'][e2e_bw_key] = bytes_written / max_e2e_write / (1024 * 1024)

        if max_e2e_read != 0 and bytes_read != 0:
            metricObj.metrics[filename]['read'][meta_time_key] = max_e2e_read
            metricObj.metrics[filename]['read'][e2e_bw_key] = bytes_read / max_e2e_read / (1024 * 1024)


def aggregate_metrics(metricObj: MetricObject, write: bool):

    def set_agg_metrics(metricObj: MetricObject, op_key, file_metrics, posix: bool):
        total_bytes = metricObj.metrics['overall'][op_key]['total_bytes']
        level = "posix" if posix else "mpiio"
    
        max_op_time = max(x[level + "_op_time"] for x in file_metrics)
        max_meta_time = max(x[level + "_meta_time"] for x in file_metrics)
        metricObj.metrics['overall'][op_key]["max_" + level + "_op_time"] = max_op_time
        metricObj.metrics['overall'][op_key]["max_" + level + "_meta_time"] = max_meta_time
        if max_op_time != 0:
            metricObj.metrics['overall'][op_key]["agg_" + level + "_pure_bw"] = total_bytes / max_op_time / (1024*1024)  # MiB/s
        if max_meta_time != 0:
            metricObj.metrics['overall'][op_key]["agg_" + level + "_e2e_bw"] = total_bytes / max_meta_time / (1024*1024) # MiB/s
        if len(file_metrics) != 0:
            metricObj.metrics['overall'][op_key]["avg_" + level + "_pure_bw"] = sum(x[level + "_pure_bw"] for x in file_metrics) / len(file_metrics)
            metricObj.metrics['overall'][op_key]["avg_" + level + "_e2e_bw"] = sum(x[level + "_e2e_bw"] for x in file_metrics) / len(file_metrics)

    op_key = "write" if write else "read"
    file_metrics = [metricObj.metrics[x][op_key] for x in metricObj.metrics if not ignore_filename(x, metricObj)]
    set_agg_metrics(metricObj, op_key, file_metrics, True)
    set_agg_metrics(metricObj, op_key, file_metrics, False)


def print_overall_operation(file, op):
    max_text_len = 49
    decimals = 17
    max_val_len = max(len(str(int(op[x]))) for x in op if x != 'total_bytes') + decimals + 1

    file.write(f"\tTotal Bytes: {op['total_bytes']} \n")
    file.write(f"\tPOSIX Level Metrics:\n")
    file.write(f"\t\t{'Max Pure Operation Time (s)':<{max_text_len}}: {op['max_posix_op_time']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'Pure Operation Bandwidth with Max Op Time (MiB/s)':<{max_text_len}}: {op['agg_posix_pure_bw']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'Pure Operation Bandwidth as File BW Avg (MiB/s)':<{max_text_len}}: {op['avg_posix_pure_bw']:>{max_val_len}.{decimals}f} \n\n")
    file.write(f"\t\t{'Max E2E Operation Time (s)':<{max_text_len}}: {op['max_posix_meta_time']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'E2E Operation Bandwidth with Max Op Time (MiB/s)':<{max_text_len}}: {op['agg_posix_e2e_bw']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'E2E Operation Bandwidth as File BW Avg (MiB/s)':<{max_text_len}}: {op['avg_posix_e2e_bw']:>{max_val_len}.{decimals}f} \n\n")
    file.write(f"\tMPIIO Level Metrics:\n")
    file.write(f"\t\t{'Max Pure Operation Time (s)':<{max_text_len}}: {op['max_mpiio_op_time']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'Pure Operation Bandwidth with Max Op Time (MiB/s)':<{max_text_len}}: {op['agg_mpiio_pure_bw']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'Pure Operation Bandwidth as File BW Avg (MiB/s)':<{max_text_len}}: {op['avg_mpiio_pure_bw']:>{max_val_len}.{decimals}f} \n\n")
    file.write(f"\t\t{'Max E2E Operation Time (s)':<{max_text_len}}: {op['max_mpiio_meta_time']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'E2E Operation Bandwidth with Max Op Time (MiB/s)':<{max_text_len}}: {op['agg_mpiio_e2e_bw']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'E2E Operation Bandwidth as File BW Avg (MiB/s)':<{max_text_len}}: {op['avg_mpiio_e2e_bw']:>{max_val_len}.{decimals}f} \n\n")


def print_file_operation(file, op):
    max_text_len = 32
    decimals = 17
    max_val_len = max(len(str(int(op[x]))) for x in op if x != 'bytes') + decimals + 1
    
    file.write(f"\tBytes: {op['bytes']} \n")
    file.write(f"\tPOSIX Level Metrics:\n")
    file.write(f"\t\t{'Pure Operation Time (s)':<{max_text_len}}: {op['posix_op_time']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'Pure Operation Bandwidth (MiB/s)':<{max_text_len}}: {op['posix_pure_bw']:>{max_val_len}.{decimals}f} \n\n")
    file.write(f"\t\t{'E2E Operation Time (s)':<{max_text_len}}: {op['posix_meta_time']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'E2E Operation Bandwidth (MiB/s)':<{max_text_len}}: {op['posix_e2e_bw']:>{max_val_len}.{decimals}f} \n\n")
    file.write(f"\tMPIIO Level Metrics:\n")
    file.write(f"\t\t{'Pure Operation Time (s)':<{max_text_len}}: {op['mpiio_op_time']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'Pure Operation Bandwidth (MiB/s)':<{max_text_len}}: {op['mpiio_pure_bw']:>{max_val_len}.{decimals}f} \n\n")
    file.write(f"\t\t{'E2E Operation Time (s)':<{max_text_len}}: {op['mpiio_meta_time']:>{max_val_len}.{decimals}f} \n")
    file.write(f"\t\t{'E2E Operation Bandwidth (MiB/s)':<{max_text_len}}: {op['mpiio_e2e_bw']:>{max_val_len}.{decimals}f} \n\n")


def ignore_filename(filename, metricObj: MetricObject):
    if filename == "overall":
        return True
    return all(x == 0 for x in metricObj.metrics[filename]['write'].values()) and all(x == 0 for x in metricObj.metrics[filename]['read'].values())


def print_metrics(reader, output_path):
    start = datetime.now()
    metrics = MetricObject(reader)
    ranks = reader.GM.total_ranks

    posix_intervals = build_intervals(reader, True)
    mpiio_intervals = build_intervals(reader, False)

    file_bytes = {}
    get_file_bytes(posix_intervals, file_bytes, True)
    get_file_bytes(mpiio_intervals, file_bytes, False)
    set_byte_counts(file_bytes, metrics)

    posix_pure_times = op_time_pure_bw(posix_intervals, ranks, metrics, True)
    meta_time_e2e_bw(posix_intervals, ranks, metrics, posix_pure_times, True)

    mpiio_pure_times = op_time_pure_bw(mpiio_intervals, ranks, metrics, False)
    meta_time_e2e_bw(mpiio_intervals, ranks, metrics, mpiio_pure_times, False)

    aggregate_metrics(metrics, True)
    aggregate_metrics(metrics, False)

    
    with open(output_path, "w") as f:
        f.write(f"{'=' * 50}\n")
        f.write(f"Overall Metrics:\n")
        f.write(f"{'=' * 50}\n")
        f.write("Write:\n")
        print_overall_operation(f, metrics.metrics['overall']['write'])
        f.write("Read:\n")
        print_overall_operation(f, metrics.metrics['overall']['read'])

        f.write(f"\n{'=' * 50}\n")
        f.write(f"Per File Metrics: \n")
        f.write(f"{'=' * 50}")
        for filename in metrics.metrics:
            if ignore_filename(filename, metrics): continue
            f.write(f"\n{'-' * (len(filename) + 6)}\n")
            f.write(f"File: {filename}\n")
            f.write(f"Write:\n")
            print_file_operation(f, metrics.metrics[filename]['write'])
            f.write(f"Read:\n")
            print_file_operation(f, metrics.metrics[filename]['read'])
    stop = datetime.now()
    duration = stop - start
    print(f"[recorder-pm]: Total processing time: {duration}")

        

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
