#!/usr/bin/env python
# encoding: utf-8
from __future__ import absolute_import
from .creader_wrapper import RecorderReader
from .build_intervals import *
from .metrics import MetricObject
from datetime import datetime
from bisect import bisect_left, bisect_right


#def filter_intervals(intervals, op):
#    return_intervals = {}
#    for filename in intervals:
#        return_intervals[filename] = []
#        for interval in intervals[filename]:
#            if op == interval[3]:
#                return_intervals[filename].append(interval)
#    return return_intervals

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


def assign_metaops(ioops, opens, closes, seeks, syncs, set_sizes, writeOps: bool):

    def get_last_before(ioop, metaops, starts):
        if not metaops: return []
        
        start = ioop[1]
        pos = bisect_left(starts, start)
        if pos >= len(starts):
            last_index = len(starts) - 1
            if metaops[last_index][2] < start:
                return metaops[last_index]
            else: return []

        while(pos > 0 and metaops[pos][2] >= start):
            pos -= 1
        
        if pos > 0:
            return metaops[pos]
        elif pos == 0:
            if metaops[pos][2] < start:
                return metaops[pos]
        return []

    
    def get_first_after(ioop, metaops, starts):
        if not metaops: return []
        end = ioop[2]
        pos = bisect_right(starts, end)
        if pos >= len(starts):
            return []
        return metaops[pos]


    assigned_opens = []
    assigned_closes = []
    assigned_other = []

    opens_starts = [x[1] for x in opens]
    closes_starts = [x[1] for x in closes]
    seeks_starts = [x[1] for x in seeks]
    syncs_starts = [x[1] for x in syncs]
    set_sizes_starts = [x[1] for x in set_sizes]

    for op in ioops:
        last_open = get_last_before(op, opens, opens_starts)
        last_seek = get_last_before(op, seeks, seeks_starts)
        first_close = get_first_after(op, closes, closes_starts)

        if last_open and last_open not in assigned_opens:
            assigned_opens.append(last_open)
        if last_seek and last_seek not in assigned_other:
            assigned_other.append(last_seek)
        if first_close and first_close not in assigned_closes:
            assigned_closes.append(first_close)
        if writeOps:
            first_sync = get_first_after(op, syncs, syncs_starts)
            last_set_size = get_last_before(op, set_sizes, set_sizes_starts)

            if first_sync and first_sync not in assigned_other:
                assigned_other.append(first_sync)
            if last_set_size and last_set_size not in assigned_other:
                assigned_other.append(last_set_size)

    return {"open": assigned_opens, "close": assigned_closes, "other": assigned_other}

     
def op_time_pure_bw(intervals, ranks, metricObj: MetricObject, posix: bool):
    total_write_size = 0
    total_read_size = 0
    op_time_key = "posix_op_time" if posix else "mpiio_op_time"
    pure_bw_key = "posix_pure_bw" if posix else "mpiio_pure_bw"
    files_write_times = {}
    files_read_times = {}
    
    for filename in intervals:

        sum_write_size = 0
        write_times = [0.0] * ranks
        sum_read_size = 0
        read_times = [0.0] * ranks

        # aggregate all bytes written in file
        # aggregate write / read durations for each rank seperately
        # so that only the maximum aggregate duration gets used for bw
        for interval in intervals[filename]:
            rank, operation, io_size = interval[0], interval[3], interval[4]
            duration = float(interval[2]) - float(interval[1])

            if operation == "read":
                sum_read_size  += io_size
                read_times[rank]  += duration
            elif operation == "write":
                sum_write_size += io_size
                write_times[rank] += duration

        total_write_size += sum_write_size
        total_read_size += sum_read_size

        files_write_times[filename] = write_times
        files_read_times[filename] = read_times
        
        # bandwidth has MiB/s as unit
        max_read_time = max(read_times)
        max_write_time = max(write_times)

        if filename not in metricObj.metrics: metricObj.add_filename(filename)

        if max_read_time != 0:
            if posix: metricObj.metrics[filename]['read']['bytes'] = sum_read_size
            metricObj.metrics[filename]['read'][op_time_key] = max_read_time
            metricObj.metrics[filename]['read'][pure_bw_key] = metricObj.metrics[filename]['read']['bytes'] / max_read_time / (1024*1024)

        if max_write_time != 0:
            if posix: metricObj.metrics[filename]['write']['bytes'] = sum_write_size
            metricObj.metrics[filename]['write'][op_time_key] = max_write_time
            metricObj.metrics[filename]['write'][pure_bw_key] = metricObj.metrics[filename]['write']['bytes'] / max_write_time / (1024*1024)

    if posix:
        metricObj.metrics['overall']['write']['total_bytes'] = total_write_size
        metricObj.metrics['overall']['read']['total_bytes'] = total_read_size
        
    return files_write_times, files_read_times


def meta_time_e2e_bw(intervals, ranks, metricObj: MetricObject, files_write_times, files_read_times, posix: bool):
    write_intervals = filter_intervals(intervals, 'write')
    read_intervals  = filter_intervals(intervals, 'read')
    open_intervals  = filter_intervals(intervals, 'open')
    close_intervals = filter_intervals(intervals, 'close')
    # these intervals are not necessarily relevant, however in this case filter_intervals just returns an empty list
    seek_intervals  = filter_intervals(intervals, 'seek')
    sync_intervals  = filter_intervals(intervals, 'sync')
    set_size_intervals = filter_intervals(intervals, 'set_size')

    meta_time_key = "posix_meta_time" if posix else "mpiio_meta_time"
    e2e_bw_key = "posix_e2e_bw" if posix else "mpiio_e2e_bw"

    for filename in intervals:

        meta_w_times  = [0.0] * ranks
        open_w_times  = [0.0] * ranks
        close_w_times = [0.0] * ranks
        e2e_w_times   = [0.0] * ranks
        write_times = files_write_times[filename]

        meta_r_times  = [0.0] * ranks
        open_r_times  = [0.0] * ranks
        close_r_times = [0.0] * ranks
        e2e_r_times   = [0.0] * ranks
        read_times = files_read_times[filename]
        
        for rank in range(ranks):
            # the meta time gets partitioned more than necessary for the overall meta time
            # however for possible future metrics (i.e. max open / close time) it stays that way for now
            writes = sorted([x for x in write_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            reads  = sorted([x for x in read_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            opens  = sorted([x for x in open_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            closes = sorted([x for x in close_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            seeks  = sorted([x for x in seek_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            syncs  = sorted([x for x in sync_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            set_sizes = sorted([x for x in set_size_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            
            write_metaops = assign_metaops(writes, opens, closes, seeks, syncs, set_sizes, True)
            read_metaops = assign_metaops(reads, opens, closes, seeks, syncs, set_sizes, False)

            open_w_times[rank]  = get_duration_sum(write_metaops['open'])
            close_w_times[rank] = get_duration_sum(write_metaops['close'])
            meta_w_times[rank]  = get_duration_sum(write_metaops['other']) + open_w_times[rank] + close_w_times[rank]
            e2e_w_times[rank]   = write_times[rank] + meta_w_times[rank]

            open_r_times[rank]  = get_duration_sum(read_metaops['open'])
            close_r_times[rank] = get_duration_sum(read_metaops['close'])
            meta_r_times[rank]  = get_duration_sum(read_metaops['other']) + open_r_times[rank] + close_r_times[rank]
            e2e_r_times[rank]   = read_times[rank] + meta_r_times[rank]

        max_e2e_write = max(e2e_w_times)
        max_e2e_read = max(e2e_r_times)
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
    file_metrics = [metricObj.metrics[x][op_key] for x in metricObj.metrics if x != 'overall']
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

    posix_write_times, posix_read_times = op_time_pure_bw(posix_intervals, ranks, metrics, True)
    meta_time_e2e_bw(posix_intervals, ranks, metrics, posix_write_times, posix_read_times, True)

    mpiio_write_times, mpiio_read_times = op_time_pure_bw(mpiio_intervals, ranks, metrics, False)
    meta_time_e2e_bw(mpiio_intervals, ranks, metrics, mpiio_write_times, mpiio_read_times, False)

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
