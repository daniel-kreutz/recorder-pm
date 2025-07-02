#!/usr/bin/env python
# encoding: utf-8
from __future__ import absolute_import
from .creader_wrapper import RecorderReader
from .build_intervals import *
from .metrics import MetricObject
from datetime import datetime
from bisect import bisect_left, bisect_right


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


def assign_metaops(ioops, opens, closes, seeks, syncs, set_sizes, writeOps: bool):
    
    #def get_last_before(ioop, metaops):
    #    if not metaops: return []
    #    start = ioop[1]
    #    for op in reversed(metaops):
    #        if op[1] < start and op[2] < start:
    #            return op
    #    return []
#
    #
    #def get_first_after(ioop, metaops):
    #    if not metaops: return []
    #    end = ioop[2]
    #    for op in metaops:
    #        if op[1] > end:
    #            return op
    #    return []
    

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

    print(f"assign_metaops CALLED for {'WRITE' if writeOps else 'READ'}")
    op_counter = 0
    total_ops = len(ioops)

    #max_start = max(ioops, key=lambda x: x[1], default=0)
    #min_end = min(ioops, key=lambda x: x[2], default=0)

    # filter out non-relevant meta ops
    #opens = [x for x in opens if x[2] <= max_start[1]]
    #closes = [x for x in closes if x[1] >= min_end[2]]
    #seeks = [x for x in seeks if x[2] <= max_start[1]]
    #syncs = [x for x in syncs if x[1] >= min_end[2]]
    #set_sizes = [x for x in set_sizes if x[2] <= max_start[1]]
    opens_starts = [x[1] for x in opens]
    closes_starts = [x[1] for x in closes]
    seeks_starts = [x[1] for x in seeks]
    syncs_starts = [x[1] for x in syncs]
    set_sizes_starts = [x[1] for x in set_sizes]

    for op in ioops:
        op_counter += 1
        for i in range(1, 6):
            if (total_ops // 5) * i == op_counter:
                print(f"current op: {op[3]}\t({op_counter} / {total_ops})")
                break
        

        last_open = get_last_before(op, opens, opens_starts)
        #print("last open done")
        last_seek = get_last_before(op, seeks, seeks_starts)
        #print("last seek done")
        first_close = get_first_after(op, closes, closes_starts)
        #print("first close done")

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
            #print("first sync done")
            if last_set_size and last_set_size not in assigned_other:
                assigned_other.append(last_set_size)
            #print("last set size done")
        #print("assigned all metaops for current op\n")
    print(f"assign_metaops DONE for {'WRITE' if writeOps else 'READ'}")
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
        metricObj.metrics['overall']['write']['bytes_total'] = total_write_size
        metricObj.metrics['overall']['read']['bytes_total'] = total_read_size
        
    return files_write_times, files_read_times


def meta_time_e2e_bw(intervals, ranks, metricObj: MetricObject, files_write_times, files_read_times, posix: bool):
    write_intervals = filter_intervals(intervals, ['write'])
    read_intervals  = filter_intervals(intervals, ['read'])
    open_intervals  = filter_intervals(intervals, ['open'])
    close_intervals = filter_intervals(intervals, ['close'])
    # these intervals are not necessarily relevant, however in this case filter_intervals just returns an empty list
    seek_intervals  = filter_intervals(intervals, ['seek'])
    sync_intervals  = filter_intervals(intervals, ['sync'])
    set_size_intervals = filter_intervals(intervals, ['set_size'])

    meta_time_key = "posix_meta_time" if posix else "mpiio_meta_time"
    e2e_bw_key = "posix_e2e_bw" if posix else "mpiio_e2e_bw"

    file_counter = 0
    total_files = len(intervals)

    for filename in intervals:
        file_counter += 1
        print(f"File {file_counter} / {total_files}")
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
            print(f"Rank {rank} ({ranks} total)")
            # the meta time gets partitioned more than necessary for the overall meta time
            # however for possible future metrics (i.e. max open / close time) it stays that way for now
            writes = sorted([x for x in write_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            reads  = sorted([x for x in read_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            opens  = sorted([x for x in open_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            closes = sorted([x for x in close_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            seeks  = sorted([x for x in seek_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            syncs  = sorted([x for x in sync_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            set_sizes = sorted([x for x in set_size_intervals[filename] if x[0] == rank], key=lambda x: x[1])
            
            write_metaops = assign_metaops(writes, opens.copy(), closes.copy(), seeks.copy(), syncs.copy(), set_sizes.copy(), True)
            read_metaops = assign_metaops(reads, opens.copy(), closes.copy(), seeks.copy(), syncs.copy(), set_sizes.copy(), False)

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


def print_operation(file, op):
    file.write(f"\tBytes: {op['bytes']} \n")
    file.write(f"\tPOSIX Level Metrics:\n")
    file.write(f"\t\tPure Operation Time: {op['posix_op_time']} \n")
    file.write(f"\t\tPure Operation Bandwidth (MiB/s): {op['posix_pure_bw']} \n")
    file.write(f"\t\tE2E Operation Time: {op['posix_meta_time']} \n")
    file.write(f"\t\tE2E Operation Bandwidth (MiB/s): {op['posix_e2e_bw']} \n")
    file.write(f"\tMPIIO Level Metrics:\n")
    file.write(f"\t\tPure Operation Time: {op['mpiio_op_time']} \n")
    file.write(f"\t\tPure Operation Bandwidth (MiB/s): {op['mpiio_pure_bw']} \n")
    file.write(f"\t\tE2E Operation Time: {op['mpiio_meta_time']} \n")
    file.write(f"\t\tE2E Operation Bandwidth (MiB/s): {op['mpiio_e2e_bw']} \n\n")


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

    
    with open(output_path, "w") as f:
        f.write(f"Overall Metrics:\n")
        f.write(f"Total bytes written: {metrics.metrics['overall']['write']['bytes_total']}\n")
        f.write(f"Total bytes read: {metrics.metrics['overall']['read']['bytes_total']}\n")

        f.write(f"Per File Metrics: \n\n")
        for filename in metrics.metrics:
            if ignore_filename(filename, metrics): continue
            f.write(f"File: {filename}\n")
            f.write(f"Write:\n")
            print_operation(f, metrics.metrics[filename]['write'])
            f.write(f"Read:\n")
            print_operation(f, metrics.metrics[filename]['read'])
    stop = datetime.now()
    duration = stop - start
    print(f"[recorder-pm]: Total time: {duration}")

        

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
