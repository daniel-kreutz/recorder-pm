#!/usr/bin/env python
# encoding: utf-8
from __future__ import absolute_import
import math, os
import numpy as np
from bokeh.plotting import figure, output_file, show
from bokeh.embed import components
from bokeh.models import FixedTicker, ColumnDataSource, LabelSet
from prettytable import PrettyTable


from .creader_wrapper import RecorderReader
from .html_writer import HTMLWriter
from .build_offset_intervals import *
from .metrics import Metrics



# For local test
"""
from creader_wrapper import RecorderReader
from html_writer import HTMLWriter
from build_offset_intervals import ignore_files
from build_offset_intervals import build_offset_intervals
"""


# 2.1
def function_layers(reader, htmlWriter):
    func_list = reader.funcs
    x = {'hdf5':0, 'mpi':0, 'posix':0 }
    for LM in reader.LMs:
        for func_id in range(len(func_list)):
            count = LM.function_count[func_id]
            if count <= 0: continue
            if "H5" in func_list[func_id]:
                x['hdf5'] += count
            elif "MPI" in func_list[func_id]:
                x['mpi'] += count
            else:
                x['posix'] += count
    script, div = components(pie_chart(x))
    htmlWriter.functionLayers = script+div


def function_times(reader, htmlWriter):
    func_list = reader.funcs

    aggregate = np.zeros(256)
    for rank in range(reader.GM.total_ranks):
        records = reader.records[rank]
        for i in range(reader.LMs[rank].total_records):
            record = records[i]

            # ignore user functions
            if record.func_id >= len(func_list): continue

            aggregate[record.func_id] += (record.tend - record.tstart)

    funcnames, times = np.array([]), np.array([])

    for i in range(len(aggregate)):
        if aggregate[i] > 0:
            funcnames = np.append(funcnames, func_list[i])
            times = np.append(times, aggregate[i])

    index = np.argsort(times)[::-1]
    times = times[index]
    times = [str(t) for t in times]
    funcnames = funcnames[index]

    p = figure(x_axis_label="Spent Time (Seconds)", y_axis_label="Function", y_range=funcnames)
    p.hbar(y=funcnames, right=times, height=0.8, left=0)
    labels = LabelSet(x='x', y='y', text='x', x_offset=0, y_offset=-8, text_font_size="10pt",
                source=ColumnDataSource(dict(x=times, y=funcnames)))
    p.add_layout(labels)

    script, div = components(p)
    htmlWriter.functionTimes = div + script


# 3.1
def overall_io_activities(reader, htmlWriter):

    func_list = reader.funcs
    nan = float('nan')

    def io_activity(rank):
        x_read, x_write, y_read, y_write = [], [], [], []

        for i in range(reader.LMs[rank].total_records):
            record = reader.records[rank][i]

            # ignore user functions
            if record.func_id >= len(func_list): continue

            funcname = func_list[record.func_id]
            if "MPI" in funcname or "H5" in funcname: continue
            if "dir" in funcname: continue

            if "write" in funcname or "fprintf" in funcname:
                x_write.append(record.tstart)
                x_write.append(record.tend)
                x_write.append(nan)
            if "read" in funcname:
                x_read.append(record.tstart)
                x_read.append(record.tend)
                x_read.append(nan)

        if(len(x_write)>0): x_write = x_write[0: len(x_write)-1]
        if(len(x_read)>0): x_read = x_read[0: len(x_read)-1]

        y_write = [rank] * len(x_write)
        y_read = [rank] * len(x_read)

        return x_read, x_write, y_read, y_write


    p = figure(x_axis_label="Time", y_axis_label="Rank", width=600, height=400)
    for rank in range(reader.GM.total_ranks):
        x_read, x_write, y_read, y_write = io_activity(rank)
        p.line(x_write, y_write, line_color='red', line_width=20, alpha=1.0, legend_label="write")
        p.line(x_read, y_read, line_color='blue', line_width=20, alpha=1.0, legend_label="read")

    p.legend.location = "top_left"
    script, div = components(p)
    htmlWriter.overallIOActivities = div + script


def pure_file_metrics(intervals, metrics: Metrics):

    for filename in intervals:
        if filename not in metrics.unique_files: continue

        sum_write_size = 0
        sum_write_time = 0
        sum_read_size = 0
        sum_read_time = 0
        tmp_bw = 0

        for interval in intervals[filename]:
            io_size , is_read = interval[4], interval[5]
            duration = float(interval[2]) - float(interval[1])

            if is_read:
                sum_read_size  += io_size
                sum_read_time  += duration
            else:
                sum_write_size += io_size
                sum_write_time += duration
        
        # bandwidth has MiB/s as unit
        if is_read:
            if sum_read_size == 0 or sum_read_time == 0: continue

            metrics.files_bytes_read[filename] = sum_read_size
            metrics.files_pure_read_time[filename] = sum_read_time

            tmp_bw = sum_read_size / sum_read_time / (1024*1024)
            metrics.files_pure_read_bw[filename] = tmp_bw
        else:
            if sum_write_size == 0 or sum_write_time == 0: continue

            metrics.files_bytes_written[filename] = sum_write_size
            metrics.files_pure_write_time[filename] = sum_write_time

            tmp_bw = sum_write_size / sum_write_time / (1024*1024)
            metrics.files_pure_write_bw[filename] = tmp_bw


def interface_file_metrics(intervals, metrics: Metrics):
    
    for filename in intervals:
        if filename not in metrics.unique_files: continue

        sum_write_time = 0
        sum_write_size = metrics.files_bytes_written[filename]
        sum_read_time = 0
        sum_read_size = metrics.files_bytes_read[filename]
        tmp_bw = 0

        for interval in intervals[filename]:
            is_read = interval[3]
            duration = float(interval[2]) - float(interval[1])

            if is_read:
                sum_read_time  += duration
            else:
                sum_write_time += duration

        # bandwidth has MiB/s as unit
        if is_read:
            if sum_read_size == 0 or sum_read_time == 0: continue

            metrics.files_interface_read_time[filename] = sum_read_time
            sum_read_time += metrics.files_pure_read_time[filename]

            tmp_bw = sum_read_size / sum_read_time / (1024*1024)
            metrics.files_interface_read_bw[filename] = tmp_bw
        else:
            if sum_write_size == 0 or sum_write_time == 0: continue

            metrics.files_interface_write_time[filename] = sum_write_time
            sum_write_time += metrics.files_pure_write_time[filename]

            tmp_bw = sum_write_size / sum_write_time / (1024*1024)
            metrics.files_interface_write_bw[filename] = tmp_bw


def e2e_file_metrics(reader, metrics: Metrics):

    pure_meta_write_records, pure_meta_read_records, pure_open_records, pure_close_records = file_open_close_records(reader, True)
    interface_meta_write_records, interface_meta_read_records, interface_open_records, interface_close_records = file_open_close_records(reader, False)

    for filename in metrics.files_bytes_written:
        metrics.files_pure_e2e_write_bw[filename] = 0
        metrics.files_pure_e2e_read_bw[filename] = 0
        metrics.files_interface_e2e_write_bw[filename] = 0
        metrics.files_interface_e2e_read_bw[filename] = 0

        sum_pure_meta_write_time = 0
        sum_pure_meta_read_time = 0
        sum_pure_open_time = 0
        sum_pure_close_time = 0
        sum_interface_meta_write_time = 0
        sum_interface_meta_read_time = 0
        sum_interface_open_time = 0
        sum_interface_close_time = 0

        for record in pure_meta_write_records[filename]:
            sum_pure_meta_write_time = record.tend - record.tstart
        metrics.files_pure_meta_write_time[filename] = sum_pure_meta_write_time
        
        for record in pure_meta_read_records[filename]:
            sum_pure_meta_read_time = record.tend - record.tstart
        metrics.files_pure_meta_read_time[filename] = sum_pure_meta_read_time

        for record in pure_open_records[filename]:
            sum_pure_open_time = record.tend - record.tstart
        metrics.files_posix_open_time[filename] = sum_pure_open_time
        
        for record in pure_close_records[filename]:
            sum_pure_close_time = record.tend - record.tstart
        metrics.files_posix_close_time[filename] = sum_pure_close_time

        for record in interface_meta_write_records[filename]:
            sum_interface_meta_write_time = record.tend - record.tstart
        metrics.files_interface_meta_write_time[filename] = sum_interface_meta_write_time
        
        for record in interface_meta_read_records[filename]:
            sum_interface_meta_read_time = record.tend - record.tstart
        metrics.files_interface_meta_read_time[filename] = sum_interface_meta_read_time

        for record in interface_open_records[filename]:
            sum_interface_open_time = record.tend - record.tstart
        metrics.files_interface_open_time[filename] = sum_interface_open_time
        
        for record in interface_close_records[filename]:
            sum_interface_close_time = record.tend - record.tstart
        metrics.files_interface_close_time[filename] = sum_interface_close_time

        
        if sum_pure_meta_write_time != 0:
            e2e_time = metrics.files_pure_write_time.get(filename, 0) + sum_pure_meta_write_time
            metrics.files_pure_e2e_write_bw[filename] = metrics.files_bytes_written[filename] / e2e_time / (1024*1024)

        if sum_pure_meta_read_time != 0:
            e2e_time = metrics.files_pure_read_time.get(filename, 0) + sum_pure_meta_read_time
            metrics.files_pure_e2e_read_bw[filename] = metrics.files_bytes_read[filename] / e2e_time / (1024*1024)

        if sum_interface_meta_write_time != 0:
            e2e_time = metrics.files_interface_write_time.get(filename, 0) + metrics.files_pure_write_time.get(filename, 0) + sum_interface_meta_write_time
            metrics.files_interface_e2e_write_bw[filename] = metrics.files_bytes_written[filename] / e2e_time / (1024*1024)

        if sum_interface_meta_read_time != 0:
            e2e_time = metrics.files_interface_read_time.get(filename, 0) + metrics.files_pure_read_time.get(filename, 0) + sum_interface_meta_read_time
            metrics.files_interface_e2e_read_bw[filename] = metrics.files_bytes_read[filename] / e2e_time / (1024*1024)
        

# for each rank, gets the tstart of the first open before write / read respectively
# and the tend of the last close after write / read respectively
# only_pure determines, if its the timestamps of posix file open / close or mpi file open / close
# the timestamps are then used to determine which file operations of each rank belong to
# e2e_write_bw / e2e_read_bw
def file_open_close_records(reader, only_pure: bool):
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
            if only_pure:
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


def aggregate_file_metrics(metrics: Metrics):
    # TODO: check if further logic is required to choose files for aggregation
    # e.g. if temporary mpi files are in metrics.unique_files, that would
    # distort the aggregation results
    if len(metrics.files_pure_write_bw) != 0:
        metrics.min_pure_write_bw = min(metrics.files_pure_write_bw.values())
        metrics.max_pure_write_bw = max(metrics.files_pure_write_bw.values())
        metrics.avg_pure_write_bw = sum(metrics.files_pure_write_bw.values()) / len(metrics.files_pure_write_bw)
    
    if len(metrics.files_pure_read_bw) != 0:
        metrics.min_pure_read_bw = min(metrics.files_pure_read_bw.values())
        metrics.max_pure_read_bw = max(metrics.files_pure_read_bw.values())
        metrics.avg_pure_read_bw = sum(metrics.files_pure_read_bw.values()) / len(metrics.files_pure_read_bw)

    if len(metrics.files_interface_write_bw) != 0:
        metrics.min_interface_write_bw = min(metrics.files_interface_write_bw.values())
        metrics.max_interface_write_bw = max(metrics.files_interface_write_bw.values())
        metrics.avg_interface_write_bw = sum(metrics.files_interface_write_bw.values()) / len(metrics.files_interface_write_bw)

    if len(metrics.files_interface_read_bw) != 0:
        metrics.min_interface_read_bw = min(metrics.files_interface_read_bw.values())
        metrics.max_interface_read_bw = max(metrics.files_interface_read_bw.values())
        metrics.avg_interface_read_bw = sum(metrics.files_interface_read_bw.values()) / len(metrics.files_interface_read_bw)

    if len(metrics.files_pure_e2e_write_bw) != 0:
        metrics.min_pure_e2e_write_bw = min(metrics.files_pure_e2e_write_bw.values())
        metrics.max_pure_e2e_write_bw = max(metrics.files_pure_e2e_write_bw.values())
        metrics.avg_pure_e2e_write_bw = sum(metrics.files_pure_e2e_write_bw.values()) / len(metrics.files_pure_e2e_write_bw)
    
    if len(metrics.files_pure_e2e_read_bw) != 0:
        metrics.min_pure_e2e_read_bw = min(metrics.files_pure_e2e_read_bw.values())
        metrics.max_pure_e2e_read_bw = max(metrics.files_pure_e2e_read_bw.values())
        metrics.avg_pure_e2e_read_bw = sum(metrics.files_pure_e2e_read_bw.values()) / len(metrics.files_pure_e2e_read_bw)

    if len(metrics.files_interface_e2e_write_bw) != 0:
        metrics.min_interface_e2e_write_bw = min(metrics.files_interface_e2e_write_bw.values())
        metrics.max_interface_e2e_write_bw = max(metrics.files_interface_e2e_write_bw.values())
        metrics.avg_interface_e2e_write_bw = sum(metrics.files_interface_e2e_write_bw.values()) / len(metrics.files_interface_e2e_write_bw)
    
    if len(metrics.files_interface_e2e_read_bw) != 0:
        metrics.min_interface_e2e_read_bw = min(metrics.files_interface_e2e_read_bw.values())
        metrics.max_interface_e2e_read_bw = max(metrics.files_interface_e2e_read_bw.values())
        metrics.avg_interface_e2e_read_bw = sum(metrics.files_interface_e2e_read_bw.values()) / len(metrics.files_interface_e2e_read_bw)

    if len(metrics.files_pure_write_time) != 0:
        metrics.min_pure_write_time = min(metrics.files_pure_write_time.values())
        metrics.max_pure_write_time = max(metrics.files_pure_write_time.values())
        metrics.avg_pure_write_time = sum(metrics.files_pure_write_time.values()) / len(metrics.files_pure_write_time)

    if len(metrics.files_pure_read_time) != 0:
        metrics.min_pure_read_time = min(metrics.files_pure_read_time.values())
        metrics.max_pure_read_time = max(metrics.files_pure_read_time.values())
        metrics.avg_pure_read_time = sum(metrics.files_pure_read_time.values()) / len(metrics.files_pure_read_time)

    if len(metrics.files_interface_write_time) != 0:
        metrics.min_interface_write_time = min(metrics.files_interface_write_time.values())
        metrics.max_interface_write_time = max(metrics.files_interface_write_time.values())
        metrics.avg_interface_write_time = sum(metrics.files_interface_write_time.values()) / len(metrics.files_interface_write_time)

    if len(metrics.files_interface_read_time) != 0:
        metrics.min_interface_read_time = min(metrics.files_interface_read_time.values())
        metrics.max_interface_read_time = max(metrics.files_interface_read_time.values())
        metrics.avg_interface_read_time = sum(metrics.files_interface_read_time.values()) / len(metrics.files_interface_read_time)

    if len(metrics.files_pure_meta_write_time) != 0:
        metrics.min_pure_meta_write_time = min(metrics.files_pure_meta_write_time.values())
        metrics.max_pure_meta_write_time = max(metrics.files_pure_meta_write_time.values())
        metrics.avg_pure_meta_write_time = sum(metrics.files_pure_meta_write_time.values()) / len(metrics.files_pure_meta_write_time)

    if len(metrics.files_pure_meta_read_time) != 0:
        metrics.min_pure_meta_read_time = min(metrics.files_pure_meta_read_time.values())
        metrics.max_pure_meta_read_time = max(metrics.files_pure_meta_read_time.values())
        metrics.avg_pure_meta_read_time = sum(metrics.files_pure_meta_read_time.values()) / len(metrics.files_pure_meta_read_time)

    if len(metrics.files_interface_meta_write_time) != 0:
        metrics.min_interface_meta_write_time = min(metrics.files_interface_meta_write_time.values())
        metrics.max_interface_meta_write_time = max(metrics.files_interface_meta_write_time.values())
        metrics.avg_interface_meta_write_time = sum(metrics.files_interface_meta_write_time.values()) / len(metrics.files_interface_meta_write_time)

    if len(metrics.files_interface_meta_read_time) != 0:
        metrics.min_interface_meta_read_time = min(metrics.files_interface_meta_read_time.values())
        metrics.max_interface_meta_read_time = max(metrics.files_interface_meta_read_time.values())
        metrics.avg_interface_meta_read_time = sum(metrics.files_interface_meta_read_time.values()) / len(metrics.files_interface_meta_read_time)

    if len(metrics.files_posix_open_time) != 0:
        metrics.min_posix_open_time = min(metrics.files_posix_open_time.values())
        metrics.max_posix_open_time = max(metrics.files_posix_open_time.values())
        metrics.avg_posix_open_time = sum(metrics.files_posix_open_time.values()) / len(metrics.files_posix_open_time)

    if len(metrics.files_posix_close_time) != 0:
        metrics.min_posix_close_time = min(metrics.files_posix_close_time.values())
        metrics.max_posix_close_time = max(metrics.files_posix_close_time.values())
        metrics.avg_posix_close_time = sum(metrics.files_posix_close_time.values()) / len(metrics.files_posix_close_time)

    if len(metrics.files_interface_open_time) != 0:
        metrics.min_interface_open_time = min(metrics.files_interface_open_time.values())
        metrics.max_interface_open_time = max(metrics.files_interface_open_time.values())
        metrics.avg_interface_open_time = sum(metrics.files_interface_open_time.values()) / len(metrics.files_interface_open_time)

    if len(metrics.files_interface_close_time) != 0:
        metrics.min_interface_close_time = min(metrics.files_interface_close_time.values())
        metrics.max_interface_close_time = max(metrics.files_interface_close_time.values())
        metrics.avg_interface_close_time = sum(metrics.files_interface_close_time.values()) / len(metrics.files_interface_close_time)



def generate_report(reader, output_path):

    output_path = os.path.abspath(output_path)
    if output_path[-5:] != ".html":
        output_path += ".html"

    htmlWriter = HTMLWriter(output_path)

    intervals = build_offset_intervals(reader)

    function_layers(reader, htmlWriter)
    function_times(reader, htmlWriter)

    overall_io_activities(reader, htmlWriter)

    htmlWriter.write_html()


def print_metrics(reader, output_path):
    metrics = Metrics()
    offset_intervals = build_offset_intervals(reader)
    interface_intervals = build_interface_intervals(reader)

    pure_file_metrics(offset_intervals, metrics)
    interface_file_metrics(interface_intervals, metrics)
    e2e_file_metrics(reader, metrics)
    aggregate_file_metrics(metrics)



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
