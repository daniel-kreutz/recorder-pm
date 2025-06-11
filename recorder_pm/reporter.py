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
from .build_offset_intervals import ignore_files
from .build_offset_intervals import build_offset_intervals, build_interface_intervals
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


def pure_metrics(intervals, metrics: Metrics):

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
    
    # TODO: check if further logic is required to choose files for aggregation
    # e.g. if temporary mpi files are in metrics.unique_files, that would
    # distort the aggregation results
    metrics.min_pure_write_bw = min(metrics.files_pure_write_bw.values())
    metrics.max_pure_write_bw = max(metrics.files_pure_write_bw.values())
    metrics.avg_pure_write_bw = sum(metrics.files_pure_write_bw.values()) / len(metrics.files_pure_write_bw)
    metrics.min_pure_read_bw = min(metrics.files_pure_read_bw.values())
    metrics.max_pure_read_bw = max(metrics.files_pure_read_bw.values())
    metrics.avg_pure_read_bw = sum(metrics.files_pure_read_bw.values()) / len(metrics.files_pure_read_bw)


def interface_metrics(intervals, metrics: Metrics):
    
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

            sum_read_time += metrics.files_pure_read_time[filename]
            metrics.files_interface_read_time[filename] = sum_read_time

            tmp_bw = sum_read_size / sum_read_time / (1024*1024)
            metrics.files_interface_read_bw[filename] = tmp_bw
        else:
            if sum_write_size == 0 or sum_write_time == 0: continue

            sum_write_time += metrics.files_pure_write_time[filename]
            metrics.files_interface_write_time[filename] = sum_write_time

            tmp_bw = sum_write_size / sum_write_time / (1024*1024)
            metrics.files_interface_write_bw[filename] = tmp_bw


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

    pure_metrics(offset_intervals, metrics)
    interface_metrics(interface_intervals, metrics)



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
