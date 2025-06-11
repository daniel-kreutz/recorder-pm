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
from .build_offset_intervals import build_offset_intervals
from .metrics import Metrics



# For local test
"""
from creader_wrapper import RecorderReader
from html_writer import HTMLWriter
from build_offset_intervals import ignore_files
from build_offset_intervals import build_offset_intervals
"""


# 0.0
def record_counts(reader, htmlWriter):
    y = []
    for LM in reader.LMs:
        y.append(LM.total_records)
    x = list(range(reader.GM.total_ranks))
    p = figure(x_axis_label="Rank", y_axis_label="Number of records", width=400, height=300)
    p.vbar(x=x, top=y, width=0.6)
    script, div = components(p)
    htmlWriter.recordCount = div+script

# 1.1
def file_counts(reader, htmlWriter):
    y = []
    for LM in reader.LMs:
        num = 0
        for filename in LM.filemap:
            if not ignore_files(filename):
                num += 1
        y.append(num)
    x = list(range(reader.GM.total_ranks))
    p = figure(x_axis_label="Rank", y_axis_label="Number of files accessed", width=400, height=300)
    p.vbar(x=x, top=y, width=0.6)
    script, div = components(p)
    htmlWriter.fileCount = div+script

# Helper for pie charts in 2.
# where x is a dict with keys as categories
def pie_chart(x):
    import pandas as pd
    from bokeh.palettes import Category20c
    data = pd.Series(x).reset_index(name='value').rename(columns={'index':'layer'})
    data['angle'] = data['value']/data['value'].sum() * 2*math.pi
    data['color'] = Category20c[len(x)]

    from bokeh.transform import cumsum
    p = figure(height=300, width=400)
    p.wedge(x=0, y=1, radius=0.4, start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
            line_color="white", fill_color='color', legend_field='layer', source=data)

    p.axis.axis_label=None
    p.axis.visible=False
    p.grid.grid_line_color = None
    return p

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

# 2.2
def function_patterns(all_intervals, htmlWriter):
    # 1,2,3 - consecutive
    # 1,3,9 - sequential
    # 1,3,2 - random
    x = {'consecutive':0, 'sequential':0, 'random':0}
    for filename in all_intervals.keys():
        if ignore_files(filename): continue
        intervals = sorted(all_intervals[filename], key=lambda x: x[1])   # sort by tstart
        '''
        This code consider each rank separately
        lastOffsets = [0] * reader.globalMetadata.numRanks
        for interval in intervals:
            rank, offset, count = interval[0], interval[3], interval[4]
            lastOffset = lastOffsets[rank]
            if (offset + count) == lastOffset:
                x['consecutive'] += 1
            elif (offset + count) > lastOffset:
                x['sequential'] += 1
            else:
                #print filename, interval
                x['random'] += 1
            lastOffsets[rank] = offset + count
        '''
        for i in range(len(intervals)-1):
            i1, i2 = intervals[i], intervals[i+1]
            offset1, count1 = i1[3], i1[4]
            offset2, count2 = i2[3], i2[4]

            if (offset1 + count1) == offset2:
                x['consecutive'] += 1
            elif (offset1 + count1) < offset2:
                x['sequential'] += 1
            else:
                x['random'] += 1
        total = x['consecutive'] + x['sequential'] + x['random']
    #print("consecutive:",  x['consecutive'] )
    #print("sequential:",  x['sequential'] )
    #print("random:",  x['random'])

    script, div = components(pie_chart(x))
    htmlWriter.functionPatterns = script+div

# 2.3
def function_counts(reader, htmlWriter):
    func_list = reader.funcs
    aggregate = np.zeros(2162)
    for LM in reader.LMs:
        aggregate += np.array(LM.function_count)

    funcnames, counts = np.array([]), np.array([])
    for i in range(len(aggregate)):
        if aggregate[i] > 0:
            funcnames = np.append(funcnames, func_list[i].replace("PMPI", "MPI"))
            counts = np.append(counts, aggregate[i])

    index = np.argsort(counts)[::-1]
    counts = counts[index]
    # This converts float array to str array, a fix needed for python3/and latest bokeh
    counts = [str(c) for c in counts]
    funcnames = funcnames[index]

    p = figure(x_axis_label="Count", x_axis_type="log", y_axis_label="Function", y_range=funcnames)
    p.hbar(y=funcnames, right=counts, height=0.8, left=1)
    labels = LabelSet(x='x', y='y', text='z', x_offset=0, y_offset=-8, text_font_size="10pt",
                source=ColumnDataSource(dict(x=counts, y=funcnames, z=counts)))
    p.add_layout(labels)

    script, div = components(p)
    htmlWriter.functionCount = div + script

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

#3.2
def offset_vs_rank(intervals, htmlWriter):
    # interval = [rank, tstart, tend, offset, count]
    def plot_for_one_file(filename, intervals):
        intervals = sorted(intervals, key=lambda x: x[3])   # sort by starting offset
        x_read, y_read, x_write, y_write, nan = [], [], [], [], float('nan')
        for interval in intervals:
            rank, offset, count, isRead = interval[0], interval[3], interval[4], interval[5]
            if isRead:
                x_read += [rank, rank, rank]
                y_read += [offset, offset+count, nan]
            else:
                x_write += [rank, rank, rank]
                y_write += [offset, offset+count, nan]

        if len(x_read) > 0 : x_read = x_read[0:len(x_read)-1]
        if len(y_read) > 0 : y_read = y_read[0:len(y_read)-1]
        if len(x_write) > 0 : x_write = x_write[0:len(x_write)-1]
        if len(y_write) > 0 : y_write = y_write[0:len(y_write)-1]
        p = figure(title=filename.split("/")[-1], x_axis_label="Rank", y_axis_label="Offset")
        p.line(x_read, y_read, line_color='blue', line_width=5, alpha=1.0, legend_label="read")
        p.line(x_write, y_write, line_color='red', line_width=5, alpha=1.0, legend_label="write")
        return p

    plots = []
    idx = 0
    for filename in intervals:
        if ignore_files(filename): continue
        if 'junk' in filename and int(filename.split('junk.')[-1]) > 0: continue    # NWChem
        if 'pout' in filename and int(filename.split('pout.')[-1]) > 0: continue    # Chombo
        if idx < 16 and (len(intervals[filename]) > 0): # only show 12 files at most
            p = plot_for_one_file(filename, intervals[filename])
            plots.append(p)
            idx += 1

    from bokeh.layouts import gridplot
    script, div = components(gridplot(plots, ncols=3, width=400, height=300))
    htmlWriter.offsetVsRank = script+div

# 3.3
def offset_vs_time(intervals, htmlWriter):
    # interval = [rank, tstart, tend, offset, count]
    def plot_for_one_file(filename, intervals):
        intervals = sorted(intervals, key=lambda x: x[1])   # sort by tstart
        x_read, y_read, x_write, y_write, nan = [], [], [], [], float('nan')
        for interval in intervals:
            tstart, tend, offset, count, isRead = interval[1], interval[2], interval[3], interval[4], interval[5]
            if isRead:
                x_read += [tstart, tend, nan]
                y_read += [offset, offset+count, offset+count]
            else:
                x_write += [tstart, tend, nan]
                y_write += [offset, offset+count, offset+count]

        if len(x_read) > 0 : x_read = x_read[0:len(x_read)-1]
        if len(y_read) > 0 : y_read = y_read[0:len(y_read)-1]
        if len(x_write) > 0 : x_write = x_write[0:len(x_write)-1]
        if len(y_write) > 0 : y_write = y_write[0:len(y_write)-1]
        p = figure(title=filename.split("/")[-1], x_axis_label="Time", y_axis_label="Offset")
        p.line(x_read, y_read, line_color='blue', line_width=2, alpha=1.0, legend_label="read")
        p.line(x_write, y_write, line_color='red', line_width=2, alpha=1.0, legend_label="write")
        return p


    plots = []
    idx = 0
    for filename in intervals:
        if ignore_files(filename): continue
        if 'junk' in filename and int(filename.split('junk.')[-1]) > 0: continue    # NWChem
        if 'pout' in filename and int(filename.split('pout.')[-1]) > 0: continue    # Chombo
        if idx < 16 and (len(intervals[filename]) > 0): # only show 12 files at most
            p = plot_for_one_file(filename, intervals[filename])
            plots.append(p)
            idx += 1

    from bokeh.layouts import gridplot
    script, div = components(gridplot(plots, ncols=3, width=400, height=300))
    htmlWriter.offsetVsTime = script+div

# 3.4
def file_access_patterns(intervals, htmlWriter):

    def pattern_for_one_file(filename, intervals):
        pattern = {"RAR": {'S':0, 'D':0}, "RAW": {'S':0, 'D':0},
                    "WAW": {'S':0, 'D':0}, "WAR": {'S':0, 'D':0}}
        intervals = sorted(intervals, key=lambda x: x[3])   # sort by starting offset
        for i in range(len(intervals)-1):
            i1, i2 = intervals[i], intervals[i+1]
            tstart1, offset1, count1, segments1 = i1[1], i1[3], i1[4], i1[6]
            tstart2, offset2, count2, segments2 = i2[1], i2[3], i2[4], i2[6]

            # no overlapping
            if offset1+count1 <= offset2:
                continue
            if len(segments1) == 0 or len(segments2) ==0:
                #print("Without a session? ", filename, i1, i2)
                continue
            # has overlapping but may not conflicting
            # if segments1 intersets segments2, and
            # one of the common segments is the local session
            # then there's a conflict
            if not (segments1[0] in segments2 or segments2[0] in segments1):
                continue

            isRead1 = i1[5] if tstart1 < tstart2 else i2[5]
            isRead2 = i2[5] if tstart2 > tstart1 else i1[5]
            rank1 = i1[0] if tstart1 < tstart2 else i2[0]
            rank2 = i2[0] if tstart2 > tstart1 else i1[0]

            #print(filename, i1, i2)
            # overlap
            if isRead1 and isRead2:             # RAR
                if rank1 == rank2: pattern['RAR']['S'] += 1
                else: pattern['RAR']['D'] += 1
            if isRead1 and not isRead2:         # WAR
                if rank1 == rank2: pattern['WAR']['S'] += 1
                else: pattern['WAR']['D'] += 1
            if not isRead1 and not isRead2:     # WAW
                if rank1 == rank2: pattern['WAW']['S'] += 1
                else: pattern['WAW']['D'] += 1
            if not isRead1 and isRead2:         # RAW
                if rank1 == rank2: pattern['RAW']['S'] += 1
                else: pattern['RAW']['D'] += 1
        # debug info
        if pattern['RAW']['S']: print("RAW-S", pattern['RAW']['S'], filename)
        if pattern['RAW']['D']: print("RAW-D", pattern['RAW']['D'], filename)
        if pattern['WAW']['S']: print("WAW-S", pattern['WAW']['S'], filename)
        if pattern['WAW']['D']: print("WAW-D", pattern['WAW']['D'], filename)
        if pattern['WAR']['S']: print("WAR-S", pattern['WAR']['S'], filename)
        if pattern['WAR']['D']: print("WAR-D", pattern['WAR']['D'], filename)
        return pattern

    table = PrettyTable()
    table.field_names = ['Filename', 'RAR(Same Rank)', 'RAW(Same Rank)', 'WAW(Same Rank)', 'WAR(Same Rank)', \
            'RAR(Different Rank)', 'RAW(Different Rank)', 'WAW(Different Rank)', 'WAR(Different Rank)']
    for filename in intervals.keys():
        if not ignore_files(filename):
            pattern = pattern_for_one_file(filename, intervals[filename])
            table.add_row([filename,    \
                pattern['RAR']['S'], pattern['RAW']['S'], pattern['WAW']['S'], pattern['WAR']['S'], \
                pattern['RAR']['D'], pattern['RAW']['D'], pattern['WAW']['D'], pattern['WAR']['D']])
    htmlWriter.fileAccessPatterns = table.get_html_string()

# 4
def io_sizes(intervals, htmlWriter, read=True):

    sizes = {}
    for filename in intervals:
        if ignore_files(filename): continue
        for interval in intervals[filename]:
            io_size , isRead = interval[4], interval[5]
            if read != isRead: continue
            if io_size not in sizes: sizes[io_size] = 0
            sizes[io_size] += 1


    xs = sorted(sizes.keys())
    ys = [ str(sizes[x]) for x in xs ]
    xs = [ str(x) for x in xs ]

    p = figure(x_range=xs, x_axis_label="IO Size", y_axis_label="Count", y_axis_type='log', width=500 if not read else 400, height=350)
    p.vbar(x=xs, top=ys, width=0.6, bottom=1)
    p.xaxis.major_label_orientation = math.pi/2

    labels = LabelSet(x='x', y='y', text='y', x_offset=-10, y_offset=0, text_font_size="10pt",
                source=ColumnDataSource(dict(x=xs ,y=ys)))
    p.add_layout(labels)

    script, div = components(p)
    if read:
        htmlWriter.readIOSizes = div + script
    else:
        htmlWriter.writeIOSizes = div + script

# 4.1
def io_statistics(reader, intervals, htmlWriter):
    sum_write_size = {}
    sum_write_time = {}
    sum_read_size = {}
    sum_read_time = {}
    sum_meta_time = {}

    for filename in intervals:
        if ignore_files(filename): continue

        sum_write_size[filename] = 0
        sum_write_time[filename] = 0
        sum_read_size[filename]  = 0
        sum_read_time[filename]  = 0
        sum_meta_time[filename]  = 0

        for interval in intervals[filename]:
            io_size , is_read = interval[4], interval[5]
            duration = float(interval[2]) - float(interval[1])

            if is_read:
                sum_read_size[filename]  += io_size
                sum_read_time[filename]  += duration
            else:
                sum_write_size[filename] += io_size
                sum_write_time[filename] += duration

    for rank in range(reader.GM.total_ranks):
        records = reader.records[rank]

        # ignore user functions

        for i in range(reader.LMs[rank].total_records):
            record = records[i]

            if record.func_id >= len(reader.funcs): continue
            func = reader.funcs[record.func_id]

            if "dir" in func or "MPI" in func or "H5" in func: continue
            if "open" in func or "close" in func or "sync" in func or "seek" in func:
                filename = record.args[0]
                if filename in sum_write_size.keys():
                    sum_meta_time[filename] += record.tend - record.tstart


    table = PrettyTable()
    table.field_names = ['Filename', 'Bytes written', 'Write time (s)', 'Write Bandwidth (MB/s)', \
                         'Bytes read', 'Read time (s)', 'Read Bandwidth (MB/s)', 'Metadata time (s)']
    for filename in sum_write_size:
        write_bw = 0
        if sum_write_size[filename] != 0 and sum_write_time[filename] != 0:
            write_bw = sum_write_size[filename]/sum_write_time[filename]/(1024*1024)
        read_bw  = 0
        if sum_read_size[filename] != 0 and sum_read_time[filename] != 0:
            read_bw = sum_read_size[filename]/sum_read_time[filename]/(1024*1024)

        table.add_row([filename, sum_write_size[filename], sum_write_time[filename], write_bw,
                                sum_read_size[filename], sum_read_time[filename], read_bw, sum_meta_time[filename]])

    print(table)
    htmlWriter.perFileIOStatistics = table.get_html_string()


def generate_report(reader, output_path):

    output_path = os.path.abspath(output_path)
    if output_path[-5:] != ".html":
        output_path += ".html"

    htmlWriter = HTMLWriter(output_path)

    intervals = build_offset_intervals(reader)

    record_counts(reader, htmlWriter)

    file_counts(reader, htmlWriter)

    function_layers(reader, htmlWriter)
    function_patterns(intervals, htmlWriter)
    function_counts(reader, htmlWriter)
    function_times(reader, htmlWriter)

    overall_io_activities(reader, htmlWriter)
    offset_vs_time(intervals, htmlWriter)
    offset_vs_rank(intervals, htmlWriter)

    file_access_patterns(intervals, htmlWriter)

    io_statistics(reader, intervals, htmlWriter)
    io_sizes(intervals, htmlWriter, read=True)
    io_sizes(intervals, htmlWriter, read=False)

    htmlWriter.write_html()


def pure_bw(reader, metrics, read):
    test = 0
    
    


def print_metrics(reader):
    metrics = Metrics()
    pure_bw(reader, metrics, read=False)



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
    generate_report(reader, args.output_path)
