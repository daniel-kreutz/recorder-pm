from ctypes import *
import os, glob, struct
from .creader_wrapper import RecorderReader
from build_offset_intervals import ignore_files

class Metrics(RecorderReader):
    def __init__(self, reader):
        self.total_files = 0
        self.unique_files = set()
        for lm in reader.LMs:
            for file in lm.filemap:
                if not ignore_files(file) and file not in self.unique_files:
                    self.unique_files.add(file)
                    self.total_files += 1

        self.files_bytes_written = {}
        self.files_bytes_read = {}

        # bandwidths per file, only using timings of posix operations
        self.files_pure_write_bw = {}
        self.files_pure_read_bw = {}

        # aggregate values of the values in files_pure_..._bw
        self.min_pure_write_bw = 0.0
        self.max_pure_write_bw = 0.0
        self.avg_pure_write_bw = 0.0
        self.min_pure_read_bw = 0.0
        self.max_pure_read_bw = 0.0
        self.avg_pure_read_bw = 0.0
        
        # bandwidths covering all ranks, using timings of posix operations
        # used time interval starts at first overall pwrite / pread timestamp
        # and ends after last overall pwrite / pread timestamp
        # TODO: evaluate if this metric is necessary / makes sense
        # because files_pure_..._bw only uses the sum of the actual write / read times
        # but this metric would also include everything else between the timestamps
        # self.overall_pure_write_bw = 0.0
        # self.overall_pure_read_bw = 0.0

        # analogous to pure bw, but also including write / read calls
        # of interfaces that are higher up in the I/O stack
        self.files_interface_read_bw = {}
        self.files_interface_write_bw = {}

        self.min_interface_write_bw = 0.0
        self.max_interface_write_bw = 0.0
        self.avg_interface_write_bw = 0.0
        self.min_interface_read_bw = 0.0
        self.max_interface_read_bw = 0.0
        self.avg_interface_read_bw = 0.0

        # self.overall_interface_write_bw = 0.0
        # self.overall_interface_read_bw = 0.0

        # bandwidths that also include file open / close times
        self.files_full_write_bw = {}
        self.files_full_read_bw = {}

        self.min_full_write_bw = 0.0
        self.max_full_write_bw = 0.0
        self.avg_full_write_bw = 0.0
        self.min_full_read_bw = 0.0
        self.max_full_read_bw = 0.0
        self.avg_full_read_bw = 0.0

        # only differs from files_full_..._bw, if there are several files
        # TODO: choose approach for aggregation (max / min or analogous to ior)
        ## elf.overall_full_write_bw = 0.0
        ## elf.overall_full_read_bw = 0.0

        # time that each rank needed to write / read all data (posix timings only)
        self.files_pure_write_time = {}
        self.files_pure_read_time = {}

        # analogous to pure time, but also including write / read calls
        # of interfaces that are higher up in the I/O stack
        self.files_interface_write_time = {}
        self.files_interface_read_time = {}

        self.files_open_time = {}
        self.files_close_time = {}

        # TODO: add IOPS if there is enough time


