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

        # self.files_w_open_tstart = {}
        # self.files_w_close_tend = {}
        # self.files_r_open_tstart = {}
        # self.files_r_close_tend = {}

        self.files_bytes_written = {}
        self.files_bytes_read = {}

        self.total_bytes_written = 0
        self.total_bytes_read = 0

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

        # pure bandwidths that also include interface file open / close times
        self.files_pure_e2e_write_bw = {}
        self.files_pure_e2e_read_bw = {}

        self.min_pure_e2e_write_bw = 0.0
        self.max_pure_e2e_write_bw = 0.0
        self.avg_pure_e2e_write_bw = 0.0
        self.min_pure_e2e_read_bw = 0.0
        self.max_pure_e2e_read_bw = 0.0
        self.avg_pure_e2e_read_bw = 0.0

        # interface bandwidths that also include interface (mpi) file open / close times
        self.files_interface_e2e_write_bw = {}
        self.files_interface_e2e_read_bw = {}

        self.min_interface_e2e_write_bw = 0.0
        self.max_interface_e2e_write_bw = 0.0
        self.avg_interface_e2e_write_bw = 0.0
        self.min_interface_e2e_read_bw = 0.0
        self.max_interface_e2e_read_bw = 0.0
        self.avg_interface_e2e_read_bw = 0.0

        # time that each rank needed to write / read all data (posix timings only)
        self.files_pure_write_time = {}
        self.files_pure_read_time = {}

        self.min_pure_write_time = 0.0
        self.max_pure_write_time = 0.0
        self.avg_pure_write_time = 0.0
        self.min_pure_read_time = 0.0
        self.max_pure_read_time = 0.0
        self.avg_pure_read_time = 0.0

        # analogous to pure time, but only including write / read calls
        # of interfaces that are higher up in the I/O stack
        self.files_interface_write_time = {}
        self.files_interface_read_time = {}

        self.min_interface_write_time = 0.0
        self.max_interface_write_time = 0.0
        self.avg_interface_write_time = 0.0
        self.min_interface_read_time = 0.0
        self.max_interface_read_time = 0.0
        self.avg_interface_read_time = 0.0

        # time spent on all posix (pure) / interface (mpi) meta operations
        # during writes / reads
        # TODO: check approach for collecting meta records (see file_open_close_records())
        self.files_pure_meta_write_time = {}
        self.files_pure_meta_read_time = {}

        self.min_pure_meta_write_time = 0.0
        self.max_pure_meta_write_time = 0.0
        self.avg_pure_meta_write_time = 0.0
        self.min_pure_meta_read_time = 0.0
        self.max_pure_meta_read_time = 0.0
        self.avg_pure_meta_read_time = 0.0

        
        self.files_interface_meta_write_time = {}
        self.files_interface_meta_read_time = {}

        self.min_interface_meta_write_time = 0.0
        self.max_interface_meta_write_time = 0.0
        self.avg_interface_meta_write_time = 0.0
        self.min_interface_meta_read_time = 0.0
        self.max_interface_meta_read_time = 0.0
        self.avg_interface_meta_read_time = 0.0

        # overall time per file spent on posix / interface file open / close
        # regardless of if it belongs to writes / reads
        self.files_posix_open_time = {}
        self.files_posix_close_time = {}

        self.min_posix_open_time = 0.0
        self.max_posix_open_time = 0.0
        self.avg_posix_open_time = 0.0
        self.min_posix_close_time = 0.0
        self.max_posix_close_time = 0.0
        self.avg_posix_close_time = 0.0

        self.files_interface_open_time = {}
        self.files_interface_close_time = {}

        self.min_interface_open_time = 0.0
        self.max_interface_open_time = 0.0
        self.avg_interface_open_time = 0.0
        self.min_interface_close_time = 0.0
        self.max_interface_close_time = 0.0
        self.avg_interface_close_time = 0.0

        # TODO: add IOPS if there is enough time


