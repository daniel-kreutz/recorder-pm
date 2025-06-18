from ctypes import *
import os, glob, struct
from .creader_wrapper import RecorderReader
from build_offset_intervals import ignore_files

class MetricObject(RecorderReader):
    def __init__(self, reader):
        self.total_files = 0
        self.unique_files = set()
        for lm in reader.LMs:
            for file in lm.filemap:
                if not ignore_files(file) and file not in self.unique_files:
                    self.unique_files.add(file)
                    self.total_files += 1

        # TODO: maybe add open / close time seperately
        self.metrics = {
            "write": {
                "bytes_total": 0,       # total bytes written across all files
                "bytes_per_file": {},   # total bytes written per file
                "posix_op_time": {},    # posix write time per file (max of all rank times) 
                "posix_meta_time": {},  # posix meta + write time per file (max of all rank times)
                "posix_pure_bw": {},    # bandwidth per file that only contains posix write times
                "posix_e2e_bw": {},     # bandwidth per file that only contains posix meta / write times
                "mpiio_op_time": {},    # mpiio write time per file (max of all rank times) 
                "mpiio_meta_time": {},  # mpiio meta + write time per file (max of all rank times)   
                "mpiio_pure_bw": {},    # mpiio write time per file (max of all rank times)     
                "mpiio_e2e_bw": {}      # mpiio meta + write time per file (max of all rank times)
            },
            "read": {                   # analogous to write metrics
                "bytes_total": 0,
                "bytes_per_file": {},
                "posix_op_time": {},
                "posix_meta_time": {},
                "posix_pure_bw": {},
                "posix_e2e_bw": {},
                "mpiio_op_time": {},
                "mpiio_meta_time": {},
                "mpiio_pure_bw": {},
                "mpiio_e2e_bw": {}
            }
        }

        # TODO: add IOPS if there is enough time


