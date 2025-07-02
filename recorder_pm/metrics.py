from ctypes import *
import os, glob, struct
from .creader_wrapper import RecorderReader
from recorder_pm.build_intervals import ignore_files

class MetricObject(RecorderReader):
    def __init__(self, reader):
        # TODO: maybe add open / close time seperately
        # metrics has this structure: metrics[filename][write/read][metric]
        # the structure for each metrics[filename] can be seen in add_filename
        self.metrics = {
            "overall": {
                "write": {"bytes_total": 0},
                "read": {"bytes_total": 0}
            }
        }

        # TODO: add IOPS if there is enough time

    def add_filename(self, filename):
        self.metrics[filename] = {
            "write": {
                "bytes": 0,              # total bytes written per file
                "posix_op_time": 0.0,    # posix write time per file (max of all rank times) 
                "posix_meta_time": 0.0,  # posix meta + write time per file (max of all rank times)
                "posix_pure_bw": 0.0,    # bandwidth per file that only contains posix write times
                "posix_e2e_bw": 0.0,     # bandwidth per file that only contains posix meta / write times
                "mpiio_op_time": 0.0,    # mpiio write time per file (max of all rank times) 
                "mpiio_meta_time": 0.0,  # mpiio meta + write time per file (max of all rank times)   
                "mpiio_pure_bw": 0.0,    # mpiio write time per file (max of all rank times)     
                "mpiio_e2e_bw": 0.0      # mpiio meta + write time per file (max of all rank times)
            },
            "read": {                   # analogous to write metrics
                "bytes": 0,
                "posix_op_time": 0.0,
                "posix_meta_time": 0.0,
                "posix_pure_bw": 0.0,
                "posix_e2e_bw": 0.0,
                "mpiio_op_time": 0.0,
                "mpiio_meta_time": 0.0,
                "mpiio_pure_bw": 0.0,
                "mpiio_e2e_bw": 0.0
            }
        }

