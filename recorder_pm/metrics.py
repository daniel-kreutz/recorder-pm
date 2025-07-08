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
                "write": {
                    "total_bytes": 0,               
                    "max_posix_op_time": 0.0,       # max of file posix_op_time (-> max op time of all ranks and files)
                    "max_posix_meta_time": 0.0,     # analogous to the above
                    "agg_posix_pure_bw": 0.0,       # posix_pure_bw aggregated over all files (bytes / max_posix_op_time)
                    "agg_posix_e2e_bw": 0.0,        # analogous to the above
                    "avg_posix_pure_bw": 0.0,       # posix_pure_bw over all files as the average over all file posix_pure_bw
                    "avg_posix_e2e_bw": 0.0,        # analogous to the above
                    "max_mpiio_op_time": 0.0,       # rest analogous to the above posix metrics
                    "max_mpiio_meta_time": 0.0,
                    "agg_mpiio_pure_bw": 0.0,
                    "agg_mpiio_e2e_bw": 0.0,
                    "avg_mpiio_pure_bw": 0.0,
                    "avg_mpiio_e2e_bw": 0.0
                },
                "read": {
                    "total_bytes": 0,
                    "max_posix_op_time": 0.0,
                    "max_posix_meta_time": 0.0,
                    "agg_posix_pure_bw": 0.0,
                    "agg_posix_e2e_bw": 0.0,
                    "avg_posix_pure_bw": 0.0,
                    "avg_posix_e2e_bw": 0.0,
                    "max_mpiio_op_time": 0.0,
                    "max_mpiio_meta_time": 0.0,
                    "agg_mpiio_pure_bw": 0.0,
                    "agg_mpiio_e2e_bw": 0.0,
                    "avg_mpiio_pure_bw": 0.0,
                    "avg_mpiio_e2e_bw": 0.0
                },
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

