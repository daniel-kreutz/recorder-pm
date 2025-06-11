#!/usr/bin/env python
# encoding: utf-8
from ctypes import *
import os, glob, struct

"""
Global metadata information:
    must match the struct RecorderMetadata in
    include/recorder-logger.h
"""
class RecorderMetadata(Structure):
    _fields_ = [
            ("total_ranks", c_int),
            ("posix_tracing", c_bool),
            ("mpi_tracing", c_bool),
            ("mpiio_tracing", c_bool),
            ("hdf5_tracing", c_bool),
            ("pnetcdf_tracing", c_bool),
            ("netcdf_tracing", c_bool),
            ("store_tid", c_bool),
            ("store_call_depth", c_bool),
            ("start_ts", c_double),
            ("time_resolution", c_double),
            ("ts_buffer_elements", c_int),
            ("ts_compression", c_bool),
            ("interprocess_compression", c_bool),
            ("interprocess_pattern_recognition", c_bool),
            ("intraprocess_pattern_recognition", c_bool),
    ]


"""
Per-rank metadata:
    keeps total number of records,
    number of files accessed, and
    function counter.

    This is a recorder-viz only class
    not used in C reader code.
"""
class LocalMetadata():
    def __init__(self, func_list, records, total_records):
        self.total_records = total_records
        self.num_files =0
        self.filemap = set()
        self.function_count = [0] * len(func_list)

        for idx in range(total_records):
            r = records[idx]

            # Ignore user functions for now
            if r.func_id < len(func_list):
                func = func_list[r.func_id]
                self.function_count[r.func_id] += 1

            if func.startswith("MPI") or func.startswith("H5") or \
               func.startswith("ncmpi") or func.startswith("nc_"):
                continue
            if "dir" in func: continue

            if "open" in func or "close" in func or "creat" in func \
                or "seek" in func or "sync" in func:
                fstr = r.args[0]
                self.filemap.add(fstr if type(fstr)==str else fstr.decode('utf-8'))

        self.num_files = len(self.filemap)


class PyRecord(Structure):
    # The fields must be identical as PyRecord in tools/reader.h
    _fields_ = [
            ("tstart",     c_double),
            ("tend",       c_double),
            ("call_depth", c_ubyte),
            ("func_id",    c_int),
            ("tid",        c_int),
            ("arg_count",  c_ubyte),
            ("args",       POINTER(c_char_p)),    # Note in python3, args[i] is 'bytes' type
    ]

    # In Python3, self.args[i] is 'bytes' type
    # For compatable reason, we convert it to str type
    # and will only use self.arg_strs[i] to access the filename
    def args_to_strs(self):
        arg_strs = [''] * self.arg_count
        for i in range(self.arg_count):
            if(type(self.args[i]) == str):
                arg_strs[i] = self.args[i]
            else:
                arg_strs[i] = self.args[i].decode('utf-8')
        return arg_strs

'''
GM: Global Metadata
LMs: List of Local Metadata
records: List (# ranks) of Record*, each entry (Record*) is a list of records for that rank
'''
class RecorderReader:
    def str2char_p(self, s):
        return c_char_p( s.encode('utf-8') )

    def __init__(self, logs_dir):
        if "RECORDER_INSTALL_PATH" not in os.environ:
            msg="Error:\n"\
                "    RECORDER_INSTALL_PATH environment variable is not set.\n" \
                "    Please set it to the path where you installed Recorder."
            print(msg)
            exit(1)

        recorder_install_path = os.path.abspath(os.environ["RECORDER_INSTALL_PATH"])
        libreader_path = recorder_install_path + "/lib/libreader.so"

        if not os.path.isfile(libreader_path):
            msg="Error:\n"\
                "    Could not find Recorder reader library\n"\
                "    Please make sure Recorder is installed at %s",\
                recorder_install_path
            print(msg)
            exit(0);

        libreader = cdll.LoadLibrary(libreader_path)
        libreader.read_all_records.restype = POINTER(POINTER(PyRecord))

        # Load function list, also return the total number of processes
        nprocs = self.load_func_list(logs_dir + "/recorder.mt")

        self.GM = RecorderMetadata()
        SizeArray = c_size_t * nprocs
        counts = SizeArray()
        # This function also fills in self.GM
        self.records = libreader.read_all_records(self.str2char_p(logs_dir), counts, pointer(self.GM))

        self.LMs = []
        for rank in range(self.GM.total_ranks):
            LM = LocalMetadata(self.funcs, self.records[rank], counts[rank])
            self.LMs.append(LM)
            print("Rank: %d, intercepted calls: %d, accessed files: %d" %(rank, counts[rank], LM.num_files))

    def load_func_list(self, global_metadata_path):
        nprocs = 0
        with open(global_metadata_path, 'rb') as f:
            nprocs = struct.unpack('i', f.read(4))[0]
            f.seek(1024, 0)   # skip the reserved metadata block (fixed 1024 bytes)
            self.funcs = f.read().splitlines()
            self.funcs = [func.decode('utf-8') for func in self.funcs]
            #print(self.funcs)
        return nprocs


if __name__ == "__main__":
    import sys
    reader = RecorderReader(sys.argv[1])
