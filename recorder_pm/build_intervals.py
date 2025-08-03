#!/usr/bin/env python
# encoding: utf-8
from mpi4py import MPI

def get_mpi_datatype(type_str):
    try:
        datatype = getattr(MPI, type_str)
        return datatype
    except:
        return None


def get_mpi_type_size(type_str):
    datatype = get_mpi_datatype(type_str.removeprefix("MPI_"))
    if datatype is None:
        return 0
    else:
        return datatype.Get_size()


def ignore_files(filename):
    if not filename or filename == "":
        return True
    #else: return False # DEBUG
    ignore_prefixes = ["/sys/", "/proc", "/p/lustre2/wang116/applications/ParaDis.v2.5.1.1/Copper/Copper_results/fluxdata/", "/etc/", "stdout", "stderr", "stdin"]
    ignore_parts = [".locktest", "_cid-", "pipe:"]
    for prefix in ignore_prefixes:
        if filename.startswith(prefix):
            return True
    for part in ignore_parts:
        if part in filename:
            return True

    return False


def ignore_funcs(func):
    ignore = ["MPI", "H5", "writev"]
    for f in ignore:
        if f in func:
            return True
    return False


# only return record data of write / read calls by MPI / HDF5
def build_intervals(reader, posix: bool):

    def ignore_operations(func):
        ops = ["fwrite", "fread", "writev", "readv", "fprintf"]
        for f in ops:
            if f in func:
                return True
        return False

    func_list = reader.funcs
    ranks = reader.GM.total_ranks
    intervals = {}

    # merge the list(reader.records) of list(each rank's records) into one flat list
    # then sort the whole list by tstart
    records = []
    for rank in range(ranks):
        for i in range(reader.LMs[rank].total_records):
            record = reader.records[rank][i]
            record.rank = rank

            # ignore user functions
            if record.func_id >= len(func_list): continue
            func = func_list[record.func_id]

            if posix:
                if not ignore_funcs(func):
                    records.append(record)
            else:
                if "MPI" in func:
                    records.append(record)

    records = sorted(records, key=lambda x: x.tstart)

    # MPI uses shortened file handles to refer to the actual files
    # each key corresponds to the actual filename that is used by all other records
    mpi_file_handles = {}

    for record in records:

        rank = record.rank
        func = func_list[record.func_id]
        args = record.args_to_strs()
        filename = ""

        if posix:
            filename = args[0]
        else:
            if func == "MPI_File_open":
                filename = args[1]
                mpi_file_handles[args[4]] = filename
            else:
                filename = mpi_file_handles[args[0]]

        if ignore_files(filename): continue

        operation = ""
        count = 0

        if posix:
            # TODO: other write / read calls have count at different index in args
            if ("write" in func or "pwrite" in func) and not ignore_operations(func):
                count = int(args[2])
                operation = "write"
            elif ("read" in func or "pread" in func) and not ignore_operations(func):
                count = int(args[2])
                operation = "read"
            elif "open" in func:
                operation = "open"
            elif "close" in func:
                operation = "close"
            elif "seek" in func:
                operation = "seek"
            elif "sync" in func:
                operation = "sync"
            elif "ftruncate" in func:
                operation = "ftruncate"
            #elif "fcntl" in func:
            #    operation = "fcntl"
            else: continue
        else:
            if "write" in func:
                if "at" in func:
                    count = int(args[3]) * get_mpi_type_size(args[4])
                else:
                    count = int(args[2]) * get_mpi_type_size(args[3])
                operation = "write"
            elif "read" in func:
                if "at" in func:
                    count = int(args[3]) * get_mpi_type_size(args[4])
                else:
                    count = int(args[2]) * get_mpi_type_size(args[3])
                operation = "read"
            elif "open" in func:
                operation = "open"
            elif "close" in func:
                operation = "close"
            elif "set_size" in func:
                operation = "set_size"
            else: continue

        if filename not in intervals:
            intervals[filename] = []
        # func currently only for debug purposes
        intervals[filename].append([rank, record.tstart, record.tend, operation, count, func])
    return intervals