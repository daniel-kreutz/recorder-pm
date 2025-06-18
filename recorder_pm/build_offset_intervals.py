#!/usr/bin/env python
# encoding: utf-8
def handle_data_operations(record, func_list):
    func = func_list[record.func_id]
    args = record.args_to_strs()

    filename, count = "", -1

    # Ignore the functions that may confuse later conditions test
    if "readlink" in func or "dir" in func:
        return filename, count

    # TODO: maybe change back later to include fwrite / fread
    #if "writev" in func or "readv" in func:
    #    filename, count = args[0], int(args[1])
    #    offset = offsetBook[filename][rank]
    #    offsetBook[filename][rank] += count
    #    update_end_of_file(rank, filename, endOfFile, offsetBook)
    #elif "fwrite" in func or "fread" in func:
    #    filename, size, count = args[3], int(args[1]), int(args[2])
    #    offset, count = offsetBook[filename][rank], size*count
    #    offsetBook[filename][rank] += count
    #    update_end_of_file(rank, filename, endOfFile, offsetBook)
    elif "pwrite" in func or "pread" in func:
        filename, count= args[0], int(args[2])
    elif "write" in func or "read" in func:
        filename, count = args[0], int(args[2])

    #elif "fprintf" in func:
    #    filename, count = args[0], int(args[1])
    #    offset = offsetBook[filename][rank]
    #    offsetBook[filename][rank] += count
    #    update_end_of_file(rank, filename, endOfFile, offsetBook)

    return filename, count


def handle_metadata_operations(record, offsetBook, func_list, closeBook, segmentBook, endOfFile):
    rank, func = record.rank, func_list[record.func_id]
    args = record.args_to_strs()

    # Ignore directory related operations
    if "dir" in func:
        return

    if "fopen" in func or "fdopen" in func:
        # TODO check fdopen
        filename = args[0]
    elif "open" in func:
        filename = args[0]
    elif "seek" in func:
        filename= args[0]
    elif "close" in func or "sync" in func:
        filename = args[0]


def ignore_files(filename):
    if not filename or filename == "":
        return True
    ignore_prefixes = ["/sys/", "/proc", "/p/lustre2/wang116/applications/ParaDis.v2.5.1.1/Copper/Copper_results/fluxdata/", "/etc/", "stdout", "stderr", "stdin"]
    for prefix in ignore_prefixes:
        if filename.startswith(prefix):
            return True
    if "pipe:" in filename:
        return True

    return False

def ignore_funcs(func):
    ignore = ["MPI", "H5", "writev"]
    for f in ignore:
        if f in func:
            return True
    return False


def build_offset_intervals(reader):
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

            if not ignore_funcs(func_list[record.func_id]):
                records.append(record)

    records = sorted(records, key=lambda x: x.tstart)

    for record in records:

        rank = record.rank
        func = func_list[record.func_id]

        #handle_metadata_operations(record, func_list)
        filename, count = handle_data_operations(record, func_list)

        if not ignore_files(filename):
            isRead = "read" in func
            if filename not in intervals:
                intervals[filename] = []
            intervals[filename].append( [rank, record.tstart, record.tend, count, isRead] )

    return intervals

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

            if posix:
                if not ignore_funcs(func):
                    records.append(record)
            else:
                if "MPI" in func:
                    records.append(record)

    records = sorted(records, key=lambda x: x.tstart)

    for record in records:

        rank = record.rank
        func = func_list[record.func_id]
        args = record.args_to_strs()
        filename = args[0]
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
                operation = "sync"
            elif "sync" in func:
                operation = "sync"
            else: continue
        else:
            if "write" in func:
                operation = "write"
            elif "read" in func:
                operation = "read"
            elif "open" in func:
                operation = "open"
            elif "close" in func:
                operation = "close"
            else: continue

        if not ignore_files(filename):
            if filename not in intervals:
                intervals[filename] = []
            intervals[filename].append([rank, record.tstart, record.tend, operation, count])

    return intervals