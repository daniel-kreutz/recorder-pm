#!/usr/bin/env python
# encoding: utf-8
def handle_data_operations(record, offsetBook, func_list, endOfFile):

    def update_end_of_file(rank, filename, endOfFile, offsetBook):
        if filename in endOfFile and filename in offsetBook:
            endOfFile[filename][rank] = max(endOfFile[filename][rank], offsetBook[filename][rank])
        elif filename not in endOfFile:
            endOfFile[filename] = [0] * len(offsetBook[0])
            endOfFile[filename][rank] = offsetBook[filename][rank]
        else:
            print("Not possible: ", rank, filename)
            endOfFile[filename][rank] = 0

    func = func_list[record.func_id]
    rank, args = record.rank, record.args_to_strs()

    filename, offset, count = "", -1, -1

    # Ignore the functions that may confuse later conditions test
    if "readlink" in func or "dir" in func:
        return filename, offset, count

    if "writev" in func or "readv" in func:
        filename, count = args[0], int(args[1])
        offset = offsetBook[filename][rank]
        offsetBook[filename][rank] += count
        update_end_of_file(rank, filename, endOfFile, offsetBook)
    elif "fwrite" in func or "fread" in func:
        filename, size, count = args[3], int(args[1]), int(args[2])
        offset, count = offsetBook[filename][rank], size*count
        offsetBook[filename][rank] += count
        update_end_of_file(rank, filename, endOfFile, offsetBook)
    elif "pwrite" in func or "pread" in func:
        filename, count, offset = args[0], int(args[2]), int(args[3])
        update_end_of_file(rank, filename, endOfFile, offsetBook)
    elif "write" in func or "read" in func:
        filename, count = args[0], int(args[2])
        offset = offsetBook[filename][rank]
        offsetBook[filename][rank] += count
        update_end_of_file(rank, filename, endOfFile, offsetBook)
    elif "fprintf" in func:
        filename, count = args[0], int(args[1])
        offset = offsetBook[filename][rank]
        offsetBook[filename][rank] += count
        update_end_of_file(rank, filename, endOfFile, offsetBook)

    return filename, offset, count


def handle_metadata_operations(record, offsetBook, func_list, closeBook, segmentBook, endOfFile):

    def get_latest_offset(filename, rank, closeBook, endOfFile):
        # Every filename should be in endOfFile becuase we initialized it at the begining
        if filename not in closeBook and filename not in endOfFile:
            print("Encounter an unknown filename "+filename+". Should abort!")
            return 0
        if filename in closeBook:
            return max(endOfFile[filename][rank], closeBook[filename])
        else:
            return endOfFile[filename][rank]

    def create_new_segment(filename, rank, segmentBook):
        newSegmentID = 0
        if filename in segmentBook and len(segmentBook[filename]) > 0:
            newSegmentID = 1+segmentBook[filename][-1][1]
        if filename not in segmentBook:
            segmentBook[filename] = []
        segmentBook[filename].append([rank, newSegmentID, False])

    rank, func = record.rank, func_list[record.func_id]
    args = record.args_to_strs()

    # Ignore directory related operations
    if "dir" in func:
        return

    if "fopen" in func or "fdopen" in func:
        # TODO check fdopen
        filename = args[0]
        offsetBook[filename][rank] = 0
        openMode = args[1]
        if 'a' in openMode:
            offsetBook[filename][rank] = get_latest_offset(filename, rank, closeBook, endOfFile)
        create_new_segment(filename, rank, segmentBook)
    elif "open" in func:
        filename = args[0]
        offsetBook[filename][rank] = 0
        openMode = int( args[1] )
        if openMode == 2:  # TODO need  a better way to test for O_APPEND
            offsetBook[filename][rank] = get_latest_offset(filename, rank, closeBook, endOfFile)
        create_new_segment(filename, rank, segmentBook)
    elif "seek" in func:
        filename, offset, whence = args[0], int(args[1]), int(args[2])

        if whence == 0:     # SEEK_SET
            offsetBook[filename][rank] = offset
        elif whence == 1:   # SEEK_CUR
            offsetBook[filename][rank] += offset
        elif whence == 2:   # SEEK_END
            offsetBook[filename][rank] = get_latest_offset(filename, rank, closeBook, endOfFile)

    elif "close" in func or "sync" in func:
        filename = args[0]
        closeBook[filename] = endOfFile[filename][rank]

        # 1. Close all segments on the local process for this file
        for i in range(len(segmentBook[filename])):
            if segmentBook[filename][i][0] == rank:
                segmentBook[filename][i][2] = True
        # 2. And starts a new segment for all other processes have the same file opened
        # Skip this step for session semantics
        visitedRanks = set()
        tmpSegments = []
        # [::-1] check the most recent unclosed segment to get the largest segmentId
        for segment in segmentBook[filename][::-1]:
            if segment[0] in visitedRanks:
                continue
            if segment[0] != rank and not segment[2]:
                tmpSegments.append([segment[0], 1+segment[1], False])
                visitedRanks.add(segment[0])
        segmentBook[filename] = segmentBook[filename] + tmpSegments


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

    closeBook = {}  # Keep track the most recent close function and its file size so a later append operation knows the most recent file size
    segmentBook = {}    # segmentBook[filename] maintains all segments for filename, it is a list of list (rank, segment-id, closed)
    offsetBook = {}
    endOfFile = {}  # endOfFile[filename][rank] keep tracks the end of file, only the local rank can see it. When close/fsync, the value is stored in closeBook so other rank can see it.
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
                records.append( record )

    records = sorted(records, key=lambda x: x.tstart)

    segmentBook["stdin"] = []
    segmentBook["stderr"] = []
    segmentBook["stdout"] = []
    endOfFile["stdin"] = [0] * ranks
    endOfFile["stderr"] = [0] * ranks
    endOfFile["stdout"] = [0] * ranks

    for rank in range(ranks):
        LM = reader.LMs[rank]
        for filename in LM.filemap:
            segmentBook[filename] = []
            endOfFile[filename] = [0] * ranks
            offsetBook[filename] = [0] * ranks


    for record in records:

        rank = record.rank
        func = func_list[record.func_id]

        handle_metadata_operations(record, offsetBook, func_list, closeBook, segmentBook, endOfFile)
        filename, offset, count = handle_data_operations(record, offsetBook, func_list, endOfFile)

        if not ignore_files(filename):
            isRead = "read" in func
            if filename not in intervals:
                intervals[filename] = []

            # segments[0] is the local segment, the others are remote segments
            segments = []
            # 1. Add local segment
            # Find the most recent unclosed local segment
            for segment in segmentBook[filename][::-1]:
                if segment[0] == rank and not segment[2]:
                    segments.append(segment[1])

            # 2. Add all remote segments
            for segment in segmentBook[filename]:
                if segment[0] != rank and not segment[2]:
                    segments.append(segment[1])
            intervals[filename].append( [rank, record.tstart, record.tend, offset, count, isRead, segments] )

    return intervals

# only return record data of write / read calls by MPI / HDF5
def build_interface_intervals(reader):
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

            # only get functions that are not used in build_offset_intervals
            if ignore_funcs(func_list[record.func_id]):
                records.append( record )

    records = sorted(records, key=lambda x: x.tstart)

    for record in records:

        rank = record.rank
        func = func_list[record.func_id]
        args = record.args_to_strs()
        filename = ""
        # ignore MPI_File_open / close
        if "read" in func or "write" in func:
            filename = args[0]
        else: continue

        if not ignore_files(filename):
            isRead = "read" in func
            if filename not in intervals:
                intervals[filename] = []
            intervals[filename].append( [rank, record.tstart, record.tend, isRead] )

    return intervals