#!/usr/bin/env python
# encoding: utf-8

import sys
from recorder_pm import RecorderReader

reader = RecorderReader(sys.argv[1])

for rank in range(reader.GM.total_ranks):
    LM = reader.LMs[rank]
    print("Rank: %d, Number of trace records: %d" %(rank, LM.total_records))
