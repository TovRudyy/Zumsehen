import itertools
import logging
import time
from concurrent.futures.process import ProcessPoolExecutor
from functools import reduce

import pandas as pd
import numpy as np
import progressbar

from src.reader import parse_file

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)

STATE_RECORD = 1
EVENT_RECORD = 2
COMM_RECORD = 3

COL_STATE_RECORD = [
    "cpu_id",
    "appl_id",
    "task_id",
    "thread_id",
    "time_ini",
    "time_fi",
    "state",
]
COL_EVENT_RECORD = [
    "cpu_id",
    "appl_id",
    "task_id",
    "thread_id",
    "time",
    "event_t",
    "event_val",
]
COL_COMM_RECORD = [
    "cpu_send_id",
    "ptask_send_id",
    "task_send_id",
    "thread_send_id",
    "lsend",
    "psend",
    "cpu_recv_id",
    "ptask_recv_id",
    "task_recv_id",
    "thread_recv_id",
    "lrecv",
    "precv",
    "size",
    "tag",
]

MAX_READ_BYTES = None
# TRACE = "/home/orudyy/Repositories/Zumsehen/traces/bt-mz.2x2-+A.x.prv"
# TRACE_HUGE = "/home/orudyy/apps/OpenFoam-Ashee/traces/rhoPimpleExtrae10TimeSteps.01.chop1.prv"


TRACE = "/Users/adrianespejo/otros/Zusehen/traces/bt-mz.2x2-+A.x.prv"


# TRACE = "/home/orudyy/apps/OpenFoam-Ashee/traces/rhoPimpleExtrae40TimeSteps.prv"
def get_state_row(line):
    # We discard the record type field
    return list(map(int, line.split(":")))[1:]


def get_event_rows(line):
    # We discard the record type field
    row = list(map(int, line.split(":")))[1:]
    event_iter = iter(row[5:])
    # The same Event record line can contain more than 1 Event
    return [row[:5] + [event, next(event_iter)] for event in event_iter]


def get_comm_row(line):
    # We discard the record type fied
    return list(map(int, line.split(":")))[1:]


def get_record_type(line):
    # The record type should be a 1 size long numerical character
    record_t = line[0]
    # Some records in the trace are not numerical
    try:
        record_t = int(record_t)
    except ValueError:
        return record_t
    return record_t


def get_row(line):
    row = []
    record_t = get_record_type(line)
    if record_t == STATE_RECORD:
        row = get_state_row(line)
    elif record_t == EVENT_RECORD:
        row = get_event_rows(line)
    elif record_t == COMM_RECORD:
        row = get_comm_row(line)
    else:
        logger.warning(f"The line contains an invalid record type: {record_t}")

    return row, record_t


def read_lines(file, bytes=None):
    ''' Try to read 'bytes' bytes if defined, if not, read the entire file '''
    start_position = file.tell()
    if bytes is None:
        start_time = time.time()
        lines = file.readlines()
        elapsed = time.time() - start_time
        size = (file.tell() - start_position) / (1024 * 1024)
    else:
        start_time = time.time()
        lines = file.readlines(bytes)
        elapsed = time.time() - start_time
        size = bytes / (1024 * 1024)

    logger.info(
        f"Has been read {'{:,.0f}'.format(size)} MB in {'{:.2f}'.format(elapsed)} seconds ({'{:.3f}'.format(size / elapsed)} MB/s)"
    )
    return lines


# TODO: concatenate blocks of rows might be more efficient than append one by one
def load_as_dataframe(file):
    # df_state_event = pd.DataFrame(columns=COL_STATE_EVENT_RECORD)
    # df_comm = pd.DataFrame(columns=COL_COMM_RECORD)
    df_state = []
    df_event = []
    df_comm = []
    with open(file, "r") as f:
        # Discard the header
        f.readline()
        # Read records
        lines = read_lines(f, bytes=MAX_READ_BYTES)
        start_time = time.time()
        for line in progressbar.progressbar(lines, max_value=len(lines)):
            row, record_t = get_row(line)
            if record_t == STATE_RECORD:
                df_state.append(row)
            elif record_t == EVENT_RECORD:
                df_event.extend(row)
            elif record_t == COMM_RECORD:
                df_comm.append(row)
            else:
                logger.warning(f"Invalid record type, skipping...")
        logger.info(f"Elapsed parsing time: {time.time() - start_time}")

    start_time = time.time()
    df_state = pd.DataFrame(df_state, columns=COL_STATE_RECORD)
    df_event = pd.DataFrame(df_event, columns=COL_EVENT_RECORD)
    df_comm = pd.DataFrame(df_comm, columns=COL_COMM_RECORD)
    logger.info(f"Elapsed time converting to dataframe sequentially: {time.time() - start_time}")

    return df_state, df_event, df_comm


def isplit(iterable, part_size, group=list):
    """ Yields groups of length `part_size` of items found in iterator.
    group is a constructor which transforms an iterator into a object
    with `part_size` or less elements (example: list, tuple or set)
    """
    iterator = iter(iterable)
    while True:
        tmp = group(itertools.islice(iterator, 0, part_size))
        if not tmp:
            return
        yield tmp


def get_records(line_chunk):
    df_state = []
    df_event = []
    df_comm = []
    for line in line_chunk:
        row, record_t = get_row(line)
        if record_t == STATE_RECORD:
            df_state.append(row)
        elif record_t == EVENT_RECORD:
            df_event.extend(row)
        elif record_t == COMM_RECORD:
            df_comm.append(row)
        else:
            pass
    return df_state, df_event, df_comm


def reduce_dfs(chunk_a, chunk_b):
    chunk_a[0].extend(chunk_b[0])
    chunk_a[1].extend(chunk_b[1])
    chunk_a[2].extend(chunk_b[2])
    return chunk_a


def load_as_dataframe2(file):
    with open(file, "r") as f:
        # Discard the header
        f.readline()
        # Read records
        lines = read_lines(f, bytes=MAX_READ_BYTES)
    part_size = 1000
    start_time = time.time()
    with ProcessPoolExecutor() as executor:
        parsed = executor.map(get_records, isplit(lines, part_size))

    state_result, event_result, comm_result = reduce(reduce_dfs, parsed)

    # logger.info(f"Elapsed parsing time: {time.time() - start_time}")

    start_time = time.time()
    df_state = pd.DataFrame(state_result, columns=COL_STATE_RECORD)
    df_event = pd.DataFrame(event_result, columns=COL_EVENT_RECORD)
    df_comm = pd.DataFrame(comm_result, columns=COL_COMM_RECORD)
    logger.info(f"Elapsed time converting to dataframe in parallel: {time.time() - start_time}")

    return df_state, df_event, df_comm


def test():
    # TraceMetaData = parse_file(TRACE)
    df_state, df_event, df_comm = load_as_dataframe2(TRACE)
    # pd.set_option("display.max_rows", None)
    logger.info(f"\nResulting Event records data:\n {df_state.shape}")
    logger.info(f"\nResulting Event records data:\n {df_event.shape}")
    logger.info(f"\nResulting Comm. records data:\n {df_comm.shape}")

    df_state2, df_event2, df_comm2 = load_as_dataframe(TRACE)
    print(df_comm == df_comm2)
    print(df_state == df_state2)
    print(df_event == df_event2)


if __name__ == "__main__":
    test()
