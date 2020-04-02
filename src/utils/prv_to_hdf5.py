import itertools
import logging
import time
from concurrent.futures.process import ProcessPoolExecutor
from functools import reduce

import numpy as np

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)

STATE_RECORD = "1"
EVENT_RECORD = "2"
COMM_RECORD = "3"

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

MB = 1024 * 1024
GB = 1024 * 1024 * 1024
MAX_READ_BYTES = GB * 4
MAX_READ_BYTES_PARALLEL = MB * 50
BLOCK_LINE = 500000


def get_state_or_comm_row(line):
    # We discard the record type field
    return [int(x) for x in line.split(":")][1:]


def get_event_rows(line):
    # We discard the record type field
    row = list(map(int, line.split(":")))[1:]
    event_iter = iter(row[5:])
    # The same Event record line can contain more than 1 Event
    return [row[:5] + [event, next(event_iter)] for event in event_iter]


def get_record_type(line):
    # The record type should be a 1 size long numerical character
    return line[0]


def parse_lines_as_list(lines):
    lstate = []
    levent = []
    lcomm = []

    for line in lines:
        record_type = get_record_type(line)
        if record_type == STATE_RECORD or record_type == COMM_RECORD:
            record = get_state_or_comm_row(line)
            lstate.append(record)
        elif record_type == EVENT_RECORD:
            record = get_event_rows(line)
            levent.extend(record)
        else:
            pass
    return [lstate, levent, lcomm]


def parse_lines_to_nparray(lines):
    """ Parse 'lines' lines to numpy.array(s) in batches of 'block' lines"""
    parsed_lines = None
    for block_lines in isplit(lines, BLOCK_LINE):
        if parsed_lines is None:
            parsed_lines = [np.array(array) for array in parse_lines_as_list(block_lines)]
        else:
            parsed_lines = np.concatenate([parsed_lines, parse_lines_as_list(block_lines)])
    return parsed_lines


def chunk_reader(filename, read_bytes):
    with open(filename, "r") as f:
        # Discard the header
        f.readline()
        while True:
            chunk = f.readlines(read_bytes)
            if not chunk:
                break
            yield chunk


def seq_parse_as_dataframe(file):
    parsed_file = None
    start_time = time.time()
    for chunk in chunk_reader(file, MAX_READ_BYTES):
        if parsed_file is None:
            parsed_file = parse_lines_to_nparray(chunk)
        else:
            parsed_file = np.concatenate([parsed_file, parse_lines_to_nparray(chunk)])

    df_state = parsed_file[0]
    df_event = parsed_file[1]
    df_comm = parsed_file[2]

    print(f"Total time: {time.time() - start_time}")
    return df_state, df_event, df_comm


def parallel_parse_as_dataframe(file):
    start_time = time.time()
    with ProcessPoolExecutor() as executor:
        parsed_file = executor.map(parse_lines_to_nparray, chunk_reader(file, MAX_READ_BYTES_PARALLEL))

    parsed_file = reduce(np.concatenate, parsed_file)

    df_state = parsed_file[0]
    df_event = parsed_file[1]
    df_comm = parsed_file[2]

    print(f"Total time: {time.time() - start_time}")
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


# TRACE = "/home/orudyy/Repositories/Zumsehen/traces/bt-mz.2x2-+A.x.prv"
# TRACE_HUGE = "/home/orudyy/apps/OpenFoam-Ashee/traces/rhoPimpleExtrae10TimeSteps.01.chop1.prv"
TRACE = "/Users/adrianespejo/otros/Zusehen/traces/bt-mz.2x2-+A.x.prv"
# TRACE = "/home/orudyy/apps/OpenFoam-Ashee/traces/rhoPimpleExtrae40TimeSteps.prv"


def test():
    # TraceMetaData = parse_file(TRACE)
    df_state, df_event, df_comm = parallel_parse_as_dataframe(TRACE)
    # pd.set_option("display.max_rows", None)
    logger.info(f"\nResulting Event records data:\n {df_state.shape}")
    logger.info(f"\nResulting Event records data:\n {df_event.shape}")
    logger.info(f"\nResulting Comm. records data:\n {df_comm.shape}")

    # df_state2, df_event2, df_comm2 = load_as_dataframe(TRACE_HUGE)
    # print(df_comm == df_comm2)
    # print(df_state == df_state2)
    # print(df_event == df_event2)


if __name__ == "__main__":
    test()
