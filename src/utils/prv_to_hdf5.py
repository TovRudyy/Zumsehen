import logging
import sys
import time

import pandas as pd
from reader import parse_file
from TraceMetaData import TraceMetaData

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)


STATE_RECORD = 1
EVENT_RECORD = 2
COMM_RECORD = 3

COL_STATE_EVENT_RECORD = [
    "record_t",
    "cpu_id",
    "appl_id",
    "task_id",
    "thread_id",
    "time_ini",
    "time_fi",
    "state",
    "event_t",
    "event_val",
]
COL_COMM_RECORD = [
    "record_t",
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

TRACE = "/home/orudyy/Repositories/Zumsehen/traces/bt-mz.2x2-+A.x.prv"
# TRACE = "/home/orudyy/apps/OpenFoam-Ashee/traces/rhoPimpleExtrae40TimeSteps.prv"
def get_state_rows(line):
    rows = []
    row = list(map(int, line.split(":"))) + [None, None]
    rows.append(row)
    logger.debug(f"Parsed state rows: {rows}")
    return rows


def get_event_rows(line):
    """ This function can return various rows because
    one event record line can contain multiple events """
    rows = []
    row = list(map(int, line.split(":")))
    row = row[:6] + [row[5], None] + row[6:]
    # rows = [row[0:7] + list(event_t, event_v) for event_t, event_v in row[8:]]
    event_iter = iter(row[8:])
    for event_type in event_iter:
        aux = row[:8] + [event_type, next(event_iter)]
        rows.append(aux)
    logger.debug(f"Parsed event rows: {rows}")
    return rows


def get_comm_row(line):
    row = list(map(int, line.split(":")))
    logger.warning(f"Parsed comm. rows: {row}")
    return row


def get_record_type(line):
    record_t, _, other = line.partition(":")
    # Some records in the trace are not numerical
    try:
        record_t = int(record_t)
    except:
        return record_t
    return record_t


def get_rows(line):
    row = []
    record_t = get_record_type(line)
    if record_t == STATE_RECORD:
        row = get_state_rows(line)
    elif record_t == EVENT_RECORD:
        row = get_event_rows(line)
    elif record_t == COMM_RECORD:
        row = get_comm_row(line)
    else:
        logger.warning(f"The line contains an invalid record type: {record_t}")

    return row, record_t


def read_lines(file, bytes=None):
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
        f"Has been read {'{:,.0f}'.format(size)} MB in {'{:.2f}'.format(elapsed)} seconds ({'{:.3f}'.format(size/elapsed)} MB/s)"
    )
    return lines


# TODO: concatenate blocks of rows might be more efficient than append one by one
def load_as_dataframe(file):
    df_state_event = pd.DataFrame(columns=COL_STATE_EVENT_RECORD)
    df_comm = pd.DataFrame(columns=COL_COMM_RECORD)
    with open(file, "r") as f:
        # Discard the header
        f.readline()
        # Read records
        lines = read_lines(f)
        start_time = time.time()
        for numline, line in enumerate(lines):
            logger.info(f"Parsing line {numline+2}: {line[:-1]}")
            rows, record_t = get_rows(line)
            if record_t == STATE_RECORD or record_t == EVENT_RECORD:
                df_state_event = df_state_event.append(
                    pd.DataFrame(rows, columns=COL_STATE_EVENT_RECORD), ignore_index=True
                )
            elif record_t == COMM_RECORD:
                df_comm = df_comm.append(rows)
            else:
                # numline + 1 (header_line)
                logger.warning(f"Invalid record type, skipping...")
        logger.info(f"Elapsed parsing time: {time.time()-a}")
    return df_state_event, df_comm


def test():
    TraceMetaData = parse_file(TRACE)
    df_state_event, df_comm = load_as_dataframe(TRACE)
    pd.set_option("display.max_rows", None)
    logger.debug(f"\nResulting State and Event records data:\n {df_state_event}")
    logger.debug(f"\nResulting Comm. records data:\n{df_comm}")

    test()
