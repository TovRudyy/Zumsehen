import itertools
import logging
import os
import time

import numpy as np
import pandas as pd

from src.persistence.format_converter import FormatConverter, chunk_reader, isplit

logging.basicConfig(format="%(levelname)s :: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


# import pyextrae.sequential as pyextrae

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
    "event_v",
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
MAX_READ_BYTES = int(os.environ.get("STEPS", GB * 2))

# For pre-allocating memory
MIN_ELEM = int(os.environ.get("STEPS", 40000000))

STEPS = int(os.environ.get("STEPS", 200000))
RESIZE = 1


class ParaverToHDF5(FormatConverter):
    @staticmethod
    def get_state_row(line):
        # We discard the record type field
        return np.array([int(x) for x in line.split(":")[1:]])

    @staticmethod
    def get_comm_row(line):
        # We discard the record type field
        return [int(x) for x in line.split(":")[1:]]

    @staticmethod
    def get_event_row(line):
        # We discard the record type field
        record = [int(x) for x in line.split(":")[1:]]
        # The same Event record line can contain more than 1 Event
        event_iter = iter(record[5:])
        return list(itertools.chain.from_iterable([record[:5] + [event, next(event_iter)] for event in event_iter]))

    def parse_records(self, chunk, *args):
        arr_state, arr_event, arr_comm = args
        stcount, evcount, commcount = 0, 0, 0
        # This is the padding between different records respectively
        stpadding, commpadding, evpadding = len(COL_STATE_RECORD), len(COL_COMM_RECORD), len(COL_EVENT_RECORD) * 10
        # The loop is divided in chunks of STEPS size
        for records in isplit(chunk, STEPS):
            for record in records:
                record_type = record[0]
                if record_type == STATE_RECORD:
                    state = ParaverToHDF5.get_state_row(record)
                    try:
                        arr_state[stcount : stcount + stpadding] = state
                    except ValueError:
                        logger.warning("Catched exception: 'arr_state' need more space. Handling it...")
                        arr_state = np.concatenate((arr_state, np.zeros(STEPS * stpadding * RESIZE)))
                        arr_state[stcount : stcount + stpadding] = state
                    stcount += stpadding
                elif record_type == EVENT_RECORD:
                    # EVENT is a special type because we don't know how
                    # long will be the returned list
                    events = ParaverToHDF5.get_event_row(record)
                    try:
                        arr_event[evcount : evcount + len(events)] = events
                    except ValueError:
                        logger.warning("Catched exception: 'arr_event' need more space. Handling it...")
                        arr_event = np.concatenate((arr_event, np.zeros(STEPS * evpadding * RESIZE)))
                        arr_event[evcount : evcount + len(events)] = events
                    evcount += len(events)
                elif record_type == COMM_RECORD:
                    comm = ParaverToHDF5.get_comm_row(record)
                    try:
                        arr_comm[commcount : commcount + commpadding] = comm
                    except ValueError:
                        logger.warning("Catched exception: 'arr_comm' need more space. Handling it...")
                        arr_comm = np.concatenate((arr_comm, np.zeros(STEPS * commpadding * RESIZE)))
                        arr_comm[commcount : commcount + commpadding] = comm
                    commcount += commpadding

            # Check if the arrays have enough free space for the next chunk iteration
            # If not, resize the arrays heuristically
            if (arr_state.size - stcount) < STEPS * stpadding:
                arr_state = np.concatenate((arr_state, np.zeros(STEPS * stpadding * RESIZE)))
            if (arr_event.size - evcount) < STEPS * evpadding:
                arr_event = np.concatenate((arr_event, np.zeros(STEPS * evpadding * RESIZE)))
            if (arr_comm.size - commcount) < STEPS * commpadding:
                arr_comm = np.concatenate((arr_comm, np.zeros(STEPS * commpadding * RESIZE)))

        # Remove the positions that have not been used when returning
        return arr_state[0:stcount], stcount, arr_event[0:evcount], evcount, arr_comm[0:commcount], commcount

    def seq_parser(self, chunk):
        start_time = time.time()
        # Pre-allocation of arrays
        arr_state = np.zeros(MIN_ELEM, dtype="int64")
        arr_event = np.zeros(MIN_ELEM, dtype="int64")
        arr_comm = np.zeros(MIN_ELEM, dtype="int64")

        arr_state, stcount, arr_event, evcount, arr_comm, commcount = self.parse_records(
            chunk, arr_state, arr_event, arr_comm
        )

        return arr_state, stcount, arr_event, evcount, arr_comm, commcount

    def parse_as_dataframe(self, file):
        """ Memory complexity: O_max(N+(3N*)), O_nominal(N). O_max could be 4*N/CHUNK if the algorithm wrote to disk after each CHUNK
            Computational complexity: O(N+c)
        """
        logger.debug(f"Using parameters: STEPS {STEPS}, MAX_READ_BYTES {MAX_READ_BYTES}, MIN_ELEM {MIN_ELEM}")
        start_time = time.time()
        # *count variables count how many elements we actually have
        arr_state, stcount, arr_event, evcount, arr_comm, commcount = (
            np.array([], dtype="int64"),
            0,
            np.array([], dtype="int64"),
            0,
            np.array([], dtype="int64"),
            0,
        )
        # This algorithm is a loop divided in chunks of MAX_READ_BYTES
        for chunk in chunk_reader(file, MAX_READ_BYTES):
            tmp_arr_state, tmp_stcount, tmp_arr_event, tmp_evcount, tmp_arr_comm, tmp_commcount = self.seq_parser(chunk)
            stcount, evcount, commcount = stcount + tmp_stcount, evcount + tmp_evcount, commcount + tmp_commcount
            # Join the temporal arrays with the main
            arr_state, arr_event, arr_comm = (
                np.concatenate((arr_state, tmp_arr_state)),
                np.concatenate((arr_event, tmp_arr_event)),
                np.concatenate((arr_comm, tmp_arr_comm)),
            )

        logger.info(f"TIMING (s) el_time_parser:".ljust(30, " ") + "{:.3f}".format(time.time() - start_time))
        logger.info(
            f"ARRAY MAX SIZES (MB): {arr_state.nbytes//(1024*1024)} | { arr_event.nbytes//(1024*1024)} | {arr_comm.nbytes//(1024*1024)}"
        )

        # Reshape the arrays
        arr_state, arr_event, arr_comm = (
            arr_state.reshape((stcount // len(COL_STATE_RECORD), len(COL_STATE_RECORD))),
            arr_event.reshape((evcount // len(COL_EVENT_RECORD), len(COL_EVENT_RECORD))),
            arr_comm.reshape((commcount // len(COL_COMM_RECORD), len(COL_COMM_RECORD))),
        )

        df_state = pd.DataFrame(data=arr_state, columns=COL_STATE_RECORD)
        df_event = pd.DataFrame(data=arr_event, columns=COL_EVENT_RECORD)
        df_comm = pd.DataFrame(data=arr_comm, columns=COL_COMM_RECORD)

        return df_state, df_event, df_comm
