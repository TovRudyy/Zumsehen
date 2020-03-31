import pandas as pd
from reader import parse_file
from TraceMetaData import TraceMetaData
import logging
logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)

STATE_RECORD    = 1
EVENT_RECORD    = 2
COMM_RECORD     = 3

TRACE = "/home/orudyy/Repositories/Zumsehen/traces/bt-mz.2x2-+A.x.prv"

def get_state_row(line):
    row = pd.DataFrame()
    return row

def get_event_row(line):
    row = pd.DateFrame()
    return row

def get_comm_row(line):
    row = pd.DateFrame()
    return row
    
def get_record_type(line):
    record_t, _, other = line.partition(":")
    return int(record_t)

def get_row(line):
    row = pd.DataFrame()
    record_t = get_record_type(line)
    if record_t == STATE_RECORD:
        row = get_state_row(line)
    elif record_t == EVENT_RECORD:
        row = get_event_row(line)
    elif record_t == COMM_RECORD:
        row = get_comm_row(line)
    else:
        logger.error(f"The file contains an invalid record type: {record_t}")
    
    return row, record_t
def load_as_dataframe(file):
    df_state_event = pd.DataFrame()
    df_comm = pd.DataFrame()
    with open(file) as f:
        f.readline()
        for numline, line in enumerate(f.readline()):
            row, record_t = get_row(line)
            if record_t == STATE_RECORD or record_t == EVENT_RECORD:
                df_state_event.append(row)
            elif record_t == COMM_RECORD:
                df_comm.append(row)
            else:
                # numline + 1 (header_line)
                logger.error(f"Fatal record type in line {numline+1}")
                            
    return df_state_event, df_comm   
    

TraceMetaData = parse_file(TRACE)
df = load_as_dataframe(TRACE)