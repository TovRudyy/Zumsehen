import datetime
import json
import logging
import re
import sys
from time import perf_counter

import pandas as pd

COL_STATE_EVENT_RECORD = [
    "record_type",
    "cpu_id",
    "app_id",
    "task_id",
    "thread_id",
    "time_ini",
    "time_fi",
    "state",
    "event_type",
    "event_value",
]
COL_COMM_RECORD = [
    "record_type",
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


def load_prv(tracefile):
    # read trace header
    headerLine = tracefile.readline().strip()
    # Paraver (dd/mm/yy at hh:mm):ftime:nNodes(nCPUs1,nCPUs2,...):nAppl:applicationList1:...
    traceinfo, resourceinfo = headerLine.split("):", 1)
    date, time = re.search(r"#Paraver \((.+) at (.+)", traceinfo).groups()
    date_time = datetime.datetime.strptime(date + "T" + time, "%d/%m/%YT%H:%M")
    ftime, nNodes, nAppl, applicationListsAndCommcount = resourceinfo.split(":", 3)

    # ftime has format "<time>_<unit>", allowed units are 'ns', 'us', 'ms'
    # @TODO: 'us' seems to be the default in paraver if not specified, which is not take into account here for now
    tracetime, tracetimeunit = ftime.split("_")
    tracetime = int(tracetime)

    # format is nNodes(nCPUs1,nCPUs2,...) so find all integers and we are done
    nNodes, *nCpus = re.findall(r"[0-9]+", nNodes)

    # format of applicationListsAndCommcount is
    # nTasks(nThreads1:node,...,nTasksN:node):nTasks(nThreads1:node,...):...,nComm
    applicationLists = re.findall(r"([0-9]+\(.+?\))[:,]?", applicationListsAndCommcount)
    applicationList = []
    for application in applicationLists:
        nTasks, nThreadsSpecs = re.search("([0-9]+)\((.+?)\)", application).groups()
        tasks_list = []
        for nThreadsSpec in nThreadsSpecs.split(","):
            nThreads, node = nThreadsSpec.split(":")
            tasks_list.append({"nthreads": int(nThreads), "node": int(node)})
        applicationList.append(tasks_list)
    numComms = re.search(r"([0-9]+)$", applicationListsAndCommcount).group(0)

    # @TODO: communicator specifications from paraver sourcecode is without documentation
    # communicators are in seperate lines with form
    # c:app_id:comm_id:num_tasks:task_id1:task_id2:...
    communicators = []
    for _ in range(int(numComms)):
        commLine = tracefile.readline().strip()
        record_type, app_id, comm_id, num_tasks, taskList = commLine.split(":", 4)
        if record_type not in ["C", "c", "I", "i"]:
            logging.critical("Expected communicator record but found record of type %s", record_type)
            logging.debug("Line was: %s", commLine)
            exit(1)
        communicators.append(
            {"app_id": int(app_id), "comm_id": int(comm_id), "task_ids": list(map(int, taskList.split(":")))}
        )

    header = {
        "date": date_time.isoformat(),
        "tracetime": int(tracetime),
        "tracetimeunit": tracetimeunit,
        "nNodes": int(nNodes),
        "nCPUs": list(map(int, nCpus)),
        "applicationList": applicationList,
        "communicators": communicators,
    }

    # read trace body
    # @TODO: need to implement storing data into dataframe(s)

    STATE_FIELDS = ["record_type", "cpu_id", "appl_id", "task_id", "thread_id", "time_ini", "time_fi", "state"]
    state_record_list = []

    EVENT_FIELDS = ["record_type", "cpu_id", "appl_id", "task_id", "thread_id", "time", "event_t", "event_v"]
    event_record_list = []

    COMMUNICATION_FIELDS = [
        "record_type",
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
    communication_record_list = []

    starttime_body = perf_counter()
    # fileoffset in tracefile is at first line of body after reading header data here
    for line in tracefile:
        if line[0] == "#":
            continue
        record = line.strip().split(":")
        record_type = record[0]
        # logging.debug("line to parse: %s", line.strip())
        # logging.debug('record type: %s', record_type)
        if record_type == "1":
            # state record
            # 1:cpu_id:appl_id:task_id:thread_id:begin_time:end_time:state
            # record_type, cpu_id, appl_id, task_id, thread_id, begin_time, end_time, state = record
            # logging.debug("%s "*7, cpu_id, appl_id, task_id, thread_id, begin_time, end_time, state)
            state_record_list.append(record)
        elif record_type == "2":
            # event record
            record_type, cpu_id, appl_id, task_id, thread_id, time, *events = record
            # logging.debug("%s "*5, cpu_id, appl_id, task_id, thread_id, time)
            event_iter = iter(events)
            for event_type in event_iter:
                event_value = next(event_iter)
                event_record_list.append(record[:6] + [event_type, event_value])
                # logging.debug("%s %s", event_type, event_value)
        elif record_type == "3":
            # communication record
            # record_type, \
            # cpu_send_id, ptask_send_id, task_send_id, thread_send_id, lsend, psend, \
            # cpu_recv_id, ptask_recv_id, task_recv_id, thread_recv_id, lrecv, precv, \
            # size, tag = record
            # logging.debug("%s "*14,
            #         cpu_send_id, ptask_send_id, task_send_id, thread_send_id, lsend, psend,
            #         cpu_recv_id, ptask_recv_id, task_recv_id, thread_recv_id, lrecv, precv,
            #         size, tag)
            communication_record_list.append(record)
        else:
            logging.warning('skipping record of unknown type "%s"', record_type)
            continue
    endtime_body = perf_counter()
    logging.info("Reading trace body took %s seconds", endtime_body - starttime_body)
    logging.info("# state records: %s", len(state_record_list))
    logging.info("# event records: %s", len(event_record_list))
    logging.info("# communication records: %s", len(communication_record_list))
    starttime_pandas = perf_counter()
    returnvalues = {
        "header": header,
        "state_records": pd.DataFrame(state_record_list, columns=STATE_FIELDS),
        "event_records": pd.DataFrame(event_record_list, columns=EVENT_FIELDS),
        "communication_records": pd.DataFrame(communication_record_list, columns=COMMUNICATION_FIELDS),
    }
    endtime_pandas = perf_counter()
    logging.info("Converting data to pandas DataFrames took %s seconds", endtime_pandas - starttime_pandas)
    return returnvalues


def write_to_excel(file, tracedata):
    writer = pd.ExcelWriter(f"{file}.parsed.xlsx")
    tracedata["state_records"].drop(columns=["record_type"]).to_excel(writer, sheet_name="States")
    tracedata["event_records"].drop(columns=["record_type"]).to_excel(writer, sheet_name="Events")
    tracedata["communication_records"].drop(columns=["record_type"]).to_excel(writer, sheet_name="Comm")
    writer.save()


def write_to_hdf(file, tracedata):
    file = f"{file}.parsed.hdf"
    tracedata["state_records"].drop(columns=["record_type"]).to_hdf(file, key="States")
    tracedata["event_records"].drop(columns=["record_type"]).to_hdf(file, key="Events")
    tracedata["communication_records"].drop(columns=["record_type"]).to_hdf(file, key="Comm")


def main(argv):
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO)
    filename = argv[1]
    with open(filename, "r") as tracefile:
        tracedata = load_prv(tracefile)
        print("HEADER")
        print("-" * 78)
        print(json.dumps(tracedata["header"]))
        print("STATES")
        print("-" * 78)
        print(tracedata["state_records"])
        print("EVENTS")
        print("-" * 78)
        print(tracedata["event_records"])
        print("COMMUNICATION")
        print("-" * 78)
        print(tracedata["communication_records"])

    write_to_hdf(argv[2], tracedata)
    return 0


if "__main__" == __name__:
    main(sys.argv)
