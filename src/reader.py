import logging
import os
from datetime import datetime

from src.TraceMetaData import TraceMetaData

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

PARAVER_FILE = "Paraver (.prv)"
PARAVER_MAGIC_HEADER = "#Paraver"


def header_parser(header):
    header = header.replace("#Paraver (", "").replace("at ", "")
    traceDate, _, other = header.partition("):")
    traceDate = datetime.strptime(traceDate, "%d/%m/%Y %H:%M")
    traceExecTime, _, other = other.partition(":")
    traceExecTime = int(traceExecTime[:-3])

    htraceNodes, _, other = other.partition("(")
    htraceCPUs, _, other = other.partition(")")
    htraceCPUs = htraceCPUs.split(",")
    traceNodes = []
    for i in range(len(htraceNodes)):
        # mucho mas eficiente (aqui no haria falta, pero los maps te pueden ser muy muy utiles en el futuro para
        # la eficiencia ya que cosas tan simples se compilan a C directamente
        traceNodes.append(list(map(lambda trace_cpu: int(trace_cpu), htraceCPUs[i])))

    appls_list, _, other = other[1:].partition(":")
    number_apps = int(appls_list)
    traceApps = []
    # ejemplo logging
    logger.debug(f"appl list: {other}")
    for i in range(number_apps):
        traceTasks, _, other = other.partition("(")
        number_tasks = int(traceTasks)
        config, _, other = other.partition(")")
        config = config.split(",")
        traceTasks = []
        for j in range(number_tasks):
            config_threads = config[j].split(":")
            threads = []
            for k in range(int(config_threads[0])):
                threads.append(int(config_threads[1]))

            traceTasks.append(threads)
    traceApps.append(traceTasks)

    return traceExecTime, traceDate, traceNodes, traceApps


def parse_file(file):
    with open("file") as f:
        header = f.readline()
        if PARAVER_MAGIC_HEADER not in header:
            logger.error(f"The file {file.name} is not a valid Paraver file!")

        traceName = os.path.basename(file.name)
        tracePath = os.path.abspath(file.name)
        traceType = PARAVER_FILE

        traceExecTime, traceDate, traceNodes, traceApps = header_parser(header)

        return TraceMetaData(traceName, tracePath, traceType, traceExecTime, traceDate, traceNodes, traceApps)
