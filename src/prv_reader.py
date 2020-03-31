import logging
import re
import sys


def load_prv(filename):
    with open(filename, 'r') as fp:
        # read trace header
        header = fp.readline().strip()
        #Paraver (dd/mm/yy t hh:mm):ftime:nNodes(nCPUs1,nCPUs2,...):nAppl:allicationList1:...
        traceinfo, *resourceinfo = header.split('):', 1)
        date, time = re.search(r'#Paraver \((.+) at (.+)', traceinfo).groups()
        print(f"date: {date}")
        print(f"time: {time}")

        # read trace body
        for line in fp:
            record = line.strip().split(':')
            record_type = record[0]
            logging.debug("line to parse: %s", line.strip())
            logging.info('record type: %s', record_type)
            if record_type == '1':
                # state record
                # 1:cpu_id:appl_id:task_id:thread_id:begin_time:end_time:state
                _, cpu_id, appl_id, task_id, thread_id, begin_time, end_time, stat = record
                print(cpu_id, appl_id, task_id, thread_id, begin_time, end_time, stat)
            elif record_type == '2':
                # event record
                _, cpu_id, appl_id, task_id, thread_id, time, *events = record
                logging.debug("%s "*5, cpu_id, appl_id, task_id, thread_id, time)
                event_iter = iter(events)
                for event_type in event_iter:
                    event_value = next(event_iter)
                    print(event_type, event_value)
            elif record_type == '3':
                # communication record
                _, \
                cpu_send_id, ptask_send_id, task_send_id, thread_send_id, lsend, psend, \
                cpu_recv_id, ptask_recv_id, task_recv_id, thread_recv_id, lrecv, precv, \
                size, tag = record
                logging.debug("%s "*14,
                        cpu_send_id, ptask_send_id, task_send_id, thread_send_id, lsend, psend,
                        cpu_recv_id, ptask_recv_id, task_recv_id, thread_recv_id, lrecv, precv,
                        size, tag)
            else:
                logging.warning('skipping record of unknown type "%s"', record_type)
                continue


def main(argv):
    logging.basicConfig(level=logging.DEBUG)
    load_prv(argv[1])

if '__main__' == __name__:
    main(sys.argv)
