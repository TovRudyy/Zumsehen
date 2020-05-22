from enum import Enum


class Record(Enum):
    cpu_id = 1
    appl_id = 2
    task_id = 3
    thread_id = 4
    time_ini = 5
    time_fi = 6
    state = 7
    time = 8
    event_t = 9
    event_v = 10
    cpu_send_id = 11
    ptask_send_id = 12
    task_send_id = 13
    thread_send_id = 14
    lsend = 15
    psend = 16
    cpu_recv_id = 17
    ptask_recv_id = 18
    task_recv_id = 19
    thread_recv_id = 20
    lrecv = 21
    precv = 22
    size = 23
    tag = 24

    @staticmethod
    def _filter_attributes():
        return (
            Record.state,
            Record.event_t,
            Record.event_v,
            Record.thread_id,
            Record.lsend,
            Record.psend,
            Record.size,
            Record.time,
        )

    @staticmethod
    def filter_attributes():
        return [attr.name for attr in Record._filter_attributes()]

    @property
    def can_filter(self):
        return self in Record._filter_attributes()

    @staticmethod
    def _group_attributes():
        return Record.cpu_id, Record.appl_id, Record.task_id, Record.thread_id

    @staticmethod
    def group_attributes():
        return [attr.name for attr in Record._group_attributes()]

    @property
    def can_group(self):
        return self in Record._group_attributes()


class StateRecord(Enum):
    cpu_id = Record.cpu_id
    appl_id = Record.appl_id
    task_id = Record.task_id
    thread_id = Record.thread_id
    time_ini = Record.time_ini
    time_fi = Record.time_fi
    state = Record.state

    @staticmethod
    def all_attributes():
        return [attr.name for attr in StateRecord]


class EventRecord(Enum):
    cpu_id = Record.cpu_id
    appl_id = Record.appl_id
    task_id = Record.task_id
    thread_id = Record.thread_id
    time = Record.time
    event_t = Record.event_t
    event_v = Record.event_v

    @staticmethod
    def all_attributes():
        return [attr.name for attr in EventRecord]


class CommRecord(Enum):
    cpu_send_id = Record.cpu_send_id
    ptask_send_id = Record.ptask_send_id
    task_send_id = Record.task_send_id
    thread_send_id = Record.thread_send_id
    lsend = Record.lsend
    psend = Record.psend
    cpu_recv_id = Record.cpu_recv_id
    ptask_recv_id = Record.ptask_recv_id
    task_recv_id = Record.task_recv_id
    thread_recv_id = Record.thread_recv_id
    lrecv = Record.lrecv
    precv = Record.precv
    size = Record.size
    tag = Record.tag

    @staticmethod
    def all_attributes():
        return [attr.name for attr in CommRecord]
