import itertools

from src import Trace


def get_table_data(trace: Trace):
    cols_header = ["MPI_call", "MPI_exec", "MPI_otracosa"]
    table_data = [
        ["Thread 1.1", 1., 2., 1.4],
        ["Thread 1.2", 4., 3., 7.],
        ["Thread 2.1", 0.5, 2., 1.],
        ["Thread 2.2", 5., 1., 6.4]
    ]

    flattened_data = list(itertools.chain.from_iterable(table_data))
    min_value = min([value for value in flattened_data if isinstance(value, (float, int))])
    max_value = max([value for value in flattened_data if isinstance(value, (float, int))])
    return cols_header, table_data, min_value, max_value
