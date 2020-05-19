import pandas as pd

TRACES_DIR = "src/persistence/test/test_files/traces"

files_dir = "src/persistence/test/test_files/traces"
files = ["10MB.test.prv"]


def get_prv_test_traces():
    data = []
    for test in files:
        parsed_test = f"{files_dir}/{test[:-8]}parsed.hdf"
        sol_state = pd.read_hdf(parsed_test, key="States").astype("int64")
        sol_event = pd.read_hdf(parsed_test, key="Events").astype("int64")
        sol_comm = pd.read_hdf(parsed_test, key="Comm").astype("int64")
        data.append(
            {
                "Input": f"{files_dir}/{test}",
                "states_records": sol_state,
                "event_records": sol_event,
                "comm_records": sol_comm,
            }
        )
    return data


def assert_equals_if_rows(df1, df2):
    if df1.shape[0] != 0 and df2.shape[0] != 0:
        assert df1.equals(df2)
