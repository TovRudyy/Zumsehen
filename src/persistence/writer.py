import dask.dataframe as dd
import pandas as pd


class Writer:
    def _write_if_rows(self, df, *args, **kwargs):
        if isinstance(df, dd.DataFrame):
            df = df.compute()
        if df.shape[0] > 0:
            df.to_hdf(*args, **kwargs)
        else:
            pass

    def dataframe_to_hdf5(self, file: str, df_state, df_event, df_comm):
        self._write_if_rows(df_state, file, key="States", format="table")
        self._write_if_rows(df_event, file, key="Events", format="table")
        self._write_if_rows(df_comm, file, key="Comm", format="table")

    def dataframe_to_excel(self, file: str, df_state, df_event, df_comm):
        writer = pd.ExcelWriter(file)
        df_state.to_excel(writer, sheet_name="States")
        df_event.to_excel(writer, sheet_name="Events")
        df_comm.to_excel(writer, sheet_name="Comm")
        writer.save()
