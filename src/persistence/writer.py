import pandas as pd


class Writer:
    def dataframe_to_hdf5(self, file, df_state, df_event, df_comm):
        writer = pd.HDFStore(file, complevel=9, complib="zlib")
        writer.put("States", df_state)
        writer.put("Events", df_event)
        writer.put("Comms", df_comm)
        writer.close()

    def dataframe_to_excel(self, file, df_state, df_event, df_comm):
        writer = pd.ExcelWriter(file)
        df_state.to_excel(writer, sheet_name="States")
        df_event.to_excel(writer, sheet_name="Events")
        df_comm.to_excel(writer, sheet_name="Comms")
        writer.save()
