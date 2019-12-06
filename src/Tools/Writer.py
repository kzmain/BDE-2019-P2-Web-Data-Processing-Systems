import xlsxwriter

class Writer():
    @staticmethod
    def excel_writer(out_file, df):
        df.toPandas().to_csv(out_file, index=False)
