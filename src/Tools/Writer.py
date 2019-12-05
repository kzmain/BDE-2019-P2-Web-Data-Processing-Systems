import xlsxwriter


class Writer():
    @staticmethod
    def excel_writer(out_file, df):
        df.to_excel(out_file, engine='xlsxwriter')
