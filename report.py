import pdf
import pandas as pd

TRANSACTIONS_TABLE_COLUMN_NAMES = [
    "Movimiento",
    "Fecha Operacion",
    "Fecha Valor",
    "Concepto",
    "Cargos",
    "Abonos",
    "Saldo"
]

RESUME_TABLE_COLUMN_NAMES = [
    "Resumen Movimientos",
    "Nro",
    "Valor",
    "Descripcion",
    "Valor1"
]


class Report():
    def __init__(self, filepath: str, password: str) -> None:
        self.filepath: str = filepath
        self.password: str = password

        self.transactions_table = None
        self.resume_table = None
        self.stamp = None
        self.tables: dict = {}

    def extract_tables(self):
        tables_readed = pdf.read_pdf(self.filepath, self.password)
        if tables_readed.n > 0:
            for index, table in enumerate(tables_readed):
                df = table.df
                if len(df.columns) == len(TRANSACTIONS_TABLE_COLUMN_NAMES):
                    self.tables["transactions"] = self.prepare_df(df, TRANSACTIONS_TABLE_COLUMN_NAMES)
                elif len(df.columns) == len(RESUME_TABLE_COLUMN_NAMES):
                    self.tables["resume"] = self.prepare_df(df, RESUME_TABLE_COLUMN_NAMES)
                else:
                    self.tables[f"table_{index}"] = self.prepare_df(df)

            print("Extracción de tablas completada.")
            print(f"Se encontraron {len(self.tables)} tablas en el PDF.")
            print(f"Tablas extraídas: {self.tables.keys()}")
        else:
            print("Extracción de tablas completada.")
            print("No se encontraron tablas en el PDF.")

    def prepare_df(self, table: pd.DataFrame = None, columns: dict = None) -> pd.DataFrame:

        if table is None:
            return None

        self.transactions_table = table
        if self.transactions_table is not None:

            if columns is not None:
                self.transactions_table.columns = columns  # Renombra las columnas

            self.transactions_table = self.transactions_table.dropna(how='all')  # Elimina filas vacías
            self.transactions_table.reset_index(drop=True, inplace=True)  # Reinicia el índice

        return self.transactions_table
