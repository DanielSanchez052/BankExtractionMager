import camelot
import pandas as pd
from typing import List, Optional
from logger import setup_logger
import pypdf


class PDFExtractor:
    def __init__(self, logger=None):
        """
        Inicializa el extractor de PDFs

        Args:
            logger: Logger opcional para registrar el proceso
        """
        self.logger = logger or setup_logger("pdf_extractor")

    def check_pdf_access(self, file_path: str, password: Optional[str] = None) -> bool:
        """
        Verifica si se puede acceder al PDF

        Args:
            file_path: Ruta al archivo PDF
            password: Contraseña opcional del PDF

        Returns:
            bool: True si se puede acceder al PDF, False en caso contrario
        """
        try:
            tables = camelot.read_pdf(
                file_path,
                password=password,
                pages='1',
                flavor='lattice'
            )
            return len(tables) > 0
        except Exception as e:
            self.logger.error(f"Error al acceder al PDF {file_path}: {str(e)}")
            return False

    def extract_tables(
        self,
        file_path: str,
        password: Optional[str] = None,
        pages: str = 'all',
        flavor: str = 'lattice',
        table_areas: Optional[List[str]] = None
    ) -> List[pd.DataFrame]:
        """
        Extrae tablas de un archivo PDF

        Args:
            file_path: Ruta al archivo PDF
            password: Contraseña opcional del PDF
            pages: Páginas a procesar ('all' o rango específico)
            flavor: Método de extracción ('lattice' o 'stream')
            table_areas: Áreas específicas donde buscar tablas

        Returns:
            List[pd.DataFrame]: Lista de DataFrames con las tablas extraídas
        """
        try:
            self.logger.info(f"Extrayendo tablas de {file_path}")

            # Configurar parámetros de extracción
            kwargs = {
                'password': password,
                'pages': pages,
                'flavor': flavor
            }

            if table_areas:
                kwargs['table_areas'] = table_areas

            # Extraer tablas
            tables = camelot.read_pdf(file_path, **kwargs)

            if len(tables) == 0:
                self.logger.warning(f"No se encontraron tablas en {file_path}")
                return []

            # Convertir tablas a DataFrames
            dfs = []
            for i, table in enumerate(tables):
                df = table.df
                # Limpiar el DataFrame
                df = self._clean_dataframe(df)
                dfs.append(df)
                self.logger.info(f"Tabla {i + 1} extraída con {len(df)} filas")

            return dfs

        except Exception as e:
            self.logger.error(f"Error al extraer tablas de {file_path}: {str(e)}")
            raise

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia un DataFrame extraído

        Args:
            df: DataFrame a limpiar

        Returns:
            pd.DataFrame: DataFrame limpio
        """
        # Eliminar filas y columnas vacías
        df = df.replace('', pd.NA).dropna(how='all', axis=0).dropna(how='all', axis=1)

        # # Usar la primera fila como encabezados si es necesario
        # if df.iloc[0].str.contains('|'.join(['Fecha', 'Descripción', 'Importe'])).any():
        #     df.columns = df.iloc[0]
        #     df = df.iloc[1:]

        # Limpiar valores
        df = df.replace(r'^\s*$', pd.NA, regex=True)
        df = df.fillna('')

        return df

    def get_text(self, file: str, password: str = None, pages: int = -1, *args, **kwargs):
        pdfFile = pypdf.PdfReader(file)
        if pdfFile.is_encrypted:
            pdfFile.decrypt(password)

        if pages < 0:
            num_pages = pdfFile.get_num_pages()
        else:
            num_pages = pages - 1

        text = ''.join([pdfFile.get_page(page).extract_text() for page in range(num_pages)])
        return text
