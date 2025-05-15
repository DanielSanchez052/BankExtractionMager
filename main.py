from dataclasses import dataclass
import datetime
import report as reportmod


@dataclass
class FileToExprot:
    file_path: str
    password: str = None
    stamp: str = None

    def __post_init__(self):
        if self.stamp is None:
            now = datetime.datetime.now()
            self.stamp = now.strftime("%Y%m%d_%H%M%S")


def export_pdfs_to_json(pdf_path: str, password_pdf: str = None) -> None:
    try:
        report = reportmod.Report(pdf_path, password_pdf)
        report.extract_tables()

        now = datetime.datetime.now()
        stamp = now.strftime("%Y%m%d_%H%M%S")
        print(f"Fecha y hora de extracción: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Nombre del archivo PDF: {pdf_path}")

        for key, value in report.tables.items():
            print(f"exportando tabla {key}")
            value.to_json(f"output/{stamp}_{key}.json", orient="records", force_ascii=True)

    except Exception as e:
        print(f"Ocurrió un error: {e}")
        print("Asegúrate de tener las dependencias correctas instaladas (ej. Ghostscript para Camelot).")
        print("También verifica que la contraseña sea correcta y el PDF no esté corrupto.")


if __name__ == "__main__":
    files = [FileToExprot()]

    for file in files:
        export_pdfs_to_json(file.file_path, file.password)
        print(f"Archivo exportado: {file.file_path} con contraseña: {file.password}")
        print(f"Marca de tiempo: {file.stamp}")
