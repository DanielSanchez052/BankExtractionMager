import camelot


def read_pdf(filepath: str, password: str) -> list:
    """
    Reads a PDF file and returns a list of tables (camelot.TableList) found in it.
    :param filepath: Path to the PDF file.
    :param password: Password for the PDF file, if it is encrypted.
    :return: List of tables found in the PDF.
    """

    try:
        tables = camelot.read_pdf(filepath, pages='all', flavor="hybrid", password=password)

        return tables

    except Exception as e:
        print(f"Ocurrió un error: {e}")
        print("Asegúrate de tener las dependencias correctas instaladas (ej. Ghostscript para Camelot).")
        print("También verifica que la contraseña sea correcta y el PDF no esté corrupto.")
    return []
