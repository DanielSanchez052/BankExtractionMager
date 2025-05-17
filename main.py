from dotenv import load_dotenv
# from discord_bot.bot import DiscordBot
import constants
from logger import setup_logger
from settings import Settings
from files_process.etls.etl import ETL


load_dotenv()


def main():
    try:
        settings = Settings()

        # Configurar el logger
        logger = setup_logger()

        etl = ETL(settings)
        etl.run(constants.PROCESS_TRANSACTIONS_ETL, filepath="files_process\\etls\\to_process\\Extracto Cuenta 04-2025.pdf", password="1001773168")
        etl.run(constants.PROCESS_RESUME_ETL, filepath="files_process\\etls\\to_process\\Extracto Cuenta 04-2025.pdf", password="1001773168")
        # # Iniciar el bot
        # bot = DiscordBot(logger, settings)
        # logger.info("Iniciando bot de Discord...")
        # if bot.run():
        #     logger.info("Bot iniciado correctamente")
        # else:
        #     logger.error("Error al iniciar el bot")

    except Exception as ex:
        logger.error(f"Error inesperado: {str(ex)}")


if __name__ == "__main__":
    main()
