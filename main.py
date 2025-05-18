from dotenv import load_dotenv
from discord_bot.bot import DiscordBot
from files_process.etls.etl import ETL
from logger import setup_logger
from settings import Settings


load_dotenv()


def main():
    try:
        settings = Settings()

        # Configurar el logger
        logger = setup_logger()

        etl = ETL(settings)
        log1 = etl.run("process_transactions", password="1001773168")
        log = etl.run("process_resume", password="1001773168")
        print(log1)
        print(log)
        # Iniciar el bot
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
