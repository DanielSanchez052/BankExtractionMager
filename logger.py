import logging
from datetime import datetime
import os

class LoggingFormatter(logging.Formatter):
    # Colors
    black = "\x1b[30m"
    red = "\x1b[31m"
    green = "\x1b[32m"
    yellow = "\x1b[33m"
    blue = "\x1b[34m"
    gray = "\x1b[38m"
    # Styles
    reset = "\x1b[0m"
    bold = "\x1b[1m"

    COLORS = {
        logging.DEBUG: gray + bold,
        logging.INFO: blue + bold,
        logging.WARNING: yellow + bold,
        logging.ERROR: red,
        logging.CRITICAL: red + bold,
    }

    def format(self, record):
        log_color = self.COLORS[record.levelno]
        format = "(black){asctime}(reset) (levelcolor){levelname:<8}(reset) (green){name}(reset) {message}"
        format = format.replace("(black)", self.black + self.bold)
        format = format.replace("(reset)", self.reset)
        format = format.replace("(levelcolor)", log_color)
        format = format.replace("(green)", self.green + self.bold)
        formatter = logging.Formatter(format, "%Y-%m-%d %H:%M:%S", style="{")
        return formatter.format(record)

def setup_logger(name="discord_bot", level=logging.INFO):
    """
    Configura y retorna un logger con los handlers necesarios
    
    Args:
        name: Nombre del logger
        level: Nivel de logging (default: INFO)
    
    Returns:
        logging.Logger: Logger configurado
    """
    # Crear el logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Crear directorio de logs si no existe
    os.makedirs("logs", exist_ok=True)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(LoggingFormatter())
    
    # File handler
    file_handler = logging.FileHandler(
        filename=f"logs/{datetime.now().strftime('%Y-%m-%d')}-discord.log",
        encoding="utf-8",
        mode="w"
    )
    file_handler_formatter = logging.Formatter(
        "[{asctime}] [{levelname:<8}] {name}: {message}",
        "%Y-%m-%d %H:%M:%S",
        style="{"
    )
    file_handler.setFormatter(file_handler_formatter)
    
    # Limpiar handlers existentes
    logger.handlers.clear()
    
    # Agregar los handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
