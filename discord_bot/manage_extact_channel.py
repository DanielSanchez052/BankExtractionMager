import os

import pandas as pd
import constants
from files_process.etls.etl import ETL
from files_process.extractors import PDFExtractor

# Emojis para reacciones
EMOJI_PROCESSING = "⏳"  # Reloj de arena
EMOJI_SUCCESS = "✅"
EMOJI_ERROR = "❌"
EMOJI_WARNING = "⚠️"      # Advertencia
EMOJI_PDF = "📄"          # Documento


async def handle_error(message, file_name, error_message, log, debug_channel, clear_processing=True, error_category="error"):
    """
    Handles error messages consistently

    Args:
        message: Discord message
        file_name: Name of the file that caused the error
        error_message: Error message to display
        log: Logger instance
        debug_channel: Debugging channel
        clear_processing: Whether to remove the processing reaction
        error_category: Type of error (error, warning, access)
    """
    emoji = "❌" if error_category == "error" else "⚠️" if error_category == "warning" else "🔒"

    if debug_channel:
        await debug_channel.send(f"{emoji} {error_message}")

    # Log the error
    if error_category == "error":
        log.error(f"Error processing {file_name}: {error_message}")
    elif error_category == "warning":
        log.warning(f"Warning processing {file_name}: {error_message}")
    else:
        log.warning(f"Access error in {file_name}: {error_message}")

    # Change reaction
    if clear_processing:
        await message.remove_reaction(EMOJI_PROCESSING, message.guild.me)
    await message.add_reaction(EMOJI_ERROR if error_category == "error" else EMOJI_WARNING)

    # Reply to the message
    await message.reply(f"{emoji} {error_message}")


async def handle_extract_message(message, logger, debug_channel, settings):
    """
    Handles messages with PDF attachments consistently.

    Args:
        message: Discord message
        logger: Logger instance
        debug_channel: Debugging channel
        settings: Application settings
    """
    logger.info(f"Processing message from channel: {message.channel.name}")

    if not message.attachments:
        return

    for attachment in message.attachments:
        if not attachment.filename.lower().endswith('.pdf'):
            await handle_error(
                message,
                attachment.filename,
                f"The file {attachment.filename} is not a PDF. Please upload only PDF files.",
                logger,
                debug_channel,
                clear_processing=False,
                error_category="warning"
            )
            continue

        await message.add_reaction(EMOJI_PROCESSING)

        pdf_password = None
        if "password:" in (message.content or "").lower():
            pdf_password = (message.content.lower().split("password:")[1].strip())

        try:
            os.makedirs(settings.dowload_files, exist_ok=True)
            download_path = os.path.join(settings.dowload_files, attachment.filename)
            await attachment.save(download_path)

            pdf_extractor = PDFExtractor(logger)
            if not pdf_extractor.check_pdf_access(download_path, pdf_password):
                await handle_error(
                    message,
                    attachment.filename,
                    f"Could not access the file {attachment.filename}. Please check if the password is correct.",
                    logger,
                    debug_channel,
                    error_category="access"
                )
                continue

            etl = ETL(settings)
            log_transactions = etl.run(constants.PROCESS_TRANSACTIONS_ETL, filepath=download_path, password=pdf_password)
            log_resume = etl.run(constants.PROCESS_RESUME_ETL, filepath=download_path, password=pdf_password)

            logger.info(f"File {attachment.filename} processed successfully")
            await message.remove_reaction(EMOJI_PROCESSING, message.guild.me)
            await message.add_reaction(EMOJI_SUCCESS)
            await message.reply(f"✅ File {attachment.filename} processed successfully.")

            transaction_log_messages = [f"{row['identifier']} {row['message']}" for _, row in log_transactions.iterrows() if row['identifier'] != 'file_processed']
            combined_message = f"✅ Log transactions {attachment.filename}.\n" + "\n".join(transaction_log_messages)

            resume_log_messages = [f"{row['identifier']} {row['message']}" for _, row in log_resume.iterrows() if row['identifier'] != 'file_processed']
            combined_message = f"✅ Log resume {attachment.filename}.\n" + "\n".join(resume_log_messages)

            await message.reply(combined_message)

        except Exception as e:
            await handle_error(message, attachment.filename, str(e), logger, debug_channel)
