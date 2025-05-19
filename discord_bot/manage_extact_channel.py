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
EMOJI_ACCESS = "🔒"      # Acceso


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
    emoji = get_identifier_icon(error_category)

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


def get_identifier_icon(identifier):
    """
    Returns the emoji associated with the identifier.

    Args:
        identifier: Identifier string

    Returns:
        str: Emoji string
    """
    if identifier == "file_processed":
        return EMOJI_SUCCESS
    elif identifier == "error":
        return EMOJI_ERROR
    elif identifier == "warning":
        return EMOJI_WARNING
    elif identifier == "access":
        return EMOJI_ACCESS
    elif identifier == "success":
        return EMOJI_SUCCESS
    else:
        return EMOJI_PDF


def get_params_from_message(message):
    """
    Extracts parameters from the message content.

    Args:
        message: Discord message

    Returns:
        dict: Dictionary with extracted parameters
    """
    params = {}
    message_splitted = message.content.split(" ")

    for message_part in message_splitted:
        if "pass:" in (message_part or "").lower():
            params['pdf_password'] = (message_part.lower().split("pass:")[1].strip())
        elif "month:" in (message_part or "").lower():
            mes = (message_part.lower().split("month:")[1].strip())
            if mes.isdigit() and 1 <= int(mes) <= 12:
                params['month'] = int(mes)
        elif "year:" in (message_part or "").lower():
            year = (message_part.lower().split("year:")[1].strip())
            if year.isdigit() and 1900 <= int(year) <= 2100:
                params['year'] = int(year)

    return params


async def handle_extract_message(message, logger, debug_channel, log_channel, settings):
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

        task_params = get_params_from_message(message)
        pdf_password = task_params.get('pdf_password', None)

        if "month" not in task_params or "year" not in task_params:
            await handle_error(
                message,
                attachment.filename,
                "Please provide the month and year in the format 'mes: <month>' and 'año: <year>'",
                logger,
                debug_channel,
                clear_processing=False,
                error_category="warning"
            )
            continue

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

            task_params['filepath'] = download_path
            etl = ETL(settings)
            log_transactions = etl.run(constants.PROCESS_TRANSACTIONS_ETL, **task_params)
            log_resume = etl.run(constants.PROCESS_RESUME_ETL, **task_params)

            logger.info(f"File {attachment.filename} processed successfully")
            await message.remove_reaction(EMOJI_PROCESSING, message.guild.me)
            await message.add_reaction(EMOJI_SUCCESS)
            await message.reply(f"✅ File {attachment.filename} processed successfully. month: {task_params['month']} year: {task_params['year']}")
            await log_channel.send(f"✅ File {attachment.filename} processed successfully.")

            transaction_log_messages = [f"{get_identifier_icon(row['identifier'])} {row['message']}" for _, row in log_transactions.iterrows() if row['identifier'] != 'file_processed']
            transaction_log_reply = f"{EMOJI_PDF} Log transactions {attachment.filename}.\n" + "\n".join(transaction_log_messages)
            await log_channel.send(transaction_log_reply)

            resume_log_messages = [f"{get_identifier_icon(row['identifier'])} {row['message']}" for _, row in log_resume.iterrows() if row['identifier'] != 'file_processed']
            resume_log_reply = f"{EMOJI_PDF} Log resume {attachment.filename}.\n" + "\n".join(resume_log_messages)
            await log_channel.send(resume_log_reply)
            await log_channel.send("#" * 15)

        except Exception as e:
            await handle_error(message, attachment.filename, str(e), logger, debug_channel)
        finally:
            await message.delete()
            if os.path.exists(download_path):
                os.remove(download_path)
