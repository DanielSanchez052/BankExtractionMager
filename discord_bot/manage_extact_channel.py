import os

from ollama import chat
from ollama import ChatResponse
from pydantic import BaseModel
from files_process.extractors import PDFExtractor
from files_process import FileProcessor


# Emojis para reacciones
EMOJI_PROCESSING = "⏳"  # Reloj de arena
EMOJI_SUCCESS = "✅"
EMOJI_ERROR = "❌"
EMOJI_WARNING = "⚠️"      # Advertencia
EMOJI_PDF = "📄"          # Documento
EMOJI_ACCESS = "🔒"      # Acceso


class PDfInfo(BaseModel):
    bank_name: str
    month: int
    year: int

    def to_dict(self):
        return self.__dict__


def get_extract_info(text) -> PDfInfo:
    """
    Extracts information from a text that corresponds to a bank statement.

    Args:
        text (str): Text to extract information from

    Returns:
        PDfInfo: Extracted information
    """
    base_prompt = f"""
     You are going to receive a text that corresponds to a bank statement. Extract the following information from it:

     - {', '.join([f for f in PDfInfo.model_fields.keys()])}

     text: {text}
     """

    messages = [
        {
            'role': 'user',
            'content': base_prompt,
        }
    ]
    try:
        response: ChatResponse = chat(model='gemma3:4b', messages=messages, format=PDfInfo.model_json_schema())
        json_response = PDfInfo.model_validate_json(response.message.content)
        return json_response
    except Exception as e:
        print(e)
        return None


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
        else:
            param = message_part.split(":")
            params[param[0].strip().lower()] = param[1].strip()
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
    pdf_extractor = PDFExtractor(logger)

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

        try:
            os.makedirs(settings.dowload_files, exist_ok=True)
            download_path = os.path.join(settings.dowload_files, attachment.filename)
            await attachment.save(download_path)

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

            text = pdf_extractor.get_text(str(download_path), pdf_password)

            info = get_extract_info(text)
            if info is not None:
                task_params.update({key: value for key, value in info.to_dict().items() if key not in task_params})

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

            log = FileProcessor(settings).process_file(**task_params)

            logger.info(f"File {attachment.filename} processed successfully")
            await message.remove_reaction(EMOJI_PROCESSING, message.guild.me)
            await message.add_reaction(EMOJI_SUCCESS)
            await message.reply(f"✅ File {attachment.filename} processed successfully. month: {task_params['month']} year: {task_params['year']}")
            await log_channel.send(f"✅ File {attachment.filename} processed successfully.")

            transaction_log_messages = [f"{get_identifier_icon(row['identifier'])} {row['message']}" for _, row in log.iterrows() if row['identifier'] != 'file_processed']
            transaction_log_reply = f"{EMOJI_PDF} Log {attachment.filename}.\n" + "\n".join(transaction_log_messages)
            await log_channel.send(transaction_log_reply)

            # await log_channel.send("#" * 15)

        except Exception as e:
            print(e)
            await handle_error(message, attachment.filename, str(e), logger, debug_channel)
        finally:
            await message.delete()
            if os.path.exists(download_path):
                os.remove(download_path)
