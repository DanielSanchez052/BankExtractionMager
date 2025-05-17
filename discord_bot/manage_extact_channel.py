import os
from files_process.extractors import PDFExtractor  

# Emojis para reacciones
EMOJI_PROCESSING = "⏳"  # Reloj de arena
EMOJI_SUCCESS = "✅"      # Check verde
EMOJI_ERROR = "❌"        # X roja
EMOJI_WARNING = "⚠️"      # Advertencia
EMOJI_PDF = "📄"          # Documento

async def handle_error(message, filename, error_msg, logger, debug_channel, remove_processing=True, error_type="error"):
    """
    Maneja los mensajes de error de manera consistente
    
    Args:
        message: El mensaje de Discord
        filename: Nombre del archivo que causó el error
        error_msg: Mensaje de error a mostrar
        logger: Instancia del logger
        debug_channel: Canal de debug
        remove_processing: Si se debe remover la reacción de procesamiento
        error_type: Tipo de error (error, warning, access)
    """
    if debug_channel:
        await debug_channel.send(f"{'❌' if error_type == 'error' else '⚠️' if error_type == 'warning' else '🔒'} {error_msg}")
    
    # Registrar en el logger
    if error_type == 'error':
        logger.error(f"Error al procesar {filename}: {error_msg}")
    elif error_type == 'warning':
        logger.warning(f"Advertencia al procesar {filename}: {error_msg}")
    else:
        logger.warning(f"Error de acceso en {filename}: {error_msg}")
    
    # Cambiar reacción
    if remove_processing:
        await message.remove_reaction(EMOJI_PROCESSING, message.guild.me)
    await message.add_reaction(EMOJI_ERROR if error_type == 'error' else EMOJI_WARNING)
    
    # Responder al mensaje
    await message.reply(f"{'❌' if error_type == 'error' else '⚠️' if error_type == 'warning' else '🔒'} {error_msg}")

async def handle_extract_message(message, logger, debug_channel):
    logger.info(message.channel.name)
    
    filename=None
    password=None
    
    # Verificar si el mensaje tiene archivos adjuntos
    if not message.attachments:
        return
        
    # Procesar cada archivo adjunto
    for attachment in message.attachments:
        # Verificar si es un PDF
        if not attachment.filename.lower().endswith('.pdf'):
            await handle_error(
                message, 
                attachment.filename, 
                f"El archivo {attachment.filename} no es un PDF. Por favor, sube solo archivos PDF.",
                logger, 
                debug_channel,
                remove_processing=False,
                error_type="warning"
            )
            continue
        
        # Agregar reacción de procesamient
        await message.add_reaction(EMOJI_PROCESSING)
        
        if message.content and "password:" in message.content.lower():
            password = message.content.lower().split("password:")[1].strip()
        
        # Notificar en el canal de debug
        if debug_channel:
            await debug_channel.send(
                f"📄 Nuevo archivo PDF detectado:\n"
                f"Archivo: {attachment.filename}\n"
                f"URL: {attachment.url}\n"
                f"Mensaje: {message.content if message.content else 'Sin mensaje'}\n"
                f"Usuario: {message.author.name}\n"
                f"Contraseña: {'*****' * (len(password)-2) + password[-2:] if password else 'No especificada'}"
            )
            
        # Registrar en el logger
        logger.info(f"Nuevo archivo PDF detectado: {attachment.filename}")
        
        try:
            # Crear directorio si no existe
            os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
            # Descargar el archivo\
            filename = f'{DOWNLOAD_FOLDER}/{attachment.filename}'
            await attachment.save(filename)
            
            if(not PDFExtractor(logger).check_pdf_access(filename, password)):
                await handle_error(
                    message,
                    attachment.filename,
                    f"No se pudo acceder al archivo {attachment.filename}. Por favor verifica que la contraseña sea correcta.",
                    logger,
                    debug_channel,
                    error_type="access"
                )
                continue
            
            # Notificar éxito
            if debug_channel:
                await debug_channel.send(f"✅ Archivo {attachment.filename} guardado correctamente")
            
            # Registrar éxito en el logger
            logger.info(f"Archivo {attachment.filename} guardado correctamente")
            # Cambiar reacción a éxito
            await message.remove_reaction(EMOJI_PROCESSING, message.guild.me)
            await message.add_reaction(EMOJI_SUCCESS)
            # Responder al mensaje
            await message.reply(f"✅ Archivo {attachment.filename} procesado correctamente.")
            
        except Exception as e:
            await handle_error(message, attachment.filename, str(e), logger, debug_channel)
