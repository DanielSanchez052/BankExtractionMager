import discord
import os
from discord.ext import commands
from . import manage_extact_channel


class DiscordBot(commands.Bot):
    def __init__(self, logger, settings):
        self.logger = logger
        self.settings = settings
        # Configuración de intents
        intents = discord.Intents.default()
        intents.message_content = True

        # Inicializar el bot
        super().__init__(command_prefix='!', intents=intents)
        self.debug_channel = None
        self.extract_log_channel = None

        # Registrar eventos y comandos
        self._register_events()
        self._register_commands()

    async def _get_debug_channel(self):
        """Obtiene el canal de debug"""
        if self.debug_channel is None and self.settings.debugChannelName:
            for guild in self.guilds:
                channel = discord.utils.get(guild.channels, name=self.settings.debugChannelName)
                if channel:
                    self.debug_channel = channel
                    break
        return self.debug_channel

    async def _get_extract_log_channel(self):
        """Obtiene el canal de debug"""
        if self.extract_log_channel is None and self.settings.extractLogChannelName:
            for guild in self.guilds:
                channel = discord.utils.get(guild.channels, name=self.settings.extractLogChannelName)
                if channel:
                    self.extract_log_channel = channel
                    break
        return self.extract_log_channel

    def _get_bot_info(self):
        return f'''
**Información del Bot**
=======================
**Nombre:** {self.user.name}
**ID:** {self.user.id}
**Versión:** {discord.__version__}

**Comandos Disponibles**
------------------------
{self._format_commands()}

**Eventos Disponibles**
----------------------
{self._format_events()}
'''

    def _format_commands(self):
        commands_list = [f"- {cmd.name}: {cmd.help or 'Sin descripción'}" for cmd in self.commands]
        return "\n".join(commands_list)

    def _format_events(self):
        event_list = ["- on_ready", "- on_command_error"]
        return "\n".join(event_list)

    def _register_events(self):
        @self.event
        async def on_ready():
            self.logger.info(f'Bot conectado como {self.user.name}')
            # Enviar mensaje al canal de debug
            debug_channel = await self._get_debug_channel()
            if debug_channel:
                await debug_channel.send(f"✅ Bot conectado como {self.user.name}")
                await debug_channel.send(self._get_bot_info())

        @self.event
        async def on_command_error(ctx, error):
            if isinstance(error, commands.MissingPermissions):
                await ctx.send("No tienes permisos para usar este comando.")
            elif isinstance(error, commands.MissingRequiredArgument):
                await ctx.send("Falta un argumento requerido para este comando.")

            # Enviar error al canal debug
            debug_channel = await self._get_debug_channel()
            if debug_channel:
                await debug_channel.send(f"❌ Error en comando: {ctx.command}\nError: {str(error)}")

            # Registrar el error en el logger
            self.logger.error(f"Error en comando {ctx.command}: {str(error)}")

        @self.event
        async def on_message(message):
            debug_channel = await self._get_debug_channel()
            # Ignorar mensajes del bot
            if message.author == self.user:
                return

            # Verificar si el mensaje está en el canal process-extract
            if message.channel.name == "process-extract":
                log_channel = await self._get_extract_log_channel()
                await manage_extact_channel.handle_extract_message(message, self.logger, debug_channel, log_channel, self.settings)

            # Procesar comandos después de verificar el mensaje

            await self.process_commands(message)

    def _register_commands(self):
        @self.command(name='info')
        async def info(ctx):
            """Muestra información sobre el servidor"""
            await ctx.send(self._get_bot_info())
            self.logger.info(f"Comando 'info' ejecutado por {ctx.author.name}")

        @self.command(name='limpiar')
        @commands.has_permissions(manage_messages=True)
        async def limpiar(ctx, cantidad: int):
            """Elimina la cantidad especificada de mensajes"""
            await ctx.channel.purge(limit=cantidad + 1)
            await ctx.send(f'Se han eliminado {cantidad} mensajes.', delete_after=5)
            self.logger.info(f"Comando 'limpiar' ejecutado por {ctx.author.name} - {cantidad} mensajes eliminados")

    def run(self):
        """Inicia el bot"""
        TOKEN = os.getenv('DISCORD_TOKEN')
        if TOKEN is None:
            self.logger.error("No se encontró el token de Discord. Asegúrate de tener un archivo .env con DISCORD_TOKEN")
            return False

        self.logger.info("Iniciando bot de Discord...")
        super().run(TOKEN)
        return True
