import discord
import os
from discord.ext import commands
from . import manage_extact_channel 
from db.database import Database

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
        self.db = None

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

    def _register_events(self):
        @self.event
        async def on_ready():
            self.logger.info(f'Bot conectado como {self.user.name}')
            self.logger.info('------')
            
            # Enviar mensaje al canal debug
            debug_channel = await self._get_debug_channel()
            if debug_channel:
                await debug_channel.send(f"🟢 Bot iniciado como {self.user.name}\nEventos registrados:\n- on_ready\n- on_command_error")

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
                await manage_extact_channel.handle_extract_message(message, self.logger, debug_channel)
            
            # Procesar comandos después de verificar el mensaje

            await self.process_commands(message)

    def _register_commands(self):
        @self.command(name='hola')
        async def saludar(ctx):
            """Comando simple que responde con un saludo"""
            await ctx.send(f'¡Hola {ctx.author.name}!')
            self.logger.info(f"Comando 'hola' ejecutado por {ctx.author.name}")

        @self.command(name='info')
        async def info(ctx):
            """Muestra información sobre el servidor"""
            server = ctx.guild
            await ctx.send(f'''
            **Información del Servidor**
            Nombre: {server.name}
            Miembros: {server.member_count}
            Creado el: {server.created_at.strftime('%d/%m/%Y')}
            ''')
            self.logger.info(f"Comando 'info' ejecutado por {ctx.author.name}")

        @self.command(name='limpiar')
        @commands.has_permissions(manage_messages=True)
        async def limpiar(ctx, cantidad: int):
            """Elimina la cantidad especificada de mensajes"""
            await ctx.channel.purge(limit=cantidad + 1)
            await ctx.send(f'Se han eliminado {cantidad} mensajes.', delete_after=5)
            self.logger.info(f"Comando 'limpiar' ejecutado por {ctx.author.name} - {cantidad} mensajes eliminados")

    async def setup_hook(self):
        """Hook que se ejecuta cuando el bot está listo"""
        try:
            debug_channel = await self._get_debug_channel()
            if debug_channel:
                commands_list = "\n".join([f"- {cmd.name}: {cmd.help or 'Sin descripción'}" for cmd in self.commands])
                await debug_channel.send(f"📝 Comandos registrados:\n{commands_list}")
                self.logger.info("Comandos registrados y listos para usar")


            
        except Exception as ex:
            self.logger.error("An error has ocurred")
            self.logger.error(ex)

    def run(self):
        """Inicia el bot"""
        TOKEN = os.getenv('DISCORD_TOKEN')
        if TOKEN is None:
            self.logger.error("No se encontró el token de Discord. Asegúrate de tener un archivo .env con DISCORD_TOKEN")
            return False
        
        self.logger.info("Iniciando bot de Discord...")
        super().run(TOKEN)
        return True 