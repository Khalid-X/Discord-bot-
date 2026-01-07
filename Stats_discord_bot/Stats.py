import asyncio
import asyncpg
import os
from dotenv import load_dotenv
import logging
from discord import app_commands
from discord.ext import commands
import discord

# Load environment variables
load_dotenv()
token = os.getenv('DISCORD_TOKEN')

# PostgreSQL connection pool
pool = None

# Intents
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.messages = True
intents.guild_messages = True
intents.guilds = True
intents.voice_states = True
intents.presences = True
intents.invites = True

# prefix
bot = commands.Bot(command_prefix='!s', intents=intents)
bot.pool = pool


guild_id = 1424021358680998022


@bot.event
async def on_ready():
    print(f'✅ Logged in as {bot.user}')

    # PostgreSQL connection pool
    global pool
    try:
        pool = await asyncpg.create_pool(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            min_size=1,
            max_size=10
        )
        bot.pool = pool
        print(f'✅ PostgreSQL connection pool created')

        async with pool.acquire() as conn:
            await conn.fetch("SELECT 1")
            print(f'✅ PostgreSQL connection test successful')

    except Exception as e:
        print(f'❌ Failed to connect to PostgreSQL: {e}')

        pool = None
        bot.pool = None

    # Sync commands
    try:
        synced = await bot.tree.sync()
        print(f'🔄 Synced {len(synced)} commands globally')
        guild = discord.Object(id=guild_id)
        synced = await bot.tree.sync(guild=guild)
        print(f'🔄 Synced commands to guild ID: {guild_id}')
    except Exception as e:
        print(f'❌ Failed to sync commands: {e}')


# sync up commands on rejoins/joins

@bot.event
async def on_guild_join(guild):
    try:
        synced = await bot.tree.sync(guild=guild)
        print(f"🏠 Synced {len(synced)} commands to new guild: {guild.name}")
    except Exception as e:
        print(f"❌ Failed to sync to {guild.name}: {e}")


async def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cogs_path = os.path.join(current_dir, 'cogs')

    for filename in os.listdir(cogs_path):
        if filename.endswith('.py') and filename != '__init__.py':
            try:
                await bot.load_extension(f'cogs.{filename[:-3]}')
                print(f'✅ Loaded cog: {filename}')
            except Exception as e:
                print(f'❌ Failed to load cog {filename}: {e}')

    await bot.start(token)

if __name__ == "__main__":
    asyncio.run(main())
