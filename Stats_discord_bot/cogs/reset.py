import discord
from discord import app_commands
from discord.ui import View, Select, Button, Modal, TextInput
from discord.ext import commands
from typing import Optional, Literal
import asyncpg
import os
from dotenv import load_dotenv
import logging
import traceback
from pathlib import Path


# CONFIGURATION

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent


class DatabaseManager:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        logger.info("DatabaseManager initialized")

    async def ensure_tables_exist(self, guild_id: int):

        async with self.pool.acquire() as conn:
            tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            table_names = [t['table_name'] for t in tables]
            logger.info(f"Available tables in database: {table_names}")

            required_tables = [
                'message_tracking', 'voice_session_history', 'voice_active_sessions',
                'emoji_usage', 'user_mentions', 'invite_tracking',
                'activity_active_sessions', 'activity_sessions'
            ]

            missing_tables = [
                t for t in required_tables if t not in table_names]
            if missing_tables:
                logger.warning(f"Missing tables: {missing_tables}")
                return False
            return True

    # SERVER DELETION LOGIC

    async def delete_server_all(self, guild_id: int,
                                channel_id: Optional[int] = None,
                                category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_server_all: guild={guild_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = ["guild_id = $1"]
                    params = [guild_id]
                    param_counter = 2

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)

                    tables = [
                        'message_tracking',
                        'voice_session_history',
                        'voice_active_sessions',
                        'emoji_usage',
                        'user_mentions'
                    ]

                    deleted_counts = {}

                    for table in tables:
                        try:
                            result = await conn.execute(f"DELETE FROM {table} WHERE {where_clause}", *params)

                            deleted_counts[table] = result.split()[-1]
                            logger.info(
                                f"Deleted from {table}: {deleted_counts[table]} rows")
                        except Exception as e:
                            logger.warning(f"Error deleting from {table}: {e}")
                            deleted_counts[table] = "0 (error)"

                    if not channel_id and not category_id:

                        server_tables = [
                            'activity_active_sessions',
                            'activity_sessions',
                            'invite_tracking'
                        ]

                        for table in server_tables:
                            try:
                                result = await conn.execute(f"DELETE FROM {table} WHERE guild_id = $1", guild_id)
                                deleted_counts[table] = result.split()[-1]
                                logger.info(
                                    f"Deleted from {table}: {deleted_counts[table]} rows")
                            except Exception as e:
                                logger.warning(
                                    f"Error deleting from {table}: {e}")
                                deleted_counts[table] = "0 (error)"

                    return deleted_counts

        except Exception as e:
            logger.error(f"Error in delete_server_all: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_server_messages(self, guild_id: int,
                                     channel_id: Optional[int] = None,
                                     category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_server_messages: guild={guild_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = ["guild_id = $1"]
                    params = [guild_id]
                    param_counter = 2

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)
                    result = await conn.execute(f"DELETE FROM message_tracking WHERE {where_clause}", *params)

                    deleted_count = result.split()[-1]
                    logger.info(
                        f"Deleted {deleted_count} rows from message_tracking")
                    return int(deleted_count) if deleted_count.isdigit() else 0

        except Exception as e:
            logger.error(f"Error in delete_server_messages: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_server_voice(self, guild_id: int,
                                  channel_id: Optional[int] = None,
                                  category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_server_voice: guild={guild_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = ["guild_id = $1"]
                    params = [guild_id]
                    param_counter = 2

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)

                    deleted_counts = {}

                    result1 = await conn.execute(f"DELETE FROM voice_session_history WHERE {where_clause}", *params)
                    deleted_counts['voice_session_history'] = result1.split()[-1]

                    result2 = await conn.execute(f"DELETE FROM voice_active_sessions WHERE {where_clause}", *params)
                    deleted_counts['voice_active_sessions'] = result2.split()[-1]

                    logger.info(f"Deleted voice stats: {deleted_counts}")
                    return deleted_counts

        except Exception as e:
            logger.error(f"Error in delete_server_voice: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_server_invites(self, guild_id: int):

        try:
            logger.info(f"delete_server_invites: guild={guild_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    result = await conn.execute(f"DELETE FROM invite_tracking WHERE guild_id = $1", guild_id)

                    deleted_count = result.split()[-1]
                    logger.info(
                        f"Deleted {deleted_count} rows from invite_tracking")
                    return int(deleted_count) if deleted_count.isdigit() else 0

        except Exception as e:
            logger.error(f"Error in delete_server_invites: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_server_emoji(self, guild_id: int,
                                  channel_id: Optional[int] = None,
                                  category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_server_emoji: guild={guild_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = ["guild_id = $1"]
                    params = [guild_id]
                    param_counter = 2

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)
                    result = await conn.execute(f"DELETE FROM emoji_usage WHERE {where_clause}", *params)

                    deleted_count = result.split()[-1]
                    logger.info(
                        f"Deleted {deleted_count} rows from emoji_usage")
                    return int(deleted_count) if deleted_count.isdigit() else 0

        except Exception as e:
            logger.error(f"Error in delete_server_emoji: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_server_activity(self, guild_id: int):

        try:
            logger.info(f"delete_server_activity: guild={guild_id}")

            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    deleted_counts = {}

                    result1 = await conn.execute(f"DELETE FROM activity_active_sessions WHERE guild_id = $1", guild_id)
                    deleted_counts['activity_active_sessions'] = result1.split(
                    )[-1]

                    result2 = await conn.execute(f"DELETE FROM activity_sessions WHERE guild_id = $1", guild_id)
                    deleted_counts['activity_sessions'] = result2.split()[-1]

                    logger.info(f"Deleted activity stats: {deleted_counts}")
                    return deleted_counts

        except Exception as e:
            logger.error(f"Error in delete_server_activity: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_server_mentions(self, guild_id: int,
                                     channel_id: Optional[int] = None,
                                     category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_server_mentions: guild={guild_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = ["guild_id = $1"]
                    params = [guild_id]
                    param_counter = 2

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)
                    result = await conn.execute(f"DELETE FROM user_mentions WHERE {where_clause}", *params)

                    deleted_count = result.split()[-1]
                    logger.info(
                        f"Deleted {deleted_count} rows from user_mentions")
                    return int(deleted_count) if deleted_count.isdigit() else 0

        except Exception as e:
            logger.error(f"Error in delete_server_mentions: {e}")
            logger.error(traceback.format_exc())
            raise

    # USER DELETION

    async def delete_user_all(self, guild_id: int, user_id: int,
                              channel_id: Optional[int] = None,
                              category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_user_all: guild={guild_id}, user={user_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = ["guild_id = $1", "user_id = $2"]
                    params = [guild_id, user_id]
                    param_counter = 3

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)

                    deleted_counts = {}

                    tables = [
                        'message_tracking',
                        'voice_session_history',
                        'voice_active_sessions',
                        'emoji_usage'
                    ]

                    for table in tables:
                        try:
                            result = await conn.execute(f"DELETE FROM {table} WHERE {where_clause}", *params)
                            deleted_counts[table] = result.split()[-1]
                            logger.info(
                                f"Deleted from {table}: {deleted_counts[table]} rows")
                        except Exception as e:
                            logger.warning(f"Error deleting from {table}: {e}")
                            deleted_counts[table] = "0 (error)"

                    mention_conditions = [
                        "guild_id = $1", "(mentioned_user_id = $2 OR mentioner_user_id = $2)"]
                    mention_params = [guild_id, user_id]
                    mention_counter = 3

                    if channel_id:
                        mention_conditions.append(
                            f"channel_id = ${mention_counter}")
                        mention_params.append(channel_id)
                        mention_counter += 1

                    if category_id:
                        mention_conditions.append(
                            f"category_id = ${mention_counter}")
                        mention_params.append(category_id)
                        mention_counter += 1

                    mention_where = " AND ".join(mention_conditions)
                    result = await conn.execute(f"DELETE FROM user_mentions WHERE {mention_where}", *mention_params)
                    deleted_counts['user_mentions'] = result.split()[-1]

                    if not channel_id and not category_id:

                        result = await conn.execute(f"""
                            DELETE FROM invite_tracking 
                            WHERE guild_id = $1 AND (inviter_id = $2 OR invitee_id = $2)
                        """, guild_id, user_id)
                        deleted_counts['invite_tracking'] = result.split()[-1]

                        result1 = await conn.execute(f"DELETE FROM activity_active_sessions WHERE guild_id = $1 AND user_id = $2",
                                                     guild_id, user_id)
                        deleted_counts['activity_active_sessions'] = result1.split(
                        )[-1]

                        result2 = await conn.execute(f"DELETE FROM activity_sessions WHERE guild_id = $1 AND user_id = $2",
                                                     guild_id, user_id)
                        deleted_counts['activity_sessions'] = result2.split(
                        )[-1]

                    logger.info(f"Deleted user stats: {deleted_counts}")
                    return deleted_counts

        except Exception as e:
            logger.error(f"Error in delete_user_all: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_user_messages(self, guild_id: int, user_id: int,
                                   channel_id: Optional[int] = None,
                                   category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_user_messages: guild={guild_id}, user={user_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = ["guild_id = $1", "user_id = $2"]
                    params = [guild_id, user_id]
                    param_counter = 3

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)
                    result = await conn.execute(f"DELETE FROM message_tracking WHERE {where_clause}", *params)

                    deleted_count = result.split()[-1]
                    logger.info(
                        f"Deleted {deleted_count} rows from message_tracking for user {user_id}")
                    return int(deleted_count) if deleted_count.isdigit() else 0

        except Exception as e:
            logger.error(f"Error in delete_user_messages: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_user_voice(self, guild_id: int, user_id: int,
                                channel_id: Optional[int] = None,
                                category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_user_voice: guild={guild_id}, user={user_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = ["guild_id = $1", "user_id = $2"]
                    params = [guild_id, user_id]
                    param_counter = 3

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)

                    deleted_counts = {}
                    result1 = await conn.execute(f"DELETE FROM voice_session_history WHERE {where_clause}", *params)
                    deleted_counts['voice_session_history'] = result1.split()[-1]

                    result2 = await conn.execute(f"DELETE FROM voice_active_sessions WHERE {where_clause}", *params)
                    deleted_counts['voice_active_sessions'] = result2.split()[-1]

                    logger.info(
                        f"Deleted voice stats for user {user_id}: {deleted_counts}")
                    return deleted_counts

        except Exception as e:
            logger.error(f"Error in delete_user_voice: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_user_invites(self, guild_id: int, user_id: int):

        try:
            logger.info(
                f"delete_user_invites: guild={guild_id}, user={user_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    result = await conn.execute(f"""
                        DELETE FROM invite_tracking 
                        WHERE guild_id = $1 AND (inviter_id = $2 OR invitee_id = $2)
                    """, guild_id, user_id)

                    deleted_count = result.split()[-1]
                    logger.info(
                        f"Deleted {deleted_count} invite records for user {user_id}")
                    return int(deleted_count) if deleted_count.isdigit() else 0

        except Exception as e:
            logger.error(f"Error in delete_user_invites: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_user_emoji(self, guild_id: int, user_id: int,
                                channel_id: Optional[int] = None,
                                category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_user_emoji: guild={guild_id}, user={user_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = ["guild_id = $1", "user_id = $2"]
                    params = [guild_id, user_id]
                    param_counter = 3

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)
                    result = await conn.execute(f"DELETE FROM emoji_usage WHERE {where_clause}", *params)

                    deleted_count = result.split()[-1]
                    logger.info(
                        f"Deleted {deleted_count} emoji records for user {user_id}")
                    return int(deleted_count) if deleted_count.isdigit() else 0

        except Exception as e:
            logger.error(f"Error in delete_user_emoji: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_user_activity(self, guild_id: int, user_id: int):

        try:
            logger.info(
                f"delete_user_activity: guild={guild_id}, user={user_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    deleted_counts = {}

                    result1 = await conn.execute(f"DELETE FROM activity_active_sessions WHERE guild_id = $1 AND user_id = $2",
                                                 guild_id, user_id)
                    deleted_counts['activity_active_sessions'] = result1.split(
                    )[-1]

                    result2 = await conn.execute(f"DELETE FROM activity_sessions WHERE guild_id = $1 AND user_id = $2",
                                                 guild_id, user_id)
                    deleted_counts['activity_sessions'] = result2.split()[-1]

                    logger.info(
                        f"Deleted activity stats for user {user_id}: {deleted_counts}")
                    return deleted_counts

        except Exception as e:
            logger.error(f"Error in delete_user_activity: {e}")
            logger.error(traceback.format_exc())
            raise

    async def delete_user_mentions(self, guild_id: int, user_id: int,
                                   channel_id: Optional[int] = None,
                                   category_id: Optional[int] = None):

        try:
            logger.info(
                f"delete_user_mentions: guild={guild_id}, user={user_id}, channel={channel_id}, category={category_id}")

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    conditions = [
                        "guild_id = $1", "(mentioned_user_id = $2 OR mentioner_user_id = $2)"]
                    params = [guild_id, user_id]
                    param_counter = 3

                    if channel_id:
                        conditions.append(f"channel_id = ${param_counter}")
                        params.append(channel_id)
                        param_counter += 1

                    if category_id:
                        conditions.append(f"category_id = ${param_counter}")
                        params.append(category_id)
                        param_counter += 1

                    where_clause = " AND ".join(conditions)
                    result = await conn.execute(f"DELETE FROM user_mentions WHERE {where_clause}", *params)

                    deleted_count = result.split()[-1]
                    logger.info(
                        f"Deleted {deleted_count} mention records involving user {user_id}")
                    return int(deleted_count) if deleted_count.isdigit() else 0

        except Exception as e:
            logger.error(f"Error in delete_user_mentions: {e}")
            logger.error(traceback.format_exc())
            raise

    async def reset_server_stats(self, guild_id: int, stat_type: str,
                                 channel_id: Optional[int] = None,
                                 category_id: Optional[int] = None):

        logger.info(
            f"RESET SERVER STATS: guild={guild_id}, type={stat_type}, channel={channel_id}, category={category_id}")

        try:
            stat_type_lower = stat_type.lower()
            result = None

            if stat_type_lower == "all":
                result = await self.delete_server_all(guild_id, channel_id, category_id)
            elif stat_type_lower == "messages":
                result = await self.delete_server_messages(guild_id, channel_id, category_id)
            elif stat_type_lower == "voice activity":
                result = await self.delete_server_voice(guild_id, channel_id, category_id)
            elif stat_type_lower == "invite tracking":
                result = await self.delete_server_invites(guild_id)
            elif stat_type_lower == "emoji stats":
                result = await self.delete_server_emoji(guild_id, channel_id, category_id)
            elif stat_type_lower == "activity":
                result = await self.delete_server_activity(guild_id)
            elif stat_type_lower == "mentions":
                result = await self.delete_server_mentions(guild_id, channel_id, category_id)

            logger.info(f"Reset server stats completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Failed to reset server stats: {e}")
            logger.error(traceback.format_exc())
            raise

    async def reset_user_stats(self, guild_id: int, user_id: int, stat_type: str,
                               channel_id: Optional[int] = None,
                               category_id: Optional[int] = None):

        logger.info(
            f"RESET USER STATS: guild={guild_id}, user={user_id}, type={stat_type}, channel={channel_id}, category={category_id}")

        try:
            stat_type_lower = stat_type.lower()
            result = None

            if stat_type_lower == "all":
                result = await self.delete_user_all(guild_id, user_id, channel_id, category_id)
            elif stat_type_lower == "messages":
                result = await self.delete_user_messages(guild_id, user_id, channel_id, category_id)
            elif stat_type_lower == "voice activity":
                result = await self.delete_user_voice(guild_id, user_id, channel_id, category_id)
            elif stat_type_lower == "invite tracking":
                result = await self.delete_user_invites(guild_id, user_id)
            elif stat_type_lower == "emoji stats":
                result = await self.delete_user_emoji(guild_id, user_id, channel_id, category_id)
            elif stat_type_lower == "activity":
                result = await self.delete_user_activity(guild_id, user_id)
            elif stat_type_lower == "mentions":
                result = await self.delete_user_mentions(guild_id, user_id, channel_id, category_id)

            logger.info(f"Reset user stats completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Failed to reset user stats: {e}")
            logger.error(traceback.format_exc())
            raise


# INPUT ID BUTTON

class IDInputModal(Modal, title="Enter Channel/Category ID"):

    def __init__(self, scope: str, view):
        super().__init__(timeout=300)
        self.scope = scope
        self.view = view

        self.id_input = TextInput(
            label=f"Enter {scope.capitalize()} ID",
            placeholder=f"Paste the {scope} ID here...",
            max_length=30,
            required=True
        )
        self.add_item(self.id_input)

    async def on_submit(self, interaction: discord.Interaction):

        try:
            id_str = self.id_input.value.strip()
            if not id_str.isdigit():
                await interaction.response.send_message(
                    f"❌ Invalid ID! Please enter a valid numeric ID.",
                    ephemeral=True
                )
                return

            channel_id = int(id_str)

            if self.scope == "channel":
                channel = interaction.guild.get_channel(channel_id)
                if not channel or not isinstance(channel, (discord.TextChannel, discord.VoiceChannel)):
                    await interaction.response.send_message(
                        f"❌ Channel not found! Make sure the ID is correct and the channel exists in this server.",
                        ephemeral=True
                    )
                    return

                self.view.channel_id = channel_id
                self.view.selected_channel = channel

            else:
                category = discord.utils.get(
                    interaction.guild.categories, id=channel_id)
                if not category:
                    await interaction.response.send_message(
                        f"❌ Category not found! Make sure the ID is correct and the category exists in this server.",
                        ephemeral=True
                    )
                    return

                self.view.category_id = channel_id
                self.view.selected_category = category

            await interaction.response.defer()
            await self.view.show_page_3()

        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid ID format! Please enter a valid numeric ID.",
                ephemeral=True
            )
        except Exception as e:
            logger.error(f"Error in ID input modal: {e}")
            await interaction.response.send_message(
                f"❌ An error occurred: {str(e)[:200]}",
                ephemeral=True
            )


# UI COMPONENETS


class ResetView(View):
    def __init__(self, interaction: discord.Interaction, db_manager: DatabaseManager,
                 command_type: Literal["server", "user"], target_user_id: Optional[int] = None):
        super().__init__(timeout=300)
        self.interaction = interaction
        self.db_manager = db_manager
        self.command_type = command_type
        self.target_user_id = target_user_id
        self.scope = None
        self.stat_type = None
        self.channel_id = None
        self.category_id = None
        self.current_page = 1
        self.selected_channel = None
        self.selected_category = None

    def create_embed(self, title: str, description: str, color: int = 0xff0000) -> discord.Embed:

        embed = discord.Embed(
            title=title,
            description=description,
            color=color
        )
        embed.set_footer(text="Stats Reset System")
        return embed

    # PAGE 1: SELECT SCOPE

    async def show_page_1(self):

        self.clear_items()
        self.current_page = 1

        if self.command_type == "server":
            title = "Reset Server Stats"
            description = "**Page 1/4**\n**Where do you want to reset statistics?**\n\nSelect from the dropdown below:"
        else:
            user_text = f"<@{self.target_user_id}>" if self.target_user_id else "your"
            title = f"Reset {user_text.capitalize()} Stats"
            description = f"**Page 1/4**\n**Where do you want to reset {user_text} statistics?**\n\nSelect from the dropdown below:"

        embed = self.create_embed(title, description)

        select = Select(
            placeholder="Select scope...",
            options=[
                discord.SelectOption(
                    label="Server", value="server", description="Reset stats for entire server"),
                discord.SelectOption(label="Channel", value="channel",
                                     description="Reset stats for a specific channel"),
                discord.SelectOption(label="Category", value="category",
                                     description="Reset stats for a specific category")
            ]
        )

        async def scope_callback(interaction: discord.Interaction):
            if interaction.user.id != self.interaction.user.id:
                await interaction.response.send_message("You cannot use this menu!", ephemeral=True)
                return

            self.scope = select.values[0]

            if self.scope == "server":
                await interaction.response.defer()
                await self.show_page_3()
            else:

                await interaction.response.defer()
                await self.show_page_2()

        select.callback = scope_callback
        self.add_item(select)

        await self.interaction.edit_original_response(embed=embed, view=self)

    # PAGE 2: SELECT CHANNEL/CATEGORY

    async def show_page_2(self):

        self.clear_items()
        self.current_page = 2

        if self.scope == "channel":
            title = "Select Channel"
            description = "**Page 2/4**\n**Select a channel to reset statistics from:**\n\nChoose from the dropdown below **OR** click 'Input ID' to enter a channel ID manually."
            placeholder = "Choose a channel..."
            input_button_label = "📝 Input Channel ID"

            channels = [ch for ch in self.interaction.guild.channels
                        if isinstance(ch, (discord.TextChannel, discord.VoiceChannel))]

            options = []
            for channel in channels[:25]:
                channel_type = "📝" if isinstance(
                    channel, discord.TextChannel) else "🔊"
                options.append(discord.SelectOption(
                    label=f"{channel_type} {channel.name}",
                    value=str(channel.id),
                    description=f"{channel.name[:50]}"
                ))

        else:
            title = "Select Category"
            description = "**Page 2/4**\n**Select a category to reset statistics from:**\n\nChoose from the dropdown below **OR** click 'Input ID' to enter a category ID manually."
            placeholder = "Choose a category..."
            input_button_label = "📝 Input Category ID"

            categories = self.interaction.guild.categories[:25]
            options = []
            for category in categories:
                options.append(discord.SelectOption(
                    label=f"📁 {category.name}",
                    value=str(category.id),
                    description=f"{category.name[:50]}"
                ))

        if not options:
            embed = self.create_embed(
                "Error", f"No {self.scope}s found in this server!")
            await self.interaction.edit_original_response(embed=embed, view=None)
            return

        embed = self.create_embed(title, description)

        select = Select(placeholder=placeholder, options=options)

        async def selection_callback(interaction: discord.Interaction):
            if interaction.user.id != self.interaction.user.id:
                await interaction.response.send_message("You cannot use this menu!", ephemeral=True)
                return

            selected_id = int(select.values[0])
            if self.scope == "channel":
                self.channel_id = selected_id
                self.selected_channel = interaction.guild.get_channel(
                    selected_id)
            else:
                self.category_id = selected_id
                self.selected_category = discord.utils.get(
                    interaction.guild.categories, id=selected_id)

            await interaction.response.defer()
            await self.show_page_3()

        select.callback = selection_callback
        self.add_item(select)

        input_id_button = Button(
            label=input_button_label,
            style=discord.ButtonStyle.secondary,
            emoji="🔢"
        )

        async def input_id_callback(interaction: discord.Interaction):
            if interaction.user.id != self.interaction.user.id:
                await interaction.response.send_message("You cannot use this menu!", ephemeral=True)
                return

            modal = IDInputModal(self.scope, self)
            await interaction.response.send_modal(modal)

        input_id_button.callback = input_id_callback
        self.add_item(input_id_button)

        back_button = Button(
            label="Back", style=discord.ButtonStyle.gray, emoji="⬅️")

        async def back_callback(interaction: discord.Interaction):
            if interaction.user.id != self.interaction.user.id:
                await interaction.response.send_message("You cannot use this menu!", ephemeral=True)
                return

            await interaction.response.defer()
            await self.show_page_1()

        back_button.callback = back_callback
        self.add_item(back_button)

        await self.interaction.edit_original_response(embed=embed, view=self)

    # PAGE 3: SELECT STAT TYPE

    async def show_page_3(self):

        self.clear_items()
        self.current_page = 3

        scope_text = ""
        if self.scope == "server":
            scope_text = "the entire server"
        elif self.scope == "channel" and self.selected_channel:
            scope_text = f"channel #{self.selected_channel.name}"
        elif self.scope == "category" and self.selected_category:
            scope_text = f"category {self.selected_category.name}"

        if self.command_type == "server":
            title = "Select Stat Type"
            description = f"**Page 3/4**\n**Which type of statistics do you want to reset from {scope_text}?**\n\nSelect from the dropdown below:"
        else:
            user_text = f"<@{self.target_user_id}>" if self.target_user_id else "your"
            title = "Select Stat Type"
            description = f"**Page 3/4**\n**Which type of {user_text} statistics do you want to reset from {scope_text}?**\n\nSelect from the dropdown below:"

        embed = self.create_embed(title, description)

        # reset server (server)
        if self.command_type == "server":
            if self.scope == "server":
                options = [
                    discord.SelectOption(
                        label="All", value="all", description="Reset ALL statistics (irreversible!)", emoji="⚠️"),
                    discord.SelectOption(
                        label="Messages", value="messages", description="Reset message tracking", emoji="💬"),
                    discord.SelectOption(label="Voice Activity", value="voice activity",
                                         description="Reset voice session history", emoji="🔊"),
                    discord.SelectOption(label="Invite Tracking", value="invite tracking",
                                         description="Reset invite tracking", emoji="📨"),
                    discord.SelectOption(
                        label="Emoji Stats", value="emoji stats", description="Reset emoji usage", emoji="😀"),
                    discord.SelectOption(
                        label="Activity", value="activity", description="Reset activity sessions", emoji="🎮"),
                    discord.SelectOption(
                        label="Mentions", value="mentions", description="Reset user mentions", emoji="📢")
                ]
            else:
                options = [
                    discord.SelectOption(
                        label="All", value="all", description="Reset ALL statistics", emoji="⚠️"),
                    discord.SelectOption(
                        label="Messages", value="messages", description="Reset message tracking", emoji="💬"),
                    discord.SelectOption(label="Voice Activity", value="voice activity",
                                         description="Reset voice activity", emoji="🔊"),
                    discord.SelectOption(
                        label="Emoji Stats", value="emoji stats", description="Reset emoji usage", emoji="😀"),
                    discord.SelectOption(
                        label="Mentions", value="mentions", description="Reset user mentions", emoji="📢")
                ]
        else:
            # Reset user (server)
            if self.scope == "server":

                options = [
                    discord.SelectOption(
                        label="All", value="all", description="Reset ALL statistics", emoji="⚠️"),
                    discord.SelectOption(
                        label="Messages", value="messages", description="Reset message tracking", emoji="💬"),
                    discord.SelectOption(label="Voice Activity", value="voice activity",
                                         description="Reset voice activity", emoji="🔊"),
                    discord.SelectOption(
                        label="Activity", value="activity", description="Reset activity sessions", emoji="🎮"),
                    discord.SelectOption(label="Invite Tracking", value="invite tracking",
                                         description="Reset invite tracking", emoji="📨"),
                    discord.SelectOption(
                        label="Emoji Stats", value="emoji stats", description="Reset emoji usage", emoji="😀"),
                    discord.SelectOption(
                        label="Mentions", value="mentions", description="Reset mentions", emoji="📢")
                ]
            else:

                options = [
                    discord.SelectOption(
                        label="All", value="all", description="Reset ALL statistics", emoji="⚠️"),
                    discord.SelectOption(
                        label="Messages", value="messages", description="Reset message tracking", emoji="💬"),
                    discord.SelectOption(label="Voice Activity", value="voice activity",
                                         description="Reset voice activity", emoji="🔊"),
                    discord.SelectOption(
                        label="Activity", value="activity", description="Reset activity sessions", emoji="🎮"),
                    discord.SelectOption(
                        label="Emoji Stats", value="emoji stats", description="Reset emoji usage", emoji="😀"),
                    discord.SelectOption(
                        label="Mentions", value="mentions", description="Reset mentions", emoji="📢")
                ]

        select = Select(placeholder="Select stat type...", options=options)

        async def stat_callback(interaction: discord.Interaction):
            if interaction.user.id != self.interaction.user.id:
                await interaction.response.send_message("You cannot use this menu!", ephemeral=True)
                return

            self.stat_type = select.values[0]
            await interaction.response.defer()
            await self.show_page_4()

        select.callback = stat_callback
        self.add_item(select)

        back_button = Button(
            label="Back", style=discord.ButtonStyle.gray, emoji="⬅️")

        async def back_callback(interaction: discord.Interaction):
            if interaction.user.id != self.interaction.user.id:
                await interaction.response.send_message("You cannot use this menu!", ephemeral=True)
                return

            await interaction.response.defer()
            if self.scope == "server":
                await self.show_page_1()
            else:
                await self.show_page_2()

        back_button.callback = back_callback
        self.add_item(back_button)

        await self.interaction.edit_original_response(embed=embed, view=self)

    # PAGE 4: CONFIRMATION

    async def show_page_4(self):

        self.clear_items()
        self.current_page = 4

        scope_text = ""
        if self.scope == "server":
            scope_text = "the entire server"
        elif self.scope == "channel" and self.selected_channel:
            scope_text = f"channel #{self.selected_channel.name}"
        elif self.scope == "category" and self.selected_category:
            scope_text = f"category {self.selected_category.name}"

        if self.command_type == "server":
            title = "⚠️ Confirm Server Stats Reset"
            description = f"**Page 4/4 - FINAL CONFIRMATION**\n\n**Are you sure you want to delete `{self.stat_type}` from {scope_text}?**\n\n🚨 **This action is PERMANENT and CANNOT be undone!** 🚨"
        else:
            user_text = f"<@{self.target_user_id}>" if self.target_user_id else "your"
            title = "⚠️ Confirm User Stats Reset"
            description = f"**Page 4/4 - FINAL CONFIRMATION**\n\n**Are you sure you want to delete `{self.stat_type}` for {user_text} stats from {scope_text}?**\n\n🚨 **This action is PERMANENT and CANNOT be undone!** 🚨"

        embed = self.create_embed(title, description)

        confirm_button = Button(label="✅ Confirm Reset",
                                style=discord.ButtonStyle.danger, emoji="⚠️")

        async def confirm_callback(interaction: discord.Interaction):
            if interaction.user.id != self.interaction.user.id:
                await interaction.response.send_message("You cannot use this menu!", ephemeral=True)
                return

            for item in self.children:
                item.disabled = True

            await interaction.response.edit_message(view=self)

            processing_embed = self.create_embed(
                "🔄 Processing...",
                "Deleting statistics. This may take a moment..."
            )
            await interaction.edit_original_response(embed=processing_embed)

            try:
                if self.command_type == "server":
                    result = await self.db_manager.reset_server_stats(
                        guild_id=interaction.guild.id,
                        stat_type=self.stat_type,
                        channel_id=self.channel_id,
                        category_id=self.category_id
                    )

                    if isinstance(result, dict):
                        result_text = "\n".join(
                            [f"• {table}: {count} rows deleted" for table, count in result.items()])
                        total = sum(int(str(count).split()[0]) if str(
                            count).isdigit() else 0 for count in result.values())
                    else:
                        result_text = f"• {result} rows deleted" if isinstance(
                            result, int) else f"• {result}"
                        total = result if isinstance(result, int) else 0

                    success_embed = self.create_embed(
                        "✅ Reset Successful!",
                        f"Successfully deleted **{self.stat_type}** from **{scope_text}**!\n\n" +
                        f"**Results:**\n{result_text}\n\n" +
                        f"✅ **Total: {total} records deleted.**"
                    )
                else:
                    target_user = self.target_user_id if self.target_user_id else interaction.user.id

                    result = await self.db_manager.reset_user_stats(
                        guild_id=interaction.guild.id,
                        user_id=target_user,
                        stat_type=self.stat_type,
                        channel_id=self.channel_id,
                        category_id=self.category_id
                    )

                    if isinstance(result, dict):
                        result_text = "\n".join(
                            [f"• {table}: {count} rows deleted" for table, count in result.items()])
                        total = sum(int(str(count).split()[0]) if str(
                            count).isdigit() else 0 for count in result.values())
                    else:
                        result_text = f"• {result} rows deleted" if isinstance(
                            result, int) else f"• {result}"
                        total = result if isinstance(result, int) else 0

                    user_text = f"<@{target_user}>" if self.target_user_id else "your"
                    success_embed = self.create_embed(
                        "✅ Reset Successful!",
                        f"Successfully deleted **{self.stat_type}** for {user_text} stats from **{scope_text}**!\n\n" +
                        f"**Results:**\n{result_text}\n\n" +
                        f"✅ **Total: {total} records deleted.**"
                    )

                await interaction.edit_original_response(embed=success_embed, view=None)

            except Exception as e:
                logger.error(f"Reset failed: {e}")
                logger.error(traceback.format_exc())

                error_embed = self.create_embed(
                    "❌ Reset Failed!",
                    f"An error occurred while deleting statistics:\n```{str(e)[:1000]}```\n\nPlease try again or contact support."
                )
                await interaction.edit_original_response(embed=error_embed, view=None)

        confirm_button.callback = confirm_callback
        self.add_item(confirm_button)

        back_button = Button(
            label="Back", style=discord.ButtonStyle.gray, emoji="⬅️")

        async def back_callback(interaction: discord.Interaction):
            if interaction.user.id != self.interaction.user.id:
                await interaction.response.send_message("You cannot use this menu!", ephemeral=True)
                return

            await interaction.response.defer()
            await self.show_page_3()

        back_button.callback = back_callback
        self.add_item(back_button)

        await self.interaction.edit_original_response(embed=embed, view=self)


# INITIALIZATION

class ResetStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_pool = None
        self.db_manager = None
        self.active_sessions = {}
        logger.info("ResetStats cog initialized")

    async def create_db_pool(self):

        try:
            logger.info("Attempting to create database pool...")

            db_host = os.getenv('DB_HOST', 'localhost')
            db_port = int(os.getenv('DB_PORT', 5432))
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_name = os.getenv('DB_NAME')

            logger.info(
                f"DB Config - Host: {db_host}, Port: {db_port}, User: {db_user}, DB: {db_name}")

            if not all([db_user, db_password, db_name]):
                logger.error("Missing database environment variables!")
                return False

            self.db_pool = await asyncpg.create_pool(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                database=db_name,
                min_size=1,
                max_size=10,
                command_timeout=60
            )

            async with self.db_pool.acquire() as conn:
                version = await conn.fetchval('SELECT version()')
                logger.info(f"Database connected: {version.split()[0]}")

            self.db_manager = DatabaseManager(self.db_pool)

            logger.info(
                "✅ ResetStats: Database connection established and verified!")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to create database pool: {e}")
            logger.error(traceback.format_exc())
            self.db_pool = None
            self.db_manager = None
            return False

    async def cog_load(self):

        logger.info("ResetStats cog loading...")
        success = await self.create_db_pool()
        if not success:
            logger.error("Failed to establish database connection!")

    async def cog_unload(self):

        logger.info("ResetStats cog unloading...")
        if self.db_pool:
            await self.db_pool.close()
            logger.info("ResetStats: Database connection closed.")

    # COMMANDS

    reset_group = app_commands.Group(
        name="reset",
        description="Reset statistics commands"
    )

    @reset_group.command(name="server", description="Reset server statistics (Admin only)")
    @app_commands.checks.has_permissions(administrator=True)
    async def reset_server(self, interaction: discord.Interaction):

        if not self.db_manager or not self.db_pool:
            await interaction.response.send_message(
                "⚠️ Database connection not ready. Please try again in a moment.\n"
                "If this persists, contact the bot administrator.",
                ephemeral=True
            )
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Database connection error: {str(e)[:200]}\n"
                "Please contact the bot administrator.",
                ephemeral=True
            )
            return

        view = ResetView(interaction, self.db_manager, "server")
        embed = view.create_embed(
            "🔧 Server Stats Reset",
            "This command allows you to reset server statistics. Please follow the 4-step process.\n\n"
            "**Note:** This action is irreversible!"
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        await view.show_page_1()

        message = await interaction.original_response()
        self.active_sessions[message.id] = {
            'guild_id': interaction.guild.id,
            'user_id': interaction.user.id,
            'command_type': 'server',
            'view': view
        }

    @reset_group.command(name="user", description="Reset user statistics")
    @app_commands.describe(user="Optional: User to reset stats for (requires admin if not yourself)")
    async def reset_user(self, interaction: discord.Interaction, user: Optional[discord.User] = None):

        if not self.db_manager or not self.db_pool:
            await interaction.response.send_message(
                "⚠️ Database connection not ready. Please try again in a moment.\n"
                "If this persists, contact the bot administrator.",
                ephemeral=True
            )
            return

        try:
            async with self.db_pool.acquire() as conn:
                await conn.fetchval('SELECT 1')
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Database connection error: {str(e)[:200]}\n"
                "Please contact the bot administrator.",
                ephemeral=True
            )
            return

        if user and user.id != interaction.user.id:

            if not interaction.user.guild_permissions.administrator:

                if not (interaction.user.guild_permissions.manage_messages or
                        interaction.user.guild_permissions.manage_guild):
                    error_embed = discord.Embed(
                        title="❌ Permission Denied",
                        description="You need **administrator permissions** to reset another user's stats!\n\n"
                        "You can only reset your own statistics.",
                        color=0xff0000
                    )
                    await interaction.response.send_message(embed=error_embed, ephemeral=True)
                    return

        target_user_id = user.id if user else interaction.user.id

        view = ResetView(interaction, self.db_manager, "user", target_user_id)

        if user and user.id != interaction.user.id:

            embed = view.create_embed(
                f"Reset Stats for {user.display_name}",
                f"**Admin Action**: Resetting statistics for {user.mention}\n\n"
                f"Click the dropdown to begin the 4-step process."
            )
        elif user and user.id == interaction.user.id:

            embed = view.create_embed(
                "Reset Your Stats",
                "Click the dropdown to begin the 4-step process."
            )
        else:
            embed = view.create_embed(
                "Reset Your Stats",
                "Click the dropdown to begin the 4-step process."
            )

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        await view.show_page_1()

        message = await interaction.original_response()
        self.active_sessions[message.id] = {
            'guild_id': interaction.guild.id,
            'user_id': interaction.user.id,
            'target_user_id': target_user_id,
            'command_type': 'user',
            'view': view
        }

    # ERROR HANDLING

    @reset_server.error
    async def reset_server_error(self, interaction: discord.Interaction, error):

        if isinstance(error, app_commands.MissingPermissions):
            embed = discord.Embed(
                title="❌ Permission Denied",
                description="You need **administrator permissions** to use `/reset server` command!",
                color=0xff0000
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            logger.error(f"Reset server error: {error}")
            logger.error(traceback.format_exc())
            embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred: ```{str(error)[:500]}```",
                color=0xff0000
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

    @reset_user.error
    async def reset_user_error(self, interaction: discord.Interaction, error):

        logger.error(f"Reset user error: {error}")
        logger.error(traceback.format_exc())
        embed = discord.Embed(
            title="❌ Error",
            description=f"An error occurred: ```{str(error)[:500]}```",
            color=0xff0000
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)


# SETUP

async def setup(bot):
    cog = ResetStats(bot)
    await bot.add_cog(cog)
