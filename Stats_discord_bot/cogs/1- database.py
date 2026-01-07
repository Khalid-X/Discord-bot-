import asyncio
import json
import os
import traceback
import copy
import time
import uuid
from datetime import datetime, timedelta, date, timezone
from typing import Dict, List, Optional, Tuple, Any, Set, Union
from collections import defaultdict
import discord
from discord.ext import commands, tasks
import asyncpg
from redis.asyncio import Redis
from cryptography.fernet import Fernet
import pytz
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import io
import numpy as np
import logging
import hashlib
import re
from datetime import timezone
from asyncio import Semaphore
import emoji
import unicodedata
from emoji import is_emoji
import base64

logger = logging.getLogger(__name__)

load_dotenv()

# Constants


class Constants:
    # Redis batching settings
    REDIS_BATCH_FLUSH_INTERVAL = 30
    REDIS_BATCH_MAX_SIZE = 1000

    # Voice tracking
    VOICE_TRACKER_INTERVAL = 60

    # INFINITE RECONNECTION
    RECONNECT_BASE_DELAY = 1.0
    RECONNECT_MAX_DELAY = 300.0
    RECONNECT_MAX_ATTEMPTS = 0

    # HIGH OPERATION RETRIES
    DB_RETRY_ATTEMPTS = 10
    DB_RETRY_BASE_DELAY = 0.5
    DB_RETRY_MAX_DELAY = 30.0
    REDIS_RETRY_ATTEMPTS = 5
    REDIS_RETRY_DELAY = 0.1

    # Health check
    CONNECTION_POOL_HEALTH_CHECK_INTERVAL = 300
    SHUTDOWN_TIMEOUT = 30

    # TimescaleDB settings
    HYPERTABLE_CHUNK_TIME_INTERVAL = '7 days'
    COMPRESSION_SEGMENT_BY = 'guild_id'
    COMPRESSION_ORDER_BY = 'created_at DESC, id'
    COMPRESSION_AFTER_DAYS = {
        'message_tracking': 30,
        'voice_session_history': 60,
        'emoji_usage': 90,
        'invite_tracking': 90,
        'user_mentions': 60,
        'voice_time_by_state': 30,
        'activity_sessions': 30,
        'activities': 30
    }
    RETENTION_DAYS = {
        'message_tracking': 365,
        'voice_session_history': 365,
        'emoji_usage': 365,
        'invite_tracking': 365,
        'user_mentions': 365,
        'voice_time_by_state': 365,
        'activity_sessions': 365,
        'activities': 365
    }


class DatabaseStats(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.pool: Optional[asyncpg.Pool] = None
        self.redis: Optional[Redis] = None
        self.fernet: Optional[Fernet] = None

        # Async locks for thread safety
        self.voice_lock = asyncio.Lock()
        self.reconnect_lock = asyncio.Lock()
        self.shutdown_lock = asyncio.Lock()
        self.metrics_lock = asyncio.Lock()

        self.timescale_lock = asyncio.Lock()
        self.redis_batch_lock = asyncio.Lock()

        # State tracking
        self.shutting_down = False
        self.reconnect_attempts = 0
        self.db_connected = False
        self.redis_connected = False

        self.is_timescale_initialized = False
        self.processed_messages: Dict[str, datetime] = {}
        self.invite_locks: Dict[int, asyncio.Lock] = {}
        self.rate_limits: Dict[str, list] = {}

        # Start cleanup tasks
        self.cleanup_task = None

        # (timestamp, success)
        self.recent_operations: List[Tuple[float, bool]] = []
        self.total_operations_last_minute = 0
        self.failed_operations_last_minute = 0

        # Voice session tracking
        self.active_voice_sessions: Dict[int, Dict[str, Any]] = {}
        self.connection_semaphore = Semaphore(
            20)
        if not hasattr(bot, 'invites_cache'):
            bot.invites_cache = {}

        # Voice state flags mapping
        self.voice_state_bits = {
            'afk_channel': 1 << 0,
            'server_mute': 1 << 1,
            'server_deaf': 1 << 2,
            'self_mute': 1 << 3,
            'self_deaf': 1 << 4,
            'streaming': 1 << 5,
            'video': 1 << 6,
            'suppressed': 1 << 7,
        }

        # Redis write batching system
        self.redis_batches: Dict[str, List[Dict[str, Any]]] = {
            'messages': [],
            'voice_sessions': [],
            'voice_time': [],
            'mentions': [],
            'emojis': [],
            'invites': [],
            'activities': [],
            'activity_active': []
        }
        self.batch_sizes = {key: 0 for key in self.redis_batches.keys()}
        self.last_flush_time = time.time()

        # Metrics
        self.metrics = {
            'redis_writes': 0,
            'redis_batch_flushes': 0,
            'voice_updates': 0,
            'message_inserts': 0,
            'emoji_inserts': 0,
            'invite_inserts': 0,
            'mention_inserts': 0,
            'errors': 0,

            'db_queries': 0,
            'active_voice_sessions': 0,
            'connection_retries': 0,
            'failed_operations': 0,
            'timescale_compression_ratio': 0.0,
            'hypertable_sizes': {},
            'chunk_count': 0,
            'redis_batch_sizes': self.batch_sizes.copy()
        }

        # Start tasks
        self.init_pools.start()
        self.voice_activity_tracker.start()
        self.connection_pool_health_check.start()

        self.metrics_reset_task.start()
        self.timescale_maintenance.start()

        self.redis_batch_flusher.start()
        self.cleanup_task = asyncio.create_task(self._periodic_cleanup())

    # BUFFERING

    async def buffer_event(self, event_type: str, guild_id: int, data: Dict[str, Any]):

        if event_type == 'emoji':

            await self._buffer_emoji_event(guild_id, data)
        elif event_type == 'invite':

            await self._buffer_invite_event(
                guild_id=guild_id,
                inviter_id=data.get('inviter_id', 0),
                invitee_id=data.get('invitee_id'),
                invite_code=data.get('invite_code', 'unknown'),
                invite_type=data.get('invite_type', 'valid')
            )
        elif event_type == 'message':

            message = data.get('message')
            if message:
                await self._track_message_content(message)
        else:
            logger.warning(
                f"Unknown event type for buffer_event: {event_type}")

    # BLACKLIST CHECKING METHODS

    async def is_user_or_roles_blacklisted(self, guild_id: int, user_id: int) -> bool:

        if not self.pool or not self.db_connected:
            return False

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return False

        try:

            blacklists = await self._get_cached_blacklists(guild_id)

            if user_id in blacklists['users']:
                return True

            member = guild.get_member(user_id)
            if member and blacklists['roles']:
                user_role_ids = {role.id for role in member.roles}
                if user_role_ids.intersection(blacklists['roles']):
                    return True

            return False

        except Exception as e:
            logger.error(f"Error checking user blacklist: {e}")
            return False

    async def _get_cached_blacklists(self, guild_id: int) -> Dict[str, Set[int]]:

        blacklists = {
            'users': set(),
            'channels': set(),
            'categories': set(),
            'roles': set()
        }

        if not self.pool or not self.db_connected:
            return blacklists

        try:
            async with self.connection_semaphore:
                async with self.pool.acquire() as conn:

                    rows = await conn.fetch('''
                        SELECT user_id FROM blacklisted_users 
                        WHERE guild_id = $1
                    ''', guild_id)
                    blacklists['users'] = {row['user_id'] for row in rows}

                    rows = await conn.fetch('''
                        SELECT channel_id FROM blacklisted_channels 
                        WHERE guild_id = $1
                    ''', guild_id)
                    blacklists['channels'] = {
                        row['channel_id'] for row in rows}

                    rows = await conn.fetch('''
                        SELECT category_id FROM blacklisted_categories 
                        WHERE guild_id = $1
                    ''', guild_id)
                    blacklists['categories'] = {
                        row['category_id'] for row in rows}

                    rows = await conn.fetch('''
                        SELECT role_id FROM blacklisted_roles 
                        WHERE guild_id = $1
                    ''', guild_id)
                    blacklists['roles'] = {row['role_id'] for row in rows}

        except Exception as e:
            logger.error(
                f"Error fetching blacklists for guild {guild_id}: {e}")

        return blacklists

    async def _is_user_blacklisted(self, guild_id: int, user_id: int) -> bool:

        blacklists = await self._get_cached_blacklists(guild_id)
        return user_id in blacklists['users']

    # CLEANUP TASKS

    async def _periodic_cleanup(self):

        while not self.shutting_down:
            await asyncio.sleep(3600)
            await self._cleanup_old_data()

    async def _cleanup_old_data(self):

        current_time = datetime.utcnow()

        expired_keys = []
        for key, timestamp in self.processed_messages.items():
            if (current_time - timestamp).total_seconds() > 3600:
                expired_keys.append(key)

        for key in expired_keys:
            del self.processed_messages[key]

        if expired_keys:
            logger.debug(
                f"Cleaned up {len(expired_keys)} old message keys")

        expired_rate_limits = []
        for key, timestamps in self.rate_limits.items():

            self.rate_limits[key] = [ts for ts in timestamps
                                     if (current_time - ts).total_seconds() < 3600]

            if not self.rate_limits[key]:
                expired_rate_limits.append(key)

        for key in expired_rate_limits:
            del self.rate_limits[key]

        for guild_id in list(self.bot.invites_cache.keys()):
            if not self.bot.get_guild(guild_id):
                del self.bot.invites_cache[guild_id]
                logger.debug(
                    f"Cleaned up invites cache for left guild {guild_id}")

        async with self.voice_lock:
            expired_sessions = []
            for user_id in list(self.active_voice_sessions.keys()):
                user_found = False
                for guild in self.bot.guilds:
                    if guild.get_member(user_id):
                        user_found = True
                        break

                if not user_found:
                    expired_sessions.append(user_id)

            for user_id in expired_sessions:
                del self.active_voice_sessions[user_id]

        if expired_sessions:
            logger.debug(
                f"Cleaned up {len(expired_sessions)} orphaned voice sessions")

    # REDIS WRITE BATCH SYSTEM

    async def redis_batch_write(self, batch_type: str, data: Dict[str, Any]):

        if not self.redis or not self.redis_connected:

            await self._write_direct_to_postgresql(batch_type, data)
            return

        try:
            async with self.redis_batch_lock:
                if batch_type not in self.redis_batches:
                    logger.warning(f"Unknown batch type: {batch_type}")
                    return

                self.redis_batches[batch_type].append(data)
                self.batch_sizes[batch_type] = len(
                    self.redis_batches[batch_type])

                async with self.metrics_lock:
                    self.metrics['redis_writes'] += 1
                    self.metrics['redis_batch_sizes'][batch_type] = self.batch_sizes[batch_type]

                if self.batch_sizes[batch_type] >= Constants.REDIS_BATCH_MAX_SIZE:
                    logger.info(
                        f"Batch {batch_type} reached max size, flushing...")
                    await self._flush_batch_to_postgresql(batch_type)

        except Exception as e:
            logger.error(f"Error in redis_batch_write for {batch_type}: {e}")
            await self._write_direct_to_postgresql(batch_type, data)

    async def _write_direct_to_postgresql(self, batch_type: str, data: Dict[str, Any]):

        if not self.pool:
            return False

        try:
            async with self.pool.acquire() as conn:
                if batch_type == 'messages':
                    if len(data['params']) >= 5:
                        message_id = data['params'][4]
                        exists = await conn.fetchval('''
                            SELECT EXISTS (
                                SELECT 1 FROM message_tracking 
                                WHERE message_id = $1
                            )
                        ''', message_id)
                        if exists:
                            return True

                    await conn.execute('''
                        INSERT INTO message_tracking 
                        (guild_id, user_id, channel_id, category_id, message_id,
                        encrypted_username, message_length, mentions, has_attachment,
                        has_embed, created_at, is_bot)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ''', *data['params'])

                elif batch_type == 'voice_sessions':
                    await conn.execute('''
                        INSERT INTO voice_session_history 
                        (guild_id, user_id, channel_id, category_id, encrypted_username,
                        join_time, leave_time, duration_seconds, state_flags,
                        was_muted, was_deafened, was_streaming, was_video)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ''', *data['params'])

                elif batch_type == 'voice_time':
                    params = data['params']
                    if len(params) >= 9:
                        exists = await conn.fetchval('''
                            SELECT EXISTS (
                                SELECT 1 FROM voice_time_by_state 
                                WHERE guild_id = $1 AND user_id = $2 
                                AND channel_id = $3 AND state_flags = $4
                            )
                        ''', params[0], params[1], params[2], params[5])

                        if exists:
                            await conn.execute('''
                                UPDATE voice_time_by_state 
                                SET duration_seconds = duration_seconds + $5,
                                    last_updated = $6
                                WHERE guild_id = $1 AND user_id = $2 
                                AND channel_id = $3 AND state_flags = $4
                            ''', params[0], params[1], params[2], params[5], params[7], params[8])
                        else:
                            await conn.execute('''
                                INSERT INTO voice_time_by_state 
                                (guild_id, user_id, channel_id, category_id, encrypted_username,
                                state_flags, state_category, duration_seconds, last_updated)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ''', *params)

                elif batch_type == 'mentions':
                    if len(data['params']) >= 8:

                        message_id = data['params'][5]
                        created_at = data['params'][6]

                        exists = await conn.fetchval('''
                            SELECT EXISTS (
                                SELECT 1 FROM user_mentions 
                                WHERE message_id = $1
                            )
                        ''', message_id)
                        if exists:
                            return True

                    if len(data['params']) >= 8:
                        await conn.execute('''
                            INSERT INTO user_mentions 
                            (guild_id, mentioned_user_id, mentioner_user_id, channel_id, 
                            category_id, message_id, created_at, encrypted_username)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ''', *data['params'])
                    else:

                        logger.warning(
                            f"Mentions batch has {len(data['params'])} params, expected 8")
                        return False

                elif batch_type == 'emojis':
                    params = data['params']
                    if len(params) >= 10:
                        await conn.execute('''
                            INSERT INTO emoji_usage 
                            (guild_id, user_id, channel_id, category_id, encrypted_username,
                            emoji_str, is_custom, usage_count, last_used, usage_type)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ''', *params)

                elif batch_type == 'invites':

                    await conn.execute('''
                        INSERT INTO invite_tracking 
                        (guild_id, inviter_id, invitee_id, invite_code, invite_type, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    ''', *data['params'][:6])

                else:
                    return False

                return True

        except asyncpg.exceptions.PostgresError as e:
            logger.error(f"Direct write {batch_type}: PostgreSQL error: {e}")
            return False

        except Exception as e:
            logger.error(f"Direct write {batch_type}: Unexpected error: {e}")
            traceback.print_exc()
            return False

    # BATCH FLUSHING METHODS

    async def _flush_batch_to_postgresql(self, batch_type: str):

        if not self.pool or not self.db_connected:
            return

        async with self.redis_batch_lock:
            if batch_type not in self.redis_batches or not self.redis_batches[batch_type]:
                return

            batch_data = self.redis_batches[batch_type].copy()
            self.redis_batches[batch_type].clear()
            self.batch_sizes[batch_type] = 0

        if not batch_data:
            return

        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    for data in batch_data:
                        try:
                            if batch_type == 'messages':
                                message_id = data['params'][4]
                                exists = await conn.fetchval('''
                                    SELECT EXISTS (
                                        SELECT 1 FROM message_tracking 
                                        WHERE message_id = $1
                                    )
                                ''', message_id)
                                if exists:
                                    continue

                                await conn.execute('''
                                    INSERT INTO message_tracking 
                                    (guild_id, user_id, channel_id, category_id, message_id,
                                    encrypted_username, message_length, mentions, has_attachment,
                                    has_embed, created_at, is_bot)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                                ''', *data['params'])

                            elif batch_type == 'voice_sessions':
                                await conn.execute('''
                                    INSERT INTO voice_session_history 
                                    (guild_id, user_id, channel_id, category_id, encrypted_username,
                                    join_time, leave_time, duration_seconds, state_flags,
                                    was_muted, was_deafened, was_streaming, was_video)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                                ''', *data['params'])

                            elif batch_type == 'voice_time':
                                params = data['params']
                                exists = await conn.fetchval('''
                                    SELECT EXISTS (
                                        SELECT 1 FROM voice_time_by_state 
                                        WHERE guild_id = $1 AND user_id = $2 
                                        AND channel_id = $3 AND state_flags = $4
                                    )
                                ''', params[0], params[1], params[2], params[5])

                                if exists:
                                    await conn.execute('''
                                        UPDATE voice_time_by_state 
                                        SET duration_seconds = duration_seconds + $6,
                                            last_updated = $7
                                        WHERE guild_id = $1 AND user_id = $2 
                                        AND channel_id = $3 AND state_flags = $5
                                    ''', params[0], params[1], params[2], params[3], params[5], params[7], params[8])
                                else:
                                    await conn.execute('''
                                        INSERT INTO voice_time_by_state 
                                        (guild_id, user_id, channel_id, category_id, encrypted_username,
                                        state_flags, state_category, duration_seconds, last_updated)
                                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                                    ''', *params)

                            elif batch_type == 'mentions':

                                if len(data['params']) >= 8:
                                    message_id = data['params'][5]
                                    exists = await conn.fetchval('''
                                        SELECT EXISTS (
                                            SELECT 1 FROM user_mentions 
                                            WHERE message_id = $1
                                        )
                                    ''', message_id)
                                    if exists:
                                        continue

                                if len(data['params']) >= 8:
                                    await conn.execute('''
                                        INSERT INTO user_mentions 
                                        (guild_id, mentioned_user_id, mentioner_user_id, channel_id, 
                                        category_id, message_id, created_at, encrypted_username)
                                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                                    ''', *data['params'])
                                else:
                                    logger.warning(
                                        f"Mentions batch in flush has {len(data['params'])} params")
                                    continue
                            elif batch_type == 'emojis':
                                params = data['params']
                                await conn.execute('''
                                    INSERT INTO emoji_usage 
                                    (guild_id, user_id, channel_id, category_id, encrypted_username,
                                    emoji_str, is_custom, usage_count, last_used, usage_type)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                ''', *params)

                            elif batch_type == 'invites':

                                await conn.execute('''
                                    INSERT INTO invite_tracking 
                                    (guild_id, inviter_id, invitee_id, invite_code, invite_type, created_at)
                                    VALUES ($1, $2, $3, $4, $5, $6)
                                ''', *data['params'])

                            else:
                                continue

                        except Exception as e:
                            logger.error(
                                f"Error flushing individual {batch_type} record: {e}")
                            continue

                    async with self.metrics_lock:
                        self.metrics['redis_batch_flushes'] += 1
                        if batch_type in self.metrics['redis_batch_sizes']:
                            self.metrics['redis_batch_sizes'][batch_type] = 0

        except Exception as e:
            logger.error(
                f"Error flushing {batch_type} batch to PostgreSQL: {e}")
            async with self.redis_batch_lock:
                self.redis_batches[batch_type].extend(batch_data)
                self.batch_sizes[batch_type] = len(
                    self.redis_batches[batch_type])

    async def _flush_all_batches_to_postgresql(self):

        for batch_type in list(self.redis_batches.keys()):
            await self._flush_batch_to_postgresql(batch_type)
        print("✅ Flushed all Redis batches to PostgreSQL")

    @tasks.loop(seconds=Constants.REDIS_BATCH_FLUSH_INTERVAL)
    async def redis_batch_flusher(self):

        if not self.pool or not self.db_connected:
            return

        try:
            await self._flush_all_batches_to_postgresql()
            self.last_flush_time = time.time()
        except Exception as e:
            logger.error(f"Error in redis_batch_flusher: {e}")

    # TIMESCALEDB INITIALIZATION

    async def _create_continuous_aggregates_safely(self, conn):

        try:

            is_hypertable = await conn.fetchval('''
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables 
                    WHERE hypertable_name = 'message_tracking'
                )
            ''')

            if is_hypertable:

                await conn.execute('''
                    CREATE MATERIALIZED VIEW IF NOT EXISTS daily_message_counts
                    WITH (timescaledb.continuous) AS
                    SELECT
                        time_bucket('1 day', created_at) AS bucket,
                        guild_id,
                        COUNT(*) as message_count,
                        COUNT(DISTINCT user_id) as unique_users,
                        SUM(message_length) as total_chars
                    FROM message_tracking
                    WHERE NOT is_bot
                    GROUP BY bucket, guild_id;
                ''')

                try:
                    await conn.execute('''
                        SELECT add_continuous_aggregate_policy('daily_message_counts',
                            start_offset => INTERVAL '30 days',
                            end_offset => INTERVAL '1 hour',
                            schedule_interval => INTERVAL '1 hour');
                    ''')
                except Exception:
                    pass

        except Exception as e:
            print(
                f"Failed to create daily_message_counts aggregate: {e}")

        try:
            is_hypertable = await conn.fetchval('''
                SELECT EXISTS (
                    SELECT 1 FROM timescaledb_information.hypertables 
                    WHERE hypertable_name = 'voice_session_history'
                )
            ''')

            if is_hypertable:

                await conn.execute('''
                    CREATE MATERIALIZED VIEW IF NOT EXISTS daily_voice_time
                    WITH (timescaledb.continuous) AS
                    SELECT
                        time_bucket('1 day', join_time) AS bucket,
                        guild_id,
                        SUM(duration_seconds) as total_seconds,
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(*) as session_count
                    FROM voice_session_history
                    GROUP BY bucket, guild_id;
                ''')

                try:
                    await conn.execute('''
                        SELECT add_continuous_aggregate_policy('daily_voice_time',
                            start_offset => INTERVAL '30 days',
                            end_offset => INTERVAL '1 hour',
                            schedule_interval => INTERVAL '1 hour');
                    ''')
                except Exception:
                    pass

                logger.info("Set up daily_voice_time continuous aggregate")
            else:
                logger.warning(
                    "Cannot create daily_voice_time aggregate - voice_session_history not a hypertable")
        except Exception as e:
            logger.warning(f"Failed to create daily_voice_time aggregate: {e}")

    # DATABASE INITIALIZATION

    async def _initialize_database_schema(self):

        if not self.pool or not self.db_connected:
            logger.error("No database connection for initialization")
            return False

        try:
            async with self.pool.acquire() as conn:

                # 1. Blacklist tables
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS blacklisted_users (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(guild_id, user_id)
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS blacklisted_channels (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(guild_id, channel_id)
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS blacklisted_categories (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        category_id BIGINT NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(guild_id, category_id)
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS blacklisted_roles (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        role_id BIGINT NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(guild_id, role_id)
                    )
                ''')

                # 2. Invite tracking helper table
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS invite_uses (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        invite_code TEXT NOT NULL,
                        uses INT DEFAULT 0,
                        inviter_id BIGINT,
                        deleted BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(guild_id, invite_code)
                    )
                ''')

                # HYPERTABLES

                # 1. message_tracking
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS message_tracking (
                        id BIGSERIAL,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        category_id BIGINT,
                        message_id BIGINT NOT NULL,
                        encrypted_username TEXT,
                        message_length INT NOT NULL DEFAULT 0,
                        mentions JSONB,
                        has_attachment BOOLEAN DEFAULT FALSE,
                        has_embed BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        is_bot BOOLEAN DEFAULT FALSE,
                        
                        PRIMARY KEY (id, created_at),
                        UNIQUE(message_id, created_at)
                    )
                ''')

                # 2. voice_session_history
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS voice_session_history (
                        id BIGSERIAL,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        category_id BIGINT,
                        encrypted_username TEXT,
                        join_time TIMESTAMPTZ NOT NULL,
                        leave_time TIMESTAMPTZ NOT NULL,
                        duration_seconds INT NOT NULL,
                        state_flags INT DEFAULT 0,
                        was_muted BOOLEAN DEFAULT FALSE,
                        was_deafened BOOLEAN DEFAULT FALSE,
                        was_streaming BOOLEAN DEFAULT FALSE,
                        was_video BOOLEAN DEFAULT FALSE,
                        
                        PRIMARY KEY (id, join_time)
                    )
                ''')

                # 3. emoji_usage
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS emoji_usage (
                        id BIGSERIAL,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        category_id BIGINT,
                        encrypted_username TEXT,
                        emoji_str TEXT NOT NULL,
                        is_custom BOOLEAN DEFAULT FALSE,
                        usage_count INT DEFAULT 1,
                        last_used TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        usage_type TEXT NOT NULL,  -- 'message' or 'reaction'
                        
                        PRIMARY KEY (id, last_used),
                        UNIQUE(guild_id, user_id, channel_id, emoji_str, usage_type, last_used)
                    )
                ''')

                # 4. invite_tracking
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS invite_tracking (
                        id BIGSERIAL,
                        guild_id BIGINT NOT NULL,
                        inviter_id BIGINT NOT NULL,
                        invitee_id BIGINT,
                        invite_code TEXT NOT NULL,
                        invite_type TEXT NOT NULL,  -- 'valid', 'suspicious', 'left'
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        
                        PRIMARY KEY (id, created_at)
                    )
                ''')

                # 5. user_mentions
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS user_mentions (
                        id BIGSERIAL,
                        guild_id BIGINT NOT NULL,
                        mentioned_user_id BIGINT NOT NULL,
                        mentioner_user_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        category_id BIGINT,
                        message_id BIGINT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        encrypted_username TEXT,
                        
                        PRIMARY KEY (id, created_at),
                        UNIQUE(message_id, created_at)
                    )
                ''')

                # 6. voice_time_by_state
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS voice_time_by_state (
                        id BIGSERIAL,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        channel_id BIGINT NOT NULL,
                        category_id BIGINT,
                        encrypted_username TEXT,
                        state_flags INT NOT NULL,
                        state_category TEXT NOT NULL,
                        duration_seconds INT NOT NULL DEFAULT 0,
                        last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        
                        PRIMARY KEY (id, last_updated),
                        UNIQUE(guild_id, user_id, channel_id, state_flags, state_category)
                    )
                ''')

                # 7. activity_sessions
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS activity_sessions (
                        id BIGSERIAL,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        channel_id BIGINT,
                        category_id BIGINT,
                        encrypted_username TEXT,
                        activity_type TEXT NOT NULL,
                        start_time TIMESTAMPTZ NOT NULL,
                        end_time TIMESTAMPTZ,
                        duration_seconds INT,
                        
                        PRIMARY KEY (id, start_time)
                    )
                ''')

                # 8. activities
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS activities (
                        id BIGSERIAL,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        encrypted_username TEXT,
                        activity_name TEXT NOT NULL,
                        activity_type TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        
                        PRIMARY KEY (id, created_at)
                    )
                ''')

                logger.info("✅ Created all base tables")

                # HYPERTABLE CONVERSION

                hypertables = [
                    ('message_tracking', 'created_at'),
                    ('voice_session_history', 'join_time'),
                    ('emoji_usage', 'last_used'),
                    ('invite_tracking', 'created_at'),
                    ('user_mentions', 'created_at'),
                    ('voice_time_by_state', 'last_updated'),
                    ('activity_sessions', 'start_time'),
                    ('activities', 'created_at')
                ]

                for table_name, time_column in hypertables:
                    try:
                        await conn.execute(f'''
                            SELECT create_hypertable(
                                '{table_name}', 
                                '{time_column}', 
                                chunk_time_interval => INTERVAL '7 days',
                                if_not_exists => TRUE
                            )
                        ''')
                        logger.info(f"✅ Converted {table_name} to hypertable")
                    except Exception as e:
                        logger.warning(
                            f"⚠️ Could not create hypertable for {table_name}: {e}")

                await self._create_continuous_aggregates_safely(conn)

                # INDEXES

                # Message tracking indexes
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_message_tracking_guild_user 
                    ON message_tracking (guild_id, user_id, created_at DESC)
                ''')

                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_message_tracking_guild_channel 
                    ON message_tracking (guild_id, channel_id, created_at DESC)
                ''')

                # Voice session indexes
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_voice_session_guild_user 
                    ON voice_session_history (guild_id, user_id, join_time DESC)
                ''')

                # Emoji usage indexes
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_emoji_usage_guild_emoji 
                    ON emoji_usage (guild_id, emoji_str, last_used DESC)
                ''')

                # User mentions indexes
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_user_mentions_guild_mentioned 
                    ON user_mentions (guild_id, mentioned_user_id, created_at DESC)
                ''')

                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_user_mentions_guild_mentioner 
                    ON user_mentions (guild_id, mentioner_user_id, created_at DESC)
                ''')

                logger.info("✅ Created all indexes")

                return True

        except Exception as e:
            logger.error(f"❌ Error initializing database schema: {e}")
            traceback.print_exc()
            return False

    # DIRECT POSTGRESQL QUERIES FOR BLACKLISTS

    async def _get_cached_blacklists(self, guild_id: int) -> Dict[str, Any]:

        blacklists = {
            'users': set(),
            'channels': set(),
            'categories': set(),
            'roles': set()
        }

        if not self.pool or not self.db_connected:
            return blacklists

        try:
            async with self.connection_semaphore:
                async with self.pool.acquire() as conn:

                    rows = await conn.fetch('''
                        SELECT user_id FROM blacklisted_users 
                        WHERE guild_id = $1
                    ''', guild_id)
                    blacklists['users'] = {row['user_id'] for row in rows}

                    rows = await conn.fetch('''
                        SELECT channel_id FROM blacklisted_channels 
                        WHERE guild_id = $1
                    ''', guild_id)
                    blacklists['channels'] = {
                        row['channel_id'] for row in rows}

                    rows = await conn.fetch('''
                        SELECT category_id FROM blacklisted_categories 
                        WHERE guild_id = $1
                    ''', guild_id)
                    blacklists['categories'] = {
                        row['category_id'] for row in rows}

                    rows = await conn.fetch('''
                        SELECT role_id FROM blacklisted_roles 
                        WHERE guild_id = $1
                    ''', guild_id)
                    blacklists['roles'] = {row['role_id'] for row in rows}

        except Exception as e:
            logger.error(
                f"Error fetching blacklists for guild {guild_id}: {e}")

        return blacklists

    async def _apply_comprehensive_blacklist_filters(self, guild_id: int, guild: discord.Guild,
                                                     base_query: str, params: List,
                                                     table_name: str,
                                                     include_users: bool = True,
                                                     include_channels: bool = True,
                                                     specific_channel_id: Optional[int] = None,
                                                     specific_category_id: Optional[int] = None) -> Tuple[Optional[str], List]:

        if not include_users and not include_channels:
            return base_query, params

        blacklists = await self._get_cached_blacklists(guild_id)

        # CHANNEL BLACKLISTING
        if specific_channel_id is not None:

            if specific_channel_id in blacklists['channels']:
                return None, []

            if guild:
                channel = guild.get_channel(specific_channel_id)
                if channel and hasattr(channel, 'category_id') and channel.category_id:
                    if channel.category_id in blacklists['categories']:
                        return None, []

        # CATEGORY BLACKLISTING
        if specific_category_id is not None and specific_category_id in blacklists['categories']:
            return None, []

        filter_parts = []
        new_params = params.copy()

        # USER BLACKLISTING
        if include_users and guild:
            excluded_users = set()

            if blacklists['users']:
                excluded_users.update(blacklists['users'])

            if blacklists['roles'] and guild.members:
                blacklisted_role_ids = blacklists['roles']

                for member in guild.members:
                    if any(role.id in blacklisted_role_ids for role in member.roles):
                        excluded_users.add(member.id)

            if excluded_users:
                placeholders = ', '.join([f'${i}' for i in range(len(new_params) + 1,
                                                                 len(new_params) + len(excluded_users) + 1)])
                filter_parts.append(f"user_id NOT IN ({placeholders})")
                new_params.extend(list(excluded_users))

        # CHANNEL & CATEGORY BLACKLIST FILTERS

        if include_channels and guild:
            channel_filters = []

            tables_with_channels = ['message_tracking', 'voice_session_history',
                                    'emoji_usage', 'voice_time_by_state', 'activity_sessions']

            tables_without_channels = [
                'user_mentions', 'invite_tracking', 'activities']

            if table_name in tables_with_channels:

                if specific_channel_id is None and blacklists['channels']:
                    placeholders = ', '.join([f'${i}' for i in range(len(new_params) + 1,
                                                                     len(new_params) + len(blacklists['channels']) + 1)])
                    channel_filters.append(
                        f"channel_id NOT IN ({placeholders})")
                    new_params.extend(list(blacklists['channels']))

                if specific_category_id is None and blacklists['categories']:
                    placeholders = ', '.join([f'${i}' for i in range(len(new_params) + 1,
                                                                     len(new_params) + len(blacklists['categories']) + 1)])
                    channel_filters.append(
                        f"(category_id IS NULL OR category_id NOT IN ({placeholders}))")
                    new_params.extend(list(blacklists['categories']))

            if channel_filters:
                filter_parts.append("(" + " AND ".join(channel_filters) + ")")

        # APPLY FILTERS TO QUERY
        if filter_parts:
            if 'WHERE' in base_query.upper():
                base_query += " AND " + " AND ".join(filter_parts)
            else:
                base_query += " WHERE " + " AND ".join(filter_parts)

        return base_query, new_params

    # SHUTDOWN

    async def cog_unload(self):

        async with self.shutdown_lock:
            self.shutting_down = True

        tasks_to_stop = [
            self.voice_activity_tracker,
            self.connection_pool_health_check,

            self.init_pools,
            self.metrics_reset_task,
            self.timescale_maintenance,

            self.redis_batch_flusher,
            self.cleanup_task
        ]

        for task in tasks_to_stop:
            if task and not task.done():
                task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    *[task for task in tasks_to_stop if task and not task.done()],
                    return_exceptions=True
                ),
                timeout=Constants.SHUTDOWN_TIMEOUT
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        except Exception as e:
            logger.warning(f"Error during task shutdown: {e}")

        try:
            await self._flush_all_batches_to_postgresql()
        except Exception as e:
            logger.warning(f"Error during final Redis batch flush: {e}")

        try:
            await self._cleanup_all_voice_sessions()
        except Exception as e:
            logger.warning(f"Error during voice session cleanup: {e}")

        close_tasks = []
        if self.pool:
            close_tasks.append(self.pool.close())
        if self.redis:
            close_tasks.append(self.redis.close())

        if close_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*close_tasks, return_exceptions=True),
                    timeout=10
                )
            except (asyncio.TimeoutError, Exception) as e:
                logger.error(f"Error closing connections: {e}")

        try:
            await self._cleanup_all_memory()
        except Exception as e:
            logger.warning(f"Error during memory cleanup: {e}")

        logger.info("DatabaseStats cog shutdown complete")

    async def _cleanup_all_memory(self):

        self.processed_messages.clear()

        self.rate_limits.clear()

        self.invite_locks.clear()

        if hasattr(self.bot, 'invites_cache'):
            self.bot.invites_cache.clear()

        async with self.voice_lock:
            self.active_voice_sessions.clear()

        logger.debug("All memory caches cleared")

    async def _cleanup_all_voice_sessions(self):

        async with self.voice_lock:
            sessions_copy = copy.deepcopy(self.active_voice_sessions)
            self.active_voice_sessions.clear()

        current_time = datetime.utcnow()
        for user_id, session_data in sessions_copy.items():
            try:
                duration = int(
                    (current_time - session_data['join_time']).total_seconds())
                if duration > 0:
                    await self._record_voice_session_history_direct(
                        guild_id=session_data['guild_id'],
                        user_id=user_id,
                        channel_id=session_data['channel_id'],
                        join_time=session_data['join_time'],
                        leave_time=current_time,
                        duration=duration,
                        state_flags=self._calculate_state_flags(session_data)
                    )
            except Exception as e:
                logger.warning(
                    f"Error cleaning up voice session for user {user_id}: {e}")

    # DATABASE CONNECTION RETRY

    @tasks.loop(minutes=5, count=None)
    async def init_pools(self):

        print("🎯 INIT_POOLS TASK STARTED!")

        if self.shutting_down:
            return

        # POSTGRESQL CONNECTION

        if self.db_connected and self.pool and self.redis_connected and self.redis:
            return

        print("🔄 Creating PostgreSQL pool...")

        try:

            self.pool = await asyncpg.create_pool(
                host=os.getenv('DB_HOST'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                port=int(os.getenv('DB_PORT')),
                min_size=5,
                max_size=20,
                command_timeout=30
            )

            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')

            print("✅ PostgreSQL connection established")
            self.db_connected = True
            self.is_timescale_initialized = False

            print("🔄 Initializing database schema...")
            success = await self._initialize_database_schema()

            if success:
                self.is_timescale_initialized = True
                print("✅ Database schema initialized")
            else:
                print("⚠️ Database schema initialization failed")

            # REDIS CONNECTION

            redis_host = os.getenv('REDIS_HOST', 'localhost')
            redis_port = int(os.getenv('REDIS_PORT', '6379'))
            redis_password = os.getenv('REDIS_PASSWORD')
            redis_db = int(os.getenv('REDIS_DB', '0'))

            print("🔄 Connecting to Redis...")
            try:

                if redis_password:
                    redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
                else:
                    redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"

                self.redis = Redis.from_url(
                    redis_url,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                    retry_on_timeout=True,
                    encoding='utf-8'
                )

                await self.redis.ping()
                self.redis_connected = True
                print("✅ Redis connection established")

            except Exception as e:
                print(f"⚠️ Redis connection failed: {e}")
                self.redis_connected = False
                self.redis = None

            # ENCRYPTION INITIALIZATION

            fernet_key = os.getenv('FERNET_KEY')
            if fernet_key:
                try:

                    fernet_key = fernet_key.strip()

                    try:

                        self.fernet = Fernet(fernet_key.encode())
                        print("✅ Username encryption initialized with Fernet key")
                    except (ValueError, TypeError):

                        print("⚠️ Fernet key invalid, hashing to create valid key...")
                        key_hash = hashlib.sha256(fernet_key.encode()).digest()
                        valid_key = base64.urlsafe_b64encode(key_hash)
                        self.fernet = Fernet(valid_key)
                        print("✅ Username encryption initialized with hashed key")

                except Exception as e:
                    print(f"⚠️ Fernet encryption initialization failed: {e}")
                    self.fernet = None
            else:
                print("ℹ️ No FERNET_KEY in .env, username encryption disabled")
                self.fernet = None

            # VERIFICATION
            print("🎉 Database verification complete")
            print(
                f"   PostgreSQL: {'✅ Connected' if self.db_connected else '❌ Disconnected'}")
            print(
                f"   Redis: {'✅ Connected' if self.redis_connected else '❌ Disconnected'}")
            print(
                f"   Encryption: {'✅ Enabled' if self.fernet else '❌ Disabled'}")
            print(
                f"   TimescaleDB: {'✅ Initialized' if self.is_timescale_initialized else '❌ Not initialized'}")

        except Exception as e:
            print(f"❌ Connection failed: {e}")
            traceback.print_exc()
            self.db_connected = False
            self.redis_connected = False
            self.fernet = None

    # HELPER METHODS

    def _calculate_state_flags(self, session_data: Dict[str, Any]) -> int:

        state_flags = 0

        if session_data.get('is_afk_channel'):
            state_flags |= self.voice_state_bits['afk_channel']

        if session_data.get('server_mute'):
            state_flags |= self.voice_state_bits['server_mute']

        if session_data.get('server_deaf'):
            state_flags |= self.voice_state_bits['server_deaf']

        if session_data.get('self_mute'):
            state_flags |= self.voice_state_bits['self_mute']

        if session_data.get('self_deaf'):
            state_flags |= self.voice_state_bits['self_deaf']

        if session_data.get('streaming'):
            state_flags |= self.voice_state_bits['streaming']

        if session_data.get('video'):
            state_flags |= self.voice_state_bits['video']

        if session_data.get('suppressed'):
            state_flags |= self.voice_state_bits['suppressed']

        return state_flags

    async def _record_voice_session_history_direct(self, guild_id: int, user_id: int, channel_id: int,
                                                   join_time: datetime, leave_time: datetime,
                                                   duration: int, state_flags: int):

        try:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                return False

            member = guild.get_member(user_id)
            if not member:
                return False

            encrypted_username = self.encrypt_username(str(member))
            category_id = None

            channel = guild.get_channel(channel_id)
            if channel and hasattr(channel, 'category_id') and channel.category_id:
                category_id = channel.category_id

            async with self.pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO voice_session_history 
                    (guild_id, user_id, channel_id, category_id, encrypted_username,
                     join_time, leave_time, duration_seconds, state_flags,
                     was_muted, was_deafened, was_streaming, was_video)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ''', guild_id, user_id, channel_id, category_id, encrypted_username,
                                   join_time, leave_time, duration, state_flags,
                                   False, False, False, False)

            return True

        except Exception as e:
            logger.error(f"Error recording voice session: {e}")
            return False

    def encrypt_username(self, username: str) -> Optional[str]:

        if not username:
            return None

        if not self.fernet:
            return hashlib.sha256(username.encode()).hexdigest()[:32]

        try:
            return self.fernet.encrypt(username.encode()).decode()
        except Exception as e:
            logger.warning(
                f"Error encrypting username, falling back to hash: {e}")
            return hashlib.sha256(username.encode()).hexdigest()[:32]

    def decrypt_username(self, encrypted: Optional[str]) -> Optional[str]:

        if not encrypted or not self.fernet:
            return encrypted

        try:
            return self.fernet.decrypt(encrypted.encode()).decode()
        except Exception as e:
            logger.warning(f"Error decrypting username: {e}")
            return None

    def _serialize_datetime(self, obj: Any) -> Any:

        if isinstance(obj, datetime):
            if obj.tzinfo is None:
                obj = obj.replace(tzinfo=timezone.utc)
            return obj.isoformat()
        elif isinstance(obj, (list, tuple)):
            return [self._serialize_datetime(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._serialize_datetime(value) for key, value in obj.items()}
        elif isinstance(obj, date):
            return obj.isoformat()
        return obj

    def _ensure_int(self, value: Any) -> Optional[int]:

        if value is None:
            return None
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # TIMESCALEDB MAINTENANCE

    @tasks.loop(hours=6)
    async def timescale_maintenance(self):

        if not self.pool or not self.db_connected or not self.is_timescale_initialized:
            return

        try:
            async with self.pool.acquire() as conn:

                await self._update_compression_stats(conn)

                await self._update_hypertable_sizes(conn)

                try:
                    await conn.execute("CALL refresh_continuous_aggregate('daily_message_counts', NULL, NULL);")
                except:
                    pass

                try:
                    await conn.execute("CALL refresh_continuous_aggregate('daily_voice_time', NULL, NULL);")
                except:
                    pass

                logger.debug("TimescaleDB maintenance completed")

        except Exception as e:
            logger.warning(f"Error in TimescaleDB maintenance: {e}")

    async def _update_compression_stats(self, conn):

        try:
            rows = await conn.fetch('''
                SELECT 
                    hypertable_name,
                    ROUND(compression_ratio, 2) as compression_ratio,
                    total_chunks,
                    number_compressed_chunks
                FROM timescaledb_information.compressed_hypertable_stats;
            ''')

            for row in rows:
                async with self.metrics_lock:
                    self.metrics['timescale_compression_ratio'] = row['compression_ratio'] or 0.0
                    self.metrics['chunk_count'] = row['total_chunks'] or 0

        except Exception as e:
            logger.debug(f"Could not update compression stats: {e}")

    async def _update_hypertable_sizes(self, conn):

        try:
            rows = await conn.fetch('''
                SELECT 
                    hypertable_name,
                    pg_size_pretty(total_bytes) as total_size,
                    pg_size_pretty(table_bytes) as table_size,
                    pg_size_pretty(index_bytes) as index_size
                FROM timescaledb_information.hypertable
                JOIN LATERAL hypertable_detailed_size(hypertable_name::regclass)
                    ON true
                ORDER BY total_bytes DESC;
            ''')

            sizes = {}
            for row in rows:
                sizes[row['hypertable_name']] = {
                    'total': row['total_size'],
                    'table': row['table_size'],
                    'index': row['index_size']
                }

            async with self.metrics_lock:
                self.metrics['hypertable_sizes'] = sizes

        except Exception as e:
            logger.debug(f"Could not update hypertable sizes: {e}")

    #  QUERY FUNCTIONS

    # USER STATS (10 functions)

    async def q_user_rank_messages(self, guild_id: int, user_id: int,
                                   role_filter_ids: Optional[List[int]] = None,
                                   start_time: Optional[datetime] = None,
                                   end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {'rank': 0, 'total_users': 0, 'message_count': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {'rank': 0, 'total_users': 0, 'message_count': 0}

        try:
            async with self.pool.acquire() as conn:

                query = '''
                    SELECT 
                        user_id,
                        COUNT(*) as message_count,
                        RANK() OVER (ORDER BY COUNT(*) DESC) as rank
                    FROM message_tracking
                    WHERE guild_id = $1 AND NOT is_bot
                '''
                params = [guild_id]

                if start_time:
                    query += " AND created_at >= $2"
                    params.append(start_time)
                    if end_time:
                        query += " AND created_at <= $3"
                        params.append(end_time)
                elif end_time:
                    query += " AND created_at <= $2"
                    params.append(end_time)

                query += " GROUP BY user_id ORDER BY message_count DESC"

                rows = await conn.fetch(query, *params)

                user_rank = 0
                user_count = 0
                total_users = len(rows)

                for idx, row in enumerate(rows, 1):
                    if row['user_id'] == user_id:
                        user_rank = idx
                        user_count = row['message_count']
                        break

                return {
                    'rank': user_rank,
                    'total_users': total_users,
                    'message_count': user_count
                }

        except Exception as e:
            logger.error(f"Error in q_user_rank_messages: {e}")
            traceback.print_exc()
            return {'rank': 0, 'total_users': 0, 'message_count': 0}

    async def q_user_rank_voice(self, guild_id: int, user_id: int,
                                role_filter_ids: Optional[List[int]] = None,
                                start_time: Optional[datetime] = None,
                                end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {'rank': 0, 'total_users': 0, 'voice_seconds': 0, 'voice_hours': 0.0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {'rank': 0, 'total_users': 0, 'voice_seconds': 0, 'voice_hours': 0.0}

        try:
            async with self.pool.acquire() as conn:

                query = '''
                    SELECT 
                        user_id,
                        SUM(duration_seconds) as total_seconds,
                        RANK() OVER (ORDER BY SUM(duration_seconds) DESC) as rank
                    FROM voice_session_history
                    WHERE guild_id = $1
                '''
                params = [guild_id]

                if start_time:
                    query += " AND join_time >= $2"
                    params.append(start_time)
                    if end_time:
                        query += " AND join_time <= $3"
                        params.append(end_time)
                elif end_time:
                    query += " AND join_time <= $2"
                    params.append(end_time)

                query += " GROUP BY user_id HAVING SUM(duration_seconds) > 0 ORDER BY total_seconds DESC"

                rows = await conn.fetch(query, *params)

                user_rank = 0
                user_seconds = 0
                total_users = len(rows)

                for idx, row in enumerate(rows, 1):
                    if row['user_id'] == user_id:
                        user_rank = idx
                        user_seconds = row['total_seconds']
                        break

                return {
                    'rank': user_rank,
                    'total_users': total_users,
                    'voice_seconds': user_seconds,
                    'voice_hours': round(user_seconds / 3600, 2) if user_seconds > 0 else 0.0
                }

        except Exception as e:
            logger.error(f"Error in q_user_rank_voice: {e}")
            traceback.print_exc()
            return {'rank': 0, 'total_users': 0, 'voice_seconds': 0, 'voice_hours': 0.0}

    async def q_user_timeseries_messages_1d_5d_10d_20d_30d(self, guild_id: int, user_id: int,
                                                           role_filter_ids: Optional[List[int]] = None,
                                                           start_time: Optional[datetime] = None,
                                                           end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        try:
            async with self.pool.acquire() as conn:
                periods = [1, 5, 10, 20, 30]
                result = {}

                for days in periods:

                    window_end = end_time if end_time else datetime.utcnow()
                    window_start = window_end - timedelta(days=days)

                    query = '''
                        SELECT COUNT(*) as message_count
                        FROM message_tracking
                        WHERE guild_id = $1 
                        AND user_id = $2 
                        AND NOT is_bot
                        AND created_at >= $3
                        AND created_at <= $4
                    '''
                    params = [guild_id, user_id, window_start, window_end]

                    if role_filter_ids:
                        member = guild.get_member(user_id)
                        if not member:
                            result[f'{days}d'] = 0
                            continue

                        user_roles = [role.id for role in member.roles]
                        if not any(role_id in role_filter_ids for role_id in user_roles):
                            result[f'{days}d'] = 0
                            continue

                    query, params = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query, params, 'message_tracking',
                        include_users=True, include_channels=True
                    )

                    row = await conn.fetchrow(query, *params)
                    result[f'{days}d'] = row['message_count'] or 0

                return result

        except Exception as e:
            logger.error(f"Error in q_user_timeseries_messages: {e}")
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

    async def q_user_timeseries_voice_1d_5d_10d_20d_30d(self, guild_id: int, user_id: int,
                                                        role_filter_ids: Optional[List[int]] = None,
                                                        start_time: Optional[datetime] = None,
                                                        end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        try:
            async with self.pool.acquire() as conn:
                periods = [1, 5, 10, 20, 30]
                result = {}

                for days in periods:

                    window_end = end_time if end_time else datetime.utcnow()
                    window_start = window_end - timedelta(days=days)

                    query = '''
                        SELECT COALESCE(SUM(duration_seconds), 0) as voice_seconds
                        FROM voice_session_history
                        WHERE guild_id = $1 
                        AND user_id = $2 
                        AND join_time >= $3
                        AND join_time <= $4
                    '''
                    params = [guild_id, user_id, window_start, window_end]

                    if role_filter_ids:
                        member = guild.get_member(user_id)
                        if not member:
                            result[f'{days}d'] = 0
                            continue

                        user_roles = [role.id for role in member.roles]
                        if not any(role_id in role_filter_ids for role_id in user_roles):
                            result[f'{days}d'] = 0
                            continue

                    query, params = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query, params, 'voice_session_history',
                        include_users=True, include_channels=True
                    )

                    row = await conn.fetchrow(query, *params)
                    result[f'{days}d'] = row['voice_seconds'] or 0

                return result

        except Exception as e:
            logger.error(f"Error in q_user_timeseries_voice: {e}")
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

    async def q_user_total_messages(self, guild_id: int, user_id: int,
                                    role_filter_ids: Optional[List[int]] = None,
                                    start_time: Optional[datetime] = None,
                                    end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool or not self.db_connected:
            logger.error(
                f"No database connection for query q_user_total_messages")
            return {'total_messages': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'has_attachments': 0, 'has_embeds': 0,
                'first_message': None, 'last_message': None
            }

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        COUNT(*) as total_messages,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars,
                        COUNT(*) FILTER (WHERE has_attachment = TRUE) as has_attachments,
                        COUNT(*) FILTER (WHERE has_embed = TRUE) as has_embeds,
                        MIN(created_at) as first_message,
                        MAX(created_at) as last_message
                    FROM message_tracking
                    WHERE guild_id = $1 AND user_id = $2 AND NOT is_bot
                '''
                params = [guild_id, user_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    member = guild.get_member(user_id)
                    if not member:
                        return {
                            'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                            'has_attachments': 0, 'has_embeds': 0,
                            'first_message': None, 'last_message': None
                        }

                    user_roles = [role.id for role in member.roles]
                    if not any(role_id in role_filter_ids for role_id in user_roles):
                        return {
                            'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                            'has_attachments': 0, 'has_embeds': 0,
                            'first_message': None, 'last_message': None
                        }

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                row = await conn.fetchrow(query, *params)

                return {
                    'total_messages': row['total_messages'] or 0,
                    'total_chars': row['total_chars'] or 0,
                    'avg_chars': round(row['avg_chars'] or 0, 2),
                    'has_attachments': row['has_attachments'] or 0,
                    'has_embeds': row['has_embeds'] or 0,
                    'first_message': row['first_message'].isoformat() if row['first_message'] else None,
                    'last_message': row['last_message'].isoformat() if row['last_message'] else None
                }

        except Exception as e:
            logger.error(f"Error in q_user_total_messages: {e}")
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'has_attachments': 0, 'has_embeds': 0,
                'first_message': None, 'last_message': None
            }
        logger.debug(f"Query result: {row}")

    async def q_user_total_voice(self, guild_id: int, user_id: int,
                                 role_filter_ids: Optional[List[int]] = None,
                                 start_time: Optional[datetime] = None,
                                 end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool or not self.db_connected:
            logger.error(
                f"No database connection for query q_user_total_messages")
            return {'total_messages': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'avg_session': 0.0, 'first_session': None, 'last_session': None
            }

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count,
                        COALESCE(AVG(duration_seconds), 0) as avg_session,
                        MIN(join_time) as first_session,
                        MAX(leave_time) as last_session
                    FROM voice_session_history
                    WHERE guild_id = $1 AND user_id = $2
                '''
                params = [guild_id, user_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    member = guild.get_member(user_id)
                    if not member:
                        return {
                            'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                            'avg_session': 0.0, 'first_session': None, 'last_session': None
                        }

                    user_roles = [role.id for role in member.roles]
                    if not any(role_id in role_filter_ids for role_id in user_roles):
                        return {
                            'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                            'avg_session': 0.0, 'first_session': None, 'last_session': None
                        }

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                row = await conn.fetchrow(query, *params)

                total_seconds = row['total_seconds'] or 0

                return {
                    'total_seconds': total_seconds,
                    'total_hours': round(total_seconds / 3600, 2),
                    'session_count': row['session_count'] or 0,
                    'avg_session': round(row['avg_session'] or 0, 2),
                    'first_session': row['first_session'].isoformat() if row['first_session'] else None,
                    'last_session': row['last_session'].isoformat() if row['last_session'] else None
                }

        except Exception as e:
            logger.error(f"Error in q_user_total_voice: {e}")
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'avg_session': 0.0, 'first_session': None, 'last_session': None
            }

    async def q_user_messages_per_hour_distribution(self, guild_id: int, user_id: int,
                                                    role_filter_ids: Optional[List[int]] = None,
                                                    start_time: Optional[datetime] = None,
                                                    end_time: Optional[datetime] = None,
                                                    timezone_str: str = 'UTC') -> Dict[str, Any]:

        if not self.pool:
            result = {f'hour_{i}': 0 for i in range(24)}
            result.update({'total': 0, 'peak_hour': -1})
            return result

        guild = self.bot.get_guild(guild_id)
        if not guild:
            result = {f'hour_{i}': 0 for i in range(24)}
            result.update({'total': 0, 'peak_hour': -1})
            return result

        try:
            async with self.pool.acquire() as conn:

                query = f'''
                    SELECT 
                        EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                        COUNT(*) as message_count
                    FROM message_tracking
                    WHERE guild_id = $1 AND user_id = $2 AND NOT is_bot
                '''
                params = [guild_id, user_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    member = guild.get_member(user_id)
                    if not member:
                        result = {f'hour_{i}': 0 for i in range(24)}
                        result.update({'total': 0, 'peak_hour': -1})
                        return result

                    user_roles = [role.id for role in member.roles]
                    if not any(role_id in role_filter_ids for role_id in user_roles):
                        result = {f'hour_{i}': 0 for i in range(24)}
                        result.update({'total': 0, 'peak_hour': -1})
                        return result

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                query += '''
                    GROUP BY hour
                    ORDER BY hour
                '''

                rows = await conn.fetch(query, *params)

                result = {f'hour_{i}': 0 for i in range(24)}
                total = 0
                peak_hour = -1
                peak_count = 0

                for row in rows:
                    hour = int(row['hour'])
                    count = row['message_count'] or 0
                    result[f'hour_{hour}'] = count
                    total += count

                    if count > peak_count:
                        peak_count = count
                        peak_hour = hour

                result['total'] = total
                result['peak_hour'] = peak_hour

                return result

        except Exception as e:
            logger.error(
                f"Error in q_user_messages_per_hour_distribution: {e}")
            result = {f'hour_{i}': 0 for i in range(24)}
            result.update({'total': 0, 'peak_hour': -1})
            return result

    async def q_user_voice_per_hour_distribution(self, guild_id: int, user_id: int,
                                                 role_filter_ids: Optional[List[int]] = None,
                                                 start_time: Optional[datetime] = None,
                                                 end_time: Optional[datetime] = None,
                                                 timezone_str: str = 'UTC') -> Dict[str, Any]:

        if not self.pool or not self.db_connected:
            logger.error(
                f"No database connection for query q_user_total_messages")
            return {'total_messages': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {f'hour_{i}': 0 for i in range(24)} | {'total_seconds': 0, 'peak_hour': -1}

        try:
            async with self.pool.acquire() as conn:

                query = f'''
                    SELECT 
                        EXTRACT(HOUR FROM join_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 AND user_id = $2
                '''
                params = [guild_id, user_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    member = guild.get_member(user_id)
                    if not member:
                        return {f'hour_{i}': 0 for i in range(24)} | {'total_seconds': 0, 'peak_hour': -1}

                    user_roles = [role.id for role in member.roles]
                    if not any(role_id in role_filter_ids for role_id in user_roles):
                        return {f'hour_{i}': 0 for i in range(24)} | {'total_seconds': 0, 'peak_hour': -1}

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                query += '''
                    GROUP BY hour
                    ORDER BY hour
                '''

                rows = await conn.fetch(query, *params)

                result = {f'hour_{i}': 0 for i in range(24)}
                total_seconds = 0
                peak_hour = -1
                peak_seconds = 0

                for row in rows:
                    hour = int(row['hour'])
                    seconds = row['total_seconds'] or 0
                    result[f'hour_{hour}'] = seconds
                    total_seconds += seconds

                    if seconds > peak_seconds:
                        peak_seconds = seconds
                        peak_hour = hour

                result['total_seconds'] = total_seconds
                result['peak_hour'] = peak_hour

                return result

        except Exception as e:
            logger.error(
                f"Error in q_user_voice_per_hour_distribution: {e}")
            return {f'hour_{i}': 0 for i in range(24)} | {'total_seconds': 0, 'peak_hour': -1}

    async def q_user_top3_voice_channels(self, guild_id: int, user_id: int,
                                         role_filter_ids: Optional[List[int]] = None,
                                         start_time: Optional[datetime] = None,
                                         end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 AND user_id = $2
                '''
                total_params = [guild_id, user_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    member = guild.get_member(user_id)
                    if not member:
                        return []

                    user_roles = [role.id for role in member.roles]
                    if not any(role_id in role_filter_ids for role_id in user_roles):
                        return []

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        channel_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count
                    FROM voice_session_history
                    WHERE guild_id = $1 AND user_id = $2
                '''
                params = [guild_id, user_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:

                    pass

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                query += '''
                    GROUP BY channel_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT 3
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    channel_seconds = row['total_seconds'] or 0
                    percentage = (channel_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'channel_id': row['channel_id'],
                        'total_seconds': channel_seconds,
                        'total_hours': round(channel_seconds / 3600, 2),
                        'session_count': row['session_count'] or 0,
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_user_top3_voice_channels: {e}")
            return []

    async def q_user_top3_text_channels(self, guild_id: int, user_id: int,
                                        role_filter_ids: Optional[List[int]] = None,
                                        start_time: Optional[datetime] = None,
                                        end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 AND user_id = $2 AND NOT is_bot
                '''
                total_params = [guild_id, user_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    member = guild.get_member(user_id)
                    if not member:
                        return []

                    user_roles = [role.id for role in member.roles]
                    if not any(role_id in role_filter_ids for role_id in user_roles):
                        return []

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        channel_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 AND user_id = $2 AND NOT is_bot
                '''
                params = [guild_id, user_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                query += '''
                    GROUP BY channel_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT 3
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    channel_messages = row['message_count'] or 0
                    percentage = (channel_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'channel_id': row['channel_id'],
                        'message_count': channel_messages,
                        'total_chars': row['total_chars'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_user_top3_text_channels: {e}")
            return []

    # SERVER STATS (8 functions)

    async def q_server_top3_emojis(self, guild_id: int,
                                   role_filter_ids: Optional[List[int]] = None,
                                   start_time: Optional[datetime] = None,
                                   end_time: Optional[datetime] = None,
                                   usage_type: Optional[str] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        emoji_str,
                        is_custom,
                        SUM(usage_count) as total_usage,
                        COUNT(DISTINCT user_id) as unique_users
                    FROM emoji_usage
                    WHERE guild_id = $1
                '''
                params = [guild_id]

                if usage_type:
                    query += f" AND usage_type = ${len(params) + 1}"
                    params.append(usage_type)

                if start_time:
                    query += f" AND last_used >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND last_used <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'emoji_usage',
                    include_users=True, include_channels=True

                )

                query += '''
                    GROUP BY emoji_str, is_custom
                    HAVING SUM(usage_count) > 0
                    ORDER BY total_usage DESC
                    LIMIT 3
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    total_usage = row['total_usage'] or 0
                    unique_users = row['unique_users'] or 0
                    avg_usage = total_usage / max(unique_users, 1)

                    result.append({
                        'emoji_str': row['emoji_str'],
                        'is_custom': row['is_custom'],
                        'usage_count': total_usage,
                        'unique_users': unique_users,
                        'avg_usage_per_user': round(avg_usage, 2)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_server_top3_emojis: {e}")
            return []

    async def q_server_total_messages(self, guild_id: int,
                                      role_filter_ids: Optional[List[int]] = None,
                                      start_time: Optional[datetime] = None,
                                      end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'unique_users': 0, 'messages_per_user': 0.0,
                'has_attachments': 0, 'has_embeds': 0
            }

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'unique_users': 0, 'messages_per_user': 0.0,
                'has_attachments': 0, 'has_embeds': 0
            }

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        COUNT(*) as total_messages,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars,
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(*) FILTER (WHERE has_attachment = TRUE) as has_attachments,
                        COUNT(*) FILTER (WHERE has_embed = TRUE) as has_embeds
                    FROM message_tracking
                    WHERE guild_id = $1 AND NOT is_bot
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=True

                )

                row = await conn.fetchrow(query, *params)

                total_messages = row['total_messages'] or 0
                unique_users = row['unique_users'] or 0
                messages_per_user = total_messages / \
                    max(unique_users, 1)

                return {
                    'total_messages': total_messages,
                    'total_chars': row['total_chars'] or 0,
                    'avg_chars': round(row['avg_chars'] or 0, 2),
                    'unique_users': unique_users,
                    'messages_per_user': round(messages_per_user, 2),
                    'has_attachments': row['has_attachments'] or 0,
                    'has_embeds': row['has_embeds'] or 0
                }

        except Exception as e:
            logger.error(f"Error in q_server_total_messages: {e}")
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'unique_users': 0, 'messages_per_user': 0.0,
                'has_attachments': 0, 'has_embeds': 0
            }

    async def q_server_total_voice(self, guild_id: int,
                                   role_filter_ids: Optional[List[int]] = None,
                                   start_time: Optional[datetime] = None,
                                   end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'unique_users': 0, 'avg_session': 0.0, 'seconds_per_user': 0.0
            }

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'unique_users': 0, 'avg_session': 0.0, 'seconds_per_user': 0.0
            }

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count,
                        COUNT(DISTINCT user_id) as unique_users,
                        COALESCE(AVG(duration_seconds), 0) as avg_session
                    FROM voice_session_history
                    WHERE guild_id = $1
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                row = await conn.fetchrow(query, *params)

                total_seconds = row['total_seconds'] or 0
                unique_users = row['unique_users'] or 0
                seconds_per_user = total_seconds / max(unique_users, 1)

                return {
                    'total_seconds': total_seconds,
                    'total_hours': round(total_seconds / 3600, 2),
                    'session_count': row['session_count'] or 0,
                    'unique_users': unique_users,
                    'avg_session': round(row['avg_session'] or 0, 2),
                    'seconds_per_user': round(seconds_per_user, 2)
                }

        except Exception as e:
            logger.error(f"Error in q_server_total_voice: {e}")
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'unique_users': 0, 'avg_session': 0.0, 'seconds_per_user': 0.0
            }

    async def q_server_timeseries_messages_1d_5d_10d_20d_30d(self, guild_id: int,
                                                             role_filter_ids: Optional[List[int]] = None,
                                                             start_time: Optional[datetime] = None,
                                                             end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        try:
            async with self.pool.acquire() as conn:
                periods = [1, 5, 10, 20, 30]
                result = {}

                total_days_in_window = None
                if start_time and end_time:
                    total_days_in_window = (end_time - start_time).days
                elif start_time:
                    total_days_in_window = (
                        datetime.utcnow() - start_time).days
                elif end_time:

                    total_days_in_window = None

                for days in periods:

                    if total_days_in_window is not None and days > total_days_in_window:
                        result[f'{days}d'] = 0
                        continue

                    window_end = end_time if end_time else datetime.utcnow()
                    window_start = window_end - timedelta(days=days)

                    if start_time and window_start < start_time:
                        window_start = start_time

                    query = '''
                        SELECT COUNT(*) as message_count
                        FROM message_tracking
                        WHERE guild_id = $1 
                        AND NOT is_bot
                        AND created_at >= $2
                        AND created_at <= $3
                    '''
                    params = [guild_id, window_start, window_end]

                    if role_filter_ids:
                        user_ids = []
                        for member in guild.members:
                            if any(role.id in role_filter_ids for role in member.roles):
                                user_ids.append(member.id)

                        if user_ids:
                            placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                             len(params) + len(user_ids) + 1)])
                            query += f" AND user_id IN ({placeholders})"
                            params.extend(user_ids)

                    query, params = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query, params, 'message_tracking',
                        include_users=True, include_channels=True
                    )

                    row = await conn.fetchrow(query, *params)
                    result[f'{days}d'] = row['message_count'] or 0

                return result

        except Exception as e:
            logger.error(f"Error in q_server_timeseries_messages: {e}")
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

    async def q_server_timeseries_voice_1d_5d_10d_20d_30d(self, guild_id: int,
                                                          role_filter_ids: Optional[List[int]] = None,
                                                          start_time: Optional[datetime] = None,
                                                          end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        try:
            async with self.pool.acquire() as conn:
                periods = [1, 5, 10, 20, 30]
                result = {}

                total_days_in_window = None
                if start_time and end_time:
                    total_days_in_window = (end_time - start_time).days
                elif start_time:
                    total_days_in_window = (
                        datetime.utcnow() - start_time).days
                elif end_time:

                    total_days_in_window = None

                for days in periods:

                    if total_days_in_window is not None and days > total_days_in_window:
                        result[f'{days}d'] = 0
                        continue

                    window_end = end_time if end_time else datetime.utcnow()
                    window_start = window_end - timedelta(days=days)

                    if start_time and window_start < start_time:
                        window_start = start_time

                    query = '''
                        SELECT COALESCE(SUM(duration_seconds), 0) as voice_seconds
                        FROM voice_session_history
                        WHERE guild_id = $1 
                        AND join_time >= $2
                        AND join_time <= $3
                    '''
                    params = [guild_id, window_start, window_end]

                    if role_filter_ids:
                        user_ids = []
                        for member in guild.members:
                            if any(role.id in role_filter_ids for role in member.roles):
                                user_ids.append(member.id)

                        if user_ids:
                            placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                             len(params) + len(user_ids) + 1)])
                            query += f" AND user_id IN ({placeholders})"
                            params.extend(user_ids)

                    query, params = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query, params, 'voice_session_history',
                        include_users=True, include_channels=True
                    )

                    row = await conn.fetchrow(query, *params)
                    result[f'{days}d'] = row['voice_seconds'] or 0

                return result

        except Exception as e:
            logger.error(f"Error in q_server_timeseries_voice: {e}")
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

    async def q_server_top3_voice_channels(self, guild_id: int,
                                           role_filter_ids: Optional[List[int]] = None,
                                           start_time: Optional[datetime] = None,
                                           end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1
                '''
                total_params = [guild_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        channel_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(*) as session_count
                    FROM voice_session_history
                    WHERE guild_id = $1
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                query += '''
                    GROUP BY channel_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT 3
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    channel_seconds = row['total_seconds'] or 0
                    percentage = (channel_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'channel_id': row['channel_id'],
                        'total_seconds': channel_seconds,
                        'total_hours': round(channel_seconds / 3600, 2),
                        'unique_users': row['unique_users'] or 0,
                        'session_count': row['session_count'] or 0,
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_server_top3_voice_channels: {e}")
            return []

    async def q_server_top3_text_channels(self, guild_id: int,
                                          role_filter_ids: Optional[List[int]] = None,
                                          start_time: Optional[datetime] = None,
                                          end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 AND NOT is_bot
                '''
                total_params = [guild_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        channel_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COUNT(DISTINCT user_id) as unique_users,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 AND NOT is_bot
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                query += '''
                    GROUP BY channel_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT 3
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    channel_messages = row['message_count'] or 0
                    percentage = (channel_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'channel_id': row['channel_id'],
                        'message_count': channel_messages,
                        'total_chars': row['total_chars'] or 0,
                        'unique_users': row['unique_users'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_server_top3_text_channels: {e}")
            return []

    async def q_server_top3_users_messages(self, guild_id: int,
                                           role_filter_ids: Optional[List[int]] = None,
                                           start_time: Optional[datetime] = None,
                                           end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 AND NOT is_bot
                '''
                total_params = [guild_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 AND NOT is_bot
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                query += '''
                    GROUP BY user_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT 3
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_messages = row['message_count'] or 0
                    percentage = (user_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'message_count': user_messages,
                        'total_chars': row['total_chars'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_server_top3_users_messages: {e}")
            return []

    async def q_server_top3_users_voice(self, guild_id: int,
                                        role_filter_ids: Optional[List[int]] = None,
                                        start_time: Optional[datetime] = None,
                                        end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1
                '''
                total_params = [guild_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count,
                        COALESCE(AVG(duration_seconds), 0) as avg_session
                    FROM voice_session_history
                    WHERE guild_id = $1
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                query += '''
                    GROUP BY user_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT 3
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_seconds = row['total_seconds'] or 0
                    percentage = (user_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'total_seconds': user_seconds,
                        'total_hours': round(user_seconds / 3600, 2),
                        'session_count': row['session_count'] or 0,
                        'avg_session': round(row['avg_session'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_server_top3_users_voice: {e}")
            return []

    # CATEGORY STATS (9 functions)

    async def q_category_channel_count(self,  guild_id: int, category_id: int) -> Dict[str, Any]:

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {'text_channels': 0, 'voice_channels': 0, 'total_channels': 0}

        try:
            text_channels = 0
            voice_channels = 0

            for channel in guild.channels:
                if hasattr(channel, 'category_id') and channel.category_id == category_id:
                    if isinstance(channel, discord.TextChannel):
                        text_channels += 1
                    elif isinstance(channel, discord.VoiceChannel):
                        voice_channels += 1

            return {
                'text_channels': text_channels,
                'voice_channels': voice_channels,
                'total_channels': text_channels + voice_channels
            }

        except Exception as e:
            logger.error(f"Error in q_category_channel_count: {e}")
            return {'text_channels': 0, 'voice_channels': 0, 'total_channels': 0}

    async def q_category_total_messages(self, guild_id: int, category_id: int,
                                        role_filter_ids: Optional[List[int]] = None,
                                        start_time: Optional[datetime] = None,
                                        end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'unique_users': 0, 'messages_per_user': 0.0
            }

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'unique_users': 0, 'messages_per_user': 0.0
            }

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        COUNT(*) as total_messages,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars,
                        COUNT(DISTINCT user_id) as unique_users
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND category_id = $2 
                    AND NOT is_bot
                '''
                params = [guild_id, category_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',

                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                row = await conn.fetchrow(query, *params)

                total_messages = row['total_messages'] or 0
                unique_users = row['unique_users'] or 0
                messages_per_user = total_messages / max(unique_users, 1)

                return {
                    'total_messages': total_messages,
                    'total_chars': row['total_chars'] or 0,
                    'avg_chars': round(row['avg_chars'] or 0, 2),
                    'unique_users': unique_users,
                    'messages_per_user': round(messages_per_user, 2)
                }

        except Exception as e:
            logger.error(f"Error in q_category_total_messages: {e}")
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'unique_users': 0, 'messages_per_user': 0.0
            }

    async def q_category_total_voice(self, guild_id: int, category_id: int,
                                     role_filter_ids: Optional[List[int]] = None,
                                     start_time: Optional[datetime] = None,
                                     end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'unique_users': 0, 'avg_session': 0.0
            }

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'unique_users': 0, 'avg_session': 0.0
            }

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count,
                        COUNT(DISTINCT user_id) as unique_users,
                        COALESCE(AVG(duration_seconds), 0) as avg_session
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND category_id = $2
                '''
                params = [guild_id, category_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',

                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                row = await conn.fetchrow(query, *params)

                total_seconds = row['total_seconds'] or 0

                return {
                    'total_seconds': total_seconds,
                    'total_hours': round(total_seconds / 3600, 2),
                    'session_count': row['session_count'] or 0,
                    'unique_users': row['unique_users'] or 0,
                    'avg_session': round(row['avg_session'] or 0, 2)
                }

        except Exception as e:
            logger.error(f"Error in q_category_total_voice: {e}")
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'unique_users': 0, 'avg_session': 0.0
            }

    async def q_category_timeseries_messages(self, guild_id: int, category_id: int,
                                             days: List[int] = [
                                                 1, 5, 10, 20, 30],
                                             role_filter_ids: Optional[List[int]] = None,
                                             start_time: Optional[datetime] = None,
                                             end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {f'{d}d': 0 for d in days}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {f'{d}d': 0 for d in days}

        try:
            async with self.pool.acquire() as conn:
                result = {}

                for d in days:

                    window_end = end_time if end_time else datetime.utcnow()
                    window_start = window_end - timedelta(days=d)

                    query = '''
                        SELECT COUNT(*) as message_count
                        FROM message_tracking
                        WHERE guild_id = $1 
                        AND category_id = $2 
                        AND NOT is_bot
                        AND created_at >= $3
                        AND created_at <= $4
                    '''
                    params = [guild_id, category_id, window_start, window_end]

                    if role_filter_ids:
                        user_ids = []
                        for member in guild.members:
                            if any(role.id in role_filter_ids for role in member.roles):
                                user_ids.append(member.id)

                        if user_ids:
                            placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                             len(params) + len(user_ids) + 1)])
                            query += f" AND user_id IN ({placeholders})"
                            params.extend(user_ids)

                    query, params = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query, params, 'message_tracking',

                        include_users=True, include_channels=False,
                        specific_category_id=category_id
                    )

                    row = await conn.fetchrow(query, *params)
                    result[f'{d}d'] = row['message_count'] or 0

                return result

        except Exception as e:
            logger.error(f"Error in q_category_timeseries_messages: {e}")
            return {f'{d}d': 0 for d in days}

    async def q_category_timeseries_voice(self, guild_id: int, category_id: int,
                                          days: List[int] = [1, 5, 10, 20, 30],
                                          role_filter_ids: Optional[List[int]] = None,
                                          start_time: Optional[datetime] = None,
                                          end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {f'{d}d': 0 for d in days}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {f'{d}d': 0 for d in days}

        try:
            async with self.pool.acquire() as conn:
                result = {}

                for d in days:

                    window_end = end_time if end_time else datetime.utcnow()
                    window_start = window_end - timedelta(days=d)

                    query = '''
                        SELECT COALESCE(SUM(duration_seconds), 0) as voice_seconds
                        FROM voice_session_history
                        WHERE guild_id = $1 
                        AND category_id = $2 
                        AND join_time >= $3
                        AND join_time <= $4
                    '''
                    params = [guild_id, category_id, window_start, window_end]

                    if role_filter_ids:
                        user_ids = []
                        for member in guild.members:
                            if any(role.id in role_filter_ids for role in member.roles):
                                user_ids.append(member.id)

                        if user_ids:
                            placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                             len(params) + len(user_ids) + 1)])
                            query += f" AND user_id IN ({placeholders})"
                            params.extend(user_ids)

                    query, params = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query, params, 'voice_session_history',

                        include_users=True, include_channels=False,
                        specific_category_id=category_id
                    )

                    row = await conn.fetchrow(query, *params)
                    result[f'{d}d'] = row['voice_seconds'] or 0

                return result

        except Exception as e:
            logger.error(f"Error in q_category_timeseries_voice: {e}")
            return {f'{d}d': 0 for d in days}

    async def q_category_top5_voice_channels(self, guild_id: int, category_id: int,
                                             role_filter_ids: Optional[List[int]] = None,
                                             start_time: Optional[datetime] = None,
                                             end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 AND category_id = $2
                '''
                total_params = [guild_id, category_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'voice_session_history',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        channel_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(*) as session_count
                    FROM voice_session_history
                    WHERE guild_id = $1 AND category_id = $2
                '''
                params = [guild_id, category_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                query += '''
                    GROUP BY channel_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT 5
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    channel_seconds = row['total_seconds'] or 0
                    percentage = (channel_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'channel_id': row['channel_id'],
                        'total_seconds': channel_seconds,
                        'total_hours': round(channel_seconds / 3600, 2),
                        'unique_users': row['unique_users'] or 0,
                        'session_count': row['session_count'] or 0,
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_category_top5_voice_channels: {e}")
            return []

    async def q_category_top5_text_channels(self, guild_id: int, category_id: int,
                                            role_filter_ids: Optional[List[int]] = None,
                                            start_time: Optional[datetime] = None,
                                            end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 AND category_id = $2 AND NOT is_bot
                '''
                total_params = [guild_id, category_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'message_tracking',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        channel_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COUNT(DISTINCT user_id) as unique_users,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 AND category_id = $2 AND NOT is_bot
                '''
                params = [guild_id, category_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                query += '''
                    GROUP BY channel_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT 5
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    channel_messages = row['message_count'] or 0
                    percentage = (channel_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'channel_id': row['channel_id'],
                        'message_count': channel_messages,
                        'total_chars': row['total_chars'] or 0,
                        'unique_users': row['unique_users'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_category_top5_text_channels: {e}")
            return []

    async def q_category_top5_users_messages(self, guild_id: int, category_id: int,
                                             role_filter_ids: Optional[List[int]] = None,
                                             start_time: Optional[datetime] = None,
                                             end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 AND category_id = $2 AND NOT is_bot
                '''
                total_params = [guild_id, category_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'emoji_usage',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 AND category_id = $2 AND NOT is_bot
                '''
                params = [guild_id, category_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'emoji_usage',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                query += '''
                    GROUP BY user_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT 5
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_messages = row['message_count'] or 0
                    percentage = (user_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'message_count': user_messages,
                        'total_chars': row['total_chars'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_category_top5_users_messages: {e}")
            return []

    async def q_category_top5_users_voice(self, guild_id: int, category_id: int,
                                          role_filter_ids: Optional[List[int]] = None,
                                          start_time: Optional[datetime] = None,
                                          end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 AND category_id = $2
                '''
                total_params = [guild_id, category_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'emoji_usage',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count,
                        COALESCE(AVG(duration_seconds), 0) as avg_session
                    FROM voice_session_history
                    WHERE guild_id = $1 AND category_id = $2
                '''
                params = [guild_id, category_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'emoji_usage',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                query += '''
                    GROUP BY user_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT 5
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_seconds = row['total_seconds'] or 0
                    percentage = (user_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'total_seconds': user_seconds,
                        'total_hours': round(user_seconds / 3600, 2),
                        'session_count': row['session_count'] or 0,
                        'avg_session': round(row['avg_session'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_category_top5_users_voice: {e}")
            return []

    # CHANNEL STATS (8 functions)

    async def q_channel_messages_per_hour(self, guild_id: int, channel_id: int,
                                          role_filter_ids: Optional[List[int]] = None,
                                          start_time: Optional[datetime] = None,
                                          end_time: Optional[datetime] = None,
                                          timezone_str: str = 'UTC') -> Dict[str, Any]:

        if not self.pool:
            result = {f'hour_{i}': 0 for i in range(24)}
            result.update({'total': 0, 'peak_hour': -1})
            return result

        guild = self.bot.get_guild(guild_id)
        if not guild:
            result = {f'hour_{i}': 0 for i in range(24)}
            result.update({'total': 0, 'peak_hour': -1})
            return result

        try:
            async with self.pool.acquire() as conn:

                query = f'''
                    SELECT 
                        EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                        COUNT(*) as message_count
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND channel_id = $2 
                    AND NOT is_bot
                '''
                params = [guild_id, channel_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                if query is None:
                    result = {f'hour_{i}': 0 for i in range(24)}
                    result.update({'total': 0, 'peak_hour': -1})
                    return result

                query += '''
                    GROUP BY hour
                    ORDER BY hour
                '''

                rows = await conn.fetch(query, *params)

                result = {f'hour_{i}': 0 for i in range(24)}
                total = 0
                peak_hour = -1
                peak_count = 0

                for row in rows:
                    hour = int(row['hour'])
                    count = row['message_count'] or 0
                    result[f'hour_{hour}'] = count
                    total += count

                    if count > peak_count:
                        peak_count = count
                        peak_hour = hour

                result['total'] = total
                result['peak_hour'] = peak_hour

                return result

        except Exception as e:
            logger.error(f"Error in q_channel_messages_per_hour: {e}")
            result = {f'hour_{i}': 0 for i in range(24)}
            result.update({'total': 0, 'peak_hour': -1})
            return result

    async def q_channel_voice_per_hour(self, guild_id: int, channel_id: int,
                                       role_filter_ids: Optional[List[int]] = None,
                                       start_time: Optional[datetime] = None,
                                       end_time: Optional[datetime] = None,
                                       timezone_str: str = 'UTC') -> Dict[str, Any]:

        if not self.pool:
            return {f'hour_{i}': 0 for i in range(24)} | {'total_seconds': 0, 'peak_hour': -1}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {f'hour_{i}': 0 for i in range(24)} | {'total_seconds': 0, 'peak_hour': -1}

        try:
            async with self.pool.acquire() as conn:

                query = f'''
                    SELECT 
                        EXTRACT(HOUR FROM join_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND channel_id = $2
                '''
                params = [guild_id, channel_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                if query is None:
                    return {f'hour_{i}': 0 for i in range(24)} | {'total_seconds': 0, 'peak_hour': -1}

                query += '''
                    GROUP BY hour
                    ORDER BY hour
                '''

                rows = await conn.fetch(query, *params)

                result = {f'hour_{i}': 0 for i in range(24)}
                total_seconds = 0
                peak_hour = -1
                peak_seconds = 0

                for row in rows:
                    hour = int(row['hour'])
                    seconds = row['total_seconds'] or 0
                    result[f'hour_{hour}'] = seconds
                    total_seconds += seconds

                    if seconds > peak_seconds:
                        peak_seconds = seconds
                        peak_hour = hour

                result['total_seconds'] = total_seconds
                result['peak_hour'] = peak_hour

                return result

        except Exception as e:
            logger.error(f"Error in q_channel_voice_per_hour: {e}")
            return {f'hour_{i}': 0 for i in range(24)} | {'total_seconds': 0, 'peak_hour': -1}

    async def q_channel_top5_users_messages(self, guild_id: int, channel_id: int,
                                            role_filter_ids: Optional[List[int]] = None,
                                            start_time: Optional[datetime] = None,
                                            end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND channel_id = $2 
                    AND NOT is_bot
                '''
                total_params = [guild_id, channel_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'message_tracking',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                if total_query is None:
                    return []

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND channel_id = $2 
                    AND NOT is_bot
                '''
                params = [guild_id, channel_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                if query is None:
                    return []

                query += '''
                    GROUP BY user_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT 5
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_messages = row['message_count'] or 0
                    percentage = (user_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'message_count': user_messages,
                        'total_chars': row['total_chars'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_channel_top5_users_messages: {e}")
            return []

    async def q_channel_top5_users_voice(self, guild_id: int, channel_id: int,
                                         role_filter_ids: Optional[List[int]] = None,
                                         start_time: Optional[datetime] = None,
                                         end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND channel_id = $2
                '''
                total_params = [guild_id, channel_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'voice_session_history',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                if total_query is None:
                    return []

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count,
                        COALESCE(AVG(duration_seconds), 0) as avg_session
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND channel_id = $2
                '''
                params = [guild_id, channel_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                if query is None:
                    return []

                query += '''
                    GROUP BY user_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT 5
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_seconds = row['total_seconds'] or 0
                    percentage = (user_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'total_seconds': user_seconds,
                        'total_hours': round(user_seconds / 3600, 2),
                        'session_count': row['session_count'] or 0,
                        'avg_session': round(row['avg_session'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_channel_top5_users_voice: {e}")
            return []

    async def q_channel_timeseries_messages_1d_5d_10d_20d_30d(self, guild_id: int, channel_id: int,
                                                              role_filter_ids: Optional[List[int]] = None,
                                                              start_time: Optional[datetime] = None,
                                                              end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        try:
            async with self.pool.acquire() as conn:
                periods = [1, 5, 10, 20, 30]
                result = {}
                end_time_actual = end_time if end_time else datetime.utcnow()

                for days in periods:

                    window_start = end_time_actual - timedelta(days=days)
                    window_end = end_time_actual

                    if start_time and window_start < start_time:
                        window_start = start_time

                    query = '''
                        SELECT COUNT(*) as message_count
                        FROM message_tracking
                        WHERE guild_id = $1 
                        AND channel_id = $2 
                        AND NOT is_bot
                        AND created_at >= $3
                        AND created_at <= $4
                    '''
                    params = [guild_id, channel_id, window_start, window_end]

                    if role_filter_ids:
                        user_ids = []
                        for member in guild.members:
                            if any(role.id in role_filter_ids for role in member.roles):
                                user_ids.append(member.id)

                        if user_ids:
                            placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                             len(params) + len(user_ids) + 1)])
                            query += f" AND user_id IN ({placeholders})"
                            params.extend(user_ids)

                    query, params = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query, params, 'message_tracking',
                        include_users=True, include_channels=False,
                        specific_channel_id=channel_id
                    )

                    if query is None:
                        result[f'{days}d'] = 0
                        continue

                    row = await conn.fetchrow(query, *params)
                    result[f'{days}d'] = row['message_count'] or 0

                return result

        except Exception as e:
            logger.error(f"Error in q_channel_timeseries_messages: {e}")
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

    async def q_channel_timeseries_voice_1d_5d_10d_20d_30d(self, guild_id: int, channel_id: int,
                                                           role_filter_ids: Optional[List[int]] = None,
                                                           start_time: Optional[datetime] = None,
                                                           end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

        try:
            async with self.pool.acquire() as conn:
                periods = [1, 5, 10, 20, 30]
                result = {}
                end_time_actual = end_time if end_time else datetime.utcnow()

                for days in periods:

                    window_start = end_time_actual - timedelta(days=days)
                    window_end = end_time_actual

                    if start_time and window_start < start_time:
                        window_start = start_time

                    query = '''
                        SELECT COALESCE(SUM(duration_seconds), 0) as voice_seconds
                        FROM voice_session_history
                        WHERE guild_id = $1 
                        AND channel_id = $2 
                        AND join_time >= $3
                        AND join_time <= $4
                    '''
                    params = [guild_id, channel_id, window_start, window_end]

                    if role_filter_ids:
                        user_ids = []
                        for member in guild.members:
                            if any(role.id in role_filter_ids for role in member.roles):
                                user_ids.append(member.id)

                        if user_ids:
                            placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                             len(params) + len(user_ids) + 1)])
                            query += f" AND user_id IN ({placeholders})"
                            params.extend(user_ids)

                    query, params = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query, params, 'voice_session_history',
                        include_users=True, include_channels=False,
                        specific_channel_id=channel_id
                    )

                    if query is None:
                        result[f'{days}d'] = 0
                        continue

                    row = await conn.fetchrow(query, *params)
                    result[f'{days}d'] = row['voice_seconds'] or 0

                return result

        except Exception as e:
            logger.error(f"Error in q_channel_timeseries_voice: {e}")
            return {'1d': 0, '5d': 0, '10d': 0, '20d': 0, '30d': 0}

    async def q_channel_total_messages(self, guild_id: int, channel_id: int,
                                       role_filter_ids: Optional[List[int]] = None,
                                       start_time: Optional[datetime] = None,
                                       end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'unique_users': 0, 'messages_per_user': 0.0,
                'has_attachments': 0, 'has_embeds': 0
            }

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'unique_users': 0, 'messages_per_user': 0.0,
                'has_attachments': 0, 'has_embeds': 0
            }

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        COUNT(*) as total_messages,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars,
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(*) FILTER (WHERE has_attachment = TRUE) as has_attachments,
                        COUNT(*) FILTER (WHERE has_embed = TRUE) as has_embeds
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND channel_id = $2 
                    AND NOT is_bot
                '''
                params = [guild_id, channel_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                if query is None:
                    return {
                        'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                        'unique_users': 0, 'messages_per_user': 0.0,
                        'has_attachments': 0, 'has_embeds': 0
                    }

                row = await conn.fetchrow(query, *params)

                total_messages = row['total_messages'] or 0
                unique_users = row['unique_users'] or 0
                messages_per_user = total_messages / max(unique_users, 1)

                return {
                    'total_messages': total_messages,
                    'total_chars': row['total_chars'] or 0,
                    'avg_chars': round(row['avg_chars'] or 0, 2),
                    'unique_users': unique_users,
                    'messages_per_user': round(messages_per_user, 2),
                    'has_attachments': row['has_attachments'] or 0,
                    'has_embeds': row['has_embeds'] or 0
                }

        except Exception as e:
            logger.error(f"Error in q_channel_total_messages: {e}")
            return {
                'total_messages': 0, 'total_chars': 0, 'avg_chars': 0.0,
                'unique_users': 0, 'messages_per_user': 0.0,
                'has_attachments': 0, 'has_embeds': 0
            }

    async def q_channel_total_voice(self, guild_id: int, channel_id: int,
                                    role_filter_ids: Optional[List[int]] = None,
                                    start_time: Optional[datetime] = None,
                                    end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'unique_users': 0, 'avg_session': 0.0, 'seconds_per_user': 0.0
            }

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'unique_users': 0, 'avg_session': 0.0, 'seconds_per_user': 0.0
            }

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count,
                        COUNT(DISTINCT user_id) as unique_users,
                        COALESCE(AVG(duration_seconds), 0) as avg_session
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND channel_id = $2
                '''
                params = [guild_id, channel_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                if query is None:
                    return {
                        'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                        'unique_users': 0, 'avg_session': 0.0, 'seconds_per_user': 0.0
                    }

                row = await conn.fetchrow(query, *params)

                total_seconds = row['total_seconds'] or 0
                unique_users = row['unique_users'] or 0
                seconds_per_user = total_seconds / max(unique_users, 1)

                return {
                    'total_seconds': total_seconds,
                    'total_hours': round(total_seconds / 3600, 2),
                    'session_count': row['session_count'] or 0,
                    'unique_users': unique_users,
                    'avg_session': round(row['avg_session'] or 0, 2),
                    'seconds_per_user': round(seconds_per_user, 2)
                }

        except Exception as e:
            logger.error(f"Error in q_channel_total_voice: {e}")
            return {
                'total_seconds': 0, 'total_hours': 0.0, 'session_count': 0,
                'unique_users': 0, 'avg_session': 0.0, 'seconds_per_user': 0.0
            }

    # LEADERBOARD SERVER (4 functions)

    async def q_leaderboard_server_top_text_channels(self, guild_id: int,
                                                     limit: int = 10,
                                                     role_filter_ids: Optional[List[int]] = None,
                                                     start_time: Optional[datetime] = None,
                                                     end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 AND NOT is_bot
                '''
                total_params = [guild_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        channel_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COUNT(DISTINCT user_id) as unique_users,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 AND NOT is_bot
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                query += f'''
                    GROUP BY channel_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    channel_messages = row['message_count'] or 0
                    percentage = (channel_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'channel_id': row['channel_id'],
                        'message_count': channel_messages,
                        'total_chars': row['total_chars'] or 0,
                        'unique_users': row['unique_users'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(
                f"Error in q_leaderboard_server_top_text_channels: {e}")
            return []

    async def q_leaderboard_server_top_voice_channels(self, guild_id: int,
                                                      limit: int = 10,
                                                      role_filter_ids: Optional[List[int]] = None,
                                                      start_time: Optional[datetime] = None,
                                                      end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1
                '''
                total_params = [guild_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        channel_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(*) as session_count
                    FROM voice_session_history
                    WHERE guild_id = $1
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                query += f'''
                    GROUP BY channel_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    channel_seconds = row['total_seconds'] or 0
                    percentage = (channel_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'channel_id': row['channel_id'],
                        'total_seconds': channel_seconds,
                        'total_hours': round(channel_seconds / 3600, 2),
                        'unique_users': row['unique_users'] or 0,
                        'session_count': row['session_count'] or 0,
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(
                f"Error in q_leaderboard_server_top_voice_channels: {e}")
            return []

    async def q_leaderboard_server_top_categories_messages(self, guild_id: int,
                                                           limit: int = 10,
                                                           role_filter_ids: Optional[List[int]] = None,
                                                           start_time: Optional[datetime] = None,
                                                           end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 AND NOT is_bot
                '''
                total_params = [guild_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        category_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COUNT(DISTINCT user_id) as unique_users,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND NOT is_bot
                    AND category_id IS NOT NULL
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=True
                )

                query += f'''
                    GROUP BY category_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    category_messages = row['message_count'] or 0
                    percentage = (category_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'category_id': row['category_id'],
                        'message_count': category_messages,
                        'total_chars': row['total_chars'] or 0,
                        'unique_users': row['unique_users'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(
                f"Error in q_leaderboard_server_top_categories_messages: {e}")
            return []

    async def q_leaderboard_server_top_categories_voice(self, guild_id: int,
                                                        limit: int = 10,
                                                        role_filter_ids: Optional[List[int]] = None,
                                                        start_time: Optional[datetime] = None,
                                                        end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1
                '''
                total_params = [guild_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        category_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(DISTINCT user_id) as unique_users,
                        COUNT(*) as session_count
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND category_id IS NOT NULL
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                query += f'''
                    GROUP BY category_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    category_seconds = row['total_seconds'] or 0
                    percentage = (category_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'category_id': row['category_id'],
                        'total_seconds': category_seconds,
                        'total_hours': round(category_seconds / 3600, 2),
                        'unique_users': row['unique_users'] or 0,
                        'session_count': row['session_count'] or 0,
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(
                f"Error in q_leaderboard_server_top_categories_voice: {e}")
            return []

    # LEADERBOARD CHANNEL (2 functions)

    async def q_leaderboard_text_users_in_channel(self, guild_id: int, channel_id: int,
                                                  limit: int = 10,
                                                  role_filter_ids: Optional[List[int]] = None,
                                                  start_time: Optional[datetime] = None,
                                                  end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND channel_id = $2 
                    AND NOT is_bot
                '''
                total_params = [guild_id, channel_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'message_tracking',
                    include_users=True, include_channels=False
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND channel_id = $2 
                    AND NOT is_bot
                '''
                params = [guild_id, channel_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=False
                )

                query += f'''
                    GROUP BY user_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_messages = row['message_count'] or 0
                    percentage = (user_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'message_count': user_messages,
                        'total_chars': row['total_chars'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(
                f"Error in q_leaderboard_text_users_in_channel: {e}")
            return []

    async def q_leaderboard_voice_users_in_channel(self, guild_id: int, channel_id: int,
                                                   limit: int = 10,
                                                   role_filter_ids: Optional[List[int]] = None,
                                                   start_time: Optional[datetime] = None,
                                                   end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND channel_id = $2
                '''
                total_params = [guild_id, channel_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'voice_session_history',
                    include_users=True, include_channels=False
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count,
                        COALESCE(AVG(duration_seconds), 0) as avg_session
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND channel_id = $2
                '''
                params = [guild_id, channel_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=False
                )

                query += f'''
                    GROUP BY user_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_seconds = row['total_seconds'] or 0
                    percentage = (user_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'total_seconds': user_seconds,
                        'total_hours': round(user_seconds / 3600, 2),
                        'session_count': row['session_count'] or 0,
                        'avg_session': round(row['avg_session'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(
                f"Error in q_leaderboard_voice_users_in_channel: {e}")
            return []

    # LEADERBOARD MENTIONS (1 function)

    async def q_leaderboard_mentions_target_user(self, guild_id: int, mentioned_user_id: int,
                                                 limit: int = 10,
                                                 role_filter_ids: Optional[List[int]] = None,
                                                 start_time: Optional[datetime] = None,
                                                 end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            print("❌ No database pool for mentions leaderboard")
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            print("❌ No guild found for mentions leaderboard")
            return []

        try:
            async with self.pool.acquire() as conn:

                total_count = await conn.fetchval('''
                    SELECT COUNT(*) as total_mentions
                    FROM user_mentions
                    WHERE guild_id = $1 
                    AND mentioned_user_id = $2
                ''', guild_id, mentioned_user_id)

                total_query = '''
                    SELECT COUNT(*) as total_mentions
                    FROM user_mentions
                    WHERE guild_id = $1 
                    AND mentioned_user_id = $2
                '''
                total_params = [guild_id, mentioned_user_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND mentioner_user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'user_mentions',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_mentions = total_row['total_mentions'] or 0

                if total_mentions == 0:

                    return []

                query = '''
                    SELECT 
                        mentioner_user_id,
                        COUNT(*) as mention_count,
                        MAX(created_at) as last_mention
                    FROM user_mentions
                    WHERE guild_id = $1 
                    AND mentioned_user_id = $2
                '''
                params = [guild_id, mentioned_user_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND mentioner_user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'user_mentions',
                    include_users=True, include_channels=True
                )

                query += f'''
                    GROUP BY mentioner_user_id
                    HAVING COUNT(*) > 0
                    ORDER BY mention_count DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_mentions = row['mention_count'] or 0
                    percentage = (user_mentions / total_mentions *
                                  100) if total_mentions > 0 else 0

                    result.append({
                        'user_id': row['mentioner_user_id'],
                        'mention_count': user_mentions,
                        'last_mention': row['last_mention'].isoformat() if row['last_mention'] else None,
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            print(f"❌ Error in q_leaderboard_mentions_target_user: {e}")
            traceback.print_exc()
            return []

    # LEADERBOARD CATEGORY (2 functions)

    async def q_leaderboard_category_users_messages(self, guild_id: int, category_id: int,
                                                    limit: int = 10,
                                                    role_filter_ids: Optional[List[int]] = None,
                                                    start_time: Optional[datetime] = None,
                                                    end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COUNT(*) as total_messages
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND category_id = $2 
                    AND NOT is_bot
                '''
                total_params = [guild_id, category_id]

                if start_time:
                    total_query += f" AND created_at >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND created_at <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'message_tracking',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_messages = total_row['total_messages'] or 0

                if total_messages == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COUNT(*) as message_count,
                        COALESCE(SUM(message_length), 0) as total_chars,
                        COALESCE(AVG(message_length), 0) as avg_chars
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND category_id = $2 
                    AND NOT is_bot
                '''
                params = [guild_id, category_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'message_tracking',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                query += f'''
                    GROUP BY user_id
                    HAVING COUNT(*) > 0
                    ORDER BY message_count DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_messages = row['message_count'] or 0
                    percentage = (user_messages / total_messages *
                                  100) if total_messages > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'message_count': user_messages,
                        'total_chars': row['total_chars'] or 0,
                        'avg_chars': round(row['avg_chars'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(
                f"Error in q_leaderboard_category_users_messages: {e}")
            return []

    async def q_leaderboard_category_users_voice(self, guild_id: int, category_id: int,
                                                 limit: int = 10,
                                                 role_filter_ids: Optional[List[int]] = None,
                                                 start_time: Optional[datetime] = None,
                                                 end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND category_id = $2
                '''
                total_params = [guild_id, category_id]

                if start_time:
                    total_query += f" AND join_time >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND join_time <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'voice_session_history',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_seconds = total_row['total_seconds'] or 0

                if total_seconds == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds,
                        COUNT(*) as session_count,
                        COALESCE(AVG(duration_seconds), 0) as avg_session
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND category_id = $2
                '''
                params = [guild_id, category_id]

                if start_time:
                    query += f" AND join_time >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND join_time <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'voice_session_history',
                    include_users=True, include_channels=False,
                    specific_category_id=category_id
                )

                query += f'''
                    GROUP BY user_id
                    HAVING COALESCE(SUM(duration_seconds), 0) > 0
                    ORDER BY total_seconds DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_seconds = row['total_seconds'] or 0
                    percentage = (user_seconds / total_seconds *
                                  100) if total_seconds > 0 else 0

                    result.append({
                        'user_id': row['user_id'],
                        'total_seconds': user_seconds,
                        'total_hours': round(user_seconds / 3600, 2),
                        'session_count': row['session_count'] or 0,
                        'avg_session': round(row['avg_session'] or 0, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(
                f"Error in q_leaderboard_category_users_voice: {e}")
            return []

    # EMOJI LEADERBOARDS (4 functions)

    async def q_emoji_server_leaderboard(self, guild_id: int,
                                         limit: int = 10,
                                         role_filter_ids: Optional[List[int]] = None,
                                         start_time: Optional[datetime] = None,
                                         end_time: Optional[datetime] = None,
                                         usage_type: Optional[str] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(usage_count), 0) as total_usage
                    FROM emoji_usage
                    WHERE guild_id = $1
                '''
                total_params = [guild_id]

                if usage_type:
                    total_query += f" AND usage_type = ${len(total_params) + 1}"
                    total_params.append(usage_type)

                if start_time:
                    total_query += f" AND last_used >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND last_used <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'emoji_usage',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_usage = total_row['total_usage'] or 0

                if total_usage == 0:
                    return []

                query = '''
                    SELECT 
                        emoji_str,
                        is_custom,
                        SUM(usage_count) as total_usage,
                        COUNT(DISTINCT user_id) as unique_users
                    FROM emoji_usage
                    WHERE guild_id = $1
                '''
                params = [guild_id]

                if usage_type:
                    query += f" AND usage_type = ${len(params) + 1}"
                    params.append(usage_type)

                if start_time:
                    query += f" AND last_used >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND last_used <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'emoji_usage',
                    include_users=True, include_channels=True
                )

                query += f'''
                    GROUP BY emoji_str, is_custom
                    HAVING SUM(usage_count) > 0
                    ORDER BY total_usage DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    emoji_usage = row['total_usage'] or 0
                    unique_users = row['unique_users'] or 0
                    percentage = (emoji_usage / total_usage *
                                  100) if total_usage > 0 else 0
                    avg_usage = emoji_usage / max(unique_users, 1)

                    result.append({
                        'emoji_str': row['emoji_str'],
                        'is_custom': row['is_custom'],
                        'usage_count': emoji_usage,
                        'unique_users': unique_users,
                        'avg_usage_per_user': round(avg_usage, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_emoji_server_leaderboard: {e}")
            return []

    async def q_emoji_user_leaderboard(self, guild_id: int,
                                       limit: int = 10,
                                       role_filter_ids: Optional[List[int]] = None,
                                       start_time: Optional[datetime] = None,
                                       end_time: Optional[datetime] = None,
                                       usage_type: Optional[str] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(usage_count), 0) as total_usage
                    FROM emoji_usage
                    WHERE guild_id = $1
                '''
                total_params = [guild_id]

                if usage_type:
                    total_query += f" AND usage_type = ${len(total_params) + 1}"
                    total_params.append(usage_type)

                if start_time:
                    total_query += f" AND last_used >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND last_used <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'emoji_usage',
                    include_users=True, include_channels=True
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_usage = total_row['total_usage'] or 0

                if total_usage == 0:
                    return []

                query = '''
                    SELECT 
                        user_id,
                        SUM(usage_count) as total_usage,
                        COUNT(DISTINCT emoji_str) as unique_emojis
                    FROM emoji_usage
                    WHERE guild_id = $1
                '''
                params = [guild_id]

                if usage_type:
                    query += f" AND usage_type = ${len(params) + 1}"
                    params.append(usage_type)

                if start_time:
                    query += f" AND last_used >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND last_used <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'emoji_usage',
                    include_users=True, include_channels=True
                )

                query += f'''
                    GROUP BY user_id
                    HAVING SUM(usage_count) > 0
                    ORDER BY total_usage DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    user_usage = row['total_usage'] or 0
                    unique_emojis = row['unique_emojis'] or 0
                    percentage = (user_usage / total_usage *
                                  100) if total_usage > 0 else 0
                    avg_usage = user_usage / max(unique_emojis, 1)

                    result.append({
                        'user_id': row['user_id'],
                        'usage_count': user_usage,
                        'unique_emojis': unique_emojis,
                        'avg_usage_per_emoji': round(avg_usage, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_emoji_user_leaderboard: {e}")
            return []

    async def q_emoji_channel_leaderboard(self, guild_id: int, channel_id: int,
                                          limit: int = 10,
                                          role_filter_ids: Optional[List[int]] = None,
                                          start_time: Optional[datetime] = None,
                                          end_time: Optional[datetime] = None,
                                          usage_type: Optional[str] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(usage_count), 0) as total_usage
                    FROM emoji_usage
                    WHERE guild_id = $1 
                    AND channel_id = $2
                '''
                total_params = [guild_id, channel_id]

                if usage_type:
                    total_query += f" AND usage_type = ${len(total_params) + 1}"
                    total_params.append(usage_type)

                if start_time:
                    total_query += f" AND last_used >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND last_used <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'emoji_usage',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_usage = total_row['total_usage'] or 0

                if total_usage == 0:
                    return []

                query = '''
                    SELECT 
                        emoji_str,
                        is_custom,
                        SUM(usage_count) as total_usage,
                        COUNT(DISTINCT user_id) as unique_users
                    FROM emoji_usage
                    WHERE guild_id = $1 
                    AND channel_id = $2
                '''
                params = [guild_id, channel_id]

                if usage_type:
                    query += f" AND usage_type = ${len(params) + 1}"
                    params.append(usage_type)

                if start_time:
                    query += f" AND last_used >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND last_used <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'emoji_usage',

                    include_users=True, include_channels=False,
                    specific_channel_id=channel_id
                )

                query += f'''
                    GROUP BY emoji_str, is_custom
                    HAVING SUM(usage_count) > 0
                    ORDER BY total_usage DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    emoji_usage = row['total_usage'] or 0
                    unique_users = row['unique_users'] or 0
                    percentage = (emoji_usage / total_usage *
                                  100) if total_usage > 0 else 0
                    avg_usage = emoji_usage / max(unique_users, 1)

                    result.append({
                        'emoji_str': row['emoji_str'],
                        'is_custom': row['is_custom'],
                        'usage_count': emoji_usage,
                        'unique_users': unique_users,
                        'avg_usage_per_user': round(avg_usage, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_emoji_channel_leaderboard: {e}")
            return []

    async def q_emoji_category_leaderboard(self, guild_id: int, category_id: int,
                                           limit: int = 10,
                                           role_filter_ids: Optional[List[int]] = None,
                                           start_time: Optional[datetime] = None,
                                           end_time: Optional[datetime] = None,
                                           usage_type: Optional[str] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                total_query = '''
                    SELECT COALESCE(SUM(usage_count), 0) as total_usage
                    FROM emoji_usage
                    WHERE guild_id = $1 
                    AND category_id = $2
                '''
                total_params = [guild_id, category_id]

                if usage_type:
                    total_query += f" AND usage_type = ${len(total_params) + 1}"
                    total_params.append(usage_type)

                if start_time:
                    total_query += f" AND last_used >= ${len(total_params) + 1}"
                    total_params.append(start_time)
                if end_time:
                    total_query += f" AND last_used <= ${len(total_params) + 1}"
                    total_params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(total_params) + 1,
                                                                         len(total_params) + len(user_ids) + 1)])
                        total_query += f" AND user_id IN ({placeholders})"
                        total_params.extend(user_ids)

                total_query, total_params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, total_query, total_params, 'emoji_usage',
                    include_users=True, include_channels=False
                )

                total_row = await conn.fetchrow(total_query, *total_params)
                total_usage = total_row['total_usage'] or 0

                if total_usage == 0:
                    return []

                query = '''
                    SELECT 
                        emoji_str,
                        is_custom,
                        SUM(usage_count) as total_usage,
                        COUNT(DISTINCT user_id) as unique_users
                    FROM emoji_usage
                    WHERE guild_id = $1 
                    AND category_id = $2
                '''
                params = [guild_id, category_id]

                if usage_type:
                    query += f" AND usage_type = ${len(params) + 1}"
                    params.append(usage_type)

                if start_time:
                    query += f" AND last_used >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND last_used <= ${len(params) + 1}"
                    params.append(end_time)

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                         len(params) + len(user_ids) + 1)])
                        query += f" AND user_id IN ({placeholders})"
                        params.extend(user_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'emoji_usage',
                    include_users=True, include_channels=False
                )

                query += f'''
                    GROUP BY emoji_str, is_custom
                    HAVING SUM(usage_count) > 0
                    ORDER BY total_usage DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    emoji_usage = row['total_usage'] or 0
                    unique_users = row['unique_users'] or 0
                    percentage = (emoji_usage / total_usage *
                                  100) if total_usage > 0 else 0
                    avg_usage = emoji_usage / max(unique_users, 1)

                    result.append({
                        'emoji_str': row['emoji_str'],
                        'is_custom': row['is_custom'],
                        'usage_count': emoji_usage,
                        'unique_users': unique_users,
                        'avg_usage_per_user': round(avg_usage, 2),
                        'percentage': round(percentage, 1)
                    })

                return result

        except Exception as e:
            logger.error(f"Error in q_emoji_category_leaderboard: {e}")
            return []

    # INVITE TRACKING (2 functions)

    async def q_user_invite_stats(self, guild_id: int, user_id: int,
                                  start_time: Optional[datetime] = None,
                                  end_time: Optional[datetime] = None) -> Dict[str, Any]:

        if not self.pool:
            return {
                'total_invites': 0, 'valid_invites': 0, 'left_invites': 0,
                'suspicious_invites': 0, 'percentage_valid': 0.0
            }

        try:
            async with self.pool.acquire() as conn:
                query = '''
                    SELECT 
                        COUNT(*) as total_invites,
                        COUNT(*) FILTER (WHERE invite_type = 'valid') as valid_invites,
                        COUNT(*) FILTER (WHERE invite_type = 'left') as left_invites,  -- FIXED
                        COUNT(*) FILTER (WHERE invite_type = 'suspicious') as suspicious_invites
                    FROM invite_tracking
                    WHERE guild_id = $1 
                    AND inviter_id = $2
                '''
                params = [guild_id, user_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                row = await conn.fetchrow(query, *params)

                total_invites = row['total_invites'] or 0
                valid_invites = row['valid_invites'] or 0
                percentage_valid = (valid_invites / total_invites *
                                    100) if total_invites > 0 else 0

                return {
                    'total_invites': total_invites,
                    'valid_invites': valid_invites,
                    'left_invites': row['left_invites'] or 0,
                    'suspicious_invites': row['suspicious_invites'] or 0,
                    'percentage_valid': round(percentage_valid, 1)
                }

        except Exception as e:
            print(f"❌ InviteTracker: Error in q_user_invite_stats: {e}")
            traceback.print_exc()
            return {
                'total_invites': 0, 'valid_invites': 0, 'left_invites': 0,
                'suspicious_invites': 0, 'percentage_valid': 0.0
            }

    async def q_invite_leaderboard(self, guild_id: int,
                                   limit: int = 10,
                                   role_filter_ids: Optional[List[int]] = None,
                                   start_time: Optional[datetime] = None,
                                   end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:

        if not self.pool:
            return []

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return []

        try:
            async with self.pool.acquire() as conn:

                filtered_inviter_ids = None
                if role_filter_ids:
                    filtered_inviter_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            filtered_inviter_ids.append(member.id)

                    if not filtered_inviter_ids:
                        return []

                query = '''
                    SELECT 
                        inviter_id,
                        COUNT(*) as total_invites,
                        COUNT(*) FILTER (WHERE invite_type = 'valid') as valid_invites,
                        COUNT(*) FILTER (WHERE invite_type = 'suspicious') as suspicious_invites,
                        COUNT(*) FILTER (WHERE invite_type = 'left') as left_invites
                    FROM invite_tracking
                    WHERE guild_id = $1
                '''
                params = [guild_id]

                if start_time:
                    query += f" AND created_at >= ${len(params) + 1}"
                    params.append(start_time)
                if end_time:
                    query += f" AND created_at <= ${len(params) + 1}"
                    params.append(end_time)

                if filtered_inviter_ids:
                    placeholders = ', '.join([f'${i}' for i in range(len(params) + 1,
                                                                     len(params) + len(filtered_inviter_ids) + 1)])
                    query += f" AND inviter_id IN ({placeholders})"
                    params.extend(filtered_inviter_ids)

                query, params = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query, params, 'invite_tracking',
                    include_users=True, include_channels=False
                )

                query += f'''
                    GROUP BY inviter_id
                    HAVING COUNT(*) > 0
                    ORDER BY valid_invites DESC, total_invites DESC
                    LIMIT {limit}
                '''

                rows = await conn.fetch(query, *params)

                result = []
                for row in rows:
                    total_invites = row['total_invites'] or 0
                    valid_invites = row['valid_invites'] or 0
                    percentage_valid = (valid_invites / total_invites *
                                        100) if total_invites > 0 else 0

                    result.append({
                        'user_id': row['inviter_id'],
                        'valid_invites': valid_invites,
                        'total_invites': total_invites,
                        'left_invites': row['left_invites'] or 0,
                        'suspicious_invites': row['suspicious_invites'] or 0,
                        'percentage_valid': round(percentage_valid, 1),
                        'rank': len(result) + 1
                    })

                return result

        except Exception as e:
            print(f"❌ InviteTracker: Error in q_invite_leaderboard: {e}")
            traceback.print_exc()
            return []

    # TIMEZONE ACTIVITY DISTRIBUTION (3 functions)

    async def q_server_activity_distribution(self, guild_id: int,
                                             days_back: int = 30,
                                             role_filter_ids: Optional[List[int]] = None,
                                             timezone_str: str = 'UTC') -> Dict[str, Any]:
        if not self.pool:
            return {
                'messages': {f'hour_{i}': 0 for i in range(24)},
                'voice': {f'hour_{i}': 0 for i in range(24)},
                'activities': {f'hour_{i}': 0 for i in range(24)},
                'total_messages': 0,
                'total_voice_seconds': 0,
                'total_activities': 0
            }

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'messages': {f'hour_{i}': 0 for i in range(24)},
                'voice': {f'hour_{i}': 0 for i in range(24)},
                'activities': {f'hour_{i}': 0 for i in range(24)},
                'total_messages': 0,
                'total_voice_seconds': 0,
                'total_activities': 0
            }

        try:
            async with self.pool.acquire() as conn:
                messages = {f'hour_{i}': 0 for i in range(24)}
                voice = {f'hour_{i}': 0 for i in range(24)}
                activities = {f'hour_{i}': 0 for i in range(24)}
                total_messages = 0
                total_voice_seconds = 0
                total_activities = 0

                query_messages = f'''
                    SELECT 
                        EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                        COUNT(*) as message_count
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND NOT is_bot
                    AND created_at >= NOW() - INTERVAL '1 day' * $2
                    GROUP BY EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}')
                    ORDER BY hour
                '''
                params_messages = [guild_id, days_back]

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params_messages) + 1,
                                                                         len(params_messages) + len(user_ids) + 1)])
                        query_messages += f" AND user_id IN ({placeholders})"
                        params_messages.extend(user_ids)

                query_messages, params_messages = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query_messages, params_messages, 'message_tracking',
                    include_users=True, include_channels=True
                )

                rows_messages = await conn.fetch(query_messages, *params_messages)
                for row in rows_messages:
                    hour = int(row['hour'])
                    count = row['message_count'] or 0
                    messages[f'hour_{hour}'] = count
                    total_messages += count

                query_voice = f'''
                    SELECT 
                        EXTRACT(HOUR FROM join_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND join_time >= NOW() - INTERVAL '1 day' * $2
                    GROUP BY EXTRACT(HOUR FROM join_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}')
                    ORDER BY hour
                '''
                params_voice = [guild_id, days_back]

                if role_filter_ids:
                    user_ids = []
                    for member in guild.members:
                        if any(role.id in role_filter_ids for role in member.roles):
                            user_ids.append(member.id)

                    if user_ids:
                        placeholders = ', '.join([f'${i}' for i in range(len(params_voice) + 1,
                                                                         len(params_voice) + len(user_ids) + 1)])
                        query_voice += f" AND user_id IN ({placeholders})"
                        params_voice.extend(user_ids)

                query_voice, params_voice = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query_voice, params_voice, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                rows_voice = await conn.fetch(query_voice, *params_voice)
                for row in rows_voice:
                    hour = int(row['hour'])
                    seconds = row['total_seconds'] or 0
                    voice[f'hour_{hour}'] = seconds
                    total_voice_seconds += seconds

                try:
                    query_activities = f'''
                        SELECT 
                            EXTRACT(HOUR FROM start_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                            COUNT(*) as activity_count
                        FROM activity_sessions
                        WHERE guild_id = $1 
                        AND start_time >= NOW() - INTERVAL '1 day' * $2
                        GROUP BY EXTRACT(HOUR FROM start_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}')
                        ORDER BY hour
                    '''
                    params_activities = [guild_id, days_back]

                    rows_activities = await conn.fetch(query_activities, *params_activities)
                    for row in rows_activities:
                        hour = int(row['hour'])
                        count = row['activity_count'] or 0
                        activities[f'hour_{hour}'] = count
                        total_activities += count
                except Exception as e:
                    logger.debug(f"Could not get activities: {e}")

                return {
                    'messages': messages,
                    'voice': voice,
                    'activities': activities,
                    'total_messages': total_messages,
                    'total_voice_seconds': total_voice_seconds,
                    'total_activities': total_activities
                }

        except Exception as e:
            logger.error(f"Error in q_server_activity_distribution: {e}")
            return {
                'messages': {f'hour_{i}': 0 for i in range(24)},
                'voice': {f'hour_{i}': 0 for i in range(24)},
                'activities': {f'hour_{i}': 0 for i in range(24)},
                'total_messages': 0,
                'total_voice_seconds': 0,
                'total_activities': 0
            }

    async def q_user_activity_distribution(self, guild_id: int, user_id: int,
                                           days_back: int = 30,
                                           role_filter_ids: Optional[List[int]] = None,
                                           timezone_str: str = 'UTC') -> Dict[str, Any]:
        if not self.pool:
            return {
                'messages': {f'hour_{i}': 0 for i in range(24)},
                'voice': {f'hour_{i}': 0 for i in range(24)},
                'activities': {f'hour_{i}': 0 for i in range(24)},
                'total_messages': 0,
                'total_voice_seconds': 0,
                'total_activities': 0
            }

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'messages': {f'hour_{i}': 0 for i in range(24)},
                'voice': {f'hour_{i}': 0 for i in range(24)},
                'activities': {f'hour_{i}': 0 for i in range(24)},
                'total_messages': 0,
                'total_voice_seconds': 0,
                'total_activities': 0
            }

        try:
            async with self.pool.acquire() as conn:
                messages = {f'hour_{i}': 0 for i in range(24)}
                voice = {f'hour_{i}': 0 for i in range(24)}
                activities = {f'hour_{i}': 0 for i in range(24)}
                total_messages = 0
                total_voice_seconds = 0
                total_activities = 0

                query_messages = f'''
                    SELECT 
                        EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                        COUNT(*) as message_count
                    FROM message_tracking
                    WHERE guild_id = $1 
                    AND user_id = $2 
                    AND NOT is_bot
                    AND created_at >= NOW() - INTERVAL '1 day' * $3
                    GROUP BY EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}')
                    ORDER BY hour
                '''
                params_messages = [guild_id, user_id, days_back]

                if role_filter_ids:
                    member = guild.get_member(user_id)
                    if not member:
                        return {
                            'messages': {f'hour_{i}': 0 for i in range(24)},
                            'voice': {f'hour_{i}': 0 for i in range(24)},
                            'activities': {f'hour_{i}': 0 for i in range(24)},
                            'total_messages': 0,
                            'total_voice_seconds': 0,
                            'total_activities': 0
                        }

                    user_roles = [role.id for role in member.roles]
                    if not any(role_id in role_filter_ids for role_id in user_roles):
                        return {
                            'messages': {f'hour_{i}': 0 for i in range(24)},
                            'voice': {f'hour_{i}': 0 for i in range(24)},
                            'activities': {f'hour_{i}': 0 for i in range(24)},
                            'total_messages': 0,
                            'total_voice_seconds': 0,
                            'total_activities': 0
                        }

                query_messages, params_messages = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query_messages, params_messages, 'message_tracking',
                    include_users=True, include_channels=True
                )

                rows_messages = await conn.fetch(query_messages, *params_messages)
                for row in rows_messages:
                    hour = int(row['hour'])
                    count = row['message_count'] or 0
                    messages[f'hour_{hour}'] = count
                    total_messages += count

                query_voice = f'''
                    SELECT 
                        EXTRACT(HOUR FROM join_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                        COALESCE(SUM(duration_seconds), 0) as total_seconds
                    FROM voice_session_history
                    WHERE guild_id = $1 
                    AND user_id = $2 
                    AND join_time >= NOW() - INTERVAL '1 day' * $3
                    GROUP BY EXTRACT(HOUR FROM join_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}')
                    ORDER BY hour
                '''
                params_voice = [guild_id, user_id, days_back]

                query_voice, params_voice = await self._apply_comprehensive_blacklist_filters(
                    guild_id, guild, query_voice, params_voice, 'voice_session_history',
                    include_users=True, include_channels=True
                )

                rows_voice = await conn.fetch(query_voice, *params_voice)
                for row in rows_voice:
                    hour = int(row['hour'])
                    seconds = row['total_seconds'] or 0
                    voice[f'hour_{hour}'] = seconds
                    total_voice_seconds += seconds

                try:
                    query_activities = f'''
                        SELECT 
                            EXTRACT(HOUR FROM start_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                            COUNT(*) as activity_count
                        FROM activity_sessions
                        WHERE guild_id = $1 
                        AND user_id = $2 
                        AND start_time >= NOW() - INTERVAL '1 day' * $3
                        GROUP BY EXTRACT(HOUR FROM start_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}')
                        ORDER BY hour
                    '''
                    params_activities = [guild_id, user_id, days_back]

                    rows_activities = await conn.fetch(query_activities, *params_activities)
                    for row in rows_activities:
                        hour = int(row['hour'])
                        count = row['activity_count'] or 0
                        activities[f'hour_{hour}'] = count
                        total_activities += count
                except Exception as e:
                    logger.debug(f"Could not get user activities: {e}")

                return {
                    'messages': messages,
                    'voice': voice,
                    'activities': activities,
                    'total_messages': total_messages,
                    'total_voice_seconds': total_voice_seconds,
                    'total_activities': total_activities
                }

        except Exception as e:
            logger.error(f"Error in q_user_activity_distribution: {e}")
            return {
                'messages': {f'hour_{i}': 0 for i in range(24)},
                'voice': {f'hour_{i}': 0 for i in range(24)},
                'activities': {f'hour_{i}': 0 for i in range(24)},
                'total_messages': 0,
                'total_voice_seconds': 0,
                'total_activities': 0
            }

    async def q_channel_activity_distribution(self, guild_id: int, channel_id: int,
                                              days_back: int = 30,
                                              role_filter_ids: Optional[List[int]] = None,
                                              timezone_str: str = 'UTC') -> Dict[str, Any]:

        if not self.pool:
            return {
                'messages': {f'hour_{i}': 0 for i in range(24)},
                'voice': {f'hour_{i}': 0 for i in range(24)},
                'total_messages': 0,
                'total_voice_seconds': 0
            }

        guild = self.bot.get_guild(guild_id)
        if not guild:
            return {
                'messages': {f'hour_{i}': 0 for i in range(24)},
                'voice': {f'hour_{i}': 0 for i in range(24)},
                'total_messages': 0,
                'total_voice_seconds': 0
            }

        try:
            async with self.pool.acquire() as conn:

                messages = {f'hour_{i}': 0 for i in range(24)}
                voice = {f'hour_{i}': 0 for i in range(24)}
                total_messages = 0
                total_voice_seconds = 0

                channel = guild.get_channel(channel_id)
                if not channel:
                    return {
                        'messages': {f'hour_{i}': 0 for i in range(24)},
                        'voice': {f'hour_{i}': 0 for i in range(24)},
                        'total_messages': 0,
                        'total_voice_seconds': 0
                    }

                if isinstance(channel, discord.TextChannel):

                    query_messages = f'''
                        SELECT 
                            EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                            COUNT(*) as message_count
                        FROM message_tracking
                        WHERE guild_id = $1 
                        AND channel_id = $2 
                        AND NOT is_bot
                        AND created_at >= NOW() - INTERVAL '1 day' * $3
                    '''
                    params_messages = [guild_id, channel_id, days_back]

                    if role_filter_ids:
                        user_ids = []
                        for member in guild.members:
                            if any(role.id in role_filter_ids for role in member.roles):
                                user_ids.append(member.id)

                        if user_ids:
                            placeholders = ', '.join([f'${i}' for i in range(len(params_messages) + 1,
                                                                             len(params_messages) + len(user_ids) + 1)])
                            query_messages += f" AND user_id IN ({placeholders})"
                            params_messages.extend(user_ids)

                    query_messages, params_messages = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query_messages, params_messages, 'message_tracking',
                        include_users=True, include_channels=False
                    )

                    query_messages += " GROUP BY hour ORDER BY hour"

                    rows_messages = await conn.fetch(query_messages, *params_messages)
                    for row in rows_messages:
                        hour = int(row['hour'])
                        count = row['message_count'] or 0
                        messages[f'hour_{hour}'] = count
                        total_messages += count

                elif isinstance(channel, discord.VoiceChannel):

                    query_voice = f'''
                        SELECT 
                            EXTRACT(HOUR FROM join_time AT TIME ZONE 'UTC' AT TIME ZONE '{timezone_str}') as hour,
                            COALESCE(SUM(duration_seconds), 0) as total_seconds
                        FROM voice_session_history
                        WHERE guild_id = $1 
                        AND channel_id = $2 
                        AND join_time >= NOW() - INTERVAL '1 day' * $3
                    '''
                    params_voice = [guild_id, channel_id, days_back]

                    if role_filter_ids:
                        user_ids = []
                        for member in guild.members:
                            if any(role.id in role_filter_ids for role in member.roles):
                                user_ids.append(member.id)

                        if user_ids:
                            placeholders = ', '.join([f'${i}' for i in range(len(params_voice) + 1,
                                                                             len(params_voice) + len(user_ids) + 1)])
                            query_voice += f" AND user_id IN ({placeholders})"
                            params_voice.extend(user_ids)

                    query_voice, params_voice = await self._apply_comprehensive_blacklist_filters(
                        guild_id, guild, query_voice, params_voice, 'voice_session_history',
                        include_users=True, include_channels=False
                    )

                    query_voice += " GROUP BY hour ORDER BY hour"

                    rows_voice = await conn.fetch(query_voice, *params_voice)
                    for row in rows_voice:
                        hour = int(row['hour'])
                        seconds = row['total_seconds'] or 0
                        voice[f'hour_{hour}'] = seconds
                        total_voice_seconds += seconds

                return {
                    'messages': messages,
                    'voice': voice,
                    'total_messages': total_messages,
                    'total_voice_seconds': total_voice_seconds
                }

        except Exception as e:
            logger.error(f"Error in q_channel_activity_distribution: {e}")
            return {
                'messages': {f'hour_{i}': 0 for i in range(24)},
                'voice': {f'hour_{i}': 0 for i in range(24)},
                'total_messages': 0,
                'total_voice_seconds': 0
            }

    # VOICE ACTIVITY TRACKING

    @tasks.loop(seconds=60)
    async def voice_activity_tracker(self):

        if not self.pool or not self.active_voice_sessions:
            return

        try:
            current_time = datetime.utcnow()

            async with self.voice_lock:
                sessions_copy = copy.deepcopy(self.active_voice_sessions)

            for user_id, session_data in sessions_copy.items():
                try:
                    guild_id = session_data['guild_id']
                    channel_id = session_data['channel_id']

                    guild = self.bot.get_guild(guild_id)
                    if not guild:
                        continue

                    member = guild.get_member(user_id)
                    if not member or not member.voice or not member.voice.channel:
                        continue

                    if member.voice.channel.id != channel_id:
                        continue

                    session_start = session_data['join_time']
                    session_duration = int(
                        (current_time - session_start).total_seconds())

                    if session_duration < 30:
                        continue

                    session_data.update({
                        'server_mute': member.voice.mute,
                        'server_deaf': member.voice.deaf,
                        'self_mute': member.voice.self_mute,
                        'self_deaf': member.voice.self_deaf,
                        'streaming': member.voice.self_stream,
                        'video': member.voice.self_video,
                        'suppressed': member.voice.suppress
                    })

                    state_flags = self._calculate_state_flags(session_data)

                    await self.redis_batch_write('voice_time', {
                        'params': [
                            guild_id, user_id, channel_id,
                            session_data.get('category_id'),
                            self.encrypt_username(
                                str(member)) if member else None,
                            state_flags,
                            'active',
                            session_duration,
                            current_time
                        ]
                    })

                    async with self.voice_lock:
                        if user_id in self.active_voice_sessions:
                            self.active_voice_sessions[user_id]['last_activity'] = current_time

                    async with self.metrics_lock:
                        self.metrics['voice_updates'] += 1

                except Exception as e:
                    logger.debug(
                        f"Error in voice tracker for user {user_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error in voice activity tracker: {e}")

    # CLEANUP TASKS

    @tasks.loop(minutes=5)
    async def connection_pool_health_check(self):

        if not self.pool or not self.db_connected:
            return

        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval('SELECT 1')

            if self.redis and self.redis_connected:
                await self.redis.ping()

        except Exception as e:
            logger.warning(f"Connection pool health check failed: {e}")
            self.db_connected = False
            self.redis_connected = False

    @tasks.loop(hours=24)
    async def metrics_reset_task(self):

        async with self.metrics_lock:
            self.metrics.update({
                'redis_writes': 0,
                'redis_batch_flushes': 0,
                'voice_updates': 0,
                'message_inserts': 0,
                'emoji_inserts': 0,
                'invite_inserts': 0,
                'mention_inserts': 0,
                'errors': 0,
                'db_queries': 0,
                'connection_retries': 0,
                'failed_operations': 0
            })

    # EVENT LISTENERS

    # VOICE TRACKING

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):

        if member.bot or not member.guild or self.shutting_down:
            return

        try:
            guild = member.guild
            guild_id = guild.id
            user_id = member.id
            current_time = datetime.utcnow()

            if before.channel is None and after.channel is not None:
                is_afk_channel = getattr(after.channel, 'afk_channel', False)

                async with self.voice_lock:
                    self.active_voice_sessions[user_id] = {
                        'guild_id': guild_id,
                        'user_id': user_id,
                        'channel_id': after.channel.id,
                        'server_mute': after.mute,
                        'server_deaf': after.deaf,
                        'self_mute': after.self_mute,
                        'self_deaf': after.self_deaf,
                        'streaming': after.self_stream,
                        'video': after.self_video,
                        'suppressed': after.suppress,
                        'is_afk_channel': is_afk_channel,
                        'join_time': current_time,
                        'last_activity': current_time,
                        'last_minute_tracked': current_time
                    }

            elif before.channel is not None and after.channel is None:
                async with self.voice_lock:
                    session_data = self.active_voice_sessions.pop(
                        user_id, None)

                if session_data:
                    duration = int(
                        (current_time - session_data['join_time']).total_seconds())

                    if duration > 0:
                        state_flags = self._calculate_state_flags(session_data)

                        category_id = None
                        if before.channel and hasattr(before.channel, 'category_id') and before.channel.category_id:
                            category_id = before.channel.category_id

                        encrypted_username = self.encrypt_username(
                            str(member)) if member else None

                        await self.redis_batch_write('voice_sessions', {
                            'params': [
                                guild_id, user_id, before.channel.id, category_id,
                                encrypted_username,
                                session_data['join_time'], current_time, duration,
                                state_flags, False, False, False, False
                            ]
                        })

            elif before.channel is not None and after.channel is not None and before.channel.id != after.channel.id:
                async with self.voice_lock:
                    old_session = self.active_voice_sessions.pop(user_id, None)

                if old_session:
                    duration = int(
                        (current_time - old_session['join_time']).total_seconds())

                    if duration > 0:
                        state_flags = self._calculate_state_flags(old_session)

                        category_id = None
                        if before.channel and hasattr(before.channel, 'category_id') and before.channel.category_id:
                            category_id = before.channel.category_id

                        encrypted_username = self.encrypt_username(
                            str(member)) if member else None

                        await self.redis_batch_write('voice_sessions', {
                            'params': [
                                guild_id, user_id, before.channel.id, category_id,
                                encrypted_username,
                                old_session['join_time'], current_time, duration,
                                state_flags, False, False, False, False
                            ]
                        })

                is_afk_channel = getattr(after.channel, 'afk_channel', False)

                async with self.voice_lock:
                    self.active_voice_sessions[user_id] = {
                        'guild_id': guild_id,
                        'user_id': user_id,
                        'channel_id': after.channel.id,
                        'server_mute': after.mute,
                        'server_deaf': after.deaf,
                        'self_mute': after.self_mute,
                        'self_deaf': after.self_deaf,
                        'streaming': after.self_stream,
                        'video': after.self_video,
                        'suppressed': after.suppress,
                        'is_afk_channel': is_afk_channel,
                        'join_time': current_time,
                        'last_activity': current_time,
                        'last_minute_tracked': current_time
                    }

            elif before.channel is not None and after.channel is not None and before.channel.id == after.channel.id:
                async with self.voice_lock:
                    if user_id in self.active_voice_sessions:
                        session = self.active_voice_sessions[user_id]

                        state_changed = (
                            session['server_mute'] != after.mute or
                            session['server_deaf'] != after.deaf or
                            session['self_mute'] != after.self_mute or
                            session['self_deaf'] != after.self_deaf or
                            session['streaming'] != after.self_stream or
                            session['video'] != after.self_video or
                            session['suppressed'] != after.suppress
                        )

                        if state_changed:

                            session.update({
                                'server_mute': after.mute,
                                'server_deaf': after.deaf,
                                'self_mute': after.self_mute,
                                'self_deaf': after.self_deaf,
                                'streaming': after.self_stream,
                                'video': after.self_video,
                                'suppressed': after.suppress,
                                'last_activity': current_time
                            })

        except Exception as e:
            logger.error(f"Error in voice state update: {e}")
            async with self.metrics_lock:
                self.metrics['errors'] += 1

    # MESSAGE TRACKING

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):

        if message.author.bot or not message.guild or self.shutting_down:
            return

        if not await self.check_rate_limit(message.author.id, 'message', limit=30, window=10):
            logger.debug(f"Rate limited message from {message.author.id}")
            return

        try:

            message_key = f"msg_{message.id}"
            current_time = datetime.utcnow()

            if message_key in self.processed_messages:
                time_since = (
                    current_time - self.processed_messages[message_key]).total_seconds()
                if time_since < 0.1:
                    logger.warning(
                        f"⚠️ Duplicate message detected within {time_since*1000:.0f}ms: {message.id}")
                    return

            self.processed_messages[message_key] = current_time

            cleanup_time = current_time - timedelta(hours=1)
            old_keys = [k for k, v in self.processed_messages.items()
                        if v < cleanup_time]
            for key in old_keys:
                del self.processed_messages[key]

            await self._track_message_emojis(message)

            await self._track_message_content(message)

        except Exception as e:
            logger.error(f"Error tracking message emojis: {e}")
            async with self.metrics_lock:
                self.metrics['errors'] += 1

    # EMOJI AND MENTION TRACKING

    def _extract_all_emojis_from_content(self, content: str) -> List[Dict[str, Any]]:

        if not content:
            return []

        all_emojis = []

        custom_pattern = r'<a?:\w{2,32}:\d{18,22}>'

        for match in re.finditer(custom_pattern, content):
            emoji_str = match.group()
            all_emojis.append({
                'emoji_str': emoji_str,
                'is_custom': True,
                'is_animated': emoji_str.startswith('<a:'),
                'start_pos': match.start(),
                'end_pos': match.end()
            })

        custom_positions = set()
        for emoji in all_emojis:
            for pos in range(emoji['start_pos'], emoji['end_pos']):
                custom_positions.add(pos)

        i = 0
        while i < len(content):

            if i in custom_positions:
                i += 1
                continue

            char = content[i]

            if is_emoji(char):

                emoji_end = i
                sequence = char

                while emoji_end + 1 < len(content):
                    next_char = content[emoji_end + 1]

                    if (is_emoji(sequence + next_char) or
                            unicodedata.category(next_char).startswith('M')):
                        emoji_end += 1
                        sequence += next_char
                    else:
                        break

                all_emojis.append({
                    'emoji_str': sequence,
                    'is_custom': False,
                    'is_animated': False,
                    'start_pos': i,
                    'end_pos': emoji_end + 1
                })

                for pos in range(i, emoji_end + 1):
                    custom_positions.add(pos)

                i = emoji_end + 1
            else:
                i += 1

        all_emojis.sort(key=lambda x: x['start_pos'])

        return all_emojis

    async def track_message_emojis(self, message: discord.Message):

        await self._track_message_emojis(message)

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):

        if after.author.bot or not after.guild or self.shutting_down:
            return

        if not await self.check_rate_limit(after.author.id, 'message_edit', limit=20, window=10):
            logger.debug(f"Rate limited message edit from {after.author.id}")
            return

        try:

            if before.content == after.content:
                return

            edit_key = f"edit_{after.id}"
            current_time = datetime.utcnow()

            if edit_key in self.processed_messages:
                time_since = (
                    current_time - self.processed_messages[edit_key]).total_seconds()
                if time_since < 0.1:
                    logger.debug(f"Skipping duplicate edit {after.id}")
                    return

            self.processed_messages[edit_key] = current_time

            await self._track_message_emojis(after)

        except Exception as e:
            logger.error(f"Error tracking message edit: {e}")
            async with self.metrics_lock:
                self.metrics['errors'] += 1

    async def _track_message_content(self, message: discord.Message):

        try:
            guild_id = message.guild.id
            user_id = message.author.id
            channel_id = message.channel.id

            category_id = None
            if hasattr(message.channel, 'category_id') and message.channel.category_id:
                category_id = message.channel.category_id

            encrypted_username = self.encrypt_username(str(message.author))
            created_at = message.created_at.astimezone(
                timezone.utc).replace(tzinfo=None)

            await self.redis_batch_write('messages', {
                'params': [
                    guild_id, user_id, channel_id, category_id, message.id,
                    encrypted_username, len(message.content),
                    json.dumps([user.id for user in message.mentions]),
                    len(message.attachments) > 0,
                    len(message.embeds) > 0,
                    created_at, message.author.bot
                ]
            })

            for user in message.mentions:
                if user.id == user_id:

                    continue

                if not await self.check_rate_limit(user_id, 'mention', limit=50, window=30):

                    continue

                encrypted_mentioned_username = self.encrypt_username(str(user))

                await self.redis_batch_write('mentions', {
                    'params': [
                        guild_id,
                        user.id,
                        user_id,
                        channel_id,
                        category_id,
                        message.id,
                        created_at,
                        encrypted_mentioned_username
                    ]
                })

            async with self.metrics_lock:
                self.metrics['message_inserts'] += 1
                self.metrics['mention_inserts'] += len(message.mentions)

        except Exception as e:
            print(f"Error tracking message content: {e}")
            traceback.print_exc()

    def _is_single_emoji(self, text: str) -> bool:

        if not text:
            return False

        cleaned = ''.join(
            c for c in text if not unicodedata.category(c).startswith('M'))

        return is_emoji(cleaned) and len(cleaned) == 1

    def _extract_custom_emojis(self, content: str) -> List[str]:

        pattern = r'<a?:\w{2,32}:\d{18,22}>'
        return re.findall(pattern, content)

    def _extract_unicode_emojis(self, content: str) -> List[str]:

        emojis = []
        i = 0

        while i < len(content):
            char = content[i]

            if is_emoji(char):

                emoji_end = i
                while emoji_end < len(content) and (
                    is_emoji(content[i:emoji_end+1]) or
                    unicodedata.category(content[emoji_end]).startswith('M')
                ):
                    emoji_end += 1

                emoji_text = content[i:emoji_end]
                emojis.append(emoji_text)
                i = emoji_end
            else:
                i += 1

        return emojis

    async def _track_message_emojis(self, message: discord.Message):

        if not message.guild:
            return

        if not await self.check_rate_limit(message.author.id, 'emoji_message', limit=50, window=30):
            return

        content = message.content.strip()
        if not content:
            return

        current_time = datetime.utcnow()

        category_id = None
        if hasattr(message.channel, 'category_id') and message.channel.category_id:
            category_id = message.channel.category_id

        emojis_found = self._extract_all_emojis_from_content(content)

        logger.debug(f"Message from {message.author.id}: '{content[:50]}...'")
        logger.debug(
            f"Found {len(emojis_found)} emojis: {[e['emoji_str'] for e in emojis_found]}")

        for emoji_data in emojis_found:
            data = {
                'guild_id': message.guild.id,
                'user_id': message.author.id,
                'emoji_str': emoji_data['emoji_str'],
                'usage_type': 'message',
                'created_at': current_time,
                'channel_id': message.channel.id,
                'category_id': category_id,
                'username': str(message.author),
                'is_custom': emoji_data['is_custom'],
                'is_animated': emoji_data.get('is_animated', False)
            }

            await self._buffer_emoji_event(message.guild.id, data)

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction: discord.Reaction, user: Union[discord.Member, discord.User]):

        if user.bot or self.shutting_down:
            return

        if not reaction.message.guild:
            return

        if not await self.check_rate_limit(user.id, 'reaction', limit=40, window=10):
            logger.debug(f"Rate limited reaction from {user.id}")
            return

        try:
            emoji = reaction.emoji
            current_time = datetime.utcnow()

            if isinstance(emoji, discord.Emoji):
                emoji_str = f"<{'a' if emoji.animated else ''}:{emoji.name}:{emoji.id}>"
                is_custom = True
                is_animated = emoji.animated
            else:

                emoji_str = str(emoji)
                is_custom = False
                is_animated = False

            if not is_custom and not is_emoji(emoji_str):
                logger.debug(f"Skipping non-emoji reaction: {emoji_str}")
                return

            category_id = None
            if hasattr(reaction.message.channel, 'category_id') and reaction.message.channel.category_id:
                category_id = reaction.message.channel.category_id

            data = {
                'guild_id': reaction.message.guild.id,
                'user_id': user.id,
                'emoji_str': emoji_str,
                'usage_type': 'reaction',
                'created_at': current_time,
                'channel_id': reaction.message.channel.id,
                'category_id': category_id,
                'username': str(user),
                'is_custom': is_custom,
                'is_animated': is_animated
            }

            await self._buffer_emoji_event(reaction.message.guild.id, data)

            logger.debug(f"Tracked reaction: {emoji_str} by {user.id}")

        except Exception as e:
            logger.error(f"Error tracking reaction add: {e}")
            async with self.metrics_lock:
                self.metrics['errors'] += 1

    async def _buffer_emoji_event(self, guild_id: int, data: Dict[str, Any]):

        try:

            encrypted_username = self.encrypt_username(data['username'])

            params = [
                data['guild_id'],
                data['user_id'],
                data['channel_id'],
                data.get('category_id'),
                encrypted_username,
                data['emoji_str'],
                data['is_custom'],
                1,
                data['created_at'],
                data['usage_type']
            ]

            await self.redis_batch_write('emojis', {
                'params': params,
                'source': 'emoji_tracking',
                'timestamp': time.time()
            })

            async with self.metrics_lock:
                self.metrics['emoji_inserts'] += 1

            logger.debug(
                f"Buffered {data['emoji_str']} as {data['usage_type']} by {data['user_id']}")

        except Exception as e:
            logger.error(f"Error buffering emoji event: {e}")

    # INVITE TRACKING

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):

        if member.bot or self.shutting_down:
            return

        try:

            try:

                if not member.guild.me.guild_permissions.manage_guild:

                    await self._track_unknown_join(member)
                    return

                invites_after = await member.guild.invites()

            except discord.Forbidden:
                print(
                    f"❌ InviteTracker: No permission to get invites in {member.guild.name}")
                await self._track_unknown_join(member)
                return
            except Exception as e:
                print(f"❌ InviteTracker: Error getting invites: {e}")
                await self._track_unknown_join(member)
                return

            db_invite_uses = {}
            try:
                async with self.pool.acquire() as conn:
                    records = await conn.fetch("""
                        SELECT invite_code, uses 
                        FROM invite_uses 
                        WHERE guild_id = $1
                    """, member.guild.id)
                    for record in records:
                        db_invite_uses[record['invite_code']] = record['uses']

            except Exception as e:
                print(
                    f"⚠️ InviteTracker: Error getting invite uses from DB: {e}")
                db_invite_uses = {}

            used_invite = None
            for invite in invites_after:
                if invite.uses is None:
                    continue

                before_uses = db_invite_uses.get(invite.code, 0)

                if isinstance(before_uses, int) and isinstance(invite.uses, int):
                    if invite.uses > before_uses:
                        used_invite = invite

                        inviter_id = invite.inviter.id if invite.inviter else 0
                        await self._update_invite_in_database(
                            guild_id=member.guild.id,
                            invite_code=invite.code,
                            uses=invite.uses,
                            inviter_id=inviter_id
                        )
                        break

            invite_type = "valid"
            current_time = datetime.utcnow()

            if used_invite and used_invite.inviter:

                if used_invite.inviter.id == member.id:

                    return

                inviter_created = used_invite.inviter.created_at.replace(
                    tzinfo=None)
                inviter_age = current_time - inviter_created
                invitee_age = current_time - \
                    member.created_at.replace(tzinfo=None)

                if inviter_age.total_seconds() < (3 * 24 * 60 * 60):
                    invite_type = "suspicious"

                if invitee_age.total_seconds() < (3 * 24 * 60 * 60):
                    invite_type = "suspicious"

                await self._buffer_invite_event(
                    guild_id=member.guild.id,
                    inviter_id=used_invite.inviter.id,
                    invitee_id=member.id,
                    invite_code=used_invite.code,
                    invite_type=invite_type
                )

            else:

                invitee_age = current_time - \
                    member.created_at.replace(tzinfo=None)

                if invitee_age.total_seconds() < (3 * 24 * 60 * 60):
                    invite_type = "suspicious"

                await self._buffer_invite_event(
                    guild_id=member.guild.id,
                    inviter_id=0,
                    invitee_id=member.id,
                    invite_code="unknown",
                    invite_type=invite_type
                )

        except Exception as e:
            print(f"❌ InviteTracker: Error tracking member join: {e}")
            traceback.print_exc()
            async with self.metrics_lock:
                self.metrics['errors'] += 1

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):

        if member.bot or self.shutting_down:
            return

        try:

            original_inviter_id = 0

            if self.pool:
                try:
                    async with self.pool.acquire() as conn:

                        row = await conn.fetchrow('''
                            SELECT inviter_id 
                            FROM invite_tracking
                            WHERE guild_id = $1 
                            AND invitee_id = $2
                            AND invite_type IN ('valid', 'suspicious')
                            ORDER BY created_at DESC
                            LIMIT 1
                        ''', member.guild.id, member.id)

                        if row:
                            original_inviter_id = row['inviter_id']

                except Exception as e:
                    print(
                        f"⚠️ InviteTracker: Error finding original inviter: {e}")

            await self._buffer_invite_event(
                guild_id=member.guild.id,
                inviter_id=original_inviter_id,
                invitee_id=member.id,
                invite_code="left",
                invite_type="left"
            )

        except Exception as e:
            print(f"❌ InviteTracker: Error marking user left: {e}")
            traceback.print_exc()
            async with self.metrics_lock:
                self.metrics['errors'] += 1

    @commands.Cog.listener()
    async def on_invite_create(self, invite: discord.Invite):

        if self.shutting_down:
            return

        try:
            guild_id = invite.guild.id

            if invite.uses is not None:

                inviter_id = invite.inviter.id if invite.inviter else 0

                await self._update_invite_in_database(
                    guild_id=guild_id,
                    invite_code=invite.code,
                    uses=invite.uses,
                    inviter_id=inviter_id
                )

                if invite.inviter:

                    invite_type = "valid"
                    inviter_account_age = datetime.utcnow(
                    ) - invite.inviter.created_at.replace(tzinfo=None)
                    if inviter_account_age.total_seconds() < (3 * 24 * 60 * 60):
                        invite_type = "suspicious"

                    await self._buffer_invite_event(
                        guild_id=guild_id,
                        inviter_id=inviter_id,
                        invitee_id=None,
                        invite_code=invite.code,
                        invite_type=invite_type
                    )

        except Exception as e:
            print(f"❌ InviteTracker: Error tracking invite creation: {e}")
            traceback.print_exc()
            async with self.metrics_lock:
                self.metrics['errors'] += 1

    @commands.Cog.listener()
    async def on_invite_delete(self, invite: discord.Invite):

        if self.shutting_down:
            return

        try:
            guild_id = invite.guild.id

            try:
                async with self.pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE invite_uses 
                        SET deleted = true, updated_at = NOW()
                        WHERE guild_id = $1 AND invite_code = $2
                    """, guild_id, invite.code)

            except Exception as e:
                print(
                    f"⚠️ InviteTracker: Error marking invite as deleted: {e}")

        except Exception as e:
            print(f"❌ InviteTracker: Error processing invite deletion: {e}")
            traceback.print_exc()
            async with self.metrics_lock:
                self.metrics['errors'] += 1

    # HELPER FUNCTIONS

    async def _track_unknown_join(self, member: discord.Member):

        current_time = datetime.utcnow()
        invitee_age = current_time - member.created_at.replace(tzinfo=None)
        invite_type = "suspicious" if invitee_age.total_seconds() < (3 * 24 * 60 *
                                                                     60) else "valid"

        await self._buffer_invite_event(
            guild_id=member.guild.id,
            inviter_id=0,
            invitee_id=member.id,
            invite_code="unknown",
            invite_type=invite_type
        )

    async def _buffer_invite_event(self, guild_id: int, inviter_id: int,
                                   invitee_id: Optional[int], invite_code: str,
                                   invite_type: str):

        try:
            current_time = datetime.utcnow()

            params = [
                guild_id,
                inviter_id,
                invitee_id,
                invite_code,
                invite_type,
                current_time
            ]

            await self.redis_batch_write('invites', {
                'params': params
            })

            async with self.metrics_lock:
                self.metrics['invite_inserts'] += 1

        except Exception as e:
            print(f"❌ InviteTracker: Error buffering invite event: {e}")
            traceback.print_exc()

    async def _update_invite_in_database(self, guild_id: int, invite_code: str, uses: int, inviter_id: int):

        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO invite_uses (guild_id, invite_code, uses, inviter_id, updated_at)
                    VALUES ($1, $2, $3, $4, NOW())
                    ON CONFLICT (guild_id, invite_code) 
                    DO UPDATE SET 
                        uses = EXCLUDED.uses,
                        inviter_id = EXCLUDED.inviter_id,
                        updated_at = NOW()
                """, guild_id, invite_code, uses, inviter_id)

        except Exception as e:
            print(f"❌ InviteTracker: Error updating invite in database: {e}")
            traceback.print_exc()

    # METRICS

    async def get_metrics(self) -> Dict[str, Any]:

        async with self.metrics_lock:
            metrics_copy = self.metrics.copy()

        async with self.voice_lock:
            active_sessions = len(self.active_voice_sessions)

        metrics_copy.update({
            'db_connected': self.db_connected,
            'redis_connected': self.redis_connected,

            'active_voice_sessions': active_sessions,
            'is_timescale_initialized': self.is_timescale_initialized,

            'redis_batch_stats': {
                'last_flush_time': self.last_flush_time,
                'batch_sizes': self.batch_sizes.copy()
            }
        })

        return metrics_copy

    # RATE LIMITING

    async def check_rate_limit(self, identifier: int, action: str, limit: int = 10, window: int = 60) -> bool:

        current = datetime.utcnow()
        key = f"{identifier}:{action}"

        if key not in self.rate_limits:
            self.rate_limits[key] = []

        self.rate_limits[key] = [
            ts for ts in self.rate_limits[key]
            if (current - ts).total_seconds() < window
        ]

        if len(self.rate_limits[key]) >= limit:
            return False

        self.rate_limits[key].append(current)
        return True


# SETUP

async def setup(bot: commands.Bot):

    try:
        cog = DatabaseStats(bot)

        await bot.add_cog(cog)
        logger.info("DatabaseStats cog added to bot")

        if bot.is_ready():
            print("Bot is ready, starting init_pools...")
            cog.init_pools.start()
        else:
            print("Bot not ready yet, will start tasks when ready")

        print("✅ DatabaseStats cog loaded successfully")

        return cog

    except Exception as e:
        logger.error(f"❌ Error loading DatabaseStats cog: {e}")
        traceback.print_exc()
        raise
