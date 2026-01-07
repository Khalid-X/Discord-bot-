import os
import asyncpg
import discord
from discord import app_commands
from discord.ext import commands, tasks
from datetime import datetime, timedelta, date, time, timezone
import asyncio
from typing import List, Dict, Optional, Tuple
import logging
from PIL import Image, ImageDraw, ImageFont
import io
import aiohttp
import redis.asyncio as redis
import json
import traceback
from pathlib import Path


def ensure_timezone_aware(dt: datetime) -> datetime:

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


logger = logging.getLogger(__name__)

# CONFIGURATION
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
REDIS_DB = int(os.getenv('REDIS_DB', 0))

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', 5432))
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

SESSION_THRESHOLD = 600
CACHE_TTL = 300
BATCH_SAVE_INTERVAL = 30
USERS_PER_PAGE = 10
MAX_CUSTOM_DAYS = 2000
TIME_PERIODS = [7, 14, 30]
STREAK_MIN_SESSION_SECONDS = 60
DAILY_STREAK_UPDATE_HOUR = 2


# REDIS CONNECTION


def get_redis_connection():

    try:
        if REDIS_PASSWORD and REDIS_PASSWORD.strip():

            print(
                f"🔐 Connecting to Redis with password at {REDIS_HOST}:{REDIS_PORT}"
            )
            return redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                db=REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
        else:

            print(
                f"🔓 Connecting to Redis without password at {REDIS_HOST}:{REDIS_PORT}"
            )
            return redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
    except Exception as e:
        print(f"❌ Error creating Redis connection: {e}")
        return None

# POSTGRESQL CONNECTION


async def create_db_pool():
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# FONT MANAGEMENT

BASE_DIR = Path(__file__).resolve().parent.parent


def get_fonts():
    try:

        custom_font_path = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"
        if os.path.exists(custom_font_path):
            font_small = ImageFont.truetype(custom_font_path, 16)
            font_medium = ImageFont.truetype(custom_font_path, 20)
            font_large = ImageFont.truetype(custom_font_path, 24)
            font_larger = ImageFont.truetype(custom_font_path, 30)
            font_huge = ImageFont.truetype(custom_font_path, 40)
            font_giant = ImageFont.truetype(custom_font_path, 60)
            return font_small, font_medium, font_large, font_larger, font_huge, font_giant
    except:
        pass

    try:
        font_small = ImageFont.truetype("arial.ttf", 16)
        font_medium = ImageFont.truetype("arial.ttf", 20)
        font_large = ImageFont.truetype("arial.ttf", 24)
        font_larger = ImageFont.truetype("arial.ttf", 30)
        font_huge = ImageFont.truetype("arial.ttf", 40)
        font_giant = ImageFont.truetype("arial.ttf", 60)
        return font_small, font_medium, font_large, font_larger, font_huge, font_giant
    except:
        font_small = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_large = ImageFont.load_default()
        font_larger = ImageFont.load_default()
        font_huge = ImageFont.load_default()
        font_giant = ImageFont.load_default()
        return font_small, font_medium, font_large, font_larger, font_huge, font_giant


# DRAWING

def draw_text_with_stroke(draw, position, text, font, fill, stroke_fill, stroke_width):
    x, y = position
    for dx in [-stroke_width, 0, stroke_width]:
        for dy in [-stroke_width, 0, stroke_width]:
            if dx != 0 or dy != 0:
                draw.text((x + dx, y + dy), text, font=font, fill=stroke_fill)
    draw.text((x, y), text, font=font, fill=fill)


def draw_text_centered_with_stroke(draw, text, center_position, font, fill="white", stroke_fill="black", stroke_width=2, max_width=None):

    center_x, center_y = center_position

    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]

    x = center_x - (text_width // 2)
    y = center_y - (text_height // 2)

    if max_width and text_width > max_width:
        current_font = font
        original_size = font.size

        for size in range(original_size - 2, 8, -2):
            try:
                smaller_font = ImageFont.truetype(current_font.path, size)
                bbox = draw.textbbox((0, 0), text, font=smaller_font)
                text_width = bbox[2] - bbox[0]

                if text_width <= max_width:

                    text_height = bbox[3] - bbox[1]
                    x = center_x - (text_width // 2)
                    y = center_y - (text_height // 2)
                    draw_text_with_stroke(
                        draw, (x, y), text, smaller_font, fill, stroke_fill, stroke_width)
                    return
            except:
                continue

    draw_text_with_stroke(draw, (x, y), text, font, fill,
                          stroke_fill, stroke_width)


def draw_rounded_rectangle(draw, xy, radius, fill=None, outline=None):

    x1, y1, x2, y2 = xy
    draw.rectangle([x1 + radius, y1, x2 - radius, y2],
                   fill=fill, outline=outline)
    draw.rectangle([x1, y1 + radius, x2, y2 - radius],
                   fill=fill, outline=outline)
    draw.pieslice([x1, y1, x1 + radius * 2, y1 + radius * 2],
                  180, 270, fill=fill, outline=outline)
    draw.pieslice([x2 - radius * 2, y1, x2, y1 + radius * 2],
                  270, 360, fill=fill, outline=outline)
    draw.pieslice([x1, y2 - radius * 2, x1 + radius * 2, y2],
                  90, 180, fill=fill, outline=outline)
    draw.pieslice([x2 - radius * 2, y2 - radius * 2, x2, y2],
                  0, 90, fill=fill, outline=outline)


# FORMATS

def format_duration(total_seconds):
    if total_seconds == 0:
        return "0s"

    days = total_seconds // (3600 * 24)
    hours = (total_seconds % (3600 * 24)) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    if days > 0:
        return f"{days}d {hours}h"
    elif hours > 0:
        return f"{hours}h {minutes}m"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{int(seconds)}s"


def format_user_count(count):
    return f"{count:,}"


def format_streak(streak_days):
    if streak_days >= 365:
        years = streak_days // 365
        days = streak_days % 365
        if days > 0:
            return f"{years}y {days}d"
        return f"{years}y"
    return f"{streak_days}d"


# REDIS BATCH


class RedisBatchManager:
    def __init__(self, db_pool):
        self.redis_client = None
        self.db_pool = db_pool
        self.batch_prefix = "activity_batch:"
        self.active_sessions_prefix = "active_sessions:"
        self.batch_size = 100
        self.is_initialized = False

    async def initialize(self):
        try:
            print(
                f"🔄 Initializing Redis connection to {REDIS_HOST}:{REDIS_PORT}..."
            )
            self.redis_client = get_redis_connection()

            if not self.redis_client:
                print("❌ Failed to create Redis connection object")
                return False

            try:
                print(f"  Testing connection...")
                await asyncio.wait_for(self.redis_client.ping(), timeout=5)
                print(
                    f"✅ Redis batch manager initialized at {REDIS_HOST}:{REDIS_PORT}"
                )
                self.is_initialized = True
                return True
            except asyncio.TimeoutError:
                print(
                    f"❌ Redis connection timeout at {REDIS_HOST}:{REDIS_PORT}"
                )
                self.redis_client = None
                return False
            except redis.AuthenticationError as e:
                print(f"❌ Redis authentication error: {e}")
                print(
                    f"  Hint: Check if Redis password is correct or if Redis requires no password"
                )
                self.redis_client = None
                return False
            except Exception as e:
                print(f"❌ Redis connection failed: {e}")

                print("🔄 Trying fallback connection without password...")
                try:
                    self.redis_client = redis.Redis(
                        host=REDIS_HOST,
                        port=REDIS_PORT,
                        db=REDIS_DB,
                        decode_responses=True,
                        socket_connect_timeout=5
                    )
                    await asyncio.wait_for(self.redis_client.ping(), timeout=5)
                    print(f"✅ Connected without password")
                    self.is_initialized = True
                    return True
                except Exception as fallback_error:
                    print(
                        f"❌ Fallback connection also failed: {fallback_error}"
                    )
                    self.redis_client = None
                    return False
        except Exception as e:
            print(f"❌ Failed to create Redis connection: {e}")
            self.redis_client = None
            return False

    async def add_session_to_batch(self, guild_id: int, user_id: int, activity_type: str,
                                   display_name: str, start_time: datetime, end_time: datetime):

        if not self.redis_client or not self.is_initialized:
            print("  ⚠️ Redis client not available or not initialized!")
            return

        batch_key = f"{self.batch_prefix}{int(datetime.utcnow().timestamp() // BATCH_SAVE_INTERVAL)}"

        duration_seconds = int((end_time - start_time).total_seconds())

        if duration_seconds <= 0:

            return

        if display_name is None:
            display_name = f"{activity_type.title()} Activity"
        elif not isinstance(display_name, str):
            display_name = str(display_name)

        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        session_data = {

            'guild_id': str(guild_id),

            'user_id': str(user_id),
            'activity_type': str(activity_type),
            'display_name': display_name[:255],
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': int(duration_seconds)
        }

        print(f"  Pushing session to Redis...")
        await self.redis_client.rpush(batch_key, json.dumps(session_data))

        list_len = await self.redis_client.llen(batch_key)
        print(f"  ✅ Session added to batch. Batch now has {list_len} sessions")

        await self.redis_client.expire(batch_key, BATCH_SAVE_INTERVAL * 2)

    async def save_batches_to_postgresql(self):
        print("🔄 RedisBatchManager.save_batches_to_postgresql() called")

        if not self.redis_client or not self.db_pool or not self.is_initialized:
            print("❌ Redis, DB pool, or not initialized")
            return

        try:

            batch_keys = await self.redis_client.keys(f"{self.batch_prefix}*")

            if not batch_keys:

                return

            for batch_key in batch_keys:

                session_data_list = await self.redis_client.lrange(batch_key, 0, -1)

                successful_sessions = 0
                merged_sessions = 0
                failed_sessions = 0

                if session_data_list:
                    async with self.db_pool.acquire() as conn:

                        for i, session_json in enumerate(session_data_list):
                            try:
                                session_data = json.loads(session_json)

                                required_fields = ['guild_id', 'user_id', 'activity_type',
                                                   'display_name', 'start_time', 'end_time', 'duration_seconds']
                                if not all(key in session_data for key in required_fields):
                                    print(
                                        f"    ❌ Missing required fields in session data: {session_data}"
                                    )
                                    failed_sessions += 1
                                    continue

                                if session_data['display_name'] is None:
                                    session_data['display_name'] = f"{session_data['activity_type'].title()} Activity"
                                elif not isinstance(session_data['display_name'], str):
                                    session_data['display_name'] = str(
                                        session_data['display_name']
                                    )

                                try:
                                    guild_id = int(session_data['guild_id'])
                                    user_id = int(session_data['user_id'])
                                except (ValueError, TypeError) as e:
                                    print(f"    ❌ Invalid ID format: {e}")
                                    failed_sessions += 1
                                    continue

                                if session_data['duration_seconds'] <= 0:
                                    print(
                                        f"    ❌ Invalid duration: {session_data['duration_seconds']}"
                                    )
                                    failed_sessions += 1
                                    continue

                                activity_name_id = await self.get_or_create_activity_name(
                                    conn,
                                    session_data['display_name'],
                                    session_data['activity_type']
                                )

                                if not activity_name_id:
                                    print(
                                        f"    ❌ Failed to get/create activity name"
                                    )
                                    failed_sessions += 1
                                    continue

                                start_time_str = session_data['start_time']
                                end_time_str = session_data['end_time']

                                if start_time_str.endswith('Z'):
                                    start_time_str = start_time_str[:-
                                                                    1] + '+00:00'
                                if end_time_str.endswith('Z'):
                                    end_time_str = end_time_str[:-1] + '+00:00'

                                try:
                                    session_start_dt = datetime.fromisoformat(
                                        start_time_str)
                                    session_end_dt = datetime.fromisoformat(
                                        end_time_str)
                                except ValueError as e:
                                    print(
                                        f"    ❌ Invalid datetime format: {e}"
                                    )
                                    failed_sessions += 1
                                    continue

                                if session_start_dt.tzinfo is None:
                                    session_start_dt = session_start_dt.replace(
                                        tzinfo=timezone.utc)
                                if session_end_dt.tzinfo is None:
                                    session_end_dt = session_end_dt.replace(
                                        tzinfo=timezone.utc)

                                recent_session = await conn.fetchrow('''
                                    SELECT id, start_time, end_time, duration_seconds
                                    FROM activity_sessions
                                    WHERE guild_id = $1::bigint
                                    AND user_id = $2::bigint
                                    AND activity_name_id = $3::integer
                                    AND end_time >= $4::timestamptz - INTERVAL '10 minutes'
                                    ORDER BY end_time DESC
                                    LIMIT 1
                                ''',
                                                                     guild_id,
                                                                     user_id,
                                                                     activity_name_id,
                                                                     session_start_dt
                                                                     )

                                if recent_session:

                                    existing_end_time = recent_session['end_time']
                                    if existing_end_time.tzinfo is None:
                                        existing_end_time = existing_end_time.replace(
                                            tzinfo=timezone.utc)

                                    final_end_time = max(
                                        existing_end_time, session_end_dt)

                                    existing_duration = recent_session['duration_seconds']
                                    new_duration = session_data['duration_seconds']
                                    final_duration = existing_duration + new_duration

                                    await conn.execute('''
                                        UPDATE activity_sessions
                                        SET end_time = $1::timestamptz,
                                            duration_seconds = $2::integer,
                                            created_at = NOW()
                                        WHERE id = $3::bigint AND start_time = $4::timestamptz
                                    ''',
                                                       final_end_time,
                                                       final_duration,
                                                       recent_session['id'],
                                                       recent_session['start_time']
                                                       )

                                    merged_sessions += 1
                                    print(
                                        f"    ✅ Merged session (ID: {recent_session['id']})"
                                    )

                                else:

                                    try:
                                        await conn.execute('''
                                            INSERT INTO activity_sessions
                                            (guild_id, user_id, activity_name_id, start_time,
                                             end_time, duration_seconds, created_at)
                                            VALUES ($1::bigint, $2::bigint, $3::integer, $4::timestamptz,
                                                    $5::timestamptz, $6::integer, NOW())
                                        ''',
                                                           guild_id,
                                                           user_id,
                                                           activity_name_id,
                                                           session_start_dt,
                                                           session_end_dt,
                                                           session_data['duration_seconds']
                                                           )
                                        successful_sessions += 1

                                    except asyncpg.UniqueViolationError as e:

                                        failed_sessions += 1
                                    except Exception as e:
                                        print(f"    ❌ Insert error: {e}")
                                        traceback.print_exc()
                                        failed_sessions += 1

                            except Exception as e:
                                print(f"    ❌ Error processing session: {e}")
                                traceback.print_exc()
                                failed_sessions += 1

                    print(
                        f"   Activity Batch result: {successful_sessions} new, {merged_sessions} merged, {failed_sessions} failed"
                    )

                    await self.redis_client.delete(batch_key)

            print(
                f"✅ Total: {successful_sessions} new sessions, {merged_sessions} merged sessions"
            )

        except Exception as e:
            print(f"❌ Error saving batches to PostgreSQL: {e}")
            traceback.print_exc()

    async def get_or_create_activity_name(self, conn, display_name: str, activity_type: str) -> int:

        try:

            if display_name is None:
                display_name = f"{activity_type.title()} Activity"
            elif not isinstance(display_name, str):
                display_name = str(display_name)

            result = await conn.fetchrow('''
                SELECT id FROM activity_names
                WHERE display_name = $1::varchar AND activity_type = $2::varchar
            ''', display_name[:255], activity_type)

            if result:
                return result['id']

            result = await conn.fetchrow('''
                INSERT INTO activity_names (display_name, activity_type)
                VALUES ($1::varchar, $2::varchar)
                RETURNING id
            ''', display_name[:255], activity_type)

            if result:
                return result['id']
            else:
                logger.error(
                    f"Failed to create activity name: {display_name} ({activity_type})"
                )
                return None

        except asyncpg.UniqueViolationError:

            result = await conn.fetchrow('''
                SELECT id FROM activity_names
                WHERE display_name = $1::varchar AND activity_type = $2::varchar
            ''', display_name[:255], activity_type)
            return result['id'] if result else None

        except Exception as e:
            logger.error(f"Error in get_or_create_activity_name: {e}")
            return None


# DATABASE MANAGER

class ActivityDatabase:
    def __init__(self):
        self.pool = None
        self.batch_manager = None
        self.is_initialized = False

    # INITIALIZATION OF TABLES
    async def initialize(self):
        try:
            self.pool = await create_db_pool()
            async with self.pool.acquire() as conn:

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS activity_names (
                        id SERIAL PRIMARY KEY,
                        display_name VARCHAR(255) NOT NULL,
                        activity_type VARCHAR(100) NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(display_name, activity_type)
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS activity_sessions (
                        id SERIAL,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        activity_name_id INTEGER REFERENCES activity_names(id),
                        start_time TIMESTAMPTZ NOT NULL,
                        end_time TIMESTAMPTZ,
                        duration_seconds INTEGER,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (id, start_time)
                    )
                ''')

                try:
                    await conn.execute("""
                        SELECT create_hypertable(
                            'activity_sessions',
                            'start_time',
                            if_not_exists => TRUE,
                            chunk_time_interval => INTERVAL '7 days',
                            create_default_indexes => FALSE
                        )
                    """)
                    print("✅ Created activity_sessions hypertable")
                except Exception as e:
                    print(
                        f"Not using TimescaleDB or hypertable already exists: {e}"
                    )

                # INDEXES

                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_activity_sessions_guild_user_time
                    ON activity_sessions(guild_id, user_id, start_time DESC)
                ''')
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_activity_sessions_merge_check
                    ON activity_sessions(guild_id, user_id, activity_name_id, end_time DESC)
                ''')
                print("✅ Created merge check index")
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_activity_sessions_guild_time
                    ON activity_sessions(guild_id, start_time DESC)
                ''')
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_activity_sessions_user_time
                    ON activity_sessions(user_id, start_time DESC)
                ''')
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_activity_sessions_activity_time
                    ON activity_sessions(activity_name_id, start_time DESC)
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS guild_members (
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        role_ids BIGINT[],
                        updated_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (guild_id, user_id)
                    )
                ''')

                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_guild_members_roles
                    ON guild_members USING GIN(role_ids)
                ''')

                # STREAK TABLES

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS daily_activity_log (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        activity_date DATE NOT NULL,
                        has_activity BOOLEAN DEFAULT FALSE,
                        total_duration_seconds INTEGER DEFAULT 0,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(guild_id, user_id, activity_date)
                    )
                ''')

                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS activity_streaks (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        current_streak_days INTEGER DEFAULT 0,
                        best_streak_days INTEGER DEFAULT 0,
                        streak_start_date DATE,
                        last_activity_date DATE,
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(guild_id, user_id)
                    )
                ''')

                # INDEXES FOR STREAKS
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_daily_activity_log_guild_user_date
                    ON daily_activity_log(guild_id, user_id, activity_date DESC)
                ''')
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_activity_streaks_guild_user
                    ON activity_streaks(guild_id, user_id)
                ''')
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_activity_streaks_best
                    ON activity_streaks(guild_id, best_streak_days DESC)
                ''')

                print("✅ Created streak tracking tables")

            print("🔄 Initializing Redis batch manager...")
            self.batch_manager = RedisBatchManager(self.pool)
            batch_manager_success = await self.batch_manager.initialize()

            if batch_manager_success:
                print(
                    f"✅ PostgreSQL database initialized at {DB_HOST}:{DB_PORT}/{DB_NAME}")
                self.is_initialized = True
                return True
            else:
                print(
                    f"⚠️ PostgreSQL initialized but Redis failed at {DB_HOST}:{DB_PORT}/{DB_NAME}")
                self.is_initialized = False
                return False

        except Exception as e:
            logger.error(f"❌ Failed to initialize database: {e}")
            traceback.print_exc()
            self.is_initialized = False
            raise

    async def update_member_roles(self, guild_id: int, user_id: int, role_ids: List[int]):
        async with self.pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO guild_members (guild_id, user_id, role_ids, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (guild_id, user_id) DO UPDATE
                SET role_ids = EXCLUDED.role_ids, updated_at = NOW()
            ''', guild_id, user_id, role_ids)

   # QUERY METHOS

    async def get_user_activity_days_and_streak(self, guild_id: int, user_id: int, activity_name: str,
                                                activity_type: str, days: int) -> Dict:

        async with self.pool.acquire() as conn:

            activity_name_id = await conn.fetchval('''
                SELECT id FROM activity_names
                WHERE display_name = $1 AND activity_type = $2
            ''', activity_name, activity_type)

            if not activity_name_id:
                return {'days_used': 0, 'highest_streak': 0}

            result = await conn.fetch('''
                SELECT DISTINCT DATE(start_time) as activity_date
                FROM activity_sessions
                WHERE guild_id = $1
                AND user_id = $2
                AND activity_name_id = $3
                AND start_time >= NOW() - INTERVAL '1 day' * $4
                ORDER BY activity_date
            ''', guild_id, user_id, activity_name_id, days)

            dates = [row['activity_date'] for row in result]
            total_days = len(dates)

            highest_streak = 0
            current_streak = 0
            previous_date = None

            for current_date in dates:
                if previous_date is None:
                    current_streak = 1
                else:

                    days_diff = (current_date - previous_date).days
                    if days_diff == 1:
                        current_streak += 1
                    else:

                        highest_streak = max(highest_streak, current_streak)
                        current_streak = 1

                previous_date = current_date

            highest_streak = max(highest_streak, current_streak)

            return {
                'days_used': total_days,
                'highest_streak': highest_streak,
                'dates': dates
            }

    async def get_user_activity_stats(self, guild_id: int, user_id: int, days: int,
                                      role_id: int = None, sort_by: str = "time") -> List[Dict]:
        async with self.pool.acquire() as conn:
            logger.info(
                f"Querying user activity for user_id={user_id}, guild_id={guild_id}, days={days}"
            )

            if role_id:
                query = '''
                    SELECT
                        an.display_name,
                        an.activity_type,
                        COUNT(*) as session_count,
                        SUM(a.duration_seconds) as total_duration_seconds
                    FROM activity_sessions a
                    JOIN activity_names an ON a.activity_name_id = an.id
                    JOIN guild_members gm ON a.user_id = gm.user_id AND a.guild_id = gm.guild_id
                    WHERE a.guild_id = $1
                    AND a.user_id = $2
                    AND a.start_time >= NOW() - INTERVAL '1 day' * $3
                    AND $4 = ANY(gm.role_ids)
                    GROUP BY an.display_name, an.activity_type
                '''
                results = await conn.fetch(query, guild_id, user_id, days, role_id)
            else:
                query = '''
                    SELECT
                        an.display_name,
                        an.activity_type,
                        COUNT(*) as session_count,
                        SUM(a.duration_seconds) as total_duration_seconds
                    FROM activity_sessions a
                    JOIN activity_names an ON a.activity_name_id = an.id
                    WHERE a.guild_id = $1
                    AND a.user_id = $2
                    AND a.start_time >= NOW() - INTERVAL '1 day' * $3
                    GROUP BY an.display_name, an.activity_type
                '''
                results = await conn.fetch(query, guild_id, user_id, days)

            logger.info(f"Found {len(results)} activities for user {user_id}")
            stats = [dict(row) for row in results]

            if sort_by == "sessions":
                stats.sort(key=lambda x: x['session_count'], reverse=True)
            elif sort_by == "time":
                stats.sort(
                    key=lambda x: x['total_duration_seconds'], reverse=True)

            return stats

    async def get_server_activity_stats(self, guild_id: int, days: int, role_id: int = None,
                                        sort_by: str = "users") -> List[Dict]:
        async with self.pool.acquire() as conn:
            if role_id:
                query = '''
                    SELECT
                        an.display_name,
                        an.activity_type,
                        COUNT(DISTINCT a.user_id) as user_count,
                        SUM(a.duration_seconds) as total_duration_seconds,
                        COUNT(*) as session_count
                    FROM activity_sessions a
                    JOIN activity_names an ON a.activity_name_id = an.id
                    JOIN guild_members gm ON a.user_id = gm.user_id AND a.guild_id = gm.guild_id
                    WHERE a.guild_id = $1
                    AND a.start_time >= NOW() - INTERVAL '1 day' * $2
                    AND $3 = ANY(gm.role_ids)
                    GROUP BY an.display_name, an.activity_type
                '''
                results = await conn.fetch(query, guild_id, days, role_id)
            else:
                query = '''
                    SELECT
                        an.display_name,
                        an.activity_type,
                        COUNT(DISTINCT a.user_id) as user_count,
                        SUM(a.duration_seconds) as total_duration_seconds,
                        COUNT(*) as session_count
                    FROM activity_sessions a
                    JOIN activity_names an ON a.activity_name_id = an.id
                    WHERE a.guild_id = $1
                    AND a.start_time >= NOW() - INTERVAL '1 day' * $2
                    GROUP BY an.display_name, an.activity_type
                '''
                results = await conn.fetch(query, guild_id, days)

            stats = [dict(row) for row in results]
            if sort_by == "users":
                stats.sort(key=lambda x: x['user_count'], reverse=True)
            elif sort_by == "sessions":
                stats.sort(key=lambda x: x['session_count'], reverse=True)
            elif sort_by == "time":
                stats.sort(
                    key=lambda x: x['total_duration_seconds'], reverse=True)
            return stats

    async def get_leaderboard_stats(self, guild_id: int, category: str, days: int,
                                    role_id: int = None, sort_by: str = "users") -> List[Dict]:
        async with self.pool.acquire() as conn:
            if category == "all":
                category_filter = ""
                params = [guild_id, days]
            else:
                category_filter = "AND an.activity_type = $3"
                params = [guild_id, days, category]

            if role_id:
                query = f'''
                    SELECT
                        an.display_name,
                        an.activity_type,
                        COUNT(DISTINCT a.user_id) as user_count,
                        SUM(a.duration_seconds) as total_duration_seconds,
                        COUNT(*) as session_count
                    FROM activity_sessions a
                    JOIN activity_names an ON a.activity_name_id = an.id
                    JOIN guild_members gm ON a.user_id = gm.user_id AND a.guild_id = gm.guild_id
                    WHERE a.guild_id = $1
                    AND a.start_time >= NOW() - INTERVAL '1 day' * $2
                    AND $4 = ANY(gm.role_ids)
                    {category_filter}
                    GROUP BY an.display_name, an.activity_type
                '''
                params.append(role_id)
            else:
                query = f'''
                    SELECT
                        an.display_name,
                        an.activity_type,
                        COUNT(DISTINCT a.user_id) as user_count,
                        SUM(a.duration_seconds) as total_duration_seconds,
                        COUNT(*) as session_count
                    FROM activity_sessions a
                    JOIN activity_names an ON a.activity_name_id = an.id
                    WHERE a.guild_id = $1
                    AND a.start_time >= NOW() - INTERVAL '1 day' * $2
                    {category_filter}
                    GROUP BY an.display_name, an.activity_type
                '''

            results = await conn.fetch(query, *params)
            stats = [dict(row) for row in results]
            if sort_by == "users":
                stats.sort(key=lambda x: x['user_count'], reverse=True)
            elif sort_by == "sessions":
                stats.sort(key=lambda x: x['session_count'], reverse=True)
            elif sort_by == "time":
                stats.sort(
                    key=lambda x: x['total_duration_seconds'], reverse=True)
            return stats

    # STREAK FUNCTIONS

    async def log_daily_activity(self, guild_id: int, user_id: int, activity_date: date, duration_seconds: int):

        async with self.pool.acquire() as conn:

            await conn.execute('''
                INSERT INTO daily_activity_log
                (guild_id, user_id, activity_date, has_activity, total_duration_seconds)
                VALUES ($1::bigint, $2::bigint, $3::date, TRUE, $4::integer)
                ON CONFLICT (guild_id, user_id, activity_date) DO UPDATE
                SET has_activity = TRUE,
                    total_duration_seconds = daily_activity_log.total_duration_seconds + EXCLUDED.total_duration_seconds,
                    created_at = NOW()
            ''', guild_id, user_id, activity_date, duration_seconds)

    async def update_streak_for_user(self, guild_id: int, user_id: int, check_date: date = None):

        if check_date is None:
            check_date = datetime.utcnow().date()

        async with self.pool.acquire() as conn:

            streak_record = await conn.fetchrow('''
                SELECT * FROM activity_streaks
                WHERE guild_id = $1::bigint AND user_id = $2::bigint
            ''', guild_id, user_id)

            if not streak_record:

                await conn.execute('''
                    INSERT INTO activity_streaks
                    (guild_id, user_id, current_streak_days, best_streak_days,
                    streak_start_date, last_activity_date)
                    VALUES ($1::bigint, $2::bigint, 0, 0, NULL, NULL)
                ''', guild_id, user_id)
                streak_record = await conn.fetchrow('''
                    SELECT * FROM activity_streaks
                    WHERE guild_id = $1::bigint AND user_id = $2::bigint
                ''', guild_id, user_id)

            yesterday = check_date - timedelta(days=1)

            yesterday_activity = await conn.fetchrow('''
                SELECT has_activity FROM daily_activity_log
                WHERE guild_id = $1::bigint AND user_id = $2::bigint AND activity_date = $3::date
            ''', guild_id, user_id, yesterday)

            today_activity = await conn.fetchrow('''
                SELECT has_activity FROM daily_activity_log
                WHERE guild_id = $1::bigint AND user_id = $2::bigint AND activity_date = $3::date
            ''', guild_id, user_id, check_date)

            current_streak = streak_record['current_streak_days']
            best_streak = streak_record['best_streak_days']
            last_activity = streak_record['last_activity_date']

            if today_activity and today_activity['has_activity']:

                if yesterday_activity and yesterday_activity['has_activity']:

                    if current_streak == 0:
                        current_streak = 2
                        streak_start = yesterday
                    else:
                        current_streak += 1

                elif last_activity and (check_date - last_activity).days == 1:

                    current_streak += 1
                else:

                    current_streak = 1
                    streak_start = check_date

                if current_streak > best_streak:
                    best_streak = current_streak

                await conn.execute('''
                    UPDATE activity_streaks
                    SET current_streak_days = $3::integer,
                        best_streak_days = $4::integer,
                        streak_start_date = COALESCE($5::date, streak_start_date),
                        last_activity_date = $6::date,
                        updated_at = NOW()
                    WHERE guild_id = $1::bigint AND user_id = $2::bigint
                ''', guild_id, user_id, current_streak, best_streak,
                                   streak_start if 'streak_start' in locals() else None, check_date)

                return current_streak, best_streak

            elif current_streak > 0:

                if last_activity and (check_date - last_activity).days > 1:

                    await conn.execute('''
                        UPDATE activity_streaks
                        SET current_streak_days = 0,
                            streak_start_date = NULL,
                            updated_at = NOW()
                        WHERE guild_id = $1::bigint AND user_id = $2::bigint
                    ''', guild_id, user_id)
                    return 0, best_streak

            return current_streak, best_streak

    async def get_user_streak_stats(self, guild_id: int, user_id: int) -> Dict:

        async with self.pool.acquire() as conn:
            streak_record = await conn.fetchrow('''
                SELECT current_streak_days, best_streak_days,
                    streak_start_date, last_activity_date
                FROM activity_streaks
                WHERE guild_id = $1::bigint AND user_id = $2::bigint
            ''', guild_id, user_id)

            if not streak_record:
                return {
                    'current_streak': 0,
                    'best_streak': 0,
                    'streak_start': None,
                    'last_activity': None,
                    'is_active_today': False
                }

            today = datetime.utcnow().date()
            today_activity = await conn.fetchrow('''
                SELECT has_activity FROM daily_activity_log
                WHERE guild_id = $1::bigint AND user_id = $2::bigint AND activity_date = $3::date
            ''', guild_id, user_id, today)

            return {
                'current_streak': streak_record['current_streak_days'],
                'best_streak': streak_record['best_streak_days'],
                'streak_start': streak_record['streak_start_date'],
                'last_activity': streak_record['last_activity_date'],
                'is_active_today': today_activity['has_activity'] if today_activity else False
            }

    async def update_all_streaks(self, check_date: date = None):

        if check_date is None:
            check_date = datetime.utcnow().date()

        async with self.pool.acquire() as conn:

            users = await conn.fetch('''
                SELECT DISTINCT guild_id, user_id
                FROM daily_activity_log
                WHERE activity_date >= $1::date - INTERVAL '30 days'
            ''', check_date)

            updated_count = 0
            for user in users:
                await self.update_streak_for_user(user['guild_id'], user['user_id'], check_date)
                updated_count += 1

            logger.info(f"Updated streaks for {updated_count} users")
            return updated_count


# DROPDOWN MENUS

class SortBySelect(discord.ui.Select):
    def __init__(self, command_type: str, current_value: str = "time"):
        if command_type == "user":
            options = [
                discord.SelectOption(label="By Sessions", value="sessions",
                                     description="Sort by session count", default=current_value == "sessions"),
                discord.SelectOption(label="By Time", value="time",
                                     description="Sort by total time spent", default=current_value == "time")
            ]
        else:
            options = [
                discord.SelectOption(label="By Users", value="users",
                                     description="Sort by number of users", default=current_value == "users"),
                discord.SelectOption(label="By Sessions", value="sessions",
                                     description="Sort by session count", default=current_value == "sessions"),
                discord.SelectOption(label="By Time", value="time",
                                     description="Sort by total time spent", default=current_value == "time")
            ]
        super().__init__(placeholder="Sort by...",
                         options=options, custom_id="sort_by_select")

    async def callback(self, interaction: discord.Interaction):

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        view.sort_by = self.values[0]
        await view.update_message(interaction)


class CategorySelect(discord.ui.Select):
    def __init__(self, guild: discord.Guild, current_value: str = "all"):
        options = [
            discord.SelectOption(label="All Activities", value="all",
                                 description="Show all activity types", default=current_value == "all"),
            discord.SelectOption(label="Playing", value="playing",
                                 description="Game activities", default=current_value == "playing"),
            discord.SelectOption(label="Streaming", value="streaming",
                                 description="Streaming activities", default=current_value == "streaming"),
            discord.SelectOption(label="Listening", value="listening",
                                 description="Music activities", default=current_value == "listening"),
            discord.SelectOption(label="Watching", value="watching",
                                 description="Video activities", default=current_value == "watching"),
            discord.SelectOption(label="Competing", value="competing",
                                 description="Competitive activities", default=current_value == "competing"),
            discord.SelectOption(label="Custom", value="custom",
                                 description="Custom statuses", default=current_value == "custom")
        ]
        super().__init__(placeholder="Filter category...",
                         options=options, custom_id="category_select")

    async def callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        view.category = self.values[0]
        await view.update_message(interaction)


class RoleSelect(discord.ui.Select):
    def __init__(self, guild: discord.Guild, current_value: str = "none"):
        options = [
            discord.SelectOption(label="No Filter", value="none",
                                 description="Show all users", default=current_value == "none")
        ]
        roles = [role for role in guild.roles if role.name != "@everyone"]
        roles.sort(key=lambda x: x.position, reverse=True)
        for role in roles[:24]:
            options.append(discord.SelectOption(
                label=role.name[:100],
                value=str(role.id),
                description=f"Filter by {role.name}"[:100]
            ))
        super().__init__(placeholder="Filter by role...",
                         options=options, custom_id="role_select")

    async def callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        view.role_id = self.values[0]
        await view.update_message(interaction)


# TIME MODAL

class TimeModal(discord.ui.Modal, title='Custom Time Period'):
    days = discord.ui.TextInput(
        label='Enter number of days',
        placeholder='e.g., 7, 14, 30, 90...',
        min_length=1,
        max_length=4,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days = int(self.days.value)
            if days <= 0 or days > MAX_CUSTOM_DAYS:
                await interaction.response.send_message(f"❌ Please enter a number between 1 and {MAX_CUSTOM_DAYS} days.", ephemeral=True)
                return
            await interaction.response.defer()
            view = self.view
            view.days = days
            view.show_time_buttons = False
            view.page = 0
            await view.update_message(interaction)
        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)


# PAGINATION BUTTONS

class PrevButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="⬅️", custom_id="leaderboard_prev")

    async def callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        if view.page > 0:
            view.page -= 1
            await view.update_message(interaction)


class NextButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="➡️", custom_id="leaderboard_next")

    async def callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        view.page += 1
        await view.update_message(interaction)


class PageIndicator(discord.ui.Button):
    def __init__(self, page: int = 0, total_pages: int = 1):
        super().__init__(style=discord.ButtonStyle.primary,
                         label=f"Page {page + 1}/{total_pages}",
                         custom_id="leaderboard_page", disabled=True)


# ACTIVITY VIEW

class ActivityView(discord.ui.View):
    def __init__(self, cog, guild: discord.Guild, command_type: str,
                 initial_days: int = 14, initial_role: str = "none",
                 target_user: discord.Member = None, initial_category: str = "all",
                 initial_sort: str = "time", page: int = 0):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild
        self.command_type = command_type
        self.days = initial_days
        self.role_id = initial_role
        self.target_user = target_user
        self.category = initial_category
        self.sort_by = initial_sort
        self.page = page
        self.show_time_buttons = False
        self._update_components()

    def _update_components(self):
        self.clear_items()

        if self.command_type in ["server", "leaderboard"]:
            self.add_item(RoleSelect(self.guild, self.role_id))

        if self.command_type == "leaderboard":
            self.add_item(CategorySelect(self.guild, self.category))

        if self.command_type in ["user", "server", "leaderboard"]:
            self.add_item(SortBySelect(self.command_type, self.sort_by))

        if self.command_type == "leaderboard":
            self.add_item(PrevButton())
            self.add_item(NextButton())

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="refresh"
        )
        refresh_button.callback = self.refresh_callback
        self.add_item(refresh_button)

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="time_settings"
        )
        time_settings_button.callback = self.time_settings_callback
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            for days in TIME_PERIODS:
                days_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.days == days else discord.ButtonStyle.secondary,
                    label=f"{days} Days",
                    custom_id=f"days_{days}"
                )
                days_button.callback = self.create_days_callback(days)
                self.add_item(days_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.days}d)" if self.days not in TIME_PERIODS else "Custom",
                custom_id="custom_days"
            )
            custom_button.callback = self.custom_days_callback
            self.add_item(custom_button)

    def create_days_callback(self, days: int):
        async def callback(interaction: discord.Interaction):
            await self.handle_button_click(interaction, days=days)
        return callback

    async def refresh_callback(self, interaction: discord.Interaction):
        await self.handle_button_click(interaction)

    async def time_settings_callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.show_time_buttons = not self.show_time_buttons
        self._update_components()
        await interaction.edit_original_response(view=self)

    async def custom_days_callback(self, interaction: discord.Interaction):
        modal = TimeModal()
        modal.view = self
        await interaction.response.send_modal(modal)

    async def handle_button_click(self, interaction: discord.Interaction, days: int = None):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        if days:
            self.days = days
            self.show_time_buttons = False
            self.page = 0

        await self.update_message(interaction)

    async def update_message(self, interaction: discord.Interaction):
        try:
            if self.command_type == "user":

                logger.info(
                    f"Updating user activity for {self.target_user.display_name}, sort_by={self.sort_by}, days={self.days}"
                )

                role_id_param = None
                if self.role_id != "none":
                    try:
                        role_id_param = int(self.role_id)
                    except ValueError:
                        pass

                stats = await self.cog.db.get_user_activity_stats(
                    self.guild.id,
                    self.target_user.id,
                    self.days,
                    role_id_param,
                    self.sort_by
                )

                streak_stats = await self.cog.db.get_user_streak_stats(
                    self.guild.id,
                    self.target_user.id
                )

                logger.info(f"Retrieved {len(stats)} activities for user")

                file = await generate_user_activity_image(
                    self.guild,
                    self.target_user,
                    self.days,
                    stats,
                    streak_stats,
                    self.sort_by
                )

                self._update_components()

                await interaction.edit_original_response(attachments=[file], view=self)

            elif self.command_type == "server":

                logger.info(
                    f"Updating server activity, sort_by={self.sort_by}, days={self.days}"
                )

                role_id_param = None
                if self.role_id != "none":
                    try:
                        role_id_param = int(self.role_id)
                    except ValueError:
                        pass

                stats = await self.cog.db.get_server_activity_stats(
                    self.guild.id,
                    self.days,
                    role_id_param,
                    self.sort_by
                )

                file = await generate_server_activity_image(
                    self.guild,
                    self.days,
                    stats,
                    self.role_id,
                    self.sort_by
                )

                self._update_components()
                await interaction.edit_original_response(attachments=[file], view=self)

            elif self.command_type == "leaderboard":

                logger.info(
                    f"Updating leaderboard, sort_by={self.sort_by}, days={self.days}, category={self.category}"
                )

                role_id_param = None
                if self.role_id != "none":
                    try:
                        role_id_param = int(self.role_id)
                    except ValueError:
                        pass

                stats = await self.cog.db.get_leaderboard_stats(
                    self.guild.id,
                    self.category,
                    self.days,
                    role_id_param,
                    self.sort_by
                )

                total_items = len(stats)
                total_pages = max(
                    1, (total_items + USERS_PER_PAGE - 1) // USERS_PER_PAGE)

                if self.page >= total_pages:
                    self.page = max(0, total_pages - 1)

                file = await generate_activity_leaderboard_image(
                    self.guild,
                    stats,
                    self.category,
                    self.days,
                    self.role_id,
                    self.sort_by,
                    self.page
                )

                for child in self.children:
                    if isinstance(child, PrevButton):
                        child.disabled = (self.page == 0 or total_pages <= 1)
                    elif isinstance(child, NextButton):
                        child.disabled = (
                            self.page >= total_pages - 1 or total_pages <= 1)

                await interaction.edit_original_response(attachments=[file], view=self)

        except Exception as e:
            logger.error(f"Error updating activity message: {e}")
            traceback.print_exc()
            try:

                if interaction.response.is_done():
                    await interaction.followup.send("❌ An error occurred while updating stats.", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ An error occurred while updating stats.", ephemeral=True)
            except:
                pass


# WHOS VIEW

class WhosView(discord.ui.View):
    def __init__(self, cog, guild: discord.Guild, activity_type: str, activity_name: str, page: int = 0):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild = guild
        self.activity_type = activity_type
        self.activity_name = activity_name
        self.page = page
        self.users_per_page = 10

        self._update_components()

    def _update_components(self):
        self.clear_items()

        self.add_item(PrevButton())
        self.add_item(NextButton())

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="whos_refresh"
        )
        refresh_button.callback = self.refresh_callback
        self.add_item(refresh_button)

    async def refresh_callback(self, interaction: discord.Interaction):
        await self.update_message(interaction)

    async def update_message(self, interaction: discord.Interaction):
        current_time = datetime.utcnow()
        current_sessions = []

        for user_id, sessions in self.cog.active_sessions.items():
            for session_key, session_data in sessions.items():
                if (session_data['display_name'].lower() == self.activity_name.lower() and
                    session_data['activity_type'] == self.activity_type and
                        session_data['guild_id'] == self.guild.id):
                    member = self.guild.get_member(user_id)
                    if member:
                        duration = current_time - session_data['start_time']
                        current_sessions.append((member, duration))

        current_sessions.sort(key=lambda x: x[1], reverse=True)

        total_sessions = len(current_sessions)
        total_pages = max(
            1, (total_sessions + self.users_per_page - 1) // self.users_per_page)

        if self.page >= total_pages:
            self.page = total_pages - 1
        if self.page < 0:
            self.page = 0

        start_idx = self.page * self.users_per_page
        end_idx = min(start_idx + self.users_per_page, len(current_sessions))
        page_sessions = current_sessions[start_idx:end_idx]

        embed = discord.Embed(
            title=f"👀 Who's {self.activity_type.title()} {self.activity_name}",
            color=discord.Color.green(),
            timestamp=datetime.utcnow()
        )

        if page_sessions:
            description = ""
            for i, (member, duration) in enumerate(page_sessions, 1):
                hours, remainder = divmod(int(duration.total_seconds()), 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m {seconds}s"
                description += f"{start_idx + i}. {member.mention} - **{time_str}**\n"

            embed.description = description
            embed.set_footer(
                text=f"Page {self.page + 1}/{total_pages} • Total: {total_sessions} users • Updated")
        else:
            embed.description = f"No one is currently doing this activity."
            embed.color = discord.Color.orange()
            embed.set_footer(text="No users found")

        for child in self.children:
            if isinstance(child, PrevButton):
                child.disabled = (self.page == 0 or total_pages <= 1)
            elif isinstance(child, NextButton):
                child.disabled = (self.page >= total_pages -
                                  1 or total_pages <= 1)

        await interaction.edit_original_response(embed=embed, view=self)

    async def handle_button_click(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.update_message(interaction)


# ACTIVITY TRACKER

class ActivityTracker(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = ActivityDatabase()
        self.active_sessions = {}
        self.activity_types = ['playing', 'streaming',
                               'listening', 'watching', 'competing', 'custom']
        self.initialization_complete = False

        self.save_batches_task = None
        self.update_members_task = None
        self.session_cleanup_task = None
        self.save_ongoing_sessions_task = None
        self.daily_streak_update_task = None

    # REDIS AND POSTGRESQL INITIALIZATION

    async def cog_load(self):
        print("🔄 Loading ActivityTracker cog...")
        try:

            success = await self.db.initialize()
            if success:
                self.initialization_complete = True

                @tasks.loop(seconds=BATCH_SAVE_INTERVAL)
                async def save_batches_task():
                    await self._save_batches_task()

                @tasks.loop(minutes=5)
                async def update_members_task():
                    await self._update_members_task()

                @tasks.loop(seconds=60)
                async def session_cleanup_task():
                    await self._session_cleanup_task()

                @tasks.loop(minutes=1)
                async def save_ongoing_sessions_task():
                    await self._save_ongoing_sessions_task()

                @tasks.loop(time=time(hour=DAILY_STREAK_UPDATE_HOUR, minute=0))
                async def daily_streak_update_task():
                    await self._daily_streak_update_task()

                self.save_batches_task = save_batches_task
                self.update_members_task = update_members_task
                self.session_cleanup_task = session_cleanup_task
                self.save_ongoing_sessions_task = save_ongoing_sessions_task
                self.daily_streak_update_task = daily_streak_update_task

                self.save_batches_task.start()
                self.update_members_task.start()
                self.session_cleanup_task.start()
                self.save_ongoing_sessions_task.start()
                self.daily_streak_update_task.start()

                print("✅ ActivityTracker fully loaded and tasks started")
            else:
                print(
                    "⚠️ ActivityTracker loaded but Redis initialization failed - batch saving disabled")

                @tasks.loop(minutes=5)
                async def update_members_task():
                    await self._update_members_task()

                @tasks.loop(seconds=60)
                async def session_cleanup_task():
                    await self._session_cleanup_task()

                @tasks.loop(time=time(hour=DAILY_STREAK_UPDATE_HOUR, minute=0))
                async def daily_streak_update_task():
                    await self._daily_streak_update_task()

                self.update_members_task = update_members_task
                self.session_cleanup_task = session_cleanup_task
                self.daily_streak_update_task = daily_streak_update_task

                self.update_members_task.start()
                self.session_cleanup_task.start()
                self.daily_streak_update_task.start()

                self.initialization_complete = True

        except Exception as e:
            print(f"❌ Failed to load ActivityTracker: {e}")
            traceback.print_exc()
            self.initialization_complete = False

    async def cog_unload(self):
        if self.save_batches_task and self.save_batches_task.is_running():
            self.save_batches_task.cancel()
        if self.update_members_task and self.update_members_task.is_running():
            self.update_members_task.cancel()
        if self.session_cleanup_task and self.session_cleanup_task.is_running():
            self.session_cleanup_task.cancel()
        if self.save_ongoing_sessions_task and self.save_ongoing_sessions_task.is_running():
            self.save_ongoing_sessions_task.cancel()
        if self.daily_streak_update_task and self.daily_streak_update_task.is_running():
            self.daily_streak_update_task.cancel()

    async def _save_batches_task(self):

        if not self.initialization_complete:
            return

        try:
            if self.db.batch_manager and self.db.batch_manager.is_initialized:
                await self.db.batch_manager.save_batches_to_postgresql()
                print("  ✅ Batch save attempt completed")
            else:
                print("  ⚠️ Batch manager not initialized!")
        except Exception as e:
            print(f"❌ Error in save batches task: {e}")
            traceback.print_exc()

    async def _update_members_task(self):

        if not self.initialization_complete:
            return

        try:
            for guild in self.bot.guilds:
                for member in guild.members:
                    if member.bot:
                        continue
                    role_ids = [
                        role.id for role in member.roles if role.name != "@everyone"]
                    await self.db.update_member_roles(guild.id, member.id, role_ids)
        except Exception as e:
            logger.error(f"Error updating members: {e}")

    async def _save_ongoing_sessions_task(self):

        if not self.initialization_complete:
            print("⏰ save_ongoing_sessions_task: Cog not fully initialized, skipping...")
            return

        print("⏰ save_ongoing_sessions_task running...")
        try:
            if not self.db.batch_manager or not self.db.batch_manager.is_initialized:
                print("❌ Redis batch manager not initialized, skipping save")
                return

            current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
            sessions_saved = 0
            sessions_skipped = 0

            for user_id, sessions in list(self.active_sessions.items()):

                for activity_key, session in list(sessions.items()):
                    try:

                        if session['start_time'].tzinfo is None:
                            session['start_time'] = session['start_time'].replace(
                                tzinfo=timezone.utc)

                        total_duration = (
                            current_time - session['start_time']).total_seconds()

                        last_save_time = session.get('last_save_time')

                        if last_save_time:

                            if last_save_time.tzinfo is None:
                                last_save_time = last_save_time.replace(
                                    tzinfo=timezone.utc)

                            new_duration = (
                                current_time - last_save_time).total_seconds()

                            if new_duration >= 10:

                                await self.db.batch_manager.add_session_to_batch(
                                    int(session['guild_id']),
                                    int(user_id),
                                    session['activity_type'],
                                    session['display_name'],
                                    last_save_time,
                                    current_time
                                )
                                sessions_saved += 1

                                session['last_save_time'] = current_time
                                session['last_seen'] = current_time

                                today = current_time.date()
                                await self.db.log_daily_activity(
                                    int(session['guild_id']),
                                    int(user_id),
                                    today,
                                    int(new_duration)
                                )

                            else:

                                sessions_skipped += 1
                                session['last_seen'] = current_time

                        else:

                            if total_duration >= 30:

                                await self.db.batch_manager.add_session_to_batch(
                                    int(session['guild_id']),
                                    int(user_id),
                                    session['activity_type'],
                                    session['display_name'],
                                    session['start_time'],
                                    current_time
                                )
                                sessions_saved += 1

                                session['last_save_time'] = current_time
                                session['last_seen'] = current_time

                                today = current_time.date()
                                await self.db.log_daily_activity(
                                    int(session['guild_id']),
                                    int(user_id),
                                    today,
                                    int(total_duration)
                                )

                            else:

                                sessions_skipped += 1
                                session['last_seen'] = current_time

                    except Exception as e:
                        print(
                            f"    ❌ Error saving session {activity_key}: {e}"
                        )
                        traceback.print_exc()

        except Exception as e:
            print(f"❌ Error in save_ongoing_sessions_task: {e}")
            traceback.print_exc()

    # CLEANUP

    async def _session_cleanup_task(self):

        if not self.initialization_complete:
            return

        try:
            await self.cleanup_old_sessions()
        except Exception as e:
            logger.error(f"Error in session cleanup task: {e}")

    # DAILY STREAK UPDATE

    async def _daily_streak_update_task(self):

        if not self.initialization_complete:
            return

        print("📅 Running daily streak update...")
        try:
            updated_count = await self.db.update_all_streaks()
            print(f"✅ Updated streaks for {updated_count} users")
        except Exception as e:
            print(f"❌ Error in daily streak update: {e}")
            traceback.print_exc()

   # HELPER FUNCTIONS

    def get_activity_info(self, activity) -> Tuple[str, str]:

        try:

            raw_type = str(activity.type).split('.')[-1].lower()

            if hasattr(activity, 'name') and activity.name:
                name = activity.name.strip()

            else:

                name = f"{raw_type.title()} Activity"
                print(f"  No activity.name found, using fallback: '{name}'")

            lower_name = name.lower()

            # LIST OF IGNORED ACTIVITIES
            ignore_activities = [
                'hang status',
                'hanging out',
                'in a voice channel',
                'voice channel',
                'voice chat',
                'discord voice',
                'vc',
                'in call'
            ]

            if any(ignore_term in lower_name for ignore_term in ignore_activities):

                return None, None

            if isinstance(activity, discord.Spotify):
                print(f"  🎵 Spotify detected")
                name = "Spotify"
                activity_type = 'listening'
            else:

                if raw_type in self.activity_types:
                    activity_type = raw_type
                else:

                    activity_type = 'custom'

            name = name[:100] if name else f"{activity_type.title()} Activity"

            return activity_type, name

        except Exception as e:
            print(f"  ❌ Error getting activity info: {e}")
            traceback.print_exc()

            return None, None

    def get_activity_key(self, user_id: int, activity_type: str, activity_name: str) -> str:
        return f"{user_id}_{activity_type}_{activity_name.lower()}"

    async def start_activity_session(self, user_id: int, guild_id: int, activity):

        activity_type, activity_name = self.get_activity_info(activity)

        if activity_type is None or activity_name is None or not activity_name:
            print(f"❓ Skipping hangout/voice activity for user {user_id}")
            return

        activity_key = self.get_activity_key(
            user_id, activity_type, activity_name)

        if user_id in self.active_sessions and activity_key in self.active_sessions[user_id]:

            self.active_sessions[user_id][activity_key]['last_seen'] = datetime.utcnow(
            ).replace(tzinfo=timezone.utc)

            return

        session_data = {
            'start_time': datetime.utcnow().replace(tzinfo=timezone.utc),
            'last_seen': datetime.utcnow().replace(tzinfo=timezone.utc),
            'last_save_time': None,
            'display_name': activity_name,
            'activity_type': activity_type,
            'guild_id': guild_id
        }

        if user_id not in self.active_sessions:
            self.active_sessions[user_id] = {}

        self.active_sessions[user_id][activity_key] = session_data

    async def end_activity_session(self, user_id: int, activity_key: str, force_end: bool = False):

        if user_id not in self.active_sessions or activity_key not in self.active_sessions[user_id]:
            print(f"  ❌ Session not found")
            return

        session = self.active_sessions[user_id][activity_key]
        current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
        start_time = session['start_time']

        time_since_last_seen = (
            current_time - session['last_seen']).total_seconds()

        if not force_end and time_since_last_seen < SESSION_THRESHOLD:

            return

        duration = (current_time - start_time).total_seconds()

        last_save_time = session.get('last_save_time')

        if last_save_time:

            if last_save_time.tzinfo is None:
                last_save_time = last_save_time.replace(tzinfo=timezone.utc)

            unsaved_duration = (current_time - last_save_time).total_seconds()

            if unsaved_duration > 0:

                if self.db.batch_manager and self.db.batch_manager.is_initialized:

                    await self.db.batch_manager.add_session_to_batch(
                        int(session['guild_id']),
                        int(user_id),
                        session['activity_type'],
                        session['display_name'],
                        last_save_time,
                        current_time
                    )

                    today = current_time.date()
                    await self.db.log_daily_activity(
                        int(session['guild_id']),
                        int(user_id),
                        today,
                        int(unsaved_duration)
                    )

                else:
                    print(f"  ⚠️ Batch manager not available, unsaved portion lost!")
        else:

            print(f"  Session was never saved to batch")
            if duration >= 10:
                if self.db.batch_manager and self.db.batch_manager.is_initialized:

                    await self.db.batch_manager.add_session_to_batch(
                        int(session['guild_id']),
                        int(user_id),
                        session['activity_type'],
                        session['display_name'],
                        start_time,
                        current_time
                    )

                    today = current_time.date()
                    await self.db.log_daily_activity(
                        int(session['guild_id']),
                        int(user_id),
                        today,
                        int(duration)
                    )

                else:
                    print(f"  ⚠️ Batch manager not available, session lost!")

        self.active_sessions[user_id].pop(activity_key)
        if not self.active_sessions[user_id]:
            del self.active_sessions[user_id]

    async def cleanup_old_sessions(self):

        current_time = datetime.utcnow().replace(tzinfo=timezone.utc)

        for user_id in list(self.active_sessions.keys()):
            for activity_key in list(self.active_sessions[user_id].keys()):
                session = self.active_sessions[user_id][activity_key]
                time_since_last_seen = (
                    current_time - session['last_seen']).total_seconds()

                if time_since_last_seen >= SESSION_THRESHOLD:

                    await self.end_activity_session(user_id, activity_key, force_end=True)

    async def end_all_user_sessions(self, user_id: int):

        if user_id in self.active_sessions:
            for activity_key in list(self.active_sessions[user_id].keys()):
                await self.end_activity_session(user_id, activity_key, force_end=True)

    # ACTIVITY LISTENING

    @commands.Cog.listener()
    async def on_presence_update(self, before, after):

        if after.bot or not after.guild:

            return

        user_id = after.id
        guild_id = after.guild.id

        role_ids = [role.id for role in after.roles if role.name != "@everyone"]
        await self.db.update_member_roles(guild_id, user_id, role_ids)

        current_activities = {}
        if after.activities:

            for activity in after.activities:
                activity_type, activity_name = self.get_activity_info(activity)

                if activity_type is None or activity_name is None:
                    print(f"    🚫 Skipping filtered activity")
                    continue

                if activity_name:
                    activity_key = self.get_activity_key(
                        user_id, activity_type, activity_name)
                    current_activities[activity_key] = activity
                    print(f"    • {activity_type}: {activity_name}")
        else:
            print(f"  No Discord activities found")

        previous_activities = {}
        if user_id in self.active_sessions:
            previous_activities = {key: session for key,
                                   session in self.active_sessions[user_id].items()}

        sessions_to_end = set(previous_activities.keys()) - \
            set(current_activities.keys())

        for activity_key in sessions_to_end:
            await self.end_activity_session(user_id, activity_key, force_end=True)

        new_sessions = set(current_activities.keys()) - \
            set(previous_activities.keys())

        for activity_key in new_sessions:
            await self.start_activity_session(user_id, guild_id, current_activities[activity_key])

        ongoing_sessions = set(current_activities.keys()) & set(
            previous_activities.keys())
        if ongoing_sessions:

            for activity_key in ongoing_sessions:
                if user_id in self.active_sessions and activity_key in self.active_sessions[user_id]:
                    self.active_sessions[user_id][activity_key]['last_seen'] = datetime.utcnow(
                    ).replace(tzinfo=timezone.utc)
                    print(f"    ↻ Updated {activity_key}")

    @commands.Cog.listener()
    async def on_member_remove(self, member):

        await self.end_all_user_sessions(member.id)

    @commands.Cog.listener()
    async def on_member_update(self, before, after):

        if after.bot or not after.guild:
            return
        role_ids = [role.id for role in after.roles if role.name != "@everyone"]
        await self.db.update_member_roles(after.guild.id, after.id, role_ids)


# IMAGE GENERATION FUNCTIONS

async def generate_user_activity_image(guild: discord.Guild, user: discord.Member, days: int,
                                       activity_data: dict, streak_stats: dict, sort_by: str = "time"):

    try:
        template_path = BASE_DIR / "assets" / "images" / "activity user final png.png"
        image = Image.open(template_path)
        if image.mode != "RGB":
            image = image.convert("RGB")
        draw = ImageDraw.Draw(image)

        font_small, font_medium, font_large, font_larger, font_huge, font_giant = get_fonts()

        avatar_x, avatar_y = 10, 10
        avatar_size = (51, 51)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(user.display_avatar.url) as response:
                    avatar_data = await response.read()

            avatar_image = Image.open(io.BytesIO(avatar_data))
            avatar_image = avatar_image.resize(
                avatar_size, Image.Resampling.LANCZOS).convert('RGBA')

            mask = Image.new('L', avatar_size, 0)
            mask_draw = ImageDraw.Draw(mask)
            mask_draw.ellipse((0, 0, avatar_size[0], avatar_size[1]), fill=255)
            avatar_image.putalpha(mask)

            avatar_area = image.crop(
                (avatar_x, avatar_y, avatar_x +
                 avatar_size[0], avatar_y + avatar_size[1])
            ).convert('RGBA')

            avatar_with_bg = Image.new('RGBA', avatar_size, (0, 0, 0, 0))
            avatar_with_bg.paste(avatar_image, (0, 0), avatar_image)
            avatar_area.paste(avatar_with_bg, (0, 0), avatar_with_bg)
            image.paste(avatar_area.convert('RGB'), (avatar_x, avatar_y))

        except Exception as e:
            print(f"  ❌ Could not add user avatar: {e}")

        username = user.name
        text_start_x = 73
        text_start_y = 28

        username_rect_center = (145, 35)
        username_rect_width = 160
        username_rect_height = 32

        def fit_text_to_username_rect(text, start_x, start_y, rect_center_x, rect_center_y, rect_width, rect_height):

            font_sizes = [36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16]

            for font_size in font_sizes:
                try:
                    font = ImageFont.truetype(
                        BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", font_size)
                except:
                    font = ImageFont.load_default()

                bbox = draw.textbbox((0, 0), text, font=font)
                text_width = bbox[2] - bbox[0]
                text_height = bbox[3] - bbox[1]

                text_x = start_x
                text_y = start_y - (text_height // 2)

                rect_left = rect_center_x - (rect_width // 2)
                rect_right = rect_center_x + (rect_width // 2)
                rect_top = rect_center_y - (rect_height // 2)
                rect_bottom = rect_center_y + (rect_height // 2)

                fits_h = (text_x >= rect_left - 1) and (text_x +
                                                        text_width <= rect_right + 1)
                fits_v = (text_y >= rect_top - 1) and (text_y +
                                                       text_height <= rect_bottom + 1)

                if fits_h and fits_v:
                    return text, font, (text_x, text_y)

            smallest_font = 16
            try:
                font = ImageFont.truetype(
                    BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", smallest_font)
            except:
                font = ImageFont.load_default()

            current_text = text
            while len(current_text) > 3:

                current_text = current_text[:-4] + "..."
                bbox = draw.textbbox((0, 0), current_text, font=font)
                text_width = bbox[2] - bbox[0]

                text_x = start_x
                bbox = draw.textbbox((0, 0), current_text, font=font)
                text_height = bbox[3] - bbox[1]
                text_y = start_y - (text_height // 2)

                if text_x + text_width <= rect_right:
                    return current_text, font, (text_x, text_y)

            return text[:3] + "...", font, (text_start_x, text_start_y)

        fitted_username, username_font, username_pos = fit_text_to_username_rect(
            username, text_start_x, text_start_y,
            username_rect_center[0], username_rect_center[1],
            username_rect_width, username_rect_height
        )

        stroke_width = 1
        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw.text((username_pos[0] + dx, username_pos[1] + dy),
                              fitted_username, font=username_font, fill="black")

        draw.text(username_pos, fitted_username,
                  font=username_font, fill="white")

        # Created on and time period
        draw_text_with_stroke(draw, (550, 38), datetime.now().strftime(
            "%B %d, %Y"), font_small, "white", "black", 1)
        draw_text_with_stroke(
            draw, (630, 422), f"{days} days", font_small, "white", "black", 1)

        # Streak
        if streak_stats and streak_stats.get('current_streak', 0) > 0:
            current_streak = streak_stats['current_streak']
            best_streak = streak_stats['best_streak']

            streak_text = f"Current Streak: {format_streak(current_streak)}"
            draw_text_with_stroke(draw, (385, 75), streak_text,
                                  font_large, "white", "black", 1)

            if best_streak > current_streak:
                best_streak_text = f"Best: {format_streak(best_streak)}"
                draw_text_with_stroke(draw, (385, 105), best_streak_text,
                                      font_small, "white", "black", 1)

            if streak_stats.get('is_active_today', False):
                active_text = "Active Today ✓"
                draw_text_with_stroke(draw, (385, 125), active_text,
                                      font_small, "#00FF00", "black", 1)

        else:

            print("  No streak data available")

        if activity_data and len(activity_data) > 0:

            activities_by_type = {
                'playing': [],
                'listening': [],
                'watching': [],
                'streaming': [],
                'competing': [],
                'custom': []
            }

            for activity in activity_data:
                activity_type = activity.get('activity_type', 'unknown')
                if activity_type in activities_by_type:
                    activities_by_type[activity_type].append(activity)

            for activity_type in activities_by_type:
                if sort_by == "sessions":
                    activities_by_type[activity_type].sort(
                        key=lambda x: x.get('session_count', 0), reverse=True)
                else:
                    activities_by_type[activity_type].sort(
                        key=lambda x: x.get('total_duration_seconds', 0), reverse=True)

            print(f"Top activities by type:")
            for activity_type, activities in activities_by_type.items():
                if activities:
                    top_activity = activities[0]

            type_positions = {
                'playing': (35, 155),
                'streaming': (35, 310),
                'listening': (265, 155),
                'watching': (265, 310),
                'competing': (500, 155),
                'custom': (500, 310)
            }

            for activity_type, (x, y) in type_positions.items():
                activities = activities_by_type[activity_type]
                if activities:

                    top_activity = activities[0]
                    display_name = top_activity['display_name'][:20]

                    if sort_by == "sessions":
                        session_count = top_activity.get('session_count', 0)
                        value_text = f"{session_count} session{'s' if session_count != 1 else ''}"
                    else:
                        total_seconds = top_activity.get(
                            'total_duration_seconds', 0)
                        value_text = format_duration(total_seconds)

                    draw_text_with_stroke(
                        draw, (x, y), display_name, font_large, "white", "black", 1)

                    draw_text_with_stroke(
                        draw, (x, y + 40), value_text, font_medium, "white", "black", 1)

        img_bytes = io.BytesIO()
        image.save(img_bytes, format='PNG')
        img_bytes.seek(0)
        return discord.File(img_bytes, filename="user_activity.png")
    except Exception as e:
        print(f"Error generating user activity image: {e}")
        traceback.print_exc()

        image = Image.new('RGB', (800, 600), color='#2F3136')
        draw = ImageDraw.Draw(image)
        font_small, font_medium, font_large, font_larger, font_huge, font_giant = get_fonts()
        draw_text_with_stroke(draw, (200, 250), "ERROR GENERATING IMAGE",
                              font_larger, "white", "black", 2)
        img_bytes = io.BytesIO()
        image.save(img_bytes, format='PNG')
        img_bytes.seek(0)
        return discord.File(img_bytes, filename="user_activity_error.png")


async def generate_server_activity_image(guild: discord.Guild, days: int, activity_data: dict,
                                         role_id: str = None, sort_by: str = "users"):

    try:
        template_path = BASE_DIR / "assets" / "images" / "activity server final png.png"
        image = Image.open(template_path)
        if image.mode != "RGB":
            image = image.convert("RGB")
        draw = ImageDraw.Draw(image)

        font_small, font_medium, font_large, font_larger, font_huge, font_giant = get_fonts()

        avatar_x, avatar_y = 7, 10
        avatar_size = (60, 60)

        try:
            if guild.icon:
                async with aiohttp.ClientSession() as session:
                    async with session.get(guild.icon.url) as response:
                        avatar_data = await response.read()

                avatar_image = Image.open(io.BytesIO(avatar_data))
                avatar_image = avatar_image.resize(
                    avatar_size, Image.Resampling.LANCZOS).convert('RGBA')

                mask = Image.new('L', avatar_size, 0)
                mask_draw = ImageDraw.Draw(mask)
                mask_draw.ellipse(
                    (0, 0, avatar_size[0], avatar_size[1]), fill=255)
                avatar_image.putalpha(mask)

                avatar_area = image.crop(
                    (avatar_x, avatar_y, avatar_x +
                     avatar_size[0], avatar_y + avatar_size[1])
                ).convert('RGBA')

                avatar_with_bg = Image.new('RGBA', avatar_size, (0, 0, 0, 0))
                avatar_with_bg.paste(avatar_image, (0, 0), avatar_image)
                avatar_area.paste(avatar_with_bg, (0, 0), avatar_with_bg)
                image.paste(avatar_area.convert('RGB'), (avatar_x, avatar_y))

        except Exception as e:
            print(f"  ❌ Could not add server icon: {e}")

        # SERVER NAME
        server_name = guild.name
        text_start_x = 85
        text_start_y = 30

        servername_rect_center = (150, 30)
        servername_rect_width = 183
        servername_rect_height = 35

        def fit_text_to_servername_rect(text, start_x, start_y, rect_center_x, rect_center_y, rect_width, rect_height):
            font_sizes = [40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16]

            for font_size in font_sizes:
                try:
                    font = ImageFont.truetype(
                        BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", font_size)
                except:
                    font = ImageFont.load_default()

                bbox = draw.textbbox((0, 0), text, font=font)
                text_width = bbox[2] - bbox[0]
                text_height = bbox[3] - bbox[1]

                text_x = start_x
                text_y = start_y - (text_height // 2)

                vertical_offset = 0
                if font_size <= 40:
                    vertical_offset += 1
                if font_size <= 30:
                    vertical_offset += 1
                if font_size <= 20:
                    vertical_offset += 1
                if font_size <= 16:
                    vertical_offset += 1
                text_y += vertical_offset

                rect_left = rect_center_x - (rect_width // 2)
                rect_right = rect_center_x + (rect_width // 2)
                rect_top = rect_center_y - (rect_height // 2)
                rect_bottom = rect_center_y + (rect_height // 2)

                fits_h = (text_x >= rect_left - 1) and (text_x +
                                                        text_width <= rect_right + 1)
                fits_v = (text_y >= rect_top - 1) and (text_y +
                                                       text_height <= rect_bottom + 1)

                if fits_h and fits_v:
                    return text, font, (text_x, text_y)

            smallest_font = 16
            try:
                font = ImageFont.truetype(
                    BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", smallest_font)
            except:
                font = ImageFont.load_default()

            current_text = server_name
            while len(current_text) > 3:
                current_text = current_text[:-4] + "..."
                bbox = draw.textbbox((0, 0), current_text, font=font)
                text_width = bbox[2] - bbox[0]

                text_x = text_start_x
                bbox = draw.textbbox((0, 0), current_text, font=font)
                text_height = bbox[3] - bbox[1]
                text_y = text_start_y - (text_height // 2)

                if text_x + text_width <= rect_right:
                    return current_text, font, (text_x, text_y)

            return server_name[:3] + "...", font, (text_start_x, text_start_y)

        fitted_servername, servername_font, servername_pos = fit_text_to_servername_rect(
            server_name, text_start_x, text_start_y,
            servername_rect_center[0], servername_rect_center[1],
            servername_rect_width, servername_rect_height
        )

        stroke_width = 1
        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw.text((servername_pos[0] + dx, servername_pos[1] + dy),
                              fitted_servername, font=servername_font, fill="black")

        draw.text(servername_pos, fitted_servername,
                  font=servername_font, fill="white")

        # ROLE FILTER
        role_text = "No Filter"
        if role_id and role_id != "none":
            role = guild.get_role(int(role_id))
            role_text = role.name if role else "Unknown Role"

        draw_text_with_stroke(draw, (65, 459), role_text,
                              font_small, "white", "black", 1)

        # Created on and time period
        draw_text_with_stroke(draw, (550, 38), datetime.now().strftime(
            "%B %d, %Y"), font_small, "white", "black", 1)
        draw_text_with_stroke(
            draw, (630, 457), f"{days} days", font_small, "white", "black", 1)

        if activity_data:
            activities_by_type = {}
            for activity in activity_data:
                activity_type = activity.get('activity_type', 'unknown')
                if activity_type not in activities_by_type:
                    activities_by_type[activity_type] = []
                activities_by_type[activity_type].append(activity)

            for activity_type in activities_by_type:
                if sort_by == "users":
                    activities_by_type[activity_type].sort(
                        key=lambda x: x.get('user_count', 0), reverse=True)
                elif sort_by == "sessions":
                    activities_by_type[activity_type].sort(
                        key=lambda x: x.get('session_count', 0), reverse=True)
                else:
                    activities_by_type[activity_type].sort(
                        key=lambda x: x.get('total_duration_seconds', 0), reverse=True)

            # ACTIVITY POSITIONS
            activity_positions = {
                'playing': {
                    'name': (20, 135),
                    'value': (20, 180)
                },
                'streaming': {
                    'name': (20, 330),
                    'value': (20, 360)
                },
                'listening': {
                    'name': (265, 135),
                    'value': (265, 180)
                },
                'watching': {
                    'name': (265, 330),
                    'value': (265, 360)
                },
                'competing': {
                    'name': (512, 135),
                    'value': (512, 180)
                },
                'custom': {
                    'name': (512, 330),
                    'value': (512, 360)
                }
            }

            # INVISIBLE RECTANGLES
            invisible_rectangles = [

                ((130, 135), 220, 32),
                ((130, 180), 220, 32),
                ((130, 330), 220, 32),
                ((130, 360), 220, 32),


                ((370, 135), 220, 32),
                ((370, 180), 220, 32),
                ((370, 330), 220, 32),
                ((370, 360), 220, 32),

                ((620, 135), 220, 32),
                ((620, 180), 220, 32),
                ((620, 330), 220, 32),
                ((620, 360), 220, 32)
            ]

            for activity_type, positions in activity_positions.items():
                if activity_type in activities_by_type and activities_by_type[activity_type]:

                    top_activity = activities_by_type[activity_type][0]

                    display_name = top_activity['display_name'][:20]
                    name_x, name_y = positions['name']

                    if sort_by == "users":
                        value = f"{top_activity.get('user_count', 0)} users"
                    elif sort_by == "sessions":
                        value = f"{top_activity.get('session_count', 0)} sessions"
                    else:
                        value = format_duration(top_activity.get(
                            'total_duration_seconds', 0))

                    value_x, value_y = positions['value']

                    def fit_text_to_rect(text, target_x, target_y, rect_center_x, rect_center_y, rect_width, rect_height, is_left_aligned=True):
                        font_sizes = [24, 22, 20, 18, 16]

                        for font_size in font_sizes:
                            try:
                                font = ImageFont.truetype(
                                    BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", font_size)
                            except:
                                font = ImageFont.load_default()

                            bbox = draw.textbbox((0, 0), text, font=font)
                            text_width = bbox[2] - bbox[0]
                            text_height = bbox[3] - bbox[1]

                            if is_left_aligned:
                                text_x = target_x
                            else:
                                text_x = target_x - (text_width // 2)

                            text_y = target_y - (text_height // 2)

                            rect_left = rect_center_x - (rect_width // 2)
                            rect_right = rect_center_x + (rect_width // 2)
                            rect_top = rect_center_y - (rect_height // 2)
                            rect_bottom = rect_center_y + (rect_height // 2)

                            fits_h = (text_x >= rect_left - 1) and (text_x +
                                                                    text_width <= rect_right + 1)
                            fits_v = (text_y >= rect_top - 1) and (text_y +
                                                                   text_height <= rect_bottom + 1)

                            if fits_h and fits_v:
                                return text, font, (text_x, text_y)

                        smallest_font = 16
                        try:
                            font = ImageFont.truetype(
                                BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", smallest_font)
                        except:
                            font = ImageFont.load_default()

                        current_text = text
                        while len(current_text) > 3:
                            current_text = current_text[:-4] + "..."
                            bbox = draw.textbbox(
                                (0, 0), current_text, font=font)
                            text_width = bbox[2] - bbox[0]

                            if is_left_aligned:
                                text_x = target_x
                            else:
                                text_x = target_x - (text_width // 2)

                            bbox = draw.textbbox(
                                (0, 0), current_text, font=font)
                            text_height = bbox[3] - bbox[1]
                            text_y = target_y - (text_height // 2)

                            if text_x + text_width <= rect_right:
                                return current_text, font, (text_x, text_y)

                        return text[:3] + "...", font, (target_x, target_y)

                    rect_index = list(activity_positions.keys()
                                      ).index(activity_type) * 2
                    name_rect_center, name_rect_width, name_rect_height = invisible_rectangles[
                        rect_index]
                    value_rect_center, value_rect_width, value_rect_height = invisible_rectangles[
                        rect_index + 1]

                    fitted_name, name_font, name_pos = fit_text_to_rect(
                        display_name, name_x, name_y,
                        name_rect_center[0], name_rect_center[1],
                        name_rect_width, name_rect_height,
                        is_left_aligned=True
                    )

                    fitted_value, value_font, value_pos = fit_text_to_rect(
                        value, value_x, value_y,
                        value_rect_center[0], value_rect_center[1],
                        value_rect_width, value_rect_height,
                        is_left_aligned=True
                    )

                    stroke_width = 1
                    for dx in [-stroke_width, 0, stroke_width]:
                        for dy in [-stroke_width, 0, stroke_width]:
                            if dx != 0 or dy != 0:
                                draw.text((name_pos[0] + dx, name_pos[1] + dy),
                                          fitted_name, font=name_font, fill="black")

                    draw.text(name_pos, fitted_name,
                              font=name_font, fill="white")

                    for dx in [-stroke_width, 0, stroke_width]:
                        for dy in [-stroke_width, 0, stroke_width]:
                            if dx != 0 or dy != 0:
                                draw.text((value_pos[0] + dx, value_pos[1] + dy),
                                          fitted_value, font=value_font, fill="black")

                    draw.text(value_pos, fitted_value,
                              font=value_font, fill="white")

        img_bytes = io.BytesIO()
        image.save(img_bytes, format='PNG')
        img_bytes.seek(0)
        return discord.File(img_bytes, filename="server_activity.png")
    except Exception as e:
        logger.error(f"Error generating server activity image: {e}")
        raise


async def generate_activity_leaderboard_image(guild: discord.Guild, leaderboard_data: list,
                                              category: str, days_back: int, role_id: str = None,
                                              sort_by: str = "users", page: int = 0):

    try:
        template_path = BASE_DIR / "assets" / "images" / "leaderboards final png.png"
        image = Image.open(template_path)
    except FileNotFoundError:
        try:
            fallback_path = BASE_DIR / "assets" / "images" / "leaderboards final png.png"
            image = Image.open(fallback_path)
        except FileNotFoundError:
            image = Image.new('RGB', (800, 600), color='#2F3136')

    if image.mode != "RGB":
        image = image.convert("RGB")

    draw = ImageDraw.Draw(image)
    font_small, font_medium, font_large, font_larger, font_huge, font_giant = get_fonts()

    # SERVER PROFILE PICTURE
    avatar_x, avatar_y = 7, 10
    avatar_size = (60, 60)

    try:
        if guild.icon:
            async with aiohttp.ClientSession() as session:
                async with session.get(guild.icon.url) as response:
                    avatar_data = await response.read()

            avatar_image = Image.open(io.BytesIO(avatar_data))
            avatar_image = avatar_image.resize(
                avatar_size, Image.Resampling.LANCZOS).convert('RGBA')

            mask = Image.new('L', avatar_size, 0)
            mask_draw = ImageDraw.Draw(mask)
            mask_draw.ellipse((0, 0, avatar_size[0], avatar_size[1]), fill=255)
            avatar_image.putalpha(mask)

            avatar_area = image.crop(
                (avatar_x, avatar_y, avatar_x +
                 avatar_size[0], avatar_y + avatar_size[1])
            ).convert('RGBA')

            avatar_with_bg = Image.new('RGBA', avatar_size, (0, 0, 0, 0))
            avatar_with_bg.paste(avatar_image, (0, 0), avatar_image)
            avatar_area.paste(avatar_with_bg, (0, 0), avatar_with_bg)
            image.paste(avatar_area.convert('RGB'), (avatar_x, avatar_y))

    except Exception as e:
        print(f"  ❌ Could not add server icon: {e}")

    # SERVER NAME
    server_name = guild.name
    text_start_x = 85
    text_start_y = 30

    servername_rect_center = (150, 30)
    servername_rect_width = 183
    servername_rect_height = 35

    def fit_text_to_servername_rect(text, start_x, start_y, rect_center_x, rect_center_y, rect_width, rect_height):
        font_sizes = [40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16]

        for font_size in font_sizes:
            try:
                font = ImageFont.truetype(
                    BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", font_size)
            except:
                font = ImageFont.load_default()

            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            text_x = start_x
            text_y = start_y - (text_height // 2)

            vertical_offset = 0
            if font_size <= 40:
                vertical_offset += 1
            if font_size <= 30:
                vertical_offset += 1
            if font_size <= 20:
                vertical_offset += 1
            if font_size <= 16:
                vertical_offset += 1
            text_y += vertical_offset

            rect_left = rect_center_x - (rect_width // 2)
            rect_right = rect_center_x + (rect_width // 2)
            rect_top = rect_center_y - (rect_height // 2)
            rect_bottom = rect_center_y + (rect_height // 2)

            fits_h = (text_x >= rect_left - 1) and (text_x +
                                                    text_width <= rect_right + 1)
            fits_v = (text_y >= rect_top - 1) and (text_y +
                                                   text_height <= rect_bottom + 1)

            if fits_h and fits_v:
                return text, font, (text_x, text_y)

        smallest_font = 16
        try:
            font = ImageFont.truetype(
                BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", smallest_font)
        except:
            font = ImageFont.load_default()

        current_text = server_name
        while len(current_text) > 3:
            current_text = current_text[:-4] + "..."
            bbox = draw.textbbox((0, 0), current_text, font=font)
            text_width = bbox[2] - bbox[0]

            text_x = text_start_x
            bbox = draw.textbbox((0, 0), current_text, font=font)
            text_height = bbox[3] - bbox[1]
            text_y = text_start_y - (text_height // 2)

            if text_x + text_width <= rect_right:
                return current_text, font, (text_x, text_y)

        return server_name[:3] + "...", font, (text_start_x, text_start_y)

    fitted_servername, servername_font, servername_pos = fit_text_to_servername_rect(
        server_name, text_start_x, text_start_y,
        servername_rect_center[0], servername_rect_center[1],
        servername_rect_width, servername_rect_height
    )

    stroke_width = 1
    for dx in [-stroke_width, 0, stroke_width]:
        for dy in [-stroke_width, 0, stroke_width]:
            if dx != 0 or dy != 0:
                draw.text((servername_pos[0] + dx, servername_pos[1] + dy),
                          fitted_servername, font=servername_font, fill="black")

    draw.text(servername_pos, fitted_servername,
              font=servername_font, fill="white")

    # Role filter / date / time range
    role_text = "No Filter"
    if role_id and role_id != "none":
        role = guild.get_role(int(role_id))
        role_text = role.name if role else "Unknown Role"
    draw_text_with_stroke(draw, (70, 424), role_text,
                          font_small, "white", "black", 1)

    draw_text_with_stroke(
        draw, (630, 422), f"{days_back} days", font_small, "white", "black", 1)
    draw_text_with_stroke(draw, (550, 38), datetime.now().strftime(
        "%B %d, %Y"), font_small, "white", "black", 1)

    total_items = len(leaderboard_data)
    total_pages = max(1, (total_items + USERS_PER_PAGE - 1) // USERS_PER_PAGE)

    if page >= total_pages:
        page = total_pages - 1
    if page < 0:
        page = 0

    draw_text_with_stroke(
        draw, (400, 450), f"Page {page + 1}/{total_pages}", font_medium, "white", "black", 1)

    if leaderboard_data:
        start_idx = page * USERS_PER_PAGE
        end_idx = min(start_idx + USERS_PER_PAGE, len(leaderboard_data))
        page_data = leaderboard_data[start_idx:end_idx]

        image_width, _ = image.size

        if len(page_data) <= 4:
            positions = [(60, 100 + i * 65) for i in range(len(page_data))]
            box_width, box_height = 600, 40
        else:
            box_width, box_height = 280, 40
            box_spacing = 65
            start_y = 100
            total_box_width = (2 * box_width) + 15
            start_x = (image_width - total_box_width) // 2
            positions = [
                (start_x + (i % 2) * (box_width + 15),
                 start_y + (i // 2) * box_spacing)
                for i in range(len(page_data))
            ]

        for i, (activity_data, (box_x, box_y)) in enumerate(zip(page_data, positions)):
            global_rank = start_idx + i + 1

            draw_rounded_rectangle(draw, [box_x, box_y, box_x + box_width, box_y + box_height],
                                   radius=8, fill=(0, 0, 0, 220), outline=None)

            placement_width = 35

            if global_rank == 1:
                placement_color = (255, 215, 0, 220)  # Gold
            elif global_rank == 2:
                placement_color = (192, 192, 192, 220)  # Silver
            elif global_rank == 3:
                placement_color = (205, 127, 50, 220)  # Bronze
            else:
                placement_color = (93, 0, 136, 255)  # Purple

            draw_rounded_rectangle(draw, [box_x, box_y, box_x + placement_width, box_y + box_height],
                                   radius=8, fill=placement_color, outline=None)

            display_name = activity_data['display_name'][:20]

            if sort_by == "users":
                value_text = f"{format_user_count(activity_data['user_count'])} users"
            elif sort_by == "sessions":
                value_text = f"{format_user_count(activity_data['session_count'])} sessions"
            else:
                total_seconds = activity_data['total_duration_seconds'] or 0
                value_text = format_duration(total_seconds)

            rank_text = f"#{global_rank}"
            rank_bbox = draw.textbbox((0, 0), rank_text, font=font_medium)
            rank_width = rank_bbox[2] - rank_bbox[0]
            value_bbox = draw.textbbox((0, 0), value_text, font=font_medium)
            value_width = value_bbox[2] - value_bbox[0]

            if len(display_name) > 20:
                display_name = display_name[:17] + "..."
            draw_text_with_stroke(draw, (box_x + placement_width + 8, box_y + 10),
                                  display_name, font_medium, "white", "black", 1)

            rank_height = rank_bbox[3] - rank_bbox[1]
            value_height = value_bbox[3] - value_bbox[1]
            rank_y = box_y + (box_height - rank_height) // 2
            value_y = box_y + (box_height - value_height) // 2

            rank_x = box_x + (placement_width - rank_width) // 2
            draw_text_with_stroke(draw, (rank_x, rank_y),
                                  rank_text, font_medium, "white", "black", 1)

            value_x = box_x + box_width - value_width - 8
            draw_text_with_stroke(draw, (value_x, value_y),
                                  value_text, font_medium, "white", "black", 1)
    else:

        cx, cy = image.size[0] // 2, image.size[1] // 2
        draw_text_with_stroke(draw, (cx - 150, cy - 15),
                              "NO DATA AVAILABLE", font_larger, "white", "black", 1)

    img_bytes = io.BytesIO()
    image.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    return discord.File(img_bytes, filename="activity_leaderboard.png")


# COMMANDS

activity_group = app_commands.Group(
    name="activity", description="Activity tracking commands"
)


@activity_group.command(name="user", description="View a user's activity statistics")
@app_commands.describe(user="The user to check activity for")
async def activity_user_command(interaction: discord.Interaction, user: discord.Member):
    await interaction.response.defer()
    cog = interaction.client.get_cog('ActivityTracker')
    if not cog:
        await interaction.followup.send("❌ Activity tracker is not loaded.", ephemeral=True)
        return

    view = ActivityView(cog, interaction.guild, "user", 14,
                        "none", user, "all", "time", 0)

    stats = await cog.db.get_user_activity_stats(
        interaction.guild.id,
        user.id,
        14,
        None,
        "time"
    )

    streak_stats = await cog.db.get_user_streak_stats(
        interaction.guild.id,
        user.id
    )

    logger.info(
        f"Initial user command for {user.display_name}: {len(stats)} activities found"
    )

    file = await generate_user_activity_image(
        interaction.guild,
        user,
        14,
        stats,
        streak_stats,
        "time"
    )

    await interaction.followup.send(file=file, view=view)


@activity_group.command(name="server", description="View server activity statistics")
async def activity_server_command(interaction: discord.Interaction):
    await interaction.response.defer()
    cog = interaction.client.get_cog('ActivityTracker')
    if not cog:
        await interaction.followup.send("❌ Activity tracker is not loaded.", ephemeral=True)
        return

    view = ActivityView(cog, interaction.guild, "server",
                        14, "none", None, "all", "users", 0)
    stats = await cog.db.get_server_activity_stats(interaction.guild.id, 14, None, "users")
    file = await generate_server_activity_image(interaction.guild, 14, stats, "none", "users")
    await interaction.followup.send(file=file, view=view)


@activity_group.command(name="leaderboard", description="Show activity leaderboards")
async def activity_leaderboard_command(interaction: discord.Interaction):
    await interaction.response.defer()
    cog = interaction.client.get_cog('ActivityTracker')
    if not cog:
        await interaction.followup.send("❌ Activity tracker is not loaded.", ephemeral=True)
        return

    view = ActivityView(cog, interaction.guild, "leaderboard",
                        14, "none", None, "all", "users", 0)
    stats = await cog.db.get_leaderboard_stats(interaction.guild.id, "all", 14, None, "users")
    file = await generate_activity_leaderboard_image(interaction.guild, stats, "all", 14, "none", "users", 0)
    await interaction.followup.send(file=file, view=view)


@activity_group.command(name="whos", description="See who's currently doing an activity")
@app_commands.describe(
    activity_type="Type of activity",
    activity_name="Name of the activity"
)
@app_commands.choices(activity_type=[
    app_commands.Choice(name="Playing", value="playing"),
    app_commands.Choice(name="Streaming", value="streaming"),
    app_commands.Choice(name="Listening", value="listening"),
    app_commands.Choice(name="Watching", value="watching"),
    app_commands.Choice(name="Competing", value="competing"),
    app_commands.Choice(name="Custom", value="custom")
])
async def whos_activity_command(interaction: discord.Interaction, activity_type: str, activity_name: str):
    await interaction.response.defer()
    cog = interaction.client.get_cog('ActivityTracker')
    if not cog:
        await interaction.followup.send("❌ Activity tracker is not loaded.", ephemeral=True)
        return

    view = WhosView(cog, interaction.guild, activity_type, activity_name, 0)

    current_time = datetime.utcnow()
    current_sessions = []

    for user_id, sessions in cog.active_sessions.items():
        for session_key, session_data in sessions.items():
            if (session_data['display_name'].lower() == activity_name.lower() and
                session_data['activity_type'] == activity_type and
                    session_data['guild_id'] == interaction.guild.id):
                member = interaction.guild.get_member(user_id)
                if member:
                    duration = current_time - session_data['start_time']
                    current_sessions.append((member, duration))

    current_sessions.sort(key=lambda x: x[1], reverse=True)

    total_sessions = len(current_sessions)
    total_pages = max(
        1, (total_sessions + view.users_per_page - 1) // view.users_per_page)

    page_sessions = current_sessions[:view.users_per_page]

    embed = discord.Embed(
        title=f"👀 Who's {activity_type.title()} {activity_name}",
        color=discord.Color.green(),
        timestamp=datetime.utcnow()
    )

    if page_sessions:
        description = ""
        for i, (member, duration) in enumerate(page_sessions, 1):
            hours, remainder = divmod(int(duration.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            time_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m {seconds}s"
            description += f"{i}. {member.mention} - **{time_str}**\n"

        embed.description = description
        embed.set_footer(
            text=f"Page 1/{total_pages} • Total: {total_sessions} users • Updated")
    else:
        embed.description = "No one is currently doing this activity."
        embed.color = discord.Color.orange()
        embed.set_footer(text="No users found")

    await interaction.followup.send(embed=embed, view=view)


# SETUP

async def setup(bot):
    bot.tree.add_command(activity_group)
    await bot.add_cog(ActivityTracker(bot))
