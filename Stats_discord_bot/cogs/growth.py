import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import discord
from discord.ext import commands
from discord import app_commands
import asyncpg
import asyncio
from datetime import datetime, timedelta, date
import io
import os
from typing import Optional, Dict, List, Tuple
from dotenv import load_dotenv
import aiohttp
import traceback
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from pilmoji import Pilmoji
import matplotlib
matplotlib.use('Agg')


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


# DRAWING FUNCTIONS

def draw_text_with_stroke(draw, position, text, font, text_color, stroke_color, stroke_width):

    x, y = position
    for dx in range(-stroke_width, stroke_width + 1):
        for dy in range(-stroke_width, stroke_width + 1):
            if dx != 0 or dy != 0:
                draw.text((x + dx, y + dy), text, font=font, fill=stroke_color)

    draw.text((x, y), text, font=font, fill=text_color)


def draw_text_with_stroke_and_emoji(pilmoji_draw, position, text, font, text_color, stroke_color, stroke_width):

    x, y = position

    temp_draw = pilmoji_draw._draw

    for dx in range(-stroke_width, stroke_width + 1):
        for dy in range(-stroke_width, stroke_width + 1):
            if dx != 0 or dy != 0:
                pilmoji_draw.text((x + dx, y + dy), text, font=font,
                                  fill=stroke_color, emoji_position_offset=(0, 0))

    pilmoji_draw.text((x, y), text, font=font, fill=text_color,
                      emoji_position_offset=(0, 0))


# TIME SETTINGS MODAL

class GrowthTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, view):
        super().__init__(timeout=300)
        self.view = view

    days = discord.ui.TextInput(
        label='Enter number of days',
        placeholder='e.g., 7, 14, 30, 90, 365, 2000...',
        min_length=1,
        max_length=4,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days = int(self.days.value)
            if days <= 0 or days > 2000:
                await interaction.response.send_message("❌ Please enter a number between 1 and 2000 days.", ephemeral=True)
                return

            await interaction.response.defer()

            self.view.current_days = days
            self.view.show_time_buttons = False

            await self.view.update_chart(interaction)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            print(f"Error in growth modal submit: {e}")
            await interaction.followup.send("❌ An error occurred while generating chart preview.", ephemeral=True)


# GROWTH DROPDOWN MENU

class GrowthSelectMenu(discord.ui.Select):
    def __init__(self, current_selection: str):
        options = [
            discord.SelectOption(
                label="Joins and Leaves",
                value="joins_leaves",
                description="Track server joins and leaves",
                emoji="📈",
                default=(current_selection == "joins_leaves")
            ),
            discord.SelectOption(
                label="Projected Growth",
                value="projected",
                description="Project future server growth",
                emoji="🔮",
                default=(current_selection == "projected")
            )
        ]

        super().__init__(
            placeholder="Select growth type...",
            options=options,
            custom_id="growth_type_select",
            min_values=1,
            max_values=1,
            row=0
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        view.growth_type = self.values[0]
        view.current_selection = self.values[0]
        await interaction.response.defer()
        await view.update_chart(interaction)


# GROWTH VIEW

class GrowthView(discord.ui.View):
    def __init__(self, cog, guild_id: int, days: int = 14, growth_type: str = "joins_leaves"):
        super().__init__(timeout=600)
        self.cog = cog
        self.guild_id = guild_id
        self.guild = cog.bot.get_guild(guild_id)
        self.current_days = days
        self.growth_type = growth_type
        self.current_selection = growth_type
        self.show_time_buttons = False
        self.message_id = None

        self._update_buttons()

    def get_time_label(self, days: int = None) -> str:

        if days is None:
            days = self.current_days
        if self.growth_type == "joins_leaves":
            return f"Last {days} Days"
        else:
            return f"Next {days} Days"

    def _update_buttons(self):
        self.clear_items()

        growth_select = GrowthSelectMenu(self.current_selection)
        growth_select.row = 0
        self.add_item(growth_select)

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="growth_refresh",
            row=1
        )
        refresh_button.callback = self.refresh_callback
        self.add_item(refresh_button)

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="growth_time_settings",
            row=1
        )
        time_settings_button.callback = self.time_settings_callback
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            days_7_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 7 else discord.ButtonStyle.secondary,
                label="7 Days",
                custom_id="growth_days_7",
                row=2
            )
            days_7_button.callback = self.days_7_callback
            self.add_item(days_7_button)

            days_14_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 14 else discord.ButtonStyle.secondary,
                label="14 Days",
                custom_id="growth_days_14",
                row=2
            )
            days_14_button.callback = self.days_14_callback
            self.add_item(days_14_button)

            days_30_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 30 else discord.ButtonStyle.secondary,
                label="30 Days",
                custom_id="growth_days_30",
                row=2
            )
            days_30_button.callback = self.days_30_callback
            self.add_item(days_30_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label="Custom",
                custom_id="growth_custom_days",
                row=2
            )
            custom_button.callback = self.custom_days_callback
            self.add_item(custom_button)

    # BUTTON CALLBACKS

    async def refresh_callback(self, interaction: discord.Interaction):

        await self.handle_button_click(interaction, refresh=True)

    async def time_settings_callback(self, interaction: discord.Interaction):

        await self.handle_time_settings(interaction)

    async def days_7_callback(self, interaction: discord.Interaction):

        await self.handle_button_click(interaction, days=7)

    async def days_14_callback(self, interaction: discord.Interaction):

        await self.handle_button_click(interaction, days=14)

    async def days_30_callback(self, interaction: discord.Interaction):

        await self.handle_button_click(interaction, days=30)

    async def custom_days_callback(self, interaction: discord.Interaction):

        if not self.guild:
            await interaction.response.send_message("❌ Could not find guild information.", ephemeral=True)
            return

        modal = GrowthTimeModal(self)
        await interaction.response.send_modal(modal)

    async def handle_button_click(self, interaction: discord.Interaction, refresh: bool = False, days: int = None):

        try:
            await interaction.response.defer()

            if days:
                self.current_days = days
                self.show_time_buttons = False

            await self.update_chart(interaction)

        except Exception as e:
            print(f"Error handling button click: {e}")
            try:
                await interaction.followup.send("❌ An error occurred while updating the chart.", ephemeral=True)
            except:
                pass

    async def handle_time_settings(self, interaction: discord.Interaction):

        try:
            self.show_time_buttons = not self.show_time_buttons
            self._update_buttons()

            await interaction.response.edit_message(view=self)

        except Exception as e:
            print(f"Error handling time settings: {e}")
            try:
                await interaction.response.send_message("❌ An error occurred while updating time settings.", ephemeral=True)
            except:
                pass

    async def update_chart(self, interaction: discord.Interaction):

        try:
            data = await self.fetch_growth_data()

            if not data:
                await self._safe_edit_response(interaction, content="❌ No growth data available", view=self)
                return

            chart_img, timestamps, y_labels = self.cog.generate_growth_chart(
                data, self.current_days, self.growth_type)

            label = self.create_label(data, self.current_days)

            final_image_bytes = await self.cog.draw_growth_on_template(
                chart_img, label, self.current_days,
                timestamps, y_labels, self.current_selection, self.growth_type, self.guild_id)

            file = discord.File(final_image_bytes, filename='growth_chart.png')

            self._update_buttons()

            await self._safe_edit_response(interaction, content=None, attachments=[file], view=self)

        except Exception as e:
            print(f"Error updating growth chart: {e}")
            traceback.print_exc()
            try:
                await self._safe_edit_response(interaction, content="❌ Error generating growth chart", view=self)
            except:
                pass

    async def _safe_edit_response(self, interaction: discord.Interaction, **kwargs):

        try:
            await interaction.edit_original_response(**kwargs)
        except discord.NotFound:
            try:
                await interaction.followup.send(**kwargs, ephemeral=True)
            except:
                pass
        except Exception as e:
            print(f"Error in _safe_edit_response: {e}")

    def create_label(self, data: Dict, days: int) -> str:

        total_joins = data.get('total_joins', 0)
        total_leaves = data.get('total_leaves', 0)
        net_growth = total_joins - total_leaves

        if self.growth_type == "joins_leaves":
            return f"Joins: {total_joins} | Leaves: {total_leaves} | Net: {net_growth}"
        else:
            return f"Projected Joins: {total_joins} | Leaves: {total_leaves} | Net: {net_growth}"

    async def fetch_growth_data(self) -> Dict:

        if self.growth_type == "joins_leaves":
            return await self.cog.fetch_historical_growth(self.guild_id, self.current_days)
        else:
            return await self.cog.fetch_projected_growth(self.guild_id, self.current_days)


# INITIALIZATION

class GrowthSystem(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.pool = None
        self.tables_created = False

    async def cog_load(self):

        try:
            self.pool = await asyncpg.create_pool(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', 5432),
                database=os.getenv('DB_NAME', 'discord_bot'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', ''),
                min_size=5,
                max_size=20
            )
            print("✅ GrowthSystem: Database pool connected")

            await self.create_tables()

            await self.test_database_connection()

        except Exception as e:
            print(f"❌ GrowthSystem: Database connection failed: {e}")
            traceback.print_exc()

    async def test_database_connection(self):

        try:
            async with self.pool.acquire() as conn:

                tables = await conn.fetch("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('member_joins', 'member_leaves', 'member_growth_stats')
                """)
                print(
                    f"✅ GrowthSystem: Found tables: {[row['table_name'] for row in tables]}")

                test_guild_id = 123456789
                await conn.execute("""
                    INSERT INTO member_growth_stats (guild_id, date, joins_count, leaves_count, net_growth)
                    VALUES ($1, $2, 0, 0, 0)
                    ON CONFLICT (guild_id, date) DO NOTHING
                """, test_guild_id, datetime.utcnow().date())
                print("✅ GrowthSystem: Database test successful")

        except Exception as e:
            print(f"❌ GrowthSystem: Database test failed: {e}")
            traceback.print_exc()

    async def cog_unload(self):

        if self.pool:
            await self.pool.close()
            print("✅ GrowthSystem: Database pool closed")

    # TABLE CREATION METHODS

    async def create_tables(self):

        if self.tables_created:
            return

        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS member_joins (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        join_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(guild_id, user_id, join_time)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_member_joins_guild_time 
                    ON member_joins(guild_id, join_time);
                    
                    CREATE INDEX IF NOT EXISTS idx_member_joins_guild_user 
                    ON member_joins(guild_id, user_id);
                """)
                print("✅ GrowthSystem: Created/verified member_joins table")

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS member_leaves (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        user_id BIGINT NOT NULL,
                        leave_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(guild_id, user_id, leave_time)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_member_leaves_guild_time 
                    ON member_leaves(guild_id, leave_time);
                    
                    CREATE INDEX IF NOT EXISTS idx_member_leaves_guild_user 
                    ON member_leaves(guild_id, user_id);
                """)
                print("✅ GrowthSystem: Created/verified member_leaves table")

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS member_growth_stats (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        date DATE NOT NULL,
                        joins_count INTEGER DEFAULT 0,
                        leaves_count INTEGER DEFAULT 0,
                        net_growth INTEGER DEFAULT 0,
                        UNIQUE(guild_id, date)
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_growth_stats_guild_date 
                    ON member_growth_stats(guild_id, date);
                    
                    CREATE INDEX IF NOT EXISTS idx_growth_stats_date 
                    ON member_growth_stats(date);
                """)
                print("✅ GrowthSystem: Created/verified member_growth_stats table")

                self.tables_created = True
                print("✅ GrowthSystem: All database tables created/verified")

        except Exception as e:
            print(f"❌ GrowthSystem: Error creating tables: {e}")
            traceback.print_exc()

    # LISTENING EVENTS

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):

        try:
            print(
                f"📥 GrowthSystem: Member join detected - {member.name} (ID: {member.id}) in guild {member.guild.name} (ID: {member.guild.id})")

            current_utc = datetime.utcnow()
            current_timestamp = current_utc.timestamp()

            if not self.pool:
                print("❌ GrowthSystem: Database pool not initialized for member join")
                return

            async with self.pool.acquire() as conn:
                db_time = await conn.fetchval("SELECT NOW();")

                result = await conn.execute("""
                    INSERT INTO member_joins (guild_id, user_id, join_time)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (guild_id, user_id, join_time) DO NOTHING
                    RETURNING id, join_time;
                """, member.guild.id, member.id, current_utc)

                if "INSERT" in result:
                    inserted = await conn.fetchrow("""
                        SELECT id, join_time, DATE(join_time) as join_date
                        FROM member_joins 
                        WHERE guild_id = $1 AND user_id = $2
                        ORDER BY join_time DESC 
                        LIMIT 1;
                    """, member.guild.id, member.id)

                else:
                    print(
                        f"⚠️ GrowthSystem: Join already recorded for {member.name}")

                today = current_utc.date()

                update_result = await conn.execute("""
                    INSERT INTO member_growth_stats (guild_id, date, joins_count, leaves_count, net_growth)
                    VALUES ($1, $2, 1, 0, 1)
                    ON CONFLICT (guild_id, date) 
                    DO UPDATE SET 
                        joins_count = member_growth_stats.joins_count + 1,
                        net_growth = member_growth_stats.net_growth + 1
                    RETURNING joins_count;
                """, member.guild.id, today)

        except Exception as e:
            print(f"❌ GrowthSystem: Error tracking member join: {e}")
            traceback.print_exc()

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):

        try:

            current_utc = datetime.utcnow()

            if not self.pool:
                print("❌ GrowthSystem: Database pool not initialized for member leave")
                return

            async with self.pool.acquire() as conn:
                result = await conn.execute("""
                    INSERT INTO member_leaves (guild_id, user_id, leave_time)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (guild_id, user_id, leave_time) DO NOTHING
                    RETURNING id;
                """, member.guild.id, member.id, current_utc)

                today = current_utc.date()

                update_result = await conn.execute("""
                    INSERT INTO member_growth_stats (guild_id, date, joins_count, leaves_count, net_growth)
                    VALUES ($1, $2, 0, 1, -1)
                    ON CONFLICT (guild_id, date) 
                    DO UPDATE SET 
                        leaves_count = member_growth_stats.leaves_count + 1,
                        net_growth = member_growth_stats.net_growth - 1
                    RETURNING leaves_count;
                """, member.guild.id, today)

        except Exception as e:
            print(f"❌ GrowthSystem: Error tracking member leave: {e}")
            traceback.print_exc()

    # QUERY METHODS

    async def fetch_historical_growth(self, guild_id: int, days: int = 14) -> Dict:

        print(
            f"📊 GrowthSystem: Fetching historical growth for guild {guild_id}, {days} days")

        current_utc = datetime.utcnow()

        try:
            if not self.pool:
                print("❌ GrowthSystem: Database pool not available")
                return self._get_empty_growth_data()

            async with self.pool.acquire() as conn:

                start_date = current_utc.date() - timedelta(days=days - 1)
                print(
                    f"📊 GrowthSystem: Querying from {start_date} to {current_utc.date()} (inclusive, {days} days)")

                joins_points = []
                leaves_points = []

                for day_offset in range(days):
                    current_date = start_date + timedelta(days=day_offset)

                    daily_joins = await conn.fetchval("""
                        SELECT COUNT(*) 
                        FROM member_joins 
                        WHERE guild_id = $1 
                        AND DATE(join_time) = $2;
                    """, guild_id, current_date) or 0

                    daily_leaves = await conn.fetchval("""
                        SELECT COUNT(*) 
                        FROM member_leaves 
                        WHERE guild_id = $1 
                        AND DATE(leave_time) = $2;
                    """, guild_id, current_date) or 0

                    joins_points.append(
                        {'date': current_date, 'count': daily_joins})
                    leaves_points.append(
                        {'date': current_date, 'count': daily_leaves})

                total_joins = sum(p['count'] for p in joins_points)
                total_leaves = sum(p['count'] for p in leaves_points)

                max_daily_joins = max([p['count']
                                       for p in joins_points], default=0)
                max_daily_leaves = max([p['count']
                                        for p in leaves_points], default=0)
                max_daily_value = max(max_daily_joins, max_daily_leaves, 1)

                result = {
                    'joins': joins_points,
                    'leaves': leaves_points,
                    'total_joins': total_joins,
                    'total_leaves': total_leaves,
                    'max_daily_value': max_daily_value,
                    'max_total_value': max(total_joins, total_leaves, 1)
                }

                return result

        except Exception as e:
            print(f"❌ GrowthSystem: Error fetching historical growth: {e}")
            traceback.print_exc()
            return self._get_empty_growth_data()

    def _get_empty_growth_data(self):

        return {
            'joins': [],
            'leaves': [],
            'total_joins': 0,
            'total_leaves': 0,
            'max_daily_value': 1,
            'max_total_value': 1
        }

    async def fetch_projected_growth(self, guild_id: int, days: int = 14) -> Dict:

        try:
            if not self.pool:
                print("❌ GrowthSystem: Database pool not available for projections")
                return self._get_default_projection(days)

            async with self.pool.acquire() as conn:
                avg_days = min(days * 3, 90)
                avg_cutoff = datetime.utcnow() - timedelta(days=avg_days)

                avg_joins_query = """
                    SELECT COALESCE(AVG(daily_joins), 0) as avg_joins
                    FROM (
                        SELECT date, SUM(joins_count) as daily_joins
                        FROM member_growth_stats
                        WHERE guild_id = $1 AND date >= $2
                        GROUP BY date
                    ) daily_stats;
                """
                avg_joins_result = await conn.fetchval(avg_joins_query, guild_id, avg_cutoff.date())

                avg_leaves_query = """
                    SELECT COALESCE(AVG(daily_leaves), 0) as avg_leaves
                    FROM (
                        SELECT date, SUM(leaves_count) as daily_leaves
                        FROM member_growth_stats
                        WHERE guild_id = $1 AND date >= $2
                        GROUP BY date
                    ) daily_stats;
                """
                avg_leaves_result = await conn.fetchval(avg_leaves_query, guild_id, avg_cutoff.date())

                if avg_joins_result == 0:
                    raw_avg_joins_query = """
                        SELECT COALESCE(COUNT(*) / NULLIF($3, 0), 1) as avg_joins
                        FROM member_joins
                        WHERE guild_id = $1 AND join_time >= $2;
                    """
                    avg_joins_result = await conn.fetchval(raw_avg_joins_query, guild_id, avg_cutoff, avg_days) or 1

                if avg_leaves_result == 0:
                    raw_avg_leaves_query = """
                        SELECT COALESCE(COUNT(*) / NULLIF($3, 0), 0.5) as avg_leaves
                        FROM member_leaves
                        WHERE guild_id = $1 AND leave_time >= $2;
                    """
                    avg_leaves_result = await conn.fetchval(raw_avg_leaves_query, guild_id, avg_cutoff, avg_days) or 0.5

                avg_daily_joins = float(avg_joins_result)
                avg_daily_leaves = float(avg_leaves_result)

                end_time = datetime.utcnow()

                if days <= 2:
                    intervals = 24 * days
                    time_delta = timedelta(hours=1)
                elif days <= 30:
                    intervals = days
                    time_delta = timedelta(days=1)
                elif days <= 180:
                    intervals = days // 7
                    time_delta = timedelta(days=7)
                else:
                    intervals = days // 30
                    time_delta = timedelta(days=30)

                projected_joins = []
                projected_leaves = []
                total_projected_joins = 0
                total_projected_leaves = 0

                for i in range(intervals):
                    interval_start = end_time + time_delta * i
                    interval_end = end_time + time_delta * (i + 1)

                    if days <= 2:
                        date_obj = interval_start
                    else:
                        date_obj = interval_start.date()

                    join_factor = np.random.uniform(0.8, 1.2)
                    leave_factor = np.random.uniform(0.8, 1.2)

                    if days <= 2:

                        projected_join_count = max(
                            0, int((avg_daily_joins / 24) * join_factor))
                        projected_leave_count = max(
                            0, int((avg_daily_leaves / 24) * leave_factor))
                    elif days <= 30:

                        projected_join_count = max(
                            0, int(avg_daily_joins * join_factor))
                        projected_leave_count = max(
                            0, int(avg_daily_leaves * leave_factor))
                    elif days <= 180:

                        projected_join_count = max(
                            0, int(avg_daily_joins * 7 * join_factor))
                        projected_leave_count = max(
                            0, int(avg_daily_leaves * 7 * leave_factor))
                    else:

                        projected_join_count = max(
                            0, int(avg_daily_joins * 30 * join_factor))
                        projected_leave_count = max(
                            0, int(avg_daily_leaves * 30 * leave_factor))

                    projected_joins.append({
                        'date': date_obj,
                        'count': projected_join_count
                    })
                    projected_leaves.append({
                        'date': date_obj,
                        'count': projected_leave_count
                    })
                    total_projected_joins += projected_join_count
                    total_projected_leaves += projected_leave_count

                projected_joins.sort(key=lambda x: x['date'])
                projected_leaves.sort(key=lambda x: x['date'])

                max_daily_joins = max([p['count']
                                      for p in projected_joins], default=0)
                max_daily_leaves = max([p['count']
                                       for p in projected_leaves], default=0)
                max_daily_value = max(max_daily_joins, max_daily_leaves, 1)

                result = {
                    'joins': projected_joins,
                    'leaves': projected_leaves,
                    'total_joins': total_projected_joins,
                    'total_leaves': total_projected_leaves,
                    'max_daily_value': max_daily_value,
                    'max_total_value': max(total_projected_joins, total_projected_leaves, 1),
                    'is_projection': True,
                    'avg_daily_joins': avg_daily_joins,
                    'avg_daily_leaves': avg_daily_leaves
                }

                return result

        except Exception as e:
            print(f"❌ GrowthSystem: Error fetching projected growth: {e}")
            traceback.print_exc()
            return self._get_default_projection(days)

    def _get_default_projection(self, days: int):

        end_time = datetime.utcnow()

        if days <= 2:
            intervals = 24 * days
            time_delta = timedelta(hours=1)
        elif days <= 30:
            intervals = days
            time_delta = timedelta(days=1)
        elif days <= 180:
            intervals = days // 7
            time_delta = timedelta(days=7)
        else:
            intervals = days // 30
            time_delta = timedelta(days=30)

        default_joins = []
        default_leaves = []

        for i in range(intervals):
            interval_start = end_time + time_delta * i
            if days <= 2:
                date_obj = interval_start
            else:
                date_obj = interval_start.date()

            default_joins.append({'date': date_obj, 'count': 5})
            default_leaves.append({'date': date_obj, 'count': 2})

        return {
            'joins': default_joins,
            'leaves': default_leaves,
            'total_joins': 5 * intervals,
            'total_leaves': 2 * intervals,
            'max_daily_value': max(5, 2, 1),
            'max_total_value': max(5 * intervals, 2 * intervals, 1),
            'is_projection': True
        }

    # TIMESTAMP SYSTEM

    def generate_growth_timestamps(self, data_points: List[Dict], is_projection: bool = False) -> List[str]:

        if not data_points or len(data_points) < 2:

            return [f"Day {i+1}" for i in range(8)]

        dates = [row['date'] for row in data_points if 'date' in row]
        if not dates:
            return [f"Day {i+1}" for i in range(8)]

        start_time = min(dates)
        end_time = max(dates)
        delta = end_time - start_time

        total_days = delta.days + (delta.seconds / 86400)

        if total_days <= 2:

            if isinstance(start_time, date) and not isinstance(start_time, datetime):

                start_time = datetime.combine(start_time, datetime.min.time())
                end_time = datetime.combine(end_time, datetime.min.time())

            timestamps = []

            if total_days <= 1:
                hours = [0, 3, 6, 9, 12, 15, 18, 21]
                for hour in hours:
                    timestamps.append(f"{hour:02d}:00")
            else:
                hours = [0, 6, 12, 18, 24, 30, 36, 42]
                for i, hour in enumerate(hours):
                    if hour < 24:
                        timestamps.append(f"{hour:02d}:00")
                    else:
                        display_hour = hour - 24
                        timestamps.append(f"{display_hour:02d}:00")

            return timestamps

        elif total_days <= 30:
            fmt = "%b %d"
        elif total_days <= 180:
            fmt = "Week %W"
        else:
            fmt = "%b %Y"

        timestamps = []
        for i in range(8):
            t = start_time + (delta * (i / 7))
            if fmt == "Week %W":
                week_num = t.isocalendar()[1]
                timestamps.append(f"Week {week_num}")
            else:
                timestamps.append(t.strftime(fmt))

        return timestamps

    def get_timestamp_x_positions(self) -> List[int]:

        x_start = 65
        x_end = 650
        interval = (x_end - x_start) / 7
        return [int(x_start + interval * i) for i in range(8)]

    # CHART GENERATION METHODS

    def generate_growth_chart(self, data: Dict, days: int, growth_type: str) -> Tuple[Image.Image, List[str], List[Tuple[float, str]]]:

        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(6, 2.5), dpi=300,
                               facecolor=(0, 0, 0, 0))

        ax.set_axis_off()

        timestamps = []
        y_labels = []

        joins_data = data.get('joins', [])
        leaves_data = data.get('leaves', [])
        is_projection = data.get('is_projection', False)

        max_daily_value = data.get('max_daily_value', 1)

        if max_daily_value <= 10:
            if max_daily_value <= 5:
                y_max = max(5, max_daily_value * 1.2)
            else:
                y_max = max_daily_value * 1.5
        else:
            y_max = max_daily_value * 1.1

        y_max = max(y_max, 5)

        y_labels = [
            (y_max, self.format_number(y_max)),
            (y_max * 0.75, self.format_number(y_max * 0.75)),
            (y_max * 0.5, self.format_number(y_max * 0.5)),
            (y_max * 0.25, self.format_number(y_max * 0.25)),
            (0, "0")
        ]

        all_points = joins_data + leaves_data

        if all_points:
            timestamps = self.generate_growth_timestamps(
                all_points, is_projection)

            if joins_data:
                joins_data.sort(key=lambda x: x['date'])
                join_dates = [row['date'] for row in joins_data]
                join_values = [float(row['count']) for row in joins_data]

                join_dates_numeric = [mdates.date2num(
                    date) for date in join_dates]

                if len(join_dates_numeric) > 1:
                    x_smooth = np.linspace(
                        min(join_dates_numeric), max(join_dates_numeric), 500)
                    y_smooth = np.interp(
                        x_smooth, join_dates_numeric, join_values)

                    from scipy.ndimage import gaussian_filter1d
                    y_smooth = gaussian_filter1d(y_smooth, sigma=2)

                    ax.plot(x_smooth, y_smooth, linewidth=4, color='#21ef00',
                            solid_capstyle='round', antialiased=True, label='Joins')
                else:
                    ax.plot(join_dates_numeric, join_values, 'o',
                            markersize=3, color='#21ef00', alpha=0.7)

            if leaves_data:
                leaves_data.sort(key=lambda x: x['date'])
                leave_dates = [row['date'] for row in leaves_data]
                leave_values = [float(row['count']) for row in leaves_data]

                leave_dates_numeric = [mdates.date2num(
                    date) for date in leave_dates]

                if len(leave_dates_numeric) > 1:
                    x_smooth = np.linspace(
                        min(leave_dates_numeric), max(leave_dates_numeric), 500)
                    y_smooth = np.interp(
                        x_smooth, leave_dates_numeric, leave_values)

                    from scipy.ndimage import gaussian_filter1d
                    y_smooth = gaussian_filter1d(y_smooth, sigma=2)

                    ax.plot(x_smooth, y_smooth, linewidth=4, color='#ef0000',
                            solid_capstyle='round', antialiased=True, label='Leaves')
                else:
                    ax.plot(leave_dates_numeric, leave_values, 'o',
                            markersize=3, color='#ef0000', alpha=0.7)

        else:
            print("⚠️ GrowthSystem: No data points, creating placeholder chart")
            timestamps = [f"Day {i+1}" for i in range(8)]
            x_smooth = np.linspace(1, 8, 100)
            y_joins = np.ones_like(x_smooth) * (y_max * 0.5)
            y_leaves = np.ones_like(x_smooth) * (y_max * 0.25)

            ax.plot(x_smooth, y_joins, linewidth=4, color='#21ef00',
                    solid_capstyle='round', antialiased=True, label='Joins', alpha=0.5)
            ax.plot(x_smooth, y_leaves, linewidth=4, color='#ef0000',
                    solid_capstyle='round', antialiased=True, label='Leaves', alpha=0.5)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', transparent=True,
                    bbox_inches='tight', pad_inches=0, dpi=300)
        buf.seek(0)
        chart_img = Image.open(buf)
        plt.close(fig)

        return chart_img, timestamps, y_labels

    def format_number(self, num: float) -> str:

        if num >= 1000:
            return f"{num/1000:.1f}k"
        return str(int(num))

    async def draw_growth_header(self, template: Image.Image, growth_type: str, guild_id: int) -> Image.Image:

        draw = ImageDraw.Draw(template)

        avatar_x, avatar_y = 10, 10
        avatar_size = (52, 52)

        text_start_x = 78
        text_start_y = 28

        username_rectangle = {"center": (143, 28), "width": 183, "height": 35}

        current_date = datetime.now().strftime("%B %d, %Y")
        try:
            horndon_font = ImageFont.truetype(
                BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", 16)
        except:
            horndon_font = ImageFont.load_default()

        draw_text_with_stroke(draw, (570, 38), current_date,
                              horndon_font, "white", "black", 2)

        guild = self.bot.get_guild(int(guild_id))
        if not guild:
            return template

        # FITTING LOGIC FUNCTIONS

        def fit_text_to_rectangle(text, text_start_x, text_start_y, rect_center_x, rect_center_y, rect_width, rect_height):

            font_paths = [
                BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"]

            font_sizes = [32, 30, 28, 26, 24, 22, 20,
                          18, 16]

            rect_left = rect_center_x - (rect_width // 2)
            rect_right = rect_center_x + (rect_width // 2)
            rect_top = rect_center_y - (rect_height // 2)
            rect_bottom = rect_center_y + (rect_height // 2)

            for font_size in font_sizes:
                try:
                    font = ImageFont.truetype(font_paths[0], font_size) if os.path.exists(
                        font_paths[0]) else ImageFont.load_default()
                except:
                    font = ImageFont.load_default()

                bbox = draw.textbbox((0, 0), text, font=font)
                text_width = bbox[2] - bbox[0]
                text_height = bbox[3] - bbox[1]

                text_x = text_start_x
                text_y = text_start_y - (text_height // 2)

                vertical_offset = 0
                if font_size <= 32:
                    vertical_offset += 1
                if font_size <= 30:
                    vertical_offset += 1
                if font_size <= 28:
                    vertical_offset += 1
                if font_size <= 26:
                    vertical_offset += 1
                if font_size <= 24:
                    vertical_offset += 1
                if font_size <= 22:
                    vertical_offset += 1
                if font_size <= 20:
                    vertical_offset += 1
                if font_size <= 18:
                    vertical_offset += 1
                if font_size <= 16:
                    vertical_offset += 1
                text_y += vertical_offset

                text_fits_horizontally = (
                    text_x + text_width + 5 <= rect_right + 1)
                text_fits_vertically = (
                    text_y >= rect_top - 1) and (text_y + text_height <= rect_bottom + 1)

                if text_fits_horizontally and text_fits_vertically:
                    return text, font, (text_x, text_y)

            smallest_font = 16
            try:
                font = ImageFont.truetype(font_paths[0], smallest_font) if os.path.exists(
                    font_paths[0]) else ImageFont.load_default()
            except:
                font = ImageFont.load_default()

            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            text_x = text_start_x
            text_y = text_start_y - (text_height // 2)

            vertical_offset = 0
            if smallest_font <= 32:
                vertical_offset += 1
            if smallest_font <= 30:
                vertical_offset += 1
            if smallest_font <= 28:
                vertical_offset += 1
            if smallest_font <= 26:
                vertical_offset += 1
            if smallest_font <= 24:
                vertical_offset += 1
            if smallest_font <= 22:
                vertical_offset += 1
            if smallest_font <= 20:
                vertical_offset += 1
            if smallest_font <= 18:
                vertical_offset += 1
            if smallest_font <= 16:
                vertical_offset += 1
            text_y += vertical_offset

            text_fits_horizontally = (
                text_x + text_width + 5 <= rect_right + 1)
            text_fits_vertically = (
                text_y >= rect_top - 1) and (text_y + text_height <= rect_bottom + 1)

            if text_fits_horizontally and text_fits_vertically:
                return text, font, (text_x, text_y)

            current_text = text
            while len(current_text) > 3:
                current_text = current_text[:-4] + "..."
                bbox = draw.textbbox((0, 0), current_text, font=font)
                text_width = bbox[2] - bbox[0]

                text_x = text_start_x
                text_y = text_start_y - (text_height // 2)
                text_y += vertical_offset

                if (text_x + text_width + 5 <= rect_right + 1):
                    return current_text, font, (text_x, text_y)

            if len(text) > 3:
                final_text = text[:3] + "..."
            else:
                final_text = text

            return final_text, font, (text_x, text_y)

        # DISPLAY

        target_name = guild.name
        use_server_icon = True

        # PROFILE PICTURE
        try:
            if use_server_icon:
                if guild.icon:
                    icon_url = guild.icon.url
                    async with aiohttp.ClientSession() as session:
                        async with session.get(icon_url) as response:
                            icon_data = await response.read()

                    icon_image = Image.open(io.BytesIO(icon_data))
                    icon_image = icon_image.resize(
                        avatar_size, Image.Resampling.LANCZOS).convert('RGBA')

                    mask = Image.new('L', avatar_size, 0)
                    mask_draw = ImageDraw.Draw(mask)
                    mask_draw.ellipse(
                        (0, 0, avatar_size[0], avatar_size[1]), fill=255)
                    icon_image.putalpha(mask)

                    avatar_area = template.crop(
                        (avatar_x, avatar_y, avatar_x +
                         avatar_size[0], avatar_y + avatar_size[1])
                    ).convert('RGBA')

                    icon_with_bg = Image.new('RGBA', avatar_size, (0, 0, 0, 0))
                    icon_with_bg.paste(icon_image, (0, 0), icon_image)
                    avatar_area.paste(icon_with_bg, (0, 0), icon_with_bg)
                    template.paste(avatar_area.convert(
                        'RGB'), (avatar_x, avatar_y))
            else:
                if guild.icon:
                    icon_url = guild.icon.url
                    async with aiohttp.ClientSession() as session:
                        async with session.get(icon_url) as response:
                            icon_data = await response.read()

                    icon_image = Image.open(io.BytesIO(icon_data))
                    icon_image = icon_image.resize(
                        avatar_size, Image.Resampling.LANCZOS).convert('RGBA')

                    mask = Image.new('L', avatar_size, 0)
                    mask_draw = ImageDraw.Draw(mask)
                    mask_draw.ellipse(
                        (0, 0, avatar_size[0], avatar_size[1]), fill=255)
                    icon_image.putalpha(mask)

                    avatar_area = template.crop(
                        (avatar_x, avatar_y, avatar_x +
                         avatar_size[0], avatar_y + avatar_size[1])
                    ).convert('RGBA')

                    icon_with_bg = Image.new(
                        'RGBA', avatar_size, (0, 0, 0, 0))
                    icon_with_bg.paste(icon_image, (0, 0), icon_image)
                    avatar_area.paste(icon_with_bg, (0, 0), icon_with_bg)
                    template.paste(avatar_area.convert(
                        'RGB'), (avatar_x, avatar_y))

        except Exception as e:
            print(f"❌ Could not add profile picture: {e}")

        fitted_text, text_font, text_pos = fit_text_to_rectangle(
            target_name,
            text_start_x,
            text_start_y,
            username_rectangle["center"][0],
            username_rectangle["center"][1],
            username_rectangle["width"],
            username_rectangle["height"]
        )

        if fitted_text and text_font:
            with Pilmoji(template) as pilmoji:

                stroke_width = 1
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            pilmoji.text((text_pos[0] + dx, text_pos[1] + dy),
                                         fitted_text, font=text_font, fill="black")

                pilmoji.text(text_pos, fitted_text,
                             font=text_font, fill="white")
        else:
            try:
                fallback_font = ImageFont.truetype(
                    BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", 32)
            except:
                fallback_font = ImageFont.load_default()

            # Use Pilmoji for fallback too
            with Pilmoji(template) as pilmoji:
                pilmoji.text((text_start_x, text_start_y),
                             target_name, font=fallback_font, fill="white")

        # INVISIBLE RECTANGLE (for testing - uncomment to see boundaries)
        # rect_left = username_rectangle["center"][0] - (username_rectangle["width"] // 2)
        # rect_top = username_rectangle["center"][1] - (username_rectangle["height"] // 2)
        # rect_right = username_rectangle["center"][0] + (username_rectangle["width"] // 2)
        # rect_bottom = username_rectangle["center"][1] + (username_rectangle["height"] // 2)
        # draw.rectangle([rect_left, rect_top, rect_right, rect_bottom], outline="red", width=1)

        return template

    async def draw_growth_on_template(self, chart_img: Image.Image, label_text: str, days: int,
                                      timestamps: List[str], y_labels: List[Tuple[float, str]],
                                      growth_type: str, chart_type: str, guild_id: int) -> io.BytesIO:

        template_path = BASE_DIR / "assets" / "images" / "growth final png.png"

        try:
            template = Image.open(template_path).convert("RGBA")
        except FileNotFoundError:
            template = Image.new('RGBA', (1200, 700), (47, 49, 54, 255))
            print(
                f"⚠️ Growth template not found at {template_path}, using fallback background")

        chart_width = 644
        chart_height = 250
        chart_img = chart_img.resize(
            (chart_width, chart_height), Image.Resampling.LANCZOS)

        template.paste(chart_img, (38, 170), chart_img)

        template = await self.draw_growth_header(template, growth_type, guild_id)

        draw = ImageDraw.Draw(template)
        try:
            try:
                horndon_font = ImageFont.truetype(
                    BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", 16)
                horndon_small_font = ImageFont.truetype(
                    BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", 12)
            except:
                horndon_font = ImageFont.load_default()
                horndon_small_font = ImageFont.load_default()
        except:
            font = ImageFont.load_default()
            small_font = ImageFont.load_default()
            horndon_font = ImageFont.load_default()
            horndon_small_font = ImageFont.load_default()

        text_color = "white"
        shadow_color = "black"

        # TIME PERIOD TEXT
        if growth_type == "joins_leaves":
            time_text = f"Last {days} days"
        else:
            time_text = f"Next {days} days"
        draw_text_with_stroke(draw, (108, 88), time_text,
                              horndon_font, text_color, shadow_color, 1)

        if "Joins:" in label_text or "Projected Joins:" in label_text:
            try:
                if "Projected Joins:" in label_text:
                    joins_text = label_text.split("Projected Joins: ")[
                        1].split(" |")[0]
                else:
                    joins_text = label_text.split("Joins: ")[1].split(" |")[0]
                draw_text_with_stroke(draw, (358, 95), joins_text, horndon_font,
                                      "#21ef00", shadow_color, 1)
            except:
                draw_text_with_stroke(draw, (358, 95), "0", horndon_font,
                                      "#21ef00", shadow_color, 1)

        if "Leaves:" in label_text or "Leaves:" in label_text:
            try:
                leaves_text = label_text.split("Leaves: ")[1].split(" |")[0]
                draw_text_with_stroke(draw, (573, 95), leaves_text, horndon_font,
                                      "#ef0000", shadow_color, 1)
            except:
                draw_text_with_stroke(draw, (573, 95), "0", horndon_font,
                                      "#ef0000", shadow_color, 1)

        if timestamps and len(timestamps) == 8:
            x_positions = self.get_timestamp_x_positions()
            timestamp_y_position = 430

            for i, (timestamp, x_pos) in enumerate(zip(timestamps, x_positions)):
                bbox = draw.textbbox(
                    (0, 0), timestamp, font=horndon_small_font)
                text_width = bbox[2] - bbox[0]
                centered_x = x_pos - (text_width // 2)

                draw_text_with_stroke(draw, (centered_x, timestamp_y_position),
                                      timestamp, horndon_small_font, text_color, shadow_color, 1)

        y_label_positions = [
            (27, 148),  # Highest number
            (27, 215),  # Second highest
            (27, 280),  # Third highest
            (27, 345),  # Fourth highest
            (27, 400)   # Zero
        ]

        for (x, y), (value, label) in zip(y_label_positions, y_labels):
            draw_text_with_stroke(draw, (x, y), label,
                                  horndon_small_font, text_color, shadow_color, 1)

        output = io.BytesIO()
        template.save(output, format='PNG')
        output.seek(0)
        return output

    # COMMANDS

    @app_commands.command(name="growth", description="Track server growth and projections")
    async def growth(self, interaction: discord.Interaction):

        await interaction.response.defer()
        print(
            f"📊 GrowthSystem: /growth command used by {interaction.user.name} in {interaction.guild.name}")

        view = GrowthView(self, interaction.guild.id, days=14)

        await view.update_chart(interaction)


# SETUP

async def setup(bot):
    await bot.add_cog(GrowthSystem(bot))
