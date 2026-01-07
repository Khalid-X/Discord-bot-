from pilmoji import Pilmoji
import aiohttp
import io
from PIL import Image, ImageDraw, ImageFont, ImageOps
from dotenv import load_dotenv
import os
from typing import Optional, List
import math
import random
from datetime import datetime, timedelta
import asyncpg
from discord import app_commands
from discord.ext import commands
import discord
from pathlib import Path


# CONFIGURATION

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


class ShipConfig:

    SHIPS_PER_PAGE = 10
    DEFAULT_DAYS_BACK = 30
    MAX_CUSTOM_DAYS = 2000
    TIME_PERIODS = [7, 14, 30, 60, 90]
    MAX_ROLES_IN_SELECT = 25

    TEMPLATE_PATH = BASE_DIR / "assets" / "images" / "leaderboards final png.png"
    SHIP_TEMPLATE_PATH = BASE_DIR / "assets" / "images" / "ship final png.png"
    FONT_PATH = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"

    SERVER_UI_POSITIONS = {
        'avatar': {'x': 7, 'y': 10, 'size': (60, 60)},
        'text_start': {'x': 85, 'y': 30},
        'username_rectangle': {'center': (150, 30), 'width': 183, 'height': 35}
    }

    COMPATIBILITY_UI_POSITIONS = {
        'avatar': {'x': 7, 'y': 7, 'size': (60, 60)},
        'text_start': {'x': 85, 'y': 27},

        'username_rectangle': {'center': (150, 27), 'width': 183, 'height': 35}
    }


# SHIP LEADERBOARD VIEW

class ShipLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, days_back: int = 30,
                 page: int = 0, selected_role_id: str = None):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.current_days = days_back
        self.page = page
        self.selected_role_id = selected_role_id
        self.show_time_buttons = False
        self._update_buttons()

    def _update_buttons(self):

        self.clear_items()

        self.add_item(RoleSelectMenu(self.guild, self.selected_role_id))

        self.add_item(PrevButton())
        self.add_item(PageIndicator())
        self.add_item(NextButton())

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="ship_refresh",
            row=2
        )
        refresh_button.callback = self.refresh_callback

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="ship_time_settings",
            row=2
        )
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            row = 3
            for days in ShipConfig.TIME_PERIODS:
                days_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == days else discord.ButtonStyle.secondary,
                    label=f"{days} Days",
                    custom_id=f"ship_days_{days}",
                    row=row
                )
                days_button.callback = self.create_days_callback(days)
                self.add_item(days_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in ShipConfig.TIME_PERIODS else "Custom",
                custom_id="ship_custom_days",
                row=row
            )
            custom_button.callback = self.custom_days_callback
            self.add_item(custom_button)

    def create_days_callback(self, days: int):
        async def callback(interaction: discord.Interaction):
            await self.handle_time_period_change(interaction, days)
        return callback

    async def refresh_callback(self, interaction: discord.Interaction):
        await self.handle_refresh(interaction)

    async def time_settings_callback(self, interaction: discord.Interaction):
        await self.handle_time_settings(interaction)

    async def custom_days_callback(self, interaction: discord.Interaction):
        await self.handle_custom_days(interaction)

    async def handle_refresh(self, interaction: discord.Interaction):

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        await self.update_message(interaction)

    async def handle_time_settings(self, interaction: discord.Interaction):

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.show_time_buttons = not self.show_time_buttons
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def handle_time_period_change(self, interaction: discord.Interaction, days: int):

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.current_days = days
        self.show_time_buttons = False
        self.page = 0
        await self.update_message(interaction)

    async def handle_custom_days(self, interaction: discord.Interaction):

        modal = ShipLeaderboardTimeModal(
            self.cog, interaction.message.id, self.guild)
        await interaction.response.send_modal(modal)

    async def generate_image(self):

        leaderboard_data = await self.cog.get_ship_leaderboard_data(
            self.guild.id, self.current_days, self.selected_role_id
        )

        return await self.cog.generate_ship_leaderboard_image(
            guild=self.guild,
            leaderboard_data=leaderboard_data,
            days_back=self.current_days,
            role_id=self.selected_role_id,
            page=self.page
        )

    async def update_message(self, interaction: discord.Interaction):

        try:
            leaderboard_data = await self.cog.get_ship_leaderboard_data(
                self.guild.id, self.current_days, self.selected_role_id
            )
            total_ships = len(leaderboard_data)
            total_pages = max(
                1, (total_ships + ShipConfig.SHIPS_PER_PAGE - 1) // ShipConfig.SHIPS_PER_PAGE)

            if self.page >= total_pages:
                self.page = max(0, total_pages - 1)

            img_bytes = await self.generate_image()

            for child in self.children:
                if isinstance(child, PageIndicator):
                    child.label = f"Page {self.page + 1}/{total_pages}"
                elif isinstance(child, PrevButton):
                    child.disabled = (self.page == 0 or total_pages <= 1)
                elif isinstance(child, NextButton):
                    child.disabled = (
                        self.page >= total_pages - 1 or total_pages <= 1)

            file = discord.File(img_bytes, filename="ship_leaderboard.png")

            if interaction.response.is_done():
                await interaction.edit_original_response(attachments=[file], view=self)
            else:
                await interaction.response.edit_message(attachments=[file], view=self)

        except Exception as e:
            print(f"❌ Error updating ship leaderboard: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            else:
                await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)


# TIME MODAL

class ShipLeaderboardTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, cog_instance, original_message_id, guild):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.original_message_id = original_message_id
        self.guild = guild

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
            if days <= 0 or days > ShipConfig.MAX_CUSTOM_DAYS:
                await interaction.response.send_message(f"❌ Please enter a number between 1 and {ShipConfig.MAX_CUSTOM_DAYS} days.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=False, thinking=True)

            if self.original_message_id in self.cog.active_views:
                view = self.cog.active_views[self.original_message_id]
                view.current_days = days
                view.show_time_buttons = False
                view.page = 0
                await view.update_message(interaction)
            else:
                await interaction.followup.send("❌ Could not find the leaderboard view. Please try the command again.", ephemeral=True)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            print(f"❌ Error in ship modal submit: {e}")
            await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)


# ROLE DROPDOWN MENU

class RoleSelectMenu(discord.ui.Select):
    def __init__(self, guild: discord.Guild, current_role_id: str = None):
        self.guild = guild

        roles = [role for role in guild.roles if role.name != "@everyone"]
        roles.sort(key=lambda x: x.position, reverse=True)
        roles = roles[:ShipConfig.MAX_ROLES_IN_SELECT]

        options = [
            discord.SelectOption(
                label="No Filter",
                value="none",
                description="Show all ships",
                emoji="🌐",
                default=(current_role_id == "none" or not current_role_id)
            )
        ]

        for role in roles:
            options.append(
                discord.SelectOption(
                    label=role.name,
                    value=str(role.id),
                    description=f"Filter by {role.name} role",
                    default=(str(role.id) == current_role_id)
                )
            )

        super().__init__(
            placeholder="Filter by role...",
            options=options,
            custom_id="ship_role_filter_select",
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        if hasattr(view, 'selected_role_id'):
            view.selected_role_id = self.values[0]
            view.page = 0
            await view.update_message(interaction)


# PAGINATION

class PrevButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="⬅️", custom_id="ship_prev")

    async def callback(self, interaction: discord.Interaction):
        if self.disabled:
            await interaction.response.defer(ephemeral=True, thinking=False)
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        if view.page > 0:
            view.page -= 1
            await view.update_message(interaction)


class NextButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="➡️", custom_id="ship_next")

    async def callback(self, interaction: discord.Interaction):
        if self.disabled:
            await interaction.response.defer(ephemeral=True, thinking=False)
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        view.page += 1
        await view.update_message(interaction)


class PageIndicator(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Page 1/1",
                         custom_id="ship_page", disabled=True)


# DRAWING FUNCTIONS

class ServerUIIntegration:

    @staticmethod
    async def add_server_profile_pic_and_name(image, guild, target_name, font_huge, is_compatibility=False):

        draw = ImageDraw.Draw(image)

        def check_text_against_rectangle(text, font, text_x, text_y, rect_center_x, rect_center_y, rect_width, rect_height):

            rect_left = rect_center_x - (rect_width // 2)
            rect_right = rect_center_x + (rect_width // 2)
            rect_top = rect_center_y - (rect_height // 2)
            rect_bottom = rect_center_y + (rect_height // 2)

            bbox = draw.textbbox((text_x, text_y), text, font=font)
            text_left = bbox[0]
            text_right = bbox[2]
            text_top = bbox[1]
            text_bottom = bbox[3]

            exceeds_left = text_left < rect_left
            exceeds_right = text_right > rect_right
            exceeds_top = text_top < rect_top
            exceeds_bottom = text_bottom > rect_bottom

            return exceeds_left or exceeds_right or exceeds_top or exceeds_bottom

        def fit_text_to_rectangle(text, text_start_x, text_start_y, rect_center_x, rect_center_y, rect_width, rect_height, is_username=False):

            font_paths = [ShipConfig.FONT_PATH]

            initial_font_size = 40

            rect_left = rect_center_x - (rect_width // 2)
            rect_right = rect_center_x + (rect_width // 2)
            rect_top = rect_center_y - (rect_height // 2)
            rect_bottom = rect_center_y + (rect_height // 2)

            font_sizes = [40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16]

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
                if font_size <= 40:
                    vertical_offset += 1
                if font_size <= 30:
                    vertical_offset += 1
                if font_size <= 20:
                    vertical_offset += 1
                if font_size <= 16:
                    vertical_offset += 1
                text_y += vertical_offset

                text_fits_horizontally = (
                    text_x >= rect_left - 1) and (text_x + text_width <= rect_right + 1)
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
            if smallest_font <= 40:
                vertical_offset += 1
            if smallest_font <= 30:
                vertical_offset += 1
            if smallest_font <= 20:
                vertical_offset += 1
            if smallest_font <= 16:
                vertical_offset += 1
            text_y += vertical_offset

            text_fits_horizontally = (
                text_x >= rect_left - 1) and (text_x + text_width <= rect_right + 1)
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

                if (text_x >= rect_left - 1) and (text_x + text_width <= rect_right + 1):
                    return current_text, font, (text_x, text_y)

            if len(text) > 3:
                final_text = text[:3] + "..."
            else:
                final_text = text

            bbox = draw.textbbox((0, 0), final_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            text_x = text_start_x
            text_y = text_start_y - (text_height // 2)
            text_y += vertical_offset

            return final_text, font, (text_x, text_y)

        if is_compatibility:

            positions = ShipConfig.COMPATIBILITY_UI_POSITIONS
        else:

            positions = ShipConfig.SERVER_UI_POSITIONS

        # User profile picture
        avatar_x, avatar_y = positions['avatar']['x'], positions['avatar']['y']
        avatar_size = positions['avatar']['size']

        # Server name
        text_start_x = positions['text_start']['x']
        text_start_y = positions['text_start']['y']

        # Rectangle for text constraints
        username_rectangle = positions['username_rectangle']

        # Server profile picture
        try:
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

                avatar_area = image.crop(
                    (avatar_x, avatar_y, avatar_x +
                     avatar_size[0], avatar_y + avatar_size[1])
                ).convert('RGBA')

                icon_with_bg = Image.new('RGBA', avatar_size, (0, 0, 0, 0))
                icon_with_bg.paste(icon_image, (0, 0), icon_image)
                avatar_area.paste(icon_with_bg, (0, 0), icon_with_bg)
                image.paste(avatar_area.convert('RGB'), (avatar_x, avatar_y))

        except Exception as e:
            print(f"❌ Could not add server icon: {e}")

        fitted_text, text_font, text_pos = fit_text_to_rectangle(
            target_name,
            text_start_x,
            text_start_y,
            username_rectangle["center"][0],
            username_rectangle["center"][1],
            username_rectangle["width"],
            username_rectangle["height"],
            is_username=True
        )

        if fitted_text and text_font:

            stroke_width = 1
            for dx in [-stroke_width, 0, stroke_width]:
                for dy in [-stroke_width, 0, stroke_width]:
                    if dx != 0 or dy != 0:
                        draw.text((text_pos[0] + dx, text_pos[1] + dy),
                                  fitted_text, font=text_font, fill="black")

            draw.text(text_pos, fitted_text, font=text_font, fill="white")
        else:

            draw.text((text_start_x, text_start_y),
                      target_name, font=font_huge, fill="white")


# INITIALIZATION

class ShipCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.pool = None
        self.ships_per_page = ShipConfig.SHIPS_PER_PAGE
        self.active_views = {}

        self.server_averages = {}

    async def cog_load(self):

        await self.init_db()
        await self.create_ship_table()

    async def init_db(self):

        try:
            self.pool = await asyncpg.create_pool(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', 5432)),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                database=os.getenv('DB_NAME'),
                min_size=1,
                max_size=10
            )
            print("✅ Ship cog database pool ready")
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            self.pool = None

    async def create_ship_table(self):

        if not self.pool:
            return

        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS ship_scores (
                        id SERIAL PRIMARY KEY,
                        guild_id BIGINT NOT NULL,
                        user1_id BIGINT NOT NULL,
                        user2_id BIGINT NOT NULL,
                        ship_name TEXT NOT NULL,
                        compatibility_score DECIMAL(5,4) NOT NULL,
                        mentions_score DECIMAL(5,4) NOT NULL,
                        activity_score DECIMAL(5,4) NOT NULL,
                        channels_score DECIMAL(5,4) NOT NULL,
                        vc_score DECIMAL(5,4) NOT NULL,
                        recentness_score DECIMAL(5,4) NOT NULL,
                        times_shipped INTEGER DEFAULT 1,
                        last_shipped TIMESTAMP DEFAULT NOW(),
                        created_at TIMESTAMP DEFAULT NOW(),
                        -- Store users in consistent order to prevent duplicates
                        -- We'll enforce this at application level, not database level
                        UNIQUE(guild_id, user1_id, user2_id)
                    )
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_ship_scores_guild_score 
                    ON ship_scores (guild_id, compatibility_score DESC)
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_ship_scores_users 
                    ON ship_scores (guild_id, user1_id, user2_id)
                """)
            print("✅ Ship scores table ready")
        except Exception as e:
            print(f"❌ Failed to create ship scores table: {e}")
            import traceback
            traceback.print_exc()

    # SHIP AVERAGE CALCULATION LOGIC

    async def calculate_server_average_compatibility(self, guild_id: int, days: int = 30) -> float:

        if not self.pool:
            return 0.5

        try:
            async with self.pool.acquire() as conn:
                avg_score = await conn.fetchval("""
                    SELECT AVG(compatibility_score) 
                    FROM ship_scores 
                    WHERE guild_id = $1 
                    AND last_shipped >= NOW() - INTERVAL '1 day' * $2
                """, guild_id, days)

                if avg_score is None:
                    return 0.5

                return float(avg_score)
        except Exception as e:
            print(f"❌ Error calculating server average: {e}")
            return 0.5

    async def store_ship_score(self, guild_id: int, user1_id: int, user2_id: int,
                               ship_name: str, compatibility_data: dict):

        if not self.pool:
            return

        server_avg = await self.calculate_server_average_compatibility(guild_id)

        raw_score = compatibility_data['final_score']

        if server_avg > 0.5:
            if raw_score > server_avg:
                adjusted_score = 0.5 + \
                    (raw_score - server_avg) * (0.5 / (1 - server_avg))
            else:
                adjusted_score = raw_score * (0.5 / server_avg)
        else:

            if raw_score > server_avg:
                adjusted_score = 0.5 + \
                    (raw_score - server_avg) * (0.5 / (1 - server_avg))
            else:
                adjusted_score = raw_score * (0.5 / server_avg)

        adjusted_score = max(0.0, min(1.0, adjusted_score))

        compatibility_data['final_score'] = adjusted_score
        compatibility_data['server_average'] = server_avg
        compatibility_data['is_adjusted'] = True

        try:
            async with self.pool.acquire() as conn:

                user_min = min(user1_id, user2_id)
                user_max = max(user1_id, user2_id)

                existing = await conn.fetchrow("""
                    SELECT ship_name, compatibility_score, times_shipped, user1_id, user2_id 
                    FROM ship_scores 
                    WHERE guild_id = $1 
                    AND (
                        (user1_id = $2 AND user2_id = $3) OR
                        (user1_id = $3 AND user2_id = $2)
                    )
                """, guild_id, user_min, user_max)

                if existing:
                    existing_user1 = existing['user1_id']
                    existing_user2 = existing['user2_id']

                    if existing_user1 == user_min and existing_user2 == user_max:

                        await conn.execute("""
                            UPDATE ship_scores 
                            SET ship_name = $4,
                                compatibility_score = $5,
                                mentions_score = $6,
                                activity_score = $7,
                                channels_score = $8,
                                vc_score = $9,
                                recentness_score = $10,
                                times_shipped = times_shipped + 1,
                                last_shipped = NOW()
                            WHERE guild_id = $1 
                            AND user1_id = $2 
                            AND user2_id = $3
                        """,
                                           guild_id,
                                           user_min,
                                           user_max,
                                           ship_name,
                                           compatibility_data['final_score'],
                                           compatibility_data['breakdown']['mentions']['score'],
                                           compatibility_data['breakdown']['activity_overlap']['score'],
                                           compatibility_data['breakdown']['shared_channels']['score'],
                                           compatibility_data['breakdown']['vc_time']['score'],
                                           compatibility_data['breakdown']['recentness']['score']
                                           )
                        print(
                            f"✅ Updated existing ship (already in correct order): {ship_name}")
                    else:

                        await conn.execute("""
                            DELETE FROM ship_scores 
                            WHERE guild_id = $1 
                            AND user1_id = $2 
                            AND user2_id = $3
                        """, guild_id, existing_user1, existing_user2)

                        await conn.execute("""
                            INSERT INTO ship_scores 
                            (guild_id, user1_id, user2_id, ship_name, compatibility_score, 
                             mentions_score, activity_score, channels_score, vc_score, recentness_score,
                             times_shipped, last_shipped)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                                    $11 + 1, NOW())
                        """,
                                           guild_id,
                                           user_min,
                                           user_max,
                                           ship_name,
                                           compatibility_data['final_score'],
                                           compatibility_data['breakdown']['mentions']['score'],
                                           compatibility_data['breakdown']['activity_overlap']['score'],
                                           compatibility_data['breakdown']['shared_channels']['score'],
                                           compatibility_data['breakdown']['vc_time']['score'],
                                           compatibility_data['breakdown']['recentness']['score'],

                                           existing['times_shipped']
                                           )
                        print(
                            f"✅ Reordered and updated existing ship: {ship_name}")
                else:
                    await conn.execute("""
                        INSERT INTO ship_scores 
                        (guild_id, user1_id, user2_id, ship_name, compatibility_score, 
                         mentions_score, activity_score, channels_score, vc_score, recentness_score,
                         times_shipped, last_shipped)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, NOW())
                    """,
                                       guild_id,
                                       user_min,
                                       user_max,
                                       ship_name,
                                       compatibility_data['final_score'],
                                       compatibility_data['breakdown']['mentions']['score'],
                                       compatibility_data['breakdown']['activity_overlap']['score'],
                                       compatibility_data['breakdown']['shared_channels']['score'],
                                       compatibility_data['breakdown']['vc_time']['score'],
                                       compatibility_data['breakdown']['recentness']['score']
                                       )
                    print(f"✅ Created new ship: {ship_name}")

        except Exception as e:
            print(f"❌ Error storing ship score: {e}")
            import traceback
            traceback.print_exc()

    # QUERY FUNCTIONS

    async def get_ship_leaderboard_data(self, guild_id: int, days_back: int = 30,
                                        role_id: str = None, limit: int = 100):

        if not self.pool:
            return []

        try:

            role_member_ids = None
            if role_id and role_id != "none":
                guild = self.bot.get_guild(guild_id)
                if guild:
                    role = guild.get_role(int(role_id))
                    if role:
                        role_member_ids = [
                            member.id for member in role.members]

            async with self.pool.acquire() as conn:
                if role_member_ids:

                    placeholders = ', '.join(
                        f'${i+3}' for i in range(len(role_member_ids)))
                    query = f"""
                        SELECT 
                            guild_id, user1_id, user2_id, ship_name, compatibility_score,
                            mentions_score, activity_score, channels_score, 
                            vc_score, recentness_score, times_shipped, last_shipped
                        FROM ship_scores 
                        WHERE guild_id = $1 
                        AND last_shipped >= NOW() - INTERVAL '1 day' * $2
                        AND user1_id IN ({placeholders})
                        AND user2_id IN ({placeholders})
                        ORDER BY compatibility_score DESC 
                        LIMIT {limit}
                    """
                    params = [guild_id, days_back] + role_member_ids
                    results = await conn.fetch(query, *params)
                else:

                    results = await conn.fetch("""
                        SELECT 
                            guild_id, user1_id, user2_id, ship_name, compatibility_score,
                            mentions_score, activity_score, channels_score, 
                            vc_score, recentness_score, times_shipped, last_shipped
                        FROM ship_scores 
                        WHERE guild_id = $1 
                        AND last_shipped >= NOW() - INTERVAL '1 day' * $2
                        ORDER BY compatibility_score DESC 
                        LIMIT $3
                    """, guild_id, days_back, limit)

            ship_data = []
            for row in results:
                ship = dict(row)
                ship_data.append(ship)

            return ship_data
        except Exception as e:
            print(f"❌ Error getting ship leaderboard data: {e}")
            import traceback
            traceback.print_exc()
            return []

    async def get_total_ships_count(self, guild_id: int, days_back: int = 30, role_id: str = None):

        if not self.pool:
            return 0

        try:

            role_member_ids = None
            if role_id and role_id != "none":
                guild = self.bot.get_guild(guild_id)
                if guild:
                    role = guild.get_role(int(role_id))
                    if role:
                        role_member_ids = [
                            member.id for member in role.members]

            async with self.pool.acquire() as conn:
                if role_member_ids:
                    placeholders = ', '.join(
                        f'${i+3}' for i in range(len(role_member_ids)))
                    query = f"""
                        SELECT COUNT(*) 
                        FROM ship_scores 
                        WHERE guild_id = $1 
                        AND last_shipped >= NOW() - INTERVAL '1 day' * $2
                        AND user1_id IN ({placeholders})
                        AND user2_id IN ({placeholders})
                    """
                    params = [guild_id, days_back] + role_member_ids
                    count = await conn.fetchval(query, *params)
                else:

                    count = await conn.fetchval("""
                        SELECT COUNT(*) 
                        FROM ship_scores 
                        WHERE guild_id = $1 
                        AND last_shipped >= NOW() - INTERVAL '1 day' * $2
                    """, guild_id, days_back)
            return count
        except Exception as e:
            print(f"❌ Error getting total ships count: {e}")
            import traceback
            traceback.print_exc()
            return 0

    # SHIP COMPATABILITY LOGIC

    async def calculate_compatibility_score(self, guild_id: int, user1_id: int, user2_id: int) -> dict:

        days = 30

        try:

            mentions_data = await self.get_mutual_mentions(guild_id, user1_id, user2_id, days)

            activity_overlap = await self.get_activity_overlap(guild_id, user1_id, user2_id, days)
            if activity_overlap is None:
                activity_overlap = 0.0

            shared_channels = await self.get_shared_channels(guild_id, user1_id, user2_id, days)
            if shared_channels is None:
                shared_channels = 0.0

            vc_time = await self.get_vc_time_together(guild_id, user1_id, user2_id, days)
            if vc_time is None:
                vc_time = 0.0

            recentness = await self.get_recentness_factor(guild_id, user1_id, user2_id, days)
            if recentness is None:
                recentness = 0.0

            print(f"📈 Mentions: {mentions_data}")
            print(f"📈 Activity overlap: {activity_overlap:.2f}")
            print(f"📈 Shared channels: {shared_channels:.2f}")
            print(f"📈 VC time: {vc_time:.2f}")
            print(f"📈 Recentness: {recentness:.2f}")

            mentions_score = self._calculate_mentions_score(mentions_data)
            activity_score = activity_overlap
            channels_score = shared_channels
            vc_score = vc_time
            recentness_score = recentness

            weights = {
                'mentions': 0.35,
                'activity': 0.25,
                'channels': 0.20,
                'vc': 0.10,
                'recentness': 0.10
            }

            if mentions_data['total'] == 0:

                redistribute = weights['mentions'] / 4
                weights['activity'] += redistribute
                weights['channels'] += redistribute
                weights['vc'] += redistribute
                weights['recentness'] += redistribute
                weights['mentions'] = 0

            if vc_time == 0:
                weights['activity'] += weights['vc'] * 0.5
                weights['channels'] += weights['vc'] * 0.5
                weights['vc'] = 0

            final_score = (
                mentions_score * weights['mentions'] +
                activity_score * weights['activity'] +
                channels_score * weights['channels'] +
                vc_score * weights['vc'] +
                recentness_score * weights['recentness']
            )
            # LOGARITHMIC SCALING
            final_score = self._apply_logarithmic_scaling(final_score)

            print(f"🎯 Final raw score: {final_score:.4f}")

            return {
                'final_score': final_score,
                'breakdown': {
                    'mentions': {
                        'score': mentions_score,
                        'weight': weights['mentions'],
                        'data': mentions_data
                    },
                    'activity_overlap': {
                        'score': activity_score,
                        'weight': weights['activity']
                    },
                    'shared_channels': {
                        'score': channels_score,
                        'weight': weights['channels']
                    },
                    'vc_time': {
                        'score': vc_score,
                        'weight': weights['vc']
                    },
                    'recentness': {
                        'score': recentness_score,
                        'weight': weights['recentness']
                    }
                }
            }

        except Exception as e:
            print(f"❌ Error in calculate_compatibility_score: {e}")
            import traceback
            traceback.print_exc()
            return {
                'final_score': 0.1,
                'breakdown': {
                    'mentions': {'score': 0.0, 'weight': 0.35, 'data': {'total': 0, 'user1_to_user2': 0, 'user2_to_user1': 0}},
                    'activity_overlap': {'score': 0.0, 'weight': 0.25},
                    'shared_channels': {'score': 0.0, 'weight': 0.20},
                    'vc_time': {'score': 0.0, 'weight': 0.10},
                    'recentness': {'score': 0.0, 'weight': 0.10}
                }
            }

    def _apply_logarithmic_scaling(self, score: float) -> float:

        if score <= 0:
            return 0.0
        if score >= 1:
            return 1.0
        scaled = 1 / (1 + math.exp(-10 * (score - 0.5)))
        return max(0.0, min(1.0, scaled))

    # SHIP NAME

    def _get_ship_name(self, user1_name: str, user2_name: str) -> str:

        name1 = ''.join(c for c in user1_name.lower() if c.isalnum())
        name2 = ''.join(c for c in user2_name.lower() if c.isalnum())

        if not name1:
            name1 = "user"
        if not name2:
            name2 = "user"

        strategies = [
            lambda n1, n2: f"{n1[:3]}{n2[-3:]}".capitalize(),
            lambda n1, n2: f"{n2[:2]}{n1[-2:]}".capitalize(),
            lambda n1, n2: f"{n1[:2]}{n2[:2]}".capitalize(),
            lambda n1, n2: f"{n1[:4]}{n2[:4]}".capitalize() if len(
                n1) > 3 and len(n2) > 3 else f"{n1}{n2}".capitalize()
        ]

        strategy = random.choice(strategies)
        ship_name = strategy(name1, name2)

        suffixes = ["", "ship", "love", "heart", "forever"]
        if random.random() < 0.3:
            suffix = random.choice(suffixes)
            if suffix:
                ship_name = f"{ship_name}{suffix.capitalize()}"

        return ship_name

    # QUERY FUNCTIONS

    # MUTUAL MENTIONS

    async def get_mutual_mentions(self, guild_id: int, user1_id: int, user2_id: int, days: int = 30) -> dict:

        if not self.pool:
            print("❌ No database pool available")
            return {'total': 0, 'user1_to_user2': 0, 'user2_to_user1': 0}

        cutoff_time = datetime.utcnow() - timedelta(days=days)

        query = """
        SELECT 
            COUNT(*) as total_mentions,
            SUM(CASE WHEN (mentioner_user_id = $1 AND mentioned_user_id = $2) THEN 1 ELSE 0 END) as a_to_b,
            SUM(CASE WHEN (mentioner_user_id = $2 AND mentioned_user_id = $1) THEN 1 ELSE 0 END) as b_to_a
        FROM user_mentions 
        WHERE guild_id = $3 
        AND created_at >= $4
        AND (
            (mentioner_user_id = $1 AND mentioned_user_id = $2) OR 
            (mentioner_user_id = $2 AND mentioned_user_id = $1)
        )
        """

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(
                    query,
                    user1_id,
                    user2_id,
                    guild_id,
                    cutoff_time
                )

            if result:

                total = result['total_mentions'] or 0
                a_to_b = result['a_to_b'] or 0
                b_to_a = result['b_to_a'] or 0

                return {
                    'total': total,
                    'user1_to_user2': a_to_b,
                    'user2_to_user1': b_to_a
                }
            else:
                return {'total': 0, 'user1_to_user2': 0, 'user2_to_user1': 0}

        except Exception as e:
            print(f"❌ Error in get_mutual_mentions: {e}")
            return {'total': 0, 'user1_to_user2': 0, 'user2_to_user1': 0}

    # ACTIVITY OVERLAP

    async def get_activity_overlap(self, guild_id: int, user1_id: int, user2_id: int, days: int = 30) -> float:

        if not self.pool:
            return 0.0

        cutoff_time = datetime.utcnow() - timedelta(days=days)

        query = """
        SELECT 
            user_id,
            EXTRACT(HOUR FROM created_at) as hour_of_day,
            COUNT(*) as message_count
        FROM message_tracking 
        WHERE guild_id = $1 
        AND created_at >= $2
        AND (user_id = $3 OR user_id = $4)
        AND is_bot = FALSE
        GROUP BY user_id, EXTRACT(HOUR FROM created_at)
        """

        try:
            async with self.pool.acquire() as conn:
                results = await conn.fetch(
                    query,
                    guild_id,
                    cutoff_time,
                    user1_id,
                    user2_id
                )

            user1_hours = {}
            user2_hours = {}

            for row in results:
                hour = int(row['hour_of_day'])
                count = row['message_count']
                user_id = row['user_id']

                if user_id == user1_id:
                    user1_hours[hour] = count
                elif user_id == user2_id:
                    user2_hours[hour] = count

            all_hours = set(list(user1_hours.keys()) +
                            list(user2_hours.keys()))
            if not all_hours:
                return 0.0

            user1_vector = [user1_hours.get(hour, 0) for hour in all_hours]
            user2_vector = [user2_hours.get(hour, 0) for hour in all_hours]

            dot_product = sum(
                u1 * u2 for u1, u2 in zip(user1_vector, user2_vector))

            user1_magnitude = math.sqrt(sum(u1 * u1 for u1 in user1_vector))
            user2_magnitude = math.sqrt(sum(u2 * u2 for u2 in user2_vector))

            if user1_magnitude == 0 or user2_magnitude == 0:
                return 0.0

            similarity = dot_product / (user1_magnitude * user2_magnitude)

            return max(0.0, min(1.0, similarity))

        except Exception as e:
            print(f"❌ Error in get_activity_overlap: {e}")
            return 0.0

    # SHARED CHANNELS

    async def get_shared_channels(self, guild_id: int, user1_id: int, user2_id: int, days: int = 30) -> float:

        if not self.pool:
            return 0.0

        cutoff_time = datetime.utcnow() - timedelta(days=days)

        query = """
        SELECT 
            channel_id,
            user_id,
            COUNT(*) as message_count
        FROM message_tracking 
        WHERE guild_id = $1 
        AND created_at >= $2
        AND (user_id = $3 OR user_id = $4)
        AND is_bot = FALSE
        GROUP BY channel_id, user_id
        """

        try:
            async with self.pool.acquire() as conn:
                results = await conn.fetch(
                    query,
                    guild_id,
                    cutoff_time,
                    user1_id,
                    user2_id
                )

            user1_channels = {}
            user2_channels = {}

            for row in results:
                channel_id = row['channel_id']
                count = row['message_count']
                user_id = row['user_id']

                if user_id == user1_id:
                    user1_channels[channel_id] = count
                elif user_id == user2_id:
                    user2_channels[channel_id] = count

            shared_channels = set(user1_channels.keys()) & set(
                user2_channels.keys())
            total_channels = set(user1_channels.keys()) | set(
                user2_channels.keys())

            if not total_channels:
                return 0.0

            shared_activity = 0
            total_activity = sum(user1_channels.values()) + \
                sum(user2_channels.values())

            for channel in shared_channels:
                shared_activity += user1_channels.get(
                    channel, 0) + user2_channels.get(channel, 0)

            if total_activity == 0:
                return 0.0

            channel_similarity = len(shared_channels) / len(total_channels)
            activity_ratio = shared_activity / total_activity

            score = channel_similarity * activity_ratio
            return max(0.0, min(1.0, score))

        except Exception as e:
            print(f"❌ Error in get_shared_channels: {e}")
            return 0.0

    # VC TIME

    async def get_vc_time_together(self, guild_id: int, user1_id: int, user2_id: int, days: int = 30) -> float:

        if not self.pool:
            return 0.0

        cutoff_time = datetime.utcnow() - timedelta(days=days)

        query = """
        SELECT COUNT(*) as shared_sessions
        FROM (
            SELECT DISTINCT channel_id, DATE_TRUNC('hour', join_time) as hour_bucket
            FROM voice_session_history 
            WHERE guild_id = $1 
            AND join_time >= $2
            AND user_id = $3
        ) u1
        JOIN (
            SELECT DISTINCT channel_id, DATE_TRUNC('hour', join_time) as hour_bucket
            FROM voice_session_history 
            WHERE guild_id = $1 
            AND join_time >= $2
            AND user_id = $4
        ) u2 ON u1.channel_id = u2.channel_id AND u1.hour_bucket = u2.hour_bucket
        """

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(
                    query,
                    guild_id,
                    cutoff_time,
                    user1_id,
                    user2_id
                )

            shared_sessions = result['shared_sessions'] if result else 0

            max_sessions = 10
            score = min(shared_sessions / max_sessions, 1.0)
            return score

        except Exception as e:
            print(f"❌ Error in get_vc_time_together: {e}")
            return 0.0

    # RECENTNESS

    async def get_recentness_factor(self, guild_id: int, user1_id: int, user2_id: int, days: int = 30) -> float:

        if not self.pool:
            return 0.0

        cutoff_time = datetime.utcnow() - timedelta(days=days)

        query = """
        SELECT MAX(created_at) as latest_interaction
        FROM user_mentions 
        WHERE guild_id = $1 
        AND created_at >= $2
        AND (
            (mentioned_user_id = $3 AND mentioner_user_id = $4) OR 
            (mentioned_user_id = $4 AND mentioner_user_id = $3)
        )
        """

        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow(
                    query,
                    guild_id,
                    cutoff_time,
                    user1_id,
                    user2_id
                )

            if not result or not result['latest_interaction']:
                return 0.0

            latest_interaction = result['latest_interaction']
            days_ago = (datetime.utcnow() - latest_interaction).days

            weight = math.exp(-0.1 * days_ago)

            return max(0.0, min(1.0, weight))

        except Exception as e:
            print(f"❌ Error in get_recentness_factor: {e}")
            return 0.0

    def _calculate_mentions_score(self, mentions_data: dict) -> float:

        total_mentions = mentions_data['user1_to_user2'] + \
            mentions_data['user2_to_user1']

        max_mentions = 20
        score = min(total_mentions / max_mentions, 1.0)

        if mentions_data['user1_to_user2'] > 0 and mentions_data['user2_to_user1'] > 0:
            balance_ratio = min(mentions_data['user1_to_user2'], mentions_data['user2_to_user1']) / max(
                mentions_data['user1_to_user2'], mentions_data['user2_to_user1'])

            score *= (0.7 + 0.3 * balance_ratio)

        return max(0.0, min(1.0, score))

    def _get_ship_tier(self, score: float) -> dict:

        score = max(0.0, min(1.0, score))
        if score >= 0.9:
            return {"name": "Soulmates 💖", "color": 0xFF69B4, "emoji": "💕"}
        elif score >= 0.8:
            return {"name": "Best Friends 💘", "color": 0xFF1493, "emoji": "💘"}
        elif score >= 0.7:
            return {"name": "Great Match 💝", "color": 0xFF6B6B, "emoji": "💝"}
        elif score >= 0.6:
            return {"name": "Good Match ❤️", "color": 0xFF4444, "emoji": "❤️"}
        elif score >= 0.5:
            return {"name": "Friends 💗", "color": 0xFF7F7F, "emoji": "💗"}
        elif score >= 0.4:
            return {"name": "Okay Match 💓", "color": 0xFF9999, "emoji": "💓"}
        elif score >= 0.3:
            return {"name": "Potential 💞", "color": 0xFFB6C1, "emoji": "💞"}
        elif score >= 0.1:
            return {"name": "Acquaintances 🤍", "color": 0xCCCCCC, "emoji": "🤍"}
        else:
            return {"name": "Strangers 🖤", "color": 0x666666, "emoji": "🖤"}

    # IMAGE GENERATION

    async def generate_ship_leaderboard_image(self, guild: discord.Guild, leaderboard_data: list,
                                              days_back: int, role_id: str = None, page: int = 0):

        try:
            image = Image.open(ShipConfig.TEMPLATE_PATH)
        except FileNotFoundError:
            image = Image.new('RGB', (800, 600), color='#2F3136')

        if image.mode != "RGB":
            image = image.convert("RGB")

        draw = ImageDraw.Draw(image)
        font_small, font_medium, font_large, font_larger, font_huge, font_giant = self.get_fonts()

        await ServerUIIntegration.add_server_profile_pic_and_name(
            image, guild, guild.name, font_huge, is_compatibility=False
        )

        # ROLE FILTER
        role_text = "No Filter"
        if role_id and role_id != "none":
            role = guild.get_role(int(role_id))
            role_text = role.name if role else "Unknown Role"
        self.draw_text_with_stroke(draw, (70, 424), role_text,
                                   font_small, "white", "black", 1)

        # TIME PERIOD
        self.draw_text_with_stroke(
            draw, (630, 422), f"{days_back} days", font_small, "white", "black", 1)

        # DATE
        self.draw_text_with_stroke(draw, (550, 38), datetime.now().strftime(
            "%B %d, %Y"), font_small, "white", "black", 1)

        # PAGINATION
        if leaderboard_data:
            total_ships = len(leaderboard_data)
            total_pages = max(
                1, (total_ships + self.ships_per_page - 1) // self.ships_per_page)
            self.draw_text_with_stroke(
                draw, (400, 450), f"Page {page + 1}/{total_pages}", font_medium, "white", "black", 1)
        else:
            total_pages = 1

        # LEADERBOARD CONTENT
        if leaderboard_data:
            start_idx = page * self.ships_per_page
            end_idx = min(start_idx + self.ships_per_page,
                          len(leaderboard_data))
            page_data = leaderboard_data[start_idx:end_idx]

            display_data = []
            for ship in page_data:
                user1 = guild.get_member(ship['user1_id'])
                user2 = guild.get_member(ship['user2_id'])

                user1_name = user1.name if user1 else f"User {ship['user1_id']}"
                user2_name = user2.name if user2 else f"User {ship['user2_id']}"

                score_percent = int(ship['compatibility_score'] * 100)
                ship_tier = self._get_ship_tier(ship['compatibility_score'])

                display_name = f"{user1_name} × {user2_name}"
                value_text = f"{score_percent}%"

                display_data.append(
                    (display_name, value_text, score_percent, ship_tier['emoji']))

            image_width, _ = image.size

            if len(display_data) <= 4:
                positions = [(60, 100 + i * 65)
                             for i in range(len(display_data))]
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
                    for i in range(len(display_data))
                ]

            for i, ((display_name, value_text, score_percent, heart_emoji), (box_x, box_y)) in enumerate(zip(display_data, positions)):
                global_rank = start_idx + i + 1

                self.draw_rounded_rectangle(draw, [box_x, box_y, box_x + box_width, box_y + box_height],
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

                self.draw_rounded_rectangle(draw, [box_x, box_y, box_x + placement_width, box_y + box_height],
                                            radius=8, fill=placement_color, outline=None)

                rank_text = f"#{global_rank}"
                rank_bbox = draw.textbbox((0, 0), rank_text, font=font_medium)
                rank_width = rank_bbox[2] - rank_bbox[0]

                value_bbox = draw.textbbox(
                    (0, 0), value_text, font=font_medium)
                value_width = value_bbox[2] - value_bbox[0]

                available_name_width = box_width - placement_width - \
                    value_width - 40

                current_display_name = display_name
                name_bbox = draw.textbbox(
                    (0, 0), current_display_name, font=font_medium)
                name_width = name_bbox[2] - name_bbox[0]

                if name_width > available_name_width:

                    try:
                        smaller_font = ImageFont.truetype(font_medium.path, 16) if hasattr(
                            font_medium, 'path') else font_small
                        smaller_bbox = draw.textbbox(
                            (0, 0), current_display_name, font=smaller_font)
                        smaller_width = smaller_bbox[2] - smaller_bbox[0]

                        if smaller_width <= available_name_width:
                            name_font = smaller_font
                        else:
                            name_font = font_medium
                            while len(current_display_name) > 3:
                                current_display_name = current_display_name[:-4] + "..."
                                truncated_bbox = draw.textbbox(
                                    (0, 0), current_display_name, font=name_font)
                                truncated_width = truncated_bbox[2] - \
                                    truncated_bbox[0]
                                if truncated_width <= available_name_width:
                                    break
                    except:
                        name_font = font_medium
                        while len(current_display_name) > 3:
                            current_display_name = current_display_name[:-4] + "..."
                            truncated_bbox = draw.textbbox(
                                (0, 0), current_display_name, font=name_font)
                            truncated_width = truncated_bbox[2] - \
                                truncated_bbox[0]
                            if truncated_width <= available_name_width:
                                break
                else:
                    name_font = font_medium

                rank_height = rank_bbox[3] - rank_bbox[1]
                name_height = name_bbox[3] - name_bbox[1] if name_font == font_medium else draw.textbbox(
                    (0, 0), current_display_name, font=name_font)[3] - draw.textbbox((0, 0), current_display_name, font=name_font)[1]
                value_height = value_bbox[3] - value_bbox[1]

                rank_y = box_y + (box_height - rank_height) // 2
                name_y = box_y + (box_height - name_height) // 2
                value_y = box_y + (box_height - value_height) // 2

                # Rank number
                rank_x = box_x + (placement_width - rank_width) // 2
                self.draw_text_with_stroke(draw, (rank_x, rank_y), rank_text,
                                           font_medium, "white", "black", 1)

                # Usernames
                name_x = box_x + placement_width + 8
                self.draw_text_with_stroke(draw, (name_x, name_y),
                                           current_display_name, name_font, "white", "black", 1)

                # Score percentage
                value_x = box_x + box_width - value_width - 30
                self.draw_text_with_stroke(draw, (value_x, value_y), value_text,
                                           font_medium, "white", "black", 1)

                # Heart emoji
                heart_x = box_x + box_width - 25
                heart_y = box_y + (box_height - 15) // 2

                with Pilmoji(image) as pilmoji:
                    pilmoji.text((heart_x, heart_y), heart_emoji,
                                 font=font_medium, embedded_color=True)
        else:
            cx, cy = image.size[0] // 2, image.size[1] // 2
            self.draw_text_with_stroke(draw, (cx - 150, cy - 30), "NO DATA AVAILABLE",
                                       font_giant, "white", "black", 3)

        img_bytes = io.BytesIO()
        image.save(img_bytes, format='PNG')
        img_bytes.seek(0)
        return img_bytes

    async def generate_ship_compatibility_image(self, guild: discord.Guild, user1: discord.Member, user2: discord.Member, compatibility_data: dict):

        try:
            image = Image.open(ShipConfig.SHIP_TEMPLATE_PATH).convert('RGB')
        except FileNotFoundError:
            print(f"❌ Template not found at {ShipConfig.SHIP_TEMPLATE_PATH}")

            image = Image.new('RGB', (800, 600), color='#2F3136')
            draw = ImageDraw.Draw(image)
            font_large = ImageFont.load_default()
            draw.text((400, 300), "Ship Compatibility",
                      fill="white", font=font_large, anchor="mm")

        draw = ImageDraw.Draw(image)
        font_small, font_medium, font_large, font_larger, font_huge, font_giant = self.get_fonts()

        await ServerUIIntegration.add_server_profile_pic_and_name(
            image, guild, guild.name, font_huge, is_compatibility=True
        )

        # CREATED ON DATE
        self.draw_text_with_stroke(draw, (570, 38), datetime.now().strftime(
            "%B %d, %Y"), font_small, "white", "black", 1)

        # USER 1 INFO
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(user1.display_avatar.url) as response:
                    avatar1_data = await response.read()
            avatar1 = Image.open(io.BytesIO(avatar1_data)).resize(
                (180, 180), Image.Resampling.LANCZOS).convert('RGBA')

            mask = Image.new('L', (180, 180), 0)
            mask_draw = ImageDraw.Draw(mask)
            mask_draw.ellipse((0, 0, 180, 180), fill=255)
            avatar1.putalpha(mask)

            avatar_area = image.crop((80, 100, 260, 280)).convert('RGBA')
            avatar_with_bg = Image.new('RGBA', (180, 180), (0, 0, 0, 0))
            avatar_with_bg.paste(avatar1, (0, 0), avatar1)
            avatar_area.paste(avatar_with_bg, (0, 0), avatar_with_bg)
            image.paste(avatar_area.convert('RGB'), (80, 100))

        except Exception as e:
            print(f"❌ Could not load user1 avatar: {e}")

        user1_name = user1.name
        name_font = font_larger
        max_width = 200

        bbox = draw.textbbox((0, 0), user1_name, font=name_font)
        name_width = bbox[2] - bbox[0]

        while name_width > max_width and name_font.size > 16:
            try:
                name_font = ImageFont.truetype(
                    name_font.path, name_font.size - 2)
                bbox = draw.textbbox((0, 0), user1_name, font=name_font)
                name_width = bbox[2] - bbox[0]
            except:
                break

        username1_x = 80 + (180 - name_width) // 2
        self.draw_text_with_stroke(draw, (username1_x, 290), user1_name,
                                   name_font, "white", "black", 2)

        # USER 2 INFO
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(user2.display_avatar.url) as response:
                    avatar2_data = await response.read()
            avatar2 = Image.open(io.BytesIO(avatar2_data)).resize(
                (180, 180), Image.Resampling.LANCZOS).convert('RGBA')

            mask = Image.new('L', (180, 180), 0)
            mask_draw = ImageDraw.Draw(mask)
            mask_draw.ellipse((0, 0, 180, 180), fill=255)
            avatar2.putalpha(mask)

            avatar_area = image.crop((490, 100, 670, 280)).convert('RGBA')
            avatar_with_bg = Image.new('RGBA', (180, 180), (0, 0, 0, 0))
            avatar_with_bg.paste(avatar2, (0, 0), avatar2)
            avatar_area.paste(avatar_with_bg, (0, 0), avatar_with_bg)
            image.paste(avatar_area.convert('RGB'), (490, 100))

        except Exception as e:
            print(f"❌ Could not load user2 avatar: {e}")

        user2_name = user2.name
        name_font = font_larger
        max_width = 200

        bbox = draw.textbbox((0, 0), user2_name, font=name_font)
        name_width = bbox[2] - bbox[0]

        while name_width > max_width and name_font.size > 16:
            try:
                name_font = ImageFont.truetype(
                    name_font.path, name_font.size - 2)
                bbox = draw.textbbox((0, 0), user2_name, font=name_font)
                name_width = bbox[2] - bbox[0]
            except:
                break

        username2_x = 490 + (180 - name_width) // 2
        self.draw_text_with_stroke(draw, (username2_x, 290), user2_name,
                                   name_font, "white", "black", 2)

        # COMPATIBILITY SCORE
        final_score = compatibility_data['final_score']
        ship_tier = self._get_ship_tier(final_score)
        compatibility_percentage = f"{int(final_score * 100)}%"

        score_color = self._get_hex_color(ship_tier['color'])

        compatibility_font = font_huge
        score_x = 335

        self.draw_text_with_stroke(draw, (score_x, 255), compatibility_percentage,
                                   compatibility_font, score_color, "black", 2)

        bbox = draw.textbbox(
            (0, 0), compatibility_percentage, font=compatibility_font)
        text_width = bbox[2] - bbox[0]
        heart_x = score_x + text_width + 10

        heart_font = font_larger

        with Pilmoji(image) as pilmoji:
            pilmoji.text((heart_x, 265), ship_tier['emoji'],
                         font=heart_font, embedded_color=True)

        # COMPATIBILITY SCORES BREAKDOWN
        breakdown = compatibility_data['breakdown']

        mentions_score = f"{int(breakdown['mentions']['score'] * 100)}%"
        activity_score = f"{int(breakdown['activity_overlap']['score'] * 100)}%"
        channels_score = f"{int(breakdown['shared_channels']['score'] * 100)}%"
        vc_score = f"{int(breakdown['vc_time']['score'] * 100)}%"
        recentness_score = f"{int(breakdown['recentness']['score'] * 100)}%"

        score_font = font_medium

        # Mentions score
        self.draw_text_with_stroke(draw, (102, 368), mentions_score,
                                   score_font, "white", "black", 1)

        # Activity overlap score
        self.draw_text_with_stroke(draw, (415, 368), activity_score,
                                   score_font, "white", "black", 1)

        # Shared channels score
        self.draw_text_with_stroke(draw, (665, 368), channels_score,
                                   score_font, "white", "black", 1)

        # VC time score
        self.draw_text_with_stroke(draw, (195, 410), vc_score,
                                   score_font, "white", "black", 1)

        # Recent activity score
        self.draw_text_with_stroke(draw, (540, 410), recentness_score,
                                   score_font, "white", "black", 1)

        img_bytes = io.BytesIO()
        image.save(img_bytes, format='PNG')
        img_bytes.seek(0)
        return img_bytes

    def _get_hex_color(self, color_int: int) -> str:

        return f"#{color_int:06x}"

    # FONTS

    def get_fonts(self):

        try:
            if os.path.exists(ShipConfig.FONT_PATH):
                font_small = ImageFont.truetype(ShipConfig.FONT_PATH, 16)
                font_medium = ImageFont.truetype(ShipConfig.FONT_PATH, 20)
                font_large = ImageFont.truetype(ShipConfig.FONT_PATH, 24)
                font_larger = ImageFont.truetype(ShipConfig.FONT_PATH, 30)
                font_huge = ImageFont.truetype(ShipConfig.FONT_PATH, 40)
                font_giant = ImageFont.truetype(ShipConfig.FONT_PATH, 60)
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
            try:
                font_small = ImageFont.truetype("Helvetica.ttf", 16)
                font_medium = ImageFont.truetype("Helvetica.ttf", 20)
                font_large = ImageFont.truetype("Helvetica.ttf", 24)
                font_larger = ImageFont.truetype("Helvetica.ttf", 30)
                font_huge = ImageFont.truetype("Helvetica.ttf", 40)
                font_giant = ImageFont.truetype("Helvetica.ttf", 60)
                return font_small, font_medium, font_large, font_larger, font_huge, font_giant
            except:
                font_small = ImageFont.load_default()
                font_medium = ImageFont.load_default()
                font_large = ImageFont.load_default()
                font_larger = ImageFont.load_default()
                font_huge = ImageFont.load_default()
                font_giant = ImageFont.load_default()
                return font_small, font_medium, font_large, font_larger, font_huge, font_giant

    # STROKE

    def draw_text_with_stroke(self, draw, position, text, font, fill, stroke_fill, stroke_width):

        x, y = position

        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw.text((x + dx, y + dy), text,
                              font=font, fill=stroke_fill)

        draw.text((x, y), text, font=font, fill=fill)

    # INVISBLE RECTANGLEFUNCTION

    def draw_rounded_rectangle(self, draw, xy, radius, fill=None, outline=None):

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

    # COMMANDS

    ship = app_commands.Group(
        name="ship", description="Ship compatibility commands")

    @ship.command(
        name="compatibility",
        description="Check compatibility between two users based on server activity"
    )
    @app_commands.describe(
        user1="First user to check compatibility with",
        user2="Second user to check compatibility with"
    )
    async def ship_compatibility(self, interaction: discord.Interaction,
                                 user1: discord.Member,
                                 user2: discord.Member):

        await interaction.response.defer()

        if user1.bot or user2.bot:
            error_embed = discord.Embed(
                title="❌ Error",
                description="I can't ship bots with users!",
                color=0xFF0000
            )
            await interaction.followup.send(embed=error_embed)
            return

        if user1.id == user2.id:
            error_embed = discord.Embed(
                title="❌ Error",
                description="You can't ship someone with themselves! Try shipping with another user.",
                color=0xFF0000
            )
            await interaction.followup.send(embed=error_embed)
            return

        try:
            compatibility = await self.calculate_compatibility_score(interaction.guild_id, user1.id, user2.id)

            ship_name = self._get_ship_name(
                user1.display_name, user2.display_name)

            await self.store_ship_score(
                interaction.guild_id, user1.id, user2.id, ship_name, compatibility
            )

            img_bytes = await self.generate_ship_compatibility_image(interaction.guild, user1, user2, compatibility)

            file = discord.File(img_bytes, filename="ship_compatibility.png")
            await interaction.followup.send(file=file)

        except Exception as e:
            print(f"❌ Error in ship command: {e}")
            import traceback
            traceback.print_exc()
            error_embed = discord.Embed(
                title="❌ Error",
                description="Couldn't calculate compatibility. Make sure both users have been active on this server!",
                color=0xFF0000
            )
            await interaction.followup.send(embed=error_embed)

    @ship.command(
        name="leaderboard",
        description="Show the top ship compatibility scores on this server"
    )
    async def ship_leaderboard(self, interaction: discord.Interaction):

        await interaction.response.defer()

        try:
            leaderboard_data = await self.get_ship_leaderboard_data(
                interaction.guild.id, ShipConfig.DEFAULT_DAYS_BACK
            )

            if not leaderboard_data:
                embed = discord.Embed(
                    title="🏆 Ship Leaderboard",
                    description="No ships have been calculated yet! Use `/ship compatibility` to check compatibility between users.",
                    color=0x5865F2
                )
                await interaction.followup.send(embed=embed)
                return

            img_bytes = await self.generate_ship_leaderboard_image(
                guild=interaction.guild,
                leaderboard_data=leaderboard_data,
                days_back=ShipConfig.DEFAULT_DAYS_BACK,
                page=0
            )

            file = discord.File(img_bytes, filename="ship_leaderboard.png")

            view = ShipLeaderboardView(
                self, interaction.guild, days_back=ShipConfig.DEFAULT_DAYS_BACK
            )

            await interaction.followup.send(file=file, view=view)

            message = await interaction.original_response()
            self.active_views[message.id] = view

        except Exception as e:
            print(f"❌ Error in ship leaderboard command: {e}")
            import traceback
            traceback.print_exc()
            error_embed = discord.Embed(
                title="❌ Error",
                description="Couldn't load the ship leaderboard. Please try again later.",
                color=0xFF0000
            )
            await interaction.followup.send(embed=error_embed)


# SETUP

async def setup(bot):
    await bot.add_cog(ShipCog(bot))
