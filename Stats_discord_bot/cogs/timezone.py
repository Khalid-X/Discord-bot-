import os
import discord
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timedelta, timezone
import matplotlib.pyplot as plt
import numpy as np
from io import BytesIO
import asyncio
from PIL import Image, ImageDraw, ImageFont
import aiohttp
import io
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


# INITIALIZATION

class TimezoneDistribution(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_cog = None
        self.active_sessions = {}
        self.background_image_path = BASE_DIR / "assets" / \
            "images" / "timezone chart final png.png"
        self.font_path = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"

    async def cog_load(self):

        self.db_cog = self.bot.get_cog('DatabaseStats')
        if not self.db_cog:
            print("⚠️ TimezoneDistribution: DatabaseStats cog not found!")

    # QUERY FUNCTION

    async def get_hourly_activity_data(self, guild_id: int, activity_type: str, days_back: int = 30, role_filter_ids: list = None):

        try:
            if not self.db_cog:
                print("⚠️ DatabaseStats cog not initialized")
                return [0] * 24

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            if activity_type == "Messages":
                result = await self.db_cog.q_server_activity_distribution(
                    guild_id=guild_id,
                    days_back=days_back,
                    role_filter_ids=role_filter_ids,
                    timezone_str='UTC'
                )
                messages_data = result.get('messages', {})

                hourly_data = [messages_data.get(
                    f'hour_{i}', 0) for i in range(24)]
                return hourly_data

            elif activity_type == "Voice Activity":
                result = await self.db_cog.q_server_activity_distribution(
                    guild_id=guild_id,
                    days_back=days_back,
                    role_filter_ids=role_filter_ids,
                    timezone_str='UTC'
                )
                voice_data = result.get('voice', {})

                hourly_data = [voice_data.get(
                    f'hour_{i}', 0) for i in range(24)]
                return hourly_data

            elif activity_type == "Activities":
                result = await self.db_cog.q_server_activity_distribution(
                    guild_id=guild_id,
                    days_back=days_back,
                    role_filter_ids=role_filter_ids,
                    timezone_str='UTC'
                )
                activities_data = result.get('activities', {})

                hourly_data = [activities_data.get(
                    f'hour_{i}', 0) for i in range(24)]
                return hourly_data

            else:
                return [0] * 24

        except Exception as e:
            print(f"⚠️ Error in get_hourly_activity_data: {e}")
            import traceback
            traceback.print_exc()
            return [0] * 24

    # TIMEZONE OFFSETS

    def apply_timezone_offset(self, hourly_data: list, timezone: str):

        timezone_offsets = {
            "UTC": 0, "GMT": 0,
            "EST": -5, "PST": -8, "CST": -6, "MST": -7,
            "CET": 1, "EET": 2, "IST": 5.5, "JST": 9,
            "AEST": 10, "NZST": 12
        }

        offset = timezone_offsets.get(timezone, 0)
        offset_hours = int(offset)

        shifted_data = [0] * 24
        for i in range(24):
            new_index = (i - offset_hours) % 24
            shifted_data[new_index] = hourly_data[i]

        return shifted_data

    # ACTIVITY LEVELS LOGIC

    def categorize_activity_levels(self, hourly_data: list):

        total_activity = sum(hourly_data)
        if total_activity == 0:
            return [("Low Activity", 0)] * 24

        avg_activity = total_activity / 24

        activity_levels = []
        peak_threshold = avg_activity * 2.0
        high_threshold = avg_activity * 1.5
        medium_threshold = avg_activity * 0.8

        for hour_activity in hourly_data:
            if hour_activity >= peak_threshold:
                activity_levels.append(("Peak Activity", hour_activity))
            elif hour_activity >= high_threshold:
                activity_levels.append(("High Activity", hour_activity))
            elif hour_activity >= medium_threshold:
                activity_levels.append(("Medium Activity", hour_activity))
            else:
                activity_levels.append(("Low Activity", hour_activity))

        return activity_levels

    # DRAWING FUNCTIONS

    def draw_text_with_stroke(self, draw, position, text, font, fill_color, stroke_color, stroke_width):

        x, y = position

        for dx in range(-stroke_width, stroke_width + 1):
            for dy in range(-stroke_width, stroke_width + 1):
                if dx != 0 or dy != 0:
                    draw.text((x + dx, y + dy), text,
                              font=font, fill=stroke_color)

        draw.text((x, y), text, font=font, fill=fill_color)

    async def create_clock_pie_chart(self, activity_levels: list, timezone: str, activity_type: str, guild: discord.Guild, days_back: int):

        # FONTS

        try:

            background = Image.open(self.background_image_path).convert('RGBA')
            image = background.copy()
            draw = ImageDraw.Draw(image)

            try:
                font_huge = ImageFont.truetype(self.font_path, 40)
                font_large = ImageFont.truetype(self.font_path, 24)
                font_medium = ImageFont.truetype(self.font_path, 20)
                font_small = ImageFont.truetype(self.font_path, 16)
                font_clock = ImageFont.truetype(self.font_path, 16)
            except Exception as font_error:
                print(f"Font loading error: {font_error}")
                font_huge = ImageFont.load_default()
                font_large = ImageFont.load_default()
                font_medium = ImageFont.load_default()
                font_small = ImageFont.load_default()
                font_clock = ImageFont.load_default()

            def draw_text_with_stroke_fixed(draw, position, text, font, fill, stroke_fill, stroke_width):

                x, y = position
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw.text((x + dx, y + dy), text,
                                      font=font, fill=stroke_fill)

                draw.text((x, y), text, font=font, fill=fill)

            def fit_text_to_rectangle(text, text_start_x, text_start_y, rect_center_x, rect_center_y, rect_width, rect_height, is_username=False):

                font_paths = [self.font_path] if os.path.exists(
                    self.font_path) else []

                rect_left = rect_center_x - (rect_width // 2)
                rect_right = rect_center_x + (rect_width // 2)
                rect_top = rect_center_y - (rect_height // 2)
                rect_bottom = rect_center_y + (rect_height // 2)

                font_sizes = [40, 38, 36, 34, 32,
                              30, 28, 26, 24, 22, 20, 18, 16]

                for font_size in font_sizes:
                    try:
                        font = ImageFont.truetype(
                            font_paths[0], font_size) if font_paths else ImageFont.load_default()
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
                    font = ImageFont.truetype(
                        font_paths[0], smallest_font) if font_paths else ImageFont.load_default()
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

            # Profile picture
            avatar_x, avatar_y = 7, 10
            avatar_size = (60, 60)

            # Server name
            text_start_x = 85
            text_start_y = 30

            # Invisible Rectangle
            username_rectangle = {"center": (
                150, 30), "width": 183, "height": 35}

            # SERVER PROFILE PICTURE

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
                    image.paste(avatar_area.convert(
                        'RGB'), (avatar_x, avatar_y))

            except Exception as e:
                print(f"❌ Could not load server icon: {e}")

            server_name = guild.name
            fitted_text, text_font, text_pos = fit_text_to_rectangle(
                server_name,
                text_start_x,
                text_start_y,
                username_rectangle["center"][0],
                username_rectangle["center"][1],
                username_rectangle["width"],
                username_rectangle["height"],
                is_username=True
            )

            if fitted_text and text_font:
                draw_text_with_stroke_fixed(draw, text_pos, fitted_text, text_font,
                                            "white", "black", 1)
            else:

                draw_text_with_stroke_fixed(draw, (text_start_x, text_start_y), server_name,
                                            font_huge, "white", "black", 1)

            # CREATED ON DATE
            draw_text_with_stroke_fixed(draw, (560, 38), datetime.now().strftime("%B %d, %Y"),
                                        font_small, "white", "black", 1)

            # TIME PERIOD
            time_period_text = f"{days_back} days"
            draw_text_with_stroke_fixed(
                draw, (105, 423), time_period_text, font_medium, "white", "black", 1)

            # ACTIVITY LEVEL DRAWING

            colors = {
                "Peak Activity": (255, 215, 0, 255),
                "High Activity": (255, 107, 107, 255),
                "Medium Activity": (81, 207, 102, 255),
                "Low Activity": (134, 142, 150, 255)
            }

            center_x, center_y = 380, 240
            radius = 180

            clock_layer = Image.new('RGBA', image.size, (0, 0, 0, 0))
            clock_draw = ImageDraw.Draw(clock_layer)

            for i, (category, _) in enumerate(activity_levels):
                start_angle = i * 15 - 90
                end_angle = (i + 1) * 15 - 90

                color = colors[category]

                clock_draw.pieslice([center_x - radius, center_y - radius,
                                     center_x + radius, center_y + radius],
                                    start_angle, end_angle, fill=color, outline="white", width=1)

            inner_radius = 10
            clock_draw.ellipse([center_x - inner_radius, center_y - inner_radius,
                                center_x + inner_radius, center_y + inner_radius],
                               fill=(0, 0, 0, 255))

            # CLOCK NUMBERS

            number_radius = radius + 15

            for i in range(24):
                angle = np.radians(i * 15 - 90)

                x = center_x + number_radius * np.cos(angle)
                y = center_y + number_radius * np.sin(angle)

                try:
                    bbox = clock_draw.textbbox(
                        (0, 0), str(i+1), font=font_clock)
                    text_width = bbox[2] - bbox[0]
                    text_height = bbox[3] - bbox[1]
                    text_x = x - text_width / 2
                    text_y = y - text_height / 2
                except:
                    text_x = x - 8
                    text_y = y - 8

                draw_text_with_stroke_fixed(clock_draw, (text_x, text_y), str(i+1),
                                            font_clock, "white", "black", 2)

            image = Image.alpha_composite(image, clock_layer)

            buffer = BytesIO()
            image.save(buffer, format='PNG')
            buffer.seek(0)

            return buffer

        except Exception as e:
            print(f"Error creating Pillow chart: {e}")
            import traceback
            traceback.print_exc()
            return self.create_clock_pie_chart_fallback(activity_levels, timezone, activity_type)

    def create_clock_pie_chart_fallback(self, activity_levels: list, timezone: str, activity_type: str):

        plt.style.use('default')
        fig, ax = plt.subplots(
            figsize=(10, 10), subplot_kw=dict(projection='polar'))

        colors = {
            "Peak Activity": "#FFD700",
            "High Activity": "#FF6B6B",
            "Medium Activity": "#51CF66",
            "Low Activity": "#868E96"
        }

        theta = np.linspace(0, 2*np.pi, 24, endpoint=False)
        width = 2*np.pi / 24

        for i, (category, _) in enumerate(activity_levels):
            color = colors[category]
            ax.bar(theta[i], 1, width=width, bottom=0, color=color,
                   alpha=1.0, edgecolor='white', linewidth=1)

        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_ylim(0, 1)
        ax.set_yticklabels([])
        ax.grid(False)
        ax.spines['polar'].set_visible(False)

        buffer = BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight',
                    dpi=100, transparent=False, facecolor='white')
        buffer.seek(0)
        plt.close()

        return buffer

    # TIME MODAL

    class TimezoneTimeModal(discord.ui.Modal, title='Custom Time Period'):
        def __init__(self, cog_instance, guild):
            super().__init__(timeout=300)
            self.cog = cog_instance
            self.guild = guild

        days = discord.ui.TextInput(
            label='Enter number of days',
            placeholder='e.g., 7, 14, 30, 90, 2000...',
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

                await interaction.response.defer(ephemeral=False)

                message = interaction.message
                if not message:
                    await interaction.followup.send("❌ Could not find original message.", ephemeral=True)
                    return

                if message.id in self.cog.active_sessions:
                    session_data = self.cog.active_sessions[message.id]
                    view = session_data.get('view')

                    if view:
                        view.current_days = days
                        await view.update_message(interaction)
                    else:
                        await interaction.followup.send("❌ Could not update chart.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Session expired. Please run the command again.", ephemeral=True)

            except ValueError:
                await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
            except Exception as e:
                print(f"Error in timezone modal submit: {e}")
                await interaction.followup.send("❌ An error occurred while updating stats.", ephemeral=True)

    # ACTIVITY TYPE DROPDOWN MENU

    class ActivityTypeSelect(discord.ui.Select):
        def __init__(self, current_type: str = "Messages"):
            options = [
                discord.SelectOption(
                    label="Messages",
                    description="Message activity distribution",
                    value="Messages",
                    default=(current_type == "Messages")
                ),
                discord.SelectOption(
                    label="Voice Activity",
                    description="Voice time distribution",
                    value="Voice Activity",
                    default=(current_type == "Voice Activity")
                ),
                discord.SelectOption(
                    label="Activities",
                    description="General activity distribution",
                    value="Activities",
                    default=(current_type == "Activities")
                )
            ]

            super().__init__(
                placeholder="Select activity type...",
                options=options,
                custom_id="activity_type_select"
            )

        async def callback(self, interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=False, thinking=False)
            view = self.view
            view.activity_type = self.values[0]
            await view.update_message(interaction)

    # TIMEZONE DROPDOWN MENU

    class TimezoneSelect(discord.ui.Select):
        def __init__(self, current_timezone: str = "UTC"):
            options = [
                discord.SelectOption(label="UTC (GMT+0)", value="UTC",
                                     description="Coordinated Universal Time", default=(current_timezone == "UTC")),
                discord.SelectOption(label="EST (GMT-5)", value="EST",
                                     description="Eastern Standard Time", default=(current_timezone == "EST")),
                discord.SelectOption(label="PST (GMT-8)", value="PST",
                                     description="Pacific Standard Time", default=(current_timezone == "PST")),
                discord.SelectOption(label="CST (GMT-6)", value="CST",
                                     description="Central Standard Time", default=(current_timezone == "CST")),
                discord.SelectOption(label="MST (GMT-7)", value="MST",
                                     description="Mountain Standard Time", default=(current_timezone == "MST")),
                discord.SelectOption(label="GMT (GMT+0)", value="GMT",
                                     description="Greenwich Mean Time", default=(current_timezone == "GMT")),
                discord.SelectOption(label="CET (GMT+1)", value="CET",
                                     description="Central European Time", default=(current_timezone == "CET")),
                discord.SelectOption(label="EET (GMT+2)", value="EET",
                                     description="Eastern European Time", default=(current_timezone == "EET")),
                discord.SelectOption(label="IST (GMT+5:30)", value="IST",
                                     description="Indian Standard Time", default=(current_timezone == "IST")),
                discord.SelectOption(label="JST (GMT+9)", value="JST",
                                     description="Japan Standard Time", default=(current_timezone == "JST")),
                discord.SelectOption(label="AEST (GMT+10)", value="AEST",
                                     description="Australian Eastern Time", default=(current_timezone == "AEST")),
                discord.SelectOption(label="NZST (GMT+12)", value="NZST",
                                     description="New Zealand Standard Time", default=(current_timezone == "NZST")),
            ]

            super().__init__(
                placeholder="Select timezone...",
                options=options,
                custom_id="timezone_select"
            )

        async def callback(self, interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=False, thinking=False)
            view = self.view
            view.timezone = self.values[0]
            await view.update_message(interaction)

    # MAIN VIEW

    class TimezoneActivityView(discord.ui.View):
        def __init__(self, cog, guild: discord.Guild, timezone: str = "UTC", activity_type: str = "Messages", days_back: int = 30):
            super().__init__(timeout=600)
            self.cog = cog
            self.guild = guild
            self.timezone = timezone
            self.activity_type = activity_type
            self.current_days = days_back
            self.show_time_buttons = False
            self.role_filter_ids = None
            self._update_buttons()

        def _update_buttons(self):
            self.clear_items()

            self.add_item(TimezoneDistribution.TimezoneSelect(self.timezone))
            self.add_item(
                TimezoneDistribution.ActivityTypeSelect(self.activity_type))

            refresh_button = discord.ui.Button(
                style=discord.ButtonStyle.secondary,
                label="🔄",
                custom_id="timezone_refresh"
            )
            refresh_button.callback = self.refresh_callback

            time_settings_button = discord.ui.Button(
                style=discord.ButtonStyle.secondary,
                label="⏰ Time Settings",
                custom_id="timezone_time_settings"
            )
            time_settings_button.callback = self.time_settings_callback

            self.add_item(refresh_button)
            self.add_item(time_settings_button)

            if self.show_time_buttons:
                days_7_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == 7 else discord.ButtonStyle.secondary,
                    label="7 Days",
                    custom_id="timezone_days_7"
                )
                days_7_button.callback = self.days_7_callback

                days_14_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == 14 else discord.ButtonStyle.secondary,
                    label="14 Days",
                    custom_id="timezone_days_14"
                )
                days_14_button.callback = self.days_14_callback

                days_30_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == 30 else discord.ButtonStyle.secondary,
                    label="30 Days",
                    custom_id="timezone_days_30"
                )
                days_30_button.callback = self.days_30_callback

                days_90_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == 90 else discord.ButtonStyle.secondary,
                    label="90 Days",
                    custom_id="timezone_days_90"
                )
                days_90_button.callback = self.days_90_callback

                custom_button = discord.ui.Button(
                    style=discord.ButtonStyle.success,
                    label=f"Custom ({self.current_days}d)" if self.current_days not in [
                        7, 14, 30, 90] else "Custom",
                    custom_id="timezone_custom_days"
                )
                custom_button.callback = self.custom_days_callback

                self.add_item(days_7_button)
                self.add_item(days_14_button)
                self.add_item(days_30_button)
                self.add_item(days_90_button)
                self.add_item(custom_button)

        async def refresh_callback(self, interaction: discord.Interaction):
            await self.handle_refresh(interaction)

        async def time_settings_callback(self, interaction: discord.Interaction):
            await self.handle_time_settings(interaction)

        async def days_7_callback(self, interaction: discord.Interaction):
            await self.handle_time_period_change(interaction, 7)

        async def days_14_callback(self, interaction: discord.Interaction):
            await self.handle_time_period_change(interaction, 14)

        async def days_30_callback(self, interaction: discord.Interaction):
            await self.handle_time_period_change(interaction, 30)

        async def days_90_callback(self, interaction: discord.Interaction):
            await self.handle_time_period_change(interaction, 90)

        async def custom_days_callback(self, interaction: discord.Interaction):
            await self.handle_custom_days(interaction)

        async def handle_refresh(self, interaction: discord.Interaction):

            await interaction.response.defer(ephemeral=False, thinking=False)
            await self.update_message(interaction)

        async def handle_time_settings(self, interaction: discord.Interaction):

            await interaction.response.defer(ephemeral=False, thinking=False)
            self.show_time_buttons = not self.show_time_buttons
            self._update_buttons()
            await interaction.edit_original_response(view=self)

        async def handle_time_period_change(self, interaction: discord.Interaction, days: int):

            await interaction.response.defer(ephemeral=False, thinking=False)
            self.current_days = days
            self.show_time_buttons = False
            await self.update_message(interaction)

        async def handle_custom_days(self, interaction: discord.Interaction):

            modal = TimezoneDistribution.TimezoneTimeModal(
                self.cog, self.guild)
            await interaction.response.send_modal(modal)

        async def generate_chart(self):

            hourly_data = await self.cog.get_hourly_activity_data(
                self.guild.id,
                self.activity_type,
                self.current_days,
                self.role_filter_ids
            )
            timezone_data = self.cog.apply_timezone_offset(
                hourly_data, self.timezone)
            activity_levels = self.cog.categorize_activity_levels(
                timezone_data)
            chart_buffer = await self.cog.create_clock_pie_chart(
                activity_levels, self.timezone, self.activity_type, self.guild, self.current_days
            )
            return chart_buffer

        async def update_message(self, interaction: discord.Interaction):

            try:
                chart_buffer = await self.generate_chart()
                self._update_buttons()

                file = discord.File(
                    chart_buffer, filename="timezone_activity.png")

                content = f"*Showing {self.activity_type.lower()} activity for the past {self.current_days} days in {self.timezone}*"

                if interaction.response.is_done():
                    await interaction.edit_original_response(content=content, attachments=[file], view=self)
                else:
                    await interaction.response.edit_message(content=content, attachments=[file], view=self)

            except Exception as e:
                print(f"Error updating timezone activity message: {e}")
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message("❌ An error occurred while updating the activity chart.", ephemeral=True)
                    else:
                        await interaction.followup.send("❌ An error occurred while updating the activity chart.", ephemeral=True)
                except:
                    pass

    # COMMAND

    timezone_group = app_commands.Group(
        name="timezone",
        description="Timezone-related commands"
    )

    activity_group = app_commands.Group(
        name="activity",
        description="Activity-related commands",
        parent=timezone_group
    )

    @activity_group.command(name="distribution", description="Show 24-hour activity distribution in different timezones")
    async def timezone_activity_distribution(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            if not self.db_cog:
                await interaction.followup.send("❌ Database connection not available. Please make sure the DatabaseStats cog is loaded.", ephemeral=True)
                return

            view = self.TimezoneActivityView(
                self,
                interaction.guild,
                timezone="UTC",
                activity_type="Messages",
                days_back=30
            )

            chart_buffer = await view.generate_chart()
            file = discord.File(chart_buffer, filename="timezone_activity.png")

            content = f"*Showing message activity for the past 30 days in UTC*"

            message = await interaction.followup.send(content=content, file=file, view=view)

            self.active_sessions[message.id] = {
                'guild_id': interaction.guild.id,
                'user_id': interaction.user.id,
                'timezone': "UTC",
                'activity_type': "Messages",
                'current_days': 30,
                'view': view
            }

        except Exception as e:
            print(f"Error in timezone activity distribution command: {e}")
            import traceback
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred while generating the activity chart. Please try again later.", ephemeral=True)

    async def cog_unload(self):

        self.active_sessions.clear()
        print("TimezoneDistribution cog: Cleaned up active sessions")


# SETUP

async def setup(bot):
    await bot.add_cog(TimezoneDistribution(bot))
