import numpy as np
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import discord
from discord.ext import commands
from discord import app_commands
import asyncpg
import asyncio
from datetime import datetime, timedelta
import io
import os
from typing import Optional, Dict, List, Tuple, Set, Any
from dotenv import load_dotenv
import aiohttp
import json
from datetime import timezone
from datetime import datetime, timedelta, date
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


# ROLE SELECT MENU

class ChartRoleSelectMenu(discord.ui.Select):
    def __init__(self, guild: discord.Guild, current_role_id: str = None):
        self.guild = guild

        roles = [role for role in guild.roles if role.name != "@everyone"]
        roles.sort(key=lambda x: x.position, reverse=True)
        roles = roles[:25]

        options = [
            discord.SelectOption(
                label="No Filter",
                value="none",
                description="Show all users",
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
            custom_id="chart_role_filter_select",
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        view.selected_role_id = self.values[0]
        await interaction.response.defer()
        await view.update_chart(interaction)


# TIME SETTINGS MODAL

class ChartTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, view):
        super().__init__(timeout=300)
        self.view = view

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
            print(f"Error in chart modal submit: {e}")
            await interaction.followup.send("❌ An error occurred while updating chart.", ephemeral=True)


# CHART SELECT MENU

class ChartSelectMenu(discord.ui.Select):
    def __init__(self, options: List[str], current_selection: str):
        select_options = []

        for option in options:
            select_options.append(
                discord.SelectOption(
                    label=option,
                    value=option.lower().replace(' ', '_'),
                    emoji="📊",
                    default=(option.lower().replace(
                        ' ', '_') == current_selection)
                )
            )

        super().__init__(
            placeholder="Select chart type...",
            options=select_options,
            custom_id="chart_type_select",
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        view.current_selection = self.values[0]
        await interaction.response.defer()
        await view.update_chart(interaction)


# MAIN CHART VIEW

class ChartView(discord.ui.View):
    def __init__(self, cog, chart_type: str, target_id: int, guild_id: int, days: int = 14, role_id: Optional[str] = None):
        super().__init__(timeout=600)
        self.cog = cog
        self.chart_type = chart_type
        self.target_id = int(target_id)
        self.guild_id = int(guild_id)
        self.current_days = int(days)
        self.selected_role_id = role_id
        self.show_time_buttons = False
        self.guild = cog.bot.get_guild(self.guild_id)

        if chart_type == "user" or chart_type == "server":
            self.chart_options = ["Messages", "Voice Activity", "Invites"]
            self.current_selection = "messages"
        else:
            self.chart_options = ["Messages", "Voice Activity"]
            self.current_selection = "messages"

        self._update_buttons()

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

    def _update_buttons(self):
        self.clear_items()

        chart_select = ChartSelectMenu(
            self.chart_options, self.current_selection)
        self.add_item(chart_select)

        if self.chart_type != "user":
            if self.guild:
                role_select = ChartRoleSelectMenu(
                    self.guild, self.selected_role_id)
                role_select.row = 1
                self.add_item(role_select)

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="chart_refresh",
            row=2
        )
        refresh_button.callback = self.refresh_callback
        self.add_item(refresh_button)

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="chart_time_settings",
            row=2
        )
        time_settings_button.callback = self.time_settings_callback
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            days_7_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 7 else discord.ButtonStyle.secondary,
                label="7 Days",
                custom_id="chart_days_7",
                row=3
            )
            days_7_button.callback = self.days_7_callback
            self.add_item(days_7_button)

            days_14_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 14 else discord.ButtonStyle.secondary,
                label="14 Days",
                custom_id="chart_days_14",
                row=3
            )
            days_14_button.callback = self.days_14_callback
            self.add_item(days_14_button)

            days_30_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 30 else discord.ButtonStyle.secondary,
                label="30 Days",
                custom_id="chart_days_30",
                row=3
            )
            days_30_button.callback = self.days_30_callback
            self.add_item(days_30_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label="Custom",
                custom_id="chart_custom_days",
                row=3
            )
            custom_button.callback = self.custom_days_callback
            self.add_item(custom_button)

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

        modal = ChartTimeModal(self)
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
            data = await self.fetch_chart_data()
            if not data:
                await self._safe_edit_response(interaction, content="❌ No data available for the selected parameters", view=self)
                return
            if data.get('error'):
                await self._safe_edit_response(interaction, content=f"❌ {data['error']}", view=self)
                return

            chart_img, timestamps, y_labels = self.cog.generate_professional_chart(
                data, self.current_selection)
            label = self.create_label(data)
            final_image_bytes = await self.cog.draw_chart_on_template(
                chart_img, label, self.current_days, self.selected_role_id,
                timestamps, y_labels, self.current_selection, self.chart_type, self.target_id, self.guild_id)

            file = discord.File(final_image_bytes, filename='chart.png')
            self._update_buttons()
            await self._safe_edit_response(interaction, content=None, attachments=[file], view=self)

        except Exception as e:
            print(f"Error updating chart: {e}")
            try:
                await self._safe_edit_response(interaction, content="❌ Error generating chart", view=self)
            except:
                pass

    def create_label(self, data: Dict) -> str:

        chart_type = self.current_selection
        if chart_type == "messages":
            total = data.get('total', 0)
            return f"🔵 Messages {total}"
        elif chart_type == "voice_activity":
            total = data.get('total', 0)
            return f"🔴 Voice Activity {self.cog.format_duration_dynamic(total)}"
        elif chart_type == "invites":
            total = data.get('total', 0)
            return f"🟢 Invites {total}"
        return "🟢 Chart"

    async def fetch_chart_data(self) -> Dict:

        chart_type = self.current_selection
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=self.current_days)

        role_filter_ids = None
        if self.selected_role_id and self.selected_role_id != "none":
            try:
                role_filter_ids = [int(self.selected_role_id)]
            except ValueError:
                role_filter_ids = None

        db_stats = self.cog.bot.get_cog('DatabaseStats')
        if not db_stats:
            return {'error': 'Database system not available'}

        guild = self.cog.bot.get_guild(self.guild_id)
        if not guild:
            return {'error': 'Guild not found'}

        try:
            if self.current_days <= 2:
                intervals = 24 * self.current_days
                time_delta = timedelta(hours=1)
            elif self.current_days <= 30:
                intervals = self.current_days
                time_delta = timedelta(days=1)
            elif self.current_days <= 180:
                intervals = self.current_days // 7
                time_delta = timedelta(days=7)
            else:
                intervals = self.current_days // 30
                time_delta = timedelta(days=30)

            points = []
            total = 0

            for i in range(intervals):
                interval_start = end_time - time_delta * (i + 1)
                interval_end = end_time - time_delta * i

                if self.current_days <= 2:
                    date_obj = interval_start
                else:
                    date_obj = interval_start.date()

                if self.chart_type == "user":
                    if chart_type == "messages":
                        result = await db_stats.q_user_total_messages(
                            guild_id=self.guild_id,
                            user_id=self.target_id,
                            role_filter_ids=role_filter_ids,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        count = result.get('total_messages', 0)
                        points.append({'date': date_obj, 'count': count})
                        total += count

                    elif chart_type == "voice_activity":
                        result = await db_stats.q_user_total_voice(
                            guild_id=self.guild_id,
                            user_id=self.target_id,
                            role_filter_ids=role_filter_ids,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        seconds = result.get('total_seconds', 0)
                        points.append(
                            {'date': date_obj, 'total_seconds': seconds})
                        total += seconds

                    elif chart_type == "invites":
                        result = await db_stats.q_user_invite_stats(
                            guild_id=self.guild_id,
                            user_id=self.target_id,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        count = result.get('total_invites', 0)
                        points.append({'date': date_obj, 'count': count})
                        total += count

                elif self.chart_type == "server":
                    if chart_type == "messages":
                        result = await db_stats.q_server_total_messages(
                            guild_id=self.guild_id,
                            role_filter_ids=role_filter_ids,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        count = result.get('total_messages', 0)
                        points.append({'date': date_obj, 'count': count})
                        total += count

                    elif chart_type == "voice_activity":
                        result = await db_stats.q_server_total_voice(
                            guild_id=self.guild_id,
                            role_filter_ids=role_filter_ids,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        seconds = result.get('total_seconds', 0)
                        points.append(
                            {'date': date_obj, 'total_seconds': seconds})
                        total += seconds

                    elif chart_type == "invites":
                        leaderboard = await db_stats.q_invite_leaderboard(
                            guild_id=self.guild_id,
                            limit=100,
                            role_filter_ids=role_filter_ids,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        count = sum(item.get('valid_invites', 0)
                                    for item in leaderboard)
                        points.append({'date': date_obj, 'count': count})
                        total += count

                elif self.chart_type == "channel":
                    if chart_type == "messages":
                        result = await db_stats.q_channel_total_messages(
                            guild_id=self.guild_id,
                            channel_id=self.target_id,
                            role_filter_ids=role_filter_ids,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        count = result.get('total_messages', 0)
                        points.append({'date': date_obj, 'count': count})
                        total += count

                    elif chart_type == "voice_activity":
                        result = await db_stats.q_channel_total_voice(
                            guild_id=self.guild_id,
                            channel_id=self.target_id,
                            role_filter_ids=role_filter_ids,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        seconds = result.get('total_seconds', 0)
                        points.append(
                            {'date': date_obj, 'total_seconds': seconds})
                        total += seconds

                elif self.chart_type == "category":
                    if chart_type == "messages":
                        result = await db_stats.q_category_total_messages(
                            guild_id=self.guild_id,
                            category_id=self.target_id,
                            role_filter_ids=role_filter_ids,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        count = result.get('total_messages', 0)
                        points.append({'date': date_obj, 'count': count})
                        total += count

                    elif chart_type == "voice_activity":
                        result = await db_stats.q_category_total_voice(
                            guild_id=self.guild_id,
                            category_id=self.target_id,
                            role_filter_ids=role_filter_ids,
                            start_time=interval_start,
                            end_time=interval_end
                        )
                        seconds = result.get('total_seconds', 0)
                        points.append(
                            {'date': date_obj, 'total_seconds': seconds})
                        total += seconds

            points.sort(key=lambda x: x['date'])
            return {'points': points, 'total': total}

        except Exception as e:
            print(f"Error fetching chart data: {e}")
            traceback.print_exc()
            return {'points': [], 'total': 0, 'error': str(e)}


class ChartSystem(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        print("✅ ChartSystem: Initialized without session tracking")

    async def cog_load(self):

        try:
            print("✅ ChartSystem: Using DatabaseStats for database connections")
        except Exception as e:
            print(f"❌ ChartSystem: Setup failed: {e}")

    async def cog_unload(self):

        print("✅ ChartSystem: Unloaded")

    def _ensure_int(self, value: Any) -> Optional[int]:

        if value is None:
            return None
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    async def is_user_or_roles_blacklisted(self, guild_id: int, guild: discord.Guild, user_id: int) -> bool:

        db_stats = self.bot.get_cog('DatabaseStats')
        if not db_stats:
            return False

        try:
            return await db_stats.is_user_or_roles_blacklisted(guild_id, user_id)
        except Exception as e:
            print(f"⚠️ Error checking user blacklist via DatabaseStats: {e}")
            return False

    # CHART GENERATION METHODS

    def generate_dynamic_timestamps(self, data_points: List[Dict]) -> List[str]:

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

    def generate_professional_chart(self, data: Dict, chart_type: str) -> Tuple[Image.Image, List[str], List[Tuple[float, str]]]:

        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(6, 2.5), dpi=300,
                               facecolor=(0, 0, 0, 0))
        ax.set_axis_off()

        colors = {
            'messages': '#4287f5',
            'voice_activity': '#FF5C8D',
            'invites': '#b700ff'
        }

        timestamps = []
        y_labels = []

        if chart_type in ['messages', 'voice_activity', 'invites']:
            points = data.get('points', [])
            if points and len(points) > 1:
                points.sort(key=lambda x: x['date'])
                dates = [row['date'] for row in points]

                if chart_type == 'messages' or chart_type == 'invites':
                    values = [float(row['count']) for row in points]
                else:
                    values = [float(row['total_seconds']) for row in points]

                dates_numeric = [mdates.date2num(date) for date in dates]

                max_value = max(values) if values else 1

                y_label_values = [
                    max_value,
                    max_value * 0.75,
                    max_value * 0.5,
                    max_value * 0.25,
                    0
                ]

                y_labels = []
                for value in y_label_values:
                    if value >= 1000:

                        if value % 1000 == 0:
                            label = f"{int(value/1000)}k"
                        else:
                            label = f"{value/1000:.1f}k"
                    else:
                        label = str(int(value))
                    y_labels.append((value, label))

                timestamps = self.generate_dynamic_timestamps(points)

                if len(dates_numeric) > 1:
                    x_smooth = np.linspace(
                        min(dates_numeric), max(dates_numeric), 500)
                    y_smooth = np.interp(x_smooth, dates_numeric, values)

                    from scipy.ndimage import gaussian_filter1d
                    y_smooth = gaussian_filter1d(y_smooth, sigma=2)

                    color = colors.get(chart_type, '#4287f5')
                    ax.plot(x_smooth, y_smooth, linewidth=4, color=color,
                            solid_capstyle='round', antialiased=True)
                else:
                    color = colors.get(chart_type, '#4287f5')
                    ax.plot(dates_numeric, values, 'o',
                            markersize=3, color=color, alpha=0.7)
            else:
                timestamps = [f"Day {i+1}" for i in range(8)]
                y_labels = [
                    (100, "100"),
                    (75, "75"),
                    (50, "50"),
                    (25, "25"),
                    (0, "0")
                ]

        buf = io.BytesIO()
        plt.savefig(buf, format='png', transparent=True,
                    bbox_inches='tight', pad_inches=0, dpi=300)
        buf.seek(0)
        chart_img = Image.open(buf)
        plt.close(fig)

        return chart_img, timestamps, y_labels

    # FORMAT

    def format_number(self, num: float) -> str:

        if num >= 1000:
            if num % 1000 == 0:
                return f"{int(num/1000)}k"
            else:
                return f"{num/1000:.1f}k"
        return str(int(num))

    def format_duration_dynamic(self, seconds: int) -> str:

        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            remaining_seconds = seconds % 60
            if remaining_seconds > 0:
                return f"{minutes}m {remaining_seconds}s"
            else:
                return f"{minutes}m"
        elif seconds < 86400:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            if minutes > 0:
                return f"{hours}h {minutes}m"
            else:
                return f"{hours}h"
        else:
            days = seconds // 86400
            hours = (seconds % 86400) // 3600
            if hours > 0:
                return f"{days}d {hours}h"
            else:
                return f"{days}d"

    async def draw_header_info(self, template: Image.Image, chart_type: str, target_id: int, guild_id: int) -> Image.Image:

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

        # RECTANGLE FUNCTIONS

        def fit_text_to_rectangle(text, text_start_x, text_start_y, rect_center_x, rect_center_y, rect_width, rect_height):

            font_paths = [
                BASE_DIR / "assets" / "fonts" / "HorndonD.ttf",]

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

        target_name = guild.name
        use_server_icon = True
        target_member = None

        if chart_type == "server":

            target_name = guild.name
            use_server_icon = True

        elif chart_type == "channel":

            channel = guild.get_channel(int(target_id))
            if channel:
                target_name = f"#{channel.name}"
            use_server_icon = True

        elif chart_type == "category":
            target_name = guild.name
            use_server_icon = True

        elif chart_type == "user":

            member = guild.get_member(int(target_id))
            if member:
                target_name = member.name
                target_member = member
                use_server_icon = False
            else:
                target_name = f"User {target_id}"
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

                if target_member and target_member.avatar:
                    icon_url = target_member.avatar.url
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

            with Pilmoji(template) as pilmoji:
                pilmoji.text((text_start_x, text_start_y),
                             target_name, font=fallback_font, fill="white")

        # DRAW INVISIBLE RECTANGLE (for testing - uncomment to see boundaries)
        # rect_left = username_rectangle["center"][0] - (username_rectangle["width"] // 2)
        # rect_top = username_rectangle["center"][1] - (username_rectangle["height"] // 2)
        # rect_right = username_rectangle["center"][0] + (username_rectangle["width"] // 2)
        # rect_bottom = username_rectangle["center"][1] + (username_rectangle["height"] // 2)
        # draw.rectangle([rect_left, rect_top, rect_right, rect_bottom], outline="red", width=1)

        return template

    async def draw_chart_on_template(self, chart_img: Image.Image, label_text: str, days: int,
                                     role_id: Optional[str], timestamps: List[str],
                                     y_labels: List[Tuple[float, str]], chart_type: str,
                                     target_type: str, target_id: int, guild_id: int) -> io.BytesIO:

        template_paths = {
            'messages': BASE_DIR / "assets" / "images" / "charts messages final png.png",
            'voice_activity': BASE_DIR / "assets" / "images" / "charts voice final png.png",
            'invites': BASE_DIR / "assets" / "images" / "charts invites final png.png"
        }

        template_path = template_paths.get(chart_type)

        try:
            template = Image.open(template_path).convert("RGBA")
        except FileNotFoundError:
            template = Image.new('RGBA', (1200, 700), (47, 49, 54, 255))
            print(
                f"⚠️ Template not found at {template_path}, using fallback background")

        chart_width = 644
        chart_height = 250
        chart_img = chart_img.resize(
            (chart_width, chart_height), Image.Resampling.LANCZOS)
        template.paste(chart_img, (38, 170), chart_img)

        try:
            template = await self.draw_header_info(template, target_type, int(target_id), int(guild_id))
        except Exception as e:
            print(f"⚠️ Error drawing header: {e}")
            pass

        draw = ImageDraw.Draw(template)

        try:
            horndon_font = ImageFont.truetype(
                BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", 16)
            horndon_small_font = ImageFont.truetype(
                BASE_DIR / "assets" / "fonts" / "HorndonD.ttf", 12)
        except:
            horndon_font = ImageFont.load_default()
            horndon_small_font = ImageFont.load_default()

        text_color = "white"
        shadow_color = "black"

        # TIME PERIOD
        time_text = f"{days} days"
        draw_text_with_stroke(draw, (133, 93), time_text,
                              horndon_font, text_color, shadow_color, 1)

        # ROLE FILTER
        if target_type != "user":
            role_text = "No Filter"
            if role_id and role_id != "none":
                guild = self.bot.get_guild(guild_id)
                if guild:
                    role = guild.get_role(int(role_id))
                    role_text = role.name if role else "Unknown Role"

            with Pilmoji(template) as pilmoji:

                stroke_width = 1
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            pilmoji.text((333 + dx, 95 + dy), role_text,
                                         font=horndon_font, fill=shadow_color)

                pilmoji.text((333, 95), role_text,
                             font=horndon_font, fill=text_color)

        total_value = "0"
        if "Messages" in label_text:
            total_value = label_text.split(
                "Messages ")[1] if "Messages " in label_text else "0"

            draw_text_with_stroke(draw, (623, 95), total_value, horndon_font,
                                  text_color, shadow_color, 1)
        elif "Voice Activity" in label_text:
            total_value = label_text.split("Voice Activity ")[
                1] if "Voice Activity " in label_text else "0"

            draw_text_with_stroke(draw, (655, 95), total_value, horndon_font,
                                  text_color, shadow_color, 1)
        elif "Invites" in label_text:
            total_value = label_text.split(
                "Invites ")[1] if "Invites " in label_text else "0"

            draw_text_with_stroke(draw, (600, 95), total_value, horndon_font,
                                  text_color, shadow_color, 1)

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
            (27, 145),
            (27, 215),
            (27, 280),
            (27, 351),
            (30, 410)
        ]

        for (x, y), (value, label) in zip(y_label_positions, y_labels):
            draw_text_with_stroke(draw, (x, y), label,
                                  horndon_small_font, text_color, shadow_color, 1)

        output = io.BytesIO()
        template.save(output, format='PNG')
        output.seek(0)
        return output

    # COMMANDS

    chart_group = app_commands.Group(
        name="chart", description="Generate activity charts")

    @chart_group.command(name="user", description="Generate user activity charts")
    async def chart_user(self, interaction: discord.Interaction, user: discord.User):

        await interaction.response.defer()

        db_stats = self.bot.get_cog('DatabaseStats')
        if db_stats:
            if await db_stats.is_user_or_roles_blacklisted(int(interaction.guild.id), int(user.id)):
                await interaction.followup.send("❌ This user is blacklisted and cannot be tracked.", ephemeral=True)
                return

        view = ChartView(self, "user", int(user.id),
                         int(interaction.guild.id), days=14)
        await view.update_chart(interaction)

    @chart_group.command(name="server", description="Generate server-wide charts")
    async def chart_server(self, interaction: discord.Interaction):

        await interaction.response.defer()

        view = ChartView(self, "server", int(
            interaction.guild.id), int(interaction.guild.id), days=14)
        await view.update_chart(interaction)

    @chart_group.command(name="channel", description="Generate channel-specific charts")
    async def chart_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):

        await interaction.response.defer()

        view = ChartView(self, "channel", int(channel.id),
                         int(interaction.guild.id), days=14)
        await view.update_chart(interaction)

    @chart_group.command(name="category", description="Generate category-specific charts")
    async def chart_category(self, interaction: discord.Interaction, category: discord.CategoryChannel):

        await interaction.response.defer()

        view = ChartView(self, "category", int(category.id),
                         int(interaction.guild.id), days=14)
        await view.update_chart(interaction)


async def setup(bot):
    await bot.add_cog(ChartSystem(bot))
