import discord
from discord.ext import commands, tasks
from discord import app_commands
from collections import Counter
from datetime import datetime, timedelta
import time
import asyncio
import os
from PIL import Image, ImageDraw, ImageFont
import io
import aiohttp
from typing import Tuple, List, Dict, Any, Optional
import traceback
from pilmoji import Pilmoji
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


# IMAGE GENERATION

async def generate_channel_stats_image(guild: discord.Guild, channel: discord.TextChannel, channel_data: dict, days_back: int, role_id: str = None):

    template_path = BASE_DIR / "assets" / "images" / \
        "text channel stats final png.png"
    try:
        image = Image.open(template_path)
    except FileNotFoundError:
        image = Image.new('RGB', (800, 600), color='#2F3136')

    draw = ImageDraw.Draw(image)

    # Fonts
    try:
        font_paths = [BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"]
        for font_path in font_paths:
            if os.path.exists(font_path):
                try:
                    font_medium = ImageFont.truetype(font_path, 20)
                    font_small = ImageFont.truetype(font_path, 16)
                    font_small_arial = ImageFont.truetype("arial.ttf", 16)
                    font_large = ImageFont.truetype(font_path, 24)
                    font_huge = ImageFont.truetype(font_path, 40)
                    font_horndon_8 = ImageFont.truetype(font_path, 8)
                    font_horndon_10 = ImageFont.truetype(font_path, 10)
                    font_horndon_12 = ImageFont.truetype(font_path, 12)
                    font_horndon_14 = ImageFont.truetype(font_path, 14)
                    font_horndon_16 = ImageFont.truetype(font_path, 16)
                    font_horndon_18 = ImageFont.truetype(font_path, 18)
                    font_horndon_20 = ImageFont.truetype(font_path, 20)
                    font_horndon_22 = ImageFont.truetype(font_path, 22)
                    font_horndon_24 = ImageFont.truetype(font_path, 24)
                    font_horndon_26 = ImageFont.truetype(font_path, 26)
                    font_horndon_28 = ImageFont.truetype(font_path, 28)
                    font_horndon_30 = ImageFont.truetype(font_path, 30)
                    font_horndon_32 = ImageFont.truetype(font_path, 32)
                    font_horndon_34 = ImageFont.truetype(font_path, 34)
                    font_horndon_36 = ImageFont.truetype(font_path, 36)
                    break
                except Exception:
                    continue
        else:
            font_medium = ImageFont.truetype("arial.ttf", 20)
            font_small = ImageFont.truetype("arial.ttf", 16)
            font_small_arial = ImageFont.truetype("arial.ttf", 16)
            font_large = ImageFont.truetype("arial.ttf", 24)
            font_huge = ImageFont.truetype("arial.ttf", 40)
            font_horndon_8 = ImageFont.truetype("arial.ttf", 8)
            font_horndon_10 = ImageFont.truetype("arial.ttf", 10)
            font_horndon_12 = ImageFont.truetype("arial.ttf", 12)
            font_horndon_14 = ImageFont.truetype("arial.ttf", 14)
            font_horndon_16 = ImageFont.load_default()
            font_horndon_18 = ImageFont.load_default()
            font_horndon_20 = ImageFont.load_default()
            font_horndon_22 = ImageFont.load_default()
            font_horndon_24 = ImageFont.load_default()
            font_horndon_26 = ImageFont.load_default()
            font_horndon_28 = ImageFont.load_default()
            font_horndon_30 = ImageFont.load_default()
            font_horndon_32 = ImageFont.load_default()
            font_horndon_34 = ImageFont.load_default()
            font_horndon_36 = ImageFont.load_default()
    except Exception:
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()
        font_small_arial = ImageFont.load_default()
        font_large = ImageFont.load_default()
        font_huge = ImageFont.load_default()
        font_horndon_8 = ImageFont.load_default()
        font_horndon_10 = ImageFont.load_default()
        font_horndon_12 = ImageFont.load_default()
        font_horndon_14 = ImageFont.load_default()
        font_horndon_16 = ImageFont.load_default()
        font_horndon_18 = ImageFont.load_default()
        font_horndon_20 = ImageFont.load_default()
        font_horndon_22 = ImageFont.load_default()
        font_horndon_24 = ImageFont.load_default()
        font_horndon_26 = ImageFont.load_default()
        font_horndon_28 = ImageFont.load_default()
        font_horndon_30 = ImageFont.load_default()
        font_horndon_32 = ImageFont.load_default()
        font_horndon_34 = ImageFont.load_default()
        font_horndon_36 = ImageFont.load_default()

    # RECTANGLE FUNCTIONS
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

        exceeds_width = (text_left < rect_left) or (text_right > rect_right)
        exceeds_height = (text_top < rect_top) or (text_bottom > rect_bottom)

        return exceeds_width or exceeds_height

    def fit_text_to_position(text, text_x, text_y, rect_center_x, rect_center_y, rect_width, rect_height, text_type="username"):

        if text_type == "channel_name":
            font_sizes = [40, 38, 36, 34, 32, 30, 28,
                          26, 24, 22, 20, 18, 16, 14, 12, 10, 8]
        elif text_type == "username":
            font_sizes = [24, 22, 20, 18, 16, 14, 12, 10, 8]
        else:
            font_sizes = [24, 22, 20, 18, 16, 14, 12, 10, 8]

        for idx, font_size in enumerate(font_sizes):
            try:
                font = ImageFont.truetype(font_paths[0], font_size) if os.path.exists(
                    font_paths[0]) else ImageFont.load_default()
            except:
                font = ImageFont.load_default()
            if text_type == "channel_name":
                vertical_offset = idx
            else:
                resizes_done = idx
                vertical_offset = resizes_done // 2

            adjusted_y = text_y + vertical_offset

            if not check_text_against_rectangle(text, font, text_x, adjusted_y, rect_center_x, rect_center_y, rect_width, rect_height):
                return text, font, (text_x, adjusted_y), font_size

        smallest_font = font_sizes[-1]
        try:
            font = ImageFont.truetype(font_paths[0], smallest_font) if os.path.exists(
                font_paths[0]) else ImageFont.load_default()
        except:
            font = ImageFont.load_default()

        truncated_text = text
        while len(truncated_text) > 3:
            truncated_text = truncated_text[:-4] + "..."

            if text_type == "channel_name":
                vertical_offset = len(font_sizes) - 1
            else:
                resizes_done = len(font_sizes) - 1
                vertical_offset = resizes_done // 2

            adjusted_y = text_y + vertical_offset

            if not check_text_against_rectangle(truncated_text, font, text_x, adjusted_y, rect_center_x, rect_center_y, rect_width, rect_height):
                return truncated_text, font, (text_x, adjusted_y), smallest_font

        if text_type == "channel_name":
            vertical_offset = len(font_sizes) - 1
        else:
            resizes_done = len(font_sizes) - 1
            vertical_offset = resizes_done // 2

        final_text = text[:3] + "..." if len(text) > 3 else text
        return final_text, font, (text_x, text_y + vertical_offset), smallest_font

    def draw_text_with_stroke_and_emoji(pilmoji, text, position, font, fill="white", stroke_fill="black", stroke_width=1, font_size=24):

        x, y = position

        with Pilmoji(pilmoji.image) as pilmoji_stroke:
            for dx in [-stroke_width, 0, stroke_width]:
                for dy in [-stroke_width, 0, stroke_width]:
                    if dx != 0 or dy != 0:
                        pilmoji_stroke.text(
                            (x + dx, y + dy), text, fill=stroke_fill, font=font)

        pilmoji.text((x, y), text, fill=fill, font=font)

    # INVISIBLE RECTANGLE COORDINATES
    channel_name_rect = {"center": (135, 30), "width": 245, "height": 45}

    username_rectangles = [
        {"center": (330, 165), "width": 122, "height": 45},  # First username
        {"center": (330, 220), "width": 122, "height": 45},  # Second username
        {"center": (330, 270), "width": 122, "height": 45},  # Third username
        {"center": (330, 325), "width": 122, "height": 45},  # Fourth username
        {"center": (330, 375), "width": 122, "height": 45}   # Fifth username
    ]

    channel_name = f"#{channel.name}"

    fitted_channel_name, channel_name_font, channel_name_pos, channel_font_size = fit_text_to_position(
        channel_name,
        30,
        7,
        channel_name_rect["center"][0],
        channel_name_rect["center"][1],
        channel_name_rect["width"],
        channel_name_rect["height"],
        text_type="channel_name"
    )

    with Pilmoji(image) as pilmoji:
        draw_text_with_stroke_and_emoji(
            pilmoji, fitted_channel_name, channel_name_pos, channel_name_font, font_size=channel_font_size)

    # Server name and icon
    server_name = guild.name
    try:
        if guild.icon:
            icon_url = guild.icon.url
            async with aiohttp.ClientSession() as session:
                async with session.get(icon_url) as response:
                    icon_data = await response.read()

            icon_image = Image.open(io.BytesIO(icon_data))
            icon_image = icon_image.resize((20, 20), Image.Resampling.LANCZOS)

            bbox = draw.textbbox((0, 0), server_name, font=font_medium)
            text_width = bbox[2] - bbox[0]
            icon_x = 375 - (text_width // 2) - 29
            icon_y = 75 - 10
            image.paste(icon_image, (int(icon_x), int(icon_y)))

            text_x = 375 - (text_width // 2)
            with Pilmoji(image) as pilmoji:

                for dx in [-1, 0, 1]:
                    for dy in [-1, 0, 1]:
                        if dx != 0 or dy != 0:
                            pilmoji.text((text_x + dx, 75 - 13 + dy),
                                         server_name, fill="black", font=font_medium)

                pilmoji.text((text_x, 75 - 13), server_name,
                             fill="white", font=font_medium)
        else:
            bbox = draw.textbbox((0, 0), server_name, font=font_medium)
            text_width = bbox[2] - bbox[0]
            text_x = 375 - (text_width // 2)
            with Pilmoji(image) as pilmoji:

                for dx in [-1, 0, 1]:
                    for dy in [-1, 0, 1]:
                        if dx != 0 or dy != 0:
                            pilmoji.text(
                                (text_x + dx, 75 + dy), server_name, fill="black", font=font_medium)

                pilmoji.text((text_x, 75), server_name,
                             fill="white", font=font_medium)
    except Exception as e:
        print(f"❌ Could not add server icon: {e}")
        bbox = draw.textbbox((0, 0), server_name, font=font_medium)
        text_width = bbox[2] - bbox[0]
        text_x = 375 - (text_width // 2)
        with Pilmoji(image) as pilmoji:

            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    if dx != 0 or dy != 0:
                        pilmoji.text((text_x + dx, 75 + dy),
                                     server_name, fill="black", font=font_medium)

            pilmoji.text((text_x, 75), server_name,
                         fill="white", font=font_medium)

    total_messages = channel_data.get('total', 0)
    total_hours = days_back * 24
    messages_per_hour = total_messages / total_hours if total_hours > 0 else 0

    with Pilmoji(image) as pilmoji:

        messages_per_hour_text = f"{messages_per_hour:.1f}"
        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                if dx != 0 or dy != 0:
                    pilmoji.text(
                        (105 + dx, 155 + dy), messages_per_hour_text, fill="black", font=font_huge)
        pilmoji.text((105, 155), messages_per_hour_text,
                     fill="white", font=font_huge)

        total_messages_text = f"{total_messages:,}"
        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                if dx != 0 or dy != 0:
                    pilmoji.text(
                        (105 + dx, 315 + dy), total_messages_text, fill="black", font=font_huge)
        pilmoji.text((105, 315), total_messages_text,
                     fill="white", font=font_huge)

    # ROLE FILTER

    role_text = "No Filter"
    if role_id and role_id != "none":
        role = guild.get_role(int(role_id))
        role_text = role.name if role else "Unknown Role"

    with Pilmoji(image) as pilmoji:
        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                if dx != 0 or dy != 0:
                    pilmoji.text((70 + dx, 424 + dy), role_text,
                                 fill="black", font=font_small)
        pilmoji.text((70, 424), role_text, fill="white", font=font_small)

    # Time period

    time_period_text = f"{days_back} days"
    with Pilmoji(image) as pilmoji:
        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                if dx != 0 or dy != 0:
                    pilmoji.text((630 + dx, 422 + dy), time_period_text,
                                 fill="black", font=font_small)
        pilmoji.text((630, 422), time_period_text,
                     fill="white", font=font_small)

    # CREATED ON

    current_date = datetime.now().strftime("%B %d, %Y")
    with Pilmoji(image) as pilmoji:
        for dx in [-1, 0, 1]:
            for dy in [-1, 0, 1]:
                if dx != 0 or dy != 0:
                    pilmoji.text((550 + dx, 38 + dy), current_date,
                                 fill="black", font=font_small)
        pilmoji.text((550, 38), current_date, fill="white", font=font_small)

    # Top 5 users

    top_users = channel_data.get('users', Counter()).most_common(5)
    user_positions = [(275, 150), (275, 205), (275, 255),
                      (275, 315), (275, 365)]
    message_positions = [(405, 150), (405, 205),
                         (405, 255), (405, 315), (405, 365)]

    with Pilmoji(image) as pilmoji:
        for i, ((user_id, count), (user_x, user_y), (msg_x, msg_y)) in enumerate(zip(top_users, user_positions, message_positions)):
            member = guild.get_member(int(user_id))
            username = member.name if member else f"User {user_id}"

            rect_info = username_rectangles[i]

            fitted_username, username_font, username_pos, username_font_size = fit_text_to_position(
                username,
                user_x,
                user_y,
                rect_info["center"][0],
                rect_info["center"][1],
                rect_info["width"],
                rect_info["height"],
                text_type="username"
            )

            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    if dx != 0 or dy != 0:
                        pilmoji.text((username_pos[0] + dx, username_pos[1] + dy),
                                     fitted_username, fill="black", font=username_font)

            pilmoji.text(username_pos, fitted_username,
                         fill="white", font=username_font)

            count_text = f"{count:,}"
            for dx in [-1, 0, 1]:
                for dy in [-1, 0, 1]:
                    if dx != 0 or dy != 0:
                        pilmoji.text((msg_x + dx, msg_y + dy),
                                     count_text, fill="black", font=font_large)
            pilmoji.text((msg_x, msg_y), count_text,
                         fill="white", font=font_large)

    timeseries_data = channel_data.get('timeseries', {})
    time_positions = {1: (645, 150), 5: (645, 210), 10: (
        645, 260), 20: (645, 315), 30: (645, 365)}

    with Pilmoji(image) as pilmoji:

        for days, position in time_positions.items():
            if days <= days_back:
                count = timeseries_data.get(f'{days}d', 0)
                count_text = f"{count:,}"

                for dx in [-1, 0, 1]:
                    for dy in [-1, 0, 1]:
                        if dx != 0 or dy != 0:
                            pilmoji.text(
                                (position[0] + dx, position[1] + dy), count_text, fill="black", font=font_large)

                pilmoji.text(position, count_text,
                             fill="white", font=font_large)
            else:

                for dx in [-1, 0, 1]:
                    for dy in [-1, 0, 1]:
                        if dx != 0 or dy != 0:
                            pilmoji.text(
                                (position[0] + dx, position[1] + dy), "0", fill="black", font=font_large)

                pilmoji.text(position, "0", fill="white", font=font_large)

    img_bytes = io.BytesIO()
    image.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    return img_bytes


# ROLE DROPDOWN MENU

class ChannelRoleSelectMenu(discord.ui.Select):
    def __init__(self, guild: discord.Guild, current_role_id: str = None):
        self.guild = guild
        roles = [role for role in guild.roles if role.name != "@everyone"]
        roles.sort(key=lambda x: x.position, reverse=True)

        options = [discord.SelectOption(
            label="No Filter", value="none",
            description="Show all users", emoji="🌐",
            default=(current_role_id == "none" or not current_role_id)
        )]

        for role in roles:
            options.append(discord.SelectOption(
                label=role.name, value=str(role.id),
                description=f"Filter by {role.name} role",
                default=(str(role.id) == current_role_id)
            ))

        super().__init__(
            placeholder="Filter by role...",
            options=options,
            custom_id="channel_role_filter_select",
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):

        if not self.values:
            await interaction.response.send_message("❌ No value selected.", ephemeral=True)
            return

        view = self.view
        view.selected_role_id = self.values[0]

        await interaction.response.defer()

        channel_data = await view.cog._get_channel_stats_from_db(view.guild, view.channel, view.current_days, view.selected_role_id)

        if channel_data is None:
            channel_data = {'total': 0, 'users': Counter(
            ), 'timeseries': {}, 'timestamp': time.time()}

        view._update_buttons()
        img_bytes = await generate_channel_stats_image(view.guild, view.channel, channel_data, view.current_days, view.selected_role_id)

        file = discord.File(img_bytes, filename="channel_stats.png")
        await interaction.edit_original_response(attachments=[file], view=view)


# TIME MODAL

class ChannelTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, cog_instance, original_message_id, guild, channel):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.original_message_id = original_message_id
        self.guild = guild
        self.channel = channel

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

            if self.original_message_id not in self.cog.active_sessions:
                await interaction.followup.send("❌ Session expired or not found. Please run the command again.", ephemeral=True)
                return

            session = self.cog.active_sessions[self.original_message_id]
            view = session.get('view')
            if not view:
                await interaction.followup.send("❌ Could not find session view.", ephemeral=True)
                return

            view.current_days = days
            view.show_time_buttons = False

            channel_data = await self.cog._get_channel_stats_from_db(self.guild, self.channel, days, view.selected_role_id)
            if channel_data is None:
                channel_data = {'total': 0, 'users': Counter(
                ), 'timeseries': {}, 'timestamp': time.time()}

            img_bytes = await generate_channel_stats_image(self.guild, self.channel, channel_data, days, view.selected_role_id)
            view._update_buttons()
            file = discord.File(img_bytes, filename="channel_stats.png")

            try:
                original_message = await interaction.channel.fetch_message(self.original_message_id)
                if original_message:
                    await original_message.edit(attachments=[file], view=view)
                else:
                    await interaction.followup.send(file=file, view=view)
            except discord.NotFound:
                await interaction.followup.send(file=file, view=view)

            self.cog.active_sessions[self.original_message_id]['current_days'] = days

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            print(f"Error in channel modal submit: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred while updating stats.", ephemeral=True)


# MAIN VIEW

class ChannelStatsView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, channel: discord.TextChannel, days_back: int, selected_role_id: str = None):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.channel = channel
        self.current_days = days_back
        self.selected_role_id = selected_role_id
        self.show_time_buttons = False
        self._update_buttons()

    def _update_buttons(self):
        self.clear_items()

        role_select = ChannelRoleSelectMenu(self.guild, self.selected_role_id)
        self.add_item(role_select)

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="channel_refresh",
            row=1
        )
        refresh_button.callback = self.refresh_callback
        self.add_item(refresh_button)

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="channel_time_settings",
            row=1
        )
        time_settings_button.callback = self.time_settings_callback
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            days_7_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 7 else discord.ButtonStyle.secondary,
                label="7 Days",
                custom_id="channel_days_7",
                row=2
            )
            days_7_button.callback = self.days_7_callback
            self.add_item(days_7_button)

            days_14_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 14 else discord.ButtonStyle.secondary,
                label="14 Days",
                custom_id="channel_days_14",
                row=2
            )
            days_14_button.callback = self.days_14_callback
            self.add_item(days_14_button)

            days_30_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 30 else discord.ButtonStyle.secondary,
                label="30 Days",
                custom_id="channel_days_30",
                row=2
            )
            days_30_button.callback = self.days_30_callback
            self.add_item(days_30_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in [
                    7, 14, 30] else "Custom",
                custom_id="channel_custom_days",
                row=2
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
        modal = ChannelTimeModal(
            self.cog, interaction.message.id, self.guild, self.channel)
        await interaction.response.send_modal(modal)

    # BUTTON LOGIC

    async def handle_button_click(self, interaction: discord.Interaction, refresh: bool = False, days: int = None):
        try:
            await interaction.response.defer()

            if days:
                self.current_days = days
                self.show_time_buttons = False

            channel_data = await self.cog._get_channel_stats_from_db(self.guild, self.channel, self.current_days, self.selected_role_id)
            if channel_data is None:
                channel_data = {'total': 0, 'users': Counter(
                ), 'timeseries': {}, 'timestamp': time.time()}

            img_bytes = await generate_channel_stats_image(self.guild, self.channel, channel_data, self.current_days, self.selected_role_id)

            self._update_buttons()
            file = discord.File(img_bytes, filename="channel_stats.png")
            await interaction.edit_original_response(attachments=[file], view=self)

            if interaction.message and interaction.message.id in self.cog.active_sessions:
                self.cog.active_sessions[interaction.message.id]['current_days'] = self.current_days
                self.cog.active_sessions[interaction.message.id]['selected_role_id'] = self.selected_role_id

        except Exception as e:
            print(f"Error handling button click: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred while updating the stats.", ephemeral=True)

    async def handle_time_settings(self, interaction: discord.Interaction):
        try:
            self.show_time_buttons = not self.show_time_buttons
            self._update_buttons()
            await interaction.response.edit_message(view=self)
        except Exception as e:
            print(f"Error handling time settings: {e}")
            await interaction.response.send_message("❌ An error occurred while updating time settings.", ephemeral=True)


# INITIALIZATION

class TextCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.active_sessions = {}

    async def _get_channel_stats_from_db(self, guild: discord.Guild, channel: discord.TextChannel,
                                         days_back: int, role_id: str = None) -> Optional[Dict[str, Any]]:

        db_cog = self.bot.get_cog('DatabaseStats')
        if not db_cog:
            print("❌ DatabaseStats cog not found")
            return None

    # QUERY FUNCTIONS

        try:
            role_ids = None
            if role_id and role_id != "none":
                role_ids = [int(role_id)]

            start_time = datetime.utcnow() - timedelta(days=days_back)
            end_time = datetime.utcnow()

            all_messages = await db_cog.q_channel_total_messages(
                guild_id=int(guild.id),
                channel_id=channel.id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )

            top_users_data = await db_cog.q_channel_top5_users_messages(
                guild_id=int(guild.id),
                channel_id=channel.id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )

            user_counts = Counter()
            for user_data in top_users_data:
                if isinstance(user_data, dict) and 'user_id' in user_data:
                    user_counts[str(user_data['user_id'])] = user_data.get(
                        'message_count', 0)

            timeseries_data = await db_cog.q_channel_timeseries_messages_1d_5d_10d_20d_30d(
                guild_id=int(guild.id),
                channel_id=channel.id,
                role_filter_ids=role_ids,
                start_time=None,
                end_time=datetime.utcnow()
            )

            return {
                'total': all_messages.get('total_messages', 0),
                'users': user_counts,
                'timeseries': timeseries_data,
                'timestamp': time.time()
            }

        except Exception as e:
            print(f"❌ Error getting channel stats via DatabaseStats: {e}")
            traceback.print_exc()
            return None

    # COMMANDS

    text_group = app_commands.Group(
        name="text",
        description="Text channel related commands"
    )

    channel_group = app_commands.Group(
        name="channel",
        description="Channel statistics commands",
        parent=text_group
    )

    @channel_group.command(
        name="stats",
        description="Get the stats for a text channel."
    )
    @app_commands.describe(
        channel="Select a text channel to get stats for"
    )
    async def channel_stats(self, interaction: discord.Interaction, channel: discord.TextChannel):

        await interaction.response.defer()

        db_cog = self.bot.get_cog('DatabaseStats')
        if not db_cog:
            await interaction.followup.send("❌ Database system is not available. Please try again later.", ephemeral=True)
            return

        channel_data = await self._get_channel_stats_from_db(interaction.guild, channel, 14)

        if channel_data is None:
            channel_data = {
                'total': 0,
                'users': Counter(),
                'timeseries': {},
                'timestamp': time.time()
            }

        img_bytes = await generate_channel_stats_image(interaction.guild, channel, channel_data, 14)

        view = ChannelStatsView(
            self, interaction.guild, channel, days_back=14
        )

        file = discord.File(img_bytes, filename="channel_stats.png")
        message = await interaction.followup.send(file=file, view=view, wait=True)

        self.active_sessions[message.id] = {
            'guild_id': interaction.guild.id,
            'user_id': interaction.user.id,
            'channel_id': channel.id,
            'current_days': 14,
            'selected_role_id': None,
            'view': view,
            'created_at': time.time()
        }

    # CLEANUP

    @tasks.loop(minutes=5)
    async def cleanup_sessions(self):

        current_time = time.time()
        keys_to_remove = []

        for message_id, session in self.active_sessions.items():

            if current_time - session.get('created_at', current_time) > 3600:
                keys_to_remove.append(message_id)

        for key in keys_to_remove:
            del self.active_sessions[key]

    async def cog_load(self):

        self.cleanup_sessions.start()

    async def cog_unload(self):

        self.cleanup_sessions.cancel()


# SETUP

async def setup(bot):
    await bot.add_cog(TextCommands(bot))
