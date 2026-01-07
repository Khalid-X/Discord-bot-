import aiohttp
import os
import io
from PIL import Image, ImageDraw, ImageFont
import asyncio
import time
from datetime import timedelta, datetime
from collections import Counter
from discord import app_commands
from discord.ext import commands
import discord
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


# FORMAT

def format_voice_time(total_seconds):

    if total_seconds == 0:
        return "0m"

    days = total_seconds // (3600 * 24)
    hours = (total_seconds % (3600 * 24)) // 3600
    minutes = (total_seconds % 3600) // 60

    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m"


def format_message_count(count):

    return f"{count:,}"


# FONTS

def get_fonts():

    try:

        custom_font_path = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"
        if os.path.exists(custom_font_path):
            font_small = ImageFont.truetype(custom_font_path, 16)
            font_medium = ImageFont.truetype(custom_font_path, 20)
            font_large = ImageFont.truetype(custom_font_path, 24)
            font_larger = ImageFont.truetype(custom_font_path, 30)
            font_huge = ImageFont.truetype(custom_font_path, 40)
            return font_small, font_medium, font_large, font_larger, font_huge
    except:
        pass

    try:

        font_small = ImageFont.truetype("arial.ttf", 16)
        font_medium = ImageFont.truetype("arial.ttf", 20)
        font_large = ImageFont.truetype("arial.ttf", 24)
        font_larger = ImageFont.truetype("arial.ttf", 30)
        font_huge = ImageFont.truetype("arial.ttf", 40)
        return font_small, font_medium, font_large, font_larger, font_huge
    except:
        try:

            font_small = ImageFont.truetype("Helvetica.ttf", 16)
            font_medium = ImageFont.truetype("Helvetica.ttf", 20)
            font_large = ImageFont.truetype("Helvetica.ttf", 24)
            font_larger = ImageFont.truetype("Helvetica.ttf", 30)
            font_huge = ImageFont.truetype("Helvetica.ttf", 40)
            return font_small, font_medium, font_large, font_larger, font_huge
        except:

            font_small = ImageFont.load_default()
            font_medium = ImageFont.load_default()
            font_large = ImageFont.load_default()
            font_larger = ImageFont.load_default()
            font_huge = ImageFont.load_default()
            return font_small, font_medium, font_large, font_larger, font_huge


def get_horndon_font(size=40):

    custom_font_path = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"
    try:
        if os.path.exists(custom_font_path):

            return ImageFont.truetype(custom_font_path, size)
    except Exception as e:
        print(f"Warning: Could not load HorndonD font at size {size}: {e}")

    try:
        return ImageFont.truetype("arial.ttf", size)
    except:
        return ImageFont.load_default()


# INVISIBLE RECTANGLE

def check_text_against_rectangle(draw, text, font, text_x, text_y, rect_left, rect_top, rect_right, rect_bottom):

    bbox = draw.textbbox((text_x, text_y), text, font=font)
    text_left = bbox[0]
    text_right = bbox[2]
    text_top = bbox[1]
    text_bottom = bbox[3]

    exceeds_width = (text_left < rect_left) or (text_right > rect_right)
    exceeds_height = (text_top < rect_top) or (text_bottom > rect_bottom)

    return exceeds_width or exceeds_height


def fit_server_name(draw, text, text_x, text_y, rect_center_x, rect_center_y, rect_width, rect_height):

    rect_left = rect_center_x - (rect_width // 2)
    rect_right = rect_center_x + (rect_width // 2)
    rect_top = rect_center_y - (rect_height // 2)
    rect_bottom = rect_center_y + (rect_height // 2)

    font_sizes = [40, 38, 36, 34, 32, 30, 28,
                  26, 24, 22, 20, 18, 16, 14, 12, 10, 8]

    for idx, font_size in enumerate(font_sizes):

        font = get_horndon_font(font_size)

        vertical_offset = idx
        adjusted_y = text_y + vertical_offset

        if not check_text_against_rectangle(draw, text, font, text_x, adjusted_y,
                                            rect_left, rect_top, rect_right, rect_bottom):
            return text, font, (text_x, adjusted_y), font_size

    smallest_font = font_sizes[-1]
    font = get_horndon_font(smallest_font)

    truncated_text = text
    while len(truncated_text) > 3:
        truncated_text = truncated_text[:-4] + "..."

        vertical_offset = len(font_sizes) - 1
        adjusted_y = text_y + vertical_offset

        if not check_text_against_rectangle(draw, truncated_text, font, text_x, adjusted_y,
                                            rect_left, rect_top, rect_right, rect_bottom):
            return truncated_text, font, (text_x, adjusted_y), smallest_font

    vertical_offset = len(font_sizes) - 1
    final_text = text[:3] + "..." if len(text) > 3 else text
    return final_text, font, (text_x, text_y + vertical_offset), smallest_font


def fit_regular_text(draw, text, text_x, text_y, rect_center_x, rect_center_y, rect_width, rect_height):

    rect_left = rect_center_x - (rect_width // 2)
    rect_right = rect_center_x + (rect_width // 2)
    rect_top = rect_center_y - (rect_height // 2)
    rect_bottom = rect_center_y + (rect_height // 2)

    font_sizes = [24, 22, 20, 18, 16, 14, 12, 10, 8]

    for idx, font_size in enumerate(font_sizes):
        try:
            custom_font_path = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"
            if os.path.exists(custom_font_path):

                font = ImageFont.truetype(custom_font_path, font_size)
            else:
                font = ImageFont.load_default()
        except:
            font = ImageFont.load_default()

        vertical_offset = idx // 2
        adjusted_y = text_y + vertical_offset

        if not check_text_against_rectangle(draw, text, font, text_x, adjusted_y,
                                            rect_left, rect_top, rect_right, rect_bottom):
            return text, font, (text_x, adjusted_y), font_size

    smallest_font = font_sizes[-1]
    try:
        custom_font_path = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"
        if os.path.exists(custom_font_path):
            font = ImageFont.truetype(custom_font_path, smallest_font)
        else:
            font = ImageFont.load_default()
    except:
        font = ImageFont.load_default()

    truncated_text = text
    while len(truncated_text) > 3:
        truncated_text = truncated_text[:-4] + "..."

        vertical_offset = len(font_sizes) // 2
        adjusted_y = text_y + vertical_offset

        if not check_text_against_rectangle(draw, truncated_text, font, text_x, adjusted_y,
                                            rect_left, rect_top, rect_right, rect_bottom):
            return truncated_text, font, (text_x, adjusted_y), smallest_font

    vertical_offset = len(font_sizes) // 2
    final_text = text[:3] + "..." if len(text) > 3 else text
    return final_text, font, (text_x, text_y + vertical_offset), smallest_font


# STROKE

def draw_text_with_stroke(draw, text, position, font, fill="white", stroke_fill="black", stroke_width=1):

    x, y = position

    for dx in [-stroke_width, 0, stroke_width]:
        for dy in [-stroke_width, 0, stroke_width]:
            if dx != 0 or dy != 0:
                draw.text((x + dx, y + dy), text, fill=stroke_fill, font=font)

    draw.text((x, y), text, fill=fill, font=font)


# IMAGE GENERATION

async def generate_category_message_stats_image(guild: discord.Guild, category: discord.CategoryChannel, message_data: dict, days_back: int, role_id: str = None):

    template_path = BASE_DIR / "assets" / "images" / \
        "category message stats final png.png"
    try:
        image = Image.open(template_path)
    except FileNotFoundError:
        image = Image.new('RGB', (800, 600), color='#2F3136')

    if image.mode != "RGB":
        image = image.convert("RGB")

    draw = ImageDraw.Draw(image)

    font_small, font_medium, font_large, font_larger, font_huge = get_fonts()

    #
    server_name_rect = {"center": (135, 30), "width": 245, "height": 45}

    username_rectangles = [
        {"center": (70, 330), "width": 125, "height": 50},
        {"center": (70, 385), "width": 125, "height": 50},
        {"center": (70, 440), "width": 125, "height": 50},
        {"center": (70, 495), "width": 125, "height": 50},
        {"center": (70, 540), "width": 125, "height": 50}
    ]

    channel_name_rectangles = [
        {"center": (330, 385), "width": 125, "height": 50},
        {"center": (330, 440), "width": 125, "height": 50},
        {"center": (330, 495), "width": 125, "height": 50},
        {"center": (330, 540), "width": 125, "height": 50},
    ]

    fitted_server_name, server_name_font, server_name_pos, server_font_size = fit_server_name(
        draw,
        guild.name,
        30,
        7,
        server_name_rect["center"][0],
        server_name_rect["center"][1],
        server_name_rect["width"],
        server_name_rect["height"]
    )

    draw_text_with_stroke(
        draw, fitted_server_name, server_name_pos, server_name_font
    )

    try:
        if guild.icon:

            icon_url = guild.icon.url
            async with aiohttp.ClientSession() as session:
                async with session.get(icon_url) as response:
                    icon_data = await response.read()

            icon_image = Image.open(io.BytesIO(icon_data))

            icon_size = (20, 20)
            icon_image = icon_image.resize(icon_size, Image.Resampling.LANCZOS)

            bbox = draw.textbbox((0, 0), guild.name, font=font_medium)
            text_width = bbox[2] - bbox[0]

            icon_x = 375 - (text_width // 2) - 29
            icon_y = 75 - 10

            image.paste(icon_image, (int(icon_x), int(icon_y)))

            text_x = 375 - (text_width // 2)
            draw_text_with_stroke(
                draw, guild.name, (text_x, 75 - 13), font_medium
            )

        else:
            bbox = draw.textbbox((0, 0), guild.name, font=font_medium)
            text_width = bbox[2] - bbox[0]
            text_x = 375 - (text_width // 2)
            draw_text_with_stroke(
                draw, guild.name, (text_x, 75 - 13), font_medium
            )

    except Exception as e:
        print(f"❌ Could not add server icon: {e}")

        bbox = draw.textbbox((0, 0), guild.name, font=font_medium)
        text_width = bbox[2] - bbox[0]
        text_x = 375 - (text_width // 2)
        draw_text_with_stroke(
            draw, guild.name, (text_x, 75 - 13), font_medium
        )

    text_channels = [ch for ch in category.channels if isinstance(
        ch, discord.TextChannel)]
    channels_count = len(text_channels)

    draw_text_with_stroke(
        draw, str(channels_count), (125, 160), font_huge
    )

    total_messages = message_data.get('total_messages', 0)
    draw_text_with_stroke(
        draw, format_message_count(total_messages), (380, 160), font_huge
    )

    # CREATED ON
    current_date = datetime.now().strftime("%B %d, %Y")
    draw_text_with_stroke(
        draw, current_date, (550, 40), font_small
    )

    # ROLE
    role_text = "No Filter"
    if role_id and role_id != "none":
        role = guild.get_role(int(role_id))
        role_text = role.name if role else "Unknown Role"

    draw_text_with_stroke(
        draw, role_text, (590, 420), font_small
    )

    # TIME PERIOD
    time_period_text = f"{days_back} days"

    draw_text_with_stroke(
        draw, time_period_text, (635, 458), font_small
    )

    top_users = message_data.get('users', Counter()).most_common(5)

    user_positions = [
        (20, 320), (20, 375), (20, 430), (20, 485), (20, 535)
    ]
    message_positions = [
        (195, 320), (195, 375), (195, 430), (195, 485), (195, 535)
    ]

    for i, ((user_id, message_count), (user_x, user_y), (msg_x, msg_y)) in enumerate(zip(top_users, user_positions, message_positions)):
        member = guild.get_member(int(user_id))
        username = member.name if member else f"User {user_id}"

        if i < len(username_rectangles):
            rect_info = username_rectangles[i]

            fitted_username, username_font, username_pos, username_font_size = fit_regular_text(
                draw,
                username,
                user_x,
                user_y,
                rect_info["center"][0],
                rect_info["center"][1],
                rect_info["width"],
                rect_info["height"],
            )

            draw_text_with_stroke(
                draw, fitted_username, username_pos, username_font
            )

        count_text = format_message_count(message_count)
        draw_text_with_stroke(
            draw, count_text, (msg_x, msg_y), font_large
        )

    top_channels = message_data.get('channels', Counter()).most_common(5)

    channel_positions = [
        (280, 320), (280, 375), (280, 430), (280, 485), (280, 535)
    ]
    channel_message_positions = [
        (450, 320), (450, 375), (450, 430), (450, 485), (450, 535)
    ]

    for i, ((channel_id, message_count), (chan_x, chan_y), (msg_x, msg_y)) in enumerate(zip(top_channels, channel_positions, channel_message_positions)):
        channel = guild.get_channel(int(channel_id))
        channel_name = channel.name if channel else f"Channel {channel_id}"

        if i >= 1 and (i-1) < len(channel_name_rectangles):
            rect_info = channel_name_rectangles[i-1]

            fitted_channel_name, channel_font, channel_pos, channel_font_size = fit_regular_text(
                draw,
                channel_name,
                chan_x,
                chan_y,
                rect_info["center"][0],
                rect_info["center"][1],
                rect_info["width"],
                rect_info["height"],
            )

            draw_text_with_stroke(
                draw, fitted_channel_name, channel_pos, channel_font
            )
        elif i == 0:

            draw_text_with_stroke(
                draw, channel_name, (chan_x, chan_y), font_large
            )

        count_text = format_message_count(message_count)
        draw_text_with_stroke(
            draw, count_text, (msg_x, msg_y), font_large
        )

    messages_over_days = {}
    daily_data = message_data.get('daily', Counter())
    for days in [1, 5, 10, 20, 30]:
        if days <= days_back:
            count = 0
            for days_ago, msg_count in daily_data.items():
                if days_ago <= days:
                    count += msg_count
            messages_over_days[days] = count

    time_positions = {
        1: (670, 155),
        5: (670, 210),
        10: (670, 265),
        20: (670, 315),
        30: (670, 365)
    }

    for days, (x, y) in time_positions.items():
        message_count = messages_over_days.get(days, 0)
        count_text = format_message_count(message_count)

        draw_text_with_stroke(
            draw, count_text, (x, y), font_large
        )

    img_bytes = io.BytesIO()
    image.save(img_bytes, format='PNG')
    img_bytes.seek(0)

    return img_bytes


async def generate_category_voice_stats_image(guild: discord.Guild, category: discord.CategoryChannel, voice_data: dict, days_back: int, role_id: str = None):

    template_path = BASE_DIR / "assets" / "images" / \
        "category voice stats final png.png"
    try:
        image = Image.open(template_path)
    except FileNotFoundError:

        image = Image.new('RGB', (800, 600), color='#2F3136')

    draw = ImageDraw.Draw(image)

    font_small, font_medium, font_large, font_larger, font_huge = get_fonts()

    server_name_rect = {"center": (135, 30), "width": 245, "height": 45}

    username_rectangles = [
        {"center": (70, 330), "width": 125, "height": 50},
        {"center": (70, 385), "width": 125, "height": 50},
        {"center": (70, 440), "width": 125, "height": 50},
        {"center": (70, 495), "width": 125, "height": 50},
        {"center": (70, 540), "width": 125, "height": 50}
    ]

    channel_name_rectangles = [
        {"center": (330, 385), "width": 125, "height": 50},
        {"center": (330, 440), "width": 125, "height": 50},
        {"center": (330, 495), "width": 125, "height": 50},
        {"center": (330, 540), "width": 125, "height": 50},
    ]

    fitted_server_name, server_name_font, server_name_pos, server_font_size = fit_server_name(
        draw,
        guild.name,
        30,
        7,
        server_name_rect["center"][0],
        server_name_rect["center"][1],
        server_name_rect["width"],
        server_name_rect["height"]
    )

    draw_text_with_stroke(
        draw, fitted_server_name, server_name_pos, server_name_font
    )

    try:
        if guild.icon:

            icon_url = guild.icon.url
            async with aiohttp.ClientSession() as session:
                async with session.get(icon_url) as response:
                    icon_data = await response.read()

            icon_image = Image.open(io.BytesIO(icon_data))

            icon_size = (20, 20)
            icon_image = icon_image.resize(icon_size, Image.Resampling.LANCZOS)

            bbox = draw.textbbox((0, 0), guild.name, font=font_medium)
            text_width = bbox[2] - bbox[0]

            icon_x = 375 - (text_width // 2) - 29
            icon_y = 75 - 10

            image.paste(icon_image, (int(icon_x), int(icon_y)))

            text_x = 375 - (text_width // 2)
            draw_text_with_stroke(
                draw, guild.name, (text_x, 75 - 13), font_medium
            )

        else:

            bbox = draw.textbbox((0, 0), guild.name, font=font_medium)
            text_width = bbox[2] - bbox[0]
            text_x = 375 - (text_width // 2)
            draw_text_with_stroke(
                draw, guild.name, (text_x, 75 - 13), font_medium
            )

    except Exception as e:
        print(f"❌ Could not add server icon: {e}")

        bbox = draw.textbbox((0, 0), guild.name, font=font_medium)
        text_width = bbox[2] - bbox[0]
        text_x = 375 - (text_width // 2)
        draw_text_with_stroke(
            draw, guild.name, (text_x, 75 - 13), font_medium
        )

    voice_channels = [ch for ch in category.channels if isinstance(
        ch, discord.VoiceChannel)]
    channels_count = len(voice_channels)

    draw_text_with_stroke(
        draw, str(channels_count), (125, 160), font_huge
    )

    total_seconds = voice_data.get('total_seconds', 0)
    total_voice_text = format_voice_time(total_seconds)

    draw_text_with_stroke(
        draw, total_voice_text, (380, 160), font_huge
    )

    # CREATED ON
    current_date = datetime.now().strftime("%B %d, %Y")

    draw_text_with_stroke(
        draw, current_date, (550, 40), font_small
    )

    # ROLE
    role_text = "No Filter"
    if role_id and role_id != "none":
        role = guild.get_role(int(role_id))
        role_text = role.name if role else "Unknown Role"

    draw_text_with_stroke(
        draw, role_text, (590, 420), font_small
    )

    # TIME PERIOD
    time_period_text = f"{days_back} days"

    draw_text_with_stroke(
        draw, time_period_text, (635, 458), font_small
    )

    top_users = voice_data.get('users', Counter()).most_common(5)
    user_positions = [
        (20, 320), (20, 375), (20, 430), (20, 485), (20, 535)
    ]
    voice_time_positions = [
        (195, 320), (195, 375), (195, 430), (195, 485), (195, 535)
    ]

    for i, ((user_id, user_seconds), (user_x, user_y), (time_x, time_y)) in enumerate(zip(top_users, user_positions, voice_time_positions)):
        member = guild.get_member(int(user_id))
        username = member.name if member else f"User {user_id}"

        if i < len(username_rectangles):
            rect_info = username_rectangles[i]

            fitted_username, username_font, username_pos, username_font_size = fit_regular_text(
                draw,
                username,
                user_x,
                user_y,
                rect_info["center"][0],
                rect_info["center"][1],
                rect_info["width"],
                rect_info["height"],
            )

            draw_text_with_stroke(
                draw, fitted_username, username_pos, username_font
            )

        voice_time_text = format_voice_time(user_seconds)
        draw_text_with_stroke(
            draw, voice_time_text, (time_x, time_y), font_large
        )

    top_channels = voice_data.get('channels', Counter()).most_common(5)

    channel_positions = [
        (280, 320), (280, 375), (280, 430), (280, 485), (280, 535)
    ]
    channel_voice_positions = [
        (450, 320), (450, 375), (450, 430), (450, 485), (450, 535)
    ]

    for i, ((channel_id, channel_seconds), (chan_x, chan_y), (time_x, time_y)) in enumerate(zip(top_channels, channel_positions, channel_voice_positions)):
        channel = guild.get_channel(int(channel_id))
        channel_name = channel.name if channel else f"Channel {channel_id}"

        if i >= 1 and (i-1) < len(channel_name_rectangles):
            rect_info = channel_name_rectangles[i-1]

            fitted_channel_name, channel_font, channel_pos, channel_font_size = fit_regular_text(
                draw,
                channel_name,
                chan_x,
                chan_y,
                rect_info["center"][0],
                rect_info["center"][1],
                rect_info["width"],
                rect_info["height"],
            )

            draw_text_with_stroke(
                draw, fitted_channel_name, channel_pos, channel_font
            )
        elif i == 0:

            draw_text_with_stroke(
                draw, channel_name, (chan_x, chan_y), font_large
            )

        voice_time_text = format_voice_time(channel_seconds)
        draw_text_with_stroke(
            draw, voice_time_text, (time_x, time_y), font_large
        )

    voice_time_over_days = {}
    daily_data = voice_data.get('daily', Counter())
    for days in [1, 5, 10, 20, 30]:
        if days <= days_back:
            total_seconds = 0
            for days_ago, seconds in daily_data.items():
                if days_ago <= days:
                    total_seconds += seconds
            voice_time_over_days[days] = total_seconds

    time_positions = {
        1: (670, 155),
        5: (670, 210),
        10: (670, 265),
        20: (670, 315),
        30: (670, 365)
    }

    for days, (x, y) in time_positions.items():
        seconds = voice_time_over_days.get(days, 0)
        voice_time_text = format_voice_time(seconds)

        draw_text_with_stroke(
            draw, voice_time_text, (x, y), font_large
        )

    img_bytes = io.BytesIO()
    image.save(img_bytes, format='PNG')
    img_bytes.seek(0)

    return img_bytes


# ROLE DROPDOWN MENU

class CategoryRoleSelectMenu(discord.ui.Select):
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
            custom_id="category_role_filter_select",
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

        if view.stats_type == 'message':
            category_data = await view.cog._get_category_message_stats(view.guild, view.category, view.current_days, view.selected_role_id)

            if category_data is None:

                category_data = {
                    'total_messages': 0,
                    'channels': Counter(),
                    'users': Counter(),
                    'daily': Counter(),
                    'timestamp': time.time()
                }

            img_bytes = await generate_category_message_stats_image(view.guild, view.category, category_data, view.current_days, view.selected_role_id)
        else:
            category_data = await view.cog._get_category_voice_stats(view.guild, view.category, view.current_days, view.selected_role_id)

            if category_data is None:

                category_data = {
                    'total_seconds': 0,
                    'channels': Counter(),
                    'users': Counter(),
                    'daily': Counter(),
                    'timestamp': time.time()
                }

            img_bytes = await generate_category_voice_stats_image(view.guild, view.category, category_data, view.current_days, view.selected_role_id)

        view._update_buttons()
        file = discord.File(
            img_bytes, filename=f"category_{view.stats_type}_stats.png")
        await interaction.edit_original_response(attachments=[file], view=view)


# TIME MODAL

class CategoryMessageTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, cog_instance, original_message_id, guild, category):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.original_message_id = original_message_id
        self.guild = guild
        self.category = category

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

            await interaction.response.defer()

            try:
                original_message = await interaction.channel.fetch_message(self.original_message_id)
                if original_message:

                    if self.original_message_id in self.cog.active_category_message_sessions:
                        session = self.cog.active_category_message_sessions[self.original_message_id]
                        view = session['view']
                        view.current_days = days
                        session['current_days'] = days

                        view.show_time_buttons = False

                        category_data = await self.cog._get_category_message_stats(self.guild, self.category, days, view.selected_role_id)

                        if category_data is None:
                            category_data = {
                                'total_messages': 0,
                                'channels': Counter(),
                                'users': Counter(),
                                'daily': Counter(),
                                'timestamp': time.time()
                            }

                        img_bytes = await generate_category_message_stats_image(self.guild, self.category, category_data, days, view.selected_role_id)

                        file = discord.File(
                            img_bytes, filename="category_message_stats.png")
                        view._update_buttons()
                        await original_message.edit(attachments=[file], view=view)

                    else:
                        await interaction.followup.send("❌ Session expired. Use the command again.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Original message not found.", ephemeral=True)
            except Exception as e:
                print(f"Error updating time period: {e}")
                await interaction.followup.send("❌ An error occurred while updating the time period.", ephemeral=True)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)


class CategoryTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, cog_instance, original_message_id, guild, category):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.original_message_id = original_message_id
        self.guild = guild
        self.category = category

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

            await interaction.response.defer()

            try:
                original_message = await interaction.channel.fetch_message(self.original_message_id)
                if original_message:

                    if self.original_message_id in self.cog.active_category_voice_sessions:
                        session = self.cog.active_category_voice_sessions[self.original_message_id]
                        view = session['view']
                        view.current_days = days
                        session['current_days'] = days

                        view.show_time_buttons = False

                        category_data = await self.cog._get_category_voice_stats(self.guild, self.category, days, view.selected_role_id)

                        if category_data is None:
                            category_data = {
                                'total_seconds': 0,
                                'channels': Counter(),
                                'users': Counter(),
                                'daily': Counter(),
                                'timestamp': time.time()
                            }

                        img_bytes = await generate_category_voice_stats_image(self.guild, self.category, category_data, days, view.selected_role_id)

                        file = discord.File(
                            img_bytes, filename="category_voice_stats.png")
                        view._update_buttons()
                        await original_message.edit(attachments=[file], view=view)

                    else:
                        await interaction.followup.send("❌ Session expired. Use the command again.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ Original message not found.", ephemeral=True)
            except Exception as e:
                print(f"Error updating time period: {e}")
                await interaction.followup.send("❌ An error occurred while updating the time period.", ephemeral=True)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)


class CategoryStatsView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, category: discord.CategoryChannel, days_back: int, stats_type: str, selected_role_id: str = None):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.category = category
        self.current_days = days_back
        self.stats_type = stats_type
        self.selected_role_id = selected_role_id
        self.show_time_buttons = False
        self._update_buttons()

    def _update_buttons(self):
        self.clear_items()
        self.add_item(CategoryRoleSelectMenu(
            self.guild, self.selected_role_id))

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id=f"category_{self.stats_type}_refresh"
        )
        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id=f"category_{self.stats_type}_time_settings"
        )

        refresh_button.callback = self.refresh_callback
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            days_7_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 7 else discord.ButtonStyle.secondary,
                label="7 Days",
                custom_id=f"category_{self.stats_type}_days_7"
            )
            days_14_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 14 else discord.ButtonStyle.secondary,
                label="14 Days",
                custom_id=f"category_{self.stats_type}_days_14"
            )
            days_30_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 30 else discord.ButtonStyle.secondary,
                label="30 Days",
                custom_id=f"category_{self.stats_type}_days_30"
            )
            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in [
                    7, 14, 30] else "Custom",
                custom_id=f"category_{self.stats_type}_custom_days"
            )

            days_7_button.callback = self.days_7_callback
            days_14_button.callback = self.days_14_callback
            days_30_button.callback = self.days_30_callback
            custom_button.callback = self.custom_days_callback

            self.add_item(days_7_button)
            self.add_item(days_14_button)
            self.add_item(days_30_button)
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
        if self.stats_type == 'message':
            modal = CategoryMessageTimeModal(
                self.cog, interaction.message.id, self.guild, self.category)
        else:
            modal = CategoryTimeModal(
                self.cog, interaction.message.id, self.guild, self.category)
        await interaction.response.send_modal(modal)

    async def handle_button_click(self, interaction: discord.Interaction, refresh: bool = False, days: int = None):
        try:
            await interaction.response.defer()

            if days:
                self.current_days = days
                self.show_time_buttons = False

            if self.stats_type == 'message':
                category_data = await self.cog._get_category_message_stats(self.guild, self.category, self.current_days, self.selected_role_id)

                if category_data is None:

                    category_data = {
                        'total_messages': 0,
                        'channels': Counter(),
                        'users': Counter(),
                        'daily': Counter(),
                        'timestamp': time.time()
                    }

                img_bytes = await generate_category_message_stats_image(self.guild, self.category, category_data, self.current_days, self.selected_role_id)
            else:
                category_data = await self.cog._get_category_voice_stats(self.guild, self.category, self.current_days, self.selected_role_id)

                if category_data is None:

                    category_data = {
                        'total_seconds': 0,
                        'channels': Counter(),
                        'users': Counter(),
                        'daily': Counter(),
                        'timestamp': time.time()
                    }

                img_bytes = await generate_category_voice_stats_image(self.guild, self.category, category_data, self.current_days, self.selected_role_id)

            self._update_buttons()
            file = discord.File(
                img_bytes, filename=f"category_{self.stats_type}_stats.png")
            await interaction.edit_original_response(attachments=[file], view=self)

            if self.stats_type == 'message' and interaction.message and interaction.message.id in self.cog.active_category_message_sessions:
                self.cog.active_category_message_sessions[interaction.message.id]['current_days'] = self.current_days
                self.cog.active_category_message_sessions[interaction.message.id][
                    'selected_role_id'] = self.selected_role_id
            elif self.stats_type == 'voice' and interaction.message and interaction.message.id in self.cog.active_category_voice_sessions:
                self.cog.active_category_voice_sessions[interaction.message.id]['current_days'] = self.current_days
                self.cog.active_category_voice_sessions[interaction.message.id][
                    'selected_role_id'] = self.selected_role_id

        except Exception as e:
            print(f"Error handling button click: {e}")
            import traceback
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred while updating the stats.", ephemeral=True)

    async def handle_time_settings(self, interaction: discord.Interaction):

        try:

            self.show_time_buttons = not self.show_time_buttons
            self._update_buttons()

            await interaction.response.edit_message(view=self)

        except Exception as e:
            print(f"Error handling time settings: {e}")
            try:
                await interaction.followup.send("❌ An error occurred while updating time settings.", ephemeral=True)
            except:
                pass


class CategoryStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.active_category_message_sessions = {}
        self.active_category_voice_sessions = {}

    category_group = app_commands.Group(
        name="category",
        description="Category statistics commands"
    )

    voice_group = app_commands.Group(
        name="voice",
        description="Voice statistics for categories",
        parent=category_group
    )

    message_group = app_commands.Group(
        name="message",
        description="Message statistics for categories",
        parent=category_group
    )

    # MESSAGE QUERIES

    async def _get_category_message_stats(self, guild: discord.Guild, category: discord.CategoryChannel, days_back: int, role_id: str = None):

        db_cog = self.bot.get_cog('DatabaseStats')
        if not db_cog:
            print("❌ DatabaseStats cog not found")
            return None

        try:
            role_ids = None
            if role_id and role_id != "none":
                role_ids = [int(role_id)]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            category_data = {
                'total_messages': 0,
                'channels': Counter(),
                'users': Counter(),
                'daily': Counter(),
            }

            total_stats = await db_cog.q_category_total_messages(
                guild_id=guild.id,
                category_id=category.id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )

            category_data['total_messages'] = total_stats.get(
                'total_messages', 0)

            top_channels = await db_cog.q_category_top5_text_channels(
                guild_id=guild.id,
                category_id=category.id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )

            for channel in top_channels:
                if channel.get('message_count', 0) > 0:
                    category_data['channels'][str(
                        channel['channel_id'])] = channel['message_count']

            top_users = await db_cog.q_category_top5_users_messages(
                guild_id=guild.id,
                category_id=category.id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )

            for user in top_users:
                if user.get('message_count', 0) > 0:
                    category_data['users'][str(
                        user['user_id'])] = user['message_count']

            timeseries_data = await db_cog.q_category_timeseries_messages(
                guild_id=guild.id,
                category_id=category.id,
                days=[1, 5, 10, 20, 30],
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            for days_str, count in timeseries_data.items():
                days = int(days_str.replace('d', ''))
                if days <= days_back:
                    category_data['daily'][days] = count

            category_data['timestamp'] = time.time()
            return category_data

        except Exception as e:
            print(f"❌ Error getting category message stats: {e}")
            import traceback
            traceback.print_exc()
            return None

    # VOICE ACTIVITY QUERIES

    async def _get_category_voice_stats(self, guild: discord.Guild, category: discord.CategoryChannel, days_back: int, role_id: str = None):

        db_cog = self.bot.get_cog('DatabaseStats')
        if not db_cog:
            print("❌ DatabaseStats cog not found")
            return None

        try:
            role_ids = None
            if role_id and role_id != "none":
                role_ids = [int(role_id)]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            category_data = {
                'total_seconds': 0,
                'channels': Counter(),
                'users': Counter(),
                'daily': Counter(),
            }

            total_stats = await db_cog.q_category_total_voice(
                guild_id=guild.id,
                category_id=category.id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )

            category_data['total_seconds'] = total_stats.get(
                'total_seconds', 0)

            top_channels = await db_cog.q_category_top5_voice_channels(
                guild_id=guild.id,
                category_id=category.id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )

            for channel in top_channels:
                if channel.get('total_seconds', 0) > 0:
                    category_data['channels'][str(
                        channel['channel_id'])] = channel['total_seconds']

            top_users = await db_cog.q_category_top5_users_voice(
                guild_id=guild.id,
                category_id=category.id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )

            for user in top_users:
                if user.get('total_seconds', 0) > 0:
                    category_data['users'][str(
                        user['user_id'])] = user['total_seconds']

            timeseries_data = await db_cog.q_category_timeseries_voice(
                guild_id=guild.id,
                category_id=category.id,
                days=[1, 5, 10, 20, 30],
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )

            for days_str, seconds in timeseries_data.items():
                days = int(days_str.replace('d', ''))
                if days <= days_back:
                    category_data['daily'][days] = seconds

            category_data['timestamp'] = time.time()
            return category_data

        except Exception as e:
            print(f"❌ Error getting category voice stats: {e}")
            import traceback
            traceback.print_exc()
            return None

    # COMMANDS

    @message_group.command(
        name="stats",
        description="Get message statistics for a category."
    )
    @discord.app_commands.describe(
        category="Select a category to get message stats for"
    )
    async def category_message_stats(self, interaction: discord.Interaction, category: discord.CategoryChannel):
        await interaction.response.defer()

        category_data = await self._get_category_message_stats(interaction.guild, category, 14)

        if category_data is None:

            category_data = {
                'total_messages': 0,
                'channels': Counter(),
                'users': Counter(),
                'daily': Counter(),
                'timestamp': time.time()
            }

        img_bytes = await generate_category_message_stats_image(interaction.guild, category, category_data, 14)

        view = CategoryStatsView(
            self, interaction.guild, category, days_back=14, stats_type='message'
        )

        file = discord.File(img_bytes, filename="category_message_stats.png")
        message = await interaction.followup.send(file=file, view=view)

        self.active_category_message_sessions[message.id] = {
            'guild_id': interaction.guild.id,
            'user_id': interaction.user.id,
            'category_id': category.id,
            'current_days': 14,
            'selected_role_id': None,
            'view': view
        }

    @voice_group.command(
        name="stats",
        description="Get voice statistics for a category."
    )
    @discord.app_commands.describe(
        category="Select a category to get voice stats for"
    )
    async def category_voice_stats(self, interaction: discord.Interaction, category: discord.CategoryChannel):
        await interaction.response.defer()

        category_data = await self._get_category_voice_stats(interaction.guild, category, 14)

        if category_data is None:

            category_data = {
                'total_seconds': 0,
                'channels': Counter(),
                'users': Counter(),
                'daily': Counter(),
                'timestamp': time.time()
            }

        img_bytes = await generate_category_voice_stats_image(interaction.guild, category, category_data, 14)

        view = CategoryStatsView(
            self, interaction.guild, category, days_back=14, stats_type='voice'
        )

        file = discord.File(img_bytes, filename="category_voice_stats.png")
        message = await interaction.followup.send(file=file, view=view)

        self.active_category_voice_sessions[message.id] = {
            'guild_id': interaction.guild.id,
            'user_id': interaction.user.id,
            'category_id': category.id,
            'current_days': 14,
            'selected_role_id': None,
            'view': view
        }


# SETUP

async def setup(bot):
    await bot.add_cog(CategoryStats(bot))
