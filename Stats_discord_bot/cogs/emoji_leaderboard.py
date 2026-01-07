import discord
from discord.ext import commands
from discord import app_commands
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import re
import asyncio
import os
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont
import io
import aiohttp
from pilmoji import Pilmoji
import traceback
import json
from pathlib import Path


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


# CONFIGURATION

class EmojiConfig:

    EMOJIS_PER_PAGE = 10
    USERS_PER_PAGE = 10
    MAX_ROLES_IN_SELECT = 25
    DEFAULT_DAYS_BACK = 14
    TIME_PERIODS = [7, 14, 30]
    MAX_CUSTOM_DAYS = 2000

    # Paths
    TEMPLATE_PATH = BASE_DIR / "assets" / "images" / "leaderboards final png.png"
    FONT_PATH = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"

    # Colors
    COLORS = {
        'gold': (255, 215, 0, 220),
        'silver': (192, 192, 192, 220),
        'bronze': (205, 127, 50, 220),
        'default': (93, 0, 136, 255),
        'background': (0, 0, 0, 220)
    }


# FONTS

def get_fonts():

    try:
        # Try to load the custom font first
        custom_font_path = EmojiConfig.FONT_PATH
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


# DRAWING FUNCTIONS

def draw_text_with_stroke_and_emoji(pilmoji, position, text, font, fill, stroke_fill="black", stroke_width=1):

    x, y = position

    for dx in [-stroke_width, 0, stroke_width]:
        for dy in [-stroke_width, 0, stroke_width]:
            if dx != 0 or dy != 0:
                pilmoji.text((x + dx, y + dy), text,
                             font=font, fill=stroke_fill)

    pilmoji.text((x, y), text, font=font, fill=fill)


def draw_text_centered_with_stroke_and_emoji(pilmoji, text, center_position, font, fill="white", stroke_fill="black", stroke_width=2, max_width=None):

    center_x, center_y = center_position

    temp_draw = ImageDraw.Draw(Image.new('RGB', (1, 1)))
    bbox = temp_draw.textbbox((0, 0), text, font=font)
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
                bbox = temp_draw.textbbox((0, 0), text, font=smaller_font)
                text_width = bbox[2] - bbox[0]

                if text_width <= max_width:
                    text_height = bbox[3] - bbox[1]
                    x = center_x - (text_width // 2)
                    y = center_y - (text_height // 2)
                    draw_text_with_stroke_and_emoji(
                        pilmoji, (x, y), text, smaller_font, fill, stroke_fill, stroke_width)
                    return
            except:
                continue
    draw_text_with_stroke_and_emoji(pilmoji, (x, y), text, font, fill,
                                    stroke_fill, stroke_width)


# RECTANGLE FUNCTIONS

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


def fit_text_to_rectangle(draw, text, text_start_x, text_start_y, rect_center_x, rect_center_y, rect_width, rect_height, is_username=False):

    font_paths = [BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"]

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


async def add_server_profile_pic_and_name(image, guild, font_huge, channel=None):

    temp_draw = ImageDraw.Draw(image)

    # POSITIONS

    # Profile picture at (7, 10)
    avatar_x, avatar_y = 7, 10
    avatar_size = (60, 60)

    text_start_x = 85
    text_start_y = 30

    # Rectangle for text constraints
    rect_center_x = 150
    rect_center_y = 30
    rect_width = 183
    rect_height = 35

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
            image.paste(avatar_area.convert('RGB'), (avatar_x, avatar_y))

    except Exception as e:
        print(f"❌ Could not add server icon: {e}")

    with Pilmoji(image) as pilmoji:
        display_text = f"#{channel.name}" if channel else guild.name

        fitted_text, text_font, text_pos = fit_text_to_rectangle(
            temp_draw,
            display_text,
            text_start_x,
            text_start_y,
            rect_center_x,
            rect_center_y,
            rect_width,
            rect_height,
            is_username=True
        )

        if fitted_text and text_font:
            draw_text_with_stroke_and_emoji(pilmoji, text_pos, fitted_text,
                                            font=text_font, fill="white", stroke_fill="black", stroke_width=1)
        else:
            draw_text_with_stroke_and_emoji(pilmoji, (text_start_x, text_start_y), display_text,
                                            font=font_huge, fill="white", stroke_fill="black", stroke_width=1)


async def generate_emoji_leaderboard_image(
    guild: discord.Guild,
    leaderboard_data: list,
    leaderboard_type: str,
    days_back: int,
    role_id: str = None,
    target_member: discord.Member = None,
    usage_type: str = 'all',
    page: int = 0,
    total_pages: int = 1,
    channel: discord.TextChannel = None
):

    template_path = EmojiConfig.TEMPLATE_PATH
    try:
        image = Image.open(template_path)
    except FileNotFoundError:
        image = Image.new('RGB', (800, 600), color='#2F3136')
        print(f"❌ Template not found at: {template_path}")

    if image.mode != "RGB":
        image = image.convert("RGB")

    draw = ImageDraw.Draw(image)
    font_small, font_medium, font_large, font_larger, font_huge, font_giant = get_fonts()

    if leaderboard_type == 'channel' and channel:
        await add_server_profile_pic_and_name(image, guild, font_huge, channel)
    else:
        await add_server_profile_pic_and_name(image, guild, font_huge)

    # ROLE FILTER / DATE / TIME RANGE
    role_text = "No Filter"
    if role_id and role_id != "none":
        role = guild.get_role(int(role_id))
        role_text = role.name if role else "Unknown Role"

    with Pilmoji(image) as pilmoji:
        # role filter
        draw_text_with_stroke_and_emoji(pilmoji, (70, 424), role_text,
                                        font=font_small, fill="white", stroke_fill="black", stroke_width=1)
        # time period
        draw_text_with_stroke_and_emoji(pilmoji, (630, 422), f"{days_back} days",
                                        font=font_small, fill="white", stroke_fill="black", stroke_width=1)
        # created on
        draw_text_with_stroke_and_emoji(pilmoji, (550, 38), datetime.now(timezone.utc).strftime(
            "%B %d, %Y"), font=font_small, fill="white", stroke_fill="black", stroke_width=1)

        # Pagination
        draw_text_with_stroke_and_emoji(
            pilmoji, (400, 450), f"Page {page + 1}/{total_pages}", font=font_medium, fill="white", stroke_fill="black", stroke_width=1)

        # LEADERBOARD CONTENT
        if leaderboard_data:
            start_idx = page * EmojiConfig.EMOJIS_PER_PAGE
            end_idx = min(start_idx + EmojiConfig.EMOJIS_PER_PAGE,
                          len(leaderboard_data))
            page_data = leaderboard_data[start_idx:end_idx]

            image_width, _ = image.size

            if len(page_data) <= 5:
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

            for i, ((emoji_str, total_count, message_count, reaction_count), (box_x, box_y)) in enumerate(zip(page_data, positions)):
                global_rank = start_idx + i + 1

                draw_rounded_rectangle(draw, [box_x, box_y, box_x + box_width, box_y + box_height],
                                       radius=8, fill=EmojiConfig.COLORS['background'], outline=None)

                placement_width = 35
                if global_rank == 1:
                    placement_color = EmojiConfig.COLORS['gold']  # Gold
                elif global_rank == 2:
                    placement_color = EmojiConfig.COLORS['silver']  # Silver
                elif global_rank == 3:
                    placement_color = EmojiConfig.COLORS['bronze']  # Bronze
                else:
                    placement_color = EmojiConfig.COLORS['default']  # Purple

                draw_rounded_rectangle(draw, [box_x, box_y, box_x + placement_width, box_y + box_height],
                                       radius=8, fill=placement_color, outline=None)

                total_uses = sum(count for _, count, _, _ in leaderboard_data)
                percentage = (total_count / total_uses) * \
                    100 if total_uses > 0 else 0
                value_text = f"{total_count:,} uses ({percentage:.1f}%)"

                rank_text = f"#{global_rank}"
                rank_bbox = draw.textbbox((0, 0), rank_text, font=font_medium)
                rank_width = rank_bbox[2] - rank_bbox[0]

                value_bbox = draw.textbbox(
                    (0, 0), value_text, font=font_medium)
                value_width = value_bbox[2] - value_bbox[0]

                emoji_name = ""
                display_emoji = emoji_str

                if emoji_str.startswith('<') and emoji_str.endswith('>'):

                    emoji_parts = emoji_str.split(':')
                    if len(emoji_parts) >= 3:
                        emoji_name = emoji_parts[1]

                        emoji_id = emoji_parts[2][:-1]
                        guild_emoji = guild.get_emoji(int(emoji_id))
                        if guild_emoji:
                            display_emoji = str(guild_emoji)
                    else:
                        emoji_name = "custom"
                else:
                    emoji_name = ""

                emoji_space = 40
                if emoji_name:
                    name_bbox = draw.textbbox(
                        (0, 0), emoji_name, font=font_medium)
                    name_width = name_bbox[2] - name_bbox[0]
                    total_content_width = emoji_space + name_width + 10
                else:
                    total_content_width = emoji_space

                available_width = box_width - placement_width - value_width - 30

                name_font = font_medium
                if emoji_name and total_content_width > available_width:
                    try:
                        smaller_font = ImageFont.truetype(font_medium.path, 16) if hasattr(
                            font_medium, 'path') else font_small
                        smaller_bbox = draw.textbbox(
                            (0, 0), emoji_name, font=smaller_font)
                        smaller_width = smaller_bbox[2] - smaller_bbox[0]

                        if (emoji_space + smaller_width + 10) <= available_width:
                            name_font = smaller_font
                        else:

                            while len(emoji_name) > 3:
                                emoji_name = emoji_name[:-4] + "..."
                                truncated_bbox = draw.textbbox(
                                    (0, 0), emoji_name, font=name_font)
                                truncated_width = truncated_bbox[2] - \
                                    truncated_bbox[0]
                                if (emoji_space + truncated_width + 10) <= available_width:
                                    break
                    except:

                        while len(emoji_name) > 3 and (emoji_space + name_width + 10) > available_width:
                            emoji_name = emoji_name[:-4] + "..."
                            name_bbox = draw.textbbox(
                                (0, 0), emoji_name, font=name_font)
                            name_width = name_bbox[2] - name_bbox[0]

                rank_height = rank_bbox[3] - rank_bbox[1]
                value_height = value_bbox[3] - value_bbox[1]

                rank_y = box_y + (box_height - rank_height) // 2
                value_y = box_y + (box_height - value_height) // 2

                rank_x = box_x + (placement_width - rank_width) // 2
                draw_text_with_stroke_and_emoji(pilmoji, (rank_x, rank_y), rank_text,
                                                font=font_medium, fill="white", stroke_fill="black", stroke_width=1)

                emoji_x = box_x + placement_width + 5
                emoji_y = box_y + (box_height - 30) // 2 + 5

                try:

                    pilmoji.text((emoji_x, emoji_y), display_emoji,
                                 font=font_large, fill=(255, 255, 255))
                except Exception as e:
                    print(f"❌ Error drawing emoji {display_emoji}: {e}")

                    pilmoji.text((emoji_x, emoji_y), display_emoji,
                                 font=font_large, fill=(255, 255, 255))

                if emoji_name:
                    name_height = name_bbox[3] - name_bbox[1] if name_font == font_medium else draw.textbbox(
                        (0, 0), emoji_name, font=name_font)[3] - draw.textbbox((0, 0), emoji_name, font=name_font)[1]
                    name_y = box_y + (box_height - name_height) // 2
                    name_x = emoji_x + 35
                    draw_text_with_stroke_and_emoji(pilmoji, (name_x, name_y), emoji_name,
                                                    font=name_font, fill="white", stroke_fill="black", stroke_width=1)

                value_x = box_x + box_width - value_width - 8
                draw_text_with_stroke_and_emoji(pilmoji, (value_x, value_y), value_text,
                                                font=font_medium, fill="white", stroke_fill="black", stroke_width=1)
        else:

            cx, cy = image.size[0] // 2, image.size[1] // 2
            draw_text_centered_with_stroke_and_emoji(
                pilmoji, "NO DATA AVAILABLE", (cx, cy), font=font_giant, fill="white", stroke_fill="black", stroke_width=3)

    img_bytes = io.BytesIO()
    image.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    return img_bytes


async def generate_user_emoji_leaderboard_image(
    guild: discord.Guild,
    user_data: list,
    days_back: int,
    role_id: str = None,
    usage_type: str = "all",
    page: int = 0,
    total_pages: int = 1
):

    template_path = EmojiConfig.TEMPLATE_PATH
    try:
        image = Image.open(template_path)
    except FileNotFoundError:
        image = Image.new('RGB', (800, 600), color='#2F3136')
        print(f"❌ Template not found at: {template_path}")

    if image.mode != "RGB":
        image = image.convert("RGB")

    draw = ImageDraw.Draw(image)
    font_small, font_medium, font_large, font_larger, font_huge, font_giant = get_fonts()

    await add_server_profile_pic_and_name(image, guild, font_huge)

    role_text = "No Filter"
    if role_id and role_id != "none":
        role = guild.get_role(int(role_id))
        role_text = role.name if role else "Unknown Role"

    with Pilmoji(image) as pilmoji:
        # role filter
        draw_text_with_stroke_and_emoji(pilmoji, (70, 424), role_text,
                                        font=font_small, fill="white", stroke_fill="black", stroke_width=1)
        # time period
        draw_text_with_stroke_and_emoji(pilmoji, (630, 422), f"{days_back} days",
                                        font=font_small, fill="white", stroke_fill="black", stroke_width=1)
        # created on
        draw_text_with_stroke_and_emoji(pilmoji, (550, 38), datetime.now(timezone.utc).strftime(
            "%B %d, %Y"), font=font_small, fill="white", stroke_fill="black", stroke_width=1)

        # pagination
        draw_text_with_stroke_and_emoji(
            pilmoji, (400, 450), f"Page {page + 1}/{total_pages}", font=font_medium, fill="white", stroke_fill="black", stroke_width=1)

        # LEADERBOARD CONTENT
        if user_data:
            start_idx = page * EmojiConfig.USERS_PER_PAGE
            end_idx = min(start_idx + EmojiConfig.USERS_PER_PAGE,
                          len(user_data))
            page_data = user_data[start_idx:end_idx]

            image_width, _ = image.size

            if len(page_data) <= 5:
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

            for i, ((user_id, total_count, message_count, reaction_count), (box_x, box_y)) in enumerate(zip(page_data, positions)):
                global_rank = start_idx + i + 1

                draw_rounded_rectangle(draw, [box_x, box_y, box_x + box_width, box_y + box_height],
                                       radius=8, fill=EmojiConfig.COLORS['background'], outline=None)

                placement_width = 35
                if global_rank == 1:
                    placement_color = EmojiConfig.COLORS['gold']  # Gold
                elif global_rank == 2:
                    placement_color = EmojiConfig.COLORS['silver']  # Silver
                elif global_rank == 3:
                    placement_color = EmojiConfig.COLORS['bronze']  # Bronze
                else:
                    placement_color = EmojiConfig.COLORS['default']  # Purple

                draw_rounded_rectangle(draw, [box_x, box_y, box_x + placement_width, box_y + box_height],
                                       radius=8, fill=placement_color, outline=None)

                total_uses = sum(count for _, count, _, _ in user_data)
                percentage = (total_count / total_uses) * \
                    100 if total_uses > 0 else 0
                value_text = f"{total_count:,} uses ({percentage:.1f}%)"

                rank_text = f"#{global_rank}"
                rank_bbox = draw.textbbox((0, 0), rank_text, font=font_medium)
                rank_width = rank_bbox[2] - rank_bbox[0]

                value_bbox = draw.textbbox(
                    (0, 0), value_text, font=font_medium)
                value_width = value_bbox[2] - value_bbox[0]

                member = guild.get_member(user_id)
                if member:
                    username = member.display_name
                else:
                    try:
                        user = await guild._state.fetch_user(user_id)
                        username = user.name
                    except:
                        username = f"Unknown User ({user_id})"

                available_width = box_width - placement_width - value_width - 30

                name_bbox = draw.textbbox((0, 0), username, font=font_medium)
                name_width = name_bbox[2] - name_bbox[0]

                name_font = font_medium
                if name_width > available_width:
                    try:
                        smaller_font = ImageFont.truetype(font_medium.path, 16) if hasattr(
                            font_medium, 'path') else font_small
                        smaller_bbox = draw.textbbox(
                            (0, 0), username, font=smaller_font)
                        smaller_width = smaller_bbox[2] - smaller_bbox[0]

                        if smaller_width <= available_width:
                            name_font = smaller_font
                        else:

                            display_name = username
                            while len(display_name) > 3:
                                display_name = display_name[:-4] + "..."
                                truncated_bbox = draw.textbbox(
                                    (0, 0), display_name, font=name_font)
                                truncated_width = truncated_bbox[2] - \
                                    truncated_bbox[0]
                                if truncated_width <= available_width:
                                    username = display_name
                                    break
                    except:

                        display_name = username
                        while len(display_name) > 3 and name_width > available_width:
                            display_name = display_name[:-4] + "..."
                            name_bbox = draw.textbbox(
                                (0, 0), display_name, font=name_font)
                            name_width = name_bbox[2] - name_bbox[0]
                        username = display_name

                rank_height = rank_bbox[3] - rank_bbox[1]
                name_height = name_bbox[3] - name_bbox[1] if name_font == font_medium else draw.textbbox(
                    (0, 0), username, font=name_font)[3] - draw.textbbox((0, 0), username, font=name_font)[1]
                value_height = value_bbox[3] - value_bbox[0]

                rank_y = box_y + (box_height - rank_height) // 2
                name_y = box_y + (box_height - name_height) // 2
                value_y = box_y + (box_height - value_height) // 2

                rank_x = box_x + (placement_width - rank_width) // 2
                draw_text_with_stroke_and_emoji(pilmoji, (rank_x, rank_y), rank_text,
                                                font=font_medium, fill="white", stroke_fill="black", stroke_width=1)

                name_x = box_x + placement_width + 8
                draw_text_with_stroke_and_emoji(pilmoji, (name_x, name_y), username,
                                                font=name_font, fill="white", stroke_fill="black", stroke_width=1)

                value_x = box_x + box_width - value_width - 8
                draw_text_with_stroke_and_emoji(pilmoji, (value_x, value_y), value_text,
                                                font=font_medium, fill="white", stroke_fill="black", stroke_width=1)
        else:
            cx, cy = image.size[0] // 2, image.size[1] // 2
            draw_text_centered_with_stroke_and_emoji(
                pilmoji, "NO DATA AVAILABLE", (cx, cy), font=font_giant, fill="white", stroke_fill="black", stroke_width=3)

    img_bytes = io.BytesIO()
    image.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    return img_bytes


# USAGE TYPE SELECT MENU

class UsageTypeSelectMenu(discord.ui.Select):
    def __init__(self, current_usage_type: str = 'all'):
        options = [
            discord.SelectOption(
                label="All Usage",
                value="all",
                description="Show all emoji usage",
                emoji="📊",
                default=(current_usage_type == 'all')
            ),
            discord.SelectOption(
                label="Messages Only",
                value="message",
                description="Show emojis used in messages",
                emoji="💬",
                default=(current_usage_type == 'message')
            ),
            discord.SelectOption(
                label="Reactions Only",
                value="reaction",
                description="Show emojis used as reactions",
                emoji="❤️",
                default=(current_usage_type == 'reaction')
            )
        ]

        super().__init__(
            placeholder="Filter by usage type...",
            options=options,
            custom_id="usage_type_select",
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False, thinking=False)
        view = self.view
        if hasattr(view, 'usage_type'):
            view.usage_type = self.values[0]
            view.page = 0
            await view.update_message(interaction)


# ROLE SELECT MENUS

class EmojiRoleSelectMenu(discord.ui.Select):
    def __init__(self, guild: discord.Guild, current_role_id: str = None):
        self.guild = guild

        roles = [role for role in guild.roles if role.name != "@everyone"]
        roles.sort(key=lambda x: x.position, reverse=True)
        roles = roles[:EmojiConfig.MAX_ROLES_IN_SELECT]

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
            options.append(discord.SelectOption(
                label=role.name,
                value=str(role.id),
                description=f"Filter by {role.name} role",
                default=(str(role.id) == current_role_id)
            ))

        super().__init__(
            placeholder="Filter by role...",
            options=options,
            custom_id="emoji_role_filter_select",
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False, thinking=False)
        view = self.view
        if hasattr(view, 'selected_role_id'):
            view.selected_role_id = self.values[0]
            view.page = 0
            await view.update_message(interaction)


# BUTTONS

class EmojiPrevButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="◀️", custom_id="emoji_leaderboard_prev")

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


class EmojiNextButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="▶️", custom_id="emoji_leaderboard_next")

    async def callback(self, interaction: discord.Interaction):

        if self.disabled:
            await interaction.response.defer(ephemeral=True, thinking=False)
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        view.page += 1
        await view.update_message(interaction)


class EmojiPageIndicator(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary,
                         label="Page 1/1", custom_id="emoji_leaderboard_page", disabled=True)


class RefreshButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="🔄", custom_id="emoji_stats_refresh")

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        await self.view.update_message(interaction)


class TimeSettingsButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="⏰ Time Settings", custom_id="emoji_stats_time_settings")

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.view.show_time_buttons = not self.view.show_time_buttons
        await self.view.update_message(interaction)


class Days7Button(discord.ui.Button):
    def __init__(self, days_back):
        super().__init__(
            style=discord.ButtonStyle.primary if days_back == 7 else discord.ButtonStyle.secondary,
            label="7 Days", custom_id="emoji_stats_days_7"
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.view.days_back = 7
        self.view.page = 0
        self.view.show_time_buttons = False
        await self.view.update_message(interaction)


class Days14Button(discord.ui.Button):
    def __init__(self, days_back):
        super().__init__(
            style=discord.ButtonStyle.primary if days_back == 14 else discord.ButtonStyle.secondary,
            label="14 Days", custom_id="emoji_stats_days_14"
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.view.days_back = 14
        self.view.page = 0
        self.view.show_time_buttons = False
        await self.view.update_message(interaction)


class Days30Button(discord.ui.Button):
    def __init__(self, days_back):
        super().__init__(
            style=discord.ButtonStyle.primary if days_back == 30 else discord.ButtonStyle.secondary,
            label="30 Days", custom_id="emoji_stats_days_30"
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.view.days_back = 30
        self.view.page = 0
        self.view.show_time_buttons = False
        await self.view.update_message(interaction)


class DaysCustomButton(discord.ui.Button):
    def __init__(self, days_back):
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Custom", custom_id="emoji_stats_days_custom"
        )

    async def callback(self, interaction: discord.Interaction):
        modal = CustomDaysModal(self.view)
        await interaction.response.send_modal(modal)


class CustomDaysModal(discord.ui.Modal, title="Custom Time Range"):
    def __init__(self, view):
        super().__init__(timeout=300)
        self.view = view

        self.days_input = discord.ui.TextInput(
            label="Number of Days",
            placeholder="Enter number of days (max 2000)",
            default=str(self.view.days_back),
            min_length=1,
            max_length=4,
            required=True
        )
        self.add_item(self.days_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days = int(self.days_input.value)
            if days <= 0:
                await interaction.response.send_message("Please enter a positive number.", ephemeral=True)
                return
            if days > EmojiConfig.MAX_CUSTOM_DAYS:
                await interaction.response.send_message(f"Maximum is {EmojiConfig.MAX_CUSTOM_DAYS} days.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=False, thinking=False)
            self.view.days_back = days
            self.view.page = 0
            self.view.show_time_buttons = False
            await self.view.update_message(interaction)

        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)


# EMOJI LEADERBOARD VIEW

class EmojiLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, days_back: int = 14, page: int = 0, selected_role_id: str = None, usage_type: str = 'all'):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.days_back = days_back
        self.page = page
        self.selected_role_id = selected_role_id
        self.usage_type = usage_type
        self.show_time_buttons = False
        self.message = None

    async def _update_buttons(self, total_pages: int = 1):

        self.clear_items()

        self.add_item(UsageTypeSelectMenu(self.usage_type))

        self.add_item(EmojiRoleSelectMenu(self.guild, self.selected_role_id))

        prev_button = EmojiPrevButton()
        next_button = EmojiNextButton()
        page_indicator = EmojiPageIndicator()

        prev_button.disabled = (self.page == 0 or total_pages <= 1)
        next_button.disabled = (
            self.page >= total_pages - 1 or total_pages <= 1)
        page_indicator.label = f"Page {self.page + 1}/{total_pages}"

        self.add_item(prev_button)
        self.add_item(page_indicator)
        self.add_item(next_button)

        self.add_item(RefreshButton())

        self.add_item(TimeSettingsButton())

        if self.show_time_buttons:
            self.add_item(Days7Button(self.days_back))
            self.add_item(Days14Button(self.days_back))
            self.add_item(Days30Button(self.days_back))
            self.add_item(DaysCustomButton(self.days_back))

    async def generate_image(self):

        emoji_data = await self.cog.get_emoji_stats(self.guild, self.days_back, self.usage_type, self.selected_role_id)

        total_emojis = len(emoji_data)
        total_pages = max(
            1, (total_emojis + EmojiConfig.EMOJIS_PER_PAGE - 1) // EmojiConfig.EMOJIS_PER_PAGE)

        if self.page >= total_pages:
            self.page = max(0, total_pages - 1)

        return await generate_emoji_leaderboard_image(
            guild=self.guild,
            leaderboard_data=emoji_data,
            leaderboard_type='server',
            days_back=self.days_back,
            role_id=self.selected_role_id,
            usage_type=self.usage_type,
            page=self.page,
            total_pages=total_pages
        ), total_pages

    async def update_message(self, interaction: discord.Interaction = None):

        try:
            img_bytes, total_pages = await self.generate_image()
            await self._update_buttons(total_pages)

            file = discord.File(img_bytes, filename="emoji_leaderboard.png")

            if interaction:
                await interaction.edit_original_response(attachments=[file], view=self)
            elif self.message:
                await self.message.edit(attachments=[file], view=self)

        except Exception as e:
            print(f"Error updating emoji leaderboard message: {e}")
            try:
                if interaction:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# EMOJI USER LEADERBOARD VIEW

class UserEmojiLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, days_back: int = 14, page: int = 0, selected_role_id: str = None, usage_type: str = 'all'):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.days_back = days_back
        self.page = page
        self.selected_role_id = selected_role_id
        self.usage_type = usage_type
        self.show_time_buttons = False
        self.message = None

    async def _update_buttons(self, total_pages: int = 1):

        self.clear_items()

        self.add_item(UsageTypeSelectMenu(self.usage_type))

        self.add_item(EmojiRoleSelectMenu(self.guild, self.selected_role_id))

        prev_button = EmojiPrevButton()
        next_button = EmojiNextButton()
        page_indicator = EmojiPageIndicator()

        prev_button.disabled = (self.page == 0 or total_pages <= 1)
        next_button.disabled = (
            self.page >= total_pages - 1 or total_pages <= 1)
        page_indicator.label = f"Page {self.page + 1}/{total_pages}"

        self.add_item(prev_button)
        self.add_item(page_indicator)
        self.add_item(next_button)

        self.add_item(RefreshButton())

        self.add_item(TimeSettingsButton())

        if self.show_time_buttons:
            self.add_item(Days7Button(self.days_back))
            self.add_item(Days14Button(self.days_back))
            self.add_item(Days30Button(self.days_back))
            self.add_item(DaysCustomButton(self.days_back))

    async def generate_image(self):

        user_data = await self.cog.get_user_emoji_stats(self.guild, self.days_back, self.usage_type, self.selected_role_id)

        total_users = len(user_data)
        total_pages = max(
            1, (total_users + EmojiConfig.USERS_PER_PAGE - 1) // EmojiConfig.USERS_PER_PAGE)

        if self.page >= total_pages:
            self.page = max(0, total_pages - 1)

        return await generate_user_emoji_leaderboard_image(
            guild=self.guild,
            user_data=user_data,
            days_back=self.days_back,
            role_id=self.selected_role_id,
            usage_type=self.usage_type,
            page=self.page,
            total_pages=total_pages
        ), total_pages

    async def update_message(self, interaction: discord.Interaction = None):

        try:
            img_bytes, total_pages = await self.generate_image()
            await self._update_buttons(total_pages)

            file = discord.File(
                img_bytes, filename="user_emoji_leaderboard.png")

            if interaction:
                await interaction.edit_original_response(attachments=[file], view=self)
            elif self.message:
                await self.message.edit(attachments=[file], view=self)

        except Exception as e:
            print(f"Error updating user emoji leaderboard message: {e}")
            try:
                if interaction:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# EMOJI CHANNEL LEADERBOARD VIEW
class ChannelEmojiLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, channel: discord.TextChannel, days_back: int = 14, page: int = 0, selected_role_id: str = None, usage_type: str = 'all'):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.channel = channel
        self.days_back = days_back
        self.page = page
        self.selected_role_id = selected_role_id
        self.usage_type = usage_type
        self.show_time_buttons = False
        self.message = None

    async def _update_buttons(self, total_pages: int = 1):

        self.clear_items()

        self.add_item(UsageTypeSelectMenu(self.usage_type))

        self.add_item(EmojiRoleSelectMenu(self.guild, self.selected_role_id))

        prev_button = EmojiPrevButton()
        next_button = EmojiNextButton()
        page_indicator = EmojiPageIndicator()

        prev_button.disabled = (self.page == 0 or total_pages <= 1)
        next_button.disabled = (
            self.page >= total_pages - 1 or total_pages <= 1)
        page_indicator.label = f"Page {self.page + 1}/{total_pages}"

        self.add_item(prev_button)
        self.add_item(page_indicator)
        self.add_item(next_button)

        self.add_item(RefreshButton())

        self.add_item(TimeSettingsButton())

        if self.show_time_buttons:
            self.add_item(Days7Button(self.days_back))
            self.add_item(Days14Button(self.days_back))
            self.add_item(Days30Button(self.days_back))
            self.add_item(DaysCustomButton(self.days_back))

    async def generate_image(self):

        emoji_data = await self.cog.get_channel_emoji_stats(self.guild, self.channel.id, self.days_back, self.usage_type, self.selected_role_id)

        total_emojis = len(emoji_data)
        total_pages = max(
            1, (total_emojis + EmojiConfig.EMOJIS_PER_PAGE - 1) // EmojiConfig.EMOJIS_PER_PAGE)

        if self.page >= total_pages:
            self.page = max(0, total_pages - 1)

        return await generate_emoji_leaderboard_image(
            guild=self.guild,
            leaderboard_data=emoji_data,
            leaderboard_type='channel',
            days_back=self.days_back,
            role_id=self.selected_role_id,
            usage_type=self.usage_type,
            page=self.page,
            total_pages=total_pages,
            channel=self.channel
        ), total_pages

    async def update_message(self, interaction: discord.Interaction = None):

        try:
            img_bytes, total_pages = await self.generate_image()
            await self._update_buttons(total_pages)

            file = discord.File(
                img_bytes, filename="channel_emoji_leaderboard.png")

            if interaction:
                await interaction.edit_original_response(attachments=[file], view=self)
            elif self.message:
                await self.message.edit(attachments=[file], view=self)

        except Exception as e:
            print(f"Error updating channel emoji leaderboard message: {e}")
            try:
                if interaction:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# EMOJI CATEGORY LEADERBOARD VIEW
class CategoryEmojiLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, category: discord.CategoryChannel, days_back: int = 14, page: int = 0, selected_role_id: str = None, usage_type: str = 'all'):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.category = category
        self.days_back = days_back
        self.page = page
        self.selected_role_id = selected_role_id
        self.usage_type = usage_type
        self.show_time_buttons = False
        self.message = None

    async def _update_buttons(self, total_pages: int = 1):

        self.clear_items()

        self.add_item(UsageTypeSelectMenu(self.usage_type))

        self.add_item(EmojiRoleSelectMenu(self.guild, self.selected_role_id))

        prev_button = EmojiPrevButton()
        next_button = EmojiNextButton()
        page_indicator = EmojiPageIndicator()

        prev_button.disabled = (self.page == 0 or total_pages <= 1)
        next_button.disabled = (
            self.page >= total_pages - 1 or total_pages <= 1)
        page_indicator.label = f"Page {self.page + 1}/{total_pages}"

        self.add_item(prev_button)
        self.add_item(page_indicator)
        self.add_item(next_button)

        self.add_item(RefreshButton())

        self.add_item(TimeSettingsButton())

        if self.show_time_buttons:
            self.add_item(Days7Button(self.days_back))
            self.add_item(Days14Button(self.days_back))
            self.add_item(Days30Button(self.days_back))
            self.add_item(DaysCustomButton(self.days_back))

    async def generate_image(self):

        emoji_data = await self.cog.get_category_emoji_stats(self.guild, self.category.id, self.days_back, self.usage_type, self.selected_role_id)

        total_emojis = len(emoji_data)
        total_pages = max(
            1, (total_emojis + EmojiConfig.EMOJIS_PER_PAGE - 1) // EmojiConfig.EMOJIS_PER_PAGE)

        if self.page >= total_pages:
            self.page = max(0, total_pages - 1)

        return await generate_emoji_leaderboard_image(
            guild=self.guild,
            leaderboard_data=emoji_data,
            leaderboard_type='category',
            days_back=self.days_back,
            role_id=self.selected_role_id,
            usage_type=self.usage_type,
            page=self.page,
            total_pages=total_pages
        ), total_pages

    async def update_message(self, interaction: discord.Interaction = None):

        try:
            img_bytes, total_pages = await self.generate_image()
            await self._update_buttons(total_pages)

            file = discord.File(
                img_bytes, filename="category_emoji_leaderboard.png")

            if interaction:
                await interaction.edit_original_response(attachments=[file], view=self)
            elif self.message:
                await self.message.edit(attachments=[file], view=self)

        except Exception as e:
            print(f"Error updating category emoji leaderboard message: {e}")
            try:
                if interaction:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# QUERY FUNCTIONS

class EmojiLeaderboard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.initialized = False

    async def cog_load(self):

        self.initialized = True

    async def get_database_cog(self):

        return self.bot.get_cog('DatabaseStats')

    async def get_emoji_stats(self, guild: discord.Guild, days_back: int, usage_type: str = 'all', role_id: str = None):

        db_cog = await self.get_database_cog()
        if not db_cog:
            return []

        role_filter_ids = None
        if role_id and role_id != "none":
            role = guild.get_role(int(role_id))
            if role:
                role_filter_ids = [role.id]

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days_back)

        emoji_data = await db_cog.q_emoji_server_leaderboard(
            guild_id=guild.id,
            limit=100,
            role_filter_ids=role_filter_ids,
            start_time=start_time,
            end_time=end_time,
            usage_type=usage_type if usage_type != 'all' else None
        )

        formatted_data = []
        for emoji in emoji_data:

            total_count = emoji['usage_count']

            if usage_type == 'all':

                message_count = total_count // 2
                reaction_count = total_count - message_count
            elif usage_type == 'message':
                message_count = total_count
                reaction_count = 0
            elif usage_type == 'reaction':
                message_count = 0
                reaction_count = total_count
            else:
                message_count = 0
                reaction_count = 0

            formatted_data.append((
                emoji['emoji_str'],
                total_count,
                message_count,
                reaction_count
            ))

        return formatted_data

    async def get_user_emoji_stats(self, guild: discord.Guild, days_back: int, usage_type: str = 'all', role_id: str = None):

        db_cog = await self.get_database_cog()
        if not db_cog:
            return []

        role_filter_ids = None
        if role_id and role_id != "none":
            role = guild.get_role(int(role_id))
            if role:
                role_filter_ids = [role.id]

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days_back)

        user_data = await db_cog.q_emoji_user_leaderboard(
            guild_id=guild.id,
            limit=100,
            role_filter_ids=role_filter_ids,
            start_time=start_time,
            end_time=end_time,
            usage_type=usage_type if usage_type != 'all' else None
        )

        formatted_data = []
        for user in user_data:
            total_count = user['usage_count']

            if usage_type == 'all':
                message_count = total_count // 2
                reaction_count = total_count - message_count
            elif usage_type == 'message':
                message_count = total_count
                reaction_count = 0
            elif usage_type == 'reaction':
                message_count = 0
                reaction_count = total_count
            else:
                message_count = 0
                reaction_count = 0

            formatted_data.append((
                user['user_id'],
                total_count,
                message_count,
                reaction_count
            ))

        return formatted_data

    async def get_channel_emoji_stats(self, guild: discord.Guild, channel_id: int, days_back: int, usage_type: str = 'all', role_id: str = None):

        db_cog = await self.get_database_cog()
        if not db_cog:
            return []

        role_filter_ids = None
        if role_id and role_id != "none":
            role = guild.get_role(int(role_id))
            if role:
                role_filter_ids = [role.id]

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days_back)

        emoji_data = await db_cog.q_emoji_channel_leaderboard(
            guild_id=guild.id,
            channel_id=channel_id,
            limit=100,
            role_filter_ids=role_filter_ids,
            start_time=start_time,
            end_time=end_time,
            usage_type=usage_type if usage_type != 'all' else None
        )

        formatted_data = []
        for emoji in emoji_data:
            total_count = emoji['usage_count']

            if usage_type == 'all':
                message_count = total_count // 2
                reaction_count = total_count - message_count
            elif usage_type == 'message':
                message_count = total_count
                reaction_count = 0
            elif usage_type == 'reaction':
                message_count = 0
                reaction_count = total_count
            else:
                message_count = 0
                reaction_count = 0

            formatted_data.append((
                emoji['emoji_str'],
                total_count,
                message_count,
                reaction_count
            ))

        return formatted_data

    async def get_category_emoji_stats(self, guild: discord.Guild, category_id: int, days_back: int, usage_type: str = 'all', role_id: str = None):

        db_cog = await self.get_database_cog()
        if not db_cog:
            return []

        role_filter_ids = None
        if role_id and role_id != "none":
            role = guild.get_role(int(role_id))
            if role:
                role_filter_ids = [role.id]

        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days_back)

        emoji_data = await db_cog.q_emoji_category_leaderboard(
            guild_id=guild.id,
            category_id=category_id,
            limit=100,
            role_filter_ids=role_filter_ids,
            start_time=start_time,
            end_time=end_time,
            usage_type=usage_type if usage_type != 'all' else None
        )

        formatted_data = []
        for emoji in emoji_data:
            total_count = emoji['usage_count']

            if usage_type == 'all':
                message_count = total_count // 2
                reaction_count = total_count - message_count
            elif usage_type == 'message':
                message_count = total_count
                reaction_count = 0
            elif usage_type == 'reaction':
                message_count = 0
                reaction_count = total_count
            else:
                message_count = 0
                reaction_count = 0

            formatted_data.append((
                emoji['emoji_str'],
                total_count,
                message_count,
                reaction_count
            ))

        return formatted_data

    # COMMANDS

    emoji_group = app_commands.Group(
        name="emoji", description="Emoji statistics and leaderboard commands")
    emoji_user_group = app_commands.Group(
        name="user", description="Emoji user statistics commands", parent=emoji_group)
    emoji_channel_group = app_commands.Group(
        name="channel", description="Channel-specific emoji statistics commands", parent=emoji_group)
    emoji_category_group = app_commands.Group(
        name="category", description="Category-specific emoji statistics commands", parent=emoji_group)

    @emoji_group.command(name="leaderboard", description="Show most used emojis in the server")
    async def emoji_leaderboard(self, interaction: discord.Interaction):

        try:
            guild = interaction.guild
            if guild is None:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return

            await interaction.response.defer()

            view = EmojiLeaderboardView(
                self, guild, days_back=EmojiConfig.DEFAULT_DAYS_BACK)

            img_bytes, total_pages = await view.generate_image()
            file = discord.File(img_bytes, filename="emoji_leaderboard.png")

            await interaction.followup.send(file=file, view=view)

            message = await interaction.original_response()
            view.message = message

            await view._update_buttons(total_pages)
            await message.edit(view=view)

        except Exception as e:
            print(f"Error in emoji_leaderboard: {e}")
            await interaction.followup.send("❌ An error occurred while retrieving emoji stats.")

    @emoji_user_group.command(name="leaderboard", description="Show top emoji users in the server")
    async def emoji_user_leaderboard(self, interaction: discord.Interaction):

        try:
            guild = interaction.guild
            if guild is None:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return

            await interaction.response.defer()

            view = UserEmojiLeaderboardView(
                self, guild, days_back=EmojiConfig.DEFAULT_DAYS_BACK)

            img_bytes, total_pages = await view.generate_image()
            file = discord.File(
                img_bytes, filename="user_emoji_leaderboard.png")

            await interaction.followup.send(file=file, view=view)

            message = await interaction.original_response()
            view.message = message

            await view._update_buttons(total_pages)
            await message.edit(view=view)

        except Exception as e:
            print(f"Error in emoji_user_leaderboard: {e}")
            await interaction.followup.send("❌ An error occurred while retrieving user emoji stats.")

    @emoji_channel_group.command(name="leaderboard", description="Show most used emojis in a specific channel")
    @app_commands.describe(channel="Select a text channel")
    async def emoji_channel_leaderboard(self, interaction: discord.Interaction, channel: discord.TextChannel):

        try:
            guild = interaction.guild
            if guild is None:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return

            await interaction.response.defer()

            view = ChannelEmojiLeaderboardView(
                self, guild, channel, days_back=EmojiConfig.DEFAULT_DAYS_BACK)

            img_bytes, total_pages = await view.generate_image()
            file = discord.File(
                img_bytes, filename="channel_emoji_leaderboard.png")

            await interaction.followup.send(file=file, view=view)

            message = await interaction.original_response()
            view.message = message

            await view._update_buttons(total_pages)
            await message.edit(view=view)

        except Exception as e:
            print(f"Error in emoji_channel_leaderboard: {e}")
            await interaction.followup.send("❌ An error occurred while retrieving channel emoji stats.")

    @emoji_category_group.command(name="leaderboard", description="Show most used emojis in a specific category")
    @app_commands.describe(category="Select a category")
    async def emoji_category_leaderboard(self, interaction: discord.Interaction, category: discord.CategoryChannel):

        try:
            guild = interaction.guild
            if guild is None:
                await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
                return

            await interaction.response.defer()

            view = CategoryEmojiLeaderboardView(
                self, guild, category, days_back=EmojiConfig.DEFAULT_DAYS_BACK)

            img_bytes, total_pages = await view.generate_image()
            file = discord.File(
                img_bytes, filename="category_emoji_leaderboard.png")

            await interaction.followup.send(file=file, view=view)

            message = await interaction.original_response()
            view.message = message

            await view._update_buttons(total_pages)
            await message.edit(view=view)

        except Exception as e:
            print(f"Error in emoji_category_leaderboard: {e}")
            await interaction.followup.send("❌ An error occurred while retrieving category emoji stats.")


# SETUP

async def setup(bot):

    await bot.add_cog(EmojiLeaderboard(bot))
