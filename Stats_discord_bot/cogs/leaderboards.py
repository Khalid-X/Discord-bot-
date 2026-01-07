import discord
from discord.ext import commands
from discord import app_commands
from collections import Counter, defaultdict
from datetime import datetime, timedelta
import time
import asyncio
import os
from PIL import Image, ImageDraw, ImageFont
import io
import aiohttp
import random
import traceback
from typing import List, Dict, Any, Optional, Set, Tuple
import logging
import re
from pathlib import Path


# CONFIGURATION

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent


class Config:

    USERS_PER_PAGE = 10
    MAX_ROLES_IN_SELECT = 25

    TEMPLATE_PATH = BASE_DIR / "assets" / "images" / "leaderboards final png.png"
    FONT_PATH = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"

    DEFAULT_DAYS_BACK = 14
    TIME_PERIODS = [7, 14, 30]
    MAX_CUSTOM_DAYS = 2000

    IMAGE_WIDTH = 800
    IMAGE_HEIGHT = 600

    COLORS = {
        'gold': (255, 215, 0, 220),
        'silver': (192, 192, 192, 220),
        'bronze': (205, 127, 50, 220),
        'default': (93, 0, 136, 255),
        'background': (0, 0, 0, 220),
        'text_white': "white",
        'stroke_black': "black"
    }


class ImageGenerator:

    # FONTS

    @staticmethod
    def get_fonts():

        try:
            custom_font_path = Config.FONT_PATH
            if os.path.exists(custom_font_path):
                font_small = ImageFont.truetype(custom_font_path, 16)
                font_medium = ImageFont.truetype(custom_font_path, 20)
                font_large = ImageFont.truetype(custom_font_path, 24)
                font_larger = ImageFont.truetype(custom_font_path, 30)
                font_huge = ImageFont.truetype(custom_font_path, 40)
                font_giant = ImageFont.truetype(custom_font_path, 60)
                return font_small, font_medium, font_large, font_larger, font_huge, font_giant
        except Exception as e:
            logger.warning(f"Failed to load custom font: {e}")

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

    @staticmethod
    def draw_text_with_stroke(draw, position, text, font, fill, stroke_fill, stroke_width):

        x, y = position

        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw.text((x + dx, y + dy), text,
                              font=font, fill=stroke_fill)

        draw.text((x, y), text, font=font, fill=fill)

    @staticmethod
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
                        ImageGenerator.draw_text_with_stroke(
                            draw, (x, y), text, smaller_font, fill, stroke_fill, stroke_width)
                        return
                except:
                    continue

        ImageGenerator.draw_text_with_stroke(
            draw, (x, y), text, font, fill, stroke_fill, stroke_width)

    @staticmethod
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

    @staticmethod
    async def load_icon_from_url(url):

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        icon_data = await response.read()
                        return Image.open(io.BytesIO(icon_data))
        except Exception as e:
            logger.warning(f"Failed to load icon from URL: {e}")
        return None

    @staticmethod
    def format_leaderboard_data(guild, leaderboard_data, leaderboard_type):

        formatted_data = []

        for item_id, value in leaderboard_data:
            name = None
            try:
                if isinstance(item_id, str) and item_id.isdigit():
                    item_id = int(item_id)

                member = guild.get_member(item_id)
                if member:
                    name = member.name
                else:
                    obj = guild.get_channel(item_id)
                    if obj:
                        if isinstance(obj, discord.CategoryChannel):
                            name = obj.name
                        elif isinstance(obj, (discord.TextChannel, discord.VoiceChannel, discord.StageChannel, discord.ForumChannel)):

                            name = f"#{obj.name}"

                if not name:
                    name = f"Unknown ({item_id})"
            except Exception as e:
                logger.warning(f"Error resolving ID {item_id}: {e}")
                name = f"Unknown ({item_id})"

            formatted_data.append((name, value))

        return formatted_data

    @staticmethod
    def truncate_text_for_width(draw, text, font, max_width, ellipsis="..."):

        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]

        if text_width <= max_width:
            return text, font

        try:
            if hasattr(font, 'path'):
                for size in range(font.size - 2, 12, -2):
                    smaller_font = ImageFont.truetype(font.path, size)
                    smaller_bbox = draw.textbbox(
                        (0, 0), text, font=smaller_font)
                    smaller_width = smaller_bbox[2] - smaller_bbox[0]

                    if smaller_width <= max_width:
                        return text, smaller_font
        except:
            pass

        truncated_text = text
        while len(truncated_text) > 3:
            truncated_text = truncated_text[:-4] + ellipsis
            truncated_bbox = draw.textbbox((0, 0), truncated_text, font=font)
            truncated_width = truncated_bbox[2] - truncated_bbox[0]

            if truncated_width <= max_width:
                return truncated_text, font

        return text[:3] + ellipsis, font

    @staticmethod
    async def add_server_profile_pic_and_name(image, guild, target_name, leaderboard_type, font_huge):

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

        # INVISIBLE RECTANGLE

        def fit_text_to_rectangle(text, text_start_x, text_start_y, rect_center_x, rect_center_y, rect_width, rect_height, is_username=False):

            font_paths = [
                BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"]

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

        # User Profile picture
        avatar_x, avatar_y = 7, 10
        avatar_size = (60, 60)

        # Server name
        text_start_x = 85
        text_start_y = 30

        # Rectangle for text constraints
        username_rectangle = {"center": (150, 30), "width": 183, "height": 35}

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
            logger.error(f"Could not add server icon: {e}")

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


# FORMATS

class DataFormatter:

    @staticmethod
    def format_voice_time(total_seconds):

        if total_seconds == 0:
            return "0s"

        days = total_seconds // (3600 * 24)
        hours = (total_seconds % (3600 * 24)) // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        if days > 0:
            if hours > 0:
                return f"{days}d {hours}h {minutes}m"
            else:
                return f"{days}d {minutes}m"
        elif hours > 0:
            if minutes > 0:
                return f"{hours}h {minutes}m"
            else:
                return f"{hours}h {seconds}s"
        elif minutes > 0:
            if seconds > 0:
                return f"{minutes}m {seconds}s"
            else:
                return f"{minutes}m"
        else:
            return f"{seconds}s"

    @staticmethod
    def format_message_count(count):

        return f"{count:,}"

    @staticmethod
    def ensure_int(value):

        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f"Failed to convert to int: {value}")
            return None

    @staticmethod
    def ensure_str(value):

        if value is None:
            return ""
        return str(value)


# IMAGE GENERATION

async def generate_leaderboard_image(
    guild: discord.Guild,
    leaderboard_data: list,
    leaderboard_type: str,
    target_name: str,
    days_back: int,
    role_id: str = None,
    target_member: discord.Member = None,
    total_value: int = None,
    user_count: int = 10,
    page: int = 0,
    is_real_time: bool = False
):

    template_path = Config.TEMPLATE_PATH
    try:
        image = Image.open(template_path)
    except FileNotFoundError:
        image = Image.new('RGB', (Config.IMAGE_WIDTH,
                          Config.IMAGE_HEIGHT), color='#2F3136')

    if image.mode != "RGB":
        image = image.convert("RGB")

    draw = ImageDraw.Draw(image)
    font_small, font_medium, font_large, font_larger, font_huge, font_giant = ImageGenerator.get_fonts()

    await ImageGenerator.add_server_profile_pic_and_name(image, guild, target_name, leaderboard_type, font_huge)

    # ROLE FILTER / DATE / TIME RANGE
    role_text = "No Filter"
    if role_id and role_id != "none":
        role = guild.get_role(int(role_id))
        role_text = role.name if role else "Unknown Role"
    ImageGenerator.draw_text_with_stroke(
        draw, (70, 424), role_text, font_small, "white", "black", 1)

    # Time display
    time_text = f"{days_back} days"
    ImageGenerator.draw_text_with_stroke(
        draw, (630, 422), time_text, font_small, "white", "black", 1)
    ImageGenerator.draw_text_with_stroke(draw, (550, 38), datetime.now().strftime(
        "%B %d, %Y"), font_small, "white", "black", 1)

    # PAGINATION
    if leaderboard_data:
        total_users = len(leaderboard_data)
        users_per_page = Config.USERS_PER_PAGE
        total_pages = (total_users + users_per_page - 1) // users_per_page
        ImageGenerator.draw_text_with_stroke(
            draw, (400, 450), f"Page {page + 1}/{total_pages}", font_medium, "white", "black", 1)
    else:
        total_pages = 1

    # LEADERBOARD CONTENT
    if leaderboard_data:
        display_data = ImageGenerator.format_leaderboard_data(
            guild, leaderboard_data, leaderboard_type)

        start_idx = page * Config.USERS_PER_PAGE
        end_idx = min(start_idx + Config.USERS_PER_PAGE, len(display_data))
        page_data = display_data[start_idx:end_idx]

        if len(page_data) <= 4:
            positions = [(60, 100 + i * 65) for i in range(len(page_data))]
            box_width, box_height = 600, 40
        else:
            box_width, box_height = 280, 40
            box_spacing = 65
            start_y = 100
            total_box_width = (2 * box_width) + 15
            start_x = (Config.IMAGE_WIDTH - total_box_width) // 2
            positions = [
                (start_x + (i % 2) * (box_width + 15),
                 start_y + (i // 2) * box_spacing)
                for i in range(len(page_data))
            ]

        for i, ((name, value), (box_x, box_y)) in enumerate(zip(page_data, positions)):
            global_rank = start_idx + i + 1

            ImageGenerator.draw_rounded_rectangle(draw, [box_x, box_y, box_x + box_width, box_y + box_height],
                                                  radius=8, fill=Config.COLORS['background'], outline=None)

            placement_width = 35
            if global_rank == 1:
                placement_color = Config.COLORS['gold']
            elif global_rank == 2:
                placement_color = Config.COLORS['silver']
            elif global_rank == 3:
                placement_color = Config.COLORS['bronze']
            else:
                placement_color = Config.COLORS['default']

            ImageGenerator.draw_rounded_rectangle(draw, [box_x, box_y, box_x + placement_width, box_y + box_height],
                                                  radius=8, fill=placement_color, outline=None)

            is_voice_type = leaderboard_type in [
                'voice', 'server_voice', 'category_voice', 'top_users_voice',
                'top_voice_channels', 'top_voice_categories'
            ] or 'voice' in leaderboard_type

            if is_voice_type:
                value_text = DataFormatter.format_voice_time(value)
            else:
                value_text = DataFormatter.format_message_count(value)

                if total_value and total_value > 0 and isinstance(total_value, (int, float)):
                    perc = (value / total_value) * 100
                    value_text += f" ({perc:.1f}%)"

            rank_text = f"#{global_rank}"
            rank_bbox = draw.textbbox((0, 0), rank_text, font=font_medium)
            rank_width = rank_bbox[2] - rank_bbox[0]

            value_bbox = draw.textbbox((0, 0), value_text, font=font_medium)
            value_width = value_bbox[2] - value_bbox[0]

            available_name_width = box_width - \
                placement_width - value_width - 20

            display_name = name
            name_font = font_medium

            display_name, name_font = ImageGenerator.truncate_text_for_width(
                draw, display_name, name_font, available_name_width
            )

            rank_height = rank_bbox[3] - rank_bbox[1]
            name_bbox = draw.textbbox((0, 0), display_name, font=name_font)
            name_height = name_bbox[3] - name_bbox[1]
            value_height = value_bbox[3] - value_bbox[0]

            rank_y = box_y + (box_height - rank_height) // 2
            name_y = box_y + (box_height - name_height) // 2
            value_y = box_y + (box_height - value_height) // 2

            rank_x = box_x + (placement_width - rank_width) // 2
            ImageGenerator.draw_text_with_stroke(
                draw, (rank_x, rank_y), rank_text, font_medium, "white", "black", 1)

            name_x = box_x + placement_width + 8
            ImageGenerator.draw_text_with_stroke(
                draw, (name_x, name_y), display_name, name_font, "white", "black", 1)

            value_x = box_x + box_width - value_width - 8
            ImageGenerator.draw_text_with_stroke(
                draw, (value_x, value_y), value_text, font_medium, "white", "black", 1)
    else:
        cx, cy = image.size[0] // 2, image.size[1] // 2
        ImageGenerator.draw_text_centered_with_stroke(
            draw, "NO DATA AVAILABLE", (cx, cy), font_giant, "white", "black", 3)

    img_bytes = io.BytesIO()
    image.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    return img_bytes


# CHANNEL LEADERBOARD IMAGE GENERATION

async def generate_voice_leaderboard_image(guild: discord.Guild, channel: discord.VoiceChannel,
                                           leaderboard_data: list, days_back: int, role_id: str = None,
                                           user_count: int = 10, page: int = 0, is_real_time: bool = False):

    return await generate_leaderboard_image(
        guild=guild,
        leaderboard_data=leaderboard_data,
        leaderboard_type='voice',
        target_name=channel.name,
        days_back=days_back,
        role_id=role_id,
        user_count=user_count,
        page=page,
        is_real_time=is_real_time
    )


async def generate_text_leaderboard_image(guild: discord.Guild, channel: discord.TextChannel,
                                          leaderboard_data: list, days_back: int, role_id: str = None,
                                          total_messages: int = None, user_count: int = 10, page: int = 0, is_real_time: bool = False):

    return await generate_leaderboard_image(
        guild=guild,
        leaderboard_data=leaderboard_data,
        leaderboard_type='text',
        target_name=channel.name,
        days_back=days_back,
        role_id=role_id,
        total_value=total_messages,
        user_count=user_count,
        page=page,
        is_real_time=is_real_time
    )

# SERVER LEADERBOARD IMAGE GENERATION


async def generate_server_leaderboard_image(guild: discord.Guild, leaderboard_data: list,
                                            leaderboard_type: str, days_back: int, role_id: str = None,
                                            total_value: int = None, user_count: int = 10, page: int = 0, is_real_time: bool = False):

    return await generate_leaderboard_image(
        guild=guild,
        leaderboard_data=leaderboard_data,
        leaderboard_type=leaderboard_type,
        target_name=guild.name,
        days_back=days_back,
        role_id=role_id,
        total_value=total_value,
        user_count=user_count,
        page=page,
        is_real_time=is_real_time
    )


# CATEGORY LEADERBOARD IMAGE GENERATION

async def generate_category_leaderboard_image(guild: discord.Guild, category: discord.CategoryChannel,
                                              leaderboard_data: list, leaderboard_type: str,
                                              days_back: int, role_id: str = None, total_value: int = None,
                                              user_count: int = 10, page: int = 0, is_real_time: bool = False):

    return await generate_leaderboard_image(
        guild=guild,
        leaderboard_data=leaderboard_data,
        leaderboard_type=f'category_{leaderboard_type}',
        target_name=guild.name,
        days_back=days_back,
        role_id=role_id,
        total_value=total_value,
        user_count=user_count,
        page=page,
        is_real_time=is_real_time
    )


# MENTIONS LEADERBOARD IMAGE GENERATION

async def generate_mentions_leaderboard_image(guild: discord.Guild, target_member: discord.Member,
                                              leaderboard_data: list, days_back: int, role_id: str = None,
                                              user_count: int = 10, page: int = 0, is_real_time: bool = False):

    return await generate_leaderboard_image(
        guild=guild,
        leaderboard_data=leaderboard_data,
        leaderboard_type='mentions',
        target_name=target_member.name,
        days_back=days_back,
        role_id=role_id,
        target_member=target_member,
        user_count=user_count,
        page=page,
        is_real_time=is_real_time
    )


# TIME MODALS

class VoiceLeaderboardTimeModal(discord.ui.Modal, title='Custom Time Period'):
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
            if days <= 0 or days > Config.MAX_CUSTOM_DAYS:
                await interaction.response.send_message(f"❌ Please enter a number between 1 and {Config.MAX_CUSTOM_DAYS} days.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=False, thinking=True)

            view = self.cog.active_views.get(self.original_message_id)
            if view:

                view.current_days = days
                view.show_time_buttons = False
                view.page = 0

                await view.update_message(interaction)
            else:
                await interaction.followup.send("❌ Could not find the leaderboard view. Please try the command again.", ephemeral=True)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error in voice modal submit: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)


class TextLeaderboardTimeModal(discord.ui.Modal, title='Custom Time Period'):
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
            if days <= 0 or days > Config.MAX_CUSTOM_DAYS:
                await interaction.response.send_message(f"❌ Please enter a number between 1 and {Config.MAX_CUSTOM_DAYS} days.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=False, thinking=True)

            view = self.cog.active_views.get(self.original_message_id)
            if view:

                view.current_days = days
                view.show_time_buttons = False
                view.page = 0

                await view.update_message(interaction)
            else:
                await interaction.followup.send("❌ Could not find the leaderboard view. Please try the command again.", ephemeral=True)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error in text modal submit: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)


class ServerLeaderboardTimeModal(discord.ui.Modal, title='Custom Time Period'):
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
            if days <= 0 or days > Config.MAX_CUSTOM_DAYS:
                await interaction.response.send_message(f"❌ Please enter a number between 1 and {Config.MAX_CUSTOM_DAYS} days.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=False, thinking=True)

            view = self.cog.active_views.get(self.original_message_id)
            if view:

                view.current_days = days
                view.show_time_buttons = False
                view.page = 0

                await view.update_message(interaction)
            else:
                await interaction.followup.send("❌ Could not find the leaderboard view. Please try the command again.", ephemeral=True)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error in server modal submit: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)


class CategoryLeaderboardTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, cog_instance, original_message_id, guild, category):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.original_message_id = original_message_id
        self.guild = guild
        self.category = category

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
            if days <= 0 or days > Config.MAX_CUSTOM_DAYS:
                await interaction.response.send_message(f"❌ Please enter a number between 1 and {Config.MAX_CUSTOM_DAYS} days.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=False, thinking=True)

            view = self.cog.active_views.get(self.original_message_id)
            if view:

                view.current_days = days
                view.show_time_buttons = False
                view.page = 0

                await view.update_message(interaction)
            else:
                await interaction.followup.send("❌ Could not find the leaderboard view. Please try the command again.", ephemeral=True)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error in category modal submit: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)


class MentionsLeaderboardTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, cog_instance, original_message_id, guild, target_user):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.original_message_id = original_message_id
        self.guild = guild
        self.target_user = target_user

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
            if days <= 0 or days > Config.MAX_CUSTOM_DAYS:
                await interaction.response.send_message(f"❌ Please enter a number between 1 and {Config.MAX_CUSTOM_DAYS} days.", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=False, thinking=True)

            view = self.cog.active_views.get(self.original_message_id)
            if view:

                view.current_days = days
                view.show_time_buttons = False
                view.page = 0

                await view.update_message(interaction)
            else:
                await interaction.followup.send("❌ Could not find the leaderboard view. Please try the command again.", ephemeral=True)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error in mentions modal submit: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)


# ROLE DROPDOWN MENU

class RoleSelectMenu(discord.ui.Select):
    def __init__(self, guild: discord.Guild, current_role_id: str = None):
        self.guild = guild

        roles = [role for role in guild.roles if role.name != "@everyone"]
        roles.sort(key=lambda x: x.position, reverse=True)
        roles = roles[:Config.MAX_ROLES_IN_SELECT]

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
            custom_id="role_filter_select",
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
                         label="⬅️", custom_id="leaderboard_prev")

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
                         label="➡️", custom_id="leaderboard_next")

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
                         custom_id="leaderboard_page", disabled=True)


# VOICE LEADERBOARD VIEW

class VoiceLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, channel: discord.VoiceChannel,
                 days_back: int, page: int = 0, selected_role_id: str = None):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.channel = channel
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
            custom_id="voice_refresh",
            row=2
        )
        refresh_button.callback = self.refresh_callback

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="voice_time_settings",
            row=2
        )
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            row = 3
            for days in Config.TIME_PERIODS:
                days_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == days else discord.ButtonStyle.secondary,
                    label=f"{days} Days",
                    custom_id=f"voice_days_{days}",
                    row=row
                )
                days_button.callback = self.create_days_callback(days)
                self.add_item(days_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in Config.TIME_PERIODS else "Custom",
                custom_id="voice_custom_days",
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

    async def handle_time_settings(self, interaction: discord.Interaction):

        if interaction.response.is_done():
            self.show_time_buttons = not self.show_time_buttons
            self._update_buttons()
            await interaction.edit_original_response(view=self)
        else:
            await interaction.response.defer(ephemeral=False, thinking=False)
            self.show_time_buttons = not self.show_time_buttons
            self._update_buttons()
            await interaction.edit_original_response(view=self)

    async def handle_time_period_change(self, interaction: discord.Interaction, days: int):

        if interaction.response.is_done():
            self.current_days = days
            self.show_time_buttons = False
            self.page = 0

            await self.update_message(interaction)
        else:
            await interaction.response.defer(ephemeral=False, thinking=False)
            self.current_days = days
            self.show_time_buttons = False
            self.page = 0

            await self.update_message(interaction)

    async def handle_refresh(self, interaction: discord.Interaction):

        if interaction.response.is_done():

            await self.update_message(interaction)
        else:
            await interaction.response.defer(ephemeral=False, thinking=False)

            await self.update_message(interaction)

    async def handle_custom_days(self, interaction: discord.Interaction):

        modal = VoiceLeaderboardTimeModal(
            self.cog, interaction.message.id, self.guild, self.channel)
        await interaction.response.send_modal(modal)

    async def generate_image(self):

        leaderboard_data = await self.cog.get_channel_voice_leaderboard(
            self.guild, self.channel, self.current_days, self.selected_role_id
        )

        return await generate_voice_leaderboard_image(
            guild=self.guild,
            channel=self.channel,
            leaderboard_data=leaderboard_data,
            days_back=self.current_days,
            role_id=self.selected_role_id,
            page=self.page
        )

    async def update_message(self, interaction: discord.Interaction):

        try:
            leaderboard_data = await self.cog.get_channel_voice_leaderboard(
                self.guild, self.channel, self.current_days, self.selected_role_id
            )
            total_users = len(leaderboard_data)
            total_pages = max(
                1, (total_users + Config.USERS_PER_PAGE - 1) // Config.USERS_PER_PAGE)

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

            file = discord.File(img_bytes, filename="voice_leaderboard.png")

            if interaction.response.is_done():
                await interaction.edit_original_response(attachments=[file], view=self)
            else:
                await interaction.response.edit_message(attachments=[file], view=self)

        except Exception as e:
            logger.error(f"Error updating voice leaderboard message: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ An error occurred while updating the leaderboard.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# TEXT LEADERBOARD VIEW

class TextLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, channel: discord.TextChannel,
                 days_back: int, page: int = 0, selected_role_id: str = None):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.channel = channel
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
            custom_id="text_refresh",
            row=2
        )
        refresh_button.callback = self.refresh_callback

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="text_time_settings",
            row=2
        )
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            row = 3
            for days in Config.TIME_PERIODS:
                days_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == days else discord.ButtonStyle.secondary,
                    label=f"{days} Days",
                    custom_id=f"text_days_{days}",
                    row=row
                )
                days_button.callback = self.create_days_callback(days)
                self.add_item(days_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in Config.TIME_PERIODS else "Custom",
                custom_id="text_custom_days",
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

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        await self.update_message(interaction)

    async def handle_time_settings(self, interaction: discord.Interaction):

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.show_time_buttons = not self.show_time_buttons
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def handle_time_period_change(self, interaction: discord.Interaction, days: int):

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.current_days = days
        self.show_time_buttons = False
        self.page = 0

        await self.update_message(interaction)

    async def handle_custom_days(self, interaction: discord.Interaction):

        modal = TextLeaderboardTimeModal(
            self.cog, interaction.message.id, self.guild, self.channel)
        await interaction.response.send_modal(modal)

    async def generate_image(self):

        leaderboard_data = await self.cog.get_channel_text_leaderboard(
            self.guild, self.channel, self.current_days, self.selected_role_id
        )

        total_messages = sum(count for _, count in leaderboard_data)

        return await generate_text_leaderboard_image(
            guild=self.guild,
            channel=self.channel,
            leaderboard_data=leaderboard_data,
            days_back=self.current_days,
            role_id=self.selected_role_id,
            total_messages=total_messages,
            page=self.page
        )

    async def update_message(self, interaction: discord.Interaction):

        try:

            leaderboard_data = await self.cog.get_channel_text_leaderboard(
                self.guild, self.channel, self.current_days, self.selected_role_id
            )
            total_users = len(leaderboard_data)
            total_pages = max(
                1, (total_users + Config.USERS_PER_PAGE - 1) // Config.USERS_PER_PAGE)

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

            file = discord.File(img_bytes, filename="text_leaderboard.png")

            if interaction.response.is_done():
                await interaction.edit_original_response(attachments=[file], view=self)
            else:
                await interaction.response.edit_message(attachments=[file], view=self)

        except Exception as e:
            logger.error(f"Error updating text leaderboard message: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ An error occurred while updating the leaderboard.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# SERVER LEADERBOARD TYPE DROPDOWN MENU

class LeaderboardTypeSelect(discord.ui.Select):
    def __init__(self, current_type: str):
        options = [
            discord.SelectOption(label="Top Message Users", value="top_users_messages",
                                 description="Users with most messages", emoji="💬"),
            discord.SelectOption(label="Top Voice Users", value="top_users_voice",
                                 description="Users with most voice time", emoji="🎤"),
            discord.SelectOption(label="Top Text Channels", value="top_text_channels",
                                 description="Most active text channels", emoji="📝"),
            discord.SelectOption(label="Top Voice Channels", value="top_voice_channels",
                                 description="Most active voice channels", emoji="🔊"),
        ]

        for option in options:
            option.default = (option.value == current_type)

        super().__init__(placeholder="Select leaderboard type...",
                         options=options, custom_id="server_leaderboard_type")

    async def callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view: ServerLeaderboardView = self.view
        view.leaderboard_type = self.values[0]
        view.page = 0
        await view.update_message(interaction)


# SERVER LEADERBOARD VIEW

class ServerLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, days_back: int,
                 leaderboard_type: str = "top_users_messages", page: int = 0,
                 selected_role_id: str = None):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.current_days = days_back
        self.leaderboard_type = leaderboard_type
        self.page = page
        self.selected_role_id = selected_role_id
        self.show_time_buttons = False
        self._update_buttons()

    def _update_buttons(self):
        self.clear_items()
        self.add_item(RoleSelectMenu(self.guild, self.selected_role_id))
        self.add_item(LeaderboardTypeSelect(self.leaderboard_type))

        self.add_item(PrevButton())
        self.add_item(PageIndicator())
        self.add_item(NextButton())

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="server_refresh",
            row=2
        )
        refresh_button.callback = self.refresh_callback

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="server_time_settings",
            row=2
        )
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            row = 3
            for days in Config.TIME_PERIODS:
                days_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == days else discord.ButtonStyle.secondary,
                    label=f"{days} Days",
                    custom_id=f"server_days_{days}",
                    row=row
                )
                days_button.callback = self.create_days_callback(days)
                self.add_item(days_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in Config.TIME_PERIODS else "Custom",
                custom_id="server_custom_days",
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

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        await self.update_message(interaction)

    async def handle_time_settings(self, interaction: discord.Interaction):

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.show_time_buttons = not self.show_time_buttons
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def handle_time_period_change(self, interaction: discord.Interaction, days: int):

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.current_days = days
        self.show_time_buttons = False
        self.page = 0

        await self.update_message(interaction)

    async def handle_custom_days(self, interaction: discord.Interaction):

        modal = ServerLeaderboardTimeModal(
            self.cog, interaction.message.id, self.guild)
        await interaction.response.send_modal(modal)

    async def generate_image(self):

        if self.leaderboard_type in ["top_users_voice", "top_voice_channels", "top_voice_categories"]:
            data = await self.cog.get_server_voice_data(self.guild, self.current_days, self.selected_role_id)
        else:
            data = await self.cog.get_server_message_data(self.guild, self.current_days, self.selected_role_id)

        leaderboard_data = []
        total_value = 0

        if self.leaderboard_type == "top_users_messages":
            leaderboard_data = data.get("users", [])
            total_value = sum(
                count for _, count in leaderboard_data) if leaderboard_data else 0
        elif self.leaderboard_type == "top_users_voice":
            leaderboard_data = data.get("users", [])
            total_value = sum(
                seconds for _, seconds in leaderboard_data) if leaderboard_data else 0
        elif self.leaderboard_type == "top_text_channels":
            leaderboard_data = data.get("channels", [])
            total_value = sum(
                count for _, count in leaderboard_data) if leaderboard_data else 0
        elif self.leaderboard_type == "top_voice_channels":
            leaderboard_data = data.get("channels", [])
            total_value = sum(
                seconds for _, seconds in leaderboard_data) if leaderboard_data else 0
        elif self.leaderboard_type == "top_message_categories":
            leaderboard_data = data.get("message_categories", [])
            total_value = sum(
                count for _, count in leaderboard_data) if leaderboard_data else 0
        elif self.leaderboard_type == "top_voice_categories":
            leaderboard_data = data.get("voice_categories", [])
            total_value = sum(
                seconds for _, seconds in leaderboard_data) if leaderboard_data else 0

        return await generate_server_leaderboard_image(
            guild=self.guild,
            leaderboard_data=leaderboard_data,
            leaderboard_type=self.leaderboard_type,
            days_back=self.current_days,
            role_id=self.selected_role_id,
            total_value=total_value,
            page=self.page
        )

    async def update_message(self, interaction: discord.Interaction):

        try:
            if self.leaderboard_type in ["top_users_voice", "top_voice_channels", "top_voice_categories"]:
                data = await self.cog.get_server_voice_data(self.guild, self.current_days, self.selected_role_id)
            else:
                data = await self.cog.get_server_message_data(self.guild, self.current_days, self.selected_role_id)

            if self.leaderboard_type == "top_users_messages":
                leaderboard_data = data.get("users", [])
            elif self.leaderboard_type == "top_users_voice":
                leaderboard_data = data.get("users", [])
            elif self.leaderboard_type == "top_text_channels":
                leaderboard_data = data.get("channels", [])
            elif self.leaderboard_type == "top_voice_channels":
                leaderboard_data = data.get("channels", [])
            elif self.leaderboard_type == "top_message_categories":
                leaderboard_data = data.get("message_categories", [])
            elif self.leaderboard_type == "top_voice_categories":
                leaderboard_data = data.get("voice_categories", [])
            else:
                leaderboard_data = []

            total_items = len(leaderboard_data)
            total_pages = max(
                1, (total_items + Config.USERS_PER_PAGE - 1) // Config.USERS_PER_PAGE)

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

            file = discord.File(img_bytes, filename="server_leaderboard.png")

            if interaction.response.is_done():
                await interaction.edit_original_response(attachments=[file], view=self)
            else:
                await interaction.response.edit_message(attachments=[file], view=self)

        except Exception as e:
            logger.error(f"Error updating server leaderboard message: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ An error occurred while updating the leaderboard.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# CATEGORY LEADERBOARD TYPE DROPDOWN MENU

class CategoryLeaderboardTypeSelect(discord.ui.Select):
    def __init__(self, current_type: str):
        options = [
            discord.SelectOption(label="Top Text Channels", value="top_text_channels",
                                 description="Most active text channels", emoji="📝"),
            discord.SelectOption(label="Top Voice Channels", value="top_voice_channels",
                                 description="Most active voice channels", emoji="🔊"),
            discord.SelectOption(label="Top Message Users", value="top_users_messages",
                                 description="Users with most messages", emoji="💬"),
            discord.SelectOption(label="Top Voice Users", value="top_users_voice",
                                 description="Users with most voice time", emoji="🎤"),
        ]

        for option in options:
            option.default = (option.value == current_type)

        super().__init__(placeholder="Select leaderboard type...",
                         options=options, custom_id="category_leaderboard_type")

    async def callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view: CategoryLeaderboardView = self.view
        view.leaderboard_type = self.values[0]
        view.page = 0
        await view.update_message(interaction)


# CATEGORY LEADERBOARD VIEW

class CategoryLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, category: discord.CategoryChannel,
                 days_back: int, leaderboard_type: str = "top_text_channels", page: int = 0,
                 selected_role_id: str = None):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.category = category
        self.current_days = days_back
        self.leaderboard_type = leaderboard_type
        self.page = page
        self.selected_role_id = selected_role_id
        self.show_time_buttons = False
        self._update_buttons()

    def _update_buttons(self):
        self.clear_items()
        self.add_item(RoleSelectMenu(self.guild, self.selected_role_id))
        self.add_item(CategoryLeaderboardTypeSelect(self.leaderboard_type))

        self.add_item(PrevButton())
        self.add_item(PageIndicator())
        self.add_item(NextButton())

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="category_refresh",
            row=2
        )
        refresh_button.callback = self.refresh_callback

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="category_time_settings",
            row=2
        )
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            row = 3
            for days in Config.TIME_PERIODS:
                days_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == days else discord.ButtonStyle.secondary,
                    label=f"{days} Days",
                    custom_id=f"category_days_{days}",
                    row=row
                )
                days_button.callback = self.create_days_callback(days)
                self.add_item(days_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in Config.TIME_PERIODS else "Custom",
                custom_id="category_custom_days",
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

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        await self.update_message(interaction)

    async def handle_time_settings(self, interaction: discord.Interaction):

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.show_time_buttons = not self.show_time_buttons
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def handle_time_period_change(self, interaction: discord.Interaction, days: int):

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.current_days = days
        self.show_time_buttons = False
        self.page = 0

        await self.update_message(interaction)

    async def handle_custom_days(self, interaction: discord.Interaction):

        modal = CategoryLeaderboardTimeModal(
            self.cog, interaction.message.id, self.guild, self.category)
        await interaction.response.send_modal(modal)

    async def generate_image(self):

        data = await self.cog.get_category_data(self.guild, self.category, self.current_days,
                                                self.leaderboard_type, self.selected_role_id)

        if self.leaderboard_type == "top_users_messages":
            leaderboard_data = data["users"]
            total_value = sum(
                count for _, count in leaderboard_data) if leaderboard_data else 0
        elif self.leaderboard_type == "top_users_voice":
            leaderboard_data = data["users"]
            total_value = sum(
                seconds for _, seconds in leaderboard_data) if leaderboard_data else 0
        elif self.leaderboard_type == "top_text_channels":
            leaderboard_data = data["channels"]
            total_value = sum(
                count for _, count in leaderboard_data) if leaderboard_data else 0
        elif self.leaderboard_type == "top_voice_channels":
            leaderboard_data = data["channels"]
            total_value = sum(
                seconds for _, seconds in leaderboard_data) if leaderboard_data else 0
        else:
            leaderboard_data = []
            total_value = 0

        return await generate_category_leaderboard_image(
            guild=self.guild,
            category=self.category,
            leaderboard_data=leaderboard_data,
            leaderboard_type=self.leaderboard_type,
            days_back=self.current_days,
            role_id=self.selected_role_id,
            total_value=total_value,
            page=self.page
        )

    async def update_message(self, interaction: discord.Interaction):

        try:
            data = await self.cog.get_category_data(self.guild, self.category, self.current_days,
                                                    self.leaderboard_type, self.selected_role_id)

            if self.leaderboard_type in ["top_users_messages", "top_users_voice"]:
                leaderboard_data = data.get("users", [])
            else:
                leaderboard_data = data.get("channels", [])

            total_items = len(leaderboard_data)
            total_pages = max(
                1, (total_items + Config.USERS_PER_PAGE - 1) // Config.USERS_PER_PAGE)

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

            file = discord.File(img_bytes, filename="category_leaderboard.png")

            if interaction.response.is_done():
                await interaction.edit_original_response(attachments=[file], view=self)
            else:
                await interaction.response.edit_message(attachments=[file], view=self)

        except Exception as e:
            logger.error(f"Error updating category leaderboard message: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ An error occurred while updating the leaderboard.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# MENTIONS LEADERBOARD VIEW

class MentionsLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, target_user: discord.Member,
                 days_back: int, page: int = 0, selected_role_id: str = None):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.target_user = target_user
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
            custom_id="mentions_refresh",
            row=2
        )
        refresh_button.callback = self.refresh_callback

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="mentions_time_settings",
            row=2
        )
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            row = 3
            for days in Config.TIME_PERIODS:
                days_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == days else discord.ButtonStyle.secondary,
                    label=f"{days} Days",
                    custom_id=f"mentions_days_{days}",
                    row=row
                )
                days_button.callback = self.create_days_callback(days)
                self.add_item(days_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in Config.TIME_PERIODS else "Custom",
                custom_id="mentions_custom_days",
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

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        await self.update_message(interaction)

    async def handle_time_settings(self, interaction: discord.Interaction):

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.show_time_buttons = not self.show_time_buttons
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def handle_time_period_change(self, interaction: discord.Interaction, days: int):

        if interaction.response.is_done():
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        self.current_days = days
        self.show_time_buttons = False
        self.page = 0

        await self.update_message(interaction)

    async def handle_custom_days(self, interaction: discord.Interaction):

        modal = MentionsLeaderboardTimeModal(
            self.cog, interaction.message.id, self.guild, self.target_user)
        await interaction.response.send_modal(modal)

    async def generate_image(self):

        leaderboard_data = await self.cog.get_mentions_leaderboard(self.guild, self.target_user,
                                                                   self.current_days, self.selected_role_id)

        return await generate_mentions_leaderboard_image(
            guild=self.guild,
            target_member=self.target_user,
            leaderboard_data=leaderboard_data,
            days_back=self.current_days,
            role_id=self.selected_role_id,
            page=self.page
        )

    async def update_message(self, interaction: discord.Interaction):

        try:
            leaderboard_data = await self.cog.get_mentions_leaderboard(self.guild, self.target_user,
                                                                       self.current_days, self.selected_role_id)
            total_users = len(leaderboard_data)
            total_pages = max(
                1, (total_users + Config.USERS_PER_PAGE - 1) // Config.USERS_PER_PAGE)

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

            file = discord.File(img_bytes, filename="mentions_leaderboard.png")

            if interaction.response.is_done():
                await interaction.edit_original_response(attachments=[file], view=self)
            else:
                await interaction.response.edit_message(attachments=[file], view=self)

        except Exception as e:
            logger.error(f"Error updating mentions leaderboard message: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ An error occurred while updating the leaderboard.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# INITIALIZATION

class Leaderboards(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_cog = None

        self.active_views = {}

        self.users_per_page = Config.USERS_PER_PAGE

        logger.info("Leaderboards cog initialized")

    async def cog_load(self):

        await self._wait_for_db_cog()

    async def _wait_for_db_cog(self):

        for _ in range(30):
            self.db_cog = self.bot.get_cog('DatabaseStats')
            if self.db_cog:
                logger.info("✅ Leaderboards: DatabaseStats cog found!")
                break
            await asyncio.sleep(1)

        if not self.db_cog:
            logger.warning(
                "⚠️ Leaderboards: DatabaseStats cog not found after waiting!")

    async def _filter_members_by_role(self, guild: discord.Guild, role_id: str) -> List[int]:

        if not guild or not role_id or role_id == "none":
            return []

        try:
            role_id_int = int(role_id)
            role = guild.get_role(role_id_int)
            if not role:
                return []

            member_ids = []
            for member in guild.members:
                if role in member.roles:
                    member_ids.append(member.id)

            return member_ids
        except Exception as e:
            logger.error(f"Error filtering members by role: {e}")
            return []

    # QUERY FUNCTIONS

    async def get_channel_voice_leaderboard(self, guild: discord.Guild, channel: discord.VoiceChannel,
                                            days_back: int, role_id: str = None):

        if not self.db_cog:
            logger.warning("DatabaseStats cog not available")
            return []

        try:
            guild_id_int = DataFormatter.ensure_int(guild.id)
            if guild_id_int is None:
                logger.error(f"Invalid guild ID: {guild.id}")
                return []

            role_filter_ids = None
            if role_id and role_id != "none":
                role_id_int = DataFormatter.ensure_int(role_id)
                if role_id_int:
                    role_filter_ids = [role_id_int]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            top_users = await self.db_cog.q_leaderboard_voice_users_in_channel(
                guild_id=guild_id_int,
                channel_id=channel.id,
                limit=100,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            user_stats = []
            for user_data in top_users:
                user_id = user_data.get('user_id')
                total_seconds = user_data.get('total_seconds', 0)

                if user_id and total_seconds > 0:
                    user_stats.append((str(user_id), total_seconds))

            return user_stats

        except Exception as e:
            logger.error(f"Error getting voice leaderboard: {e}")
            traceback.print_exc()
            return []

    async def get_channel_text_leaderboard(self, guild: discord.Guild, channel: discord.TextChannel,
                                           days_back: int, role_id: str = None):

        if not self.db_cog:
            logger.warning("DatabaseStats cog not available")
            return []

        try:
            guild_id_int = DataFormatter.ensure_int(guild.id)
            if guild_id_int is None:
                logger.error(f"Invalid guild ID: {guild.id}")
                return []

            role_filter_ids = None
            if role_id and role_id != "none":
                role_id_int = DataFormatter.ensure_int(role_id)
                if role_id_int:
                    role_filter_ids = [role_id_int]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            top_users = await self.db_cog.q_leaderboard_text_users_in_channel(
                guild_id=guild_id_int,
                channel_id=channel.id,
                limit=100,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            user_stats = []
            for user_data in top_users:
                user_id = user_data.get('user_id')
                message_count = user_data.get('message_count', 0)

                if user_id and message_count > 0:
                    user_stats.append((str(user_id), message_count))

            return user_stats

        except Exception as e:
            logger.error(f"Error getting text leaderboard: {e}")
            traceback.print_exc()
            return []

    async def get_server_message_data(self, guild: discord.Guild, days_back: int,
                                      role_id: str = None):

        if not self.db_cog:
            logger.warning("DatabaseStats cog not available")
            return {"users": [], "channels": [], "message_categories": [], "voice_categories": []}

        result = {
            "users": [],
            "channels": [],
            "message_categories": [],
            "voice_categories": []
        }

        try:
            guild_id_int = DataFormatter.ensure_int(guild.id)
            if guild_id_int is None:
                logger.error(f"Invalid guild ID: {guild.id}")
                return result

            role_filter_ids = None
            if role_id and role_id != "none":
                role_id_int = DataFormatter.ensure_int(role_id)
                if role_id_int:
                    role_filter_ids = [role_id_int]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            top_users = await self.db_cog.q_server_top3_users_messages(
                guild_id=guild_id_int,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            for user_data in top_users:
                user_id = user_data.get('user_id')
                message_count = user_data.get('message_count', 0)

                if user_id and message_count > 0:
                    result["users"].append((str(user_id), message_count))

            top_channels = await self.db_cog.q_leaderboard_server_top_text_channels(
                guild_id=guild_id_int,
                limit=100,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            for channel_data in top_channels:
                channel_id = channel_data.get('channel_id')
                message_count = channel_data.get('message_count', 0)

                if channel_id and message_count > 0:
                    result["channels"].append(
                        (str(channel_id), message_count))

            top_categories = await self.db_cog.q_leaderboard_server_top_categories_messages(
                guild_id=guild_id_int,
                limit=100,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            for category_data in top_categories:
                category_id = category_data.get('category_id')
                message_count = category_data.get('message_count', 0)

                if category_id and message_count > 0:
                    result["message_categories"].append(
                        (str(category_id), message_count))

        except Exception as e:
            logger.error(f"Error getting server message data: {e}")
            traceback.print_exc()

        return result

    async def get_server_voice_data(self, guild: discord.Guild, days_back: int,
                                    role_id: str = None):

        if not self.db_cog:
            logger.warning("DatabaseStats cog not available")
            return {"users": [], "channels": [], "message_categories": [], "voice_categories": []}

        result = {
            "users": [],
            "channels": [],
            "message_categories": [],
            "voice_categories": []
        }

        try:
            guild_id_int = DataFormatter.ensure_int(guild.id)
            if guild_id_int is None:
                logger.error(f"Invalid guild ID: {guild.id}")
                return result

            role_filter_ids = None
            if role_id and role_id != "none":
                role_id_int = DataFormatter.ensure_int(role_id)
                if role_id_int:
                    role_filter_ids = [role_id_int]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            top_users = await self.db_cog.q_server_top3_users_voice(
                guild_id=guild_id_int,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            for user_data in top_users:
                user_id = user_data.get('user_id')
                total_seconds = user_data.get('total_seconds', 0)

                if user_id and total_seconds > 0:
                    result["users"].append((str(user_id), total_seconds))

            top_channels = await self.db_cog.q_leaderboard_server_top_voice_channels(
                guild_id=guild_id_int,
                limit=100,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            for channel_data in top_channels:
                channel_id = channel_data.get('channel_id')
                total_seconds = channel_data.get('total_seconds', 0)

                if channel_id and total_seconds > 0:
                    result["channels"].append(
                        (str(channel_id), total_seconds))

            top_categories = await self.db_cog.q_leaderboard_server_top_categories_voice(
                guild_id=guild_id_int,
                limit=100,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            for category_data in top_categories:
                category_id = category_data.get('category_id')
                total_seconds = category_data.get('total_seconds', 0)

                if category_id and total_seconds > 0:
                    result["voice_categories"].append(
                        (str(category_id), total_seconds))

        except Exception as e:
            logger.error(f"Error getting server voice data: {e}")
            traceback.print_exc()

        return result

    async def get_category_data(self, guild: discord.Guild, category: discord.CategoryChannel,
                                days_back: int, leaderboard_type: str, role_id: str = None):

        if not self.db_cog:
            logger.warning("DatabaseStats cog not available")
            return {"users": [], "channels": []}

        result = {
            "users": [],
            "channels": []
        }

        try:
            guild_id_int = DataFormatter.ensure_int(guild.id)
            if guild_id_int is None:
                logger.error(f"Invalid guild ID: {guild.id}")
                return result

            role_filter_ids = None
            if role_id and role_id != "none":
                role_id_int = DataFormatter.ensure_int(role_id)
                if role_id_int:
                    role_filter_ids = [role_id_int]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            if leaderboard_type == "top_users_messages":

                top_users = await self.db_cog.q_leaderboard_category_users_messages(
                    guild_id=guild_id_int,
                    category_id=category.id,
                    limit=100,
                    role_filter_ids=role_filter_ids,
                    start_time=start_time,
                    end_time=end_time
                )

                for user_data in top_users:
                    user_id = user_data.get('user_id')
                    message_count = user_data.get('message_count', 0)

                    if user_id and message_count > 0:
                        result["users"].append((str(user_id), message_count))

            elif leaderboard_type == "top_users_voice":

                top_users = await self.db_cog.q_leaderboard_category_users_voice(
                    guild_id=guild_id_int,
                    category_id=category.id,
                    limit=100,
                    role_filter_ids=role_filter_ids,
                    start_time=start_time,
                    end_time=end_time
                )

                for user_data in top_users:
                    user_id = user_data.get('user_id')
                    total_seconds = user_data.get('total_seconds', 0)

                    if user_id and total_seconds > 0:
                        result["users"].append((str(user_id), total_seconds))

            elif leaderboard_type == "top_text_channels":

                top_channels = await self.db_cog.q_category_top5_text_channels(
                    guild_id=guild_id_int,
                    category_id=category.id,
                    role_filter_ids=role_filter_ids,
                    start_time=start_time,
                    end_time=end_time
                )

                for channel_data in top_channels:
                    channel_id = channel_data.get('channel_id')
                    message_count = channel_data.get('message_count', 0)

                    if channel_id and message_count > 0:
                        result["channels"].append(
                            (str(channel_id), message_count))

            elif leaderboard_type == "top_voice_channels":

                top_channels = await self.db_cog.q_category_top5_voice_channels(
                    guild_id=guild_id_int,
                    category_id=category.id,
                    role_filter_ids=role_filter_ids,
                    start_time=start_time,
                    end_time=end_time
                )

                for channel_data in top_channels:
                    channel_id = channel_data.get('channel_id')
                    total_seconds = channel_data.get('total_seconds', 0)

                    if channel_id and total_seconds > 0:
                        result["channels"].append(
                            (str(channel_id), total_seconds))

        except Exception as e:
            logger.error(f"Error getting category data: {e}")
            traceback.print_exc()

        return result

    async def get_mentions_leaderboard(self, guild: discord.Guild, target_user: discord.Member,
                                       days_back: int, role_id: str = None):

        if not self.db_cog:
            logger.warning("DatabaseStats cog not available")
            return []

        try:

            guild_id_int = DataFormatter.ensure_int(guild.id)
            if guild_id_int is None:
                logger.error(f"Invalid guild ID: {guild.id}")
                return []

            role_filter_ids = None
            if role_id and role_id != "none":
                role_id_int = DataFormatter.ensure_int(role_id)
                if role_id_int:
                    role_filter_ids = [role_id_int]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            mention_data = await self.db_cog.q_leaderboard_mentions_target_user(
                guild_id=guild_id_int,
                mentioned_user_id=target_user.id,
                limit=100,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            user_stats = []
            for mention in mention_data:
                user_id = mention.get('user_id')
                mention_count = mention.get('mention_count', 0)

                if user_id and mention_count > 0:
                    user_stats.append((str(user_id), mention_count))

            return user_stats

        except Exception as e:
            logger.error(f"Error getting mentions leaderboard: {e}")
            traceback.print_exc()
            return []

    # COMMANDS

    leaderboard_group = app_commands.Group(
        name="leaderboard",
        description="Various leaderboards"
    )

    @leaderboard_group.command(name="voice", description="Show voice time leaderboard for a voice channel")
    @app_commands.describe(channel="Select a voice channel to show leaderboard for")
    async def voice_leaderboard(self, interaction: discord.Interaction, channel: discord.VoiceChannel):
        await interaction.response.defer()

        if not self.db_cog:
            await self._wait_for_db_cog()

        if not self.db_cog:
            await interaction.followup.send("❌ Database connection not available. Please make sure the DatabaseStats cog is loaded.", ephemeral=True)
            return

        leaderboard_data = await self.get_channel_voice_leaderboard(interaction.guild, channel, Config.DEFAULT_DAYS_BACK)

        if not leaderboard_data:
            await interaction.followup.send("⚠️ No data available for this channel.", ephemeral=True)
            return

        img_bytes = await generate_voice_leaderboard_image(
            guild=interaction.guild,
            channel=channel,
            leaderboard_data=leaderboard_data,
            days_back=Config.DEFAULT_DAYS_BACK,
            page=0
        )

        file = discord.File(img_bytes, filename="voice_leaderboard.png")

        view = VoiceLeaderboardView(
            self, interaction.guild, channel, days_back=Config.DEFAULT_DAYS_BACK
        )

        await interaction.followup.send(file=file, view=view)

        message = await interaction.original_response()
        self.active_views[message.id] = view

    @leaderboard_group.command(name="text", description="Show message leaderboard for a text channel")
    @app_commands.describe(channel="Select a text channel to show leaderboard for")
    async def text_leaderboard(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.defer()

        if not self.db_cog:
            await self._wait_for_db_cog()

        if not self.db_cog:
            await interaction.followup.send("❌ Database connection not available. Please make sure the DatabaseStats cog is loaded.", ephemeral=True)
            return

        leaderboard_data = await self.get_channel_text_leaderboard(interaction.guild, channel, Config.DEFAULT_DAYS_BACK)

        if not leaderboard_data:
            await interaction.followup.send("⚠️ No data available for this channel.", ephemeral=True)
            return

        total_messages = sum(count for _, count in leaderboard_data)

        img_bytes = await generate_text_leaderboard_image(
            guild=interaction.guild,
            channel=channel,
            leaderboard_data=leaderboard_data,
            days_back=Config.DEFAULT_DAYS_BACK,
            total_messages=total_messages,
            page=0
        )

        file = discord.File(img_bytes, filename="text_leaderboard.png")

        view = TextLeaderboardView(
            self, interaction.guild, channel, days_back=Config.DEFAULT_DAYS_BACK
        )

        await interaction.followup.send(file=file, view=view)

        message = await interaction.original_response()
        self.active_views[message.id] = view

    @leaderboard_group.command(name="server", description="Show server-wide leaderboards")
    async def server_leaderboard(self, interaction: discord.Interaction):
        await interaction.response.defer()

        if not self.db_cog:
            await self._wait_for_db_cog()

        if not self.db_cog:
            await interaction.followup.send("❌ Database connection not available. Please make sure the DatabaseStats cog is loaded.", ephemeral=True)
            return

        data = await self.get_server_message_data(interaction.guild, Config.DEFAULT_DAYS_BACK)
        leaderboard_data = data["users"]

        if not leaderboard_data:
            total_messages = 0
        else:
            total_messages = sum(
                count for _, count in leaderboard_data) if leaderboard_data else 0

            img_bytes = await generate_server_leaderboard_image(
                guild=interaction.guild,
                leaderboard_data=leaderboard_data,
                leaderboard_type="top_users_messages",
                days_back=Config.DEFAULT_DAYS_BACK,
                total_value=total_messages,
                page=0
            )

        file = discord.File(img_bytes, filename="server_leaderboard.png")

        view = ServerLeaderboardView(
            self, interaction.guild, days_back=Config.DEFAULT_DAYS_BACK
        )

        await interaction.followup.send(file=file, view=view)

        message = await interaction.original_response()
        self.active_views[message.id] = view

    @leaderboard_group.command(name="category", description="Show category-specific leaderboards")
    @app_commands.describe(category="Select a category to show leaderboard for")
    async def category_leaderboard(self, interaction: discord.Interaction, category: discord.CategoryChannel):
        await interaction.response.defer()

        if not self.db_cog:
            await self._wait_for_db_cog()

        if not self.db_cog:
            await interaction.followup.send("❌ Database connection not available. Please make sure the DatabaseStats cog is loaded.", ephemeral=True)
            return

        data = await self.get_category_data(
            interaction.guild,
            category,
            Config.DEFAULT_DAYS_BACK,
            "top_text_channels"
        )
        leaderboard_data = data["channels"]

        if not leaderboard_data:

            img_bytes = await generate_category_leaderboard_image(
                guild=interaction.guild,
                category=category,
                leaderboard_data=[],
                leaderboard_type="top_text_channels",
                days_back=Config.DEFAULT_DAYS_BACK,
                total_value=0,
                page=0
            )

            file = discord.File(img_bytes, filename="category_leaderboard.png")

            view = CategoryLeaderboardView(
                self, interaction.guild, category, days_back=Config.DEFAULT_DAYS_BACK
            )

            await interaction.followup.send(file=file, view=view)

            message = await interaction.original_response()
            self.active_views[message.id] = view
            return

        total_value = sum(count for _, count in leaderboard_data)

        img_bytes = await generate_category_leaderboard_image(
            guild=interaction.guild,
            category=category,
            leaderboard_data=leaderboard_data,
            leaderboard_type="top_text_channels",
            days_back=Config.DEFAULT_DAYS_BACK,
            total_value=total_value,
            page=0
        )

        file = discord.File(img_bytes, filename="category_leaderboard.png")

        view = CategoryLeaderboardView(
            self, interaction.guild, category, days_back=Config.DEFAULT_DAYS_BACK
        )

        await interaction.followup.send(file=file, view=view)

        message = await interaction.original_response()
        self.active_views[message.id] = view

    @leaderboard_group.command(name="mentions", description="Show users who mentioned a specific user the most")
    @app_commands.describe(target_user="The user to check mentions for")
    async def mentions_leaderboard(self, interaction: discord.Interaction, target_user: discord.Member):
        await interaction.response.defer()

        if not self.db_cog:
            await self._wait_for_db_cog()

        if not self.db_cog:
            await interaction.followup.send("❌ Database connection not available. Please make sure the DatabaseStats cog is loaded.", ephemeral=True)
            return

        leaderboard_data = await self.get_mentions_leaderboard(
            interaction.guild,
            target_user,
            Config.DEFAULT_DAYS_BACK
        )

        img_bytes = await generate_mentions_leaderboard_image(
            guild=interaction.guild,
            target_member=target_user,
            leaderboard_data=leaderboard_data,
            days_back=Config.DEFAULT_DAYS_BACK,
            page=0
        )

        file = discord.File(img_bytes, filename="mentions_leaderboard.png")

        view = MentionsLeaderboardView(
            self, interaction.guild, target_user, days_back=Config.DEFAULT_DAYS_BACK
        )

        await interaction.followup.send(file=file, view=view)

        message = await interaction.original_response()
        self.active_views[message.id] = view

    # ERROR HANDLING

    @voice_leaderboard.error
    @text_leaderboard.error
    @server_leaderboard.error
    @category_leaderboard.error
    @mentions_leaderboard.error
    async def leaderboard_error_handler(self, interaction: discord.Interaction, error: app_commands.AppCommandError):

        try:
            if isinstance(error, app_commands.CommandInvokeError):
                original = error.original
                if isinstance(original, discord.Forbidden):
                    if not interaction.response.is_done():
                        await interaction.response.send_message("❌ I don't have permission to send messages in this channel.", ephemeral=True)
                    else:
                        await interaction.followup.send("❌ I don't have permission to send messages in this channel.", ephemeral=True)
                elif isinstance(original, discord.HTTPException):
                    if not interaction.response.is_done():
                        await interaction.response.send_message(f"❌ Discord API error: {str(original)}", ephemeral=True)
                    else:
                        await interaction.followup.send(f"❌ Discord API error: {str(original)}", ephemeral=True)
                else:
                    logger.error(f"Leaderboard error: {original}")
                    traceback.print_exc()
                    if not interaction.response.is_done():
                        await interaction.response.send_message("❌ An unexpected error occurred. Please try again.", ephemeral=True)
                    else:
                        await interaction.followup.send("❌ An unexpected error occurred. Please try again.", ephemeral=True)
            else:
                if not interaction.response.is_done():
                    await interaction.response.send_message(f"❌ Error: {str(error)}", ephemeral=True)
                else:
                    await interaction.followup.send(f"❌ Error: {str(error)}", ephemeral=True)
        except discord.errors.InteractionResponded:

            pass
        except Exception as e:
            logger.error(f"Error in error handler: {e}")
            traceback.print_exc()


# SETUP

async def setup(bot):
    await bot.add_cog(Leaderboards(bot))
