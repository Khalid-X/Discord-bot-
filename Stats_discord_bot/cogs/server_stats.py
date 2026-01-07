import discord
from discord.ext import commands
from discord import app_commands
from datetime import timedelta, datetime, timezone
import time
import asyncio
from PIL import Image, ImageDraw, ImageFont
import io
import aiohttp
import os
from dotenv import load_dotenv
from pilmoji import Pilmoji
import traceback
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv()


# FORMAT

def format_time(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}m {secs}s"
    elif seconds < 86400:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"
    else:
        days = seconds // 86400
        hours = (seconds % 86400) // 3600
        return f"{days}d {hours}h"


def format_message_count(count):
    count = int(count)
    if count >= 1000000:
        return f"{count/1000000:.1f}M"
    elif count >= 1000:
        return f"{count/1000:.1f}K"
    else:
        return str(count)


# DRAWING FUNCTIONS

def draw_text_centered(draw, text, position, font, fill, max_width=None):
    bbox = draw.textbbox((0, 0), text, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]

    x = position[0] - text_width // 2
    y = position[1] - text_height // 2

    if max_width and text_width > max_width:
        while text_width > max_width and len(text) > 1:
            text = text[:-1]
            bbox = draw.textbbox((0, 0), text + "...", font=font)
            text_width = bbox[2] - bbox[0]
        text = text + "..."

    draw.text((x, y), text, fill=fill, font=font)


async def create_user_stats_image(guild, stats_data, days_back=30, role_id=None):
    template_path = BASE_DIR / "assets" / "images" / "server stats final png.png"
    try:
        image = Image.open(template_path)
    except FileNotFoundError:
        image = Image.new('RGB', (800, 800), color='#2C2F33')

    draw = ImageDraw.Draw(image)

    # FONTS

    try:
        font_paths = [BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"]
        font_loaded = False
        for font_path in font_paths:
            if os.path.exists(font_path):
                try:
                    font_large = ImageFont.truetype(font_path, 24)
                    font_medium = ImageFont.truetype(font_path, 20)
                    font_small = ImageFont.truetype(font_path, 16)
                    font_larger = ImageFont.truetype(font_path, 30)
                    font_huge = ImageFont.truetype(font_path, 40)
                    font_horndon_medium = ImageFont.truetype(font_path, 20)
                    arial_small = ImageFont.truetype("arial.ttf", 16)
                    emoji_font = ImageFont.truetype("arial.ttf", 24)

                    font_horndon_tiny = ImageFont.truetype(font_path, 8)
                    font_horndon_extra_small = ImageFont.truetype(
                        font_path, 10)
                    font_horndon_small = ImageFont.truetype(font_path, 12)
                    font_horndon_medium_small = ImageFont.truetype(
                        font_path, 14)
                    font_horndon_medium = ImageFont.truetype(font_path, 16)
                    font_horndon_large = ImageFont.truetype(font_path, 18)
                    font_horndon_larger = ImageFont.truetype(font_path, 20)
                    font_horndon_xlarge = ImageFont.truetype(font_path, 22)
                    font_horndon_xxlarge = ImageFont.truetype(font_path, 24)
                    font_horndon_huge = ImageFont.truetype(font_path, 30)
                    font_horndon_giant = ImageFont.truetype(font_path, 40)
                    font_loaded = True
                    break
                except:
                    continue

        if not font_loaded:
            font_large = ImageFont.load_default()
            font_medium = ImageFont.load_default()
            font_small = ImageFont.load_default()
            font_larger = ImageFont.load_default()
            font_huge = ImageFont.load_default()
            font_horndon_medium = ImageFont.load_default()
            arial_small = ImageFont.load_default()
            emoji_font = ImageFont.load_default()

            font_horndon_tiny = ImageFont.load_default()
            font_horndon_extra_small = ImageFont.load_default()
            font_horndon_small = ImageFont.load_default()
            font_horndon_medium_small = ImageFont.load_default()
            font_horndon_medium = ImageFont.load_default()
            font_horndon_large = ImageFont.load_default()
            font_horndon_larger = ImageFont.load_default()
            font_horndon_xlarge = ImageFont.load_default()
            font_horndon_xxlarge = ImageFont.load_default()
            font_horndon_huge = ImageFont.load_default()
            font_horndon_giant = ImageFont.load_default()

    except:
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()
        font_larger = ImageFont.load_default()
        font_huge = ImageFont.load_default()
        font_horndon_medium = ImageFont.load_default()
        arial_small = ImageFont.load_default()
        emoji_font = ImageFont.load_default()

        font_horndon_tiny = ImageFont.load_default()
        font_horndon_extra_small = ImageFont.load_default()
        font_horndon_small = ImageFont.load_default()
        font_horndon_medium_small = ImageFont.load_default()
        font_horndon_medium = ImageFont.load_default()
        font_horndon_large = ImageFont.load_default()
        font_horndon_larger = ImageFont.load_default()
        font_horndon_xlarge = ImageFont.load_default()
        font_horndon_xxlarge = ImageFont.load_default()
        font_horndon_huge = ImageFont.load_default()
        font_horndon_giant = ImageFont.load_default()

    # INVISIBLE RECTANGLE FUNCTIONS

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

    def fit_text_to_rectangle(text, text_center_x, text_center_y, rect_center_x, rect_center_y, rect_width, rect_height, is_channel=False, is_username=False):

        if is_username:
            font_sizes = [26, 24, 22, 20, 18, 16]
        elif is_channel:
            font_sizes = [24, 22, 20, 18, 16, 14, 12, 10, 8]
        else:
            font_sizes = [24, 22, 20, 18, 16, 14, 12, 10, 8]

        for font_size in font_sizes:
            try:
                font = ImageFont.truetype(font_paths[0], font_size) if os.path.exists(
                    font_paths[0]) else ImageFont.load_default()
            except:
                font = ImageFont.load_default()

            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            text_x = text_center_x - (text_width // 2)
            text_y = text_center_y - (text_height // 2)

            vertical_offset = 0

            if is_username:
                if font_size <= 26:
                    vertical_offset += 1
                if font_size <= 20:
                    vertical_offset += 1
                if font_size <= 16:
                    vertical_offset += 1
            elif is_channel:
                if font_size <= 20:
                    vertical_offset += 1
                if font_size <= 16:
                    vertical_offset += 1
            else:
                if font_size <= 12:
                    vertical_offset += 1

            text_y += vertical_offset

            if not check_text_against_rectangle(text, font, text_x, text_y, rect_center_x, rect_center_y, rect_width, rect_height):
                return text, font, (text_x, text_y)

        smallest_font = font_sizes[-1]
        try:
            font = ImageFont.truetype(font_paths[0], smallest_font) if os.path.exists(
                font_paths[0]) else ImageFont.load_default()
        except:
            font = ImageFont.load_default()

        truncated_text = text
        while len(truncated_text) > 3:
            truncated_text = truncated_text[:-4] + "..."
            bbox = draw.textbbox((0, 0), truncated_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]

            text_x = text_center_x - (text_width // 2)
            text_y = text_center_y - (text_height // 2)

            vertical_offset = 0
            if is_username:
                if smallest_font <= 26:
                    vertical_offset += 1
                if smallest_font <= 20:
                    vertical_offset += 1
                if smallest_font <= 16:
                    vertical_offset += 1
            elif is_channel:
                if smallest_font <= 20:
                    vertical_offset += 1
                if smallest_font <= 16:
                    vertical_offset += 1
            else:
                if smallest_font <= 12:
                    vertical_offset += 1

            text_y += vertical_offset

            if not check_text_against_rectangle(truncated_text, font, text_x, text_y, rect_center_x, rect_center_y, rect_width, rect_height):
                return truncated_text, font, (text_x, text_y)

        if len(text) > 3:
            final_text = text[:3] + "..."
        elif text:
            final_text = text
        else:
            final_text = "-"

        bbox = draw.textbbox((0, 0), final_text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        text_x = text_center_x - (text_width // 2)
        text_y = text_center_y - (text_height // 2)

        vertical_offset = 0
        if is_channel and smallest_font <= 20:
            vertical_offset += 1
        if is_channel and smallest_font <= 16:
            vertical_offset += 1
        text_y += vertical_offset

        return final_text, font, (text_x, text_y)

    text_channel_rectangles_left = [
        {"center": (70, 325), "width": 120, "height": 50},
        {"center": (70, 380), "width": 120, "height": 50},
        {"center": (70, 435), "width": 120, "height": 50}
    ]

    text_channel_rectangles_right = [
        {"center": (325, 325), "width": 120, "height": 50},
        {"center": (325, 380), "width": 120, "height": 50},
        {"center": (325, 435), "width": 120, "height": 50}
    ]

    voice_channel_rectangles_left = [
        {"center": (70, 540), "width": 120, "height": 50},
        {"center": (70, 595), "width": 120, "height": 50},
        {"center": (70, 650), "width": 120, "height": 50}
    ]

    voice_channel_rectangles_right = [
        {"center": (325, 540), "width": 120, "height": 50},
        {"center": (325, 595), "width": 120, "height": 50},
        {"center": (325, 650), "width": 120, "height": 50}
    ]

    server_name_rectangle = {"center": (150, 30), "width": 183, "height": 35}

    with Pilmoji(image) as pilmoji:
        # Server profile picture
        server_name = guild.name

        try:
            if guild.icon:
                icon_url = guild.icon.url
                async with aiohttp.ClientSession() as session:
                    async with session.get(icon_url) as response:
                        icon_data = await response.read()

                icon_image = Image.open(io.BytesIO(icon_data))
                icon_size = (60, 60)
                icon_image = icon_image.resize(
                    icon_size, Image.Resampling.LANCZOS).convert('RGBA')

                mask = Image.new('L', icon_size, 0)
                mask_draw = ImageDraw.Draw(mask)
                mask_draw.ellipse((0, 0, icon_size[0], icon_size[1]), fill=255)
                icon_image.putalpha(mask)

                icon_x = 7
                icon_y = 25 - 20

                icon_area = image.crop(
                    (icon_x, icon_y, icon_x + icon_size[0], icon_y + icon_size[1])).convert('RGBA')
                icon_with_bg = Image.new('RGBA', icon_size, (0, 0, 0, 0))
                icon_with_bg.paste(icon_image, (0, 0), icon_image)
                icon_area.paste(icon_with_bg, (0, 0), icon_with_bg)
                image.paste(icon_area.convert('RGB'), (icon_x, icon_y))

                server_name_font_sizes = [
                    40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16]

                fitted_text = server_name
                server_name_font = None
                final_pos = None

                rect_center_x = server_name_rectangle["center"][0]
                rect_center_y = server_name_rectangle["center"][1]
                rect_width = server_name_rectangle["width"]
                rect_height = server_name_rectangle["height"]

                text_start_x = 75
                text_start_y = 25

                for font_size in server_name_font_sizes:
                    try:
                        font = ImageFont.truetype(font_paths[0], font_size) if os.path.exists(
                            font_paths[0]) else ImageFont.load_default()
                    except:
                        font = ImageFont.load_default()

                    bbox = draw.textbbox((0, 0), server_name, font=font)
                    text_width = bbox[2] - bbox[0]
                    text_height = bbox[3] - bbox[1]

                    text_x = text_start_x
                    text_y = text_start_y - \
                        (text_height // 2)

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

                    if not check_text_against_rectangle(server_name, font, text_x, text_y,
                                                        rect_center_x, rect_center_y,
                                                        rect_width, rect_height):
                        fitted_text = server_name
                        server_name_font = font
                        final_pos = (text_x, text_y)
                        break

                if not server_name_font:
                    smallest_font = server_name_font_sizes[-1]
                    try:
                        server_name_font = ImageFont.truetype(font_paths[0], smallest_font) if os.path.exists(
                            font_paths[0]) else ImageFont.load_default()
                    except:
                        server_name_font = ImageFont.load_default()

                    rect_left = rect_center_x - (rect_width // 2)
                    rect_right = rect_center_x + (rect_width // 2)

                    truncated_text = server_name
                    for i in range(len(server_name)):

                        test_text = server_name[:len(server_name)-i] + "..."
                        bbox = draw.textbbox(
                            (0, 0), test_text, font=server_name_font)
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

                        if (text_x >= rect_left) and ((text_x + text_width) <= rect_right):
                            fitted_text = test_text
                            final_pos = (text_x, text_y)
                            break

                    if not final_pos:
                        if len(server_name) > 3:
                            fitted_text = server_name[:3] + "..."
                        elif server_name:
                            fitted_text = server_name
                        else:
                            fitted_text = "-"

                        bbox = draw.textbbox(
                            (0, 0), fitted_text, font=server_name_font)
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

                        final_pos = (text_x, text_y)

                if fitted_text and final_pos:
                    stroke_width = 1
                    for dx in [-stroke_width, 0, stroke_width]:
                        for dy in [-stroke_width, 0, stroke_width]:
                            if dx != 0 or dy != 0:
                                pilmoji.text((final_pos[0] + dx, final_pos[1] + dy),
                                             fitted_text, "black", server_name_font,
                                             emoji_position_offset=(0, 0))

                    pilmoji.text(final_pos, fitted_text, "white", server_name_font,
                                 emoji_position_offset=(0, 0))

            else:

                server_name_font_sizes = [
                    40, 38, 36, 34, 32, 30, 28, 26, 24, 22, 20, 18, 16]

                fitted_text = server_name
                server_name_font = None
                final_pos = None

                rect_center_x = server_name_rectangle["center"][0]
                rect_center_y = server_name_rectangle["center"][1]
                rect_width = server_name_rectangle["width"]
                rect_height = server_name_rectangle["height"]

                text_start_x = 75
                text_start_y = 25

                for font_size in server_name_font_sizes:
                    try:
                        font = ImageFont.truetype(font_paths[0], font_size) if os.path.exists(
                            font_paths[0]) else ImageFont.load_default()
                    except:
                        font = ImageFont.load_default()

                    bbox = draw.textbbox((0, 0), server_name, font=font)
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

                    if not check_text_against_rectangle(server_name, font, text_x, text_y,
                                                        rect_center_x, rect_center_y,
                                                        rect_width, rect_height):
                        fitted_text = server_name
                        server_name_font = font
                        final_pos = (text_x, text_y)
                        break

                if not server_name_font:
                    smallest_font = server_name_font_sizes[-1]
                    try:
                        server_name_font = ImageFont.truetype(font_paths[0], smallest_font) if os.path.exists(
                            font_paths[0]) else ImageFont.load_default()
                    except:
                        server_name_font = ImageFont.load_default()

                    rect_left = rect_center_x - (rect_width // 2)
                    rect_right = rect_center_x + (rect_width // 2)

                    truncated_text = server_name
                    for i in range(len(server_name)):

                        test_text = server_name[:len(server_name)-i] + "..."
                        bbox = draw.textbbox(
                            (0, 0), test_text, font=server_name_font)
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

                        if (text_x >= rect_left) and ((text_x + text_width) <= rect_right):
                            fitted_text = test_text
                            final_pos = (text_x, text_y)
                            break

                    if not final_pos:
                        if len(server_name) > 3:
                            fitted_text = server_name[:3] + "..."
                        elif server_name:
                            fitted_text = server_name
                        else:
                            fitted_text = "-"

                        bbox = draw.textbbox(
                            (0, 0), fitted_text, font=server_name_font)
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

                        final_pos = (text_x, text_y)

                if fitted_text and final_pos:
                    stroke_width = 1
                    for dx in [-stroke_width, 0, stroke_width]:
                        for dy in [-stroke_width, 0, stroke_width]:
                            if dx != 0 or dy != 0:
                                pilmoji.text((final_pos[0] + dx, final_pos[1] + dy),
                                             fitted_text, "black", server_name_font,
                                             emoji_position_offset=(0, 0))

                    pilmoji.text(final_pos, fitted_text, "white", server_name_font,
                                 emoji_position_offset=(0, 0))

        except Exception as e:
            print(f"Error with server icon/name: {e}")

            server_name_font_sizes = [40, 38, 36, 34,
                                      32, 30, 28, 26, 24, 22, 20, 18, 16]

            fitted_text = server_name
            server_name_font = None
            final_pos = None

            rect_center_x = server_name_rectangle["center"][0]
            rect_center_y = server_name_rectangle["center"][1]
            rect_width = server_name_rectangle["width"]
            rect_height = server_name_rectangle["height"]

            text_start_x = 75
            text_start_y = 25

            for font_size in server_name_font_sizes:
                try:
                    font = ImageFont.truetype(font_paths[0], font_size) if os.path.exists(
                        font_paths[0]) else ImageFont.load_default()
                except:
                    font = ImageFont.load_default()

                bbox = draw.textbbox((0, 0), server_name, font=font)
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

                if not check_text_against_rectangle(server_name, font, text_x, text_y,
                                                    rect_center_x, rect_center_y,
                                                    rect_width, rect_height):
                    fitted_text = server_name
                    server_name_font = font
                    final_pos = (text_x, text_y)
                    break

            if not server_name_font:
                smallest_font = server_name_font_sizes[-1]
                try:
                    server_name_font = ImageFont.truetype(font_paths[0], smallest_font) if os.path.exists(
                        font_paths[0]) else ImageFont.load_default()
                except:
                    server_name_font = ImageFont.load_default()

                rect_left = rect_center_x - (rect_width // 2)
                rect_right = rect_center_x + (rect_width // 2)

                for i in range(len(server_name)):

                    test_text = server_name[:len(server_name)-i] + "..."
                    bbox = draw.textbbox(
                        (0, 0), test_text, font=server_name_font)
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

                    if (text_x >= rect_left) and ((text_x + text_width) <= rect_right):
                        fitted_text = test_text
                        final_pos = (text_x, text_y)
                        break

                if not final_pos:
                    if len(server_name) > 3:
                        fitted_text = server_name[:3] + "..."
                    elif server_name:
                        fitted_text = server_name
                    else:
                        fitted_text = "-"

                    bbox = draw.textbbox(
                        (0, 0), fitted_text, font=server_name_font)
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

                    final_pos = (text_x, text_y)

            if fitted_text and final_pos:
                stroke_width = 1
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            pilmoji.text((final_pos[0] + dx, final_pos[1] + dy),
                                         fitted_text, "black", server_name_font,
                                         emoji_position_offset=(0, 0))

                pilmoji.text(final_pos, fitted_text, "white", server_name_font,
                             emoji_position_offset=(0, 0))

        # CREATED ON
        current_date = datetime.now().strftime("%B %d, %Y")
        stroke_width = 1
        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw_text_centered(draw, current_date, (605 + dx, 45 + dy),
                                       font_horndon_medium, "black", max_width=150)

        draw_text_centered(draw, current_date, (605, 45),
                           font_horndon_medium, "white", max_width=150)

        # TIME PERIOD
        time_period_text = f"{days_back} days"
        stroke_width = 1
        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw_text_centered(draw, time_period_text, (147 + dx, 713 + dy),
                                       font_horndon_medium, "black", max_width=100)

        draw_text_centered(draw, time_period_text, (147, 713),
                           font_horndon_medium, "white", max_width=100)

        # ROLE FILTER
        role_text = "No Filter"
        if role_id and role_id != "none":
            try:
                role = guild.get_role(int(role_id))
                role_text = role.name if role else "Unknown Role"
            except (ValueError, TypeError):
                role_text = "Unknown Role"

        stroke_width = 1
        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw_text_centered(draw, role_text, (357 + dx, 712 + dy),
                                       font_horndon_medium, "black", max_width=150)

        draw_text_centered(draw, role_text, (357, 712),
                           font_horndon_medium, "white", max_width=150)

        # TOP EMOJIS
        top_emojis = stats_data.get('top_emojis', [])
        emoji_positions = [(50, 155), (50, 195), (50, 235)]
        uses_positions = [(155, 150), (155, 190), (155, 230)]

        for i in range(3):
            if i < len(top_emojis):
                emoji_data = top_emojis[i]
                emoji_str = emoji_data.get('emoji', '❓')
                uses = emoji_data.get('uses', 0)

                try:
                    emoji_x = emoji_positions[i][0] - 15
                    emoji_y = emoji_positions[i][1] - 15
                    pilmoji.text((emoji_x, emoji_y), emoji_str,
                                 (255, 255, 255), emoji_font)
                except Exception:
                    stroke_width = 1
                    for dx in [-stroke_width, 0, stroke_width]:
                        for dy in [-stroke_width, 0, stroke_width]:
                            if dx != 0 or dy != 0:
                                draw_text_centered(draw, emoji_str, (emoji_positions[i][0] + dx, emoji_positions[i][1] + dy),
                                                   font_medium, "black", max_width=80)

                    draw_text_centered(draw, emoji_str, emoji_positions[i],
                                       font_medium, "white", max_width=80)

                stroke_width = 1
                uses_str = str(uses)
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, uses_str, (uses_positions[i][0] + dx, uses_positions[i][1] + dy),
                                               font_medium, "black", max_width=80)

                draw_text_centered(draw, uses_str, uses_positions[i],
                                   font_medium, "white", max_width=80)
            else:
                stroke_width = 1
                placeholder = "-"
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, placeholder, (emoji_positions[i][0] + dx, emoji_positions[i][1] + dy),
                                               font_medium, "black", max_width=80)

                draw_text_centered(draw, placeholder, emoji_positions[i],
                                   font_medium, "white", max_width=80)

                stroke_width = 1
                zero_str = "0"
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, zero_str, (uses_positions[i][0] + dx, uses_positions[i][1] + dy),
                                               font_medium, "black", max_width=80)

                draw_text_centered(draw, zero_str, uses_positions[i],
                                   font_medium, "white", max_width=80)

        # TOTALS
        total_messages = stats_data.get('total_messages', 0)
        total_voice = stats_data.get('total_seconds', 0)

        total_messages_text = format_message_count(total_messages)
        stroke_width = 1
        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw_text_centered(draw, total_messages_text, (450 + dx, 160 + dy),
                                       font_large, "black", max_width=100)

        draw_text_centered(draw, total_messages_text, (450, 160),
                           font_large, "white", max_width=100)

        total_voice_text = format_time(total_voice)
        stroke_width = 1
        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw_text_centered(draw, total_voice_text, (450 + dx, 215 + dy),
                                       font_large, "black", max_width=100)

        draw_text_centered(draw, total_voice_text, (450, 215),
                           font_large, "white", max_width=100)

        # MESSAGES OVER TIME
        messages_over_time = stats_data.get('messages_over_time', {})
        message_time_positions = {
            1: (675, 160),
            5: (675, 215),
            10: (675, 270),
            20: (675, 320),
            30: (675, 370)
        }

        for days, pos in message_time_positions.items():
            count = messages_over_time.get(
                days, messages_over_time.get(f'{days}d', 0))
            count_text = format_message_count(count)

            stroke_width = 1
            for dx in [-stroke_width, 0, stroke_width]:
                for dy in [-stroke_width, 0, stroke_width]:
                    if dx != 0 or dy != 0:
                        draw_text_centered(draw, count_text, (pos[0] + dx, pos[1] + dy),
                                           font_large, "black", max_width=80)

            draw_text_centered(draw, count_text, pos,
                               font_large, "white", max_width=80)

        # VOICE OVER TIME
        voice_over_time = stats_data.get('voice_over_time', {})
        voice_time_positions = {
            1: (675, 475),
            5: (675, 530),
            10: (675, 590),
            20: (675, 645),
            30: (675, 695)
        }

        for days, pos in voice_time_positions.items():
            seconds = voice_over_time.get(
                days, voice_over_time.get(f'{days}d', 0))
            voice_time_text = format_time(seconds)

            stroke_width = 1
            for dx in [-stroke_width, 0, stroke_width]:
                for dy in [-stroke_width, 0, stroke_width]:
                    if dx != 0 or dy != 0:
                        draw_text_centered(draw, voice_time_text, (pos[0] + dx, pos[1] + dy),
                                           font_large, "black", max_width=80)

            draw_text_centered(draw, voice_time_text, pos,
                               font_large, "white", max_width=80)

        # TOP USERS BY MESSAGES
        top_users_messages = stats_data.get('top_users_messages', [])
        user_msg_positions = [(65, 325), (65, 380), (65, 435)]
        user_msg_count_positions = [(190, 325), (190, 380), (190, 435)]

        for i in range(3):
            if i < len(top_users_messages):
                user_data = top_users_messages[i]
                username = user_data.get('name', 'Unknown')
                messages = user_data.get('messages', 0)

                rect_info = text_channel_rectangles_left[i]
                fitted_text, user_font, user_pos = fit_text_to_rectangle(
                    username,
                    user_msg_positions[i][0],
                    user_msg_positions[i][1],
                    rect_info["center"][0],
                    rect_info["center"][1],
                    rect_info["width"],
                    rect_info["height"],
                    is_channel=False,
                    is_username=True
                )

                if fitted_text:
                    stroke_width = 1
                    for dx in [-stroke_width, 0, stroke_width]:
                        for dy in [-stroke_width, 0, stroke_width]:
                            if dx != 0 or dy != 0:
                                pilmoji.text((user_pos[0] + dx, user_pos[1] + dy),
                                             fitted_text, "black", user_font,
                                             emoji_position_offset=(0, 0))

                    pilmoji.text(user_pos, fitted_text, "white", user_font,
                                 emoji_position_offset=(0, 0))

                messages_text = format_message_count(messages)
                stroke_width = 1
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, messages_text,
                                               (user_msg_count_positions[i][0] + dx,
                                                user_msg_count_positions[i][1] + dy),
                                               font_large, "black", max_width=80)

                draw_text_centered(draw, messages_text, user_msg_count_positions[i],
                                   font_large, "white", max_width=80)
            else:
                stroke_width = 1
                placeholder = "-"
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, placeholder,
                                               (user_msg_positions[i][0] + dx,
                                                user_msg_positions[i][1] + dy),
                                               font_large, "black", max_width=120)

                draw_text_centered(draw, placeholder, user_msg_positions[i],
                                   font_large, "white", max_width=120)

                stroke_width = 1
                zero_str = "0"
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, zero_str,
                                               (user_msg_count_positions[i][0] + dx,
                                                user_msg_count_positions[i][1] + dy),
                                               font_large, "black", max_width=80)

                draw_text_centered(draw, zero_str, user_msg_count_positions[i],
                                   font_large, "white", max_width=80)

        # TOP USERS BY VOICE
        top_users_voice = stats_data.get('top_users_voice', [])
        user_voice_positions = [(65, 540), (65, 595), (65, 650)]
        user_voice_time_positions = [(190, 540), (190, 595), (190, 650)]

        for i in range(3):
            if i < len(top_users_voice):
                user_data = top_users_voice[i]
                username = user_data.get('name', 'Unknown')
                voice_time = user_data.get('voice_time', 0)

                rect_info = voice_channel_rectangles_left[i]
                fitted_text, user_font, user_pos = fit_text_to_rectangle(
                    username,
                    user_voice_positions[i][0],
                    user_voice_positions[i][1],
                    rect_info["center"][0],
                    rect_info["center"][1],
                    rect_info["width"],
                    rect_info["height"],
                    is_channel=False,
                    is_username=True
                )

                if fitted_text:
                    stroke_width = 1
                    for dx in [-stroke_width, 0, stroke_width]:
                        for dy in [-stroke_width, 0, stroke_width]:
                            if dx != 0 or dy != 0:
                                pilmoji.text((user_pos[0] + dx, user_pos[1] + dy),
                                             fitted_text, "black", user_font,
                                             emoji_position_offset=(0, 0))

                    pilmoji.text(user_pos, fitted_text, "white", user_font,
                                 emoji_position_offset=(0, 0))

                voice_time_text = format_time(voice_time)
                stroke_width = 1
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, voice_time_text,
                                               (user_voice_time_positions[i][0] + dx,
                                                user_voice_time_positions[i][1] + dy),
                                               font_large, "black", max_width=80)

                draw_text_centered(draw, voice_time_text, user_voice_time_positions[i],
                                   font_large, "white", max_width=80)
            else:
                stroke_width = 1
                placeholder = "-"
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, placeholder,
                                               (user_voice_positions[i][0] + dx,
                                                user_voice_positions[i][1] + dy),
                                               font_large, "black", max_width=120)

                draw_text_centered(draw, placeholder, user_voice_positions[i],
                                   font_large, "white", max_width=120)

                stroke_width = 1
                zero_str = "0s"
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, zero_str,
                                               (user_voice_time_positions[i][0] + dx,
                                                user_voice_time_positions[i][1] + dy),
                                               font_large, "black", max_width=80)

                draw_text_centered(draw, zero_str, user_voice_time_positions[i],
                                   font_large, "white", max_width=80)

        # TOP TEXT CHANNELS
        top_text_channels = stats_data.get('top_text_channels', [])
        text_channel_positions = [(325, 325), (325, 380), (325, 435)]
        text_channel_msg_positions = [(450, 325), (450, 380), (450, 435)]

        for i in range(3):
            text_pos = text_channel_positions[i]
            rect_info = text_channel_rectangles_right[i]

            if i < len(top_text_channels):
                channel_data = top_text_channels[i]
                channel_name = channel_data.get('name', '[deleted channel]')
                messages = channel_data.get('messages', 0)

                fitted_text, channel_font, final_pos = fit_text_to_rectangle(
                    channel_name,
                    text_pos[0],
                    text_pos[1],
                    rect_info["center"][0],
                    rect_info["center"][1],
                    rect_info["width"],
                    rect_info["height"],
                    is_channel=True
                )

                if fitted_text:
                    stroke_width = 1
                    for dx in [-stroke_width, 0, stroke_width]:
                        for dy in [-stroke_width, 0, stroke_width]:
                            if dx != 0 or dy != 0:
                                pilmoji.text((final_pos[0] + dx, final_pos[1] + dy),
                                             fitted_text, "black", channel_font,
                                             emoji_position_offset=(0, 0))

                    pilmoji.text(final_pos, fitted_text, "white", channel_font,
                                 emoji_position_offset=(0, 0))

                messages_text = format_message_count(messages)
                stroke_width = 1
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, messages_text,
                                               (text_channel_msg_positions[i][0] + dx,
                                                text_channel_msg_positions[i][1] + dy),
                                               font_large, "black", max_width=80)

                draw_text_centered(draw, messages_text, text_channel_msg_positions[i],
                                   font_large, "white", max_width=80)
            else:
                placeholder_font = font_large
                bbox = draw.textbbox((0, 0), "-", font=placeholder_font)
                text_width = bbox[2] - bbox[0]
                text_height = bbox[3] - bbox[1]
                placeholder_x = text_pos[0] - (text_width // 2)
                placeholder_y = text_pos[1] - (text_height // 2)

                stroke_width = 1
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw.text((placeholder_x + dx, placeholder_y + dy), "-",
                                      fill="black", font=placeholder_font)

                draw.text((placeholder_x, placeholder_y), "-",
                          fill="white", font=placeholder_font)

                stroke_width = 1
                zero_str = "0"
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, zero_str,
                                               (text_channel_msg_positions[i][0] + dx,
                                                text_channel_msg_positions[i][1] + dy),
                                               font_large, "black", max_width=80)

                draw_text_centered(draw, zero_str, text_channel_msg_positions[i],
                                   font_large, "white", max_width=80)

        # TOP VOICE CHANNELS
        top_voice_channels = stats_data.get('top_voice_channels', [])
        voice_channel_positions = [(325, 540), (325, 595), (325, 650)]
        voice_channel_time_positions = [(450, 540), (450, 595), (450, 650)]

        for i in range(3):
            text_pos = voice_channel_positions[i]
            rect_info = voice_channel_rectangles_right[i]

            if i < len(top_voice_channels):
                channel_data = top_voice_channels[i]
                channel_name = channel_data.get('name', '[deleted channel]')
                voice_time = channel_data.get('voice_time', 0)

                fitted_text, channel_font, final_pos = fit_text_to_rectangle(
                    channel_name,
                    text_pos[0],
                    text_pos[1],
                    rect_info["center"][0],
                    rect_info["center"][1],
                    rect_info["width"],
                    rect_info["height"],
                    is_channel=True
                )

                if fitted_text:
                    stroke_width = 1
                    for dx in [-stroke_width, 0, stroke_width]:
                        for dy in [-stroke_width, 0, stroke_width]:
                            if dx != 0 or dy != 0:
                                pilmoji.text((final_pos[0] + dx, final_pos[1] + dy),
                                             fitted_text, "black", channel_font,
                                             emoji_position_offset=(0, 0))

                    pilmoji.text(final_pos, fitted_text, "white", channel_font,
                                 emoji_position_offset=(0, 0))

                time_text = format_time(voice_time)
                stroke_width = 1
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, time_text,
                                               (voice_channel_time_positions[i][0] + dx,
                                                voice_channel_time_positions[i][1] + dy),
                                               font_large, "black", max_width=80)

                draw_text_centered(draw, time_text, voice_channel_time_positions[i],
                                   font_large, "white", max_width=80)
            else:
                placeholder_font = font_large
                bbox = draw.textbbox((0, 0), "-", font=placeholder_font)
                text_width = bbox[2] - bbox[0]
                text_height = bbox[3] - bbox[1]
                placeholder_x = text_pos[0] - (text_width // 2)
                placeholder_y = text_pos[1] - (text_height // 2)

                stroke_width = 1
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw.text((placeholder_x + dx, placeholder_y + dy), "-",
                                      fill="black", font=placeholder_font)

                draw.text((placeholder_x, placeholder_y), "-",
                          fill="white", font=placeholder_font)

                stroke_width = 1
                zero_str = "0s"
                for dx in [-stroke_width, 0, stroke_width]:
                    for dy in [-stroke_width, 0, stroke_width]:
                        if dx != 0 or dy != 0:
                            draw_text_centered(draw, zero_str,
                                               (voice_channel_time_positions[i][0] + dx,
                                                voice_channel_time_positions[i][1] + dy),
                                               font_large, "black", max_width=80)

                draw_text_centered(draw, zero_str, voice_channel_time_positions[i],
                                   font_large, "white", max_width=80)

    return image


# ROLE FILTER DROPDOWN MENU

class ServerRoleSelectMenu(discord.ui.Select):
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
                    label=role.name[:25],
                    value=str(role.id),
                    description=f"Filter by {role.name}"[:50],
                    default=(str(role.id) == current_role_id)
                )
            )

        super().__init__(
            placeholder="Filter by role...",
            options=options,
            custom_id="server_role_filter_select",
            min_values=1,
            max_values=1,
            row=0
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()

        view = self.view
        if hasattr(view, 'selected_role_id'):
            view.selected_role_id = self.values[0]

            stats_data = await view.cog.get_server_stats(
                view.guild.id, view.current_days, view.selected_role_id
            )
            if stats_data:
                image_stats_data = view.cog._convert_to_image_format(
                    stats_data, view.guild)
                image = await create_user_stats_image(view.guild, image_stats_data, view.current_days, view.selected_role_id)

                img_bytes = io.BytesIO()
                image.save(img_bytes, format='PNG')
                img_bytes.seek(0)

                file = discord.File(img_bytes, filename='server_stats.png')
                view._update_buttons()
                await interaction.edit_original_response(attachments=[file], view=view)
            else:
                await interaction.edit_original_response(content="❌ Error generating stats image", view=view)


class ServerTimeModal(discord.ui.Modal, title='Custom Time Period'):
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
            if days <= 0 or days > 2000:
                await interaction.response.send_message("❌ Please enter a number between 1 and 2000 days.", ephemeral=True)
                return

            await interaction.response.defer()

            view = self.cog.active_sessions.get(
                self.original_message_id, {}).get('view')
            if view:
                view.current_days = days
                view.show_time_buttons = False
                view._update_buttons()

                stats_data = await self.cog.get_server_stats(self.guild.id, days)
                if stats_data:
                    image_stats_data = self.cog._convert_to_image_format(
                        stats_data, self.guild)
                    image = await create_user_stats_image(self.guild, image_stats_data, days)

                    img_bytes = io.BytesIO()
                    image.save(img_bytes, format='PNG')
                    img_bytes.seek(0)

                    file = discord.File(img_bytes, filename='server_stats.png')
                    await interaction.edit_original_response(attachments=[file], view=view)
                else:
                    await interaction.edit_original_response(content="❌ Error generating stats image", view=view)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            print(f"Error in server modal submit: {e}")
            try:
                await interaction.edit_original_response(content="❌ An error occurred while updating stats.", view=None)
            except:
                await interaction.response.send_message("❌ An error occurred while updating stats.", ephemeral=True)


# MAIN VIEW

class ServerStatsView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, days_back: int, selected_role_id: str = None):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.guild = guild
        self.current_days = days_back
        self.selected_role_id = selected_role_id
        self.show_time_buttons = False
        self._update_buttons()

    def _update_buttons(self):
        self.clear_items()

        self.add_item(ServerRoleSelectMenu(self.guild, self.selected_role_id))

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="server_refresh",
            row=1
        )
        refresh_button.callback = self.refresh_callback
        self.add_item(refresh_button)

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings" if not self.show_time_buttons else "⏰ Hide Settings",
            custom_id="server_time_settings",
            row=1
        )
        time_settings_button.callback = self.time_settings_callback
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            days_7_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 7 else discord.ButtonStyle.secondary,
                label="7 Days",
                custom_id="server_days_7",
                row=2
            )
            days_7_button.callback = self.days_7_callback
            self.add_item(days_7_button)

            days_14_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 14 else discord.ButtonStyle.secondary,
                label="14 Days",
                custom_id="server_days_14",
                row=2
            )
            days_14_button.callback = self.days_14_callback
            self.add_item(days_14_button)

            days_30_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 30 else discord.ButtonStyle.secondary,
                label="30 Days",
                custom_id="server_days_30",
                row=2
            )
            days_30_button.callback = self.days_30_callback
            self.add_item(days_30_button)

            custom_label = f"Custom ({self.current_days}d)" if self.current_days not in [
                7, 14, 30] else "Custom"
            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=custom_label,
                custom_id="server_custom_days",
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
        modal = ServerTimeModal(
            self.cog, interaction.message.id, self.guild)
        await interaction.response.send_modal(modal)

    async def handle_button_click(self, interaction: discord.Interaction, refresh: bool = False, days: int = None):

        try:
            await interaction.response.defer()

            if days:
                self.current_days = days
                self.show_time_buttons = False

            stats_data = await self.cog.get_server_stats(
                self.guild.id, self.current_days, self.selected_role_id
            )
            if not stats_data:
                await interaction.followup.send("❌ Error retrieving server statistics", ephemeral=True)
                return

            image_stats_data = self.cog._convert_to_image_format(
                stats_data, self.guild
            )
            image = await create_user_stats_image(
                self.guild, image_stats_data, self.current_days, self.selected_role_id
            )

            self._update_buttons()
            img_bytes = io.BytesIO()
            image.save(img_bytes, format='PNG')
            img_bytes.seek(0)

            file = discord.File(img_bytes, filename="server_stats.png")
            await interaction.edit_original_response(attachments=[file], view=self)

        except Exception as e:
            print(f"Error handling button click: {e}")
            try:
                await interaction.followup.send("❌ An error occurred while updating the stats.", ephemeral=True)
            except:
                pass

    async def handle_time_settings(self, interaction: discord.Interaction):

        try:
            await interaction.response.defer()
            self.show_time_buttons = not self.show_time_buttons
            self._update_buttons()
            await interaction.edit_original_response(view=self)

        except Exception as e:
            print(f"Error handling time settings: {e}")
            try:
                await interaction.followup.send("❌ An error occurred while updating time settings.", ephemeral=True)
            except:
                pass


# INITIALIZATION

class ServerStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.active_sessions = {}

    server_group = app_commands.Group(
        name="server",
        description="server statistics commands"
    )

    def _get_db_cog(self):
        return self.bot.get_cog('DatabaseStats')

    def _convert_to_image_format(self, stats_data_db, guild):

        stats_data_image = {
            'top_emojis': [],
            'total_messages': stats_data_db.get('total_messages', 0),
            'total_seconds': stats_data_db.get('total_seconds', 0),
            'messages_over_time': stats_data_db.get('messages_over_time', {}),
            'voice_over_time': stats_data_db.get('voice_over_time', {}),
            'top_users_messages': [],
            'top_users_voice': [],
            'top_text_channels': [],
            'top_voice_channels': []
        }

        emojis_data = stats_data_db.get('top_emojis', [])
        for i, emoji_data in enumerate(emojis_data[:3]):
            stats_data_image['top_emojis'].append({
                'emoji': emoji_data.get('emoji_str', '❓'),
                'uses': emoji_data.get('usage_count', 0)
            })

        top_users_messages = stats_data_db.get('top_users_messages', [])

        for i in range(3):
            if i < len(top_users_messages):
                user_data = top_users_messages[i]
                user_id = user_data.get('user_id')

                if user_id:
                    try:
                        user_id_int = int(user_id)
                        member = guild.get_member(user_id_int)

                        if member:
                            username = member.name
                        else:
                            username = f"User {user_id_int}"
                    except (ValueError, TypeError):
                        username = f"User {user_id}"
                    message_count = user_data.get('message_count', 0)
                else:
                    username = "-"
                    message_count = 0
            else:
                username = "-"
                message_count = 0

            stats_data_image['top_users_messages'].append({
                'name': username,
                'messages': message_count,
                'rank': i + 1
            })

        top_users_voice = stats_data_db.get('top_users_voice', [])

        for i in range(3):
            if i < len(top_users_voice):
                user_data = top_users_voice[i]
                user_id = user_data.get('user_id', 0)

                try:
                    user_id_int = int(user_id)
                    member = guild.get_member(user_id_int)

                    if member:
                        username = member.name
                    else:
                        username = f"User {user_id_int}"
                except (ValueError, TypeError):
                    username = f"User {user_id}"

                voice_seconds = user_data.get('total_seconds', 0)
            else:
                username = "-"
                voice_seconds = 0

            stats_data_image['top_users_voice'].append({
                'name': username,
                'voice_time': voice_seconds,
                'rank': i + 1
            })

        top_text_channels = stats_data_db.get('top_text_channels', [])

        for i in range(3):
            if i < len(top_text_channels):
                channel_data = top_text_channels[i]
                channel_id = channel_data.get('channel_id', 0)
                if channel_id:
                    try:
                        channel_id_int = int(channel_id)
                        channel = guild.get_channel(channel_id_int)
                        channel_name = channel.name if channel else "[deleted channel]"
                    except (ValueError, TypeError):
                        channel_name = "[deleted channel]"
                else:
                    channel_name = "-"

                messages = channel_data.get('message_count', 0)
            else:
                channel_name = "-"
                messages = 0

            stats_data_image['top_text_channels'].append({
                'name': channel_name,
                'messages': messages,
                'rank': i + 1
            })

        top_voice_channels = stats_data_db.get('top_voice_channels', [])

        for i in range(3):
            if i < len(top_voice_channels):
                channel_data = top_voice_channels[i]
                channel_id = channel_data.get('channel_id', 0)
                if channel_id:
                    try:
                        channel_id_int = int(channel_id)
                        channel = guild.get_channel(channel_id_int)
                        channel_name = channel.name if channel else "[deleted channel]"
                    except (ValueError, TypeError):
                        channel_name = "[deleted channel]"
                else:
                    channel_name = "-"

                voice_time = channel_data.get('total_seconds', 0)
            else:
                channel_name = "-"
                voice_time = 0

            stats_data_image['top_voice_channels'].append({
                'name': channel_name,
                'voice_time': voice_time,
                'rank': i + 1
            })

        return stats_data_image

    # QUERY FUNCTION

    async def get_server_stats(self, guild_id: int, days_back: int, role_id: str = None):

        db_cog = self._get_db_cog()
        if not db_cog:
            print("DatabaseStats cog not found")
            return None

        try:
            role_ids = None
            if role_id and role_id != "none":
                try:
                    role_ids = [int(role_id)]
                except (ValueError, TypeError):
                    role_ids = None
                    print(f"Invalid role_id: {role_id}")

            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=days_back)

            stats_data = {
                'total_messages': 0,
                'total_seconds': 0,
                'messages_over_time': {},
                'voice_over_time': {},
                'top_emojis': [],
                'top_users_messages': [],
                'top_users_voice': [],
                'top_text_channels': [],
                'top_voice_channels': []
            }

            messages_result = await db_cog.q_server_total_messages(
                guild_id=guild_id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            stats_data['total_messages'] = messages_result.get(
                'total_messages', 0)

            voice_result = await db_cog.q_server_total_voice(
                guild_id=guild_id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            stats_data['total_seconds'] = voice_result.get('total_seconds', 0)

            messages_over_time = await db_cog.q_server_timeseries_messages_1d_5d_10d_20d_30d(
                guild_id=guild_id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            stats_data['messages_over_time'] = messages_over_time

            voice_over_time = await db_cog.q_server_timeseries_voice_1d_5d_10d_20d_30d(
                guild_id=guild_id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            stats_data['voice_over_time'] = voice_over_time

            top_emojis = await db_cog.q_server_top3_emojis(
                guild_id=guild_id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            stats_data['top_emojis'] = top_emojis

            top_users_messages = await db_cog.q_server_top3_users_messages(
                guild_id=guild_id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            stats_data['top_users_messages'] = top_users_messages

            top_users_voice = await db_cog.q_server_top3_users_voice(
                guild_id=guild_id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            stats_data['top_users_voice'] = top_users_voice

            top_text_channels = await db_cog.q_server_top3_text_channels(
                guild_id=guild_id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            stats_data['top_text_channels'] = top_text_channels

            top_voice_channels = await db_cog.q_server_top3_voice_channels(
                guild_id=guild_id,
                role_filter_ids=role_ids,
                start_time=start_time,
                end_time=end_time
            )
            stats_data['top_voice_channels'] = top_voice_channels

            stats_data['timestamp'] = end_time.isoformat()

            return stats_data

        except Exception as e:
            print(f"Error getting server stats: {e}")
            traceback.print_exc()
            return None

    # COMMAND

    @server_group.command(name="stats", description="Display the stats of the server")
    async def server_stats(self, interaction: discord.Interaction):
        await interaction.response.defer()

        stats_data = await self.get_server_stats(interaction.guild.id, 14)
        if not stats_data:
            await interaction.followup.send("❌ Error retrieving server statistics from database.")
            return

        image_stats_data = self._convert_to_image_format(
            stats_data, interaction.guild)
        image = await create_user_stats_image(interaction.guild, image_stats_data, 14)

        img_bytes = io.BytesIO()
        image.save(img_bytes, format='PNG')
        img_bytes.seek(0)

        file = discord.File(img_bytes, filename='server_stats.png')

        view = ServerStatsView(self, interaction.guild, days_back=14)

        message = await interaction.followup.send(file=file, view=view)

        self.active_sessions[message.id] = {
            'guild_id': interaction.guild.id,
            'user_id': interaction.user.id,
            'current_days': 14,
            'selected_role_id': None,
            'view': view,
            'message': message
        }


# SETUP

async def setup(bot):
    await bot.add_cog(ServerStats(bot))
