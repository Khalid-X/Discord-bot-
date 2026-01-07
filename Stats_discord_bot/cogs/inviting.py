import json
import aiohttp
import io
from PIL import Image, ImageDraw, ImageFont
from typing import Optional, List, Tuple, Dict, Any
import os
import asyncpg
from datetime import datetime, timedelta, timezone
from discord import app_commands
from discord.ext import commands
import discord
from pathlib import Path


# CONFIGURATION

BASE_DIR = Path(__file__).resolve().parent.parent


class InviteConfig:

    USERS_PER_PAGE = 10
    MAX_ROLES_IN_SELECT = 25
    DEFAULT_DAYS_BACK = 30
    TIME_PERIODS = [7, 14, 30]
    MAX_CUSTOM_DAYS = 2000
    TEMPLATE_PATH = BASE_DIR / "assets" / "images" / "leaderboards final png.png"
    FONT_PATH = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"


# FONTS

def get_fonts():

    try:
        custom_font_path = InviteConfig.FONT_PATH
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


# IMAGE GENERATION

async def generate_invite_leaderboard_image(
    guild: discord.Guild,
    leaderboard_data: list,
    days_back: int,
    role_id: str = None,
    page: int = 0
):

    template_path = InviteConfig.TEMPLATE_PATH
    try:
        image = Image.open(template_path)
    except FileNotFoundError:
        image = Image.new('RGB', (800, 600), color='#2F3136')

    if image.mode != "RGB":
        image = image.convert("RGB")

    draw = ImageDraw.Draw(image)
    font_small, font_medium, font_large, font_larger, font_huge, font_giant = get_fonts()

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

    # Profile picture at (7, 10)
    avatar_x, avatar_y = 7, 10
    avatar_size = (60, 60)

    # Server name starting position
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
        print(f"❌ Could not add server icon: {e}")

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

        stroke_width = 1
        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw.text((text_pos[0] + dx, text_pos[1] + dy),
                              fitted_text, font=text_font, fill="black")

        draw.text(text_pos, fitted_text, font=text_font, fill="white")
    else:

        draw_text_with_stroke(draw, (text_start_x, text_start_y), server_name,
                              font_huge, "white", "black", 2)

    # ROLE FILTER

    role_text = "No Filter"
    if role_id and role_id != "none":
        role = guild.get_role(int(role_id))
        role_text = role.name if role else "Unknown Role"
    draw_text_with_stroke(draw, (70, 424), role_text,
                          font_small, "white", "black", 1)

    # CREATED ON

    draw_text_with_stroke(
        draw, (630, 422), f"{days_back} days", font_small, "white", "black", 1)
    draw_text_with_stroke(draw, (550, 38), datetime.now().strftime(
        "%B %d, %Y"), font_small, "white", "black", 1)

    # PAGINATION
    if leaderboard_data:
        total_users = len(leaderboard_data)
        users_per_page = InviteConfig.USERS_PER_PAGE
        total_pages = (total_users + users_per_page - 1) // users_per_page
        draw_text_with_stroke(
            draw, (400, 450), f"Page {page + 1}/{total_pages}", font_medium, "white", "black", 1)
    else:
        total_pages = 1

    # LEADERBOARD CONTENT
    if leaderboard_data:
        display_data = []

        for user_data in leaderboard_data:
            user_id = user_data['user_id']
            name = None
            try:
                member = guild.get_member(int(user_id))
                if member:
                    name = member.name
                else:
                    name = f"User {user_id} (Left)"
            except Exception as e:
                print(f"⚠️ Error resolving user ID {user_id}: {e}")
                name = f"Unknown ({user_id})"

            invite_count = user_data.get('valid_invites', 0)

            display_data.append((name, invite_count))

        start_idx = page * InviteConfig.USERS_PER_PAGE
        end_idx = min(start_idx + InviteConfig.USERS_PER_PAGE,
                      len(display_data))
        page_data = display_data[start_idx:end_idx]

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

        for i, ((name, invite_count), (box_x, box_y)) in enumerate(zip(page_data, positions)):
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

            value_text = f"{invite_count:,} invites"

            rank_text = f"#{global_rank}"
            rank_bbox = draw.textbbox((0, 0), rank_text, font=font_medium)
            rank_width = rank_bbox[2] - rank_bbox[0]

            value_bbox = draw.textbbox((0, 0), value_text, font=font_medium)
            value_width = value_bbox[2] - value_bbox[0]

            available_name_width = box_width - placement_width - value_width - 20

            display_name = name
            name_bbox = draw.textbbox((0, 0), display_name, font=font_medium)
            name_width = name_bbox[2] - name_bbox[0]

            if name_width > available_name_width:
                try:
                    smaller_font = ImageFont.truetype(font_medium.path, size=16) if hasattr(
                        font_medium, 'path') else font_small
                    smaller_bbox = draw.textbbox(
                        (0, 0), display_name, font=smaller_font)
                    smaller_width = smaller_bbox[2] - smaller_bbox[0]

                    if smaller_width <= available_name_width:
                        name_font = smaller_font
                    else:
                        name_font = font_medium
                        while len(display_name) > 3:
                            display_name = display_name[:-4] + "..."
                            truncated_bbox = draw.textbbox(
                                (0, 0), display_name, font=name_font)
                            truncated_width = truncated_bbox[2] - \
                                truncated_bbox[0]
                            if truncated_width <= available_name_width:
                                break
                except:
                    name_font = font_medium
                    while len(display_name) > 3:
                        display_name = display_name[:-4] + "..."
                        truncated_bbox = draw.textbbox(
                            (0, 0), display_name, font=name_font)
                        truncated_width = truncated_bbox[2] - truncated_bbox[0]
                        if truncated_width <= available_name_width:
                            break
            else:
                name_font = font_medium

            rank_height = rank_bbox[3] - rank_bbox[1]
            name_height = name_bbox[3] - name_bbox[1] if name_font == font_medium else draw.textbbox(
                (0, 0), display_name, font=name_font)[3] - draw.textbbox((0, 0), display_name, font=name_font)[1]
            value_height = value_bbox[3] - value_bbox[1]

            rank_y = box_y + (box_height - rank_height) // 2
            name_y = box_y + (box_height - name_height) // 2
            value_y = box_y + (box_height - value_height) // 2

            rank_x = box_x + (placement_width - rank_width) // 2
            draw_text_with_stroke(draw, (rank_x, rank_y),
                                  rank_text, font_medium, "white", "black", 1)

            name_x = box_x + placement_width + 8
            draw_text_with_stroke(draw, (name_x, name_y),
                                  display_name, name_font, "white", "black", 1)

            value_x = box_x + box_width - value_width - 8
            draw_text_with_stroke(draw, (value_x, value_y),
                                  value_text, font_medium, "white", "black", 1)
    else:
        cx, cy = image.size[0] // 2, image.size[1] // 2
        draw_text_centered_with_stroke(
            draw, "NO DATA AVAILABLE", (cx, cy), font_giant, "white", "black", 3)

    img_bytes = io.BytesIO()
    image.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    return img_bytes


# MODAL CLASSES

class InviteLeaderboardTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, cog_instance, current_view):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.current_view = current_view

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
            if days <= 0 or days > InviteConfig.MAX_CUSTOM_DAYS:
                await interaction.response.send_message(f"❌ Please enter a number between 1 and {InviteConfig.MAX_CUSTOM_DAYS} days.", ephemeral=True)
                return

            await interaction.response.defer(thinking=True)

            self.current_view.current_days = days
            self.current_view.show_time_buttons = False
            self.current_view.page = 0
            self.current_view._update_buttons()

            img_bytes = await self.current_view.generate_image()

            try:
                file = discord.File(
                    img_bytes, filename="invite_leaderboard.png")
                await interaction.edit_original_response(attachments=[file], view=self.current_view)
            except Exception as e:
                print(f"Error editing original response: {e}")
                file = discord.File(
                    img_bytes, filename="invite_leaderboard.png")
                await interaction.followup.edit_message(interaction.message.id, attachments=[file], view=self.current_view)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            print(f"Error in invite modal submit: {e}")
            await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)


# ROLE DROPDOWN MENU

class InviteRoleSelectMenu(discord.ui.Select):
    def __init__(self, guild: discord.Guild, current_role_id: str = None):
        self.guild = guild

        roles = [role for role in guild.roles if role.name != "@everyone"]
        roles.sort(key=lambda x: x.position, reverse=True)
        roles = roles[:InviteConfig.MAX_ROLES_IN_SELECT]

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
            custom_id="invite_role_filter_select",
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


# PAGINATION BUTTONS

class InvitePrevButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="⬅️", custom_id="invite_prev")

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


class InviteNextButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary,
                         label="➡️", custom_id="invite_next")

    async def callback(self, interaction: discord.Interaction):

        if self.disabled:
            await interaction.response.defer(ephemeral=True, thinking=False)
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=False, thinking=False)

        view = self.view
        view.page += 1
        await view.update_message(interaction)


class InvitePageIndicator(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.primary, label="Page 1/1",
                         custom_id="invite_page", disabled=True)


# INVITE LEADERBOARD VIEW

class InviteLeaderboardView(discord.ui.View):
    def __init__(self, cog_instance, guild: discord.Guild, days_back: int, page: int = 0, selected_role_id: str = None):
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
        self.add_item(InviteRoleSelectMenu(self.guild, self.selected_role_id))

        self.add_item(InvitePrevButton())

        self.add_item(InvitePageIndicator())
        self.add_item(InviteNextButton())

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="invite_refresh",
            row=2
        )
        refresh_button.callback = self.refresh_callback

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="invite_time_settings",
            row=2
        )
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)
        if self.show_time_buttons:
            row = 3
            for days in InviteConfig.TIME_PERIODS:
                days_button = discord.ui.Button(
                    style=discord.ButtonStyle.primary if self.current_days == days else discord.ButtonStyle.secondary,
                    label=f"{days} Days",
                    custom_id=f"invite_days_{days}",
                    row=row
                )
                days_button.callback = self.create_days_callback(days)
                self.add_item(days_button)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in InviteConfig.TIME_PERIODS else "Custom",
                custom_id="invite_custom_days",
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

        modal = InviteLeaderboardTimeModal(self.cog, self)
        await interaction.response.send_modal(modal)

    async def generate_image(self):

        leaderboard_data = await self.cog._get_invite_leaderboard_data(self.guild, self.current_days, self.selected_role_id)

        return await generate_invite_leaderboard_image(
            guild=self.guild,
            leaderboard_data=leaderboard_data,
            days_back=self.current_days,
            role_id=self.selected_role_id,
            page=self.page
        )

    async def update_message(self, interaction: discord.Interaction):

        try:
            leaderboard_data = await self.cog._get_invite_leaderboard_data(self.guild, self.current_days, self.selected_role_id)
            total_users = len(leaderboard_data)
            total_pages = max(
                1, (total_users + InviteConfig.USERS_PER_PAGE - 1) // InviteConfig.USERS_PER_PAGE)

            if self.page >= total_pages:
                self.page = max(0, total_pages - 1)
            img_bytes = await self.generate_image()

            for child in self.children:
                if isinstance(child, InvitePageIndicator):
                    child.label = f"Page {self.page + 1}/{total_pages}"
                elif isinstance(child, InvitePrevButton):
                    child.disabled = (self.page == 0 or total_pages <= 1)
                elif isinstance(child, InviteNextButton):
                    child.disabled = (
                        self.page >= total_pages - 1 or total_pages <= 1)

            file = discord.File(img_bytes, filename="invite_leaderboard.png")

            if interaction.response.is_done():
                await interaction.edit_original_response(attachments=[file], view=self)
            else:
                await interaction.response.edit_message(attachments=[file], view=self)

        except Exception as e:
            print(f"Error updating invite leaderboard message: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message("❌ An error occurred while updating the leaderboard.", ephemeral=True)
                else:
                    await interaction.followup.send("❌ An error occurred while updating the leaderboard.", ephemeral=True)
            except:
                pass


# MAIN CLASS

class Invites(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.initialized = False
        self.db_stats = None
        self.active_views = {}

    async def cog_load(self):

        self.db_stats = self.bot.get_cog('DatabaseStats')
        self.initialized = True

    # HELPER METHODS

    async def _get_invite_leaderboard_data(self, guild: discord.Guild, days_back: int, role_id: str = None):

        if not self.db_stats or not self.db_stats.pool:
            print("⚠️ DatabaseStats not available for invite leaderboard")
            return []

        try:
            role_filter_ids = None
            if role_id and role_id != "none":
                role = guild.get_role(int(role_id))
                if role:
                    role_filter_ids = [role.id]

            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days_back)

            leaderboard_data = await self.db_stats.q_invite_leaderboard(
                guild_id=guild.id,
                limit=100,
                role_filter_ids=role_filter_ids,
                start_time=start_time,
                end_time=end_time
            )

            return leaderboard_data

        except Exception as e:
            print(f"❌ Error getting invite leaderboard: {e}")
            import traceback
            traceback.print_exc()
            return []

    # QUERY FUNCTIONS

    async def get_user_invite_stats(self, guild_id: int, user_id: int, days_back: int = None):

        if not self.db_stats:
            return None

        try:
            start_time = None
            end_time = None
            if days_back:
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(days=days_back)

            return await self.db_stats.q_user_invite_stats(
                guild_id=guild_id,
                user_id=user_id,
                start_time=start_time,
                end_time=end_time
            )
        except Exception as e:
            print(f"Error getting user invite stats: {e}")
            return None

    async def get_inviter_for_user(self, guild_id: int, user_id: int):

        if not self.db_stats or not self.db_stats.pool:
            return None

        try:
            async with self.db_stats.pool.acquire() as conn:

                row = await conn.fetchrow('''
                    SELECT 
                        inviter_id, 
                        invite_code, 
                        created_at, 
                        invite_type
                    FROM invite_tracking
                    WHERE guild_id = $1 
                    AND invitee_id = $2
                    AND invite_type = 'valid'  -- Fixed: added quotes
                    ORDER BY created_at DESC
                    LIMIT 1
                ''', guild_id, user_id)

                if row:
                    return {
                        'inviter_id': row['inviter_id'],
                        'invite_code': row['invite_code'],
                        'invite_time': row['created_at'],
                        'invite_type': row['invite_type']
                    }

                row = await conn.fetchrow('''
                    SELECT 
                        inviter_id, 
                        invite_code, 
                        created_at, 
                        invite_type
                    FROM invite_tracking
                    WHERE guild_id = $1 
                    AND invitee_id = $2
                    ORDER BY created_at DESC
                    LIMIT 1
                ''', guild_id, user_id)

                if row:
                    return {
                        'inviter_id': row['inviter_id'],
                        'invite_code': row['invite_code'],
                        'invite_time': row['created_at'],
                        'invite_type': row['invite_type']
                    }

                return None

        except Exception as e:
            print(f"Error getting inviter for user: {e}")
            return None

    # COMMANDS

    invite_group = app_commands.Group(
        name="invite",
        description="Invite tracking commands"
    )

    @invite_group.command(name="stats", description="Show invite statistics for a user")
    @app_commands.describe(user="The user to check invite stats for")
    async def invite_stats(self, interaction: discord.Interaction, user: discord.Member):

        await interaction.response.defer(thinking=True)

        if not self.db_stats or not self.db_stats.pool:
            await interaction.followup.send("❌ Database not available. Please try again later.")
            return

        try:
            stats = await self.get_user_invite_stats(interaction.guild.id, user.id)

            if not stats:
                await interaction.followup.send("❌ No invite statistics found for this user.")
                return

            embed = discord.Embed(
                title=f"📊 Invite Statistics - {user.display_name}",
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=user.display_avatar.url)

            embed.add_field(
                name="Total Invites",
                value=f"```{stats.get('total_invites', 0):,}```",
                inline=True
            )
            embed.add_field(
                name="✅ Valid Invites",
                value=f"```{stats.get('valid_invites', 0):,}```",
                inline=True
            )
            embed.add_field(
                name="⚠️ Suspicious Invites",
                value=f"```{stats.get('suspicious_invites', 0):,}```",
                inline=True
            )

            if stats.get('left_invites', 0) > 0:
                embed.add_field(
                    name="👋 Users Left",
                    value=f"```{stats.get('left_invites', 0):,}```",
                    inline=True
                )

            if stats.get('percentage_valid', 0) > 0:
                embed.add_field(
                    name="🎯 Valid Invite Rate",
                    value=f"```{stats.get('percentage_valid', 0):.1f}%```",
                    inline=True
                )

            embed.set_footer(
                text="Suspicious invites are invites made to accounts less than 3 days old OR inviters whose account is less than 3 days old.")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            print(f"❌ Error getting invite stats: {e}")
            await interaction.followup.send("❌ An error occurred while fetching invite statistics.")

    @app_commands.command(name="inviter", description="Find out who invited a user")
    @app_commands.describe(user="The user to check inviter for")
    async def inviter_command(self, interaction: discord.Interaction, user: discord.Member):

        await interaction.response.defer(thinking=True)

        if not self.db_stats or not self.db_stats.pool:
            await interaction.followup.send("❌ Database not available. Please try again later.")
            return

        inviter_info = await self.get_inviter_for_user(interaction.guild.id, user.id)

        if not inviter_info:
            embed = discord.Embed(
                title="❌ Inviter Not Found",
                description=f"Could not find who invited {user.mention}.\n\nThis could be because:\n• The user joined before invite tracking was enabled\n• The invite was deleted before tracking\n• The user joined through a different method (vanity URL, etc.)",
                color=discord.Color.orange()
            )
            await interaction.followup.send(embed=embed)
            return

        inviter_id = inviter_info.get('inviter_id')
        invite_type = inviter_info.get('invite_type', 'valid')

        if inviter_id == 0:
            embed = discord.Embed(
                title="❌ Unknown Inviter",
                description=f"**{user.mention}** was invited by an unknown user.\n\nThis usually happens when:\n• The inviter left the server before tracking\n• The invite was created before tracking started\n• It was a vanity URL invite",
                color=discord.Color.orange()
            )
            await interaction.followup.send(embed=embed)
            return

        inviter = interaction.guild.get_member(inviter_id)

        if inviter:
            embed = discord.Embed(
                title="👥 Inviter Found",
                description=f"**{user.mention}** was invited by **{inviter.mention}**",
                color=discord.Color.green()
            )
            embed.set_thumbnail(url=user.display_avatar.url)
            embed.add_field(
                name="Invited User",
                value=user.mention,
                inline=True
            )
            embed.add_field(
                name="Inviter",
                value=inviter.mention,
                inline=True
            )

            if inviter_info.get('invite_code') and inviter_info['invite_code'] not in ["unknown", "left"]:
                embed.add_field(
                    name="Invite Code",
                    value=f"`{inviter_info['invite_code']}`",
                    inline=False
                )

            if inviter_info.get('invite_time'):
                invite_time = inviter_info['invite_time']
                if isinstance(invite_time, datetime):
                    invite_time_str = invite_time.strftime("%Y-%m-%d %H:%M")
                else:
                    invite_time_str = str(invite_time)
                embed.add_field(
                    name="Invite Time",
                    value=f"`{invite_time_str}`",
                    inline=False
                )
            if invite_type == 'suspicious':
                embed.add_field(
                    name="⚠️ Warning",
                    value="This invite was marked as suspicious (new account detected)",
                    inline=False
                )
                embed.color = discord.Color.orange()
            elif invite_type == 'left':
                embed.add_field(
                    name="📝 Note",
                    value="The invited user has left the server",
                    inline=False
                )
                embed.color = discord.Color.blue()
            elif invite_type == 'unknown':
                embed.add_field(
                    name="📝 Note",
                    value="Invite source could not be determined",
                    inline=False
                )
                embed.color = discord.Color.greyple()

        else:
            embed = discord.Embed(
                title="👥 Inviter Found",
                description=f"**{user.mention}** was invited by **User {inviter_id}**",
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=user.display_avatar.url)
            embed.add_field(
                name="Invited User",
                value=user.mention,
                inline=True
            )
            embed.add_field(
                name="Inviter",
                value=f"User {inviter_id} (Left Server)",
                inline=True
            )

            if invite_type == 'suspicious':
                embed.add_field(
                    name="⚠️ Warning",
                    value="This invite was marked as suspicious",
                    inline=False
                )
                embed.color = discord.Color.orange()

        await interaction.followup.send(embed=embed)

    @invite_group.command(name="leaderboard", description="Show invite leaderboard")
    async def invite_leaderboard(self, interaction: discord.Interaction):

        await interaction.response.defer()

        leaderboard_data = await self._get_invite_leaderboard_data(interaction.guild, InviteConfig.DEFAULT_DAYS_BACK)

        img_bytes = await generate_invite_leaderboard_image(
            guild=interaction.guild,
            leaderboard_data=leaderboard_data,
            days_back=InviteConfig.DEFAULT_DAYS_BACK,
            page=0
        )

        file = discord.File(img_bytes, filename="invite_leaderboard.png")

        view = InviteLeaderboardView(
            self, interaction.guild, days_back=InviteConfig.DEFAULT_DAYS_BACK
        )

        await interaction.followup.send(file=file, view=view)

        message = await interaction.original_response()
        self.active_views[message.id] = view

    # CLEANUP

    async def cog_unload(self):
        self.active_views.clear()


# SETUP

async def setup(bot):
    await bot.add_cog(Invites(bot))
