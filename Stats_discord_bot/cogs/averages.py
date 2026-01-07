import os
import discord
from discord import app_commands
from discord.ext import commands
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import io
import textwrap
import aiohttp
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


# PER DAY/WEEK/MONTH DROPDOWN MENU

class TimePeriodSelect(discord.ui.Select):
    def __init__(self, current_period: str = "day"):
        options = [
            discord.SelectOption(
                label="Per Day",
                value="day",
                description="Show averages per day (x/day)",
                emoji="📅",
                default=(current_period == "day")
            ),
            discord.SelectOption(
                label="Per Week",
                value="week",
                description="Show averages per week (x/week)",
                emoji="📆",
                default=(current_period == "week")
            ),
            discord.SelectOption(
                label="Per Month",
                value="month",
                description="Show averages per month (x/month)",
                emoji="🗓️",
                default=(current_period == "month")
            )
        ]

        super().__init__(
            placeholder="Select display format...",
            options=options,
            custom_id="time_period_select",
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if hasattr(view, 'selected_period'):
            view.selected_period = self.values[0]
            await interaction.response.defer()

            try:
                if hasattr(view, 'is_user_stats') and view.is_user_stats:

                    image_file = await view.cog.generate_user_stats_image(
                        interaction,
                        view.current_days,
                        view.user_id,
                        view.selected_period
                    )
                else:

                    image_file = await view.cog.generate_server_stats_image(
                        interaction,
                        view.current_days,
                        view.selected_role_id,
                        view.selected_period
                    )

                if interaction.response.is_done():
                    await interaction.edit_original_response(attachments=[image_file], view=view)
                else:
                    await interaction.response.edit_message(attachments=[image_file], view=view)

            except Exception as e:
                print(f"Error in time period select: {e}")
                await interaction.followup.send("❌ An error occurred while updating the image.", ephemeral=True)


# ROLE DROPDOWN MENU

class AverageRoleSelectMenu(discord.ui.Select):
    def __init__(self, guild: discord.Guild, current_role_id: str = None):
        self.guild = guild

        roles = [role for role in guild.roles if role.name != "@everyone"]
        roles.sort(key=lambda x: x.position, reverse=True)
        roles = roles[:24]

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
            custom_id="average_role_filter_select",
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if hasattr(view, 'selected_role_id'):
            view.selected_role_id = self.values[0]
            await interaction.response.defer()

            try:
                image_file = await view.cog.generate_server_stats_image(
                    interaction,
                    view.current_days,
                    view.selected_role_id,
                    view.selected_period
                )

                if interaction.response.is_done():
                    await interaction.edit_original_response(attachments=[image_file], view=view)
                else:
                    await interaction.response.edit_message(attachments=[image_file], view=view)

            except Exception as e:
                print(f"Error in role select: {e}")
                await interaction.followup.send("❌ An error occurred while updating the image.", ephemeral=True)


# TIME MODAL

class AverageTimeModal(discord.ui.Modal, title='Custom Time Period'):
    def __init__(self, cog_instance, current_days: int, selected_role_id: str = None, selected_period: str = "day", is_user_stats: bool = False, user_id: int = None):
        super().__init__(timeout=300)
        self.cog = cog_instance
        self.current_days = current_days
        self.selected_role_id = selected_role_id
        self.selected_period = selected_period
        self.is_user_stats = is_user_stats
        self.user_id = user_id

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

            if self.is_user_stats:
                image_file = await self.cog.generate_user_stats_image(interaction, days, self.user_id, self.selected_period)
                view = AverageUserStatsView(
                    self.cog, days, self.user_id, self.selected_period)
            else:
                image_file = await self.cog.generate_server_stats_image(interaction, days, self.selected_role_id, self.selected_period)
                view = AverageServerStatsView(
                    self.cog, days, self.selected_role_id, self.selected_period)

            await interaction.edit_original_response(attachments=[image_file], view=view)

        except ValueError:
            await interaction.response.send_message("❌ Please enter a valid number.", ephemeral=True)
        except Exception as e:
            print(f"Error in average modal submit: {e}")
            await interaction.followup.send("❌ An error occurred while generating image.", ephemeral=True)


# SERVER STATS VIEW

class AverageServerStatsView(discord.ui.View):
    def __init__(self, cog_instance, current_days: int, selected_role_id: str = None, selected_period: str = "day"):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.current_days = current_days
        self.selected_role_id = selected_role_id
        self.selected_period = selected_period
        self.show_time_buttons = False
        self.is_user_stats = False
        self.user_id = None
        self._update_buttons()

    def _update_buttons(self):
        self.clear_items()

        self.add_item(TimePeriodSelect(self.selected_period))
        self.add_item(AverageRoleSelectMenu(
            self.cog.current_guild, self.selected_role_id))

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="average_refresh"
        )
        refresh_button.callback = self.refresh_callback

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="average_time_settings"
        )
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            days_7_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 7 else discord.ButtonStyle.secondary,
                label="7 Days",
                custom_id="average_days_7"
            )
            days_7_button.callback = self.create_days_callback(7)

            days_14_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 14 else discord.ButtonStyle.secondary,
                label="14 Days",
                custom_id="average_days_14"
            )
            days_14_button.callback = self.create_days_callback(14)

            days_30_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 30 else discord.ButtonStyle.secondary,
                label="30 Days",
                custom_id="average_days_30"
            )
            days_30_button.callback = self.create_days_callback(30)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in [
                    7, 14, 30] else "Custom",
                custom_id="average_custom_days"
            )
            custom_button.callback = self.custom_days_callback

            self.add_item(days_7_button)
            self.add_item(days_14_button)
            self.add_item(days_30_button)
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

        try:

            self.show_time_buttons = not self.show_time_buttons
            self._update_buttons()

            if interaction.response.is_done():
                await interaction.edit_original_response(view=self)
            else:
                await interaction.response.edit_message(view=self)

        except Exception as e:
            print(f"Error handling time settings: {e}")
            await interaction.followup.send("❌ An error occurred while updating time settings.", ephemeral=True)

    async def handle_time_period_change(self, interaction: discord.Interaction, days: int):

        try:
            self.current_days = days
            self.show_time_buttons = False
            await self.update_message(interaction)
        except Exception as e:
            print(f"Error handling time period change: {e}")
            await interaction.followup.send("❌ An error occurred while updating the image.", ephemeral=True)

    async def handle_refresh(self, interaction: discord.Interaction):

        try:
            await self.update_message(interaction)
        except Exception as e:
            print(f"Error handling refresh: {e}")
            await interaction.followup.send("❌ An error occurred while refreshing the image.", ephemeral=True)

    async def handle_custom_days(self, interaction: discord.Interaction):

        try:
            modal = AverageTimeModal(
                self.cog, self.current_days, self.selected_role_id, self.selected_period, False, None)
            await interaction.response.send_modal(modal)
        except Exception as e:
            print(f"Error handling custom days: {e}")
            await interaction.followup.send("❌ An error occurred while opening custom days modal.", ephemeral=True)

    async def update_message(self, interaction: discord.Interaction):

        try:
            image_file = await self.cog.generate_server_stats_image(
                interaction, self.current_days, self.selected_role_id, self.selected_period
            )
            self._update_buttons()

            if interaction.response.is_done():
                await interaction.edit_original_response(attachments=[image_file], view=self)
            else:
                await interaction.response.edit_message(attachments=[image_file], view=self)

        except Exception as e:
            print(f"Error updating average stats message: {e}")
            await interaction.followup.send("❌ An error occurred while updating the image.", ephemeral=True)


# USER STATS VIEW

class AverageUserStatsView(discord.ui.View):
    def __init__(self, cog_instance, current_days: int, user_id: int, selected_period: str = "day"):
        super().__init__(timeout=600)
        self.cog = cog_instance
        self.current_days = current_days
        self.user_id = user_id
        self.selected_period = selected_period
        self.show_time_buttons = False
        self.is_user_stats = True
        self.selected_role_id = None
        self._update_buttons()

    def _update_buttons(self):
        self.clear_items()

        self.add_item(TimePeriodSelect(self.selected_period))

        refresh_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="🔄 Refresh",
            custom_id="average_refresh"
        )
        refresh_button.callback = self.refresh_callback

        time_settings_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            label="⏰ Time Settings",
            custom_id="average_time_settings"
        )
        time_settings_button.callback = self.time_settings_callback

        self.add_item(refresh_button)
        self.add_item(time_settings_button)

        if self.show_time_buttons:
            days_7_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 7 else discord.ButtonStyle.secondary,
                label="7 Days",
                custom_id="average_days_7"
            )
            days_7_button.callback = self.create_days_callback(7)

            days_14_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 14 else discord.ButtonStyle.secondary,
                label="14 Days",
                custom_id="average_days_14"
            )
            days_14_button.callback = self.create_days_callback(14)

            days_30_button = discord.ui.Button(
                style=discord.ButtonStyle.primary if self.current_days == 30 else discord.ButtonStyle.secondary,
                label="30 Days",
                custom_id="average_days_30"
            )
            days_30_button.callback = self.create_days_callback(30)

            custom_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                label=f"Custom ({self.current_days}d)" if self.current_days not in [
                    7, 14, 30] else "Custom",
                custom_id="average_custom_days"
            )
            custom_button.callback = self.custom_days_callback

            self.add_item(days_7_button)
            self.add_item(days_14_button)
            self.add_item(days_30_button)
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

        try:

            self.show_time_buttons = not self.show_time_buttons
            self._update_buttons()

            if interaction.response.is_done():
                await interaction.edit_original_response(view=self)
            else:
                await interaction.response.edit_message(view=self)

        except Exception as e:
            print(f"Error handling time settings: {e}")
            await interaction.followup.send("❌ An error occurred while updating time settings.", ephemeral=True)

    async def handle_time_period_change(self, interaction: discord.Interaction, days: int):

        try:
            self.current_days = days
            self.show_time_buttons = False
            await self.update_message(interaction)
        except Exception as e:
            print(f"Error handling time period change: {e}")
            await interaction.followup.send("❌ An error occurred while updating the image.", ephemeral=True)

    async def handle_refresh(self, interaction: discord.Interaction):

        try:
            await self.update_message(interaction)
        except Exception as e:
            print(f"Error handling refresh: {e}")
            await interaction.followup.send("❌ An error occurred while refreshing the image.", ephemeral=True)

    async def handle_custom_days(self, interaction: discord.Interaction):

        try:
            modal = AverageTimeModal(
                self.cog, self.current_days, None, self.selected_period, True, self.user_id)
            await interaction.response.send_modal(modal)
        except Exception as e:
            print(f"Error handling custom days: {e}")
            await interaction.followup.send("❌ An error occurred while opening custom days modal.", ephemeral=True)

    async def update_message(self, interaction: discord.Interaction):

        try:
            image_file = await self.cog.generate_user_stats_image(
                interaction, self.current_days, self.user_id, self.selected_period
            )
            self._update_buttons()

            if interaction.response.is_done():
                await interaction.edit_original_response(attachments=[image_file], view=self)
            else:
                await interaction.response.edit_message(attachments=[image_file], view=self)

        except Exception as e:
            print(f"Error updating average stats message: {e}")
            await interaction.followup.send("❌ An error occurred while updating the image.", ephemeral=True)


# INITIALIZATION

class AverageStats(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_cog = None
        self.activity_cog = None
        self.current_guild = None
        self.active_sessions = {}

        self.font_path = BASE_DIR / "assets" / "fonts" / "HorndonD.ttf"
        self.server_background_path = BASE_DIR / "assets" / \
            "images" / "averages server final png.png"
        self.user_background_path = BASE_DIR / "assets" / \
            "images" / "averages user final png.png"

        self.COMPATIBILITY_UI_POSITIONS = {
            'avatar': {'x': 7, 'y': 7, 'size': (60, 60)},
            'text_start': {'x': 85, 'y': 27},
            'username_rectangle': {'center': (150, 27), 'width': 183, 'height': 35}
        }

    async def cog_load(self):

        self.db_cog = self.bot.get_cog('DatabaseStats')
        if not self.db_cog:
            print(
                "❌ DatabaseStats cog not found! AverageStats requires DatabaseStats to function.")
        else:
            print("✅ DatabaseStats cog loaded successfully")

        self.activity_cog = self.bot.get_cog('ActivityTracker')
        if not self.activity_cog:
            print(
                "⚠️ ActivityTracker cog not found! Activity stats will not be available.")
        else:
            print("✅ ActivityTracker cog loaded successfully")

    async def _filter_members_by_role(self, guild: discord.Guild, role_id: str):

        if role_id == "none" or not role_id:
            return None

        role = guild.get_role(int(role_id))
        if not role:
            return None

        return [str(member.id) for member in role.members]

    # FORMAT

    def _format_number_for_period(self, daily_average: float, period: str) -> str:

        if period == "day":
            return f"{daily_average:.1f}/day"
        elif period == "week":
            return f"{(daily_average * 7):.1f}/week"
        elif period == "month":
            return f"{(daily_average * 30):.1f}/month"
        return f"{daily_average:.1f}/day"

    # IMAGE HELPER FUNCTIONS
    def _draw_text_with_stroke(self, draw, position, text, font, fill_color, stroke_color, stroke_width=2, max_width=None):

        current_font = font
        text_width = draw.textlength(text, font=current_font)

        if max_width and text_width > max_width:

            font_size = int(font.size * 0.9)
            while font_size > 10 and text_width > max_width:
                current_font = ImageFont.truetype(self.font_path, font_size)
                text_width = draw.textlength(text, font=current_font)
                font_size = int(font_size * 0.9)

        x, y = position

        for dx in [-stroke_width, 0, stroke_width]:
            for dy in [-stroke_width, 0, stroke_width]:
                if dx != 0 or dy != 0:
                    draw.text((x + dx, y + dy), text,
                              font=current_font, fill=stroke_color)

        draw.text((x, y), text, font=current_font, fill=fill_color)

        return current_font

    def _fit_text_to_rectangle(self, draw, text, text_start_x, text_start_y, rect_center_x, rect_center_y, rect_width, rect_height, is_username=False):

        font_paths = [self.font_path]

        font_sizes = [32, 30, 28, 26, 24, 22, 20, 18, 17, 16, 15, 14, 13]

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

    async def _add_profile_pic_and_name(self, image, draw, interaction, target_name, is_user_stats, user=None):

        positions = self.COMPATIBILITY_UI_POSITIONS

        avatar_x, avatar_y = positions['avatar']['x'], positions['avatar']['y']
        avatar_size = positions['avatar']['size']

        text_start_x = positions['text_start']['x']
        text_start_y = positions['text_start']['y']

        username_rectangle = positions['username_rectangle']

        # PROFILE PICTURE
        try:
            if is_user_stats and user and user.avatar:
                # User profile picture
                icon_url = user.avatar.url
            elif not is_user_stats and interaction.guild.icon:
                # Server profile picture
                icon_url = interaction.guild.icon.url
            else:
                icon_url = None

            if icon_url:
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
            print(f"Could not add profile icon: {e}")

        fitted_text, text_font, text_pos = self._fit_text_to_rectangle(
            draw,
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

            try:
                font_huge = ImageFont.truetype(self.font_path, 40) if os.path.exists(
                    self.font_path) else ImageFont.load_default()
            except:
                font_huge = ImageFont.load_default()
            draw.text((text_start_x, text_start_y),
                      target_name, font=font_huge, fill="white")

    # QUERY FUNCTIONS

    async def generate_server_stats_image(self, interaction: discord.Interaction, days_back: int, role_id: str = None, period: str = "day") -> discord.File:

        guild_id = interaction.guild.id
        self.current_guild = interaction.guild
        now = datetime.utcnow()
        start_date = now - timedelta(days=days_back)

        role_filter_ids = None
        if role_id and role_id != "none":
            role_filter_ids = await self._filter_members_by_role(interaction.guild, role_id)
            if role_filter_ids:

                role_filter_ids = [int(uid) for uid in role_filter_ids]

        try:

            messages_result = await self.db_cog.q_server_total_messages(
                guild_id=guild_id,
                role_filter_ids=role_filter_ids,
                start_time=start_date,
                end_time=now
            )
            total_messages = messages_result.get('total_messages', 0)
            avg_messages_per_day = total_messages / days_back if days_back > 0 else 0

            emojis_result = await self.db_cog.q_emoji_server_leaderboard(
                guild_id=guild_id,
                role_filter_ids=role_filter_ids,
                start_time=start_date,
                end_time=now,
                limit=100
            )
            total_emojis = sum(emoji['usage_count'] for emoji in emojis_result)
            avg_emojis_per_day = total_emojis / days_back if days_back > 0 else 0

            voice_result = await self.db_cog.q_server_total_voice(
                guild_id=guild_id,
                role_filter_ids=role_filter_ids,
                start_time=start_date,
                end_time=now
            )
            total_voice_minutes = voice_result.get('total_seconds', 0) / 60
            avg_voice_per_day = total_voice_minutes / days_back if days_back > 0 else 0

            invites_result = await self.db_cog.q_invite_leaderboard(
                guild_id=guild_id,
                role_filter_ids=role_filter_ids,
                start_time=start_date,
                end_time=now,
                limit=100
            )
            total_invites = sum(invite['valid_invites']
                                for invite in invites_result)
            avg_invites_per_day = total_invites / days_back if days_back > 0 else 0

            total_activities = 0
            avg_activities_per_day = 0

            if self.activity_cog and self.activity_cog.db:
                try:

                    role_id_param = None
                    if role_id and role_id != "none":
                        role_id_param = int(role_id)

                    activity_stats = await self.activity_cog.db.get_server_activity_stats(
                        guild_id=guild_id,
                        days=days_back,
                        role_id=role_id_param,
                        sort_by="time"
                    )

                    for activity in activity_stats:
                        total_activities += activity.get('session_count', 0)

                    avg_activities_per_day = total_activities / days_back if days_back > 0 else 0

                except Exception as e:
                    print(f"Error getting activity stats: {e}")

                    if activity_stats:
                        total_activities = sum(activity.get(
                            'session_count', 0) for activity in activity_stats)
                        avg_activities_per_day = total_activities / days_back if days_back > 0 else 0
            else:
                print("ActivityTracker cog not available for activity stats")

            return await self._create_stats_image(
                interaction=interaction,
                days_back=days_back,
                period=period,
                messages_avg=avg_messages_per_day,
                total_messages=total_messages,
                voice_avg=avg_voice_per_day,
                total_voice=total_voice_minutes,
                emoji_avg=avg_emojis_per_day,
                total_emojis=total_emojis,
                invite_avg=avg_invites_per_day,
                total_invites=total_invites,
                activity_avg=avg_activities_per_day,
                total_activities=total_activities,
                is_user_stats=False,
                role_id=role_id
            )

        except Exception as e:
            print(f"Error generating server stats image: {e}")
            import traceback
            traceback.print_exc()
            return await self._create_error_image(interaction)

    async def generate_user_stats_image(self, interaction: discord.Interaction, days_back: int, user_id: int, period: str = "day") -> discord.File:

        guild_id = interaction.guild.id
        self.current_guild = interaction.guild
        now = datetime.utcnow()
        start_date = now - timedelta(days=days_back)

        try:

            messages_result = await self.db_cog.q_user_total_messages(
                guild_id=guild_id,
                user_id=user_id,
                start_time=start_date,
                end_time=now
            )
            total_messages = messages_result.get('total_messages', 0)
            avg_messages_per_day = total_messages / days_back if days_back > 0 else 0

            emojis_result = await self.db_cog.q_emoji_user_leaderboard(
                guild_id=guild_id,
                start_time=start_date,
                end_time=now,
                limit=100
            )

            user_emoji_usage = 0
            for user_data in emojis_result:
                if user_data['user_id'] == user_id:
                    user_emoji_usage = user_data['usage_count']
                    break

            total_emojis = user_emoji_usage
            avg_emojis_per_day = total_emojis / days_back if days_back > 0 else 0

            voice_result = await self.db_cog.q_user_total_voice(
                guild_id=guild_id,
                user_id=user_id,
                start_time=start_date,
                end_time=now
            )
            total_voice_minutes = voice_result.get('total_seconds', 0) / 60
            avg_voice_per_day = total_voice_minutes / days_back if days_back > 0 else 0

            invites_result = await self.db_cog.q_user_invite_stats(
                guild_id=guild_id,
                user_id=user_id,
                start_time=start_date,
                end_time=now
            )
            total_invites = invites_result.get('valid_invites', 0)
            avg_invites_per_day = total_invites / days_back if days_back > 0 else 0

            total_activities = 0
            avg_activities_per_day = 0

            if self.activity_cog and self.activity_cog.db:
                try:

                    activity_stats = await self.activity_cog.db.get_user_activity_stats(
                        guild_id=guild_id,
                        user_id=user_id,
                        days=days_back,
                        role_id=None,
                        sort_by="time"
                    )

                    for activity in activity_stats:
                        total_activities += activity.get('session_count', 0)

                    avg_activities_per_day = total_activities / days_back if days_back > 0 else 0

                except Exception as e:
                    print(f"Error getting user activity stats: {e}")

                    if activity_stats:
                        total_activities = sum(activity.get(
                            'session_count', 0) for activity in activity_stats)
                        avg_activities_per_day = total_activities / days_back if days_back > 0 else 0
            else:
                print("ActivityTracker cog not available for user activity stats")

            user = interaction.guild.get_member(user_id)
            username = user.name if user else f"User {user_id}"

            return await self._create_stats_image(
                interaction=interaction,
                days_back=days_back,
                period=period,
                messages_avg=avg_messages_per_day,
                total_messages=total_messages,
                voice_avg=avg_voice_per_day,
                total_voice=total_voice_minutes,
                emoji_avg=avg_emojis_per_day,
                total_emojis=total_emojis,
                invite_avg=avg_invites_per_day,
                total_invites=total_invites,
                activity_avg=avg_activities_per_day,
                total_activities=total_activities,
                is_user_stats=True,
                user_id=user_id,
                username=username
            )

        except Exception as e:
            print(f"Error generating user stats image: {e}")
            import traceback
            traceback.print_exc()
            return await self._create_error_image(interaction)

    # IMAGE GENERATION

    async def _create_stats_image(self, interaction: discord.Interaction, days_back: int, period: str,
                                  messages_avg: float, total_messages: int,
                                  voice_avg: float, total_voice: float,
                                  emoji_avg: float, total_emojis: int,
                                  invite_avg: float, total_invites: int,
                                  activity_avg: float, total_activities: int,
                                  is_user_stats: bool = False,
                                  user_id: int = None,
                                  username: str = "",
                                  role_id: str = None) -> discord.File:

        try:

            background_path = self.user_background_path if is_user_stats else self.server_background_path
            image = Image.open(background_path)
            draw = ImageDraw.Draw(image)

            try:
                font_huge = ImageFont.truetype(self.font_path, 40)
                font_medium = ImageFont.truetype(self.font_path, 30)
                font_small = ImageFont.truetype(self.font_path, 16)
            except Exception as font_error:
                print(f"Font loading error: {font_error}")

                font_huge = ImageFont.load_default()
                font_medium = ImageFont.load_default()
                font_small = ImageFont.load_default()

            if is_user_stats:
                target_name = username
                user_obj = interaction.guild.get_member(user_id)
            else:
                target_name = interaction.guild.name
                user_obj = None

            await self._add_profile_pic_and_name(
                image, draw, interaction, target_name, is_user_stats, user_obj
            )

            if is_user_stats:
                role_text = ""
            else:
                role_text = "No Filter"
                if role_id and role_id != "none":
                    role = interaction.guild.get_role(int(role_id))
                    role_text = role.name if role else "Unknown Role"

            self._draw_text_with_stroke(draw, (70, 424), role_text,
                                        font_small, "white", "black", 1)

            # Time period and created on
            self._draw_text_with_stroke(
                draw, (630, 424), f"{days_back} days", font_small, "white", "black", 1)
            self._draw_text_with_stroke(draw, (550, 38), datetime.now().strftime(
                "%B %d, %Y"), font_small, "white", "black", 1)

            # Format numbers based on period
            messages_formatted = self._format_number_for_period(
                messages_avg, period)
            voice_formatted = self._format_number_for_period(voice_avg, period)
            emoji_formatted = self._format_number_for_period(emoji_avg, period)
            invite_formatted = self._format_number_for_period(
                invite_avg, period)
            activity_formatted = self._format_number_for_period(
                activity_avg, period)

            # Messages
            self._draw_text_with_stroke(
                draw, (25, 130), messages_formatted, font_medium, "white", "black", max_width=200)
            self._draw_text_with_stroke(
                draw, (25, 165), f"{total_messages:,}", font_medium, "white", "black", max_width=200)

            # Voice
            self._draw_text_with_stroke(
                draw, (270, 130), voice_formatted, font_medium, "white", "black", max_width=200)
            self._draw_text_with_stroke(
                draw, (270, 165), f"{total_voice:.0f}", font_medium, "white", "black", max_width=200)

            # Emojis
            self._draw_text_with_stroke(
                draw, (515, 130), emoji_formatted, font_medium, "white", "black", max_width=200)
            self._draw_text_with_stroke(
                draw, (515, 165), f"{total_emojis:,}", font_medium, "white", "black", max_width=200)

            # Invites
            self._draw_text_with_stroke(
                draw, (100, 285), invite_formatted, font_medium, "white", "black", max_width=200)
            self._draw_text_with_stroke(
                draw, (100, 335), f"{total_invites:,}", font_medium, "white", "black", max_width=200)

            # Activities
            self._draw_text_with_stroke(
                draw, (405, 285), activity_formatted, font_medium, "white", "black", max_width=200)
            self._draw_text_with_stroke(
                draw, (405, 335), f"{total_activities:,}", font_medium, "white", "black", max_width=200)

            img_bytes = io.BytesIO()
            image.save(img_bytes, format='PNG')
            img_bytes.seek(0)

            filename = f"{'user' if is_user_stats else 'server'}_average_stats_{interaction.guild.id}.png"
            return discord.File(img_bytes, filename=filename)

        except Exception as e:
            print(f"Error creating image: {e}")
            import traceback
            traceback.print_exc()
            return await self._create_error_image(interaction)

    async def _create_error_image(self, interaction: discord.Interaction) -> discord.File:

        image = Image.new('RGB', (800, 600), color='blue')
        draw = ImageDraw.Draw(image)
        draw.text((50, 50), "Error generating statistics image", fill='white')

        img_bytes = io.BytesIO()
        image.save(img_bytes, format='PNG')
        img_bytes.seek(0)
        return discord.File(img_bytes, filename="error.png")

    # COMMANDS

    average = app_commands.Group(
        name="average", description="Average statistics commands")

    @average.command(name="server", description="Show average statistics for the server")
    async def average_server(self, interaction: discord.Interaction):

        await interaction.response.defer()

        if not self.db_cog:
            self.db_cog = self.bot.get_cog('DatabaseStats')
            if not self.db_cog:
                await interaction.followup.send(
                    "❌ DatabaseStats cog is not loaded. AverageStats requires DatabaseStats to function.",
                    ephemeral=True
                )
                return

        try:

            image_file = await self.generate_server_stats_image(interaction, 30, None, "day")
            view = AverageServerStatsView(self, 30, None, "day")

            await interaction.followup.send(file=image_file, view=view)

        except Exception as e:
            print(f"Error in average server command: {e}")
            await interaction.followup.send(
                "❌ An error occurred while generating statistics image. Please try again later.",
                ephemeral=True
            )

    @average.command(name="user", description="Show average statistics for a specific user")
    @app_commands.describe(user="The user to show statistics for")
    async def average_user(self, interaction: discord.Interaction, user: discord.User):

        await interaction.response.defer()

        if not self.db_cog:
            self.db_cog = self.bot.get_cog('DatabaseStats')
            if not self.db_cog:
                await interaction.followup.send(
                    "❌ DatabaseStats cog is not loaded. AverageStats requires DatabaseStats to function.",
                    ephemeral=True
                )
                return

        try:

            image_file = await self.generate_user_stats_image(interaction, 30, user.id, "day")
            view = AverageUserStatsView(self, 30, user.id, "day")

            await interaction.followup.send(file=image_file, view=view)

        except Exception as e:
            print(f"Error in average user command: {e}")
            await interaction.followup.send(
                "❌ An error occurred while generating statistics image. Please try again later.",
                ephemeral=True
            )


# SETUP

async def setup(bot):

    await bot.add_cog(AverageStats(bot))
