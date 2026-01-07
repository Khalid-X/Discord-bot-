import discord
from discord.ext import commands
from discord import app_commands


class BotLinks(commands.Cog):

    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="links",
        description="Get the bot's invite link or support server"
    )
    @app_commands.choices(option=[
        app_commands.Choice(name="Bot Invite Link", value="invite"),
        app_commands.Choice(name="Support Server", value="support")
    ])
    async def links_command(
        self,
        interaction: discord.Interaction,
        option: app_commands.Choice[str] = None
    ):

        embed = discord.Embed(color=discord.Color.from_str("#FFFFFF"))

        if option is None:
            embed.title = "Bot Links"
            embed.description = "Here are all the important links for this bot:"

            embed.add_field(
                name="🤖 Bot Invite Link",
                value="[Click here to invite the bot to your server!](https://discord.com/oauth2/authorize?client_id=1424327933417492480&permissions=352807976696992&integration_type=0&scope=bot+applications.commands)",
                inline=False
            )

            embed.add_field(
                name="🆘 Support Server",
                value="[Click here to join our support server!](https://discord.gg/KXSk4Q84)",
                inline=False
            )

            embed.set_footer(
                text="Use /links with an option to see only one link")

        elif option.value == "invite":
            embed.title = "Bot Invite Link"
            embed.description = "Invite this bot to your server!"

            embed.add_field(
                name="invite link",
                value="[Click here to invite the bot!](https://discord.com/oauth2/authorize?client_id=1424327933417492480&permissions=352807976696992&integration_type=0&scope=bot+applications.commands)",
                inline=False
            )

        elif option.value == "support":
            embed.title = "Support Server"
            embed.description = "Contact the developers directly and get help!"

            embed.add_field(
                name="Support Server",
                value="[Click here to join our support server!](https://discord.gg/KXSk4Q84)",
                inline=False
            )

        await interaction.response.send_message(embed=embed)


async def setup(bot):
    await bot.add_cog(BotLinks(bot))
