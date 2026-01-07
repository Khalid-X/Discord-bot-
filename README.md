# Discord Stats Bot

## Overview

**Discord** is a popular communication platform designed for creating communities. It allows users to communicate via text, voice, and video channels, as well as share multimedia content. Discord is widely used by organizations, hobby groups, and educational communities to foster collaboration and engagement.

Discord servers are virtual communities where users can communicate and collaborate. Each server contains multiple channels for text, voice, or video communication, along with roles and permissions to manage members. Servers are similar to WhatsApp groups in that they provide a shared space for people with common interests to interact, but they offer more organizational tools and customizable channels.

**Discord bots** are automated programs that interact with servers on Discord. They can perform a wide range of tasks, including moderating channels, sending notifications, providing entertainment, and tracking server activity. Bots enhance the user experience by automating repetitive tasks and providing valuable insights into server dynamics.

The discord bot in this repository is a **Discord Stats Bot** that is a specialized bot designed to monitor and analyze user activity within a Discord server. It collects comprehensive statistics on server engagement, allowing server administrators and community managers to better understand participation and interaction patterns. 

It was fully made using **python** and multiple massive libraries such as: Pillow, matplotlib, numpy, discord.py, etc. It was also made with the help of popular services like PostgreSQL and redis. It took about **3 months** to fully finalize this project.


## Features

This **Discord Stats Bot** tracks the following metrics:

- **Messages**: Counts and analyzes messages sent by users across different channels.
- **Voice Call Activity**: Monitors time spent by users in voice channels, providing insights into engagement levels.
- **Emojis**: Tracks usage of emojis within the server to identify popular expressions and reactions.
- **Application Activity**: Observes interactions with Discord applications and commands.
- **Invites**: Keeps a record of server invites, including who invited whom, to assess community growth.

All Statistics are permanently recorded using a PostgreSQL database. Multiple tables in the database are aggregated and were converted into hypertables that work with the timescaledb extension. 

---

## Benefits

- Provides **insights into community engagement** and activity trends.
- Helps server administrators **identify active users** and recognize contributions.
- Assists in **optimizing server organization and content** based on user behavior.
- Offers a **comprehensive overview** of server dynamics in a single tool.




