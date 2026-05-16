# Statistics Discord Bot Program

## Overview and Context

**Discord** is a platform where people communicate through text, voice, and video in organized communities called servers. Servers contain channels, roles, and permissions that help communities collaborate and stay organized.


**Discord bots** are automated applications that interact with Discord servers. They can moderate communities, automate tasks, send notifications, and provide analytics or entertainment features.


## About the project

This repository contains a **Statistics Bot** designed to monitor and analyze activity inside Discord servers. It tracks statistics such as messages, voice activity, invites, mentions, emojis, and more, then converts the collected data into readable **charts**, **leaderboards**, **averages**, and **heatmaps**.

The bot is built to scale efficiently and currently tracks over **5,000 users** with ease.

The project was developed entirely in Python using libraries such as discord.py, Pillow, matplotlib, numpy, and more.

---


## Infrastructure 

Data received from the Discord API is first stored in a **Redis batch system** before being written to the database every 30 seconds. The database currently contains **28 tables** used for tracking and caching.

Most tables in the database are **hourly aggregated** and use **indexes** to improve query performance and efficiency. Timestamp reliant tables are also converted to **hypertables** with the use of the **timescaledb extension** to take advantage of separating data into time chunks that allow for even faster querying. 

The infrastructure also handles role membership syncing, channel/category syncing, and other maintenance tasks through **cron jobs**. All syncing operations are **rate-limited** to reduce unnecessary database load and IOPS usage.


---

## Modularization

The bot is organized into professional command groups called **cogs**. Each cog manages its own commands and logic, helping keep the project modular, easier to maintain, and simpler to debug.



---


## Where can I use this bot?

You simply have to make a discord server on the discord application then use this invite link: https://discord.com/oauth2/authorize?client_id=1481328229909270568&permissions=414734863601&integration_type=0&scope=bot+applications.commands 

