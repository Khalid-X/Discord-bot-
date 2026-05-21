
# Statistics Discord Bot Program

## Overview and Context

**Discord** is a communication platform where users interact through text, voice, and video in organized communities called servers. These servers utilize channels, roles, and permissions to help communities collaborate and stay organized.

**Discord bots** are automated applications that interact with Discord servers. They moderate communities, automate tasks, send notifications, and provide engagement analytics or entertainment features.

## About the Project

This repository contains a **Statistics Bot** designed to monitor and analyze activity inside Discord servers. It tracks metrics such as messages, voice activity, invites, mentions, and emojis, then converts that raw data into readable **charts**, **leaderboards**, **averages**, and **heatmaps**.

The system currently supports analytics for over 6,000 users while maintaining efficient database and memory usage.

The project was developed entirely in Python using libraries such as `discord.py`, `Pillow`, `matplotlib`, and `numpy`. Building the platform also required a strong working knowledge of `SQL`, `PostgreSQL`, `TimescaleDB`, and `Redis`.

---

## Infrastructure

The bot was designed with a strong focus on backend engineering and scalable system architecture. It was built to handle thousands of active users with the structural capacity to scale up to millions.

The project required solving real world engineering challenges, including:

* Managing API rate limits
* Designing efficient caching systems
* Handling real time event processing
* Optimizing database queries
* Scaling analytics storage
* Implementing large scale data aggregation

### PostgreSQL Database

For persistent storage and efficient querying, the bot uses a PostgreSQL database coupled with a Redis caching layer.

#### Data Flow

Data received from the Discord API is first staged in a **Redis batch system** before being written to the database every 30 seconds. This drastically reduces the write load on the database during high traffic periods. The database currently utilizes **28 tables** dedicated to tracking and caching.

#### Schema Structure

Most tables use **hourly aggregation** and **indexes** to optimize query performance. Timestamp heavy tables are converted into **hypertables** using the **TimescaleDB extension**. This divides data into distinct time chunks, allowing for significantly faster time series queries.

---

## Software Architecture

The project follows a modular architecture using Discord's native command groups, called **Cogs**. Each Cog independently manages its own commands, logic, and internal systems, making the codebase much easier to maintain, debug, and scale.

This modular structure allows new tracking systems and analytics features to be added seamlessly without breaking unrelated parts of the application.

Each Cog pulls data directly by leveraging functions defined in a centralized database module. From there, the bot uses the `Pillow` library to dynamically render this data onto a visual template precisely handling custom fonts, strokes, bounding rectangles, and exact layout coordinates to generate polished images for the user.

---

## Deployment & DevOps Architecture

The production environment is hosted on dedicated cloud infrastructure utilizing a Hetzner VPS (Virtual Private Server) located in Germany.

### Production Network & System Topology

* **Process Supervision:** The Discord bot gateway is managed as a native Linux `systemd` service. This ensures 24/7 runtime reliability through automated recovery loops, centralized logging, and instant crash restarts.
* **Containerized State Layers:** To enforce strict network isolation, the PostgreSQL/TimescaleDB instance and the Redis caching layer run inside isolated Docker containers. They are bound exclusively to localhost ports (`5432` and `6379`), making them completely inaccessible to the public internet.

### CI/CD Deployment Pipeline

To maintain a fast development loop, the project implements a custom deployment automation script (`deploy.sh`).

1. **Secure Transport:** Code updates and backend Python modules are pushed to the remote server using encrypted `scp` (Secure Copy Protocol) channels over SSH.
2. **Automated Lifecycle Management:** The remote execution script automatically handles dependency resolution, updates environment variables, and performs rolling restarts of the `systemd` services to minimize user facing downtime.

### Secure Local Development

To ensure local changes never impact the live site, the development environment utilizes an encrypted **SSH Tunnel** to securely pull real world data from the remote production database to a local machine. This allows for rigorous, sandboxed testing before any code is promoted to production.

---

## Project Goals and Impact

The primary goal of this project is to help online communities better understand engagement patterns and server growth through data driven insights.

Beyond the end user features, the project served as a comprehensive software engineering exercise involving:

* Backend infrastructure design
* Data engineering and pipeline optimization
* Real-time application development
* Scalable architecture planning
* Product and feature design

Ultimately, this project demonstrates the ability to independently design, build, optimize, and maintain a production-scale software system used by real communities.

## Where can I use this bot?

To try out the bot, simply create a server on the Discord application and invite it using this link:
[Authorize StatsHQ Bot](https://discord.com/oauth2/authorize?client_id=1481328229909270568&permissions=414734863601&integration_type=0&scope=bot+applications.commands)