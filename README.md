# Statistics Discord Bot Program

## Overview and Context

**Discord** is a platform where people communicate through text, voice, and video in organized communities called servers. Servers contain channels, roles, and permissions that help communities collaborate and stay organized.


**Discord bots** are automated applications that interact with Discord servers. They can moderate communities, automate tasks, send notifications, and provide analytics or entertainment features.


## About the project

This repository contains a **Statistics Bot** designed to monitor and analyze activity inside Discord servers. It tracks statistics such as messages, voice activity, invites, mentions, emojis, and more, then converts the collected data into readable **charts**, **leaderboards**, **averages**, and **heatmaps**.

The system currently supports analytics for over 5,000 users while maintaining efficient database and memory usage.

The project was developed entirely in Python using libraries such as discord.py, Pillow, matplotlib, numpy, and more.

---


## Infrastructure 

The bot was designed with a strong focus on backend engineering and scalable system architecture. It was built to be capable of tracking hundreds of thousands of users with the potential of tracking millions. It uses a postgresql database with multiple different extensions. 

The project required solving real-world engineering challenges such as:

- API rate limits
- Efficient caching
- Real time event processing
- Database optimization
- Scalable analytics storage
- Large scale data aggregation




Data received from the Discord API is first stored in a **Redis batch system** before being written to the database every 30 seconds. The database currently contains **28 tables** used for tracking and caching.

Most tables in the database are **hourly aggregated** and use **indexes** to improve query performance and efficiency. Timestamp reliant tables are also converted to **hypertables** with the use of the **timescaledb extension** to take advantage of separating data into time chunks that allow for even faster querying. 


---

## Software Architecture 

The project follows a modular architecture using professional command groups called cogs. Each cog independently manages its own commands, logic, and systems, making the codebase easier to maintain, debug, and expand.

The modular structure allows new tracking systems and analytics features to be added efficiently without affecting unrelated parts of the application.

Each cog focuses on using the functions from the database file in order to get data directly from the database. Then using the pillow library, it draws the data on a template and adds fonts, strokes, restricting rectangles, and specific coordinates.


---

## Deployment & DevOps Architecture

The production environment is hosted on a dedicated cloud infrastructure utilizing a Hetzner VPS (Virtual Private Server) located in Germany.

### Production Network & System Topology 

- **Process Supervision:** The Discord bot gateway is managed as native Linux `systemd` services. This ensures 24/7 runtime reliability through automated recovery loops, logging, and crash-restarts.

- **Containerized State Layers:** To enforce strict network isolation, the PostgreSQL/TimescaleDB instance and the Redis caching layer run within isolated Docker containers. They are bound exclusively to localhost ports (`5432` and `6379`), making them entirely inaccessible to the public internet.

### CI/CD Deployment Pipeline

To maintain high development velocity, the project implements a custom CI/CD pipeline using a bash-engineered deployment automation script (`deploy.sh`). 

1. **Secure Transport:** Assets and backend Python modules are pushed to the remote server using encrypted `scp` (Secure Copy Protocol) channels over SSH.
2. **Automated Lifecycle Management:** The remote execution script handles dependency resolution, updates environmental configurations, and performs rolling restarts of the `systemd` microservices to minimize user-facing downtime.

### Secure Local Development (Hybrid Cloud Environment)

To ensure local changes never impact the live site, the development environment utilizes an encrypted **SSH Tunnel** to securely pull real-world data from the remote production database to the local machine. This allows for rigorous, sandboxed testing before any code is promoted to production.


---


## Project Goals and Impact

The primary goal of the project is to help online communities better understand engagement patterns and server growth through data-driven insights.

Beyond analytics, the project also served as a practical software engineering experience involving:

- Backend infrastructure design
- Data engineering
- System optimization
- Real time application development
- Scalable architecture planning
- Product and feature design

The project demonstrates the ability to independently design, build, optimize, and maintain a production-scale software system used by real communities.


## Where can I use this bot?

You simply have to make a discord server on the discord application then use this invite link: https://discord.com/oauth2/authorize?client_id=1481328229909270568&permissions=414734863601&integration_type=0&scope=bot+applications.commands 

