import asyncio
from collections import defaultdict

from telethon import TelegramClient

active_clients: dict[int, TelegramClient] = {}
client_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
