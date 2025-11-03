#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MASTER FINAL: master_movie_bot_with_qr.py
- Preserves existing features from your original file.
- Fixes: verification consistency, race conditions in suggestions,
  session persistence (buttons remain usable), and reliable auto-delete.
- Behavior adjustments per your request:
  * All suggestion buttons remain valid (they map to stored suggestions).
  * Messages that are sent AFTER the user had access are auto-deleted after 20 minutes.
  * Only messages that were delivered while user had access are scheduled for deletion.
  * Sessions are kept for 24 hours (so buttons remain usable).
- IMPORTANT: Replace BOT_TOKEN, CHANNEL_ID, CHANNEL_USERNAME, ADMIN_USER_ID if needed.
"""
import re
import asyncio
import time
import requests
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
import asyncio
import uuid
from datetime import datetime, timedelta, timezone
import nest_asyncio
from rapidfuzz import fuzz, process
import nest_asyncio nest_asyncio.apply()

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto,
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)
from telegram.error import RetryAfter, Conflict, TelegramError

nest_asyncio.apply()

# ------------------ CONFIG ------------------
BOT_TOKEN = "BOT_TOKEN"  # your bot token (keep private)
CHANNEL_ID = -1002899143167   # channel id (int)
CHANNEL_USERNAME = "movie_storm"  # channel username (no @)
ADMIN_USER_ID = 7681368329    # admin user id (int)

# Files for persistence
MOVIES_DB_FILE = "movies_db.json"
VERIFIED_USERS_FILE = "verified_users.json"
USER_ACCESS_FILE = "user_access.json"
REFERRALS_FILE = "referrals.json"
USER_WALLET_FILE = "user_wallet.json"
USER_STREAK_FILE = "user_streak.json"
USER_HISTORY_FILE = "user_history.json"
WITHDRAW_REQUESTS_FILE = "withdraw_requests.json"
USER_WITHDRAW_RECORDS_FILE = "user_withdraw_records.json"
REDEEM_CODES_FILE = "redeem_codes.json"

# Behavior constants
USER_COOLDOWN = 20          # seconds
DELETE_DELAY = 10 * 60     # 10 minutes (seconds) - per your request
FREE_ACCESS_DURATION = 24 * 3600  # 24 hours in seconds
FREE_ACCESS_URL = "https://vplink.in/qWUKsG"  # replace with your free access URL

# Premium plan definitions (text, code, days)
PREMIUM_PLANS = [
    ("Basic 1 Month - â¹25", "plan_1m", 30,),
    ("Standard 3 Month - â¹50", "plan_3m", 90,),
    ("Pro 6 Month - â¹100", "plan_6m", 180,),
]

# Premium plan definitions (text, code, days,coins)
PREMIUMM_PLANS = [
    ("Basic 1 Month - â¹25", "plan_1m", 30, 500),
    ("Standard 3 Month - â¹50", "plan_3m", 90, 1200),
    ("Pro 6 Month - â¹100", "plan_6m", 180, 2200),
]

# Mapping plan_code -> QR image URL (Google Drive link or direct image URL)
PREMIUM_PLAN_QR = {
    "plan_1m": "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEgu7MIaZKd7SGhz3R9-48c5FEfpZHQKSND_io98RQHDYXjC73xkBahvYHtPWPTi7JxJwseE_35LelOIVbu6c8vzSVjp1ThsrRlEuIuLXTPxlPC7-95BnqjHjnqY-2EIliJZPyWe5ZoGEgjpomU-VWNDzbOzX2CUMTVg012tC4DnbWcoXIIHpSTo1xln30s5/s300/frame%20(2).png",
    "plan_3m": "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEjvWgATrXrthGfIK-P93I1AuGhvMOhk__WUP34eoJW8jo35_H_6zdAvUEhYPVChEH597imR2pT1-eKKMsNo-ECtwkryiTl5i6Nkfr2YcxiQjcCrqtKWv3qH4JNpKOsDPN0XLgCLK_EOnTDssSTXbD_-ZtHF54SAyxSnYmnZ2UhdIj1zHvK2XpNlliNkGi3V/s300/frame%20(1).png",
    "plan_6m": "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEhBMsXkVxAJ0EMnPnNdo5YrNScRzs42KOTAl3C1y3YLAvgpaRI2Kh5Q8-mNSuZVDVRuChkqzpREUAh5VvrRk5N8M_XobiZukRJnGT2e9nbmwstxO7orvale0FN4vwYcVxCUTmCgxevaoNsqGOXhogEe54Zp00cmhroLP7co2neorJ26JLuHzFEOOakr2wiR/s300/frame.png",
}

# Personalized start image (use a URL Telegram can access)
DEFAULT_START_IMAGE = "https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEjHoOiFbGOJgoZamEQXRSorCan1ma_oVouEb354CJ7mF1O9NbCUKyZzCwenWYGPPmrheFX82lsqWJkjNe7TFNDI7f8Ir83U5SH5P3HIplaRe-9_U5FQNnzlyysg_SOX3uRjBmanOrj-vsdIAhe5v2PPICRHuQYkcIKcbtDyeQD5zaQTthwAbGE-z33Ov0VR/s1536/file_0000000011d061f8b586307360cbd095.png"

# ===== Gemini AI Direct Call (Termux compatible) =====
GEMINI_KEY = "GEMINI_KEY"
_executor = ThreadPoolExecutor(max_workers=2)
_AI_CACHE = {}
AI_CACHE_TTL = 60 * 60

def call_gemini_direct(prompt_text: str) -> str:
    """Call Gemini API directly using HTTP (no google-genai lib)."""
    if not GEMINI_KEY:
        print("â ï¸ Gemini key missing")
        return ""
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_KEY}"
        headers = {"Content-Type": "application/json"}
        payload = {"contents": [{"parts": [{"text": prompt_text}]}]}
        res = requests.post(url, headers=headers, data=json.dumps(payload), timeout=15)
        if res.status_code == 200:
            data = res.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        else:
            print("â Gemini API Error:", res.status_code, res.text)
            return ""
    except Exception as e:
        print("â ï¸ Gemini call failed:", e)
        return ""

async def get_ai_clean_title(raw_caption: str) -> str:
    """Use Gemini AI or fallback regex cleaner."""
    raw = (raw_caption or "").strip()
    if not raw:
        return "Unknown Title"

    cached = _AI_CACHE.get(raw)
    if cached and (time.time() - cached[1]) < AI_CACHE_TTL:
        return cached[0]

    pre = re.sub(r"http\S+|@\S+|#\S+|â|ð¥|â|â|â¢", " ", raw)
    pre = re.sub(r"[^a-zA-Z0-9\s\.\-_()]", " ", pre)
    pre = re.sub(r"\s+", " ", pre).strip()
    pre = pre.replace("_", " ").replace(".", " ")

    prompt = (
        "You are a smart movie/series title normalizer.\n"
        "Convert messy caption into a clean title.\n"
        "Include Season/Episode (like S 02 or E 05), Year in parentheses, and language if visible.\n"
        "Return only the clean title.\n\n"
        f"Caption: {pre}\n\nClean title:"
    )

    loop = asyncio.get_running_loop()
    ai_text = await loop.run_in_executor(_executor, call_gemini_direct, prompt)

    if not ai_text:
        junk = {
            "1080p","720p","480p","WEB","DL","HDRip","BluRay","H264",
            "x264","x265","HEVC","AAC","AMZN","HQ","RIP","UNCUT","DUAL","HDM2"
        }
        words = [w for w in pre.split() if w.upper() not in junk]
        title = " ".join(words[:10]).strip().title()
        season = re.search(r"(?:S|Season)\s?(\d+)", pre, re.IGNORECASE)
        episode = re.search(r"(?:E|Ep|Episode)\s?(\d+)", pre, re.IGNORECASE)
        year = re.search(r"(19|20)\d{2}", pre)
        lang = re.search(r"\b(Hindi|English|Tamil|Telugu|Malayalam|Kannada|Dual|Multi)\b", pre, re.IGNORECASE)
        parts = [title] if title else []
        if season:
            parts.append(f"Season {season.group(1)}")
        if episode:
            parts.append(f"Ep {episode.group(1)}")
        if year:
            parts.append(f"({year.group(0)})")
        if lang:
            parts.append(lang.group(0).capitalize())
        ai_text = " ".join(parts).strip() or title or "Unknown Title"

    _AI_CACHE[raw] = (ai_text, time.time())
    return ai_text
# ------------------ UTIL ------------------
def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"â ï¸ Failed to load {path}: {e}")
            return default
    return default

def save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"â ï¸ Failed to save {path}: {e}")

# persistent data
# Ensure verified_users stored/compared as strings everywhere (consistent)
verified_users = set(str(x) for x in load_json(VERIFIED_USERS_FILE, []))
movies_db = load_json(MOVIES_DB_FILE, {})  # normalized -> message_id
user_access = load_json(USER_ACCESS_FILE, {})  # user_id (str) -> expiry (float timestamp)
referrals = load_json(REFERRALS_FILE, {})  # token -> {"owner": user_id_str, "used_by": [user_id_strs]}
user_wallet = load_json(USER_WALLET_FILE, {})
user_streak = load_json(USER_STREAK_FILE, {})
user_history = load_json(USER_HISTORY_FILE, {})
withdraw_requests = load_json(WITHDRAW_REQUESTS_FILE, {})
user_withdraw_records = load_json(USER_WITHDRAW_RECORDS_FILE, {})
redeem_codes = load_json(REDEEM_CODES_FILE, {})

# runtime / ephemeral
last_request_time = {}   # user_id (str) -> timestamp
# Tokenized search sessions to avoid race conditions. Long expiry (24h) so buttons remain usable.
search_sessions = {}     # token -> {"user_id": str, "suggestions": [titles], "ts": float}
SUGGESTION_EXPIRY = 24 * 3600  # 24 hours - sessions persist for a day

# Track messages that were delivered while user had access
# Mapping: user_id(str) -> set of message_id(int)
active_user_messages = {}

def save_all():
    save_json(MOVIES_DB_FILE, movies_db)
    save_json(VERIFIED_USERS_FILE, list(verified_users))
    save_json(USER_ACCESS_FILE, user_access)
    save_json(REFERRALS_FILE, referrals)
    save_json(USER_WALLET_FILE, user_wallet)
    save_json(USER_STREAK_FILE, user_streak)
    save_json(USER_HISTORY_FILE, user_history)
    save_json(WITHDRAW_REQUESTS_FILE, withdraw_requests)

def normalize_title(s: str) -> str:
    return (s or "").strip().lower()

def make_search_token():
    return uuid.uuid4().hex[:18]

def cleanup_search_sessions():
    now = time.time()
    tokens_to_remove = [t for t,info in search_sessions.items() if now - info.get("ts", 0) > SUGGESTION_EXPIRY]
    for t in tokens_to_remove:
        search_sessions.pop(t, None)

def today_str():
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d")

def coins_to_rupees(coins):
    return coins / 100.0

def rupees_to_coins(rupees):
    return int(rupees * 100)

# ------------------ WALLET / HISTORY HELPERS ------------------
def add_coins(user_id: str, amount: int, reason: str):
    if user_id not in user_wallet:
        user_wallet[user_id] = 0
    user_wallet[user_id] += amount
    if user_id not in user_history:
        user_history[user_id] = {"premium": [], "withdraw": [], "earn": []}
    user_history[user_id]["earn"].append({"timestamp": time.time(), "amount": amount, "reason": reason})
    save_all()

def deduct_coins(user_id: str, amount: int) -> bool:
    if user_id not in user_wallet or user_wallet[user_id] < amount:
        return False
    user_wallet[user_id] -= amount
    save_all()
    return True

def get_wallet_balance(user_id: str) -> int:
    return user_wallet.get(user_id, 0)

def get_user_history(user_id: str):
    return user_history.get(user_id, {"premium": [], "withdraw": [], "earn": []})

# ------------------ DELETE AFTER (reliable scheduling) ------------------
async def delete_after(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, owner_user_id: str, delay: int = DELETE_DELAY):
    """
    Delete a specific message after `delay` seconds.
    Only deletes the message (and removes from tracking).
    """
    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        return

    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        print(f"ðï¸ Deleted message {message_id} from chat {chat_id}")
    except Exception as e:
        print(f"â ï¸ delete_after: Failed to delete message {message_id} in chat {chat_id}: {e}")

    # cleanup tracking
    try:
        if owner_user_id in active_user_messages:
            if message_id in active_user_messages[owner_user_id]:
                active_user_messages[owner_user_id].remove(message_id)
    except Exception as e:
        print("â ï¸ cleanup active_user_messages failed:", e)

    # send a short deletion notice (optional). commented out to reduce noise.
    try:
        await context.bot.send_message(chat_id, "ð Your File Was Deleted Successfully To Avoid Copyright ï¸.")
    except Exception:
        pass

# ------------------ HYBRID SEARCH (advanced hybrid) ------------------
def find_advanced_matches(query: str, choices, limit: int = 25, score_cutoff: int = 60):
    """
    Hybrid search:
    - substring (contains) matches first
    - then fuzzy matches (rapidfuzz token_set_ratio)
    - then a token overlap fallback
    """
    q = normalize_title(query)
    if not q:
        return []

    choice_list = list(choices)

    # keyword contains matches first (high priority)
    keyword_matches = [title for title in choice_list if q in title.lower()]

    # fuzzy matches
    fuzzy_results = process.extract(q, choice_list, scorer=fuzz.token_set_ratio, limit=limit*2, score_cutoff=score_cutoff)
    fuzzy_matches = [match[0] for match in fuzzy_results]

    merged = []
    for t in keyword_matches + fuzzy_matches:
        if t not in merged:
            merged.append(t)

    # fallback: token overlap
    if not merged and " " in q:
        tokens = [tok for tok in q.split() if tok]
        hits = []
        for title in choice_list:
            tl = title.lower()
            score = sum(1 for tok in tokens if tok in tl)
            if score > 0:
                hits.append((score, title))
        hits.sort(key=lambda x: (-x[0], x[1]))
        merged = [t for _, t in hits]

    return merged[:limit]

# ------------------ INDEX OLD CHANNEL MESSAGES ------------------
async def index_old_channel_messages(app):
    print("ð Attempting to index channel history (bot must be admin and have rights)...")
    try:
        await app.bot.get_chat(CHANNEL_ID)
        print("â Channel accessible. (Indexing by channel_post and /index command will populate the DB.)")
    except Exception as e:
        print("â ï¸ index_old_channel_messages error (ignore if bot not admin):", e)

# ------------------ REFERRAL HELPERS ------------------
def make_ref_token():
    return uuid.uuid4().hex[:16]

def ensure_user_has_token(user_id_str):
    # find existing token for owner
    for token, rec in referrals.items():
        if str(rec.get("owner")) == str(user_id_str):
            return token
    # create new
    token = make_ref_token()
    referrals[token] = {"owner": str(user_id_str), "used_by": []}
    save_json(REFERRALS_FILE, referrals)
    return token

# ------------------ STREAK & DAILY ------------------
def update_user_streak(user_id: str):
    today = today_str()
    streak_info = user_streak.get(user_id, {"last_search_day": "", "streak": 0})
    last_day = streak_info.get("last_search_day", "")
    streak = streak_info.get("streak", 0)
    try:
        last_date = datetime.strptime(last_day, "%Y-%m-%d")
        today_date = datetime.strptime(today, "%Y-%m-%d")
        delta = (today_date - last_date).days
    except Exception:
        delta = None
    if delta == 1:
        streak += 1
    elif delta == 0:
        pass
    else:
        streak = 1
    user_streak[user_id] = {"last_search_day": today, "streak": streak}
    save_all()
    return streak

def check_and_give_daily_coins(user_id: str) -> bool:
    today = today_str()
    last_day = user_streak.get(user_id, {}).get("last_search_day", "")
    if last_day != today:
        add_coins(user_id, 10, "Daily search bonus")
        return True
    return False

def check_jackpot_streak(user_id: str, streak: int):
    today = today_str()
    if streak > 0 and streak % 7 == 0:
        # â make sure reward is NOT already given today
        hist = user_history.get(user_id, {}).get("earn", [])
        for e in hist:
            if "7-day streak jackpot" in e["reason"] and today in datetime.fromtimestamp(e["timestamp"]).strftime("%Y-%m-%d"):
                return False  # â already rewarded today

        add_coins(user_id, 50, f"7-day streak jackpot reward (Day {streak})")
        return True
    return False

# ------------------ LEADERBOARD ------------------
def get_daily_leaderboard():
    today = today_str()
    user_earn_today = {}
    for uid, history in user_history.items():
        earned = 0
        for rec in history.get("earn", []):
            ts = rec.get("timestamp", 0)
            reason = rec.get("reason", "").lower()
            dt = datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=5, minutes=30)))
            if dt.strftime("%Y-%m-%d") == today and "movie search" in reason:
                earned += rec.get("amount", 0)
        if earned > 0:
            user_earn_today[uid] = earned
    sorted_users = sorted(user_earn_today.items(), key=lambda x: -x[1])[:10]
    return sorted_users

def get_user_rank(user_id: str):
    ld = get_daily_leaderboard()
    for i, (uid, _) in enumerate(ld):
        if uid == user_id:
            return i + 1
    return None

def reward_leaderboard_top(users):
    today = today_str()
    rewarded = []
    for uid, _ in users:
        if uid not in user_history:
            user_history[uid] = {"premium": [], "withdraw": [], "earn": []}
        already = False
        for rec in user_history[uid]["earn"]:
            ts = rec.get("timestamp", 0)
            reason = rec.get("reason", "")
            dt = datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=5, minutes=30)))
            if dt.strftime("%Y-%m-%d") == today and "leaderboard daily top reward" in reason.lower():
                already = True
                break
        if not already:
            add_coins(uid, 1000, "Leaderboard daily top reward")
            rewarded.append(uid)
    save_all()
    return rewarded

async def notify_and_reward_leaderboard(bot):
    users = get_daily_leaderboard()
    if not users:
        try:
            await bot.send_message(ADMIN_USER_ID, "ð Leaderboard reward job: no data today.")
        except Exception:
            pass
        return
    rewarded = reward_leaderboard_top(users)
    if not rewarded:
        try:
            await bot.send_message(ADMIN_USER_ID, "ð Leaderboard reward job: top users were already rewarded.")
        except Exception:
            pass
        return
    for uid in rewarded:
        try:
            await bot.send_message(int(uid), "ð Congrats! You are in today's Top 10. 1000 coins added to your wallet!")
        except Exception:
            pass
    lines = []
    for i, (uid, score) in enumerate(users, start=1):
        try:
            chat = await bot.get_chat(int(uid))
            name = chat.first_name or chat.username or str(uid)
        except Exception:
            name = str(uid)
        lines.append(f"{i}. {name} - {score} coins")
    summary = "ð Daily Leaderboard Rewarded:\n\n" + "\n".join(lines)
    try:
        await bot.send_message(ADMIN_USER_ID, summary)
    except Exception:
        pass

async def schedule_daily_leaderboard_rewards(app):
    while True:
        try:
            now = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30)))
            target = now.replace(hour=21, minute=0, second=0, microsecond=0)
            if now >= target:
                target = target + timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            await asyncio.sleep(wait_seconds)
            try:
                await notify_and_reward_leaderboard(app.bot)
            except Exception as e:
                print("Error rewarding leaderboard:", e)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print("schedule_daily_leaderboard_rewards error:", e)
            await asyncio.sleep(60)

# ------------------ HANDLERS ------------------
def ist_now():
    # simple IST: UTC +5:30 using timezone-aware now()
    return datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)

def greeting_period():
    h = ist_now().hour
    if h < 12:
        return "Good morning ð"
    else:
        return "Good afternoon ð"

# Utilities for scheduling deletes and tracking sent messages
def record_sent_message_for_deletion(owner_user_id: str, message):
    """
    Record a message id (int) that should be auto-deleted later.
    Owner_user_id is a string user id.
    """
    try:
        if owner_user_id not in active_user_messages:
            active_user_messages[owner_user_id] = set()
        active_user_messages[owner_user_id].add(message.message_id)
    except Exception as e:
        print("â ï¸ record_sent_message_for_deletion:", e)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Handles:
    # - /start
    # - /start freeaccess
    # - /start ref_<token>
    if update.message is None:
        return
    user = update.effective_user
    user_id = str(user.id)
    text = update.message.text or ""
    args = text.split(maxsplit=1)
    cmd_arg = ""
    if len(args) > 1:
        cmd_arg = args[1].strip()

    # If user came via a referral: start ref_<token>
    if cmd_arg.startswith("ref_"):
        token = cmd_arg[len("ref_"):]
        tokinfo = referrals.get(token)
        # Process referral join attempt
        if not tokinfo:
            # invalid token -> fallthrough to normal start
            pass
        else:
            owner = str(tokinfo.get("owner"))
            used_by = tokinfo.get("used_by", [])
            # prevent self-referral
            if user_id == owner:
                await update.message.reply_text("â Hey Dude, You Can't Refer Yourself ð¿.")
                return
            # prevent duplicate counting
            if user_id in used_by:
                await update.message.reply_text("â You already used this referral link earlier. Thanks!")
                return
            # add this user as a unique referrer
            tokinfo.setdefault("used_by", []).append(user_id)
            referrals[token] = tokinfo
            save_json(REFERRALS_FILE, referrals)
            # notify owner about progress
            await update.message.reply_text("â Joined via referral! Search a movie to complete referral bonus.")
            return

    # freeaccess param - still grant time-based access, but user must verify (join channel) before searching.
    if cmd_arg == "freeaccess":
        access_until = time.time() + FREE_ACCESS_DURATION
        user_access[user_id] = access_until
        save_json(USER_ACCESS_FILE, user_access)
        await update.message.reply_text(
            f"â Congratulations! You got free access until {time.ctime(access_until)}.\n"
        )
        return

    # Normal /start: send image + personalized caption + buttons depending on verification
    greet = greeting_period()
    name_display = user.first_name or "there"
    caption_lines = [
        f"Hey {name_display}, {greet}",
        "",
        "I am The Most Powerful Movie Searching ð bot with Premium Earnings Features",
        "",
        "Search âï¸ any movie by typing.. name and Earn ð°",
        "Start Earning â¤ï¸ Now...",
        "Tutorial video ð¥ -https://t.me/movie_storm/70"
    ]
    caption_text = "\n".join(caption_lines)

    # Send image with caption (best-effort). Use DEFAULT_START_IMAGE or send only text if that fails.
    try:
        sent = await context.bot.send_photo(chat_id=update.effective_chat.id, photo=DEFAULT_START_IMAGE, caption=caption_text)
    except Exception as e:
        # fallback: send plain text
        print("Failed to send start image:", e)
        await update.message.reply_text(caption_text)

    # After greeting, show options. If user already has access show greeting + options; else show gate for access/verify
    now_ts = time.time()
    expiry = user_access.get(user_id, 0)
    if expiry > now_ts:
        # user currently has access -> check verified (IMPORTANT: verified_users contains strings)
        if user_id in verified_users:
            kb = [
            [InlineKeyboardButton("ð Search a Movie", callback_data="noop")],
            [InlineKeyboardButton("ð° Channel", url=f"https://t.me/{CHANNEL_USERNAME}")],
            [InlineKeyboardButton("ðââï¸ Refer", callback_data="refer")],
            [InlineKeyboardButton("ð Premium (QR)", callback_data="show_qr_plans")],
            [InlineKeyboardButton("ð Dashboard", callback_data="dashboard")],
            [InlineKeyboardButton("ð Leaderboard", callback_data="leaderboard")],
        ]
            await context.bot.send_message(update.effective_chat.id, "â You already have access. Type any movie name to search.", reply_markup=InlineKeyboardMarkup(kb))
        else:
            kb = [
                [InlineKeyboardButton("ð¢ Join Channel", url=f"https://t.me/{CHANNEL_USERNAME}")],
                [InlineKeyboardButton("â Verify", callback_data="verify")],
            ]
            await context.bot.send_message(update.effective_chat.id, "ð Please join our Telegram channel to access this bot. Press Verify after joining.", reply_markup=InlineKeyboardMarkup(kb))
        return

    # No access yet -> show free/purchase/verify options
    kb = [
        [InlineKeyboardButton("Get Free 24h Access", url=FREE_ACCESS_URL)],
        [InlineKeyboardButton("ð Buy Premium Plans", callback_data="show_qr_plans")],
        [InlineKeyboardButton("ð¥ Refer", callback_data="refer")],
    ]
    await context.bot.send_message(update.effective_chat.id, "ð Get free 24h access or buy premium to use the bot. Or refer friends to earn free premium.", reply_markup=InlineKeyboardMarkup(kb))


async def plans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Support both /plans text command and callback invocation path
    rows = [[InlineKeyboardButton(text, callback_data=f"buyplann:{code}")] for text, code, _ in PREMIUM_PLANS]
    if update.message:
        await update.message.reply_text("ð Premium Plans:", reply_markup=InlineKeyboardMarkup(rows))
    elif update.callback_query:
        await update.callback_query.edit_message_text("ð Premium Plans:", reply_markup=InlineKeyboardMarkup(rows))


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    user_id = str(query.from_user.id)
    data = query.data or ""

    # noop placeholder to avoid confusion
    if data == "noop":
        try:
            await query.edit_message_text("â Type the movie name in chat to search.")
        except Exception:
            pass
        return

    # verify membership
    if data == "verify":
        try:
            member = await context.bot.get_chat_member(chat_id=f"@{CHANNEL_USERNAME}", user_id=int(user_id))
            if member.status in ["member", "administrator", "creator"]:
                # store as string consistently
                verified_users.add(user_id)
                save_json(VERIFIED_USERS_FILE, list(verified_users))
                try:
                    await query.edit_message_text("â Verified! You can now use the bot." )
                except Exception:
                    await context.bot.send_message(query.message.chat.id, "â Verified! You can now use the bot.", reply_markup=InlineKeyboardMarkup(kb))
            else:
                await query.edit_message_text("â You must join the channel first.")
        except Exception as e:
            print("Verification error:", e)
            await query.edit_message_text("â Unable to verify. Make sure you joined the channel and try again.")
        return

    if data == "grant_free24":
        access_until = time.time() + FREE_ACCESS_DURATION
        user_access[user_id] = access_until
        save_json(USER_ACCESS_FILE, user_access)
        try:
            await query.edit_message_text(f"â You received 24 hours free access until {time.ctime(access_until)}. Send a movie name to search.")
        except Exception:
            await context.bot.send_message(query.message.chat.id, f"â You received 24 hours free access until {time.ctime(access_until)}. Send a movie name to search.")
        return

    # show plans
    if data == "show_qr_plans":
        rows = [[InlineKeyboardButton(text, callback_data=f"buyplann:{code}")] for text, code, _ in PREMIUM_PLANS]
        try:
            await query.edit_message_text("ð Premium Plans:", reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            await context.bot.send_message(query.message.chat.id, "ð Premium Plans:", reply_markup=InlineKeyboardMarkup(rows))
        return

    # buy plan -> send QR image + payment info + notify admin
    if data.startswith("buyplann:"):
        code = data.split(":", 1)[1]
        plan = next((p for p in PREMIUM_PLANS if p[1] == code), None)
        if not plan:
            await query.edit_message_text("â Invalid plan.")
            return
        plan_name, _, days = plan

        try:
            await context.bot.send_message(
                ADMIN_USER_ID,
                f"ð User {query.from_user.full_name} ({user_id}) selected plan: {plan_name}\n"
                f"Ask for payment and then run: /grant {user_id} {days}"
            )
        except Exception as e:
            print("Failed to notify admin about plan selection:", e)

        qr_url = PREMIUM_PLAN_QR.get(code)
        caption = (
            f"â Selected {plan_name}\n\n"
            "ð¬ Payment instructions:\n\n"
            "1) Scan the QR or use the UPI : shrishtim320@okaxis.\n"
            "2) After payment, send the screenshot to the admin:-@anshchaube852 \n\n"
            "ð Instant grant access after verifying payment."
        )

        try:
            if qr_url:
                await context.bot.send_photo(chat_id=query.message.chat.id, photo=qr_url, caption=caption)
                try:
                    await query.edit_message_text("â Payment QR sent. Follow the instructions in the image and message.")
                except Exception:
                    pass
            else:
                await context.bot.send_message(chat_id=query.message.chat.id, text=caption)
                try:
                    await query.edit_message_text("â Payment instructions sent. Ask admin to grant access after payment.")
                except Exception:
                    pass
        except Exception as e:
            print("Failed to send QR image or payment message:", e)
            fallback_text = caption
            if qr_url:
                fallback_text += f"\n\nPayment link/image: {qr_url}\n\nNote: If you provided a Google Drive share link, ensure it's a direct view/download link accessible publicly."
            try:
                await context.bot.send_message(chat_id=query.message.chat.id, text=fallback_text)
                await query.edit_message_text("â Payment link sent (as text). If image failed, please use the link above.")
            except Exception as ex:
                print("Fallback send failed:", ex)
                try:
                    await query.edit_message_text("â ï¸ Failed to send payment QR/instructions. Please contact admin.")
                except Exception:
                    pass
        return

    # refer button: send personalized refer image + caption + share button
    if data == "refer":
        owner_id = str(query.from_user.id)
        token = ensure_user_has_token(owner_id)
        try:
            bot_me = await context.bot.get_me()
            bot_username = bot_me.username or ""
        except Exception:
            bot_username = ""
        if bot_username:
            ref_link = f"https://t.me/{bot_username}?start=ref_{token}"
        else:
            ref_link = f"Use start=ref_{token} with /start in the bot."
        caption = (
            f"Hey {query.from_user.first_name or ''} ð\n\n"
            f"Your refer link - {ref_link}\n\n"
            "Share this link to friends and earn 100 coins when they start bot and search a movie."
        )
        share_text = f"Join this amazing movie bot watch and earn: {ref_link}"
        share_url = f"https://t.me/share/url?url={ref_link}&text={share_text}"
        try:
            await context.bot.send_photo(
                chat_id=query.message.chat.id,
                photo="https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEioyWaj_gqATrzMZOYDXTGia7v8H46u9cn05Q7r6b9bIzKE5D8rw-eMy3M1AmhR6B3XQzYp1LLE3gHYvTzk2rb9xAGfj5efN32GXo5XE8NSL_ezfZ6F9Vnpmf_zg3kGX4X1HLzcrmIb-Ru7V3QfVzNLbUKcV8VWvOHib8I4ml02QQuVC2aXXo4R7wtFEOD7/s1536/file_000000008e1861fd8e52e7276a77acf4.png",
                caption=caption,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Share to friends", url=share_url)]])
            )
            try:
                await query.edit_message_text("ï¿½ï¿½ Referral info sent. Share your link and earn coins!")
            except Exception:
                pass
        except Exception as e:
            print("Failed to send refer image:", e)
            try:
                await context.bot.send_message(chat_id=query.message.chat.id, text=caption, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Share to friends", url=share_url)]]))
                try:
                    await query.edit_message_text("â Referral info sent. Share your link and earn coins!")
                except Exception:
                    pass
            except Exception as ex:
                print("Failed fallback refer message:", ex)
                try:
                    await query.edit_message_text("â Failed to send referral info. Try again later.")
                except Exception:
                    pass
        return

    if data == "dashboard":
        await send_user_dashboard(user_id, context, query)
        return

    if data == "leaderboard":
        await send_leaderboard(update, context, from_button=True)
        return

    if data == "show_plans":
        rows = [[InlineKeyboardButton(f"{text} ({coins} coins)", callback_data=f"buyplan:{code}")] for text, code, _, coins in PREMIUMM_PLANS]
        try:
            await query.edit_message_text("ð Premium Plans (Coins):", reply_markup=InlineKeyboardMarkup(rows))
        except Exception:
            await context.bot.send_message(query.message.chat.id, "ð Premium Plans (Coins):", reply_markup=InlineKeyboardMarkup(rows))
        return

    if data.startswith("buyplan:"):
        code = data.split(":", 1)[1]
        plan = next((p for p in PREMIUMM_PLANS if p[1] == code), None)
        if not plan:
            await query.edit_message_text("â Invalid plan.")
            return
        plan_name, _, days, cost_coins = plan
        wallet_coins = get_wallet_balance(user_id)
        if wallet_coins < cost_coins:
            await query.edit_message_text(f"â Not enough coins. Wallet: {format_coins_rupees(wallet_coins)}")
            return
        if deduct_coins(user_id, cost_coins):
            expiry = time.time() + days * 24 * 3600
            prev = user_access.get(user_id, 0)
            if prev < time.time():
                user_access[user_id] = expiry
            else:
                user_access[user_id] = prev + days * 24 * 3600
            save_all()
            if user_id not in user_history:
                user_history[user_id] = {"premium": [], "withdraw": [], "earn": []}
            user_history[user_id]["premium"].append({"timestamp": time.time(), "plan": plan_name, "paid_coins": cost_coins})
            save_all()
            await query.edit_message_text(f"â Bought {plan_name}. Premium till {time.ctime(user_access[user_id])}")
            try:
                await context.bot.send_message(int(user_id), f"ð Purchased {plan_name} using {cost_coins} coins. Enjoy!")
            except Exception:
                pass
        else:
            await query.edit_message_text("â Failed to deduct coins.")
        return

    if data == "withdraw":
        await query.edit_message_text("ð° Enter amount in rupees to withdraw (minimum â¹50):")
        context.user_data["withdraw"] = {"step": "amount"}
        return

    if data == "withdraw_confirm":
        wd = context.user_data.get("withdraw")
        if not wd or "amount" not in wd:
            await query.edit_message_text("â Withdraw session expired.")
            return
        amount = wd["amount"]
        wallet_coins = get_wallet_balance(user_id)
        coins_needed = rupees_to_coins(amount)
        if wallet_coins < coins_needed:
            await query.edit_message_text(f"â Not enough coins. Wallet: {format_coins_rupees(wallet_coins)}")
            return
        await query.edit_message_text("ð³ Enter your Upi I'd/Number or Bank ð¦ account number with IFSC code.:(â ï¸ Please type correct payment detail.)")
        context.user_data["withdraw"]["step"] = "upi"
        return

    if data == "withdraw_cancel":
        await query.edit_message_text("â Withdraw cancelled.")
        context.user_data.pop("withdraw", None)
        return

    # Confirm selection with tokenized sessions: callback_data format "confirm:<token>:<index>"
    if data.startswith("confirm:"):
        parts = data.split(":")
        if len(parts) != 3:
            await query.message.reply_text("â ï¸ Invalid selection.")
            return
        token = parts[1]
        try:
            idx = int(parts[2])
        except Exception:
            await query.message.reply_text("â ï¸ Invalid selection index.")
            return

        session = search_sessions.get(token)
        if not session:
            await query.message.reply_text("â Selection expired or invalid. Please search again.")
            return

        # Allow anyone to click a suggestion button (not only original searcher)
        suggestions = session.get("suggestions", [])
        if idx < 0 or idx >= len(suggestions):
            await query.message.reply_text("â Selection expired or invalid. Please search again.")
            return

        movie_title = suggestions[idx]
        # deliver movie if exists in movies_db
        if movie_title in movies_db:
            msg_id = movies_db[movie_title]
            try:
                # Send typing action
                try:
                    await context.bot.send_chat_action(query.message.chat.id, "typing")
                except Exception:
                    pass

                # Copy message from channel to user chat
                sent = await context.bot.copy_message(
                    chat_id=query.message.chat.id,
                    from_chat_id=CHANNEL_ID,
                    message_id=msg_id,
                )

                # determine whether the clicking user currently has access
                clicker_id_str = str(query.from_user.id)
                expiry = user_access.get(clicker_id_str, 0)
                now_ts = time.time()
                if expiry > now_ts:
                    # Only schedule delete if user had access at the time they clicked
                    # Track the sent message for deletion tied to this user
                    record_sent_message_for_deletion(clicker_id_str, sent)
                    try:
                        asyncio.create_task(delete_after(context, query.message.chat.id, sent.message_id, clicker_id_str, delay=DELETE_DELAY))
                    except Exception as e:
                        print("â ï¸ Failed to schedule delete task:", e)

                    # Inform user
                    await context.bot.send_message(
                        query.message.chat.id,
                        "â ï¸ Searched movie will be automatically deleted after 10 minutes. Please forward to saved messages ."
                    )
                else:
                    # if user does not have access, send without scheduling deletion
                    await context.bot.send_message(
                        query.message.chat.id,
                        "â¹ï¸ You don't have active access. Ask admin or get free access to use the bot fully."
                    )

                # Do NOT remove the session token; sessions persist so buttons remain usable.
            except Exception as e:
                print("Error sending suggested movie:", e)
                await query.message.reply_text("â ï¸ Could not send file right now.")
        else:
            await query.message.reply_text("â Movie not found anymore (maybe removed).")
        return

    if data == "try_again":
        try:
            await query.edit_message_text("â Please type the movie name again:")
        except Exception:
            await query.message.reply_text("â Please type the movie name again.")
        return

async def grant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /grant <user_id> <days>  (admin only)
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not allowed to use this command.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /grant <user_id> <days>")
        return
    user_id = str(context.args[0])
    try:
        days = int(context.args[1])
    except:
        await update.message.reply_text("â Days must be a number.")
        return
    expiry = time.time() + days * 24 * 3600
    user_access[user_id] = expiry
    save_json(USER_ACCESS_FILE, user_access)
    await update.message.reply_text(f"â Granted {days} days to {user_id}.")
    try:
        await context.bot.send_message(user_id, f"ð You got {days} days premium access.\nValid till {time.ctime(expiry)}.\nEnjoy!")
    except Exception:
        pass

# ------------------ WITHDRAW MESSAGE FLOW ------------------
async def handle_withdraw_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return False
    user_id = str(update.effective_user.id)
    text = (update.message.text or "").strip()
    wd = context.user_data.get("withdraw")
    if not wd:
        return False
    if wd.get("step") == "amount":
        try:
            amount = float(text)
            if amount < 50:
                await update.message.reply_text("â Minimum â¹50. Enter valid amount:")
                return True
            context.user_data["withdraw"]["amount"] = amount
            await update.message.reply_text("ð³Enter your Upi I'd/Number or Bank ð¦ account number with IFSC code:(â ï¸ Please type correct payment detail.")
            context.user_data["withdraw"]["step"] = "upi"
            return True
        except:
            await update.message.reply_text("â Invalid amount. Enter valid number:")
            return True
    elif wd.get("step") == "upi":
        upi_id = text
        amount = context.user_data["withdraw"]["amount"]
        coins_needed = rupees_to_coins(amount)
        if get_wallet_balance(user_id) < coins_needed:
            await update.message.reply_text(f"â Not enough coins. Wallet: {format_coins_rupees(get_wallet_balance(user_id))}")
            context.user_data.pop("withdraw", None)
            return True
        if deduct_coins(user_id, coins_needed):
            rid = uuid.uuid4().hex[:12]
            withdraw_requests[rid] = {"user_id": user_id, "amount": amount, "upi_id": upi_id, "status": "pending", "timestamp": time.time()}
            save_all()
            await update.message.reply_text(f"â Withdraw request submitted. Request ID: {rid} (â ï¸If Payment details is Incorrectð¤¦ Instant Notify to admin- @anshchaube852)")
            context.user_data.pop("withdraw", None)
            try:
                await context.bot.send_message(ADMIN_USER_ID, f"ð Withdraw request:\nUser: {user_id}\nAmount: â¹{amount}\nUPI: {upi_id}\nRequest ID: {rid}\nUse /withdraw {rid} to approve.")
            except Exception:
                pass
        else:
            await update.message.reply_text("â Failed to deduct coins.")
            context.user_data.pop("withdraw", None)
        return True
    return False

# ------------------ DASHBOARD / LEADERBOARD SENDERS ------------------
def format_coins_rupees(coins):
    rupees = coins_to_rupees(coins)
    return f"{coins} coins (â¹{rupees:.2f})"

async def send_user_dashboard(user_id: str, context: ContextTypes.DEFAULT_TYPE, query=None):
    name = "User"
    try:
        chat = await context.bot.get_chat(int(user_id))
        name = chat.first_name or chat.username or "User"
    except Exception:
        pass
    rank = get_user_rank(user_id)
    rank_text = "Not ranked today" if rank is None else f"#{rank}"
    streak = user_streak.get(user_id, {}).get("streak", 0)
    total_refers = 0
    for token, rec in referrals.items():
        if str(rec.get("owner")) == user_id:
            total_refers = len(rec.get("used_by", []))
            break
    wallet = get_wallet_balance(user_id)
    withdraws = sum(float(r["amount"]) for r in user_history.get(user_id, {}).get("withdraw", []))
    text = (
        f"ð Dashboard for {name}\n\n"
        f"ð Rank: {rank_text}\n"
        f"ð¥ Streak (days): {streak}\n"
        f"ð¥ Referrals: {total_refers}\n"
        f"ð° Wallet: {format_coins_rupees(wallet)}\n"
        f"ð¸ Total Withdrawn: â¹{withdraws:.2f}\n"
    )
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Withdraw", callback_data="withdraw"), InlineKeyboardButton("Get Premium", callback_data="show_plans")]])
    if query:
        await query.edit_message_text(text, reply_markup=kb)
    else:
        await context.bot.send_message(int(user_id), text, reply_markup=kb)

async def send_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE, from_button=False):
    leaderboard = get_daily_leaderboard()
    if not leaderboard:
        msg = "No leaderboard data available today."
    else:
        lines = []
        for i, (uid, score) in enumerate(leaderboard, start=1):
            try:
                chat = await context.bot.get_chat(int(uid))
                name = chat.first_name or chat.username or str(uid)
            except Exception:
                name = str(uid)
            lines.append(f"{i}. {name} - {score} coins")
        req_user_id = str(update.effective_user.id)
        rank = get_user_rank(req_user_id)
        user_line = "You are not ranked today." if rank is None else f"Your Rank: #{rank} | Coins: {get_wallet_balance(req_user_id)}"
        msg = "ð Top 10 Movie Searchers Today:\n\n" + "\n".join(lines) + "\n\n" + user_line
    if from_button and update.callback_query:
        await update.callback_query.edit_message_text(msg)
    else:
        await update.message.reply_text(msg)

async def user_dashboard_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â Not allowed.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /dashboard <user_id>")
        return
    uid = context.args[0]
    await send_user_dashboard(uid, context, query=None)

async def wallet_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â Not allowed.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /wallet <user_id> <amount>")
        return
    uid = context.args[0]
    try:
        amount = int(context.args[1])
    except:
        await update.message.reply_text("â Amount integer.")
        return
    user_wallet[uid] = amount
    save_all()
    await update.message.reply_text(f"â Wallet {uid} set to {amount} coins.")

async def activity_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â Not allowed.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /activity <user_id>")
        return
    uid = context.args[0]
    history = get_user_history(uid)
    today = today_str()
    coins_today = sum(rec["amount"] for rec in history.get("earn", []) if datetime.fromtimestamp(rec["timestamp"], timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d") == today)
    await update.message.reply_text(f"User {uid} earned {coins_today} coins today.")

async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    if update.effective_user.id == ADMIN_USER_ID and context.args:
        uid = context.args[0]
    hist = get_user_history(uid)
    txt = f"ð History for {uid}:\n\n"
    if hist.get("premium"):
        txt += "ð Premium:\n"
        for p in hist["premium"][-10:]:
            dt = datetime.fromtimestamp(p["timestamp"]).strftime("%Y-%m-%d %H:%M")
            txt += f"- {dt}: {p['plan']} for {format_coins_rupees(p['paid_coins'])}\n"
    if hist.get("earn"):
        txt += "\nðª Earned:\n"
        for e in hist["earn"][-10:]:
            dt = datetime.fromtimestamp(e["timestamp"]).strftime("%Y-%m-%d %H:%M")
            txt += f"- {dt}: +{e['amount']} ({e['reason']})\n"
    await update.message.reply_text(txt)

async def withdraw_approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â Not allowed.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /withdraw <request_id>")
        return
    rid = context.args[0]
    req = withdraw_requests.get(rid)
    if not req:
        await update.message.reply_text("â Invalid request id.")
        return
    if req["status"] != "pending":
        await update.message.reply_text("â Already processed.")
        return
    req["status"] = "approved"
    save_all()
    uid = req["user_id"]
    amt = req["amount"]
    try:
        await context.bot.send_message(int(uid), f"â Your withdrawal of â¹{amt} approved.")
    except Exception:
        pass
    await update.message.reply_text(f"â Withdrawal {rid} approved.")


async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.channel_post
    if not msg:
        return
    try:
        if msg.chat and msg.chat.id == CHANNEL_ID:
            if not (msg.video or msg.document or msg.audio or msg.photo):
                return

            raw_caption = msg.caption or ""
            if not raw_caption and msg.document and getattr(msg.document, "file_name", None):
                raw_caption = msg.document.file_name

            clean_title = await get_ai_clean_title(raw_caption)
            if clean_title:
                key = clean_title.lower()
                movies_db[key] = msg.message_id
                save_json(MOVIES_DB_FILE, movies_db)
                print(f"ð¤ AI Auto-saved: {clean_title} -> {msg.message_id}")
    except Exception as e:
        print("â handle_channel_post error:", e)
# ------------------ MESSAGE (SEARCH) HANDLER ------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None:
        return
    # withdraw flow
    if await handle_withdraw_messages(update, context):
        return
    user_id = str(update.effective_user.id)
    now = time.time()
    expiry = user_access.get(user_id, 0)
    if expiry < now:
        kb = [
            [InlineKeyboardButton("Get Free 24h Access", url=FREE_ACCESS_URL)],
            [InlineKeyboardButton("Buy Premium (QR)", callback_data="show_qr_plans")],
            [InlineKeyboardButton("ðââï¸ Refer", callback_data="refer")]
        ]
        await update.message.reply_text("â³ Your access expired. Get free 24h access or buy premium.", reply_markup=InlineKeyboardMarkup(kb))
        return
    if user_id not in verified_users:
        kb = [
            [InlineKeyboardButton("ð° Join Channel", url=f"https://t.me/{CHANNEL_USERNAME}")],
            [InlineKeyboardButton("â Verify", callback_data="verify")]
        ]
        await update.message.reply_text("ð Join the channel and Verify to continue.", reply_markup=InlineKeyboardMarkup(kb))
        return
    last = last_request_time.get(user_id, 0)
    if now - last < USER_COOLDOWN:
        await update.message.reply_text(f"â³ Please wait {int(USER_COOLDOWN - (now-last))}s before next request.")
        return
    last_request_time[user_id] = now
    query_raw = (update.message.text or "").strip()
    query = normalize_title(query_raw)
    if not query:
        await update.message.reply_text("â Please type a movie name.")
        return

    # Referral reward on first search
    for token, rec in referrals.items():
        if user_id in rec.get("used_by", []):
            owner = str(rec.get("owner"))
            if rec.get("referral_completed", []) and user_id in rec["referral_completed"]:
                break
            history_earn = user_history.get(owner, {}).get("earn", [])
            rewarded = any(f"referral bonus to {user_id}" in e.get("reason", "") for e in history_earn)
            if not rewarded:
                add_coins(owner, 100, f"Referral bonus to {user_id}")
                rec.setdefault("referral_completed", []).append(user_id)
                referrals[token] = rec
                save_json(REFERRALS_FILE, referrals)
                try:
                    await context.bot.send_message(int(owner), f"ð Your friend (ID: {user_id}) searched first movie! +100 coins added.")
                except Exception:
                    pass
            break

    # 1 coin per search
    add_coins(user_id, 1, f"Movie search coin for '{query_raw}'")

    # daily and streak
    streak = update_user_streak(user_id)
    daily_given = check_and_give_daily_coins(user_id)
    if daily_given:
        try:
            await context.bot.send_message(int(user_id), "ð You received +10 coins for today's first search!")
        except Exception:
            pass
    check_jackpot_streak(user_id, streak)

    # exact match
    if query in movies_db:
        msg_id = movies_db[query]
        try:
            try:
                await context.bot.send_chat_action(update.effective_chat.id, "typing")
            except Exception:
                pass
            sent = await context.bot.copy_message(chat_id=update.effective_chat.id, from_chat_id=CHANNEL_ID, message_id=msg_id)
            if expiry > now:
                record_sent_message_for_deletion(user_id, sent)
                try:
                    asyncio.create_task(delete_after(context, update.effective_chat.id, sent.message_id, user_id, delay=DELETE_DELAY))
                except Exception as e:
                    print("Failed to schedule delete:", e)
                await context.bot.send_message(update.effective_chat.id, "â ï¸ This file will be deleted after 10 minutes. Forward to saved messages.")
            else:
                await context.bot.send_message(update.effective_chat.id, "ð You don't have active access. Use /start or get free access.")
            return
        except Exception as e:
            print("Error copying exact-match:", e)
            await update.message.reply_text("â Could not send file right now.")
            return

    # advanced hybrid search
    matches = find_advanced_matches(query, movies_db.keys(), limit=25, score_cutoff=60)
    if matches:
        cleanup_search_sessions()
        token = make_search_token()
        search_sessions[token] = {"user_id": user_id, "suggestions": matches, "ts": now}
        kb = []
        # compress displayed text (shorten if too long)
        for i, suggested in enumerate(matches[:25]):
            text_display = suggested if len(suggested) < 60 else suggested[:57] + "..."
            kb.append([InlineKeyboardButton(text_display, callback_data=f"confirm:{token}:{i}")])
        kb.append([InlineKeyboardButton("ð Try Again", callback_data="try_again")])
        # nicer premium-like message
        await update.message.reply_text(
            "ð Similar movies found:\n\nSelect one from below or try again:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    else:
        kb = [[InlineKeyboardButton("ð Try Again", callback_data="try_again")]]
        await update.message.reply_text(
            "â Movie not found.\n\n"
            "ð Please check the spelling and try again.\n"
            "â³ If the movie name is correct but still not found, please wait a few minutes - it may be indexed soon.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        # notify admin about missing movie
        try:
            await context.bot.send_message(
                ADMIN_USER_ID,
                f"â Movie not found request from {update.effective_user.full_name} ({user_id}): {query_raw}"
            )
        except Exception as e:
            print("Failed to notify admin about missing movie:", e)

# ------------------ ADMIN COMMANDS ------------------
async def list_movies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not allowed to use this command.")
        return
    try:
        lines = [f"{name} -> {mid}" for name, mid in movies_db.items()]
        text = "Indexed movies:\n\n" + ("\n".join(lines) if lines else "(no movies indexed)")
        if len(text) > 4000:
            path = "movies_list.txt"
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            await update.message.reply_document(open(path, "rb"))
            os.remove(path)
        else:
            await update.message.reply_text(text)
    except Exception as e:
        print("list_movies error:", e)
        await update.message.reply_text("â ï¸ Error retrieving movies list.")

async def remove_movie(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not allowed to use this command.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /removemovie <name>")
        return
    name = normalize_title(" ".join(context.args))
    if name in movies_db:
        movies_db.pop(name, None)
        save_json(MOVIES_DB_FILE, movies_db)
        await update.message.reply_text(f"â Removed '{name}' from index.")
    else:
        await update.message.reply_text("â Movie not found in index.")

async def index_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not allowed to use this command.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /index <message_id> <name>")
        return
    try:
        mid = int(context.args[0])
        name = normalize_title(" ".join(context.args[1:]))
        movies_db[name] = mid
        save_json(MOVIES_DB_FILE, movies_db)
        await update.message.reply_text(f"â Indexed {name} -> {mid}")
    except Exception as e:
        print("index_message error:", e)
        await update.message.reply_text("â Error. Make sure message_id is a number and bot has access to that message.")

# Extra utility commands
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "ð¬ <b>Welcome to Movie Storm Bot!</b>\n"
        "<i>Earn coins while watching & searching your favourite movies!</i>\n\n"

        "Hereâs how to use the bot step-by-step ð\n"
        "ââââââââââââââââââââââââââââ\n\n"

        "ð <b>1. How to Search Movies</b>\n"
        "Just type the <b>movie name</b> in the chat.\n"
        "Example: <code>KGF</code>, <code>Pushpa</code>, <code>Avengers 2019</code>\n"
        "ð¦ The bot will instantly show you movie ð¿\n\n"

        "ð° <b>2. How to Earn Coins</b>\n"
        "You can earn coins in multiple ways ðµ\n\n"
        "â¢ ð¯ <b>Per Movie Search:</b> Get coins every time you search a movie.\n"
        "â¢ ð¥ <b>Refer & Earn:</b> Invite friends using your referral link and get <b>100 coins</b> per valid referral.\n"
        "â¢ ð <b>Leaderboard Rewards:</b> Top 10 users daily win <b>1000 bonus coins!</b>\n"
        "â¢ ð¥ <b>Streak Bonus:</b> Search daily to unlock extra rewards every 7th day!\n\n"
        "ð Check your wallet anytime using <code>/dashboard</code>\n\n"

        "ðï¸ <b>3. Use Your Coins</b>\n"
        "You can use your coins to unlock:\n"
        "â¢ ð« Premium Access ð\n"
        "â¢ ð Fast Search Mode\n"
        "â¢ ð Withdraw money ð° directly in <b>UPI ID / Number</b>\n\n"

        "ð¦ <b>4. How to Withdraw Money ð¤</b>\n"
        "Here are useful steps:\n\n"
        "1ï¸â£ Open your <b>Dashboard</b> ð â <code>/dashboard</code>\n"
        "2ï¸â£ Click on <b>Withdraw</b> button\n"
        "3ï¸â£ Enter amount <i>(minimum Rs.50)</i>\n"
        "4ï¸â£ Enter your correct <b>UPI ID / Number</b>\n"
        "ð <b>Note:</b> Please enter correct payment details â ï¸\n\n"
        "After withdrawal, you'll get your money ð° <b>within 2 hours!</b>\n\n"
        "â ï¸ Don't search the same movie <b>more than 2 times within 5 minutes</b> to increase coins.\n"
        "â ï¸ <b>Don't type random text</b> just to increase coins ð\n\n"

        "ð¤ <b>5. Need Help?</b>\n"
        "If you face any issue, just contact the admin ð â <a href='https://t.me/anshchaube852'>@anshchaube852</a>\n"
        "Watch the complete <b>tutorial video</b> ð¥ (https://t.me/movie_storm/70)\n"
        "Weâre here to help you ð¬ Donât worry âºï¸\n\n"

        "ð <b>Tip:</b> The more you search, the more you earn.\n"
        "Stay active & climb the leaderboard every day! ð"
    )

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=help_text,
        parse_mode="HTML"
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not allowed to use this command.")
        return
    total_users = len(user_access)
    verified_count = len(verified_users)
    total_movies = len(movies_db)
    await update.message.reply_text(f"Users with access: {total_users}\nVerified users: {verified_count}\nIndexed movies: {total_movies}")

async def set_withdrawal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Only admin can run this command
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not authorized to use this command.")
        return

    if len(context.args) < 2:
        await update.message.reply_text("Usage: /setwithdrawal <user_id> <new_total_withdrawal_amount>")
        return

    user_id = str(context.args[0])
    try:
        new_amount = float(context.args[1])
        if new_amount < 0:
            raise ValueError("Negative amount not allowed.")
    except ValueError:
        await update.message.reply_text("â Invalid amount. Please enter a non-negative number.")
        return

    # Update or create the user's withdrawal record (stored in user_history)
    if user_id not in user_history:
        user_history[user_id] = {"premium": [], "withdraw": [], "earn": []}

    # Clear current withdrawal history and add a total record as a single entry
    user_history[user_id]["withdraw"] = [{"timestamp": time.time(), "amount": new_amount, "note": "Admin adjusted total withdrawal"}]

    save_all()

    await update.message.reply_text(f"â User {user_id}'s total withdrawal amount has been set to â¹{new_amount:.2f}.")

    # Optionally notify the user
    try:
        await context.bot.send_message(int(user_id), f" Your total withdrawalð° amount is updated to â¹{new_amount:.2f}.")
    except Exception:
        pass

# Then register this handler in your main application setup:
# application.add_handler(CommandHandler("setwithdrawal", set_withdrawal))

async def refer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    owner_id = user_id
    token = ensure_user_has_token(owner_id)
    try:
        bot_me = await context.bot.get_me()
        bot_username = bot_me.username or ""
    except Exception:
        bot_username = ""
    if bot_username:
        ref_link = f"https://t.me/{bot_username}?start=ref_{token}"
    else:
        ref_link = f"Use start=ref_{token} with /start in the bot."
    caption = (
        f"Hey {update.effective_user.first_name or ''} ð"
        f"Your refer link - {ref_link}"
        "Share this link to friends and earn 100 coins when they start bot and search a movie."
    )
    share_text = f"Join this movie bot: {ref_link}"
    share_url = f"https://t.me/share/url?url={ref_link}&text={share_text}"
    try:
        await context.bot.send_photo(
            chat_id=update.effective_chat.id,
            photo="https://blogger.googleusercontent.com/img/b/R29vZ2xl/AVvXsEioyWaj_gqATrzMZOYDXTGia7v8H46u9cn05Q7r6b9bIzKE5D8rw-eMy3M1AmhR6B3XQzYp1LLE3gHYvTzk2rb9xAGfj5efN32GXo5XE8NSL_ezfZ6F9Vnpmf_zg3kGX4X1HLzcrmIb-Ru7V3QfVzNLbUKcV8VWvOHib8I4ml02QQuVC2aXXo4R7wtFEOD7/s1536/file_000000008e1861fd8e52e7276a77acf4.png",
            caption=caption,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Share to friends", url=share_url)]])
        )
        await update.message.reply_text("â Referral info sent. Share your link and earn coins!")
    except Exception as e:
        print("Failed to send refer image:", e)
        await update.message.reply_text("â Failed to send referral info. Try again later.")

async def chatbot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Check if user is admin
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not allowed to use this command.")
        return

    # Get message after /chatbot command
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /chatbot <message>")
        return

    message_text = " ".join(args)
    
    # Send message to all users who have access
    sent_count = 0
    failed_count = 0
    for user_id in user_access.keys():
        try:
            await context.bot.send_message(chat_id=int(user_id), text=message_text)
            sent_count += 1
        except Exception as e:
            print(f"Failed to send message to {user_id}: {e}")
            failed_count += 1

    await update.message.reply_text(f"â Message sent to {sent_count} users. Failed to send: {failed_count}")



# Load withdrawal records on startup


def save_withdraw_records():
    save_json(USER_WITHDRAW_RECORDS_FILE, user_withdraw_records)


async def record_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Admin-only command: /record <user_id> <amount>
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not allowed to use this command.")
        return

    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Usage: /record <user_id> <amount>")
        return

    user_id = args[0]
    amount_str = args[1]

    try:
        amount = float(amount_str)
    except ValueError:
        await update.message.reply_text("â Amount must be a number.")
        return

    date_str = today_str()  # Uses your existing today_str() function for IST date YYYY-MM-DD

    # Ensure user entry exists
    if user_id not in user_withdraw_records:
        user_withdraw_records[user_id] = []

    # Append withdrawal record
    user_withdraw_records[user_id].append({
        "date": date_str,
        "amount": amount
    })

    save_withdraw_records()
    await update.message.reply_text(
        f"â Recorded withdrawal for user {user_id} on {date_str} amount â¹{amount}"
    )


async def userrecord_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Admin-only command: /userrecord <user_id>
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not allowed to use this command.")
        return

    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Usage: /userrecord <user_id>")
        return

    user_id = args[0]

    records = user_withdraw_records.get(user_id)
    if not records:
        await update.message.reply_text(f"No withdrawal records found for user {user_id}.")
        return

    total_amount = sum(rec['amount'] for rec in records)

    lines = [f"ð Record of user {user_id}:\n"]
    for rec in records:
        lines.append(f"ð Date - {rec['date']}\nð° Withdraw amount - â¹{rec['amount']}\n")

    lines.append(f"ð§¾ Total Withdrawn: â¹{total_amount}")

    await update.message.reply_text("\n".join(lines))


# --- Add handlers for the new commands --- #

async def dash_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â You are not allowed to use this command.")
        return

    args = context.args
    if not args:
        await update.message.reply_text("Usage: /dash <user_id>")
        return

    user_id = args[0]
    await send_user_dash(user_id, context, update.message)


async def send_user_dash(user_id: str, context: ContextTypes.DEFAULT_TYPE, source):
    # Build dashboard text and keyboard
    wallet_balance = user_wallet.get(user_id, 0)
    streak_info = user_streak.get(user_id, {"streak": 0})
    streak = streak_info.get("streak", 0)
    history = get_user_history(user_id)

    text_lines = [
        f"ð Dashboard for user ID: {user_id}",
        f"ð° Wallet Balance: {wallet_balance} coins",
        f"ð¥ Current Streak: {streak} days",
        f"ð History Records: {len(history.get('earn', []))}",
    ]
    text = "\n".join(text_lines)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Back to Admin Menu", callback_data="admin_menu")]
    ])

    # Send or edit depending on source type
    if hasattr(source, "edit_message_text"):
        await source.edit_message_text(text=text, reply_markup=kb)
    elif hasattr(source, "reply_text"):
        await source.reply_text(text, reply_markup=kb)
    else:
        # fallback to sending message by chat_id if possible
        chat_id = getattr(source, "chat_id", None) or getattr(getattr(source, "chat", None), "id", None)
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)

# --- REDEEM CODES PERSISTENCE (add near top with other FILE constants) ---


# Load redeem codes (structure: code -> {"hours": int, "uses_left": int, "created_by": str, "created_at": ts, "redeemed_by": [user_ids]})


def save_redeem_codes():
    save_json(REDEEM_CODES_FILE, redeem_codes)

# ------------------ /redeem command ------------------
async def redeem_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Usage:
      /redeem <code>
    Grants access for the hours defined in the code and decrements uses_left.
    """
    if update.message is None:
        return
    user_id = str(update.effective_user.id)
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /redeem <code>")
        return
    code = args[0].strip()
    entry = redeem_codes.get(code)
    if not entry:
        await update.message.reply_text("â Invalid code.")
        return

    # Check if user already used this code
    already_used = False
    for record in entry.get("redeemed_by", []):
        if record.get("user_id") == user_id:
            already_used = True
            break
    if already_used:
        await update.message.reply_text("â ï¸ Youâve already used this code.")
        return

    # Check uses left
    uses_left = int(entry.get("uses_left", 0))
    if uses_left <= 0:
        await update.message.reply_text("â ï¸ This code has already been used up.")
        return

    # grant access
    hours = int(entry.get("hours", 2))
    expiry = time.time() + hours * 3600

    prev = float(user_access.get(user_id, 0))
    now_ts = time.time()
    if prev > now_ts:
        user_access[user_id] = prev + hours * 3600
        expiry = user_access[user_id]
    else:
        user_access[user_id] = expiry

    # Update code usage data
    entry["uses_left"] = max(0, uses_left - 1)
    entry.setdefault("redeemed_by", []).append({"user_id": user_id, "ts": time.time()})
    redeem_codes[code] = entry

    save_json(USER_ACCESS_FILE, user_access)
    save_redeem_codes()

    await update.message.reply_text(
        f"â Code accepted! You now have access for {hours} hour(s).\nð Valid till: {time.ctime(user_access[user_id])}"
    )

    # Notify admin
    try:
        await context.bot.send_message(
            ADMIN_USER_ID,
            f"ð Redeem used: code={code} by user={user_id} â expires {time.ctime(user_access[user_id])}",
        )
    except Exception:
        pass

# ------------------ Admin helpers ------------------
async def addcode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /addcode <code> <hours> [uses]
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â Not allowed.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addcode <code> <hours> [uses]")
        return
    code = context.args[0].strip()
    try:
        hours = int(context.args[1])
    except:
        await update.message.reply_text("â hours must be an integer.")
        return
    uses = 1
    if len(context.args) >= 3:
        try:
            uses = int(context.args[2])
        except:
            uses = 1
    redeem_codes[code] = {
        "hours": hours,
        "uses_left": uses,
        "created_by": str(update.effective_user.id),
        "created_at": time.time(),
        "redeemed_by": []
    }
    save_redeem_codes()
    await update.message.reply_text(f"â Code '{code}' added: {hours} hour(s), uses={uses}.")

async def listcodes_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â Not allowed.")
        return
    if not redeem_codes:
        await update.message.reply_text("(no redeem codes)")
        return
    lines = []
    for code, entry in redeem_codes.items():
        lines.append(f"{code} â {entry.get('hours',2)}h | uses_left={entry.get('uses_left',0)} | created_by={entry.get('created_by')}")
    # if too long, send as file
    text = "\n".join(lines)
    if len(text) > 4000:
        path = "redeem_codes_list.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        await update.message.reply_document(open(path, "rb"))
        os.remove(path)
    else:
        await update.message.reply_text(text)

async def removecode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("â Not allowed.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /removecode <code>")
        return
    code = context.args[0].strip()
    if code in redeem_codes:
        redeem_codes.pop(code, None)
        save_redeem_codes()
        await update.message.reply_text(f"â Removed code {code}.")
    else:
        await update.message.reply_text("â Code not found.")
# ------------------ RUN BOT ------------------
async def run_bot():
    while True:
        try:
            app = ApplicationBuilder().token(BOT_TOKEN).build()

            # Register handlers
            app.add_handler(CommandHandler("start", start))
            app.add_handler(CommandHandler("plans", plans))
            app.add_handler(CommandHandler("grant", grant))
            app.add_handler(CommandHandler("listmovies", list_movies))
            app.add_handler(CommandHandler("removemovie", remove_movie))
            app.add_handler(CommandHandler("index", index_message))
            app.add_handler(CommandHandler("help", help_command))
            app.add_handler(CommandHandler("stats", stats))
            app.add_handler(CommandHandler("history", history_command))
            app.add_handler(CommandHandler("leaderboard", send_leaderboard))
            app.add_handler(CommandHandler("dashboard", lambda u,c: send_user_dashboard(str(u.effective_user.id), c, None)))
            app.add_handler(CommandHandler("wallet", wallet_admin))
            app.add_handler(CommandHandler("activity", activity_admin))
            app.add_handler(CommandHandler("withdraw", withdraw_approve))
            app.add_handler(CommandHandler("dashboard", user_dashboard_admin))
            app.add_handler(CommandHandler("setwithdrawal", set_withdrawal))
            app.add_handler(CommandHandler("refer", refer))
            app.add_handler(CommandHandler("chatbot", chatbot))
            app.add_handler(CommandHandler("record", record_command))
            app.add_handler(CommandHandler("userrecord", userrecord_command))
            app.add_handler(CommandHandler("dash", dash_command))
            app.add_handler(CommandHandler("redeem", redeem_command))
            app.add_handler(CommandHandler("addcode", addcode_command))
            app.add_handler(CommandHandler("listcodes", listcodes_command))
            app.add_handler(CommandHandler("removecode", removecode_command))

            app.add_handler(CallbackQueryHandler(button_handler))

            # Channel posts (media) - index media posted in channel
            app.add_handler(MessageHandler(filters.ALL & (filters.VIDEO | filters.Document.ALL | filters.PHOTO | filters.AUDIO), handle_channel_post))
            # User messages (search)
            app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

            # Index channel history once (best-effort)
            await index_old_channel_messages(app)

            # Start leaderboard scheduler
            try:
                asyncio.create_task(schedule_daily_leaderboard_rewards(app))
                print("â Leaderboard scheduler started.")
            except Exception as e:
                print("â ï¸ Failed to start scheduler:", e)

            print("â Bot running...")
            await app.run_polling()
        except Conflict:
            print("â Conflict: token used elsewhere. Retrying in 15s...")
            await asyncio.sleep(15)
        except RetryAfter as ra:
            wait = getattr(ra, "retry_after", 10)
            print(f"â³ Rate limit. Waiting {wait}s...")
            await asyncio.sleep(wait)
        except TelegramError as te:
            print("â ï¸ Telegram error:", te)
            await asyncio.sleep(10)
        except Exception as e:
            print("ð¥ Unknown error:", e)
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        import nest_asyncio
        nest_asyncio.apply()

        import asyncio
        loop = asyncio.get_event_loop()
        loop.create_task(run_bot())
        loop.run_forever()
    except KeyboardInterrupt:
        print("⏹️ Stopping bot, saving data...")
        save_all()
