import logging
from multiprocessing import context
import sqlite3
import pytz
import re
import asyncio
import os
import json
import signal
import asyncio
import httpx
import time
import html
from telegram.constants import ChatMemberStatus
from telegram import error
from functools import wraps
from datetime import datetime, time as dtime
from typing import List
from urllib.parse import urlparse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, error
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =====================
# Logging
# =====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

try:
    IST = pytz.timezone('Asia/Kolkata')
except pytz.UnknownTimeZoneError:
    logger.error("Could not load timezone 'Asia/Kolkata'. Please check pytz installation.")
    # आप चाहें तो बॉट को यहाँ बंद कर सकते हैं या एक डिफ़ॉल्ट टाइमज़ोन का उपयोग कर सकते हैं
    IST = None 

# =====================
# Config
# =====================
# नया और सुरक्षित तरीका
#TOKEN = "8321548453:AAGpAqCwwpnuc6KBmWyeVIOueK-cHFeLLGw"  # DS
TOKEN = "7877055449:AAErHVB_Lhupl2_4jg0J68_zYrPfGZF9ZRQ" #SUDHANSHU 
#ADMIN_IDS = [5865209445]           # अपने Telegram User IDs
#ADMIN_IDS = [8099474031] # ✔️ DS
ADMIN_IDS = [7644128376] #sudhansu

#OWNER_ID = 8099474031 # अपना ID
OWNER_ID = 7644128376 # SUDHANSU


#-1003131533605
# -1003070978442

BACKUP_CHAT_ID = -1003070978442 # आपका backup चैनल/ग्रुप ID

# अगर TOKEN या OWNER_ID नहीं मिला तो बॉट को क्रैश कर दें
if not TOKEN or not OWNER_ID:
    raise ValueError("BOT_TOKEN and OWNER_ID environment variables must be set.")


MAX_CHANNELS_PER_BUTTON = 20
MAX_TIMES_PER_BUTTON = 22
BATCH_SIZE = 29

DB_NAME = "bot_data.db"


 
# =====================
# SQLite only for Metadata (channels, schedules, users)
# =====================
def init_db() -> None:
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # messages table हटाया गया है (Redis queue use हो रही है)
    c.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY,
            button_id TEXT,
            channel_id TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY,
            button_id TEXT,
            schedule_time TEXT  -- 'HH:MM'
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            role TEXT DEFAULT 'user',
            is_authorized BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

      # --- यहाँ नई messages टेबल जोड़ें ---
    c.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        button_id TEXT NOT NULL,
        content TEXT,
        media_type TEXT NOT NULL,
        file_id TEXT,
        status TEXT DEFAULT 'pending' -- 'pending', 'sent'
    )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS special_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            button_id TEXT NOT NULL,
            set_number INTEGER NOT NULL,
            content TEXT,
            media_type TEXT,
            file_id TEXT,
            UNIQUE(button_id, set_number)
        )
    """)
    # ------------------------------------

    c.execute("CREATE INDEX IF NOT EXISTS idx_channels_button ON channels(button_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_schedules_button ON schedules(button_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_users_authorized ON users(is_authorized)")
      # --- नई टेबल के लिए इंडेक्स जोड़ें ---
    c.execute("CREATE INDEX IF NOT EXISTS idx_messages_button_status ON messages(button_id, status)")
    
    # ------------------------------------
           # ==== YEH NAYA INDEX ADD KAREN ====
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_button_pending
        ON messages(button_id, status)
        WHERE status='pending'
    """)
    # ===================================


    conn.commit()
    conn.close()

# Async DB helpers (run sync sqlite in executor safely)
async def db_fetchall(query: str, params=()):
    loop = asyncio.get_event_loop()

    def _do():
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        return rows

    return await loop.run_in_executor(None, _do)


async def db_execute(query: str, params=()):
    loop = asyncio.get_event_loop()

    def _do():
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        c.execute(query, params)
        conn.commit()
        conn.close()

    await loop.run_in_executor(None, _do)

# Utils
# =====================
def is_valid_time_str(s: str) -> bool:
    try:
        parts = s.split(":")
        if len(parts) != 2:
            return False
        h, m = int(parts[0]), int(parts[1])
        return 0 <= h <= 23 and 0 <= m <= 59
    except Exception:
        return False


# ==== DB Snapshot Helper ====
async def send_current_db_snapshot(bot, chat_id: int, caption: str = "DB snapshot"):
    try:
        if os.path.exists(DB_NAME):
            await bot.send_document(
                chat_id=chat_id,
                document=open(DB_NAME, "rb"),
                caption=caption
            )
        else:
            await bot.send_message(chat_id, "⚠️ DB फ़ाइल नहीं मिली, snapshot भेजना संभव नहीं।")
    except Exception as e:
        logger.error(f"DB snapshot भेजने में त्रुटि: {e}")


def owner_only(func):
    """
    यह डेकोरेटर सुनिश्चित करता है कि केवल बॉट का ओनर ही कमांड का उपयोग कर सके।
    अगर कोई और उपयोगकर्ता कोशिश करता है, तो ओनर को सुरक्षित रूप से सूचना भेजी जाती है।
    """
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = getattr(update, 'effective_user', None)

        if not user or user.id != OWNER_ID:
            if update.effective_message:
                # उपयोगकर्ता की जानकारी को HTML के लिए सुरक्षित करें
                user_full_name = html.escape(user.full_name)
                user_username = f"@{html.escape(user.username)}" if user.username else "N/A"

                # संदेश की जानकारी को HTML के लिए सुरक्षित करें
                message_content = ""
                if update.effective_message.text:
                    message_content = f"✉️ <b>Message:</b>\n{html.escape(update.effective_message.text)}"
                elif update.effective_message.caption:
                    message_content = f"🖼️ <b>Caption:</b>\n{html.escape(update.effective_message.caption)}"
                elif update.effective_message.sticker:
                    message_content = "💌 User sent a <b>sticker</b>."
                elif update.effective_message.photo:
                    message_content = "🖼️ User sent a <b>photo</b>."
                else:
                    message_content = "❓ User sent an unsupported message type."

                # ओनर को भेजने के लिए पूरा संदेश तैयार करें
                notification_text = (
                    "⚠️ <b>Unauthorized User Alert</b> ⚠️\n\n"
                    f"👤 <b>User:</b> {user_full_name} ({user_username})\n"
                    f"🆔 <b>ID:</b> <code>{user.id}</code>\n\n"
                    f"{message_content}"
                )

                # सभी एडमिन्स/ओनर को सूचना भेजें
                for admin_id in ADMIN_IDS:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=notification_text,
                            parse_mode='HTML'  # <-- सबसे महत्वपूर्ण बदलाव
                        )
                    except Exception as e:
                        logger.error(f"Owner alert failed: {e}")

            # अनधिकृत उपयोगकर्ता को जवाब दें
            if update.effective_message:
                await update.effective_message.reply_text(
                    "❌ आप इस बॉट को use नहीं कर सकते!\nकृपया Owner से contact करें।"
                )
            return

        # अगर उपयोगकर्ता ओनर है, तो मूल फ़ंक्शन चलाएं
        return await func(update, context, *args, **kwargs)
    return wrapper


# =====================
# Status (async) - uses Redis and SQLite
# =====================
async def get_button_status(button_id: str) -> str:
    channels = await db_fetchall("SELECT channel_id FROM channels WHERE button_id=?", (button_id,))
    schedules = await db_fetchall("SELECT schedule_time FROM schedules WHERE button_id=? ORDER BY schedule_time", (button_id,))
    
    # --- SQLite से पेंडिंग संदेशों की गिनती करें ---
    pending_rows = await db_fetchall("SELECT COUNT(*) FROM messages WHERE button_id=? AND status='pending'", (button_id,))
    pending = pending_rows[0][0] if pending_rows else 0
    # ----------------------------------------------

    status = []
    status.append(f"बटन {button_id} स्टेटस:")
    status.append("")
    status.append(f"चैनल्स: {len(channels)}")
    status.append(f"पेंडिंग मैसेजेस: {pending}")
    status.append("शेड्यूल टाइम्स:")
    if schedules:
        for i, row in enumerate(schedules, 1):
            status.append(f"{i}. {row[0]}")
    else:
        status.append("(कोई टाइम सेट नहीं)")
    return "\n".join(status)

# =====================
# Message sending with exponential backoff
async def send_message_with_backoff(bot, chat_id, text, media_type, file_id, button_id):
    """
    मैसेज भेजने के लिए एक मज़बूत फ़ंक्शन जो नेटवर्क एरर और फ्लड कंट्रोल को संभालता है।
    """
    retry_wait = 2
    max_retries = 5
    attempt = 0

    while attempt < max_retries:
        attempt += 1
        try:
            if media_type == 'text':
                await bot.send_message(chat_id=chat_id, text=text, read_timeout=60, write_timeout=60, connect_timeout=60)
            elif media_type == 'photo':
                await bot.send_photo(chat_id=chat_id, photo=file_id, caption=text or None, read_timeout=60, write_timeout=60, connect_timeout=60)
            elif media_type == 'video':
                await bot.send_video(chat_id=chat_id, video=file_id, caption=text or None, read_timeout=60, write_timeout=60, connect_timeout=60)
            elif media_type == 'document':
                await bot.send_document(chat_id=chat_id, document=file_id, caption=text or None, read_timeout=60, write_timeout=60, connect_timeout=60)
            else:
                logger.warning(f"Unknown media_type {media_type}")
            
            # सफलतापूर्वक भेजा गया, तो लूप से बाहर निकलें
            return
        except error.Forbidden as e:
            logger.error(f"Forbidden error for chat_id {chat_id}: {e}")
            
                # --- YAHAN BADLAV KAREN ---
            
            # 1. डिलीट बटन बनाएं
            keyboard = [
                [InlineKeyboardButton("✅ हटाएं (Delete)", callback_data=f"final_del_{button_id}_{chat_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

                # 2. बटन के साथ मैसेज भेजें
            await bot.send_message(
                OWNER_ID,
                f"🚨 **बॉट चैनल का सदस्य नहीं है!**\n\n**ग्रुप:** `{button_id}`\n**चैनल ID:** `{chat_id}`\n\nइस चैनल को तुरंत हटाने के लिए नीचे दिए गए बटन को दबाएं।",
                reply_markup=reply_markup
            )
                
            notified_set.add(chat_id)

        except error.RetryAfter as e:
            # टेलीग्राम ने जितने समय के लिए कहा है, उतने समय रुकें
            retry_seconds = e.retry_after
            logger.warning(f"Flood control exceeded for {chat_id}. Retrying in {retry_seconds} seconds.")
            await asyncio.sleep(retry_seconds)
            
        except (error.TimedOut, httpx.ReadError, httpx.ConnectError) as e:
            # नेटवर्क एरर के लिए फिर से कोशिश करें
            logger.warning(f"Network error on attempt {attempt} for {chat_id}: {e}. Retrying in {retry_wait}s...")
            await asyncio.sleep(retry_wait)
            retry_wait = min(retry_wait * 2, 60) # इंतज़ार का समय बढ़ाएं

        except Exception as e:
            # किसी अन्य एरर के लिए लॉग करें और बाहर निकलें
            logger.error(f"An unhandled error occurred for {chat_id}: {e}", exc_info=True)
            break
    
    logger.error(f"Failed to send message to {chat_id} after {max_retries} attempts.")




# =====================
# Empty Queue Reminder (5 minutes)
# =====================
async def empty_queue_reminder(context: ContextTypes.DEFAULT_TYPE):
    """यह जॉब बार-बार यह याद दिलाने के लिए चलता है कि एक क्यू खाली है।"""
    data = context.job.data or {}
    button_id = data.get("button_id")
    notify_chat_id = data.get("notify_chat_id")

    if not button_id or not notify_chat_id:
        logger.warning("empty_queue_reminder को जरूरी डेटा के बिना कॉल किया गया। जॉब हटाया जा रहा है।")
        if context.job:
            context.job.schedule_removal()
        return

    try:
        # रिमाइंडर भेजने से ठीक पहले डेटाबेस में जांच करें।
        pending_rows = await db_fetchall("SELECT COUNT(*) FROM messages WHERE button_id=? AND status='pending'", (button_id,))
        pending_count = pending_rows[0][0] if pending_rows else 0

        if pending_count == 0:
            # अगर क्यू अभी भी खाली है, तो रिमाइंडर भेजें।
            print(f"DEBUG: {button_id} के लिए 'अभी भी खाली' रिमाइंडर भेजा जा रहा है।")
            await context.bot.send_message(
                notify_chat_id,
                f"⏰ रिमाइंडर: बटन {button_id} की क्यू अभी भी खाली है।"
            )
        else:
            # अगर मैसेज जोड़ दिए गए हैं, तो इस जॉब की अब कोई जरूरत नहीं है।
            print(f"DEBUG: {button_id} की क्यू में अब {pending_count} मैसेज हैं। रिमाइंडर जॉब हटाया जा रहा है।")
            if context.job:
                context.job.schedule_removal()

    except Exception as e:
        logger.error(f"empty_queue_reminder में त्रुटि ({button_id}): {e}")
        print(f"DEBUG: रिमाइंडर जॉब विफल ({button_id}): {e}")

async def _start_empty_queue_notification(context: ContextTypes.DEFAULT_TYPE, button_id: str, notify_chat_id: int):
    """
    यह जांचता है कि रिमाइंडर पहले से चल रहा है या नहीं, और अगर नहीं, तो
    सूचना भेजकर एक नया रिपीटिंग रिमाइंडर जॉब शुरू करता है।
    """
    # जांचें कि इस बटन के लिए रिमाइंडर पहले से चल रहा है या नहीं
    jobs = context.job_queue.get_jobs_by_name(f"empty_notify_{button_id}")
    if not jobs:
        try:
            # केवल तभी सूचना और जॉब शेड्यूल करें जब पहले से कोई न हो
            await context.bot.send_message(
                notify_chat_id,
                f"⚠️ सूचना: बटन {button_id} की मैसेज क्यू अब खाली है। अब से हर 30 सेकंड में रिमाइंडर भेजा जाएगा।"
            )
            context.job_queue.run_repeating(
                empty_queue_reminder,
                interval=300,  # टेस्टिंग के बाद इसे 300 (5 मिनट) करें
                first=300,     # टेस्टिंग के बाद इसे 300 (5 मिनट) करें
                name=f"empty_notify_{button_id}",
                data={"button_id": button_id, "notify_chat_id": notify_chat_id}
            )
            print(f"DEBUG: {button_id} के लिए खाली क्यू सूचना भेजी गई और रिमाइंडर जॉब शेड्यूल किया गया।")
        except Exception as e:
            print(f"DEBUG: हेल्पर फ़ंक्शन में सूचना भेजने या जॉब शेड्यूल करने में विफल: {e}")
    else:
        print(f"DEBUG: {button_id} के लिए रिमाइंडर पहले से ही सक्रिय है, कोई कार्रवाई नहीं की गई।")

# =====================
# Forwarding Job - uses Redis queue

# =====================

async def forward_messages_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    button_id = data.get("button_id")
    notify_chat_id = data.get("notify_chat_id")
    sched_time = data.get("time")
    print(f"DEBUG: Running forward job: button_id={button_id} at {sched_time}")

    if 'current_set' not in context.bot_data:
        context.bot_data['current_set'] = {}

    current_set = context.bot_data['current_set'].get(button_id, 1)
    
    # Jab aap tasks banate hain, to set ko pass karen
    
    try:
        channels_rows = await db_fetchall("SELECT channel_id FROM channels WHERE button_id=?", (button_id,))
        channels = [r[0] for r in channels_rows]
        if not channels:
            if notify_chat_id:
                await context.bot.send_message(notify_chat_id, f"⚠️ {button_id}: फॉरवर्डिंग असफल! कोई चैनल नहीं जुड़ा है।")
        
            return
        

        messages_to_send = await db_fetchall(
            "SELECT id, content, media_type, file_id FROM messages WHERE button_id=? AND status='pending' ORDER BY id ASC LIMIT ?",
            (button_id, 29)
        )
        
        if messages_to_send:
            # अगर मैसेज हैं, तो किसी भी मौजूदा रिमाइंडर को हटा दें।
            jobs = context.job_queue.get_jobs_by_name(f"empty_notify_{button_id}")
            if jobs:
                for job in jobs:
                    job.schedule_removal()
                print(f"DEBUG: {button_id} के लिए रिमाइंडर हटाया गया क्योंकि क्यू में मैसेज हैं।")
        else:
            # अगर क्यू शुरू से ही खाली है, तो सूचना प्रक्रिया शुरू करें और जॉब से बाहर निकल जाएं।
            await _start_empty_queue_notification(context, button_id, notify_chat_id)
            return

        # --- मैसेज भेजने की प्रक्रिया ---
        sent_count = 0
        sent_message_ids = []
        for msg_id, content, media_type, file_id in messages_to_send:
            tasks = [send_message_with_backoff(context.bot, ch, content, media_type, file_id, button_id) for ch in channels]
            await asyncio.gather(*tasks)
            sent_message_ids.append(msg_id)
            sent_count += 1
            if sent_count % 5 == 0:
                await asyncio.sleep(1)
        # --------------------------------
        special_messages = await db_fetchall(
            "SELECT content, media_type, file_id FROM special_messages WHERE button_id = ? AND set_number = ?",
            (button_id, current_set)
        )

        if special_messages:
            tasks = []
            for content, media_type, file_id in special_messages:
                for ch in channels:
                    tasks.append(send_message_with_backoff(context.bot, ch, content, media_type, file_id, button_id))
            await asyncio.gather(*tasks)

        # भेजे गए मैसेजों को DB से डिलीट करें
        if sent_message_ids:
            placeholders = ','.join('?' for _ in sent_message_ids)
            await db_execute(f"DELETE FROM messages WHERE id IN ({placeholders})", tuple(sent_message_ids))

        # अगली बार के लिए सेट को टॉगल करें
        next_set = 2 if current_set == 1 else 1
        context.bot_data['current_set'][button_id] = next_set

        if notify_chat_id:
            await context.bot.send_message(
                notify_chat_id,
                f"✅ {button_id}: {sent_count} सामान्य + 1 विशेष (सेट {current_set}) मैसेज भेजे गए। अगली बार सेट {next_set} चलेगा।"
            )

        # ==== NEW: शेड्यूल बैच के तुरंत बाद DB snapshot ====
        
        target_chat = BACKUP_CHAT_ID if BACKUP_CHAT_ID != 0 else notify_chat_id
        ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
        await send_current_db_snapshot(
            context.bot,
            target_chat,
            caption=f"📦 DB snapshot | {button_id} | {sched_time} | sent={sent_count} | {ts} IST"
        )

        # --- सबसे महत्वपूर्ण जांच ---
        # अब जांचें कि क्या यह जॉब खत्म होने के बाद क्यू खाली हो गई है।
        remaining_rows = await db_fetchall("SELECT COUNT(*) FROM messages WHERE button_id=? AND status='pending'", (button_id,))
        remaining_count = remaining_rows[0][0] if remaining_rows else 0

        if remaining_count == 0:
            print(f"DEBUG: {button_id} की क्यू फॉरवर्डिंग के बाद खाली हो गई है। सूचना प्रक्रिया शुरू की जा रही है।")
            # अगर क्यू खाली हो गई है, तो सूचना प्रक्रिया शुरू करें।
            await _start_empty_queue_notification(context, button_id, notify_chat_id)

    except Exception as e:
        logger.exception(f"फॉरवर्ड जॉब में गंभीर त्रुटि: {button_id}")
        if notify_chat_id:
            await context.bot.send_message(notify_chat_id, f"❌ {button_id}: फॉरवर्डिंग में त्रुटि: {str(e)}")



@owner_only
async def prompt_set_special_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """विशेष मैसेज के लिए सेट चुनने का विकल्प देता है।"""
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]

    keyboard = [
        [InlineKeyboardButton("सेट 1 (पहले/तीसरे शेड्यूल में)", callback_data=f"add_special_{button_id}_1")],
        [InlineKeyboardButton("सेट 2 (दूसरे/चौथे शेड्यूल में)", callback_data=f"add_special_{button_id}_2")],
        [InlineKeyboardButton("🔙 वापस", callback_data={button_id})]
    ]
    
    await query.edit_message_text(
        f"बटन `{button_id}` के लिए कौन सा विशेष मैसेज सेट बदलना चाहते हैं?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

@owner_only
async def add_special_message_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """विशेष मैसेज (फोटो + कैप्शन) भेजने का निर्देश देता है।"""
    query = update.callback_query
    await query.answer()
    
    # नया pattern: add_special_btn1_1 या add_special_btn2_2
    parts = query.data.split("_")
    button_id = parts[2]  # btn1, btn2, etc.
    set_number = parts[3]  # 1 या 2
    
    context.user_data["action"] = f"add_special_message_{button_id}_{set_number}"
    
    keyboard = [
        [InlineKeyboardButton("🔙 वापस", callback_data=f"{button_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # ------------------------------------

    
    await query.edit_message_text(
        f"बटन `{button_id}` के **सेट {set_number}** के लिए एक फोटो और कैप्शन भेजें। यह मैसेज 29 सामान्य मैसेज के बाद भेजा जाएगा।",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )
# =====================
# Bot UI Handlers
# =====================

# --- सबसे पहले यह 'start' फंक्शन यहाँ जोड़ें ---
@owner_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [
            InlineKeyboardButton("👉 US", callback_data="btn1"),
            InlineKeyboardButton("👉 IOS", callback_data="btn2")
        ],
        [
            InlineKeyboardButton("👉 SFB", callback_data="btn3"),
            InlineKeyboardButton("👉 personal", callback_data="btn4")
        ],
        [
            InlineKeyboardButton("बटन 5", callback_data="btn5"),
            InlineKeyboardButton("बटन 6", callback_data="btn6")
        ],
        [
            InlineKeyboardButton("बटन 7", callback_data="btn7"),
            InlineKeyboardButton("बटन 8", callback_data="btn8")
        ],
        [InlineKeyboardButton("📥 DB अपलोड करें", callback_data="upload_db")], # <-- यह लाइन जोड़ें
        [InlineKeyboardButton("📤 DB डाउनलोड करें", callback_data="download_db")]  # <-- यह लाइन जोड़ें
    ]
    
    await update.message.reply_text(
        "मुख्य मेनू:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# --- इसके बाद आपका नया 'back_to_main_menu' फंक्शन आएगा ---
@owner_only
async def back_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    keyboard = [
        [
            InlineKeyboardButton("👉 US", callback_data="btn1"),
            InlineKeyboardButton("👉 IOS", callback_data="btn2")
        ],
        [
            InlineKeyboardButton("👉 SFB", callback_data="btn3"),
            InlineKeyboardButton("👉 personal", callback_data="btn4")
        ],
        [
            InlineKeyboardButton("बटन 5", callback_data="btn5"),
            InlineKeyboardButton("बटन 6", callback_data="btn6")
        ],
        [
            InlineKeyboardButton("बटन 7", callback_data="btn7"),
            InlineKeyboardButton("बटन 8", callback_data="btn8")
        ],
        [InlineKeyboardButton("📥 DB अपलोड करें", callback_data="upload_db")], # <-- यह लाइन जोड़ें
        [InlineKeyboardButton("📤 DB डाउनलोड करें", callback_data="download_db")]  # <-- यह लाइन जोड़ें
    ]
    
    await query.edit_message_text(
        "मुख्य मेनू:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# --- और फिर बाकी के फंक्शन जैसे 'open_button' आदि ---

@owner_only
async def open_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data
    keyboard = [
        [
            InlineKeyboardButton("➕ चैनल जोड़ें ➕", callback_data=f"add_chn_{button_id}"),
            InlineKeyboardButton("💢 चैनल हटाएं 💢", callback_data=f"del_chn_{button_id}")
        ],
        [
            InlineKeyboardButton("💌 मैसेज जोड़ें 💌", callback_data=f"add_msg_{button_id}"),
            InlineKeyboardButton("🗑️ मैसेज डिलीट करें", callback_data=f"del_msg_{button_id}")
        ],
        [
            InlineKeyboardButton("📜 मैसेज देखें", callback_data=f"list_msg_{button_id}"),
            InlineKeyboardButton("✔ स्टेटस देखें 🎦", callback_data=f"status_{button_id}")
        ],
        [
            InlineKeyboardButton("🕕 टाइम सेट करें 🕛", callback_data=f"set_time_{button_id}"),
            InlineKeyboardButton("➰ फॉरवर्डिंग शुरू करें ➰", callback_data=f"start_fw_{button_id}")
        ],
        [
            InlineKeyboardButton("✨ विशेष मैसेज सेट 1", callback_data=f"add_special_{button_id}_1"),
            InlineKeyboardButton("✨ विशेष मैसेज सेट 2", callback_data=f"add_special_{button_id}_2"),
        ],
        [
            InlineKeyboardButton("🔙 वापस मेनू में", callback_data="main_menu")
        ]
    ]
    status_text = await get_button_status(button_id)
    await query.edit_message_text(status_text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    
@owner_only
async def add_channels_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    context.user_data["action"] = f"add_channels_{button_id}"
     # --- "वापस" बटन के लिए कीबोर्ड बनाएं ---
    keyboard = [
        [InlineKeyboardButton("🔙 वापस", callback_data=f"{button_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # ------------------------------------

    await query.edit_message_text(
        f"बटन {button_id} मे चैनल जोड़ने के लिए  ID भेजें\n(एक लाइन में एक, @channel या -100... फॉर्मेट):",
        reply_markup=reply_markup
    )
    #await query.edit_message_text("चैनल ID भेजें (एक लाइन में एक, @channel या -100... फॉर्मेट):")

@owner_only
async def delete_channel_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT channel_id FROM channels WHERE button_id=?", (button_id,))
    channels = [row[0] for row in c.fetchall()]
    conn.close()

    if not channels:
        logger.warning(f"No channels found for button {button_id}")
        await query.edit_message_text("❌ इस बटन में कोई चैनल नहीं है")
        return

    keyboard: List[List[InlineKeyboardButton]] = []
    for ch in channels:
        keyboard.append([InlineKeyboardButton(f"{ch}", callback_data=f"confirm_del_{button_id}_{ch}")])
    keyboard.append([InlineKeyboardButton(" 🔙वापस", callback_data=f"{button_id}")])

    await query.edit_message_text("निम्नलिखित चैनल्स में से हटाने के लिए चुनें:", reply_markup=InlineKeyboardMarkup(keyboard))

@owner_only
async def confirm_delete_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, _, button_id, channel = query.data.split("_", 3)
    keyboard = [
        [InlineKeyboardButton("✅ हाँ, हटाएं", callback_data=f"final_del_{button_id}_{channel}")],
        [InlineKeyboardButton("❌ नहीं", callback_data=f"{button_id}")],
    ]
    await query.edit_message_text(f"क्या आप वाकई चैनल {channel} को {button_id} मे से हटाना चाहते हैं?", reply_markup=InlineKeyboardMarkup(keyboard))

@owner_only
async def final_delete_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, _, button_id, channel = query.data.split("_", 3)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM channels WHERE button_id=? AND channel_id=?", (button_id, channel))
    conn.commit()
    conn.close()
     # --- "वापस" बटन के लिए कीबोर्ड बनाएं ---
    keyboard = [
        [InlineKeyboardButton("🔙 वापस", callback_data=f"{button_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # ------------------------------------

    await query.edit_message_text(
        f"✅ चैनल {channel} सफलतापूर्वक हटाया गया:",
        reply_markup=reply_markup
    )
    keyboard = [
        [InlineKeyboardButton("🔙 वापस", callback_data=f"{button_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # ------------------------------------

@owner_only
async def add_messages_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    context.user_data["action"] = f"add_messages_{button_id}"
     # --- "वापस" बटन के लिए कीबोर्ड बनाएं ---
    keyboard = [
        [InlineKeyboardButton("🔙 वापस", callback_data=f"{button_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # ------------------------------------

    await query.edit_message_text(
        f"बटन {button_id} मे 🔜मैसेज भेजें (टेक्स्ट/फोटो/डॉक्युमेंट/वीडियो) भेज सकते हैं:",
        reply_markup=reply_markup
    )
    #await query.edit_message_text ("🔜 मैसेज भेजें (टेक्स्ट/फोटो/डॉक्युमेंट/वीडियो) भेज सकते हैं:")

@owner_only
async def delete_messages_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """उपयोगकर्ता को मैसेज आईडी दर्ज करने का निर्देश देता है।"""
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    
    context.user_data["action"] = f"delete_messages_{button_id}"
    
    keyboard = [
        [InlineKeyboardButton("🔙 वापस", callback_data=button_id)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        text=(
            f"बटन `{button_id}` से मैसेज डिलीट करने के लिए ID भेजें।\n\n"
            "आप तीन तरीकों से ID दे सकते हैं:\n"
            "1.  **सिंगल ID:** `5`\n"
            "2.  **कई IDs:** `5, 10, 18`\n"
            "3.  **ID की रेंज:** `10-20`\n\n"
            "कृपया एक लाइन में एक ही कमांड का उपयोग करें।"
        ),
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )
  

@owner_only
async def list_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """सिर्फ recently added 100 messages दिखाता है।"""
    query = update.callback_query
    try:
        await query.answer()
    except error.BadRequest as e:
        if "query is too old" in str(e).lower():
            await update.effective_message.reply_text("⚠️ Request timeout हो गया। कृपया फिर से try करें।")
            return

    button_id = query.data.split("_")[-1]  # btn1, btn2, etc.

    # Loading message show karo
    await query.edit_message_text(
        text=f"⏳ {button_id} के recent messages load हो रहे हैं...",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 वापस", callback_data=button_id)]])
    )

    # सिर्फ recently added 60 messages लो (सबसे नए पहले)
    messages_rows = await db_fetchall(
        "SELECT id, media_type, content FROM messages WHERE button_id=? AND status='pending' ORDER BY id DESC LIMIT 60",
        (button_id,)
    )

    if not messages_rows:
        keyboard = [[InlineKeyboardButton("🔙 वापस", callback_data=button_id)]]
        await query.edit_message_text(
            text=f"ℹ️ बटन `{button_id}` के लिए कोई पेंडिंग मैसेज नहीं है।",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return

    response_text = [f"**बटन `{button_id}` - Recent {len(messages_rows)} Messages**\n"]
    response_text.append("(नए से पुराने क्रम में)\n")

    for msg_id, media_type, content in messages_rows:
        display_content = (content or f"({media_type})").strip()
        if len(display_content) > 30:
            display_content = display_content[:30] + "..."

        response_text.append(f"• `ID: {msg_id}` | {display_content}")

    keyboard = [[InlineKeyboardButton("🔙 वापस", callback_data=button_id)]]

    full_text = "\n".join(response_text)

    # Telegram length limit check
    if len(full_text) > 4000:
        full_text = full_text[:4000] + "\n... (and more)"

    try:
        await query.edit_message_text(
            text=full_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    except error.BadRequest as e:
        if "query is too old" in str(e).lower():
            await update.effective_message.reply_text("⌛️ Session expired. Use '/start' फिर से try करें।")
        elif "Message_too_long" in str(e):
            # Simple text without formatting
            simple_text = full_text.replace('*', '').replace('`', '').replace('_', '')
            simple_text = simple_text[:4000] + "\n... (truncated)"
            await query.edit_message_text(text=simple_text, reply_markup=InlineKeyboardMarkup(keyboard))
            
    
@owner_only
async def set_times_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]
    context.user_data["action"] = f"set_times_{button_id}"
     # --- "वापस" बटन के लिए कीबोर्ड बनाएं ---
    keyboard = [
        [InlineKeyboardButton("🔙 वापस", callback_data=f"{button_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    # ------------------------------------

    await query.edit_message_text(
        f" 🕛{button_id} मे टाइम भेजें (HH:MM फॉर्मेट में, एक लाइन में एक टाइम ):",
        reply_markup=reply_markup
    )
    #await query.edit_message_text("टाइम भेजें (HH:MM फॉर्मेट में, एक लाइन में एक):")

@owner_only
async def start_forwarding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # start_forwarding फ़ंक्शन में

    query = update.callback_query
    await query.answer()
    button_id = query.data.split("_")[-1]

    rows = await db_fetchall("SELECT schedule_time FROM schedules WHERE button_id=?", (button_id,))
    times = [r[0] for r in rows]
    if not times:
         # --- "वापस" बटन के लिए कीबोर्ड बनाएं ---
        keyboard = [
            [InlineKeyboardButton("🔙 वापस", callback_data=f"{button_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        # ------------------------------------

        await query.edit_message_text(
            " पहले टाइम सेट करें!:",
            reply_markup=reply_markup
        )
        #await query.edit_message_text("❌ पहले टाइम सेट करें!")
        return

    # cancel existing jobs for this button
    all_jobs = context.job_queue.jobs()
    for job in all_jobs:
        if job.name and job.name.startswith(f"job_{button_id}_"):
            job.schedule_removal()
    
       # ... (for job in all_jobs: ... के बाद)
    created = 0
    for t in times:
        try:
            h, m = map(int, t.split(":"))
            when = dtime(hour=h, minute=m, tzinfo=IST)
            
            context.job_queue.run_daily(
                forward_messages_job,
                time=when,
                name=f"job_{button_id}_{t}",
                data={"button_id": button_id, "notify_chat_id": query.message.chat_id, "time": t},
                job_kwargs={'misfire_grace_time': 300} 
            )
            created += 1
        except (ValueError, IndexError):
            logger.warning(f"Invalid time format found: {t}. Skipping.")
            continue # अगर टाइम फॉर्मेट गलत है तो अगले पर जाएं
    # ... (for लूप के बाद)
    
        if created > 0:
             # --- "वापस" बटन के लिए कीबोर्ड बनाएं ---
            keyboard = [
                [InlineKeyboardButton("🔙 वापस", callback_data=f"{button_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            # ------------------------------------

            await query.edit_message_text(
                f" ✅ फॉरवर्डिंग शुरू! {created} टाइम्स पर \n⏭ {button_id} मे से मैसेज भेजे जाएंगे।:",
                reply_markup=reply_markup
            )
            #await query.edit_message_tex ("✅ फॉरवर्डिंग शुरू! {created} टाइम्स पर मैसेज भेजे जाएंगे।")
        else:
            await query.edit_message_text("❌ कोई भी वैध टाइम शेड्यूल नहीं किया जा सका। कृपया सही HH:MM फॉर्मेट में टाइम भेजें।")
            # ------------------------------------
            keyboard = [
                [InlineKeyboardButton("🔙 वापस", callback_data=f"{button_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            # ------------------------------------

async def is_bot_admin_in_channel(bot, channel_id: str) -> bool:
    """यह जांचता है कि बॉट दिए गए चैनल में एडमिन है या नहीं।"""
    try:
        bot_user = await bot.get_me()
        chat_member = await bot.get_chat_member(chat_id=channel_id, user_id=bot_user.id)
        
        # अगर बॉट एडमिन या क्रिएटर है तो True लौटाएं
        if chat_member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
            return True
        else:
            return False
    except (error.BadRequest, error.Forbidden):
        # यह एरर तब आता है जब चैनल मौजूद नहीं है या बॉट उसका सदस्य नहीं है।
        return False
    except Exception as e:
        logger.error(f"चैनल {channel_id} में एडमिन स्थिति की जांच करते समय त्रुटि: {e}")
        return False



@owner_only
async def handle_all_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    यह फंक्शन ओनर द्वारा भेजे गए सभी प्राइवेट मैसेज को हैंडल करता है।
    """
    if not update.message or update.message.chat.type != 'private':
        return

    action = context.user_data.get("action")
    if not action:
        return

    message = update.message
    
    # --- चैनल जोड़ने का लॉजिक ---
    # --- इस पूरे 'if' ब्लॉक को बदलें ---
    if action.startswith("add_channels_"):
        button_id = action.split("_")[-1]
        lines = [ln.strip() for ln in (message.text or "").splitlines() if ln.strip()]
        added = 0
        for ch in lines:
            if ch.startswith("@") or (ch.startswith("-") and ch[1:].isdigit()):
                await db_execute("INSERT INTO channels (button_id, channel_id) VALUES (?, ?)", (button_id, ch))
                added += 1
        context.user_data.pop("action", None)
        keyboard = [[InlineKeyboardButton("🔙 वापस", callback_data=button_id)]]
        await message.reply_text(
            f"{button_id} मे ✅ {added} चैनल सफलतापूर्वक जोड़े गए!✅",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    # --- टाइम सेट करने का लॉजिक ---
    elif action.startswith("set_times_"):
        button_id = action.split("_")[-1]
        lines = [ln.strip() for ln in (message.text or "").splitlines() if ln.strip()]
        valid_times = [t for t in lines if is_valid_time_str(t)][:MAX_TIMES_PER_BUTTON]
        await db_execute("DELETE FROM schedules WHERE button_id=?", (button_id,))
        for t in valid_times:
            await db_execute("INSERT INTO schedules (button_id, schedule_time) VALUES (?, ?)", (button_id, t))
        context.user_data.pop("action", None)
        keyboard = [[InlineKeyboardButton("🔙 वापस", callback_data=button_id)]]
        await message.reply_text(
            f"{button_id} मे ✅ {len(valid_times)} टाइम सेट किए गए!✅",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # --- सामान्य मैसेज जोड़ने का लॉजिक ---
    elif action.startswith("add_messages_"):
        button_id = action.split("_")[-1]
        content = message.text or message.caption
        media_type = 'text'
        file_id = None
        if message.photo:
            media_type = 'photo'
            file_id = message.photo[-1].file_id
        elif message.video:
            media_type = 'video'
            file_id = message.video.file_id
        elif message.document:
            media_type = 'document'
            file_id = message.document.file_id
        
        if not content and not file_id:
            await message.reply_text("⚠️ संदेश खाली है!")
            return
            
        await db_execute(
            "INSERT INTO messages (button_id, content, media_type, file_id, status) VALUES (?, ?, ?, ?, 'pending')",
            (button_id, content, media_type, file_id)
        )
        pending_count_rows = await db_fetchall("SELECT COUNT(*) FROM messages WHERE button_id=? AND status='pending'", (button_id,))
        pending_count = pending_count_rows[0][0]
        jobs = context.job_queue.get_jobs_by_name(f"empty_notify_{button_id}")
        for j in jobs:
            j.schedule_removal()
        keyboard = [[InlineKeyboardButton("🔙 वापस", callback_data=button_id)]]
        # मैसेज add होने के बाद
        jobs = context.job_queue.get_jobs_by_name(f"empty_notify_{button_id}")
        for job in jobs:
            job.schedule_removal()
        msg_id_rows = await db_fetchall("SELECT id FROM messages WHERE button_id=? ORDER BY id DESC LIMIT 1", (button_id,))
        msg_id = msg_id_rows[0][0] if msg_id_rows else 'N/A'
        
        await message.reply_text(
            f"{button_id} मे ✅ मैसेज जोड़ा गया! (कुल पेंडिंग: {pending_count}\n• `ID: {msg_id}` |)✅",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # --- मैसेज डिलीट करने का लॉजिक ---
    elif action.startswith("delete_messages_"):
        button_id = action.split("_")[-1]
        text_input = (message.text or "").strip()
        if not text_input:
            await message.reply_text("❌ कृपया डिलीट करने के लिए कोई ID भेजें।")
            return
        
        ids_to_process = set()
        try:
            parts = [p.strip() for p in text_input.split(',') if p.strip()]
            for part in parts:
                if '-' in part:
                    start_str, end_str = part.split('-', 1)
                    start, end = int(start_str.strip()), int(end_str.strip())
                    ids_to_process.update(range(start, end + 1))
                else:
                    ids_to_process.add(int(part))
        except ValueError:
            await message.reply_text("❌ अमान्य फॉर्मेट!")
            return

        if not ids_to_process:
            await message.reply_text("❌ कोई वैध ID नहीं मिली।")
            return
        
        id_list = sorted(list(ids_to_process))
        placeholders = ', '.join('?' for _ in id_list)
        
        rows = await db_fetchall(f"SELECT COUNT(*) FROM messages WHERE button_id = ? AND id IN ({placeholders})", tuple([button_id] + id_list))
        deleted_count = rows[0][0] if rows else 0
        
        if deleted_count > 0:
            await db_execute(f"DELETE FROM messages WHERE button_id = ? AND id IN ({placeholders})", tuple([button_id] + id_list))
            await message.reply_text(f"✅ {deleted_count} मैसेज सफलतापूर्वक डिलीट कर दिए गए हैं।")
        else:
            await message.reply_text("ℹ️ इन IDs के लिए कोई मैसेज नहीं मिला।")

        context.user_data.pop("action", None)

        # ==== NEW: डिलीट के तुरंत बाद DB snapshot ====
        target_chat = BACKUP_CHAT_ID if BACKUP_CHAT_ID != 0 else message.chat_id
        await send_current_db_snapshot(context.bot, target_chat, caption=f"🗑️ DB snapshot after delete | {button_id} | deleted={deleted_count}")



    # --- विशेष मैसेज जोड़ने का लॉजिक ---
    elif action.startswith("add_special_message_"):
        _, _, _, button_id, set_number = action.split("_")
        
        content = message.text or message.caption
        media_type = 'text'
        file_id = None
        
        if message.photo:
            media_type = 'photo'
            file_id = message.photo[-1].file_id
        elif message.video:
            media_type = 'video'
            file_id = message.video.file_id
        elif message.document:
            media_type = 'document'
            file_id = message.document.file_id
            
        if not content and not file_id:
            await message.reply_text("⚠️ कुछ तो भेजें!")
            return

        # Purane message ko replace karein (Agar already hai to)
        await db_execute(
            "DELETE FROM special_messages WHERE button_id = ? AND set_number = ?",
            (button_id, int(set_number))
        )
        
        await db_execute(
            "INSERT INTO special_messages (button_id, set_number, content, media_type, file_id) VALUES (?, ?, ?, ?, ?)",
            (button_id, int(set_number), content, media_type, file_id)
        )

        context.user_data.pop("action", None)
        keyboard = [[InlineKeyboardButton("🔙 वापस", callback_data=f"set_special_{button_id}")]]
        
        await message.reply_text(
            f"✅ बटन `{button_id}` के लिए **सेट {set_number}** का विशेष मैसेज सफलतापूर्वक सेट कर दिया गया है।",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
     # --- नया ब्लॉक: डेटाबेस अपलोड का लॉजिक ---
    elif action == "upload_db":
        if not message.document:
            await message.reply_text("❌ कृपया एक दस्तावेज़ (document) भेजें।")
            return

        file_name = message.document.file_name
        if not file_name.endswith(('.db', '.sqlite', '.sqlite3')):
            await message.reply_text("❌ अमान्य फ़ाइल प्रकार। कृपया `.db`, `.sqlite`, या `.sqlite3` एक्सटेंशन वाली फ़ाइल भेजें।")
            return

        # पुरानी DB का बैकअप लें
        if os.path.exists(DB_NAME):
            os.rename(DB_NAME, DB_NAME + ".bak")
            print(f"पुराने डेटाबेस का बैकअप '{DB_NAME}.bak' के रूप में बनाया गया।")

        # नई फ़ाइल डाउनलोड करें
        db_file = await message.document.get_file()
        await db_file.download_to_drive(DB_NAME)
        
        context.user_data.pop("action", None)
        keyboard = [[InlineKeyboardButton("🔙 वापस मेनू में", callback_data="main_menu")]]
        await message.reply_text(
            f"✅ डेटाबेस सफलतापूर्वक `{DB_NAME}` के रूप में अपलोड और बदल दिया गया है।\n"
            "बॉट को रीस्टार्ट करने की सलाह दी जाती है।",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


# =====================
# Database & JSON File Management
# =====================

@owner_only
async def prompt_upload_db(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """DB अपलोड करने के लिए निर्देश देता है।"""
    query = update.callback_query
    await query.answer()
    context.user_data["action"] = "upload_db"
    keyboard = [[InlineKeyboardButton("🔙 वापस मेनू में", callback_data="main_menu")]]
    await query.edit_message_text(
        "कृपया नई डेटाबेस (.db) फ़ाइल भेजें।\n"
        "⚠️ **चेतावनी:** यह मौजूदा डेटाबेस को बदल देगा।",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def send_db_backup(context: ContextTypes.DEFAULT_TYPE):
    db_file = "bot_data.db"  # ya aap apni required file ka naam yahan de
    tg_channel_id = -1003131533605  # Datastore/backup channel ka chat_id (negative value wali ID dalen)
    if os.path.exists(db_file):
        try:
            await context.bot.send_document(
                chat_id=tg_channel_id,
                document=open(db_file, "rb"),
                caption="Auto hourly DB backup"
            )
        except Exception as e:
            print(f"Failed to send backup: {e}")


@owner_only
async def download_db(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """डेटाबेस फ़ाइल भेजता है।"""
    query = update.callback_query
    await query.answer()
    try:
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(DB_NAME, 'rb'),
            caption=f"{DB_NAME} डेटाबेस फ़ाइल।"
        )
    except FileNotFoundError:
        await query.message.reply_text("❌ डेटाबेस फ़ाइल नहीं मिली।")
    except Exception as e:
        await query.message.reply_text(f"❌ फ़ाइल भेजने में त्रुटि: {e}")

def setup_initial_files():
    """यह सुनिश्चित करता है कि DB और JSON फ़ाइलें मौजूद हैं।"""
    # SQLite DB के लिए init_db() पहले से ही यह काम करता है
    # हम बस यह सुनिश्चित करते हैं कि यह main() में कॉल हो।
    
    # JSON फ़ाइल बनाएं अगर वह मौजूद नहीं है
    json_filename = "data.json"
    if not os.path.exists(json_filename):
        print(f"'{json_filename}' नहीं मिली, एक नई फ़ाइल बना रहा हूँ।")
        with open(json_filename, 'w') as f:
            f.write("{}") # एक खाली JSON ऑब्जेक्ट
            
# ... आपका error_handler और main() फ़ंक्शन यहाँ से शुरू होगा ...


# =====================
# Error Handler
# =====================


def setup_signal_handlers(app, bot, admin_ids, db_name):
    loop = asyncio.get_event_loop()
    
    async def send_db_on_shutdown():
        for admin_id in admin_ids:
            try:
                await bot.send_document(admin_id, open(db_name, 'rb'), caption="Bot shutdown हो रहा है। डेटाबेस फाइल।")
            except Exception as e:
                print(f"Shutdown DB send error: {e}")

    def handler(signum, frame):
        print("Shutdown signal captured, sending DB...")
        loop.create_task(send_db_on_shutdown())
        # फिर बॉट बंद करें, जैसे:
        loop.stop()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Exception while handling an update: {context.error}")
    print(f"DEBUG: Error occurred with update object: {update}")

    try:
        msg = f"Error: {context.error}\n"
        if update and hasattr(update, "effective_user") and update.effective_user:
            msg += f"User: {update.effective_user.id}\n"
        if update and hasattr(update, 'message') and update.message:
            txt = update.message.text or update.message.caption
            if txt:
                msg += f"Msg: {txt[:300]}\n"

        for admin_id in ADMIN_IDS:
            await context.bot.send_message(admin_id, f"⚠️ Bot Error Alert:\n{msg}")

        # डेटाबेस फाइल भेजें
        if os.path.exists(DB_NAME):
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_document(admin_id, open(DB_NAME, 'rb'), caption="⚠️ बॉट क्रैश हो गया! यह आपकी डेटाबेस फ़ाइल है।")
                except Exception as file_err:
                    logger.error(f"Failed to send DB file to admin {admin_id}: {file_err}")

    except Exception as ex:
        logger.error(f"Failed sending error alert or DB file: {ex}")


# =====================
# Main
# =====================
def main() -> None:
    init_db()
    app = Application.builder().token(TOKEN).pool_timeout(60).connect_timeout(60).read_timeout(60).build()

    # UI handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(back_to_main_menu, pattern=r"^main_menu$"))
    # --- नए हैंडलर यहाँ जोड़ें ---
    app.add_handler(CallbackQueryHandler(prompt_upload_db, pattern=r"^upload_db$"))
    app.add_handler(CallbackQueryHandler(download_db, pattern=r"^download_db$"))
    
    app.add_handler(CallbackQueryHandler(open_button, pattern=r"^btn[1-8]$"))
    app.add_handler(CallbackQueryHandler(add_channels_prompt, pattern=r"^add_chn_"))
    app.add_handler(CallbackQueryHandler(delete_channel_menu, pattern=r"^del_chn_"))
    app.add_handler(CallbackQueryHandler(confirm_delete_channel, pattern=r"^confirm_del_"))
    app.add_handler(CallbackQueryHandler(final_delete_channel, pattern=r"^final_del_"))
    app.add_handler(CallbackQueryHandler(add_messages_prompt, pattern=r"^add_msg_"))
    app.add_handler(CallbackQueryHandler(delete_messages_prompt, pattern=r"^del_msg_"))  # <-- यह नई लाइन जोड़ें
    app.add_handler(CallbackQueryHandler(list_messages, pattern=r"^list_msg_"))  # <-- यह नई लाइन जोड़ें
    app.add_handler(CallbackQueryHandler(set_times_prompt, pattern=r"^set_time_"))
    app.add_handler(CallbackQueryHandler(start_forwarding, pattern=r"^start_fw_"))
    app.add_handler(CallbackQueryHandler(add_special_message_prompt, pattern=r"^add_special_btn[1-8]_1$"))
    app.add_handler(CallbackQueryHandler(add_special_message_prompt, pattern=r"^add_special_btn[1-8]_2$"))
    # Messages
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, handle_all_messages))
      # signal handlers सेट करें
    setup_signal_handlers(app, app.bot, ADMIN_IDS, DB_NAME)
    
    # Har 1 ghante repeat: backup job add karo!
    app.job_queue.run_repeating(
        send_db_backup,
        interval=3600,      # 1 hour = 3600 seconds, 300 5 min 
        first=30,           # Bot start hone ke 10s baad first backup (apni requirement ke anusar)
        name="auto_db_backup"
    )

    # Error handler
    app.add_error_handler(error_handler)

    logger.info("Bot started…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
#    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()






























