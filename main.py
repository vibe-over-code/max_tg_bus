from os import name as os_name, getenv
import asyncio
from asyncio import run, wait, create_task, FIRST_COMPLETED, Event, get_running_loop
from logging import getLogger, DEBUG
import signal
from datetime import datetime, time as t
from io import BytesIO
import gc

import aiohttp
from aiohttp import TCPConnector
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command
from aiogram.types import BufferedInputFile

from pymax import SocketMaxClient, MaxClient, Message
from pymax.types import FileAttach, PhotoAttach, VideoAttach

import data_handler
from logger import setup_logger

import os
import uuid
import aiofiles
from aiogram.types import FSInputFile

# --- Initial Setup ---
setup_logger()
l = getLogger()  # Use root logger with DEBUG level
l.setLevel(DEBUG)
load_dotenv()

# --- Constants & Configuration ---
CHECK_TIME = False # проверять ли время перед отправкой сообщения (если да, то давать ошибку если START_TIME <= now <= END_TIME)
START_TIME = t(7, 0)
END_TIME = t(22, 0)

BOT_POST_MESSAGE = None # доп текст в сообщении от бота
BOT_MESSAGE_PREFIX = "⫻" # префикс для отпарвляемых сообщений
BOT_START_MESSAGE = None # стартовое сообщение бота отпралвляемое в макс при запуске (если None, то не отпралвять)

REQUESTS_TIMEOUT = 15 # таймаут запросов

# --- Memory limits ---
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB — лимит Telegram, не грузим больше
MAX_MSGS_MAP_ENTRIES = 50000  # максимальное количество записей в msgs_map
FLUSH_INTERVAL = 500  # сбрасывать data.json каждые N сообщений
FLUSH_COUNTER = 0  # счётчик для периодического сброса

# --- Environment Variables ---
try:
    USE_SOCKET_CLIENT = eval(getenv('USE_SOCKET_CLIENT', 'False').title())
    MAX_PHONE = getenv('VK_PHONE')
    MAX_TOKEN = getenv('VK_COOKIE')
    TG_TOKEN = getenv('TG_TOKEN')
    ADMIN_USER_ID = int(getenv('ADMIN_USER_ID', 0))
    TG_PROXY = getenv('TG_PROXY', '') 
    if not all([TG_TOKEN, MAX_TOKEN, MAX_PHONE]):
        raise ValueError("One or more environment variables are not set.")

    assert TG_TOKEN
    assert MAX_PHONE

    mapping_str = getenv('CHAT_MAPPING', '')
    if not mapping_str.strip():
        raise ValueError("CHAT_MAPPING is not set. Format: TG_ID[/THREAD_ID]:MAX_ID,...")

    chat_pairs = []  
    for pair in mapping_str.split(','):
        pair = pair.strip()
        if not pair:
            continue
        parts = pair.split(':')
        if len(parts) != 2:
            raise ValueError(f"Invalid CHAT_MAPPING pair: {pair}. Expected TG_ID[/THREAD_ID]:MAX_ID")
        
        tg_part = parts[0].strip()
        max_id = int(parts[1].strip())
        
        # Разбор топика, если он указан через слеш
        if '/' in tg_part:
            tg_id_str, thread_id_str = tg_part.split('/', 1)
            tg_id = int(tg_id_str)
            thread_id = int(thread_id_str)
        else:
            tg_id = int(tg_part)
            thread_id = None

        chat_pairs.append((tg_id, thread_id, max_id))

    if not chat_pairs:
        raise ValueError("CHAT_MAPPING produced no valid pairs.")

    tg_chat_ids = list(set(t[0] for t in chat_pairs))  
    max_to_tg = {}  # max_id -> (tg_id, thread_id)
    tg_to_max = {}  # (tg_id, thread_id) -> [max_id, ...]
    
    for tg_id, thread_id, max_id in chat_pairs:
        max_to_tg[max_id] = (tg_id, thread_id)
        tg_to_max.setdefault((tg_id, thread_id), []).append(max_id)

    l.info(f"Loaded {len(chat_pairs)} chat pair(s) across {len(tg_chat_ids)} TG chat(s):")
    for (tg_id, thread_id), max_list in tg_to_max.items():
        max_list_str = ', '.join(str(m) for m in max_list)
        thread_info = f" (Topic: {thread_id})" if thread_id else ""
        l.info(f"  TG {tg_id}{thread_info} <- MAX [{max_list_str}]")

except (ValueError, TypeError) as e:
    l.critical(f"FATAL: Configuration error - {e}. Please check your .env file.")
    quit(1)

# --- Create Bot with optional proxy ---
if TG_PROXY:
    l.info(f"Using Telegram proxy: {TG_PROXY}")
    proxy_session = AiohttpSession(proxy=TG_PROXY)
else:
    l.info("No Telegram proxy configured, using direct connection")
    proxy_session = AiohttpSession()
bot = None  # Will be created in main()
dp = Dispatcher()

# --- Fix: Disable NOTIF_MESSAGE ack (opcode 128) ---
# pymax's _send_notification_response sends an opcode 128 ack that the server rejects,
# causing the WebSocket to disconnect. Override with a no-op.
async def _noop_notification_response(chat_id: int, message_id: str) -> None:
    """No-op replacement for _send_notification_response to prevent server disconnect."""
    pass

# Reconnect=True effectively replaces the "Watchdog" thread
if USE_SOCKET_CLIENT:
    client = SocketMaxClient(MAX_PHONE, token=MAX_TOKEN, work_dir="data/cache", reconnect=True)
else:
    client = MaxClient(MAX_PHONE, token=MAX_TOKEN, work_dir="data/cache", reconnect=True)

# Disable the buggy NOTIF_MESSAGE ack that disconnects the server
client._send_notification_response = _noop_notification_response

msgs_map = data_handler.load('msgs') or {}
last_sender_id = {}  # {max_chat_id: sender_id}
def get_last_sender_id(max_id):
    """Получить last_sender_id для конкретного MAX чата."""
    return last_sender_id.get(max_id)
def set_last_sender_id(max_id, sender_id):
    """Установить last_sender_id для конкретного MAX чата."""
    last_sender_id[max_id] = sender_id
def clear_last_sender_id(max_id):
    """Очистить last_sender_id для конкретного MAX чата."""
    last_sender_id.pop(max_id, None)

def get_tg_chat_for_max(max_id):
    """Найти TG чат и топик для данного MAX чата."""
    return max_to_tg.get(max_id)

def get_max_chats_for_tg(tg_id, thread_id=None):
    """Найти список MAX чатов для данного TG чата (и топика)."""
    if (tg_id, thread_id) in tg_to_max:
        return tg_to_max.get((tg_id, thread_id), [])
    # Фолбэк на общий чат
    return tg_to_max.get((tg_id, None), [])

def trim_msgs_map():
    """Удаляет старые записи из маппинга, если превышен лимит."""
    if len(msgs_map) > MAX_MSGS_MAP_ENTRIES:
        excess = len(msgs_map) - MAX_MSGS_MAP_ENTRIES
        for _ in range(excess):
            msgs_map.pop(next(iter(msgs_map)))
        l.info(f"Trimmed msgs_map: removed {excess} old entries, now {len(msgs_map)} entries")


async def download_to_disk(url: str, expected_filename: str = "file") -> str | None:
    """Скачивает файл на диск чанками для экономии ОЗУ и возвращает путь к нему."""
    # Убеждаемся, что папка cache существует
    os.makedirs(os.path.join("data", "cache"), exist_ok=True)
    
    # Генерируем уникальное имя файла, чтобы избежать конфликтов при одновременной загрузке
    temp_filename = f"temp_{uuid.uuid4().hex}_{expected_filename}"
    filepath = os.path.join("data", "cache", temp_filename)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url, timeout=REQUESTS_TIMEOUT) as head_resp:
                head_resp.raise_for_status()
                content_length = head_resp.content_length
                if content_length and content_length > MAX_FILE_SIZE:
                    l.warning(f"File too large: {content_length} bytes (limit {MAX_FILE_SIZE}). Skipping.")
                    return None
            
            async with session.get(url, timeout=REQUESTS_TIMEOUT) as response:
                response.raise_for_status()
                
                cl = response.content_length
                if cl and cl > MAX_FILE_SIZE:
                    l.warning(f"File too large: {cl} bytes (limit {MAX_FILE_SIZE}). Skipping.")
                    return None
                
                # Записываем чанки напрямую в файл
                async with aiofiles.open(filepath, 'wb') as f:
                    downloaded_size = 0
                    exceeded_limit = False
                    
                    async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
                        downloaded_size += len(chunk)
                        if downloaded_size > MAX_FILE_SIZE:
                            exceeded_limit = True
                            break
                        await f.write(chunk)
                
                # Если превысили лимит или файл оказался пустым - удаляем и отменяем отправку
                if exceeded_limit or downloaded_size == 0:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                    if exceeded_limit:
                        l.warning("File exceeded limit during download. Stopping.")
                    return None
                    
                return filepath
    except Exception as e:
        l.error(f"Error downloading file to disk: {e}")
        if os.path.exists(filepath):
            os.remove(filepath)
        return None

async def download_content(url: str, expected_filename: str = "file") -> bytes | None:
    """Download content from URL into memory with size limit check."""
    async with aiohttp.ClientSession() as session:
        async with session.head(url, timeout=REQUESTS_TIMEOUT) as head_resp:
            head_resp.raise_for_status()
            content_length = head_resp.content_length
            if content_length and content_length > MAX_FILE_SIZE:
                l.warning(f"File too large: {content_length} bytes (limit {MAX_FILE_SIZE}). Skipping.")
                return None
        
        async with session.get(url, timeout=REQUESTS_TIMEOUT) as response:
            response.raise_for_status()
            
            # Check Content-Length header if available
            cl = response.content_length
            if cl and cl > MAX_FILE_SIZE:
                l.warning(f"File too large: {cl} bytes (limit {MAX_FILE_SIZE}). Skipping.")
                return None
            
            # Stream in chunks to avoid loading everything at once
            chunks = bytearray()
            async for chunk in response.content.iter_chunked(1024 * 1024):  # 1MB chunks
                chunks.extend(chunk)
                if len(chunks) > MAX_FILE_SIZE:
                    l.warning(f"File exceeded limit during download: {len(chunks)} bytes. Stopping.")
                    return None
            
            if not chunks:
                return None
            
            return bytes(chunks)

async def get_sender_name(user_id: int) -> str:
    """Fetch user name via PyMax."""
    try:
        user = await client.get_user(user_id=user_id)
        if user and user.names:
            return user.names[0].name or ''
    except Exception as e:
        l.error(f"Could not fetch profile for ID {user_id}: {e}")
    return f"User {user_id}"

# --- Logic: Max -> Telegram ---

async def get_smart_sender_info(user_id: int):
    """Fetches name and determines gender-specific verb suffix."""
    try:
        user = await client.get_user(user_id=user_id)
        if user:
            name = f"{user.names[0].name}" if user.names else f"User {user_id}"
            # Sex: 1 is Female, 2 is Male. Default to 'л' (male/neutral)
            suffix = "ла" if user.gender == 1 else "л"
            return name, suffix
    except Exception as e:
        l.error(f"Error fetching user {user_id}: {e}")
    return f"User {user_id}", "л(-а)"

# --- Logic: Max -> Telegram ---

async def process_max_message(message: Message, forwarded: bool = False) -> int | None:
    """
    Handles messages. Returns the Telegram Message ID of the first part sent.
    """
    assert message.sender
    assert message.chat_id

    # 1. Top-level filter — найти TG чат для этого MAX чата
    tg_info = get_tg_chat_for_max(message.chat_id)
    if tg_info is None:
        return None  # Этот MAX чат не в маппинге
        
    tg_chat_id, thread_id = tg_info

    if message.text and message.text.startswith(BOT_MESSAGE_PREFIX):
        return None

    msg_id_str = str(message.id) if message.id else "FWD_PART"
    l.info(f"Processing Max Message ID: {msg_id_str} (Forwarded: {forwarded}, TG: {tg_chat_id}, Topic: {thread_id})")

    first_tg_id = None

    try:
        sender_name, gender_suffix = await get_smart_sender_info(message.sender)

        # 2. Header Logic
        if not forwarded and get_last_sender_id(message.chat_id) != message.sender:
            header_text = f"{BOT_MESSAGE_PREFIX} *{sender_name} написа{gender_suffix}:*"
            sent_header = await bot.send_message(
                tg_chat_id, 
                text=header_text, 
                message_thread_id=thread_id,
                parse_mode="Markdown"
            )
            first_tg_id = sent_header.message_id
            set_last_sender_id(message.chat_id, message.sender)

        # 3. Reply Mapping (Lookup)
        reply_to_tg_id = None
        if message.link and message.link.type == 'REPLY':
            replied_max_id = str(message.link.message.id)
            prefix = f"{message.chat_id}:"
            for mid, tid in msgs_map.items():
                if mid == f"{prefix}{replied_max_id}":
                    reply_to_tg_id = tid
                    break
            if reply_to_tg_id:
                l.info(f"Reply Link: Max[{replied_max_id}] -> TG[{reply_to_tg_id}]")

        # 4. Forward Recursion
        fwds_to_process = []
        if message.link and message.link.type == 'FORWARD':
            fwds_to_process.append(message.link.message)
        if hasattr(message, 'fwd_messages') and message.fwd_messages: 
            fwds_to_process.extend(message.fwd_messages) 

        for fwd_msg in fwds_to_process:
            fwd_tg_id = await process_max_message(fwd_msg, forwarded=True)
            if first_tg_id is None:
                first_tg_id = fwd_tg_id

        # 5. Content Prep
        text_content = message.text or ""
        if forwarded:
            text_content = f"↪ Переслано от {sender_name}:_\n{text_content}"

        # 6. Attachments
        if message.attaches:
            for attach in message.attaches:
                sent = None
                filepath = None
                try:
                    if isinstance(attach, PhotoAttach):
                        filepath = await download_to_disk(attach.base_url, "photo.jpg")
                        if filepath:
                            sent = await bot.send_photo(
                                tg_chat_id,
                                photo=FSInputFile(filepath, filename="photo.jpg"),
                                message_thread_id=thread_id,
                                caption=text_content if text_content else None,
                                reply_to_message_id=reply_to_tg_id,
                                parse_mode="Markdown"
                            )
                    elif isinstance(attach, VideoAttach):
                        vid_info = await client.get_video_by_id(message.chat_id, message.id, attach.video_id)
                        if vid_info and vid_info.url:
                            filepath = await download_to_disk(vid_info.url, "video.mp4")
                            if filepath:
                                sent = await bot.send_video(
                                    tg_chat_id,
                                    video=FSInputFile(filepath, filename="video.mp4"),
                                    message_thread_id=thread_id,
                                    caption=text_content if text_content else None,
                                    reply_to_message_id=reply_to_tg_id,
                                    parse_mode="Markdown"
                                )
                    elif isinstance(attach, FileAttach):
                        file_info = await client.get_file_by_id(message.chat_id, message.id, attach.file_id)
                        if file_info and file_info.url:
                            filename = getattr(file_info, 'name', 'file')
                            filepath = await download_to_disk(file_info.url, filename)
                            if filepath:
                                sent = await bot.send_document(
                                    tg_chat_id,
                                    document=FSInputFile(filepath, filename=filename),
                                    message_thread_id=thread_id,
                                    caption=text_content if text_content else None,
                                    reply_to_message_id=reply_to_tg_id,
                                    parse_mode="Markdown"
                                )

                    if sent:
                        if first_tg_id is None: first_tg_id = sent.message_id
                        text_content = "" 
                        
                except Exception as e:
                    l.error(f"Attachment error: {e}")
                finally:
                    if filepath and os.path.exists(filepath):
                        try:
                            os.remove(filepath)
                        except Exception as e:
                            l.error(f"Failed to delete temp file {filepath}: {e}")

        # 7. Remaining Text
        if text_content.strip():
            sent_msg = await bot.send_message(
                tg_chat_id,
                text=text_content,
                message_thread_id=thread_id,
                reply_to_message_id=reply_to_tg_id,
                parse_mode="Markdown"
            )
            if first_tg_id is None: first_tg_id = sent_msg.message_id

        # 8. Save Mapping
        if first_tg_id and message.id:
            key = f"{message.chat_id}:{message.id}"
            msgs_map[key] = first_tg_id
            
            trim_msgs_map() 
            
            global FLUSH_COUNTER
            FLUSH_COUNTER += 1
            if FLUSH_COUNTER % FLUSH_INTERVAL == 0:
                data_handler.save('msgs', msgs_map)
                l.info(f"Flushed msgs_map to disk ({len(msgs_map)} entries)")

        return first_tg_id

    except Exception as e:
        l.error(f"Error: {e}", exc_info=True)
        return None

# --- Logic: Telegram -> Max ---

@dp.message(Command("send"))
async def send_handler(message: types.Message):
    """Handles /send command."""
    assert message.from_user
    try:
        # Check time
        now = datetime.now().time()
        if ADMIN_USER_ID and message.from_user.id != ADMIN_USER_ID:
            await message.reply('Отправка сообщений доступна только администратору')
            return

        if not (START_TIME <= now <= END_TIME) and CHECK_TIME:
            await message.reply(f"Можно отправлять сообщения только между {START_TIME:%H:%M} и {END_TIME:%H:%M}")
            return

        # Check empty message
        text_to_send = (message.text or '').replace("/send", "", 1).strip()
        if not text_to_send:
            await message.reply("Нельзя отправить пустое сообщение.")
            return

        # Найти MAX чат для этого TG чата и топика
        thread_id = message.message_thread_id
        max_chats = get_max_chats_for_tg(message.chat.id, thread_id)
        
        if not max_chats:
            await message.reply("Этот TG чат (или тема) не привязан к никакому MAX чату.")
            return
        max_chat_id = max_chats[0]

        # Get username
        username = message.from_user.full_name or message.from_user.username

        # Create full text
        full_text = f"{BOT_MESSAGE_PREFIX} *{username} написал(-а):*\n{text_to_send}"
        if BOT_POST_MESSAGE:
            full_text += f"\n{BOT_MESSAGE_PREFIX} {BOT_POST_MESSAGE}"

        # Get id of replied message in MAX
        reply_to_max_id = None
        if message.reply_to_message:
            tg_reply_id = message.reply_to_message.message_id
            prefix = f"{max_chat_id}:"
            for mid, tid in msgs_map.items():
                if mid.startswith(prefix) and tid == tg_reply_id:
                    reply_to_max_id = mid.split(":", 1)[1]
                    break

        # Send message
        sent_msg = await client.send_message(
            chat_id=max_chat_id,
            text=full_text,
            reply_to=reply_to_max_id
        )

        # Map message
        if sent_msg and sent_msg.id:
            key = f"{max_chat_id}:{sent_msg.id}"
            msgs_map[key] = message.message_id
            
            trim_msgs_map()
            
            data_handler.save('msgs', msgs_map)
            await message.reply("Отправлено!")

    except Exception as e:
        l.error(f"Error in send_handler: {e}", exc_info=True)
        await message.reply('Произошла ошибка при отправке.')

# --- Lifecycle ---

async def on_startup():
    l.info("Bot started. Transfer is active.")

    # Send startup message (invite link) logic
    if BOT_START_MESSAGE and not data_handler.load("started"):
        unique_tg_chats = set(t[0] for t in chat_pairs)
        for tg_id in unique_tg_chats:
            try:
                invite = await bot.create_chat_invite_link(tg_id)
                msg = BOT_START_MESSAGE.replace("TG_CHAT_INVITE_LINK", invite.invite_link)
                # Берем все max_id привязанные к этому tg_id
                max_ids = [t[2] for t in chat_pairs if t[0] == tg_id]
                for max_id in max_ids:
                    await client.send_message(msg, max_id)
                data_handler.save("started", True)
                l.info(f"Startup message sent for TG {tg_id}")
            except Exception as e:
                l.error(f"Failed to send startup message for TG {tg_id}: {e}")

async def main():
    global bot
    # Create bot with session (proxy or direct)
    bot = Bot(token=TG_TOKEN, session=proxy_session)
    
    # 1. Setup Signal Handling
    stop_event = Event()
    loop = get_running_loop()
    if os_name != 'nt':
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)

    # --- Trim msgs_map on startup to save memory ---
    trim_msgs_map()

    # 2. Start Telegram Poller FIRST (as a background task)
    l.info("Starting Telegram Polling...")
    # This creates the task but doesn't block execution
    tg_task = create_task(dp.start_polling(bot))

    # Add error logging to tg_task
    def _log_tg_error(t):
        if t.cancelled():
            l.info("tg_task: cancelled")
            return
        exc = t.exception()
        if exc:
            l.error(f"Telegram polling crashed: {exc}", exc_info=exc)
        else:
            l.info("tg_task: completed normally")

    tg_task.add_done_callback(_log_tg_error)

    # 3. Run startup logic (invite links, etc.)
    l.info("Running on_startup()...")
    await on_startup()
    l.info("on_startup() completed")

    # 4. Start Max Client (This blocks and keeps the script alive)
    l.info("Initializing Max Client...")
    max_task = create_task(client.start())

    # Add error logging to max_task
    def _log_max_error(t):
        if t.cancelled():
            l.info("max_task: cancelled")
            return
        exc = t.exception()
        if exc:
            l.error(f"Max client crashed: {exc}", exc_info=exc)
        else:
            l.info("max_task: completed normally")

    max_task.add_done_callback(_log_max_error)

    # Helper to safely start polling
    async def safe_polling():
        while True:
            try:
                l.info("Starting Telegram polling...")
                await dp.start_polling(bot)
                l.info("Telegram polling exited normally")
                return  # polling exited normally
            except asyncio.CancelledError:
                l.info("Telegram polling cancelled")
                raise
            except Exception as e:
                l.error(f"Telegram polling failed: {e}. Retrying in 5s...")
                await asyncio.sleep(5)

    tg_task = create_task(safe_polling())
    tg_task.add_done_callback(_log_tg_error)

    l.info("All tasks created, entering main loop...")

    try:
        # We use a task for Max as well to allow clean shutdowns
        # Wait for either the stop signal or the tasks to fail
        # Only react to stop_task or max_task completion, ignore tg_task crashes
        stop_task = create_task(stop_event.wait())
        loop_count = 0
        while not stop_event.is_set():
            loop_count += 1
            if loop_count % 10 == 0:
                l.info(f"Main loop running... (stop_event={stop_event.is_set()}, tg_task.done()={tg_task.done()}, max_task.done()={max_task.done()}, stop_task.done()={stop_task.done()})")
            done, pending = await wait(
                [tg_task, max_task, stop_task],
                return_when=FIRST_COMPLETED
            )
            
            for task in done:
                if task == stop_task:
                    l.info("Stop signal received")
                    return
                elif task == max_task:
                    if task.cancelled() or task.exception() is not None:
                        l.error("Max client crashed, restarting...")
                        max_task = create_task(client.start())
                        max_task.add_done_callback(_log_max_error)
                    else:
                        l.info("Max client exited normally")
                        return
                elif task == tg_task:
                    if task.cancelled() or task.exception() is not None:
                        exc_msg = task.exception() if task.exception() else 'cancelled'
                        l.error(f"Telegram polling crashed: {exc_msg}")
                        # Restart polling
                        tg_task = create_task(safe_polling())
                        tg_task.add_done_callback(_log_tg_error)
                    else:
                        l.info("Telegram polling exited normally")
                        return

    except Exception as e:
        l.error(f"Critical error in main loop: {e}", exc_info=True)

    finally:
        l.info("Shutting down...")
        data_handler.save('msgs', msgs_map)

        # Clean up tasks
        tg_task.cancel()
        max_task.cancel()

        await client.close()
        if bot:
            await bot.session.close()
        l.info("Shutdown complete.")

if __name__ == '__main__':
    try:
        run(main())
    except (KeyboardInterrupt, SystemExit):
        l.info("Bot stopped.")