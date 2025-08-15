#!/usr/bin/env python3
"""
Simple Interactive Telegram Bot for Poll Creation - Python 3.13 Compatible
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
import os

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Configure logging with file output
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime


def setup_bot_logging():
    """Set up comprehensive logging with file rotation for bot"""
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )

    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # Get root logger
    root_logger = logging.getLogger()

    # Only set up if not already configured
    if not root_logger.handlers:
        root_logger.setLevel(logging.INFO)

        # File handler for bot logs (with rotation)
        bot_logs_file = os.path.join(log_dir, 'bot.log')
        file_handler = RotatingFileHandler(
            bot_logs_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(file_handler)

        # Error file handler (errors only)
        error_logs_file = os.path.join(log_dir, 'bot_errors.log')
        error_handler = RotatingFileHandler(
            error_logs_file,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=3,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(error_handler)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        root_logger.addHandler(console_handler)

        # Log startup message
        root_logger.info("=" * 50)
        root_logger.info(f"Bot logging initialized at {datetime.now()}")
        root_logger.info(f"Log directory: {os.path.abspath(log_dir)}")
        root_logger.info("=" * 50)

    return root_logger


# Initialize logging
setup_bot_logging()
logger = logging.getLogger(__name__)

try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Poll
    from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, \
        PollAnswerHandler

    # Import subscribe handler
    from subscribe_handler import handle_subscribe, handle_unsubscribe, handle_subscribers_count

    # Persistent storage for polls/votes/confirmations
    try:
        from poll_storage import (
            upsert_poll,
            set_poll_closed,
            get_poll,
            upsert_vote,
            get_votes,
        )
    except ImportError:
        upsert_poll = set_poll_closed = get_poll = upsert_vote = get_votes = None
        logger.warning("poll_storage not available; state will not persist across restarts")
    # Placeholders for removed immediate confirmation storage functions
    upsert_immediate_confirmation = None
    get_immediate_confirmation = None
except ImportError:
    print("❌ python-telegram-bot not installed!")
    print("📝 Install it with: py -m pip install python-telegram-bot")
    exit(1)

# Timeout constants (in seconds)
SESSION_TIMEOUT = 86400  # 24 hours
POLL_VOTING_TIMEOUT = 3600  # 1 hour
CLEANUP_INTERVAL = 3600  # 1 hour
FALLBACK_WAIT_TIME = 5  # 5 seconds for past times


class SimplePollBot:
    def __init__(self, token):
        self.token = token
        self.sessions = {}  # Format: {chat_id: {user_id: session_data}}
        self.active_polls = {}  # Track active polls and their voters
        # Removed: confirmation_messages - no reaction tracking needed
        self.pinned_messages = {}  # Track pinned messages for unpinning
        self.scheduled_tasks = {}  # Track scheduled tasks for cancellation
        self.cleanup_task = None  # Track cleanup task
        self.user_vote_states = {}  # Track each user's last known vote state for retraction detection
        self.immediate_confirmation_messages = {}  # Track immediate confirmation messages

        # Session timeout: 24 hours (86400 seconds)
        self.session_timeout = SESSION_TIMEOUT

        # Try to rehydrate active polls from DB
        try:
            from poll_storage import get_open_polls, get_votes
            open_polls = get_open_polls()
            for p in open_polls:
                pid = p['poll_id']
                self.active_polls[pid] = {
                    'chat_id': int(p['chat_id']),
                    'question': p['question'],
                    'vote_count': 0,  # computed below
                    'target_member_count': int(p['target_member_count']) if p.get('target_member_count') is not None else 1,
                    'context': None,  # will be filled when handlers run in this process
                    'creator_id': int(p['creator_id']),
                    'poll_message_id': int(p['poll_message_id']) if p.get('poll_message_id') else None,
                    'options': p.get('options', []),
                    'vote_counts': {}
                }
                # reconstruct vote_counts
                votes = get_votes(pid)
                # votes is {user_id_str: set(option_ids)}; map to option text buckets
                option_texts = self.active_polls[pid]['options']
                vc = {}
                unique_voters = set()
                for uid_str, option_ids in votes.items():
                    uid = int(uid_str)
                    unique_voters.add(uid)
                    for oid in option_ids:
                        if 0 <= oid < len(option_texts):
                            text = option_texts[oid]
                            vc.setdefault(text, set()).add(uid)
                self.active_polls[pid]['vote_counts'] = vc
                self.active_polls[pid]['vote_count'] = len(unique_voters)
            if open_polls:
                logger.info(f"Rehydrated {len(open_polls)} open polls from DB")
        except Exception as e:
            logger.warning(f"Could not rehydrate polls from DB: {e}")

    def start_cleanup_task(self):
        """Start the session cleanup task if not already running"""
        if self.cleanup_task is None or self.cleanup_task.done():
            try:
                self.cleanup_task = asyncio.create_task(self.cleanup_expired_sessions())
                logger.info("✅ Session cleanup task started")
            except RuntimeError as e:
                logger.warning(f"Could not start cleanup task: {e}")

    def get_day_name(self, date):
        """Get Russian day name"""
        days = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
        return days[date.weekday()]
    
    def parse_meeting_time(self, proposed_option: str):
        """Parse meeting time from proposed option string"""
        try:
            from datetime import datetime
            import re
            
            # Try to extract date and time from the proposed option
            # Expected format: "Понедельник, 25.11.2024 в 15:00"
            date_time_pattern = r'(\d{1,2})\.(\d{1,2})\.(\d{4}).*?(\d{1,2}):(\d{2})'
            match = re.search(date_time_pattern, proposed_option)
            
            if match:
                day, month, year, hour, minute = match.groups()
                meeting_datetime = datetime(
                    year=int(year),
                    month=int(month), 
                    day=int(day),
                    hour=int(hour),
                    minute=int(minute)
                )
                
                # Add timezone info (Polish timezone)
                try:
                    from zoneinfo import ZoneInfo
                    polish_tz = ZoneInfo("Europe/Warsaw")
                    meeting_datetime = meeting_datetime.replace(tzinfo=polish_tz)
                except ImportError:
                    try:
                        import pytz
                        polish_tz = pytz.timezone("Europe/Warsaw")
                        meeting_datetime = polish_tz.localize(meeting_datetime)
                    except ImportError:
                        logger.warning("No timezone library available, using naive datetime")
                
                return meeting_datetime
            else:
                logger.warning(f"Could not parse date/time from: {proposed_option}")
                return None
                
        except Exception as e:
            logger.error(f"Error parsing meeting time: {e}")
            return None

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        text = (
            "👋 Привет! Я бот, который помогает планировать встречи через удобные опросы.\n\n"
            "Я быстро собираю варианты даты и времени, выбираю лучший по голосам и отправляю нужные напоминания.\n\n"
            "• Посмотри /help — список доступных команд\n"
            "• Открой /info — подробности о возможностях и сценариях использования\n\n"
            "🚀 Готов? Начинай с /create_poll"
        )
        await update.message.reply_text(text)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command - list all available commands"""
        help_text = (
            "📚 Справка по командам\n\n"
            "🗳️ Доступные команды:\n"
            "• /start — краткое знакомство и ссылки на /help и /info\n"
            "• /help — список команд и краткие пояснения\n"
            "• /info — подробная информация о возможностях\n"
            "• /create_poll — создать новый опрос для планирования встречи\n"
            "• /cancel_bot — отменить все запланированные задачи и открепить сообщения\n"
            "• /subscribe — подписаться на уведомления\n"
            "• /unsubscribe — отписаться от уведомлений\n"
            "• /subscribers — показать количество подписчиков\n"
            "• /die — секретная команда (для развлечения) 💀\n"
        )
        await update.message.reply_text(help_text)

    async def info_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /info command - detailed capabilities and behavior"""
        info_text = (
            "ℹ️ Подробно о возможностях бота\n\n"
            "🎯 Для чего нужен: планирование встреч через опросы с датами и временем.\n\n"
            "📋 Как это работает:\n"
            "1️⃣ /create_poll — выбираешь вопрос (или свой), дни и время\n"
            "2️⃣ Бот создаёт опрос для голосования\n"
            "3️⃣ Когда все проголосуют — бот определяет результат\n"
            "4️⃣ Если выбран вариант встречи — отправляется сообщение подтверждения и закрепляется\n"
            "5️⃣ Бот автоматически планирует напоминания и последующие сообщения\n\n"
            "🤖 Автоматически бот делает:\n"
            "• Напоминание тем, кто не проголосовал (через 1 час)\n"
            "• Подтверждение встречи: за 24ч (или за 4ч, если осталось меньше суток)\n"
            "• Открепление закреплённого сообщения: через 10 часов после встречи\n"
            "• Вопрос о впечатлениях: через 72 часа после встречи\n\n"
            "🧠 Логика выбора:\n"
            "• Если все выбрали один и тот же вариант — он и побеждает\n"
            "• Если у всех несколько одинаковых вариантов — берём самый ранний\n"
            "• Если получилась ничья — бот один раз попросит переголосовать\n"
            "• Если все выбрали ‘Не могу 😔’ — встреча отменяется и бот предложит создать новый опрос\n\n"
            "🌍 Часовой пояс:\n"
            "• Время интерпретируется как Europe/Warsaw и конвертируется в UTC для планирования.\n"
            "• Это помогает корректно учитывать переходы на летнее/зимнее время.\n\n"
            "💡 Советы:\n"
            "• Один активный опрос на чат\n"
            "• Используй /cancel_bot, чтобы отменить все запланированные действия\n"
            "• Команда /help — список команд"
        )
        await update.message.reply_text(info_text)

    async def die_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /die command — now with fun fantasy responses only"""
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
            chat_type = update.effective_chat.type
            user_mention = f"@{update.effective_user.username}" if update.effective_user.username else f"[{update.effective_user.first_name}](tg://user?id={user_id})"

            logger.info(f"🎯 Die command triggered by user {user_id} in chat {chat_id}")

            if chat_type == 'private':
                await update.message.reply_text(
                    "💀 В личных сообщениях нельзя умереть! Попробуйте в группе! 😄"
                )
                return

            fantasy_messages = [
                "🔥 {user} получил 12 урона от огненного шара!",
                "💀 {user} провалил спасбросок и упал в яму.",
                "🧊 {user} заморожен магией льда на 10 секунд.",
                "⚡ {user} поражён молнией из ниоткуда!",
                "🕳 {user} провалился сквозь портал в другое измерение.",
                "👻 Духи отвергли {user} — перерождение отклонено.",
                "🩸 {user} случайно укололся ядовитой иглой.",
                "🐉 Дракон пролетел мимо и испепелил {user}.",
                "🔮 {user} оказался в ловушке иллюзии и сошёл с ума.",
                "🪓 {user} атакован топором берсерка!",
                "📜 {user} прочитал запрещённое заклинание и исчез.",
                "🧠 Мозг {user} перегрелся от чёрной магии.",
                "🪦 {user} попытался умереть, но смерть в отпуске.",
                "🥀 {user} увял от грусти и одиночества.",
                "💫 {user} был унесён космическими силами во Вселенную мемов.",
            ]

            import random
            message = random.choice(fantasy_messages).format(user=user_mention)

            if user_mention.startswith('@'):
                await update.message.reply_text(message)
            else:
                await update.message.reply_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"💥 Critical error in die_command: {e}")
            await update.message.reply_text("💀 Произошла фатальная ошибка при попытке умереть! 🤖")

    async def create_poll(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start poll creation"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        # Ensure bot has admin rights in group/supergroup to be able to pin/unpin messages
        try:
            chat_type = update.effective_chat.type
            if chat_type in ('group', 'supergroup'):
                try:
                    me = await context.bot.get_me()
                    member = await context.bot.get_chat_member(chat_id, me.id)
                    # Determine admin status (works across PTB versions)
                    status = getattr(member, 'status', None)
                    is_admin = False
                    if status in ('administrator', 'creator'):
                        is_admin = True
                    else:
                        # Fallback by class name
                        cls_name = type(member).__name__
                        if cls_name in ('ChatMemberAdministrator', 'ChatMemberOwner', 'ChatMemberCreator'):
                            is_admin = True
                    if not is_admin:
                        await update.message.reply_text(
                            "Мне нужны права администратора в этом чате, чтобы создавать опросы и закреплять сообщения.\n"
                            "Пожалуйста, добавьте бота в администраторы с правом: ‘Закреплять сообщения’."
                        )
                        return
                    # Optional: check pin permission if present
                    can_pin = getattr(member, 'can_pin_messages', True)
                    if not can_pin:
                        await update.message.reply_text(
                            "У меня нет права ‘Закреплять сообщения’.\n"
                            "Пожалуйста, дайте боту право закреплять сообщения или сделайте его администратором с этим правом."
                        )
                        return
                except Exception as e:
                    # If we cannot verify (e.g., limited API in tests), proceed but log
                    logger.warning(f"Could not verify admin rights: {e}")
        except Exception:
            pass

        # Check if someone else is already creating a poll in this chat
        if chat_id in self.sessions:
            creator_id = next(iter(self.sessions[chat_id].keys()))
            if user_id != creator_id:
                try:
                    # Get the creator's info
                    creator_info = await context.bot.get_chat_member(chat_id, creator_id)
                    creator_user = creator_info.user
                    if creator_user.username:
                        creator_mention = f"@{creator_user.username}"
                    else:
                        creator_mention = creator_user.first_name
                except:
                    creator_mention = f"пользователь {creator_id}"

                await update.message.reply_text(
                    f"В этом чате уже создается опрос пользователем {creator_mention}. Подождите, пока он закончит.")
                return

        current_time = datetime.now()

        # Initialize chat sessions if not exists
        if chat_id not in self.sessions:
            self.sessions[chat_id] = {}

        self.sessions[chat_id][user_id] = {
            'step': 'question',
            'question': None,
            'days': [],
            'times': [],
            'chat_id': chat_id,
            'created_at': current_time,
            'last_activity': current_time
        }

        keyboard = [
            [InlineKeyboardButton("Использовать 'Собираемся?' 🎯", callback_data="default_q")],
            [InlineKeyboardButton("Ввести свой вопрос ✏️", callback_data="custom_q")]
        ]
        await update.message.reply_text(
            "1️⃣ Выбери вопрос для опроса:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button presses"""
        query = update.callback_query
        await query.answer()

        user_id = update.effective_user.id
        chat_id = query.message.chat_id
        data = query.data

        # Handle proceed confirmation buttons (these don't need session check)
        if data.startswith('proceed_'):
            await self.handle_proceed_button(update, context, data)
            return

        # Only check sessions for poll creation buttons
        if chat_id not in self.sessions or user_id not in self.sessions[chat_id]:
            # Check if someone else is creating a poll in this chat
            if chat_id in self.sessions and self.sessions[chat_id]:
                creator_id = next(iter(self.sessions[chat_id].keys()))
                try:
                    # Get the creator's info
                    creator_info = await context.bot.get_chat_member(chat_id, creator_id)
                    creator_user = creator_info.user
                    if creator_user.username:
                        creator_mention = f"@{creator_user.username}"
                    else:
                        creator_mention = creator_user.first_name
                except:
                    creator_mention = f"пользователь {creator_id}"

                await query.answer(f"Только {creator_mention} может выбирать опции", show_alert=True)
            else:
                await query.edit_message_text("❌ Сессия истекла. Используй /create_poll для создания нового опроса.")
            return

        # Update last activity timestamp
        self.sessions[chat_id][user_id]['last_activity'] = datetime.now()

        session = self.sessions[chat_id][user_id]

        if data == "default_q":
            session['question'] = "Собираемся?"
            await self.show_days(query, user_id, chat_id)
        elif data == "custom_q":
            session['step'] = 'waiting_question'
            await query.edit_message_text("✏️ Введи свой вопрос:")
        elif data.startswith("day_"):
            day_idx = int(data.split("_")[1])
            if day_idx in session['days']:
                session['days'].remove(day_idx)
            else:
                session['days'].append(day_idx)
            await self.show_days(query, user_id, chat_id)
        elif data == "days_done":
            if not session['days']:
                await query.answer("❌ Выбери хотя бы один день!", show_alert=True)
                return
            await self.show_times(query, user_id, chat_id)
        elif data.startswith("time_"):
            time = data.split("_", 1)[1]
            if time in session['times']:
                session['times'].remove(time)
            else:
                session['times'].append(time)
            await self.show_times(query, user_id, chat_id)
        elif data == "times_done":
            if not session['times']:
                await query.answer("❌ Выбери хотя бы одно время!", show_alert=True)
                return
            await self.create_final_poll(query, user_id, chat_id, context)
        elif data.startswith("pin_yes_"):
            poll_id = data.split("pin_yes_")[1]
            await self.handle_pin_proposal(query, poll_id, True, context)
        elif data.startswith("pin_no_"):
            poll_id = data.split("pin_no_")[1]
            await self.handle_pin_proposal(query, poll_id, False, context)
        elif data.startswith("ignore_yes_"):
            parts = data.split("ignore_yes_")[1].split("_", 1)
            poll_id = parts[0]
            cant_make_it_user_id = int(parts[1])
            await self.handle_ignore_confirmation(query, poll_id, cant_make_it_user_id, True, context)
        elif data.startswith("ignore_no_"):
            poll_id = data.split("ignore_no_")[1]
            await self.handle_ignore_confirmation(query, poll_id, None, False, context)
        elif data.startswith("ignore_multiple_yes_"):
            poll_id = data.split("ignore_multiple_yes_")[1]
            await self.handle_multiple_ignore_confirmation(query, poll_id, True, context)
        elif data.startswith("ignore_multiple_no_"):
            poll_id = data.split("ignore_multiple_no_")[1]
            await self.handle_multiple_ignore_confirmation(query, poll_id, False, context)

    async def text_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text input"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        if (chat_id in self.sessions and user_id in self.sessions[chat_id] and
                self.sessions[chat_id][user_id]['step'] == 'waiting_question'):
            # Update last activity timestamp
            self.sessions[chat_id][user_id]['last_activity'] = datetime.now()
            self.sessions[chat_id][user_id]['question'] = update.message.text
            await self.show_days_new_message(update, user_id, chat_id)

    async def show_days(self, query, user_id, chat_id):
        """Show day selection"""
        session = self.sessions[chat_id][user_id]
        today = datetime.now()

        keyboard = []
        for i in range(7):
            day = today + timedelta(days=i)
            day_name = self.get_day_name(day)
            date_str = day.strftime("%d.%m")
            selected = "✅" if i in session['days'] else "📅"
            keyboard.append([InlineKeyboardButton(
                f"{selected} {day_name} ({date_str})",
                callback_data=f"day_{i}"
            )])

        keyboard.append([InlineKeyboardButton("Готово ➡️", callback_data="days_done")])

        text = f"2️⃣ Выбери дни для '{session['question']}':"
        if session['days']:
            text += f"\n\nВыбрано дней: {len(session['days'])}"

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def show_days_new_message(self, update, user_id, chat_id):
        """Show days as new message"""
        session = self.sessions[chat_id][user_id]
        today = datetime.now()

        keyboard = []
        for i in range(7):
            day = today + timedelta(days=i)
            day_name = self.get_day_name(day)
            date_str = day.strftime("%d.%m")
            keyboard.append([InlineKeyboardButton(
                f"📅 {day_name} ({date_str})",
                callback_data=f"day_{i}"
            )])

        keyboard.append([InlineKeyboardButton("Готово ➡️", callback_data="days_done")])

        await update.message.reply_text(
            f"2️⃣ Выбери дни для '{session['question']}':",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def show_times(self, query, user_id, chat_id):
        """Show time selection"""
        session = self.sessions[chat_id][user_id]
        times = ["09:00", "10:00", "11:00", "12:00", "13:00", "14:00",
                 "15:00", "16:00", "17:00", "18:00", "19:00", "20:00", "21:00"]

        keyboard = []
        row = []
        for time in times:
            selected = "✅" if time in session['times'] else "🕐"
            row.append(InlineKeyboardButton(f"{selected} {time}", callback_data=f"time_{time}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)

        keyboard.append([InlineKeyboardButton("Готово ➡️", callback_data="times_done")])

        text = f"3️⃣ Выбери время для '{session['question']}':"
        if session['times']:
            text += f"\n\nВыбрано времен: {len(session['times'])}"

        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def create_final_poll(self, query, user_id, chat_id, context):
        """Create the final poll"""
        session = self.sessions[chat_id][user_id]
        today = datetime.now()

        options = []
        for day_idx in sorted(session['days']):
            day = today + timedelta(days=day_idx)
            day_name = self.get_day_name(day)
            date_str = day.strftime("%d.%m")
            for time in sorted(session['times']):
                options.append(f"{day_name} ({date_str}) в {time}")

        options.append("Не могу 😔")

        try:
            poll_message = await context.bot.send_poll(
                chat_id=session['chat_id'],
                question=session['question'],
                options=options,
                is_anonymous=False,
                allows_multiple_answers=True
            )

            # Track this poll for voting monitoring
            poll_id = poll_message.poll.id
            self.active_polls[poll_id] = {
                'chat_id': session['chat_id'],
                'question': session['question'],
                'vote_count': 0,  # unique voters count
                'target_member_count': 1,  # updated below
                'context': context,
                'creator_id': user_id,
                'poll_message_id': poll_message.message_id,
                'options': options,
                'vote_counts': {}
            }

            # Persist poll
            try:
                if upsert_poll:
                    upsert_poll(
                        poll_id=poll_id,
                        chat_id=session['chat_id'],
                        question=session['question'],
                        options=options,
                        creator_id=user_id,
                        poll_message_id=poll_message.message_id,
                        target_member_count=1,
                        pinned_message_id=None,
                        is_closed=False,
                    )
            except Exception as e:
                logger.warning(f"Could not persist poll {poll_id}: {e}")

            # Get chat members and start monitoring
            await self.get_chat_members_and_monitor(poll_id, session['chat_id'], context)

            await query.edit_message_text(
                f"✅ Опрос '{session['question']}' создан!\n"
                f"📊 Вариантов: {len(options)}\n"
                f"💪 Отправлю подтверждение участия перед встречей\n"
                f"🎉 Уведомлю когда все будут готовы!"
            )
        except Exception as e:
            await query.edit_message_text(f"❌ Ошибка: {e}")

        # Clean up session
        if chat_id in self.sessions and user_id in self.sessions[chat_id]:
            del self.sessions[chat_id][user_id]
            if not self.sessions[chat_id]:  # Remove empty chat session
                del self.sessions[chat_id]

    #     Get chat gets only admins
    async def get_chat_members_and_monitor(self, poll_id, chat_id, context):
        """Get chat member count and start monitoring poll votes"""
        try:
            # Get chat info
            chat = await context.bot.get_chat(chat_id)

            if chat.type == 'private':
                # In private chat, target is 1 member
                logger.info(f"Private chat detected for poll {poll_id}")
                self.active_polls[poll_id]['target_member_count'] = 1
                logger.info(f"Private chat target: 1 member")
            else:
                # Group chat - use getChatMemberCount API
                try:
                    total_members = await context.bot.get_chat_member_count(chat_id)
                    # Exclude bots from the count (at least this bot)
                    human_members = max(1, total_members - 1)
                    self.active_polls[poll_id]['target_member_count'] = human_members
                    # persist update
                    try:
                        if upsert_poll:
                            upsert_poll(poll_id, chat_id, self.active_polls[poll_id]['question'], self.active_polls[poll_id]['options'], self.active_polls[poll_id]['creator_id'], self.active_polls[poll_id]['poll_message_id'], human_members, False)
                    except Exception as e:
                        logger.warning(f"Persist target_member_count failed for {poll_id}: {e}")
                    logger.info(
                        f"Chat {chat_id} has {total_members} total members, {human_members} human members (via getChatMemberCount)")
                except Exception as e:
                    logger.warning(f"getChatMemberCount failed: {e}, trying fallback")
                    # Fallback to chat.member_count if available
                    total_members = getattr(chat, 'member_count', 1)
                    # Exclude bots from the count (at least this bot)
                    human_members = max(1, total_members - 1)
                    self.active_polls[poll_id]['target_member_count'] = human_members
                    try:
                        if upsert_poll:
                            upsert_poll(poll_id, chat_id, self.active_polls[poll_id]['question'], self.active_polls[poll_id]['options'], self.active_polls[poll_id]['creator_id'], self.active_polls[poll_id]['poll_message_id'], human_members, False)
                    except Exception as e:
                        logger.warning(f"Persist target_member_count failed for {poll_id} (fallback): {e}")
                    logger.info(
                        f"Fallback: Chat {chat_id} has {total_members} total members, {human_members} human members (via chat.member_count)")

            logger.info(
                f"Starting 1-hour timer for poll {poll_id} with target: {self.active_polls[poll_id]['target_member_count']} members")
            # Start the 1-hour timer
            asyncio.create_task(self.monitor_poll_voting(poll_id))

        except Exception as e:
            logger.error(f"Error getting chat member count: {e}")
            # Fallback: set minimum target
            if 'target_member_count' not in self.active_polls[poll_id]:
                self.active_polls[poll_id]['target_member_count'] = 1

    async def poll_answer_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle poll answers and vote retractions"""
        poll_answer = update.poll_answer
        poll_id = poll_answer.poll_id
        user_id = poll_answer.user.id
        current_option_ids = list(poll_answer.option_ids)

        logger.info(f"Poll answer received: poll_id={poll_id}, user_id={user_id}, options={current_option_ids}")

        if poll_id in self.active_polls:
            # Initialize user vote state tracking for this poll if needed
            poll_user_key = f"{poll_id}_{user_id}"
            previous_option_ids = self.user_vote_states.get(poll_user_key, [])

            # Track vote counts manually
            vote_counts = self.active_polls[poll_id]['vote_counts']
            options = self.active_polls[poll_id]['options']

            # Handle vote retraction: remove user from previously voted options
            if previous_option_ids:
                logger.info(f"User {user_id} had previous votes: {previous_option_ids}")
                for prev_option_id in previous_option_ids:
                    if prev_option_id < len(options):
                        prev_option_text = options[prev_option_id]
                        if prev_option_text in vote_counts and user_id in vote_counts[prev_option_text]:
                            vote_counts[prev_option_text].remove(user_id)
                            logger.info(f"Retracted vote from option {prev_option_id}: '{prev_option_text}'")

                            # Clean up empty vote sets
                            if not vote_counts[prev_option_text]:
                                vote_counts[prev_option_text] = set()

            # Handle new votes: add user to newly selected options
            if current_option_ids:
                logger.info(f"User {user_id} voting for new options: {current_option_ids}")
                for option_id in current_option_ids:
                    if option_id < len(options):
                        option_text = options[option_id]
                        if option_text not in vote_counts:
                            vote_counts[option_text] = set()
                        vote_counts[option_text].add(user_id)
                        logger.info(f"Vote added for option {option_id}: '{option_text}'")
            else:
                # Complete retraction - user deselected all options
                if previous_option_ids:
                    logger.info(f"User {user_id} retracted all votes (was: {previous_option_ids})")
                else:
                    logger.info(f"User {user_id} sent empty vote (no previous votes)")

            # Update user's vote state
            self.user_vote_states[poll_user_key] = current_option_ids

            # Persist vote
            try:
                if upsert_vote:
                    upsert_vote(poll_id, user_id, current_option_ids)
            except Exception as e:
                logger.warning(f"Could not persist vote for poll {poll_id}, user {user_id}: {e}")

            # Check if user voted only for "Не могу 😔"
            if len(current_option_ids) == 1:
                cant_make_it_option_id = None
                for i, option in enumerate(options):
                    if option == "Не могу 😔":
                        cant_make_it_option_id = i
                        break

                if cant_make_it_option_id is not None and current_option_ids[0] == cant_make_it_option_id:
                    logger.info(f"User {user_id} voted only for 'Не могу 😔'")
                    # Store that this user can't make it, but don't process immediately
                    if 'cant_make_it_users' not in self.active_polls[poll_id]:
                        self.active_polls[poll_id]['cant_make_it_users'] = set()
                    self.active_polls[poll_id]['cant_make_it_users'].add(user_id)

            # Update vote count based on unique voters across all options (if poll still exists)
            if poll_id in self.active_polls:
                all_voters = set()
                for voters in vote_counts.values():
                    all_voters.update(voters)
                self.active_polls[poll_id]['vote_count'] = len(all_voters)

                logger.info(
                    f"Poll {poll_id} vote count: {self.active_polls[poll_id]['vote_count']}/{self.active_polls[poll_id].get('target_member_count', 1)}")
                logger.info(f"Current vote distribution: {vote_counts}")

                # Check if everyone has voted (this will re-evaluate resolution logic)
                poll_completed = await self.check_if_everyone_voted(poll_id, context)

                # No immediate reminders; only 1-hour timeout scheduling remains
                if not poll_completed:
                    # Check if this was a new vote (user added options) vs retraction (user removed options)
                    is_new_vote = len(current_option_ids) > len(previous_option_ids)
                    is_first_vote = len(previous_option_ids) == 0 and len(current_option_ids) > 0
                    
                    # Only the 1-hour timeout reminder exists; nothing to send here
            else:
                logger.info(f"Poll {poll_id} was already resolved and cleaned up")
        else:
            logger.warning(f"Received vote for unknown poll {poll_id}")

    async def check_if_everyone_voted(self, poll_id, context):
        """Check if everyone has voted based on getChatMembersCount"""
        if poll_id not in self.active_polls:
            return True  # Poll doesn't exist, consider it completed

        poll_data = self.active_polls[poll_id]
        vote_count = poll_data['vote_count']
        target_member_count = poll_data.get('target_member_count', 1)
        chat_id = poll_data['chat_id']

        # Check if vote count matches target member count
        if vote_count >= target_member_count and target_member_count > 0:
            logger.info(f"Everyone voted for poll {poll_id}! Votes: {vote_count}/{target_member_count}")

            # First check if everyone voted only "Не могу"
            cant_make_it_users = poll_data.get('cant_make_it_users', set())

            # Calculate effective member count (excluding users who can't make it)
            effective_member_count = target_member_count - len(cant_make_it_users)

            if effective_member_count <= 0:
                # Everyone voted "Не могу" - send playful message
                logger.info(f"Everyone voted 'Не могу' for poll {poll_id}")
                playful_message = "Никто не может прийти на встречу! 😅 Попробуйте создать новый опрос с другими вариантами времени.\n\nИспользуйте /create_poll"
                await context.bot.send_message(chat_id=chat_id, text=playful_message)
                # Everyone voted 'Не могу' → close poll and unschedule voting timeout
                await self.close_poll_and_clean_up(poll_id, context, cancel_voting_timeout=True)
                return True  # Poll completed

            # Check if some users voted only "Не могу" and handle them
            if cant_make_it_users and effective_member_count > 0:
                logger.info(f"Processing users who can't make it: {cant_make_it_users}")
                await self.handle_cant_make_it_users(poll_id, cant_make_it_users, context)
                # Proceed to resolve the poll excluding 'Не могу' voters
                await self.resolve_poll_excluding_cant_make_it(poll_id, context)
                return True  # Poll completed by resolution

            try:
                # Get current time and date
                now = datetime.now()
                time_str = now.strftime("%H:%M")
                date_str = now.strftime("%d.%m.%Y")

                # Get poll results to find the most voted option
                most_voted_result = await self.get_most_voted_option_fallback_with_new_logic(poll_id, context)

                # Early guard: if the chosen meeting time is already in the past, cancel and inform
                try:
                    parsed_result = most_voted_result if isinstance(most_voted_result, str) else None
                    if parsed_result:
                        handled = await self.meeting_in_past_guard(poll_id, chat_id, context, parsed_result)
                        if handled:
                            return True
                except Exception as e:
                    logger.warning(f"Past-meeting guard error during resolution for poll {poll_id}: {e}")

                # Send confirmation message with the most voted result
                if most_voted_result:
                    # Check if everyone can't make it
                    if most_voted_result == "EVERYONE_CANT_MAKE_IT":
                        # Send playful message and close poll
                        playful_message = "Никто не может прийти на встречу! 😅 Попробуйте создать новый опрос с другими вариантами времени.\n\nИспользуйте /create_poll"
                        await context.bot.send_message(chat_id=chat_id, text=playful_message)
                        # Everyone voted 'Не могу' → close poll and unschedule voting timeout
                        await self.close_poll_and_clean_up(poll_id, context, cancel_voting_timeout=True)
                        return
                    # Check if revote was prompted
                    elif most_voted_result == "REVOTE_PROMPTED":
                        # Don't send confirmation message, users prompted to change votes
                        logger.info(f"Revote prompted for poll {poll_id}, waiting for vote changes")
                        return
                    # Check if "Не могу 😔" won
                    elif most_voted_result == "Не могу 😔":
                        confirmation_message = "😔 Большинство не может собраться. Попробуйте создать новый опрос с другими вариантами."
                        # Don't schedule any follow-up tasks for "Не могу" result
                        send_only_message = True
                    else:
                        confirmation_message = f"Собираемся в {most_voted_result}"
                        send_only_message = False
                else:
                    confirmation_message = f"✅ Все проголосовали! (Не удалось получить результаты)"
                    send_only_message = True

                sent_message = await context.bot.send_message(
                    chat_id=chat_id,
                    text=confirmation_message
                )

                # Only pin and schedule tasks if it's not "Не могу" result
                if not send_only_message:
                    # Close the poll to prevent further voting
                    try:
                        poll_message_id = poll_data['poll_message_id']
                        await context.bot.stop_poll(chat_id=chat_id, message_id=poll_message_id)
                        try:
                            if set_poll_closed:
                                set_poll_closed(poll_id, True)
                        except Exception as e:
                            logger.warning(f"DB set_poll_closed failed for {poll_id}: {e}")
                        logger.info(f"Closed poll {poll_id} in chat {chat_id}")
                    except Exception as e:
                        logger.warning(f"Could not close poll {poll_id}: {e}")
                    # Cancel any pending voting-timeout reminders in DB for this chat
                    try:
                        from task_storage import cancel_chat_tasks
                        cancelled = cancel_chat_tasks(chat_id, task_type="poll_voting_timeout")
                        logger.info(f"Cancelled {cancelled} 'poll_voting_timeout' tasks for chat {chat_id}")
                    except Exception as e:
                        logger.warning(f"Could not cancel voting timeout tasks for chat {chat_id}: {e}")

                    # Pin the confirmation message
                    try:
                        await context.bot.pin_chat_message(
                            chat_id=chat_id,
                            message_id=sent_message.message_id,
                            disable_notification=True
                        )
                        try:
                            if upsert_poll:
                                upsert_poll(poll_id, chat_id, poll_data['question'], poll_data['options'], poll_data['creator_id'], poll_data['poll_message_id'], poll_data.get('target_member_count', 1), sent_message.message_id, False)
                        except Exception as e:
                            logger.warning(f"Could not persist pinned message id: {e}")
                        try:
                            if upsert_poll:
                                upsert_poll(poll_id, chat_id, poll_data['question'], poll_data['options'], poll_data['creator_id'], poll_data['poll_message_id'], poll_data.get('target_member_count', 1), sent_message.message_id, False)
                        except Exception as e:
                            logger.warning(f"Could not persist pinned message id: {e}")
                        logger.info(f"Pinned confirmation message in chat {chat_id}")

                        # Store pinned message info for later unpinning
                        self.pinned_messages[f"{chat_id}_{poll_id}"] = {
                            'chat_id': chat_id,
                            'message_id': sent_message.message_id
                        }
                    except Exception as e:
                        logger.warning(f"Could not pin message in chat {chat_id}: {e}")

                    # Get all voters from the poll
                    poll_voters = set()
                    if poll_id in self.active_polls:
                        poll_data = self.active_polls[poll_id]
                        if 'vote_counts' in poll_data:
                            # Extract all voters from vote_counts (which contains voters by option)
                            for option_text, voters in poll_data['vote_counts'].items():
                                poll_voters.update(voters)
                            # Exclude bots from voters
                            try:
                                bot_info = await context.bot.get_me()
                                bot_user_id = bot_info.id
                                poll_voters.discard(bot_user_id)
                            except Exception as e:
                                logger.warning(f"Could not get bot info to exclude from voters: {e}")

                    # Schedule "План в силе?" message according to timing logic (24h/4h before meeting)
                    confirmation_task = asyncio.create_task(
                        self.schedule_confirmation_message(poll_id, chat_id, context, most_voted_result, poll_voters))

                    # Schedule unpinning at the event time
                    unpin_task = asyncio.create_task(
                        self.schedule_unpin_message(poll_id, chat_id, context, most_voted_result, sent_message.message_id))

                    # Schedule follow-up message for the day after the meeting
                    followup_task = asyncio.create_task(
                        self.schedule_followup_message(chat_id, context, most_voted_result))

                    # Track scheduled tasks for this chat
                    if chat_id not in self.scheduled_tasks:
                        self.scheduled_tasks[chat_id] = []
                    self.scheduled_tasks[chat_id].extend([
                        {'task': confirmation_task, 'type': 'confirmation', 'poll_id': poll_id},
                        {'task': unpin_task, 'type': 'unpin', 'poll_id': poll_id},
                        {'task': followup_task, 'type': 'followup', 'poll_id': poll_id}
                    ])
                else:
                    logger.info(f"Poll {poll_id} result was 'Не могу' or error - no scheduling or pinning")
                    # Close the poll and mark as closed in DB, then clean up
                    try:
                        poll_message_id = poll_data['poll_message_id']
                        await context.bot.stop_poll(chat_id=chat_id, message_id=poll_message_id)
                        try:
                            if set_poll_closed:
                                set_poll_closed(poll_id, True)
                        except Exception as e:
                            logger.warning(f"DB set_poll_closed failed for {poll_id}: {e}")
                        logger.info(f"Closed poll {poll_id} after 'Не могу' result")
                    except Exception as e:
                        logger.warning(f"Could not close poll {poll_id} after 'Не могу' result: {e}")

                    # Clean up poll data completely (no further scheduling for 'Не могу')
                    self.cleanup_poll_data(poll_id)
                    if poll_id in self.active_polls:
                        del self.active_polls[poll_id]

                # Finalization log
                logger.info(f"Poll {poll_id} completed - finalization done")

            except Exception as e:
                logger.error(f"Error sending confirmation message: {e}")
        else:
            logger.info(f"Not everyone voted yet for poll {poll_id}. Votes: {vote_count}/{target_member_count}")


    async def analyze_poll_results(self, poll, poll_id):
        """Analyze poll results with new resolution logic"""
        if poll_id not in self.active_polls:
            logger.error(f"Poll {poll_id} not found for analysis")
            return {'has_tie': False, 'winner': None, 'tied_options': [], 'vote_counts': {}, 'max_votes': 0}

        vote_counts_by_users = self.active_polls[poll_id]['vote_counts']
        target_member_count = self.active_polls[poll_id].get('target_member_count', 1)

        logger.info(f"Analyzing poll {poll_id} with {target_member_count} target members")
        logger.info(f"Vote data: {vote_counts_by_users}")

        # Get all voters (union of all vote sets)
        all_voters = set()
        for voters in vote_counts_by_users.values():
            all_voters.update(voters)

        # Count votes for each option
        vote_counts = {}
        for option_text, voters in vote_counts_by_users.items():
            vote_counts[option_text] = len(voters)
            logger.info(f"Option '{option_text}': {len(voters)} votes")

        # NEW RESOLUTION LOGIC:

        # First, check if everyone has actually voted (total unique voters equals target)
        all_voters = set()
        for voters in vote_counts_by_users.values():
            all_voters.update(voters)

        if len(all_voters) < target_member_count:
            # Not everyone has voted yet, return no winner
            logger.info(f"Not everyone voted yet: {len(all_voters)}/{target_member_count} voters")
            return {
                'vote_counts': vote_counts,
                'max_votes': max(vote_counts.values()) if vote_counts else 0,
                'winner': None,
                'tied_options': [],
                'has_tie': False,
                'voter_data': vote_counts_by_users
            }

        # Everyone has voted, now apply resolution logic
        logger.info(f"Everyone has voted ({len(all_voters)}/{target_member_count}), applying resolution logic")

        # Case 1: ✅ Everyone voted one identical option → selected
        # Find options where everyone voted for ONLY that option (no other options)
        single_option_everyone = None
        for option_text, voters in vote_counts_by_users.items():
            if option_text != "Не могу 😔" and len(voters) == target_member_count:
                # Check if everyone who voted for this option voted ONLY for this option
                # by checking if any voter voted for other options too
                voters_only_this_option = True
                for voter in voters:
                    # Check if this voter voted for any other option
                    for other_option, other_voters in vote_counts_by_users.items():
                        if other_option != option_text and voter in other_voters:
                            voters_only_this_option = False
                            break
                    if not voters_only_this_option:
                        break

                if voters_only_this_option:
                    single_option_everyone = option_text
                    logger.info(f"Case 1: Everyone voted for single identical option '{option_text}'")
                    break

        if single_option_everyone:
            return {
                'vote_counts': vote_counts,
                'max_votes': target_member_count,
                'winner': single_option_everyone,
                'tied_options': [single_option_everyone],
                'has_tie': False,
                'voter_data': vote_counts_by_users
            }

        # Case 2: ✅ One option is voted by everyone → selected
        option_voted_by_everyone = None
        for option_text, voters in vote_counts_by_users.items():
            if option_text != "Не могу 😔" and len(voters) == target_member_count:
                option_voted_by_everyone = option_text
                logger.info(f"Case 2: Option '{option_text}' voted by everyone")
                break

        if option_voted_by_everyone:
            return {
                'vote_counts': vote_counts,
                'max_votes': target_member_count,
                'winner': option_voted_by_everyone,
                'tied_options': [option_voted_by_everyone],
                'has_tie': False,
                'voter_data': vote_counts_by_users
            }

        # Case 4: 🕒 If everyone voted for same multiple options → select earliest date/time
        # Find options that everyone voted for
        options_everyone_voted = []
        for option_text, voters in vote_counts_by_users.items():
            if option_text != "Не могу 😔" and len(voters) == target_member_count:
                options_everyone_voted.append(option_text)

        if len(options_everyone_voted) > 1:
            # Everyone voted for multiple options - select earliest date/time
            logger.info(f"Case 4: Everyone voted for same multiple options: {options_everyone_voted}")
            # Sort by the option text to get earliest date/time (assuming format includes date/time)
            earliest_option = min(options_everyone_voted)
            logger.info(f"Selected earliest option: '{earliest_option}'")
            return {
                'vote_counts': vote_counts,
                'max_votes': target_member_count,
                'winner': earliest_option,
                'tied_options': [earliest_option],
                'has_tie': False,
                'voter_data': vote_counts_by_users
            }

        # Check if everyone voted only "Не могу 😔"
        cant_make_it_voters = vote_counts_by_users.get("Не могу 😔", set())
        if len(cant_make_it_voters) == target_member_count:
            logger.info("Everyone voted only 'Не могу 😔'")
            return {
                'vote_counts': vote_counts,
                'max_votes': target_member_count,
                'winner': "EVERYONE_CANT_MAKE_IT",
                'tied_options': ["Не могу 😔"],
                'has_tie': False,
                'voter_data': vote_counts_by_users
            }

        # Case 3: ⚠️ Everyone voted, but no single common option → trigger revote
        logger.info("Case 3: Everyone voted but no single common option - need revote")
        return {
            'vote_counts': vote_counts,
            'max_votes': max(vote_counts.values()) if vote_counts else 0,
            'winner': "REVOTE_NEEDED",
            'tied_options': list(vote_counts.keys()),
            'has_tie': True,
            'voter_data': vote_counts_by_users
        }

    async def handle_tie_situation(self, poll_id, context, vote_analysis):
        """Handle tie situation by prompting users to change votes on existing poll"""
        try:
            if poll_id not in self.active_polls:
                logger.error(f"Poll {poll_id} not found for tie handling")
                return None

            poll_data = self.active_polls[poll_id]
            chat_id = poll_data['chat_id']

            # Send revote notification with a fun, engaging message
            import random
            # Build a normalized tie signature to persist across restarts
            tie_signature = None
            try:
                tied = [opt for opt in (vote_analysis.get('tied_options') or []) if opt != "Не могу 😔"]
                tie_signature = ",".join(sorted(tied)) if tied else ""
            except Exception:
                tie_signature = ""

            # Load persisted tie state if available
            try:
                if get_poll:
                    db_poll = get_poll(poll_id)
                    if db_poll:
                        poll_data['revote_notified'] = db_poll.get('revote_notified', poll_data.get('revote_notified', False))
                        poll_data['in_revote'] = db_poll.get('in_revote', poll_data.get('in_revote', False))
                        poll_data['last_tie_signature'] = db_poll.get('last_tie_signature', poll_data.get('last_tie_signature'))
                        poll_data['last_tie_message_at'] = db_poll.get('last_tie_message_at', poll_data.get('last_tie_message_at'))
                        poll_data['tie_message_count'] = db_poll.get('tie_message_count', poll_data.get('tie_message_count', 0))
                        poll_data['revote_message_id'] = db_poll.get('revote_message_id', poll_data.get('revote_message_id'))
            except Exception as e:
                logger.warning(f"Could not load tie-state from DB for {poll_id}: {e}")

            # Anti-spam guard: send tie message only once per poll
            if poll_data.get('revote_notified'):
                logger.info(f"Tie message already sent once for poll {poll_id}; skipping re-send")
                # Ensure DB flags are set in case of in-memory-only state
                try:
                    if upsert_poll:
                        from poll_storage import update_tie_state
                        update_tie_state(
                            poll_id,
                            revote_notified=True,
                            in_revote=True,
                            last_tie_signature=tie_signature
                        )
                except Exception as e:
                    logger.warning(f"Could not persist minimal tie-state for {poll_id}: {e}")
                return "REVOTE_PROMPTED"

            tie_messages = [
                "Ой, ничья! 🤯 Похоже, наш бот в замешательстве... Помогите ему выбрать — измените голос, если сможете!\n\nПожалуйста, измените свой голос в опросе выше.",
                "Мы застряли в голосовательной пробке 🚦 Кто-нибудь, поменяйте выбор и спасите встречу!\n\nПожалуйста, измените свой голос в опросе выше.",
                "Хьюстон, у нас проблема — голоса разделились 🛸 Попробуйте переголосовать, чтобы выйти из тупика!\n\nПожалуйста, измените свой голос в опросе выше.",
                "Пока ничья 🎲 Это как ничья в шахматах — красиво, но дальше не двинемся. Подскажете путь?\n\nПожалуйста, измените свой голос в опросе выше.",
                "Бот растерян 🤖 Голоса разделились, и он не знает, что делать. Помогите ему — пересмотрите свой выбор!\n\nПожалуйста, измените свой голос в опросе выше."
            ]
            revote_message = random.choice(tie_messages)

            sent_msg = await context.bot.send_message(chat_id=chat_id, text=revote_message)

            # Mark poll as in revote state (don't close it, keep it active)
            poll_data['in_revote'] = True
            poll_data['revote_notified'] = True
            poll_data['last_tie_signature'] = tie_signature
            poll_data['tie_message_count'] = int(poll_data.get('tie_message_count', 0) or 0) + 1
            poll_data['revote_message_id'] = getattr(sent_msg, 'message_id', None)
            poll_data['last_tie_message_at'] = time.time()

            # Persist tie-state to DB
            try:
                if upsert_poll:
                    # Convert last_tie_message_at to datetime for DB if needed
                    last_dt = None
                    try:
                        from datetime import datetime
                        last_dt = datetime.utcfromtimestamp(poll_data['last_tie_message_at']) if isinstance(poll_data['last_tie_message_at'], (int, float)) else poll_data['last_tie_message_at']
                    except Exception:
                        last_dt = None
                    from poll_storage import update_tie_state
                    update_tie_state(
                        poll_id,
                        revote_notified=poll_data['revote_notified'],
                        in_revote=poll_data['in_revote'],
                        last_tie_signature=poll_data['last_tie_signature'],
                        last_tie_message_at=last_dt,
                        tie_message_count=poll_data['tie_message_count'],
                        revote_message_id=poll_data['revote_message_id']
                    )
            except Exception as e:
                logger.warning(f"Could not persist tie-state for {poll_id}: {e}")

            logger.info(f"Poll {poll_id} marked for revote - users prompted to change votes")

            # Return special marker to indicate revote prompt was sent
            return "REVOTE_PROMPTED"

        except Exception as e:
            logger.error(f"Error handling tie situation: {e}")
            # Fallback: return None to indicate failure
            return None

    async def get_most_voted_option_fallback_with_new_logic(self, poll_id, context):
        """Fallback method using manually tracked votes with new resolution logic"""
        try:
            if poll_id not in self.active_polls:
                logger.error(f"Poll {poll_id} not found in active polls for fallback")
                return None

            logger.info("Using fallback method with new resolution logic")

            # Create a mock poll object for analyze_poll_results
            mock_poll = type('MockPoll', (), {'options': []})()

            # Use the new resolution logic
            vote_analysis = await self.analyze_poll_results(mock_poll, poll_id)

            if vote_analysis['has_tie']:
                logger.info(f"Fallback: Tie detected for poll {poll_id}")
                return await self.handle_tie_situation(poll_id, context, vote_analysis)
            else:
                logger.info(f"Fallback: Winner '{vote_analysis['winner']}'")
                return vote_analysis['winner']

        except Exception as e:
            logger.error(f"Error in fallback method with new logic: {e}")
            return None

    async def meeting_in_past_guard(self, poll_id, chat_id, context, meeting_option_text) -> bool:
        """If meeting time (Warsaw) is in the past, cancel all tasks for this poll, send playful message, close and clean up.
        Returns True if handled (i.e., meeting is in the past and we performed cleanup), else False.
        """
        try:
            # Parse meeting datetime from option text using shared parser
            from scheduled_tasks import parse_meeting_datetime_from_poll_result
            meeting_dt = parse_meeting_datetime_from_poll_result(meeting_option_text)
            if meeting_dt is None:
                return False
            # Compare against current time in Polish timezone
            try:
                from zoneinfo import ZoneInfo
                polish_tz = ZoneInfo("Europe/Warsaw")
            except ImportError:
                import pytz
                polish_tz = pytz.timezone("Europe/Warsaw")
            now_pl = datetime.now(polish_tz)
            if meeting_dt <= now_pl:
                # Cancel all scheduled tasks for this chat+poll
                try:
                    from task_storage import cancel_poll_tasks
                    cancel_poll_tasks(chat_id, poll_id)
                except Exception as e:
                    logger.warning(f"Could not cancel tasks for past meeting (chat {chat_id}, poll {poll_id}): {e}")
                # Inform users
                playful = (
                    "🙈 Время встречи уже прошло — не могу её запланировать.\n"
                    "Создайте новый опрос с /create_poll"
                )
                try:
                    await context.bot.send_message(chat_id=chat_id, text=playful)
                except Exception as e:
                    logger.warning(f"Could not send past-meeting playful message in chat {chat_id}: {e}")
                # Try to stop the poll and mark it closed
                try:
                    if poll_id in self.active_polls:
                        poll_message_id = self.active_polls[poll_id].get('poll_message_id')
                        if poll_message_id:
                            try:
                                await context.bot.stop_poll(chat_id=chat_id, message_id=poll_message_id)
                            except Exception as e:
                                logger.warning(f"Could not stop poll {poll_id} in chat {chat_id}: {e}")
                        try:
                            if set_poll_closed:
                                set_poll_closed(poll_id, True)
                        except Exception as e:
                            logger.warning(f"DB set_poll_closed failed for past meeting poll {poll_id}: {e}")
                        # Cleanup local state
                        self.cleanup_poll_data(poll_id)
                        del self.active_polls[poll_id]
                except Exception as e:
                    logger.warning(f"Cleanup after past meeting failed for poll {poll_id}: {e}")
                return True
            return False
        except Exception as e:
            logger.warning(f"meeting_in_past_guard error: {e}")
            return False

    async def schedule_confirmation_message(self, poll_id, chat_id, context, poll_result, poll_voters=None):
        """Schedule 'В силе?' confirmation question - 24h before if >24h away, 4 hours before if 4-24h away"""
        try:
            # Extract date and time from poll result (e.g., "Понедельник (30.12) в 18:00")
            import re
            date_match = re.search(r'\((\d{2})\.(\d{2})\)', poll_result)
            time_match = re.search(r'в (\d{1,2}):(\d{2})', poll_result)

            if not date_match:
                logger.error(f"Could not extract date from poll result: {poll_result}")
                return

            day = int(date_match.group(1))
            month = int(date_match.group(2))
            current_year = datetime.now().year

            # Extract time if available, default to 12:00 if not found
            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2))
            else:
                hour = 12
                minute = 0

            # Create the full meeting datetime in Polish timezone
            try:
                from zoneinfo import ZoneInfo
                polish_tz = ZoneInfo("Europe/Warsaw")
            except ImportError:
                # Fallback for older Python versions
                import pytz
                polish_tz = pytz.timezone("Europe/Warsaw")

            meeting_datetime = datetime(current_year, month, day, hour, minute, 0, 0, tzinfo=polish_tz)
            now = datetime.now(polish_tz)

            # Calculate time until meeting
            time_until_meeting = (meeting_datetime - now).total_seconds()
            hours_until_meeting = time_until_meeting / 3600

            logger.info(f"Meeting datetime: {meeting_datetime.strftime('%d.%m.%Y %H:%M %Z')} (Polish time)")
            logger.info(f"Hours until meeting: {hours_until_meeting:.1f}")

            # Past-time guard: if meeting already in the past, cancel all tasks and notify
            if hours_until_meeting <= 0:
                try:
                    from task_storage import cancel_poll_tasks
                    cancel_poll_tasks(chat_id, poll_id)
                except Exception as e:
                    logger.warning(f"Could not cancel tasks for past meeting (chat {chat_id}, poll {poll_id}): {e}")
                playful = (
                    "🙈 Время встречи уже прошло — не могу её запланировать.\n"
                    "Создайте новый опрос с /create_poll"
                )
                try:
                    await context.bot.send_message(chat_id=chat_id, text=playful)
                except Exception as e:
                    logger.warning(f"Could not send past-meeting playful message in chat {chat_id}: {e}")
                return

            # Determine when to send confirmation
            if hours_until_meeting > 24:
                # More than 24 hours - send 24 hours before meeting
                confirmation_datetime = meeting_datetime - timedelta(hours=24)
                confirmation_strategy = "24 hours before meeting"
            elif hours_until_meeting > 4:
                # Less than 24 hours but more than 4 hours - send 4 hours before
                confirmation_datetime = meeting_datetime - timedelta(hours=4)
                confirmation_strategy = "4 hours before meeting"
            else:
                # Less than 4 hours - don't send confirmation
                logger.info(f"Meeting is in {hours_until_meeting:.1f} hours (<4h), skipping confirmation question")
                return

            # Calculate how long to wait
            wait_seconds = (confirmation_datetime - now).total_seconds()

            logger.info(f"Confirmation strategy: {confirmation_strategy}")
            logger.info(f"Confirmation scheduled for: {confirmation_datetime.strftime('%d.%m.%Y %H:%M')}")
            logger.info(f"Waiting {wait_seconds} seconds ({wait_seconds / 3600:.1f} hours)")

            if wait_seconds <= 0:
                logger.warning("Confirmation time is in the past, sending immediately")
                wait_seconds = FALLBACK_WAIT_TIME  # Send in 5 seconds if time is past

            # Store in database using scheduled tasks module
            try:
                from scheduled_tasks import ScheduledTaskManager
                
                success = ScheduledTaskManager.schedule_confirmation_message(
                    chat_id=chat_id,
                    poll_id=poll_id,
                    poll_result=poll_result,
                    meeting_datetime=meeting_datetime,
                    poll_voters=poll_voters
                )
                
                if not success:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="❌ Ошибка подключения к базе данных. Подтверждение встречи не может быть запланировано."
                    )
                
            except Exception as e:
                logger.error(f"Error scheduling confirmation task: {e}")
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="❌ Ошибка подключения к базе данных. Подтверждение встречи не может быть запланировано."
                )

        except Exception as e:
            logger.error(f"Error scheduling confirmation question: {e}")

    async def schedule_unpin_message(self, poll_id, chat_id, context, poll_result, pinned_message_id):
        """Schedule unpinning of confirmation message at the event time"""
        try:
            # Extract date and time from poll result (e.g., "Пятница (01.08) в 16:00")
            import re
            date_match = re.search(r'\((\d{2})\.(\d{2})\)', poll_result)
            time_match = re.search(r'в (\d{1,2}):(\d{2})', poll_result)

            if not date_match or not time_match:
                logger.error(f"Could not extract date/time from poll result: {poll_result}")
                return

            day = int(date_match.group(1))
            month = int(date_match.group(2))
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
            current_year = datetime.now().year

            # Create the event datetime
            event_datetime = datetime(current_year, month, day, hour, minute, 0, 0)

            # Calculate unpin time: 10 hours after event starts
            unpin_datetime = event_datetime + timedelta(hours=10)

            # Calculate how long to wait
            now = datetime.now()
            wait_seconds = (unpin_datetime - now).total_seconds()

            logger.info(f"Event time: {event_datetime.strftime('%d.%m.%Y %H:%M')}")
            logger.info(f"Unpin scheduled for: {unpin_datetime.strftime('%d.%m.%Y %H:%M')} (10 hours after event)")
            logger.info(f"Waiting {wait_seconds} seconds ({wait_seconds / 3600:.1f} hours) for unpin")

            if wait_seconds <= 0:
                logger.warning("Event time is in the past, unpinning immediately")
                wait_seconds = FALLBACK_WAIT_TIME  # Unpin in 5 seconds if time is past

            # Store in database using scheduled tasks module
            try:
                from scheduled_tasks import ScheduledTaskManager, parse_meeting_datetime_from_poll_result
                
                meeting_datetime = parse_meeting_datetime_from_poll_result(poll_result)
                if meeting_datetime:
                    success = ScheduledTaskManager.schedule_unpin_message(
                        chat_id=chat_id,
                        poll_id=poll_id,
                        meeting_datetime=meeting_datetime,
                        message_id=pinned_message_id
                    )
                    
                    if not success:
                        logger.error("Failed to schedule unpin message - database connection error")
                else:
                    logger.error("Could not parse meeting datetime for unpin scheduling")
                
            except Exception as e:
                logger.error(f"Error scheduling unpin task: {e}")

        except Exception as e:
            logger.error(f"Error scheduling unpin message: {e}")

    async def unpin_confirmation_message(self, poll_id, chat_id, context):
        """Unpin the confirmation message"""
        try:
            pin_key = f"{chat_id}_{poll_id}"
            if pin_key in self.pinned_messages:
                pinned_info = self.pinned_messages[pin_key]

                await context.bot.unpin_chat_message(
                    chat_id=pinned_info['chat_id'],
                    message_id=pinned_info['message_id']
                )

                logger.info(f"Unpinned confirmation message in chat {chat_id}")

                # Clean up the stored pinned message info
                del self.pinned_messages[pin_key]
            else:
                logger.warning(f"No pinned message found for poll {poll_id} in chat {chat_id}")

        except Exception as e:
            logger.error(f"Error unpinning confirmation message: {e}")

    async def cancel_bot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel all scheduled tasks and unpin messages for this chat"""
        try:
            chat_id = update.effective_chat.id

            # Cancel all scheduled tasks for this chat
            cancelled_count = 0
            if chat_id in self.scheduled_tasks:
                for task_info in self.scheduled_tasks[chat_id]:
                    if not task_info['task'].done():
                        task_info['task'].cancel()
                        cancelled_count += 1
                        logger.info(f"Cancelled {task_info['type']} task for poll {task_info['poll_id']}")

                # Clear the tasks list for this chat
                del self.scheduled_tasks[chat_id]

            # Clear active polls for this chat
            polls_cleared = 0
            active_polls_to_remove = []
            for poll_id, poll_data in self.active_polls.items():
                if poll_data['chat_id'] == chat_id:
                    active_polls_to_remove.append(poll_id)
                    polls_cleared += 1

            for poll_id in active_polls_to_remove:
                # Attempt to stop the poll in Telegram and mark it closed in DB
                try:
                    poll_message_id = self.active_polls[poll_id].get('poll_message_id')
                    if poll_message_id:
                        try:
                            await context.bot.stop_poll(chat_id=chat_id, message_id=poll_message_id)
                            logger.info(f"Stopped poll {poll_id} in chat {chat_id} due to /cancel_bot")
                        except Exception as e:
                            logger.warning(f"Could not stop poll {poll_id} during /cancel_bot: {e}")
                    # Persist closed state
                    try:
                        if set_poll_closed:
                            set_poll_closed(poll_id, True)
                    except Exception as e:
                        logger.warning(f"DB set_poll_closed failed for {poll_id} during /cancel_bot: {e}")

                    # Cancel any pending poll-specific tasks (e.g., voting timeout)
                    try:
                        from task_storage import cancel_chat_tasks
                        cancelled = cancel_chat_tasks(chat_id)
                        logger.info(f"Cancelled {cancelled} pending tasks for poll {poll_id} in chat {chat_id}")
                    except Exception as e:
                        logger.warning(f"Could not cancel pending tasks for poll {poll_id}: {e}")
                except Exception as e:
                    logger.warning(f"Unexpected error while closing poll {poll_id} during /cancel_bot: {e}")

                # Clean up local state
                self.cleanup_poll_data(poll_id)
                del self.active_polls[poll_id]
                logger.info(f"Cleared active poll {poll_id} for chat {chat_id}")

            # Removed: confirmation message tracking - no reactions needed
            confirmations_cleared = 0

            # Cancel all scheduled tasks in database for this chat
            cancelled_db_tasks = 0
            try:
                from task_storage import cancel_chat_tasks
                cancelled_db_tasks = cancel_chat_tasks(chat_id)
                logger.info(f"Cancelled {cancelled_db_tasks} scheduled tasks in database for chat {chat_id}")
            except Exception as db_error:
                logger.error(f"Error cancelling database tasks for chat {chat_id}: {db_error}")

            # Disable immediate confirmation buttons for this chat
            disabled_immediate_count = 0
            immediate_keys_to_remove = []
            for immediate_id, immediate_data in self.immediate_confirmation_messages.items():
                if immediate_data['chat_id'] == chat_id:
                    try:
                        # Edit message to remove buttons and show cancellation
                        disabled_text = f"❌ Встреча отменена\n\n{immediate_data['poll_result']}"
                        await context.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=immediate_data['message_id'],
                            text=disabled_text
                        )
                        disabled_immediate_count += 1
                        immediate_keys_to_remove.append(immediate_id)
                        logger.info(
                            f"Disabled immediate confirmation buttons for message {immediate_data['message_id']} in chat {chat_id}")
                    except Exception as e:
                        logger.warning(
                            f"Could not disable immediate confirmation buttons for message {immediate_data['message_id']}: {e}")

            for immediate_id in immediate_keys_to_remove:
                del self.immediate_confirmation_messages[immediate_id]

            # Unpin all pinned messages for this chat
            unpinned_count = 0
            pinned_keys_to_remove = []
            for pin_key, pinned_info in self.pinned_messages.items():
                if pinned_info['chat_id'] == chat_id:
                    try:
                        await context.bot.unpin_chat_message(
                            chat_id=chat_id,
                            message_id=pinned_info['message_id']
                        )
                        unpinned_count += 1
                        pinned_keys_to_remove.append(pin_key)
                        logger.info(f"Unpinned message {pinned_info['message_id']} in chat {chat_id}")
                    except Exception as e:
                        logger.warning(f"Could not unpin message {pinned_info['message_id']}: {e}")

            # Remove unpinned messages from tracking
            for pin_key in pinned_keys_to_remove:
                del self.pinned_messages[pin_key]

            # Send confirmation message
            response_text = (
                f"🛑 Бот отменён для этого чата!\n\n"
                f"📋 Отменено задач: {cancelled_count}\n"
                f"🗳️ Очищено опросов: {polls_cleared}\n"
                f"💬 Очищено подтверждений: {confirmations_cleared}\n"
                f"🗄️ Отменено запланированных задач в БД: {cancelled_db_tasks}\n"
                f"⏰ Отключено немедленных подтверждений: {disabled_immediate_count}\n"
                f"📌 Откреплено сообщений: {unpinned_count}\n\n"
                f"Используйте /create_poll чтобы создать новый опрос."
            )

            await update.message.reply_text(response_text)
            logger.info(
                f"Bot cancelled for chat {chat_id} - {cancelled_count} tasks cancelled, {polls_cleared} polls cleared, {confirmations_cleared} confirmations cleared, {unpinned_count} messages unpinned")

        except Exception as e:
            logger.error(f"Error cancelling bot: {e}")
            await update.message.reply_text("❌ Ошибка при отмене бота. Попробуйте ещё раз.")

    async def schedule_followup_message(self, chat_id, context, poll_result):
        """Schedule follow-up message 72 hours after the meeting"""
        try:
            # Extract date and time from poll result (e.g., "Понедельник (30.12) в 18:00")
            import re
            from zoneinfo import ZoneInfo

            date_match = re.search(r'\((\d{2})\.(\d{2})\)', poll_result)
            time_match = re.search(r'в (\d{1,2}):(\d{2})', poll_result)

            if not date_match:
                logger.error(f"Could not extract date from poll result for follow-up: {poll_result}")
                return

            day = int(date_match.group(1))
            month = int(date_match.group(2))
            current_year = datetime.now().year

            # Extract time if available, default to 12:00 if not found
            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2))
            else:
                hour = 12
                minute = 0

            # Create the full meeting datetime in Polish timezone
            try:
                polish_tz = ZoneInfo("Europe/Warsaw")
            except ImportError:
                # Fallback for older Python versions
                import pytz
                polish_tz = pytz.timezone("Europe/Warsaw")

            meeting_datetime = datetime(current_year, month, day, hour, minute, 0, 0, tzinfo=polish_tz)

            # Calculate 72 hours (3 days) after the meeting
            followup_datetime = meeting_datetime + timedelta(hours=72)

            # Calculate how long to wait
            now = datetime.now(polish_tz)
            wait_seconds = (followup_datetime - now).total_seconds()

            logger.info(f"Meeting at: {meeting_datetime.strftime('%d.%m.%Y %H:%M %Z')} (Polish time)")
            logger.info(
                f"Follow-up scheduled for: {followup_datetime.strftime('%d.%m.%Y %H:%M %Z')} (72 hours after meeting)")
            logger.info(f"Waiting {wait_seconds} seconds ({wait_seconds / 3600:.1f} hours) for follow-up")

            if wait_seconds <= 0:
                logger.warning("Follow-up time is in the past, sending immediately")
                wait_seconds = FALLBACK_WAIT_TIME  # Send in 5 seconds if time is past

            # Store in database using scheduled tasks module
            try:
                from scheduled_tasks import ScheduledTaskManager
                
                success = ScheduledTaskManager.schedule_followup_message(
                    chat_id=chat_id,
                    poll_result=poll_result,
                    meeting_datetime=meeting_datetime
                )
                
                if not success:
                    logger.error("Failed to schedule follow-up message - database connection error")
                
            except Exception as e:
                logger.error(f"Error scheduling follow-up task: {e}")

        except Exception as e:
            logger.error(f"Error scheduling follow-up message: {e}")

    async def send_followup_message(self, chat_id, context):
        """Send follow-up message suggesting to create another poll"""
        try:
            followup_text = (
                "🔄 Как прошла встреча? Готовы планировать следующую?\n\n"
                "Используйте /create_poll чтобы создать новый опрос для следующей встречи!"
            )

            await context.bot.send_message(
                chat_id=chat_id,
                text=followup_text
            )

            logger.info(f"Sent follow-up message to chat {chat_id}")

        except Exception as e:
            logger.error(f"Error sending follow-up message: {e}")

    async def send_confirmation_message(self, chat_id, poll_result, context, poll_voters=None, poll_id=None):
        """Send immediate confirmation message with inline keyboard buttons"""
        try:
            # Determine prefix (Сегодня/Завтра) based on meeting date in Polish timezone
            prefix = ""
            try:
                from scheduled_tasks import parse_meeting_datetime_from_poll_result
                meeting_dt = parse_meeting_datetime_from_poll_result(poll_result)
                if meeting_dt is not None:
                    try:
                        from zoneinfo import ZoneInfo
                        polish_tz = ZoneInfo("Europe/Warsaw")
                    except ImportError:
                        import pytz
                        polish_tz = pytz.timezone("Europe/Warsaw")
                    now_pl = datetime.now(polish_tz)
                    meeting_date = meeting_dt.date()
                    today_date = now_pl.date()
                    if meeting_date == today_date:
                        prefix = "Сегодня "
                    elif meeting_date == (today_date + timedelta(days=1)):
                        prefix = "Завтра "
            except Exception as _:
                # If anything fails, fall back to no prefix
                pass

            # Extract clean meeting label from poll_result to avoid any debug suffixes
            meeting_label = poll_result
            try:
                import re
                m = re.search(r"[А-ЯA-ZЁ][а-яa-zё]+\s*\(\d{2}\.\d{2}\)(?:\s+в\s+\d{1,2}:\d{2})?", poll_result)
                if m:
                    meeting_label = m.group(0)
            except Exception:
                meeting_label = poll_result

            # Format meeting text: omit time if it's today, keep time if it's tomorrow
            meeting_text = meeting_label
            if prefix.strip() == "Сегодня":
                try:
                    import re
                    meeting_text = re.sub(r"\s+в\s+\d{1,2}:\d{2}$", "", meeting_label)
                except Exception:
                    meeting_text = meeting_label
            confirmation_text = f"{prefix}План в силе? 💪 {meeting_text}" 

            # Playful slow-processing notice (processing might take a moment)
            # await context.bot.send_message(chat_id=chat_id, text="🤖 Бот иногда задумывается. Если кнопка не сработала сразу — дайте ему минутку-другую 😊")

            # Create inline keyboard for confirmation
            keyboard = [
                [
                    InlineKeyboardButton("👍 Да, продолжаем!",
                                         callback_data=f"proceed_yes_{chat_id}_{int(time.time())}"),
                    InlineKeyboardButton("❌ Нет, отменить", callback_data=f"proceed_no_{chat_id}_{int(time.time())}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            # Send the confirmation message with inline keyboard
            message = await context.bot.send_message(
                chat_id=chat_id,
                text=confirmation_text,
                reply_markup=reply_markup
            )

            # Store immediate confirmation message info for later management
            immediate_conf_id = f"immediate_{chat_id}_{message.message_id}"
            if not hasattr(self, 'immediate_confirmation_messages'):
                self.immediate_confirmation_messages = {}

            self.immediate_confirmation_messages[immediate_conf_id] = {
                'chat_id': chat_id,
                'message_id': message.message_id,
                'poll_result': poll_result,
                'context': context,
                'confirmed_users': set(),
                'declined_users': set(),
                'all_voters': poll_voters or set(),
                'poll_id': poll_id,
            }

            # Persist immediate confirmation
            try:
                if upsert_immediate_confirmation:
                    upsert_immediate_confirmation(
                        chat_id=chat_id,
                        message_id=message.message_id,
                        poll_result=poll_result,
                        poll_id=None,
                        all_voters=poll_voters or set(),
                        confirmed_users=set(),
                        declined_users=set(),
                    )
            except Exception as e:
                logger.warning(f"Could not persist immediate confirmation for chat {chat_id}: {e}")

            logger.info(
                f"Sent immediate confirmation with inline keyboard: '{confirmation_text}' (ID: {immediate_conf_id})")

        except Exception as e:
            logger.error(f"Error sending immediate confirmation message: {e}")

    async def monitor_poll_voting(self, poll_id):
        """Monitor poll voting for 1 hour and ping non-voters - now stores task in database"""
        logger.info(f"Starting 1-hour countdown for poll {poll_id}")
        
        # Get poll data to calculate missing votes
        if poll_id not in self.active_polls:
            logger.warning(f"Poll {poll_id} not found in active polls")
            return
            
        poll_data = self.active_polls[poll_id]
        chat_id = poll_data['chat_id']
        vote_count = poll_data['vote_count']
        target_member_count = poll_data.get('target_member_count', 1)
        
        # Store poll voting timeout in database using scheduled tasks module
        try:
            from scheduled_tasks import ScheduledTaskManager
            
            # Store missing vote count for the reminder
            missing_votes = target_member_count - vote_count
            
            success = ScheduledTaskManager.schedule_poll_voting_timeout(
                chat_id=chat_id,
                poll_id=poll_id,
                missing_votes=missing_votes
            )
            
            if not success:
                logger.error(f"Poll voting timeout for {poll_id} cannot be scheduled - database connection error")
            
            return  # Exit function - reminder will be sent by scheduled task
            
        except Exception as e:
            logger.error(f"Error scheduling poll voting timeout: {e}")
            return
    # Immediate reminder updates fully removed

    async def cleanup_expired_sessions(self):
        """Periodically clean up expired sessions"""
        while True:
            try:
                current_time = datetime.now()
                expired_sessions = []

                for chat_id, chat_sessions in list(self.sessions.items()):
                    for user_id, session in list(chat_sessions.items()):
                        last_activity = session.get('last_activity', session.get('created_at', current_time))
                        time_since_activity = (current_time - last_activity).total_seconds()

                        if time_since_activity > self.session_timeout:
                            expired_sessions.append((chat_id, user_id))
                            logger.info(
                                f"Session for user {user_id} in chat {chat_id} expired after {time_since_activity / 3600:.1f} hours")

                # Remove expired sessions
                for chat_id, user_id in expired_sessions:
                    if chat_id in self.sessions and user_id in self.sessions[chat_id]:
                        del self.sessions[chat_id][user_id]
                        if not self.sessions[chat_id]:  # Remove empty chat session
                            del self.sessions[chat_id]
                        logger.info(f"Cleaned up expired session for user {user_id} in chat {chat_id}")

                if expired_sessions:
                    logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")

                # Store session cleanup task in database using scheduled tasks module
                try:
                    from scheduled_tasks import ScheduledTaskManager
                    
                    success = ScheduledTaskManager.schedule_session_cleanup()
                    
                    if not success:
                        logger.error("Session cleanup cannot be scheduled - database connection error")
                    
                    break  # Exit the loop - next cleanup will be handled by scheduled task
                    
                except Exception as e:
                    logger.error(f"Error scheduling session cleanup: {e}")
                    break  # Exit the loop

            except Exception as e:
                logger.error(f"Error in session cleanup: {e}")
                # Store session cleanup task in database for error recovery
                try:
                    from scheduled_tasks import ScheduledTaskManager
                    
                    success = ScheduledTaskManager.schedule_session_cleanup()
                    
                    if not success:
                        logger.error("Session cleanup cannot be scheduled after error - database connection error")
                    
                    break  # Exit the loop - next cleanup will be handled by scheduled task
                    
                except Exception as e:
                    logger.error(f"Error scheduling session cleanup after error: {e}")
                    break  # Exit the loop

    def is_session_valid(self, chat_id, user_id):
        """Check if a session is still valid"""
        if chat_id not in self.sessions or user_id not in self.sessions[chat_id]:
            return False

        session = self.sessions[chat_id][user_id]
        current_time = datetime.now()
        last_activity = session.get('last_activity', session.get('created_at', current_time))
        time_since_activity = (current_time - last_activity).total_seconds()

        return time_since_activity <= self.session_timeout

    def cleanup_poll_data(self, poll_id):
        """Clean up all data associated with a poll, including user vote states"""
        # Clean up user vote states for this poll
        vote_state_keys_to_remove = []
        for key in self.user_vote_states.keys():
            if key.startswith(f"{poll_id}_"):
                vote_state_keys_to_remove.append(key)

        for key in vote_state_keys_to_remove:
            del self.user_vote_states[key]

        if vote_state_keys_to_remove:
            logger.info(f"Cleaned up {len(vote_state_keys_to_remove)} user vote states for poll {poll_id}")

    async def handle_cant_make_it_vote(self, poll_id, user_id, context):
        """Handle when a user votes only for 'Не могу 😔'"""
        try:
            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            chat_id = poll_data['chat_id']
            vote_counts = poll_data['vote_counts']
            target_member_count = poll_data.get('target_member_count', 1)

            # Get user info to format username
            try:
                user_info = await context.bot.get_chat_member(chat_id, user_id)
                user = user_info.user
                if user.username:
                    user_mention = f"@{user.username}"
                else:
                    # Fallback to first name if no username
                    user_mention = f"[{user.first_name}](tg://user?id={user_id})"
                    parse_mode = 'Markdown'
            except:
                # Fallback to user ID if can't get user info
                user_mention = f"[{user_id}](tg://user?id={user_id})"
                parse_mode = 'Markdown'

            # Send notification about user not being able to attend
            cant_make_it_message = f"{user_mention} не сможет присоединиться к встрече"
            if user_mention.startswith('@'):
                await context.bot.send_message(chat_id=chat_id, text=cant_make_it_message)
            else:
                await context.bot.send_message(chat_id=chat_id, text=cant_make_it_message, parse_mode='Markdown')

            # Check if everyone has voted
            all_voters = set()
            for voters in vote_counts.values():
                all_voters.update(voters)

            if len(all_voters) >= target_member_count:
                # Everyone has voted, ask if we should ignore this user and proceed
                logger.info(f"Everyone voted, asking if should ignore user {user_id} and proceed")
                await self.ask_ignore_user_and_proceed(poll_id, user_id, context)

        except Exception as e:
            logger.error(f"Error handling 'Не могу' vote: {e}")

    async def handle_cant_make_it_users(self, poll_id, cant_make_it_users, context):
        """Handle users who voted only 'Не могу' after everyone has voted"""
        try:
            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            chat_id = poll_data['chat_id']

            if not cant_make_it_users:
                return

            # Collect all user mentions
            user_mentions = []
            has_markdown_users = False

            for user_id in cant_make_it_users:
                try:
                    user_info = await context.bot.get_chat_member(chat_id, user_id)
                    user = user_info.user
                    if user.username:
                        user_mentions.append(f"@{user.username}")
                    else:
                        # Fallback to first name if no username
                        user_mentions.append(f"[{user.first_name}](tg://user?id={user_id})")
                        has_markdown_users = True
                except:
                    # Fallback to user ID if can't get user info
                    user_mentions.append(f"[User {user_id}](tg://user?id={user_id})")
                    has_markdown_users = True

            # Create a single message with all users
            if len(user_mentions) == 1:
                message = f"{user_mentions[0]} не сможет присоединиться к встрече 😔"
            else:
                # Join users with commas and "и" for the last one
                users_text = ", ".join(user_mentions[:-1]) + f" и {user_mentions[-1]}"
                message = f"{users_text} не смогут присоединиться к встрече 😔"

            # Add playful ending with cancel command
            message += "\n\n🤖 🎪 Надумаете отменить - всегда есть команда /cancel_bot!"

            # Send single message with appropriate parse mode
            parse_mode = 'Markdown' if has_markdown_users else None
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=parse_mode
            )

        except Exception as e:
            logger.error(f"Error handling can't make it users: {e}")

    # Removed: ask_proceed_with_thumbs_up function - no reaction tracking needed

    async def monitor_proceed_timeout(self, poll_id, timeout_seconds):
        """Monitor proceed confirmation timeout"""
        try:
            # DISABLED: asyncio.sleep not supported on PythonAnywhere
            # Proceed confirmations now have no timeout and remain active indefinitely
            logger.info(f"Proceed timeout monitoring disabled for poll {poll_id} (PythonAnywhere compatibility)")
            return

            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            proceed_data = poll_data.get('proceed_confirmation')

            if not proceed_data:
                return

            chat_id = poll_data['chat_id']

            # Timeout reached without confirmation - cancel meeting
            logger.info(f"Cancelling poll {poll_id} - timeout reached without confirmation")

            playful_cancellations = [
                "😅 Никто не подтвердил... Видимо, встреча отменяется! Может, в следующий раз повезёт больше?",
                "🤷‍♂️ Тишина... Похоже, все передумали встречаться! Ну что ж, бывает.",
                "😴 Все заснули? Никто не ответил, так что встреча отменяется. Спокойной ночи!",
                "🦗 Слышу только сверчков... Встреча отменена за неимением энтузиазма!",
                "🎭 Драма! Никто не хочет встречаться. Занавес опускается, встреча отменена!"
            ]

            import random
            cancel_message = random.choice(playful_cancellations)
            await poll_data['context'].bot.send_message(
                chat_id=chat_id,
                text=cancel_message
            )

            # Close poll and clean up
            await self.close_poll_and_suggest_new(poll_id, poll_data['context'])

            # Clean up proceed confirmation data
            if 'proceed_confirmation' in poll_data:
                del poll_data['proceed_confirmation']

        except Exception as e:
            logger.error(f"Error monitoring proceed timeout: {e}")

    # Removed: check_thumbs_up_threshold function - no reaction tracking needed

    async def handle_proceed_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
        """Handle proceed confirmation buttons"""
        query = update.callback_query
        user_id = update.effective_user.id

        # Decide which flow to use (immediate confirmation vs regular proceed)
        # Immediate confirmation pattern: proceed_yes_<chat_id>_<timestamp> or proceed_no_<chat_id>_<timestamp>
        try:
            import re
            m = re.match(r"^proceed_(yes|no)_(-?\d+)_(\d+)$", data)
            if m:
                action = m.group(1)
                chat_id = int(m.group(2))
                # timestamp = m.group(3)  # not used, but validates format
                await self.handle_immediate_confirmation_button(action, chat_id, user_id, query, context)
                return
        except Exception:
            pass

        # Regular proceed pattern: proceed_yes_<poll_id> or proceed_no_<poll_id>
        parts = data.split('_', 2)
        if len(parts) >= 3 and parts[0] == 'proceed' and parts[1] in ('yes', 'no'):
            action = parts[1]
            poll_id = parts[2]
            if action == 'yes':
                await self.handle_proceed_yes(poll_id, user_id, query, context)
            else:
                await self.handle_proceed_no(poll_id, user_id, query, context)
        else:
            # Unknown format; ignore gracefully
            await query.answer("Некорректные данные кнопки", show_alert=True)

    async def handle_immediate_confirmation_button(self, action, chat_id, user_id, query, context):
        """Handle immediate confirmation buttons (yes/no for 'План в силе?')"""
        try:
            user_mention = f"@{query.from_user.username}" if query.from_user.username else f"[{query.from_user.first_name}](tg://user?id={user_id})"

            # Find the immediate confirmation message for this chat
            immediate_conf_data = None
            immediate_conf_id = None
            for conf_id, conf_data in self.immediate_confirmation_messages.items():
                if conf_data['chat_id'] == chat_id and conf_data['message_id'] == query.message.message_id:
                    immediate_conf_data = conf_data
                    immediate_conf_id = conf_id
                    break

            if not immediate_conf_data:
                await query.answer("❌ Подтверждение не найдено.", show_alert=True)
                return

            # If we don't have voter data (e.g., older scheduled messages), try to reconstruct it from DB
            try:
                if not immediate_conf_data.get('all_voters'):
                    from poll_storage import get_poll, get_votes
                    poll = None
                    pid = immediate_conf_data.get('poll_id')
                    if pid:
                        poll = get_poll(pid)
                    if poll:
                        options = poll.get('options', [])
                        # Find selected option index by matching poll_result text
                        selected_idx = None
                        try:
                            normalized_result = (immediate_conf_data.get('poll_result') or '').strip()
                            for i, opt in enumerate(options):
                                if (opt or '').strip() == normalized_result:
                                    selected_idx = i
                                    break
                        except Exception:
                            selected_idx = None
                        votes_by_user = get_votes(poll.get('poll_id')) if poll.get('poll_id') else {}
                        reconstructed = set()
                        if selected_idx is not None:
                            for uid_str, option_ids in (votes_by_user or {}).items():
                                try:
                                    if selected_idx in option_ids:
                                        reconstructed.add(int(uid_str))
                                except Exception:
                                    continue
                        else:
                            # Fallback: include all voters who voted for any option except 'Не могу 😔'
                            cant_idx = None
                            for i, opt in enumerate(options):
                                if opt == 'Не могу 😔':
                                    cant_idx = i
                                    break
                            for uid_str, option_ids in (votes_by_user or {}).items():
                                try:
                                    if any((idx != cant_idx) for idx in option_ids):
                                        reconstructed.add(int(uid_str))
                                except Exception:
                                    continue
                        # Exclude the bot account id if present
                        try:
                            me = await context.bot.get_me()
                            reconstructed.discard(me.id)
                        except Exception:
                            pass
                        immediate_conf_data['all_voters'] = reconstructed
            except Exception as e:
                logger.warning(f"Could not reconstruct voter data for immediate confirmation: {e}")

            # Check if user already voted
            if user_id in immediate_conf_data['confirmed_users'] or user_id in immediate_conf_data['declined_users']:
                await query.answer("Вы уже проголосовали!", show_alert=True)
                return

            # Track the user's vote
            if action == 'yes':
                immediate_conf_data['confirmed_users'].add(user_id)
                # User confirmed they are going - send separate message
                response_text = f"✅ {user_mention} подтвердил участие!"
                logger.info(f"User {user_id} confirmed attendance for immediate confirmation in chat {chat_id}")
            else:
                immediate_conf_data['declined_users'].add(user_id)
                # User won't be able to join - send separate message
                response_text = f"{user_mention} не сможет присоединиться к встрече. Вы можете отменить встречу командой /cancel_bot"
                logger.info(f"User {user_id} won't be able to join for immediate confirmation in chat {chat_id}")

            # Send response message
            if user_mention.startswith('@'):
                await context.bot.send_message(chat_id=chat_id, text=response_text)
            else:
                await context.bot.send_message(chat_id=chat_id, text=response_text, parse_mode='Markdown')

            # Update the message to show disabled buttons for this user
            await self.update_immediate_confirmation_buttons(immediate_conf_id, user_id, context)

            # Persist updated immediate confirmation
            try:
                if upsert_immediate_confirmation:
                    stored = get_immediate_confirmation(chat_id, query.message.message_id)
                    all_voters = stored['all_voters'] if stored else immediate_conf_data.get('all_voters', set())
                    upsert_immediate_confirmation(
                        chat_id=chat_id,
                        message_id=query.message.message_id,
                        poll_result=immediate_conf_data.get('poll_result', ''),
                        poll_id=None,
                        all_voters=all_voters,
                        confirmed_users=immediate_conf_data['confirmed_users'],
                        declined_users=immediate_conf_data['declined_users'],
                    )
            except Exception as e:
                logger.warning(f"Could not persist updated immediate confirmation: {e}")

            # Check if everyone who voted in the original poll has confirmed "yes"
            await self.check_if_everyone_confirmed(immediate_conf_id, context)

            # Just answer the callback to acknowledge the button press
            await query.answer()

        except Exception as e:
            logger.error(f"Error handling immediate confirmation button: {e}")
            await query.answer("❌ Произошла ошибка при обработке ответа.", show_alert=True)

    async def update_immediate_confirmation_buttons(self, immediate_conf_id, voted_user_id, context):
        """Update immediate confirmation message to show user-specific button states"""
        try:
            conf_data = self.immediate_confirmation_messages[immediate_conf_id]
            chat_id = conf_data['chat_id']
            message_id = conf_data['message_id']

            # For now, we'll keep the same buttons for everyone since Telegram doesn't support per-user buttons
            # The duplicate vote prevention is handled in the button handler
            # This function is a placeholder for future enhancements

            logger.info(f"User {voted_user_id} vote tracked for immediate confirmation {immediate_conf_id}")

        except Exception as e:
            logger.error(f"Error updating immediate confirmation buttons: {e}")

    async def check_if_everyone_confirmed(self, immediate_conf_id, context):
        """Check if everyone who voted in the original poll has confirmed 'yes'"""
        try:
            conf_data = self.immediate_confirmation_messages[immediate_conf_id]
            chat_id = conf_data['chat_id']
            all_voters = conf_data['all_voters']
            confirmed_users = conf_data['confirmed_users']
            declined_users = conf_data['declined_users']

            # If no voters data available, skip the check
            if not all_voters:
                logger.info(f"No voter data available for immediate confirmation {immediate_conf_id}")
                return

            # Check if everyone who voted has confirmed "yes"
            if all_voters.issubset(confirmed_users):
                # Everyone confirmed! Send playful message
                playful_messages = [
                    "🎉 Все подтвердили участие! Встреча состоится! 💪",
                    "✨ Отлично! Все готовы к встрече! 🚀",
                    "🔥 Все на месте! Встреча будет огонь! 🎯",
                    "💯 Все подтвердили! Ждём крутую встречу! ⭐",
                    "🎊 Все в деле! Встреча обещает быть продуктивной! 🎪"
                ]

                import random
                playful_message = random.choice(playful_messages)

                await context.bot.send_message(
                    chat_id=chat_id,
                    text=playful_message
                )

                logger.info(f"Everyone confirmed for immediate confirmation {immediate_conf_id}")

        except Exception as e:
            logger.error(f"Error checking if everyone confirmed: {e}")
    

    async def handle_proceed_yes(self, poll_id, user_id, query, context):
        """Handle proceed yes button"""
        try:
            if poll_id not in self.active_polls:
                await query.edit_message_text("❌ Опрос не найден.")
                return

            poll_data = self.active_polls[poll_id]
            proceed_data = poll_data.get('proceed_confirmation')

            if not proceed_data:
                await query.edit_message_text("❌ Подтверждение не найдено.")
                return

            # Check if user can vote (not in can't make it list)
            cant_make_it_users = proceed_data['cant_make_it_users']
            if user_id in cant_make_it_users:
                await query.answer("Вы не можете голосовать, так как не сможете присоединиться к встрече",
                                   show_alert=True)
                return

            # Add user's yes vote
            proceed_data['yes_votes'].add(user_id)
            proceed_data['no_votes'].discard(user_id)  # Remove from no votes if previously voted no

            logger.info(f"User {user_id} voted YES for poll {poll_id}")

            # Check if we have enough responses to make a decision
            await self.check_proceed_consensus(poll_id, query, context)

        except Exception as e:
            logger.error(f"Error handling proceed yes: {e}")
            await query.edit_message_text("❌ Произошла ошибка при обработке подтверждения.")

    async def handle_proceed_no(self, poll_id, user_id, query, context):
        """Handle proceed no button"""
        try:
            if poll_id not in self.active_polls:
                await query.edit_message_text("❌ Опрос не найден.")
                return

            poll_data = self.active_polls[poll_id]
            proceed_data = poll_data.get('proceed_confirmation')

            if not proceed_data:
                await query.edit_message_text("❌ Подтверждение не найдено.")
                return

            # Check if user can vote (not in can't make it list)
            cant_make_it_users = proceed_data['cant_make_it_users']
            if user_id in cant_make_it_users:
                await query.answer("Вы не можете голосовать, так как не сможете присоединиться к встрече",
                                   show_alert=True)
                return

            # Add user's no vote
            proceed_data['no_votes'].add(user_id)
            proceed_data['yes_votes'].discard(user_id)  # Remove from yes votes if previously voted yes

            logger.info(f"User {user_id} voted NO for poll {poll_id}")

            # Check if we have enough responses to make a decision
            await self.check_proceed_consensus(poll_id, query, context)

        except Exception as e:
            logger.error(f"Error handling proceed no: {e}")
            await query.edit_message_text("❌ Произошла ошибка при обработке подтверждения.")

    async def check_proceed_consensus(self, poll_id, query, context):
        """Check if enough users have voted to make a decision"""
        try:
            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            proceed_data = poll_data.get('proceed_confirmation')

            if not proceed_data:
                return

            yes_votes = proceed_data['yes_votes']
            no_votes = proceed_data['no_votes']
            required_responses = proceed_data['required_responses']
            total_responses = len(yes_votes) + len(no_votes)

            logger.info(
                f"Poll {poll_id}: {len(yes_votes)} yes, {len(no_votes)} no, {total_responses}/{required_responses} total responses")

            # Update the message to show current vote status
            status_message = f"Продолжаем без пользователей, которые не могут прийти?\n\n👍 Да: {len(yes_votes)}\n❌ Нет: {len(no_votes)}\n\nОтветили: {total_responses}/{required_responses}"

            if total_responses >= required_responses:
                # Everyone has responded - make decision based on votes
                if len(no_votes) > 0:
                    # If at least one person voted No - cancel
                    cancel_message = f"❌ Есть голоса против продолжения. Встреча отменена.\n\nПопробуйте создать новый опрос с другими вариантами времени.\n\nИспользуйте /create_poll"
                    await query.edit_message_text(cancel_message)

                    # Close poll and clean up
                    await self.close_poll_and_clean_up(poll_id, context)
                else:
                    # Everyone voted Yes (no_votes is 0)
                    proceed_message = f"👍 Все за продолжение! Планируем встречу для остальных."
                    await query.edit_message_text(proceed_message)

                    # Proceed with resolution excluding can't make it users
                    await self.resolve_poll_excluding_cant_make_it(poll_id, context)

                # Clean up proceed confirmation data
                if 'proceed_confirmation' in poll_data:
                    del poll_data['proceed_confirmation']
            else:
                # Not everyone has responded yet - update status
                await query.edit_message_text(status_message)

        except Exception as e:
            logger.error(f"Error checking proceed consensus: {e}")

    async def handle_proceed_confirmation(self, query, poll_id, should_proceed, context):
        """Handle proceed confirmation button click"""
        try:
            await query.answer()

            if poll_id not in self.active_polls:
                await query.edit_message_text("❌ Опрос не найден.")
                return

            poll_data = self.active_polls[poll_id]
            proceed_data = poll_data.get('proceed_confirmation')

            if not proceed_data:
                await query.edit_message_text("❌ Подтверждение не найдено.")
                return

            user_id = query.from_user.id
            chat_id = poll_data['chat_id']

            if should_proceed:
                # User clicked "Yes" - proceed with meeting
                logger.info(f"User {user_id} confirmed proceeding with poll {poll_id}")

                proceed_message = f"👍 Получено подтверждение! Планируем встречу для остальных."
                await query.edit_message_text(proceed_message)

                # Proceed with resolution excluding can't make it users
                await self.resolve_poll_excluding_cant_make_it(poll_id, context)
            else:
                # User clicked "No" - cancel meeting
                logger.info(f"User {user_id} cancelled proceeding with poll {poll_id}")

                cancel_message = "❌ Встреча отменена. Попробуйте создать новый опрос с другими вариантами времени.\n\nИспользуйте /create_poll"
                await query.edit_message_text(cancel_message)

                # Close poll and clean up
                await self.close_poll_and_clean_up(poll_id, context)

            # Clean up proceed confirmation data
            if 'proceed_confirmation' in poll_data:
                del poll_data['proceed_confirmation']

        except Exception as e:
            logger.error(f"Error handling proceed confirmation: {e}")
            await query.edit_message_text("❌ Произошла ошибка при обработке подтверждения.")

    async def ask_ignore_multiple_users_and_proceed(self, poll_id, cant_make_it_users, context):
        """Ask if we should ignore multiple users who can't make it and proceed with resolution"""
        try:
            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            chat_id = poll_data['chat_id']

            # Ask for confirmation to proceed without the users who can't make it
            user_count = len(cant_make_it_users)
            confirmation_message = f"Игнорировать {user_count} пользователей и запланировать встречу для остальных?"

            # Create yes/no keyboard
            keyboard = [
                [
                    InlineKeyboardButton("Да, запланировать ✅", callback_data=f"ignore_multiple_yes_{poll_id}"),
                    InlineKeyboardButton("Нет, отменить ❌", callback_data=f"ignore_multiple_no_{poll_id}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            confirmation_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=confirmation_message,
                reply_markup=reply_markup
            )

            # Store confirmation data for later handling
            poll_data['ignore_confirmation'] = {
                'message_id': confirmation_msg.message_id,
                'cant_make_it_users': cant_make_it_users
            }

            logger.info(f"Asked for confirmation to ignore {user_count} users in poll {poll_id}")

        except Exception as e:
            logger.error(f"Error asking multiple ignore confirmation: {e}")

    async def ask_ignore_user_and_proceed(self, poll_id, cant_make_it_user_id, context):
        """Ask if we should ignore the user who can't make it and proceed with resolution"""
        try:
            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            chat_id = poll_data['chat_id']

            # Ask for confirmation to proceed without the user who can't make it
            confirmation_message = "Игнорировать этого пользователя и запланировать встречу для остальных?"

            # Create yes/no keyboard
            keyboard = [
                [
                    InlineKeyboardButton("Да, запланировать ✅",
                                         callback_data=f"ignore_yes_{poll_id}_{cant_make_it_user_id}"),
                    InlineKeyboardButton("Нет, отменить ❌", callback_data=f"ignore_no_{poll_id}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            confirmation_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=confirmation_message,
                reply_markup=reply_markup
            )

            # Store confirmation data for later handling
            poll_data['ignore_confirmation'] = {
                'message_id': confirmation_msg.message_id,
                'cant_make_it_user_id': cant_make_it_user_id
            }

            logger.info(f"Asked for confirmation to ignore user {cant_make_it_user_id} in poll {poll_id}")

        except Exception as e:
            logger.error(f"Error asking ignore confirmation: {e}")

    async def handle_ignore_confirmation(self, query, poll_id, cant_make_it_user_id, should_ignore, context):
        """Handle yes/no response to ignore user confirmation"""
        try:
            await query.answer()

            if poll_id not in self.active_polls:
                await query.edit_message_text("❌ Опрос не найден.")
                return

            poll_data = self.active_polls[poll_id]

            if should_ignore:
                # Proceed with resolution excluding the user who can't make it
                await query.edit_message_text("✅ Планируем встречу для остальных участников...")
                await self.resolve_poll_excluding_cant_make_it(poll_id, context)
            else:
                # Don't proceed, keep poll active
                await query.edit_message_text("❌ Встреча не запланирована. Опрос продолжается.")

            # Clean up confirmation data
            if 'ignore_confirmation' in poll_data:
                del poll_data['ignore_confirmation']

        except Exception as e:
            logger.error(f"Error handling ignore confirmation: {e}")
            await query.edit_message_text("❌ Произошла ошибка при обработке ответа.")

    async def handle_multiple_ignore_confirmation(self, query, poll_id, should_ignore, context):
        """Handle yes/no response to ignore multiple users confirmation"""
        try:
            await query.answer()

            if poll_id not in self.active_polls:
                await query.edit_message_text("❌ Опрос не найден.")
                return

            poll_data = self.active_polls[poll_id]

            if should_ignore:
                # Proceed with resolution excluding the users who can't make it
                await query.edit_message_text("✅ Планируем встречу для остальных участников...")
                await self.resolve_poll_excluding_cant_make_it(poll_id, context)
            else:
                # Don't proceed, keep poll active
                await query.edit_message_text("❌ Встреча не запланирована. Опрос продолжается.")
                # Clear the cant_make_it_users so they can be processed again if needed
                if 'cant_make_it_users' in poll_data:
                    del poll_data['cant_make_it_users']

            # Clean up confirmation data
            if 'ignore_confirmation' in poll_data:
                del poll_data['ignore_confirmation']

        except Exception as e:
            logger.error(f"Error handling multiple ignore confirmation: {e}")
            await query.edit_message_text("❌ Произошла ошибка при обработке ответа.")

    async def resolve_poll_excluding_cant_make_it(self, poll_id, context):
        """Resolve poll using regular rules but excluding 'Не могу' votes"""
        try:
            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            chat_id = poll_data['chat_id']
            vote_counts = poll_data['vote_counts']
            target_member_count = poll_data.get('target_member_count', 1)

            # Create vote counts excluding "Не могу 😔"
            filtered_vote_counts = {}
            for option_text, voters in vote_counts.items():
                if option_text != "Не могу 😔":
                    filtered_vote_counts[option_text] = voters

            # Get all voters who didn't vote only for "Не могу"
            other_voters = set()
            for voters in filtered_vote_counts.values():
                other_voters.update(voters)

            if not other_voters:
                # No one voted for any real options - everyone voted "Не могу"
                playful_message = "Никто не может прийти на встречу! 😅 Попробуйте создать новый опрос с другими вариантами времени.\n\nИспользуйте /create_poll"
                await context.bot.send_message(chat_id=chat_id, text=playful_message)

                # Close poll and clean up (everyone effectively voted 'Не могу')
                await self.close_poll_and_clean_up(poll_id, context, cancel_voting_timeout=True)
                return

            # Apply resolution logic to filtered votes
            logger.info(f"Filtered vote counts (excluding 'Не могу'): {filtered_vote_counts}")

            # Check for common options among other voters
            common_option = None

            # Case 1: Everyone (who didn't vote "Не могу") voted for one identical option
            for option_text, voters in filtered_vote_counts.items():
                if len(voters) == len(other_voters):
                    # Check if these voters voted ONLY for this option
                    voters_only_this_option = True
                    for voter in voters:
                        for other_option, other_voters_set in filtered_vote_counts.items():
                            if other_option != option_text and voter in other_voters_set:
                                voters_only_this_option = False
                                break
                        if not voters_only_this_option:
                            break

                    if voters_only_this_option:
                        common_option = option_text
                        logger.info(f"Case 1: All other users voted only for '{option_text}'")
                        break

            # Case 2: One option voted by everyone (who didn't vote "Не могу")
            if not common_option:
                for option_text, voters in filtered_vote_counts.items():
                    if len(voters) == len(other_voters):
                        common_option = option_text
                        logger.info(f"Case 2: All other users voted for '{option_text}'")
                        break

            # Case 4: Everyone voted for same multiple options → select earliest
            if not common_option:
                options_everyone_voted = []
                for option_text, voters in filtered_vote_counts.items():
                    if len(voters) == len(other_voters):
                        options_everyone_voted.append(option_text)

                if len(options_everyone_voted) > 1:
                    common_option = min(options_everyone_voted)
                    logger.info(f"Case 4: Selected earliest from multiple common options: '{common_option}'")

            if common_option:
                # Found common option, proceed with normal meeting confirmation
                await self.confirm_meeting_with_option(poll_id, common_option, context)
            else:
                # No common option among other voters
                no_common_message = "Остальные участники не выбрали общий вариант"
                await context.bot.send_message(chat_id=chat_id, text=no_common_message)

                # Close poll and suggest creating new one
                await self.close_poll_and_suggest_new(poll_id, context)

        except Exception as e:
            logger.error(f"Error resolving poll excluding 'Не могу': {e}")

    async def confirm_meeting_with_option(self, poll_id, option, context):
        """Confirm meeting with the selected option"""
        try:
            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            chat_id = poll_data['chat_id']

            # Early guard: if the chosen meeting time is already in the past, cancel and inform
            try:
                handled = await self.meeting_in_past_guard(poll_id, chat_id, context, option)
                if handled:
                    return
            except Exception as e:
                logger.warning(f"Past-meeting guard error in confirm_meeting_with_option for poll {poll_id}: {e}")

            # Send confirmation message
            confirmation_message = f"Собираемся в {option}"
            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=confirmation_message
            )

            # Pin the confirmation message
            try:
                await context.bot.pin_chat_message(
                    chat_id=chat_id,
                    message_id=sent_message.message_id,
                    disable_notification=True
                )
                logger.info(f"Pinned confirmation message in chat {chat_id}")

                # Store pinned message info for later unpinning
                self.pinned_messages[f"{chat_id}_{poll_id}"] = {
                    'chat_id': chat_id,
                    'message_id': sent_message.message_id
                }
            except Exception as e:
                logger.warning(f"Could not pin message in chat {chat_id}: {e}")

            # Get all voters from the poll
            poll_voters = set()
            if poll_id in self.active_polls:
                poll_data = self.active_polls[poll_id]
                if 'vote_counts' in poll_data:
                    # Extract all voters from vote_counts (which contains voters by option)
                    for option_text, voters in poll_data['vote_counts'].items():
                        poll_voters.update(voters)
                    # Exclude bots from voters
                    try:
                        bot_info = await context.bot.get_me()
                        bot_user_id = bot_info.id
                        poll_voters.discard(bot_user_id)
                    except Exception as e:
                        logger.warning(f"Could not get bot info to exclude from voters: {e}")

            # Schedule reminders
            confirmation_task = asyncio.create_task(
                self.schedule_confirmation_message(poll_id, chat_id, context, option, poll_voters))
            unpin_task = asyncio.create_task(self.schedule_unpin_message(poll_id, chat_id, context, option, sent_message.message_id))
            followup_task = asyncio.create_task(self.schedule_followup_message(chat_id, context, option))

            # Track scheduled tasks for this chat
            if chat_id not in self.scheduled_tasks:
                self.scheduled_tasks[chat_id] = []
            self.scheduled_tasks[chat_id].extend([
                {'task': confirmation_task, 'type': 'confirmation', 'poll_id': poll_id},
                {'task': unpin_task, 'type': 'unpin', 'poll_id': poll_id},
                {'task': followup_task, 'type': 'followup', 'poll_id': poll_id}
            ])

            # Close the poll
            try:
                poll_message_id = poll_data['poll_message_id']
                await context.bot.stop_poll(chat_id=chat_id, message_id=poll_message_id)
                try:
                    if set_poll_closed:
                        set_poll_closed(poll_id, True)
                except Exception as e:
                    logger.warning(f"DB set_poll_closed failed for {poll_id}: {e}")
                logger.info(f"Closed poll {poll_id} after meeting confirmation")
            except Exception as e:
                logger.warning(f"Could not close poll {poll_id}: {e}")
            # Cancel any pending voting-timeout reminders in DB for this chat
            try:
                from task_storage import cancel_chat_tasks
                cancelled = cancel_chat_tasks(chat_id, task_type="poll_voting_timeout")
                logger.info(f"Cancelled {cancelled} 'poll_voting_timeout' tasks for chat {chat_id}")
            except Exception as e:
                logger.warning(f"Could not cancel voting timeout tasks for chat {chat_id}: {e}")

            # Clean up poll data
            self.cleanup_poll_data(poll_id)
            del self.active_polls[poll_id]

        except Exception as e:
            logger.error(f"Error confirming meeting: {e}")

    async def close_poll_and_suggest_new(self, poll_id, context):
        """Close poll and suggest creating a new one"""
        try:
            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            chat_id = poll_data['chat_id']

            # Close the poll
            try:
                poll_message_id = poll_data['poll_message_id']
                await context.bot.stop_poll(chat_id=chat_id, message_id=poll_message_id)
                try:
                    if set_poll_closed:
                        set_poll_closed(poll_id, True)
                except Exception as e:
                    logger.warning(f"DB set_poll_closed failed for {poll_id}: {e}")
                logger.info(f"Closed poll {poll_id} - no common option")
            except Exception as e:
                logger.warning(f"Could not close poll {poll_id}: {e}")
            # Cancel any pending voting-timeout reminders in DB for this chat
            try:
                from task_storage import cancel_chat_tasks
                cancelled = cancel_chat_tasks(chat_id, task_type="poll_voting_timeout")
                logger.info(f"Cancelled {cancelled} 'poll_voting_timeout' tasks for chat {chat_id}")
            except Exception as e:
                logger.warning(f"Could not cancel voting timeout tasks for chat {chat_id}: {e}")

            # Suggest creating new poll
            suggest_message = "Попробуйте создать новый опрос с другими вариантами времени.\n\nИспользуйте /create_poll"
            await context.bot.send_message(chat_id=chat_id, text=suggest_message)

            # Clean up poll data
            self.cleanup_poll_data(poll_id)
            del self.active_polls[poll_id]

        except Exception as e:
            logger.error(f"Error closing poll and suggesting new: {e}")

    async def close_poll_and_clean_up(self, poll_id, context, cancel_voting_timeout: bool = False):
        """Close poll and clean up without suggesting new poll.
        If cancel_voting_timeout is True, unschedule poll_voting_timeout tasks for this chat.
        """
        try:
            if poll_id not in self.active_polls:
                return

            poll_data = self.active_polls[poll_id]
            chat_id = poll_data['chat_id']

            # Close the poll
            try:
                poll_message_id = poll_data['poll_message_id']
                await context.bot.stop_poll(chat_id=chat_id, message_id=poll_message_id)
                try:
                    if set_poll_closed:
                        set_poll_closed(poll_id, True)
                except Exception as e:
                    logger.warning(f"DB set_poll_closed failed for {poll_id}: {e}")
                logger.info(f"Closed poll {poll_id} - everyone voted 'Не могу'")
            except Exception as e:
                logger.warning(f"Could not close poll {poll_id}: {e}")

            # Always unschedule voting timeout tasks when poll is closed
            try:
                from task_storage import cancel_chat_tasks
                cancelled = cancel_chat_tasks(chat_id, task_type="poll_voting_timeout")
                logger.info(f"Cancelled {cancelled} 'poll_voting_timeout' tasks for chat {chat_id}")
            except Exception as e:
                logger.warning(f"Could not cancel voting timeout tasks for chat {chat_id}: {e}")

            # Clean up poll data
            self.cleanup_poll_data(poll_id)
            del self.active_polls[poll_id]

        except Exception as e:
            logger.error(f"Error closing poll and cleaning up: {e}")

    async def handle_pin_proposal(self, query, poll_id, should_pin, context):
        """Handle yes/no response to pin proposal"""
        try:
            await query.answer()

            if poll_id not in self.active_polls:
                await query.edit_message_text("❌ Опрос не найден.")
                return

            poll_data = self.active_polls[poll_id]
            proposal_data = poll_data.get('proposal')

            if not proposal_data:
                await query.edit_message_text("❌ Предложение не найдено.")
                return

            proposed_option = proposal_data['proposed_option']
            chat_id = poll_data['chat_id']

            if should_pin:
                # Early guard: if the chosen meeting time is already in the past, cancel and inform
                try:
                    handled = await self.meeting_in_past_guard(poll_id, chat_id, context, proposed_option)
                    if handled:
                        return
                except Exception as e:
                    logger.warning(f"Past-meeting guard error in handle_pin_proposal for poll {poll_id}: {e}")

                # Pin the meeting confirmation and schedule reminders
                confirmation_message = f"Собираемся в {proposed_option}"

                sent_message = await context.bot.send_message(
                    chat_id=chat_id,
                    text=confirmation_message
                )

                # Pin the confirmation message
                try:
                    await context.bot.pin_chat_message(
                        chat_id=chat_id,
                        message_id=sent_message.message_id,
                        disable_notification=True
                    )
                    logger.info(f"Pinned proposal confirmation message in chat {chat_id}")

                    # Store pinned message info for later unpinning
                    self.pinned_messages[f"{chat_id}_{poll_id}"] = {
                        'chat_id': chat_id,
                        'message_id': sent_message.message_id
                    }
                except Exception as e:
                    logger.warning(f"Could not pin proposal message in chat {chat_id}: {e}")

                # Get all voters from the poll
                poll_voters = set()
                if poll_id in self.active_polls:
                    poll_data = self.active_polls[poll_id]
                    if 'vote_counts' in poll_data:
                        # Extract all voters from vote_counts (which contains voters by option)
                        for option_text, voters in poll_data['vote_counts'].items():
                            poll_voters.update(voters)
                        # Exclude bots from voters
                        try:
                            bot_info = await context.bot.get_me()
                            bot_user_id = bot_info.id
                            poll_voters.discard(bot_user_id)
                        except Exception as e:
                            logger.warning(f"Could not get bot info to exclude from voters: {e}")

                # Schedule reminders for the proposed meeting
                confirmation_task = asyncio.create_task(
                    self.schedule_confirmation_message(poll_id, chat_id, context, proposed_option, poll_voters))
                
                # Schedule unpin message using ScheduledTaskManager
                try:
                    from scheduled_tasks import ScheduledTaskManager
                    from datetime import datetime
                    
                    # Parse the proposed option to get meeting datetime
                    meeting_datetime = self.parse_meeting_time(proposed_option)
                    if meeting_datetime:
                        ScheduledTaskManager.schedule_unpin_message(
                            chat_id=chat_id,
                            poll_id=poll_id,
                            meeting_datetime=meeting_datetime,
                            message_id=sent_message.message_id
                        )
                        logger.info(f"Scheduled unpin task for meeting at {meeting_datetime}")
                    else:
                        logger.warning(f"Could not parse meeting time from: {proposed_option}")
                except ImportError:
                    logger.warning("ScheduledTaskManager not available - unpin task not scheduled")
                except Exception as e:
                    logger.error(f"Error scheduling unpin task: {e}")
                
                followup_task = asyncio.create_task(self.schedule_followup_message(chat_id, context, proposed_option))

                # Track scheduled tasks for this chat
                if chat_id not in self.scheduled_tasks:
                    self.scheduled_tasks[chat_id] = []
                self.scheduled_tasks[chat_id].extend([
                    {'task': confirmation_task, 'type': 'confirmation', 'poll_id': poll_id},
                    {'task': followup_task, 'type': 'followup', 'poll_id': poll_id}
                ])
                # Note: unpin task is now handled by ScheduledTaskManager, not asyncio

                await query.edit_message_text(
                    f"✅ Встреча запланирована: {proposed_option}\n📌 Сообщение закреплено и напоминания настроены!")

                # Close the poll since meeting is confirmed
                try:
                    poll_message_id = poll_data['poll_message_id']
                    await context.bot.stop_poll(chat_id=chat_id, message_id=poll_message_id)
                    try:
                        if set_poll_closed:
                            set_poll_closed(poll_id, True)
                    except Exception as e:
                        logger.warning(f"DB set_poll_closed failed for {poll_id}: {e}")
                    logger.info(f"Closed poll {poll_id} after proposal confirmation")
                except Exception as e:
                    logger.warning(f"Could not close poll {poll_id}: {e}")

            else:
                await query.edit_message_text(f"❌ Встреча не запланирована. Опрос продолжается.")

            # Clean up proposal data
            if 'proposal' in poll_data:
                del poll_data['proposal']

        except Exception as e:
            logger.error(f"Error handling pin proposal: {e}")
            await query.edit_message_text("❌ Произошла ошибка при обработке ответа.")


def main():
    """Main function"""
    print("🚀 Starting Simple Poll Bot...")

    token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not token:
        print("❌ TELEGRAM_BOT_TOKEN not found in environment!")
        return

    bot = SimplePollBot(token)

    # Create application
    app = Application.builder().token(token).build()

    # Add handlers
    app.add_handler(CommandHandler("start", bot.start))
    app.add_handler(CommandHandler("help", bot.help_command))
    app.add_handler(CommandHandler("info", bot.info_command))
    app.add_handler(CommandHandler("create_poll", bot.create_poll))
    app.add_handler(CommandHandler("cancel_bot", bot.cancel_bot))
    app.add_handler(CommandHandler("die", bot.die_command))
    app.add_handler(CommandHandler("subscribe", handle_subscribe))
    app.add_handler(CommandHandler("unsubscribe", handle_unsubscribe))
    app.add_handler(CommandHandler("subscribers", handle_subscribers_count))
    app.add_handler(CallbackQueryHandler(bot.button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.text_handler))
    app.add_handler(PollAnswerHandler(bot.poll_answer_handler))
    # Removed: MessageReactionHandler - no reaction tracking needed

    print("✅ Simple Poll Bot is running...")
    print("⏹️  Press Ctrl+C to stop")

    # Run the bot
    app.run_polling()


if __name__ == "__main__":
    main()
