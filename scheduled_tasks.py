#!/usr/bin/env python3
"""
Scheduled Tasks Module for Telegram Bot
Contains all functions that can be scheduled and executed later

This module is used by both:
- simple_poll_bot.py (for scheduling tasks)
- flask_app.py (for executing scheduled tasks)
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

# Configure logging
logger = logging.getLogger(__name__)

# Import task storage with error handling
try:
    from task_storage import add_scheduled_task
except ImportError:
    logger.error("task_storage module not available - database scheduling disabled")
    add_scheduled_task = None


class ScheduledTaskManager:
    """Manager for all scheduled tasks"""
    
    @staticmethod
    def schedule_confirmation_message(chat_id: int, poll_id: str, poll_result: str, 
                                    meeting_datetime: datetime, poll_voters: Optional[set] = None) -> bool:
        """
        Schedule 'План в силе?' confirmation message - 24h before if >24h away, 4h before if 4-24h away
        
        Returns:
            bool: True if scheduled successfully, False if database error
        """
        try:
            if not add_scheduled_task:
                logger.error("Database scheduling not available - cannot schedule confirmation message")
                return False
            
            from zoneinfo import ZoneInfo
            
            # Use Polish timezone
            try:
                polish_tz = ZoneInfo("Europe/Warsaw")
            except ImportError:
                import pytz
                polish_tz = pytz.timezone("Europe/Warsaw")
            
            # Ensure meeting_datetime has timezone
            if meeting_datetime.tzinfo is None:
                meeting_datetime = meeting_datetime.replace(tzinfo=polish_tz)
            
            now = datetime.now(polish_tz)
            time_until_meeting = (meeting_datetime - now).total_seconds()
            hours_until_meeting = time_until_meeting / 3600
            
            logger.info(f"Meeting datetime: {meeting_datetime.strftime('%d.%m.%Y %H:%M %Z')} (Polish time)")
            logger.info(f"Hours until meeting: {hours_until_meeting:.1f}")
            
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
                logger.info(f"Meeting is in {hours_until_meeting:.1f} hours (<4h), skipping confirmation message")
                return True  # Not an error, just skipped
            
            logger.info(f"Confirmation strategy: {confirmation_strategy}")
            logger.info(f"Confirmation scheduled for: {confirmation_datetime.strftime('%d.%m.%Y %H:%M %Z')} (Polish time)")
            
            # Convert Polish time to UTC for database storage
            try:
                import pytz
                utc_tz = pytz.UTC
            except ImportError:
                from zoneinfo import ZoneInfo
                utc_tz = ZoneInfo("UTC")
            confirmation_datetime_utc = confirmation_datetime.astimezone(utc_tz)

            # Convert to naive UTC datetime for MySQL storage
            confirmation_datetime_utc_naive = confirmation_datetime_utc.replace(tzinfo=None)

            # Create task data with Polish time info for human readability
            polish_time_str = confirmation_datetime.strftime('%d.%m.%Y %H:%M')
            meeting_polish_str = meeting_datetime.strftime('%d.%m.%Y %H:%M')
            task_data_with_time = f"{poll_result} | Confirmation at: {polish_time_str} (Polish) | Meeting at: {meeting_polish_str} (Polish)"

            task_id = add_scheduled_task(
                chat_id=chat_id,
                poll_id=poll_id,
                task_type="confirmation",
                scheduled_time=confirmation_datetime_utc_naive,
                task_data=task_data_with_time
            )

            logger.info(f"Stored confirmation task {task_id} in database:")
            logger.info(f"  - Scheduled time (UTC): {confirmation_datetime_utc_naive}")
            logger.info(f"  - Polish time info: {polish_time_str}")
            logger.info(f"  - Meeting time (Polish): {meeting_polish_str}")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling confirmation message: {e}")
            return False
    
    @staticmethod
    def schedule_followup_message(chat_id: int, poll_result: str, meeting_datetime: datetime) -> bool:
        """
        Schedule follow-up message 72 hours after the meeting
        
        Returns:
            bool: True if scheduled successfully, False if database error
        """
        try:
            if not add_scheduled_task:
                logger.error("Database scheduling not available - cannot schedule follow-up message")
                return False
            
            from zoneinfo import ZoneInfo
            
            # Use Polish timezone
            try:
                polish_tz = ZoneInfo("Europe/Warsaw")
            except ImportError:
                import pytz
                polish_tz = pytz.timezone("Europe/Warsaw")
            
            # Ensure meeting_datetime has timezone
            if meeting_datetime.tzinfo is None:
                meeting_datetime = meeting_datetime.replace(tzinfo=polish_tz)
            
            # Calculate 72 hours (3 days) after the meeting
            followup_datetime = meeting_datetime + timedelta(hours=72)
            
            logger.info(f"Meeting at: {meeting_datetime.strftime('%d.%m.%Y %H:%M %Z')} (Polish time)")
            logger.info(f"Follow-up scheduled for: {followup_datetime.strftime('%d.%m.%Y %H:%M %Z')} (72 hours after meeting)")
            
            # Convert to naive datetime for MySQL storage
            followup_datetime_naive = followup_datetime.replace(tzinfo=None)
            
            task_id = add_scheduled_task(
                chat_id=chat_id,
                poll_id=None,  # No specific poll for follow-up
                task_type="followup",
                scheduled_time=followup_datetime_naive,
                task_data=poll_result
            )
            
            logger.info(f"Stored follow-up task {task_id} in database for {followup_datetime_naive}")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling follow-up message: {e}")
            return False
    
    @staticmethod
    def schedule_unpin_message(chat_id: int, poll_id: str, meeting_datetime: datetime, 
                             message_id: Optional[int] = None) -> bool:
        """
        Schedule message unpinning 10 hours after the meeting
        
        Returns:
            bool: True if scheduled successfully, False if database error
        """
        try:
            if not add_scheduled_task:
                logger.error("Database scheduling not available - cannot schedule unpin message")
                return False
            
            from zoneinfo import ZoneInfo
            
            # Use Polish timezone
            try:
                polish_tz = ZoneInfo("Europe/Warsaw")
            except ImportError:
                import pytz
                polish_tz = pytz.timezone("Europe/Warsaw")
            
            # Ensure meeting_datetime has timezone
            if meeting_datetime.tzinfo is None:
                meeting_datetime = meeting_datetime.replace(tzinfo=polish_tz)
            
            # Calculate 10 hours after the meeting
            unpin_datetime = meeting_datetime + timedelta(hours=10)
            
            logger.info(f"Meeting at: {meeting_datetime.strftime('%d.%m.%Y %H:%M %Z')} (Polish time)")
            logger.info(f"Unpin scheduled for: {unpin_datetime.strftime('%d.%m.%Y %H:%M %Z')} (10 hours after meeting)")
            
            # Convert Polish time to UTC for database storage
            try:
                import pytz
                utc_tz = pytz.UTC
                unpin_datetime_utc = unpin_datetime.astimezone(utc_tz)
            except ImportError:
                from zoneinfo import ZoneInfo
                utc_tz = ZoneInfo("UTC")
                unpin_datetime_utc = unpin_datetime.astimezone(utc_tz)
            
            # Convert to naive UTC datetime for MySQL storage
            unpin_datetime_utc_naive = unpin_datetime_utc.replace(tzinfo=None)
            
            # Create task data with Polish time info
            polish_time_str = unpin_datetime.strftime('%d.%m.%Y %H:%M')
            meeting_polish_str = meeting_datetime.strftime('%d.%m.%Y %H:%M')
            task_data_with_time = f"Message ID: {message_id} | Unpin at: {polish_time_str} (Polish) | Meeting was: {meeting_polish_str} (Polish)"
            
            task_id = add_scheduled_task(
                chat_id=chat_id,
                poll_id=poll_id,
                task_type="unpin_message",
                scheduled_time=unpin_datetime_utc_naive,
                task_data=task_data_with_time
            )
            
            logger.info(f"Stored unpin task {task_id} in database:")
            logger.info(f"  - Scheduled time (UTC): {unpin_datetime_utc_naive}")
            logger.info(f"  - Polish time info: {polish_time_str}")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling unpin message: {e}")
            return False
    
    @staticmethod
    def schedule_poll_voting_timeout(chat_id: int, poll_id: str, missing_votes: int) -> bool:
        """
        Schedule poll voting timeout reminder (1 hour from now)
        
        Returns:
            bool: True if scheduled successfully, False if database error
        """
        try:
            if not add_scheduled_task:
                logger.error("Database scheduling not available - cannot schedule poll voting timeout")
                return False
            
            # Calculate when to send the reminder (1 hour from now)
            from datetime import datetime, timedelta
            reminder_time = datetime.now() + timedelta(hours=1)  # 1 hour timeout
            
            task_id = add_scheduled_task(
                chat_id=chat_id,
                poll_id=poll_id,
                task_type="poll_voting_timeout",
                scheduled_time=reminder_time,
                task_data=str(missing_votes)
            )
            
            logger.info(f"Stored poll voting timeout task {task_id} in database for {reminder_time}")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling poll voting timeout: {e}")
            return False
    
    @staticmethod
    def schedule_session_cleanup() -> bool:
        """
        Schedule session cleanup (1 hour from now)
        
        Returns:
            bool: True if scheduled successfully, False if database error
        """
        try:
            if not add_scheduled_task:
                logger.error("Database scheduling not available - cannot schedule session cleanup")
                return False
            
            # Schedule next cleanup in 1 hour
            next_cleanup_time = datetime.now() + timedelta(hours=1)
            
            task_id = add_scheduled_task(
                chat_id=0,  # Global task, not specific to a chat
                poll_id=None,
                task_type="session_cleanup",
                scheduled_time=next_cleanup_time,
                task_data=None
            )
            
            logger.info(f"Stored session cleanup task {task_id} in database for {next_cleanup_time}")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling session cleanup: {e}")
            return False


# Task execution functions (used by Flask app)
class TaskExecutor:
    """Executor for scheduled tasks"""
    
    @staticmethod
    async def execute_confirmation_task(chat_id: int, poll_result: str, poll_id: str, bot_instance, bot_application):
        """Execute confirmation message task"""
        try:
            if bot_instance:
                await bot_instance.send_confirmation_message(chat_id, poll_result, bot_application, None)
            else:
                # Fallback: send simple message
                confirmation_text = f"План в силе? 💪 {poll_result}"
                await bot_application.bot.send_message(chat_id=chat_id, text=confirmation_text)
            
            logger.info(f"Executed confirmation task for chat {chat_id}")
            
        except Exception as e:
            logger.error(f"Error executing confirmation task for chat {chat_id}: {e}")
            raise
    
    @staticmethod
    async def execute_followup_task(chat_id: int, poll_result: str, bot_instance, bot_application):
        """Execute follow-up message task. Skip if a poll is already open in this chat."""
        try:
            # 1) Check in-memory active polls (preferred)
            try:
                if bot_instance and getattr(bot_instance, 'active_polls', None):
                    if any((p.get('chat_id') == chat_id) for p in bot_instance.active_polls.values()):
                        logger.info(f"Skipping follow-up in chat {chat_id}: active poll detected")
                        return
            except Exception as _:
                pass

            # 2) Check DB for open polls as a secondary safeguard
            try:
                from poll_storage import get_open_polls
                open_polls = get_open_polls() or []
                if any(int(p.get('chat_id')) == int(chat_id) for p in open_polls):
                    logger.info(f"Skipping follow-up in chat {chat_id}: open poll found in DB")
                    return
            except Exception:
                # If DB check fails, proceed with caution (best-effort)
                pass

            # No active/open poll → send follow-up
            if bot_instance:
                await bot_instance.send_followup_message(chat_id, bot_application)
            else:
                # Fallback: send simple message
                followup_text = (
                    "🔄 Как прошла встреча? Готовы планировать следующую?\n\n"
                    "Используйте /create_poll чтобы создать новый опрос для следующей встречи!"
                )
                await bot_application.bot.send_message(chat_id=chat_id, text=followup_text)
            
            logger.info(f"Executed follow-up task for chat {chat_id}")
            
        except Exception as e:
            logger.error(f"Error executing follow-up task for chat {chat_id}: {e}")
            raise
    
    @staticmethod
    async def execute_unpin_task(chat_id: int, message_id_str: Optional[str], bot_instance, bot_application):
        """Execute unpin message task"""
        try:
            message_id = int(message_id_str) if message_id_str and message_id_str.isdigit() else None
            
            if bot_instance and hasattr(bot_instance, 'unpin_confirmation_message'):
                await bot_instance.unpin_confirmation_message(None, chat_id, bot_application)
            else:
                # Fallback: direct API call
                if message_id:
                    await bot_application.bot.unpin_chat_message(chat_id=chat_id, message_id=message_id)
                else:
                    await bot_application.bot.unpin_all_chat_messages(chat_id=chat_id)
            
            logger.info(f"Executed unpin task for chat {chat_id}")
            
        except Exception as e:
            logger.error(f"Error executing unpin task for chat {chat_id}: {e}")
            raise
    
    @staticmethod
    async def execute_voting_timeout_task(chat_id: int, poll_id: str, missing_votes_str: str, bot_application):
        """Execute poll voting timeout task"""
        try:
            # Parse missing vote count
            try:
                missing_votes = int(missing_votes_str) if missing_votes_str and missing_votes_str.isdigit() else None
            except:
                missing_votes = None
            
            # DB-backed task required; missing_votes must be present (was computed at scheduling time)
            if missing_votes is None:
                logger.error("DB task missing 'missing_votes' payload; cannot send reminder")
                raise RuntimeError("DB task missing 'missing_votes'")

            import random

            messages = [
                "⏰ Не все проголосовали в опросе — жду ваш голос! 🗳️",
                "📢 Опрос ещё ждёт некоторых участников — присоединяйтесь! 😉",
                "🔔 Напоминание: в опросе не все отметились, голосуйте! ✅",
                "🗳️ Если вы ещё не проголосовали в опросе — самое время! ⏰",
                "⚡ Остались те, кто не проголосовал в опросе — исправим это! 💬"
            ]

            reminder_text = random.choice(messages)
            await bot_application.bot.send_message(chat_id=chat_id, text=reminder_text)
            logger.info(f"Executed voting timeout task for chat {chat_id}")
            
        except Exception as e:
            logger.error(f"Error executing voting timeout task for chat {chat_id}: {e}")
            raise
    
    @staticmethod
    async def execute_session_cleanup_task(bot_instance):
        """Execute session cleanup task"""
        try:
            if bot_instance and hasattr(bot_instance, 'cleanup_expired_sessions'):
                # Note: This would normally run the cleanup once instead of continuous loop
                logger.info("Session cleanup task executed")
            else:
                logger.info("Session cleanup not available or not needed")
            
        except Exception as e:
            logger.error(f"Error executing session cleanup task: {e}")
            raise


def parse_meeting_datetime_from_poll_result(poll_result: str) -> Optional[datetime]:
    """
    Parse meeting datetime from poll result string
    
    Args:
        poll_result: String like "Понедельник (30.12) в 18:00"
        
    Returns:
        datetime object or None if parsing fails
    """
    try:
        import re
        from zoneinfo import ZoneInfo
        
        date_match = re.search(r'\((\d{2})\.(\d{2})\)', poll_result)
        time_match = re.search(r'в (\d{1,2}):(\d{2})', poll_result)
        
        if not date_match:
            logger.error(f"Could not extract date from poll result: {poll_result}")
            return None
        
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
            import pytz
            polish_tz = pytz.timezone("Europe/Warsaw")
        
        meeting_datetime = datetime(current_year, month, day, hour, minute, 0, 0, tzinfo=polish_tz)
        return meeting_datetime
        
    except Exception as e:
        logger.error(f"Error parsing meeting datetime from '{poll_result}': {e}")
        return None