#!/usr/bin/env python3
"""
Subscribe Handler for Telegram Bot
Handles /subscribe command with funny responses
"""

import random
import logging
from telegram import Update
from telegram.ext import ContextTypes

# Import database functions
try:
    from subscriber_storage import (
        add_subscriber, remove_subscriber, is_subscribed, 
        get_all_subscribers, get_subscriber_count, get_subscriber_ids,
        deactivate_subscriber, test_connection
    )
    DATABASE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Database not available, using in-memory storage: {e}")
    DATABASE_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)

class SubscribeHandler:
    """Handler for subscription functionality with humor"""
    
    def __init__(self):
        # Fallback to in-memory storage if database not available
        self.subscribers = set() if not DATABASE_AVAILABLE else None
        self.funny_messages = [
            "Успешно! Ты подписался на самое важное... что бы это ни было.",
            "Подписка оформлена. Уровень крутости +1. Почти как у холодильника с Wi-Fi.",
            "Ты подписан. Теперь ты получаешь всё. Ну, почти всё. Ладно, только то, что я захочу отправить.",
            "Успех! Теперь тебе будут приходить сообщения. Иногда. Когда бот вспомнит о тебе.",
            "Поздравляю! Ты теперь в элитном клубе подписчиков. Членский билет в разработке.",
            "Подписка активирована! Теперь ты будешь получать эксклюзивный контент. Или не эксклюзивный. Посмотрим.",
            "Готово! Добро пожаловать в секретное общество... которое не очень секретное.",
            "Ты подписался! Теперь ты часть чего-то большего. Или меньшего. Размер не важен.",
            "Успешно подписан! Ожидай сообщения с космической скоростью... или черепашьей.",
            "Подписка оформлена! Твой статус изменился с 'обычный человек' на 'подписчик бота'.",
            "Отлично! Теперь ты в моём списке VIP. Very Important... Person? Penguin? Potato?",
            "Ты подписан! Бот теперь знает о твоём существовании. Это хорошо или плохо - время покажет.",
            "Подписка активна! Приготовься к потоку... чего-то. Определённо чего-то.",
            "Успех! Ты теперь официально подписчик. Сертификат отправлен почтой голубей.",
            "Готово! Добро пожаловать в будущее... или в прошлое. Я не очень хорошо разбираюсь во времени."
        ]
        
        self.already_subscribed_messages = [
            "Ты уже подписан! Дважды подписываться нельзя - это против правил физики.",
            "Эй, ты уже в клубе! Повторная подписка может вызвать разрыв пространства-времени.",
            "Ты уже подписчик! Больше подписываться некуда. Это максимум крутости.",
            "Стоп! Ты уже подписан. Ещё одна подписка и бот может перегрузиться.",
            "Ты уже в списке! Повторная подписка запрещена международными конвенциями ботов.",
            "Уже подписан! Попытка двойной подписки обнаружена и заблокирована системой безопасности.",
            "Ты уже член нашего элитного клуба! Членство не передаётся и не дублируется.",
            "Стоп-стоп! Ты уже подписчик. Больше подписок - больше хаоса во вселенной."
        ]
    
    async def subscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribe command"""
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
            username = update.effective_user.username
            first_name = update.effective_user.first_name
            last_name = update.effective_user.last_name
            display_name = username or first_name or "Аноним"
            
            logger.info(f"Subscribe command from user {user_id} (@{display_name}) in chat {chat_id}")
            
            if DATABASE_AVAILABLE:
                # Check if user is already subscribed using database
                if is_subscribed(user_id):
                    message = random.choice(self.already_subscribed_messages)
                    logger.info(f"User {user_id} already subscribed")
                else:
                    # Add user to database (only user_id)
                    success = add_subscriber(user_id)
                    if success:
                        message = random.choice(self.funny_messages)
                        total_count = get_subscriber_count()
                        logger.info(f"User {user_id} subscribed successfully. Total subscribers: {total_count}")
                    else:
                        message = random.choice(self.already_subscribed_messages)
                        logger.info(f"User {user_id} already subscribed (database check)")
            else:
                # Fallback to in-memory storage
                if user_id in self.subscribers:
                    message = random.choice(self.already_subscribed_messages)
                    logger.info(f"User {user_id} already subscribed")
                else:
                    self.subscribers.add(user_id)
                    message = random.choice(self.funny_messages)
                    logger.info(f"User {user_id} subscribed successfully. Total subscribers: {len(self.subscribers)}")
            
            # Send funny response
            await update.message.reply_text(message)
            
        except Exception as e:
            logger.error(f"Error in subscribe command: {e}")
            await update.message.reply_text(
                "Что-то пошло не так с подпиской... Возможно, боты тоже иногда ошибаются. 🤖"
            )
    
    async def unsubscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /unsubscribe command"""
        try:
            user_id = update.effective_user.id
            username = update.effective_user.username or update.effective_user.first_name or "Аноним"
            
            logger.info(f"Unsubscribe command from user {user_id} (@{username})")
            
            unsubscribe_messages = [
                "Отписка оформлена. Ты покидаешь наш элитный клуб... Грустно. 😢",
                "Ты отписался. Уровень крутости -1. Теперь ты как обычный холодильник без Wi-Fi.",
                "Отписка успешна. Больше никаких сообщений... Пока ты не передумаешь.",
                "Готово! Ты свободен от наших сообщений. Лети, как птица! 🕊️",
                "Отписка завершена. Мы будем скучать... Или нет. Посмотрим.",
                "Ты покинул клуб. Членский билет аннулирован. Возврат денег не предусмотрен.",
                "Отписался? Ладно. Но знай - дверь всегда открыта для возвращения!",
                "Отписка оформлена. Теперь ты снова обычный человек. Скучно, но безопасно."
            ]
            
            not_subscribed_messages = [
                "Ты не подписан! Нельзя отписаться от того, на что не подписан. Логика!",
                "Стоп! Ты не в списке подписчиков. Отписываться не от чего.",
                "Ошибка! Ты не подписан. Сначала подпишись, потом отписывайся.",
                "Нет подписки - нет отписки. Всё честно!"
            ]
            
            if DATABASE_AVAILABLE:
                # Use database to check and remove subscription
                if is_subscribed(user_id):
                    success = deactivate_subscriber(user_id)
                    if success:
                        message = random.choice(unsubscribe_messages)
                        remaining_count = get_subscriber_count()
                        logger.info(f"User {user_id} unsubscribed. Remaining subscribers: {remaining_count}")
                    else:
                        message = "Произошла ошибка при отписке. Попробуйте позже."
                        logger.error(f"Failed to unsubscribe user {user_id}")
                else:
                    message = random.choice(not_subscribed_messages)
                    logger.info(f"User {user_id} tried to unsubscribe but wasn't subscribed")
            else:
                # Fallback to in-memory storage
                if user_id in self.subscribers:
                    self.subscribers.remove(user_id)
                    message = random.choice(unsubscribe_messages)
                    logger.info(f"User {user_id} unsubscribed. Remaining subscribers: {len(self.subscribers)}")
                else:
                    message = random.choice(not_subscribed_messages)
                    logger.info(f"User {user_id} tried to unsubscribe but wasn't subscribed")
            
            await update.message.reply_text(message)
            
        except Exception as e:
            logger.error(f"Error in unsubscribe command: {e}")
            await update.message.reply_text(
                "Что-то пошло не так с отпиской... Может, ты застрял в подписке навсегда? 🤔"
            )
    
    async def subscribers_count_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /subscribers command - show subscriber count"""
        try:
            if DATABASE_AVAILABLE:
                count = get_subscriber_count()
            else:
                count = len(self.subscribers)
            
            if count == 0:
                message = "Подписчиков пока нет. Одиноко, как в космосе... 🚀"
            elif count == 1:
                message = "У нас 1 подписчик! Это начало великой империи! 👑"
            elif count < 10:
                message = f"Подписчиков: {count}. Маленькая, но дружная семья! 👨‍👩‍👧‍👦"
            elif count < 100:
                message = f"Подписчиков: {count}. Уже можно открывать филиал! 🏢"
            else:
                message = f"Подписчиков: {count}. Это уже армия! Готовимся к захвату мира! 🌍"
            
            await update.message.reply_text(message)
            logger.info(f"Subscriber count requested: {count}")
            
        except Exception as e:
            logger.error(f"Error in subscribers count command: {e}")
            await update.message.reply_text(
                "Не могу посчитать подписчиков... Калькулятор сломался! 🧮"
            )
    
    def get_subscribers(self):
        """Get list of all subscribers"""
        if DATABASE_AVAILABLE:
            return get_subscriber_ids()
        else:
            return list(self.subscribers)
    
    def is_subscribed_local(self, user_id: int) -> bool:
        """Check if user is subscribed"""
        if DATABASE_AVAILABLE:
            return is_subscribed(user_id)
        else:
            return user_id in self.subscribers
    
    def get_subscriber_count_local(self) -> int:
        """Get total number of subscribers"""
        if DATABASE_AVAILABLE:
            return get_subscriber_count()
        else:
            return len(self.subscribers)
    
    async def broadcast_message(self, context: ContextTypes.DEFAULT_TYPE, message: str):
        """Send message to all subscribers"""
        if DATABASE_AVAILABLE:
            subscriber_ids = get_subscriber_ids()
        else:
            subscriber_ids = list(self.subscribers)
            
        if not subscriber_ids:
            logger.info("No subscribers to broadcast to")
            return 0
        
        sent_count = 0
        failed_count = 0
        
        for user_id in subscriber_ids:
            try:
                await context.bot.send_message(chat_id=user_id, text=message)
                sent_count += 1
                logger.debug(f"Broadcast sent to user {user_id}")
                
                # No need to track message timestamps in simplified version
                        
            except Exception as e:
                logger.warning(f"Failed to send broadcast to user {user_id}: {e}")
                failed_count += 1
                
                # Remove user if they blocked the bot
                if "bot was blocked" in str(e).lower() or "user is deactivated" in str(e).lower():
                    if DATABASE_AVAILABLE:
                        try:
                            deactivate_subscriber(user_id)
                            logger.info(f"Deactivated user {user_id} from subscribers (blocked bot)")
                        except Exception as db_e:
                            logger.warning(f"Failed to deactivate user {user_id}: {db_e}")
                    else:
                        self.subscribers.discard(user_id)
                        logger.info(f"Removed user {user_id} from subscribers (blocked bot)")
        
        logger.info(f"Broadcast completed: {sent_count} sent, {failed_count} failed")
        return sent_count

# Global instance
subscribe_handler = SubscribeHandler()

# Export functions for easy import
async def handle_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Wrapper function for subscribe command"""
    await subscribe_handler.subscribe_command(update, context)

async def handle_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Wrapper function for unsubscribe command"""
    await subscribe_handler.unsubscribe_command(update, context)

async def handle_subscribers_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Wrapper function for subscribers count command"""
    await subscribe_handler.subscribers_count_command(update, context)

# Example usage functions
async def send_broadcast(context: ContextTypes.DEFAULT_TYPE, message: str):
    """Send broadcast message to all subscribers"""
    return await subscribe_handler.broadcast_message(context, message)

def get_all_subscribers():
    """Get list of all subscriber IDs"""
    return subscribe_handler.get_subscribers()

def get_subscriber_count_wrapper():
    """Get total subscriber count"""
    return subscribe_handler.get_subscriber_count_local()

def is_user_subscribed(user_id: int):
    """Check if specific user is subscribed"""
    return subscribe_handler.is_subscribed_local(user_id)