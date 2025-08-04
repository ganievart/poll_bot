#!/usr/bin/env python3
"""
Flask App for Simple Poll Bot - PythonAnywhere Compatible with Authentication
"""

import os
import asyncio
import logging
from threading import Thread
from flask import Flask, request, jsonify, Response
import json
from functools import wraps

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Import the bot class
from simple_poll_bot import SimplePollBot

# Configure logging with file output
import sys
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime


def setup_logging():
    """Set up comprehensive logging with file rotation"""
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
    root_logger.setLevel(logging.INFO)

    # Clear existing handlers
    root_logger.handlers.clear()

    # File handler for all logs (with rotation)
    all_logs_file = os.path.join(log_dir, 'flask_app.log')
    file_handler = RotatingFileHandler(
        all_logs_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)

    # Error file handler (errors only)
    error_logs_file = os.path.join(log_dir, 'flask_errors.log')
    error_handler = RotatingFileHandler(
        error_logs_file,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=3,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(error_handler)

    # Console handler (for PythonAnywhere console)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    root_logger.addHandler(console_handler)

    # Log startup message
    root_logger.info("=" * 50)
    root_logger.info(f"Flask App logging initialized at {datetime.now()}")
    root_logger.info(f"Log directory: {os.path.abspath(log_dir)}")
    root_logger.info("=" * 50)

    return root_logger


# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)

# Authentication setup
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', 'admin')  # fallback to 'admin' if not set
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')  # No fallback - must be set in .env

if not ADMIN_PASSWORD:
    logger.error("❌ ADMIN_PASSWORD not found in environment variables!")
    logger.error("💡 Please add ADMIN_PASSWORD to your .env file")


def check_auth(username, password):
    """Check if username/password combination is valid"""
    return username == ADMIN_USERNAME and password == ADMIN_PASSWORD


def authenticate():
    """Send a 401 response that enables basic auth"""
    return Response(
        'Authentication required\n'
        'Please login with proper credentials to access the bot admin panel.', 401,
        {'WWW-Authenticate': 'Basic realm="Bot Admin Panel"'})


def requires_auth(f):
    """Decorator for routes that require authentication"""

    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            logger.warning(f"Unauthorized access attempt from IP: {request.remote_addr}")
            return authenticate()
        return f(*args, **kwargs)

    return decorated


# Global variables for bot
bot_instance = None
bot_application = None
webhook_url = None
_setup_done = False


def get_or_create_event_loop():
    """Get existing event loop or create a new one"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
        return loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


async def initialize_bot_async():
    """Initialize bot application asynchronously"""
    global bot_instance, bot_application

    token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not token:
        logger.error("❌ TELEGRAM_BOT_TOKEN not found in environment!")
        return False

    try:
        from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, \
            PollAnswerHandler, MessageReactionHandler

        # Create bot instance
        bot_instance = SimplePollBot(token)

        # Create application
        bot_application = Application.builder().token(token).build()

        # Initialize the application properly
        await bot_application.initialize()

        # Add handlers
        bot_application.add_handler(CommandHandler("start", bot_instance.start))
        bot_application.add_handler(CommandHandler("help", bot_instance.help_command))
        bot_application.add_handler(CommandHandler("info", bot_instance.info_command))
        bot_application.add_handler(CommandHandler("create_poll", bot_instance.create_poll))
        bot_application.add_handler(CommandHandler("cancel_bot", bot_instance.cancel_bot))
        bot_application.add_handler(CommandHandler("die", bot_instance.die_command))
        bot_application.add_handler(CallbackQueryHandler(bot_instance.button_handler))
        bot_application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot_instance.text_handler))
        bot_application.add_handler(PollAnswerHandler(bot_instance.poll_answer_handler))
        bot_application.add_handler(MessageReactionHandler(bot_instance.message_reaction_handler))

        # Start the cleanup task
        bot_instance.start_cleanup_task()

        logger.info("✅ Bot setup completed successfully")
        return True

    except Exception as e:
        logger.error(f"❌ Error setting up bot: {e}")
        return False


def setup_bot():
    """Setup the bot instance and application - wrapper for async function"""
    global _setup_done

    if _setup_done:
        return bot_application is not None

    try:
        loop = get_or_create_event_loop()
        success = loop.run_until_complete(initialize_bot_async())
        _setup_done = True
        return success
    except Exception as e:
        logger.error(f"❌ Error in setup_bot: {e}")
        _setup_done = True
        return False


def ensure_bot_setup():
    """Ensure bot is set up, set it up if not"""
    global bot_application
    if bot_application is None:
        setup_bot()
    return bot_application is not None


@app.route('/')
@requires_auth
def index():
    """Health check endpoint - now protected"""
    ensure_bot_setup()
    return jsonify({
        "status": "running",
        "message": "Simple Poll Bot Flask App is running",
        "bot_configured": bot_instance is not None,
        "authenticated_user": request.authorization.username if request.authorization else None
    })


@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming webhook updates from Telegram - MUST stay public"""
    if not ensure_bot_setup():
        logger.error("Bot application not configured")
        return jsonify({"error": "Bot not configured"}), 500

    try:
        # Get the update data
        update_data = request.get_json()

        if not update_data:
            logger.warning("No update data received")
            return jsonify({"error": "No data"}), 400

        logger.info(f"Received webhook update: {json.dumps(update_data, indent=2)}")

        # Process the update asynchronously
        loop = get_or_create_event_loop()
        loop.run_until_complete(process_update(update_data))

        return jsonify({"status": "ok"})

    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return jsonify({"error": str(e)}), 500


async def process_update(update_data):
    """Process Telegram update asynchronously"""
    try:
        from telegram import Update

        # Convert dict to Update object
        update = Update.de_json(update_data, bot_application.bot)

        if update:
            # Process the update
            await bot_application.process_update(update)
            logger.info("Update processed successfully")
        else:
            logger.warning("Could not create Update object from data")

    except Exception as e:
        logger.error(f"Error processing update: {e}")


@app.route('/set_webhook', methods=['POST'])
@requires_auth
def set_webhook():
    """Set webhook URL for the bot - protected"""
    global webhook_url

    if not ensure_bot_setup():
        return jsonify({"error": "Bot not configured"}), 500

    try:
        data = request.get_json()
        webhook_url = data.get('url')

        if not webhook_url:
            return jsonify({"error": "URL required"}), 400

        # Set webhook asynchronously
        loop = get_or_create_event_loop()
        loop.run_until_complete(set_webhook_async(webhook_url))

        logger.info(f"Webhook set by user: {request.authorization.username}")

        return jsonify({
            "status": "webhook_set",
            "url": webhook_url,
            "set_by": request.authorization.username
        })

    except Exception as e:
        logger.error(f"Error setting webhook: {e}")
        return jsonify({"error": str(e)}), 500


async def set_webhook_async(url):
    """Set webhook asynchronously"""
    try:
        await bot_application.bot.set_webhook(url=url)
        logger.info(f"Webhook set to: {url}")
    except Exception as e:
        logger.error(f"Error setting webhook: {e}")


@app.route('/get_webhook_info')
@requires_auth
def get_webhook_info():
    """Get current webhook information - protected"""
    if not ensure_bot_setup():
        return jsonify({"error": "Bot not configured"}), 500

    try:
        # Get webhook info asynchronously
        loop = get_or_create_event_loop()
        webhook_info = loop.run_until_complete(bot_application.bot.get_webhook_info())

        return jsonify({
            "url": webhook_info.url,
            "has_custom_certificate": webhook_info.has_custom_certificate,
            "pending_update_count": webhook_info.pending_update_count,
            "last_error_date": webhook_info.last_error_date.isoformat() if webhook_info.last_error_date else None,
            "last_error_message": webhook_info.last_error_message,
            "max_connections": webhook_info.max_connections,
            "allowed_updates": webhook_info.allowed_updates,
            "requested_by": request.authorization.username
        })

    except Exception as e:
        logger.error(f"Error getting webhook info: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/delete_webhook', methods=['POST'])
@requires_auth
def delete_webhook():
    """Delete webhook (switch to polling mode) - protected"""
    if not ensure_bot_setup():
        return jsonify({"error": "Bot not configured"}), 500

    try:
        # Delete webhook asynchronously
        loop = get_or_create_event_loop()
        loop.run_until_complete(delete_webhook_async())

        logger.info(f"Webhook deleted by user: {request.authorization.username}")

        return jsonify({
            "status": "webhook_deleted",
            "deleted_by": request.authorization.username
        })

    except Exception as e:
        logger.error(f"Error deleting webhook: {e}")
        return jsonify({"error": str(e)}), 500


async def delete_webhook_async():
    """Delete webhook asynchronously"""
    try:
        await bot_application.bot.delete_webhook()
        logger.info("Webhook deleted")
    except Exception as e:
        logger.error(f"Error deleting webhook: {e}")


@app.route('/bot_info')
@requires_auth
def bot_info():
    """Get bot information - protected"""
    if not ensure_bot_setup():
        return jsonify({"error": "Bot not configured"}), 500

    try:
        # Get bot info asynchronously
        loop = get_or_create_event_loop()
        bot_info_data = loop.run_until_complete(bot_application.bot.get_me())

        return jsonify({
            "id": bot_info_data.id,
            "username": bot_info_data.username,
            "first_name": bot_info_data.first_name,
            "is_bot": bot_info_data.is_bot,
            "can_join_groups": bot_info_data.can_join_groups,
            "can_read_all_group_messages": bot_info_data.can_read_all_group_messages,
            "supports_inline_queries": bot_info_data.supports_inline_queries,
            "requested_by": request.authorization.username
        })

    except Exception as e:
        logger.error(f"Error getting bot info: {e}")
        return jsonify({"error": str(e)}), 500


def run_polling_in_thread():
    """Run bot polling in a separate thread (for development)"""
    if bot_application:
        logger.info("Starting bot polling in background thread...")
        try:
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Note: cleanup task is already started in setup_bot(), no need to start again

            # Run polling
            bot_application.run_polling()
        except Exception as e:
            logger.error(f"Error in polling thread: {e}")
        finally:
            # Clean up the event loop
            try:
                loop = asyncio.get_event_loop()
                if not loop.is_closed():
                    loop.close()
            except:
                pass


@app.route('/start_polling', methods=['POST'])
@requires_auth
def start_polling():
    """Start polling mode (for development/testing) - protected"""
    if not ensure_bot_setup():
        return jsonify({"error": "Bot not configured"}), 500

    try:
        # Start polling in a separate thread
        polling_thread = Thread(target=run_polling_in_thread, daemon=True)
        polling_thread.start()

        logger.info(f"Polling started by user: {request.authorization.username}")

        return jsonify({
            "status": "polling_started",
            "started_by": request.authorization.username
        })

    except Exception as e:
        logger.error(f"Error starting polling: {e}")
        return jsonify({"error": str(e)}), 500


# Add a public status endpoint (no auth required) for basic health checks
@app.route('/status')
def status():
    """Basic status endpoint - public"""
    return jsonify({
        "status": "online",
        "service": "Simple Poll Bot",
        "auth_required": "yes - use /login for admin access"
    })


# Add a login info endpoint
@app.route('/login')
def login_info():
    """Login information - public"""
    return jsonify({
        "message": "This bot requires authentication for admin access",
        "instructions": "Use HTTP Basic Auth with your credentials",
        "admin_endpoints": [
            "/",
            "/bot_info",
            "/get_webhook_info",
            "/set_webhook",
            "/delete_webhook",
            "/start_polling"
        ],
        "public_endpoints": [
            "/status",
            "/login",
            "/webhook"
        ]
    })


# For PythonAnywhere, don't initialize immediately on import
# Initialize lazily when first endpoint is called

# For PythonAnywhere, the app will be imported, not run directly
if __name__ == "__main__":
    # Development mode - run Flask app only
    # Use /start_polling endpoint or webhook for bot functionality
    logger.info("🔧 Running in development mode...")
    logger.info("💡 Use /start_polling endpoint to enable polling or set up webhook")
    logger.info(f"🔐 Admin username: {ADMIN_USERNAME}")
    logger.info(f"🔐 Admin password: {'SET' if ADMIN_PASSWORD else 'NOT SET'}")

    # Initialize bot for development
    setup_bot()

    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)
