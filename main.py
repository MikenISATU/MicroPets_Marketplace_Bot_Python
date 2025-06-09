import os
import logging
import requests
import random
import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional, Dict, List, Set
from fastapi import FastAPI, Request, HTTPException
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, Application
from web3 import Web3
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from datetime import datetime, timedelta
from decimal import Decimal
import telegram
import aiohttp
import threading

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)
telegram_logger = logging.getLogger("telegram")
telegram_logger.setLevel(logging.WARNING)

# Load environment variables
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
BNB_RPC_URL = os.getenv('BNB_RPC_URL')
CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS')
BSCSCAN_API_KEY = os.getenv('BSCSCAN_API_KEY')
ADMIN_CHAT_ID = os.getenv('ADMIN_USER_ID')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
ALPHA_CHAT_ID = os.getenv('ALPHA_CHAT_ID')
MARKET_CHAT_ID = os.getenv('MARKET_CHAT_ID')
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', 60))
PORT = int(os.getenv('PORT', 8080))
BSC_URL = os.getenv('BSC_URL')
APP_URL = os.getenv('APP_URL')

# Validate environment variables
missing_vars = []
for var, name in [
    (TELEGRAM_BOT_TOKEN, 'TELEGRAM_BOT_TOKEN'),
    (BNB_RPC_URL, 'BNB_RPC_URL'),
    (CONTRACT_ADDRESS, 'CONTRACT_ADDRESS'),
    (BSCSCAN_API_KEY, 'BSCSCAN_API_KEY'),
    (ADMIN_CHAT_ID, 'ADMIN_USER_ID'),
    (TELEGRAM_CHAT_ID, 'TELEGRAM_CHAT_ID'),
    (ALPHA_CHAT_ID, 'ALPHA_CHAT_ID'),
    (MARKET_CHAT_ID, 'MARKET_CHAT_ID'),
    (BSC_URL, 'BSC_URL'),
    (APP_URL, 'APP_URL')
]:
    if not var:
        missing_vars.append(name)
if missing_vars:
    logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Validate Ethereum address
if not Web3.is_address(CONTRACT_ADDRESS):
    logger.error(f"Invalid Ethereum address for CONTRACT_ADDRESS: {CONTRACT_ADDRESS}")
    raise ValueError(f"Invalid Ethereum address for CONTRACT_ADDRESS: {CONTRACT_ADDRESS}")

logger.info(f"Environment loaded successfully. PORT={PORT}, APP_URL={APP_URL}")

# Initialize Web3
try:
    w3 = Web3(Web3.HTTPProvider(BNB_RPC_URL, request_kwargs={'timeout': 60}))
    if not w3.is_connected():
        raise Exception("Primary RPC URL connection failed")
    logger.info("Successfully initialized Web3 with BNB_RPC_URL")
except Exception as e:
    logger.error(f"Failed to initialize Web3 with primary URL: {e}")
    w3 = Web3(Web3.HTTPProvider('https://bsc-dataseed2.binance.org', request_kwargs={'timeout': 60}))
    if not w3.is_connected():
        logger.error("Fallback RPC URL connection failed")
        raise ValueError("Both primary and fallback Web3 connections failed")
    logger.info("Web3 initialized with fallback")

# Constants
CONTRACT_ADDRESS = Web3.to_checksum_address(CONTRACT_ADDRESS)
ADD_PUBLIC_LISTING_SELECTOR = "0xe7bab167"  # Selector for Add Public Listing
SETTLE_PUBLIC_LISTING_SELECTOR = "0x018bd58e"  # Selector for Settle Public Listing
posted_events: set = set()
last_block_number: Optional[int] = None
is_monitoring_enabled: bool = False
monitoring_task = None
recent_errors: List[Dict] = []

# Static Image URLs (replace with actual hosted URLs)
LISTING_IMAGE_URL = "https://example.com/new_listing_notification.jpg"  # Replace with actual URL
SOLD_IMAGE_URL = "https://example.com/3d_nft_sold.jpg"  # Replace with actual URL

# Links
MARKETPLACE_LINK = "https://pets.micropets.io/marketplace"
CHART_LINK = "https://www.dextools.io/app/en/bnb/pair-explorer/0x4bdece4e422fa015336234e4fc4d39ae6dd75b01?t=1749434278227"
MERCH_LINK = "https://micropets.store/"
BUY_PETS_LINK = "https://pancakeswap.finance/swap?outputCurrency=0x2466858ab5edAd0BB597FE9f008F568B00d25Fe3"

# Helper functions
def shorten_address(address: str) -> str:
    return f"{address[:6]}...{address[-4:]}" if address and Web3.is_address(address) else ''

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
def get_pets_price_from_geckoterminal(timestamp: int) -> Optional[float]:
    try:
        headers = {'Accept': 'application/json;version=20230302'}
        response = requests.get(
            f"https://api.geckoterminal.com/api/v2/simple/networks/bsc/token_price/{CONTRACT_ADDRESS}",
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        price_str = data.get('data', {}).get('attributes', {}).get('token_prices', {}).get(CONTRACT_ADDRESS.lower(), '0')
        if not price_str or not isinstance(price_str, (str, float, int)):
            raise ValueError("Invalid price data from Geckoterminal")
        price = float(price_str)
        if price <= 0:
            raise ValueError("Geckoterminal returned non-positive price")
        logger.info(f"$PETS price from Geckoterminal: ${price:.10f} at {datetime.fromtimestamp(timestamp)}")
        return price
    except Exception as e:
        logger.error(f"Geckoterminal $PETS price fetch failed: {e}")
        return None

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
def get_pets_price_from_bscscan(timestamp: int) -> Optional[float]:
    try:
        response = requests.get(
            f"https://api.bscscan.com/api?module=stats&action=tokenSupply&contractaddress={CONTRACT_ADDRESS}&apikey={BSCSCAN_API_KEY}",
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        if data.get('status') != '1':
            raise ValueError(f"BSCScan API error: {data.get('message', 'No message')}")
        supply = int(data.get('result', '0')) / 1e18
        tx_response = requests.get(
            f"https://api.bscscan.com/api?module=account&action=tokentx&contractaddress={CONTRACT_ADDRESS}&apikey={BSCSCAN_API_KEY}&sort=desc&limit=1",
            timeout=30
        )
        tx_response.raise_for_status()
        tx_data = tx_response.json()
        if tx_data.get('status') != '1' or not tx_data.get('result'):
            raise ValueError("No transaction data from BSCScan")
        value = int(tx_data['result'][0]['value']) / 1e18
        bnb_value = float(w3.from_wei(int(tx_data['result'][0]['value'], 16), 'ether')) if 'value' in tx_data['result'][0] else 0
        bnb_price = get_bnb_price_from_geckoterminal(timestamp) or 600
        price = (bnb_value * bnb_price) / value if value > 0 else 0
        logger.info(f"$PETS price from BSCScan: ${price:.10f} at {datetime.fromtimestamp(timestamp)}")
        return price if price > 0 else None
    except Exception as e:
        logger.error(f"BSCScan $PETS price fetch failed: {e}")
        return None

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
def get_bnb_price_from_geckoterminal(timestamp: int) -> Optional[float]:
    try:
        headers = {'Accept': 'application/json;version=20230302'}
        response = requests.get(
            f"https://api.geckoterminal.com/api/v2/simple/networks/bsc/token_price/0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        price_str = data.get('data', {}).get('attributes', {}).get('token_prices', {}).get('0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c', '0')
        if not price_str or not isinstance(price_str, (str, float, int)):
            raise ValueError("Invalid price data from Geckoterminal")
        price = float(price_str)
        if price <= 0:
            raise ValueError("Geckoterminal returned non-positive price")
        logger.info(f"BNB price from Geckoterminal: ${price:.2f} at {datetime.fromtimestamp(timestamp)}")
        return price
    except Exception as e:
        logger.error(f"Geckoterminal BNB price fetch failed: {e}")
        return None

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
async def fetch_logs(startblock: Optional[int] = None, endblock: Optional[int] = None) -> List[Dict]:
    global last_block_number
    try:
        if not startblock and last_block_number:
            startblock = last_block_number + 1
        latest_block = w3.eth.block_number
        if not startblock:
            startblock = max(0, latest_block - 1)
        if not endblock:
            endblock = latest_block
        logger.info(f"Fetching logs from block {startblock} to {endblock}")
        logs = w3.eth.get_logs({
            "fromBlock": startblock,
            "toBlock": endblock,
            "address": CONTRACT_ADDRESS
        })
        events = [
            {
                'transactionHash': log['transactionHash'].hex(),
                'blockNumber': log['blockNumber'],
                'topics': [topic.hex() for topic in log['topics']],
                'timeStamp': w3.eth.get_block(log['blockNumber'])['timestamp'],
                'value': log.get('data', '0x0')  # Assuming value is in log data for price calculation
            }
            for log in logs
            if log['topics'] and log['topics'][0].hex() in [ADD_PUBLIC_LISTING_SELECTOR, SETTLE_PUBLIC_LISTING_SELECTOR]
        ]
        if events:
            last_block_number = max(event['blockNumber'] for event in events)
        logger.info(f"Fetched {len(events)} events, last_block_number={last_block_number}")
        return events
    except Exception as e:
        logger.error(f"Failed to fetch logs: {e}")
        return []

async def set_webhook(application: Application) -> bool:
    try:
        webhook_url = f"{APP_URL}/webhook"
        await application.bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook set successfully at {webhook_url}")
        return True
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")
        return False

async def send_message_with_retry(bot: Bot, chat_id: str, message: str, image_url: str, parse_mode: str = 'Markdown', max_retries: int = 3, delay: int = 2) -> bool:
    for i in range(max_retries):
        try:
            logger.info(f"Attempt {i+1}/{max_retries} to send message to chat {chat_id}")
            await bot.send_photo(chat_id=chat_id, photo=image_url, caption=message, parse_mode=parse_mode)
            logger.info(f"Successfully sent message to chat {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message (attempt {i+1}/{max_retries}): {e}")
            if i == max_retries - 1:
                logger.error(f"Failed to send message to {chat_id} after {max_retries} attempts")
                return False
            await asyncio.sleep(delay)
    return False

async def process_event(context, event: Dict) -> bool:
    global posted_events
    try:
        tx_hash = event['transactionHash']
        if tx_hash in posted_events:
            logger.info(f"Skipping already posted event: {tx_hash}")
            return False
        timestamp = event['timeStamp']
        pets_price = get_pets_price_from_geckoterminal(timestamp) or get_pets_price_from_bscscan(timestamp) or 0.00003886
        bnb_price = get_bnb_price_from_geckoterminal(timestamp) or 600
        value_wei = int(event['value'], 16) if event['value'].startswith('0x') else 0
        pets_amount = value_wei / 1e18
        usd_value = pets_amount * pets_price
        bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
        method = "Add Public Listing" if event['topics'][0] == ADD_PUBLIC_LISTING_SELECTOR else "Settle Public Listing"
        block_number = event['blockNumber']
        tx_url = f"https://bscscan.com/tx/{tx_hash}"
        if method == "Add Public Listing":
            message = (
                f"🔥 *New Listing Notification* 🔥\n\n"
                f"1 x {method.split()[-1]} Evolved 3D NFT\n"
                f"Listed for: {pets_amount:,.0f} $PETS\n\n"
                f"🚀 *Join our Ascension Alpha Group* to get this notification 60 seconds earlier and with price revealed!\n\n"
                f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
            )
            image_url = LISTING_IMAGE_URL
        else:  # Settle Public Listing
            message = (
                f"🌸 *3D NFT Sold!* 1 x {method.split()[-1]} Evolved 3D NFT\n\n"
                f"💰 Sold for: {pets_amount:,.0f} $PETS\n"
                f"💳 Worth: {bnb_value:.3f} BNB (${usd_value:.2f})\n\n"
                f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
            )
            image_url = SOLD_IMAGE_URL
        # Send to Alpha Chat ID
        success = await send_message_with_retry(context.bot, ALPHA_CHAT_ID, message, image_url)
        if not success:
            logger.error(f"Failed to send {method} event to Alpha Chat ID")
            return False
        # Wait 1 minute and send to Market Chat ID
        await asyncio.sleep(60)
        success = await send_message_with_retry(context.bot, MARKET_CHAT_ID, message, image_url)
        if not success:
            logger.error(f"Failed to send {method} event to Market Chat ID")
            return False
        # Send to main Telegram Chat ID
        success = await send_message_with_retry(context.bot, TELEGRAM_CHAT_ID, message, image_url)
        if success:
            posted_events.add(tx_hash)
            logger.info(f"Processed event {tx_hash} for all chat IDs")
            return True
        logger.error(f"Failed to send {method} event to main Telegram Chat ID")
        return False
    except Exception as e:
        logger.error(f"Error processing event {event.get('transactionHash', 'unknown')}: {e}")
        return False

async def monitor_events(context) -> None:
    global last_block_number, is_monitoring_enabled, monitoring_task
    logger.info("Starting event monitoring")
    while is_monitoring_enabled:
        try:
            events = await fetch_logs(startblock=last_block_number + 1 if last_block_number else None)
            if not events:
                logger.info("No new events found")
                await asyncio.sleep(POLLING_INTERVAL)
                continue
            for event in sorted(events, key=lambda x: x['blockNumber'], reverse=True):
                if event['transactionHash'] in posted_events:
                    logger.info(f"Skipping already posted event: {event['transactionHash']}")
                    continue
                if await process_event(context, event):
                    last_block_number = max(last_block_number or 0, event['blockNumber'])
            await asyncio.sleep(POLLING_INTERVAL)
        except Exception as e:
            logger.error(f"Error monitoring events: {e}")
            recent_errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
            if len(recent_errors) > 5:
                recent_errors.pop(0)
            await asyncio.sleep(POLLING_INTERVAL)
    logger.info("Monitoring task stopped")
    monitoring_task = None

def is_admin(update: Update) -> bool:
    return str(update.effective_chat.id) == ADMIN_CHAT_ID

# Command handlers
async def start(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    await context.bot.send_message(chat_id=chat_id, text="👋 Welcome to NFT Marketplace Tracker! Use /track to start event alerts.")

async def track(update: Update, context) -> None:
    global is_monitoring_enabled, monitoring_task
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    if is_monitoring_enabled and monitoring_task:
        await context.bot.send_message(chat_id=chat_id, text="🚀 Tracking is already enabled. Notifications include images.")
    else:
        is_monitoring_enabled = True
        monitoring_task = asyncio.create_task(monitor_events(context))
        await context.bot.send_message(chat_id=chat_id, text="🚖 Tracking started. Notifications will include images.")

async def stop(update: Update, context) -> None:
    global is_monitoring_enabled, monitoring_task
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    is_monitoring_enabled = False
    if monitoring_task:
        monitoring_task.cancel()
        try:
            await monitoring_task
        except asyncio.CancelledError:
            logger.info("Monitoring task cancelled")
        monitoring_task = None
    await context.bot.send_message(chat_id=chat_id, text="🛑 Stopped")

async def stats(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Fetching $PETS data for the last 2 weeks")
    try:
        latest_block = w3.eth.block_number
        blocks_per_day = 24 * 60 * 60 // 3
        start_block = max(0, latest_block - (14 * blocks_per_day))
        events = await fetch_logs(startblock=start_block, endblock=latest_block)
        if not events:
            await context.bot.send_message(chat_id=chat_id, text="🚫 No events found in the last 2 weeks")
            return
        two_weeks_ago = int((datetime.now() - timedelta(days=14)).timestamp())
        recent_events = [e for e in events if e.get('timeStamp', 0) >= two_weeks_ago]
        if not recent_events:
            await context.bot.send_message(chat_id=chat_id, text="🚫 No events in the last 2 weeks")
            return
        add_count = sum(1 for e in recent_events if e['topics'][0] == ADD_PUBLIC_LISTING_SELECTOR)
        settle_count = sum(1 for e in recent_events if e['topics'][0] == SETTLE_PUBLIC_LISTING_SELECTOR)
        message = (
            f"📊 *NFT Marketplace Stats (Last 2 Weeks)*\n\n"
            f"🔥 New Listings: {add_count}\n"
            f"🌸 Sales: {settle_count}\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
        )
        await context.bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error in /stats: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Failed to fetch stats: {str(e)}")

async def status(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 *Status:* {'Enabled' if is_monitoring_enabled else 'Disabled'}",
        parse_mode='Markdown'
    )

async def debug(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    status = {
        'monitoringEnabled': is_monitoring_enabled,
        'lastBlockNumber': last_block_number,
        'recentErrors': recent_errors[-5:],
        'apiStatus': {
            'bscWeb3': bool(w3.is_connected())
        }
    }
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 Debug:\n```json\n{json.dumps(status, indent=2)}\n```",
        parse_mode='Markdown'
    )

# FastAPI routes
app = FastAPI()

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint called")
    try:
        if not w3.is_connected():
            raise Exception("Web3 is not connected")
        return {"status": "Connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")

@app.post("/webhook")
async def webhook(request: Request):
    logger.info("Webhook received update")
    update_data = await request.json()
    update = Update.de_json(update_data, bot_app.bot)
    # Process update if needed (currently using polling, so this is a fallback)
    return {"status": "ok"}

# Lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitoring_task
    logger.info("Starting bot application")
    try:
        application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        await application.initialize()
        await application.bot.set_my_commands([
            ('start', 'Start the bot'),
            ('track', 'Start tracking events'),
            ('stop', 'Stop tracking events'),
            ('stats', 'Show marketplace stats'),
            ('status', 'Check bot status'),
            ('debug', 'Show debug info')
        ])
        # Attempt to set webhook, fallback to polling if fails
        if not await set_webhook(application):
            logger.info("Falling back to polling")
            await application.updater.start_polling(
                poll_interval=3,
                timeout=10,
                drop_pending_updates=True,
                error_callback=lambda e: logger.error(f"Polling error: {e}")
            )
        else:
            logger.info("Webhook mode active")
        await application.start()
        monitoring_task = asyncio.create_task(monitor_events(application))
        yield
    except Exception as e:
        logger.error(f"Startup error: {e}")
    finally:
        logger.info("Initiating bot shutdown...")
        try:
            if monitoring_task:
                monitoring_task.cancel()
                try:
                    await monitoring_task
                except asyncio.CancelledError:
                    logger.info("Monitoring task cancelled")
                monitoring_task = None
            if application.running:
                await application.updater.stop() if not await set_webhook(application) else await application.stop()
                await application.shutdown()
            logger.info("Bot shutdown completed")
        except Exception as e:
            logger.error(f"Shutdown error: {str(e)}")

app = FastAPI(lifespan=lifespan)

# Bot initialization
bot_app = ApplicationBuilder() \
    .token(TELEGRAM_BOT_TOKEN) \
    .build()
bot_app.add_handler(CommandHandler("start", start))
bot_app.add_handler(CommandHandler("track", track))
bot_app.add_handler(CommandHandler("stop", stop))
bot_app.add_handler(CommandHandler("stats", stats))
bot_app.add_handler(CommandHandler("status", status))
bot_app.add_handler(CommandHandler("debug", debug))

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Uvicorn server on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
