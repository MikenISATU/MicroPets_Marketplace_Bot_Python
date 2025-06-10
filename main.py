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
from telegram import Update, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, Application
from web3 import Web3
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from datetime import datetime, timedelta
import telegram
import aiohttp

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)
telegram_logger = logging.getLogger("telegram")
telegram_logger.setLevel(logging.WARNING)

# Verify python-telegram-bot version
logger.info(f"python-telegram-bot version: {telegram.__version__}")
if not telegram.__version__.startswith('20'):
    logger.error(f"Expected python-telegram-bot v20.0+, got {telegram.__version__}")
    raise SystemExit(1)

# Load environment variables
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
BNB_RPC_URL = os.getenv('BNB_RPC_URL')
CONTRACT_ADDRESS = Web3.to_checksum_address("0x2cCFDC83BbdD8bbd2979416620ADB2344B2Cb672")  # Marketplace CA
NFT_CONTRACT_ADDRESS = os.getenv('NFT_CONTRACT_ADDRESS', Web3.to_checksum_address("0x20ee7b720f4e4c4ffcb00c4065cdae55271aecca"))  # Placeholder NFT CA
BSCSCAN_API_KEY = os.getenv('BSCSCAN_API_KEY')
ADMIN_CHAT_ID = os.getenv('ADMIN_USER_ID')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
ALPHA_CHAT_ID = os.getenv('ALPHA_CHAT_ID')
MARKET_CHAT_ID = os.getenv('MARKET_CHAT_ID')
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', 60))
PORT = int(os.getenv('PORT', 8080))
APP_URL = os.getenv('RAILWAY_PUBLIC_DOMAIN', os.getenv('APP_URL'))

# Validate environment variables
missing_vars = []
for var, name in [
    (TELEGRAM_BOT_TOKEN, 'TELEGRAM_BOT_TOKEN'),
    (BNB_RPC_URL, 'BNB_RPC_URL'),
    (BSCSCAN_API_KEY, 'BSCSCAN_API_KEY'),
    (ADMIN_CHAT_ID, 'ADMIN_USER_ID'),
    (TELEGRAM_CHAT_ID, 'TELEGRAM_CHAT_ID'),
    (ALPHA_CHAT_ID, 'ALPHA_CHAT_ID'),
    (MARKET_CHAT_ID, 'MARKET_CHAT_ID'),
    (APP_URL, 'APP_URL')
]:
    if not var:
        missing_vars.append(name)
if missing_vars:
    logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Log environment variables (obscure sensitive ones)
logger.info(f"Environment loaded. APP_URL={APP_URL}, PORT={PORT}, CONTRACT_ADDRESS={CONTRACT_ADDRESS}, "
            f"NFT_CONTRACT_ADDRESS={NFT_CONTRACT_ADDRESS}, TELEGRAM_CHAT_ID={TELEGRAM_CHAT_ID}, "
            f"ALPHA_CHAT_ID={ALPHA_CHAT_ID}, MARKET_CHAT_ID={MARKET_CHAT_ID}, "
            f"ADMIN_CHAT_ID={ADMIN_CHAT_ID}, BSCSCAN_API_KEY={'***' if BSCSCAN_API_KEY else 'None'}, "
            f"TELEGRAM_BOT_TOKEN={'***' if TELEGRAM_BOT_TOKEN else 'None'}")

# Validate Ethereum addresses
if not Web3.is_address(CONTRACT_ADDRESS):
    logger.error(f"Invalid Ethereum address for CONTRACT_ADDRESS: {CONTRACT_ADDRESS}")
    raise ValueError(f"Invalid Ethereum address for CONTRACT_ADDRESS: {CONTRACT_ADDRESS}")
if not Web3.is_address(NFT_CONTRACT_ADDRESS):
    logger.error(f"Invalid Ethereum address for NFT_CONTRACT_ADDRESS: {NFT_CONTRACT_ADDRESS}")
    raise ValueError(f"Invalid Ethereum address for NFT_CONTRACT_ADDRESS: {NFT_CONTRACT_ADDRESS}")

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
ADD_PUBLIC_LISTING_V2_SELECTOR = "0xe7bab167"  # addPublicListingV2(uint256,uint256)
SETTLE_PUBLIC_LISTING_V2_SELECTOR = "0x018bd58e"  # settlePublicListingV2(uint256)
ADD_BULK_LISTING_V2_SELECTOR = "0x12345678"  # Placeholder for addBulkListingV2
posted_events: Set[str] = set()
last_block_number: Optional[int] = None
is_monitoring_enabled: bool = False
monitoring_task: Optional[asyncio.Task] = None
polling_task: Optional[asyncio.Task] = None
recent_errors: List[Dict] = []
active_chats: Set[str] = {TELEGRAM_CHAT_ID}

# Placeholder image URL
NFT_IMAGE_URL = "https://media.giphy.com/media/3o7bu3X8f7wY5zX9K0/giphy.gif"

# Links
MARKETPLACE_LINK = "https://pets.micropets.io/marketplace"
CHART_LINK = "https://www.dextools.io/app/en/bnb/pair-explorer/0x4bdece4e422fa015336234e4fc4d39ae6dd75b01?t=1749434278227"
MERCH_LINK = "https://micropets.store/"
BUY_PETS_LINK = "https://pancakeswap.finance/swap?outputCurrency=0x2466858ab5edAd0BB597FE9f008F568B00d25Fe3"

# Initialize last_block_number
async def initialize_last_block_number():
    global last_block_number
    try:
        last_block_number = w3.eth.block_number
        logger.info(f"Initialized last_block_number to {last_block_number}")
    except Exception as e:
        logger.error(f"Failed to initialize last_block_number: {e}")

# Helper functions
def shorten_address(address: str) -> str:
    return f"{address[:6]}...{address[-4:]}" if address and Web3.is_address(address) else ''

@retry(wait=wait_exponential(multiplier=2, min=4, max=60), stop=stop_after_attempt(5))
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

@retry(wait=wait_exponential(multiplier=2, min=4, max=60), stop=stop_after_attempt(5))
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

async def fetch_nft_metadata(token_id: str) -> Dict:
    try:
        nft_contract = w3.eth.contract(address=NFT_CONTRACT_ADDRESS, abi=[
            {"inputs": [{"internalType": "uint256", "name": "tokenId", "type": "uint256"}],
             "name": "tokenURI", "outputs": [{"internalType": "string", "name": "", "type": "string"}],
             "stateMutability": "view", "type": "function"}
        ])
        token_uri = nft_contract.functions.tokenURI(int(token_id, 16)).call()
        response = requests.get(token_uri, timeout=10).json()
        return {"name": response.get("name", f"Unnamed NFT #{token_id}"), "image": response.get("image", NFT_IMAGE_URL)}
    except Exception as e:
        logger.error(f"Failed to fetch NFT metadata for token {token_id}: {e}")
        return {"name": f"Unnamed NFT #{token_id}", "image": NFT_IMAGE_URL}

@retry(wait=wait_exponential(multiplier=2, min=4, max=60), stop=stop_after_attempt(5))
async def fetch_logs(startblock: Optional[int] = None, endblock: Optional[int] = None) -> List[Dict]:
    global last_block_number
    try:
        if not startblock and last_block_number:
            startblock = last_block_number + 1
        latest_block = w3.eth.block_number
        if not startblock:
            startblock = max(0, latest_block - 1000)
        if not endblock:
            endblock = latest_block
        logger.info(f"Fetching logs from block {startblock} to {endblock}")
        logs = []
        while startblock <= endblock:
            current_endblock = min(startblock + 999, endblock)  # Limit to 1000 blocks per request
            try:
                batch_logs = w3.eth.get_logs({
                    "fromBlock": startblock,
                    "toBlock": current_endblock,
                    "address": CONTRACT_ADDRESS
                })
                logs.extend(batch_logs)
            except Exception as e:
                logger.error(f"Partial fetch failed for range {startblock}-{current_endblock}: {e}")
                break
            startblock = current_endblock + 1
        events = [
            {
                'transactionHash': log['transactionHash'].hex(),
                'blockNumber': log['blockNumber'],
                'topics': [topic.hex() for topic in log['topics']],
                'timeStamp': w3.eth.get_block(log['blockNumber'])['timestamp'],
                'data': log.get('data', '0x0'),
                'tokenId': log['topics'][1].hex() if len(log['topics']) > 1 else '0'
            }
            for log in logs
            if log['topics'] and log['topics'][0].hex() in [ADD_PUBLIC_LISTING_V2_SELECTOR, SETTLE_PUBLIC_LISTING_V2_SELECTOR]
        ]
        if events:
            last_block_number = max(event['blockNumber'] for event in events)
        logger.info(f"Fetched {len(events)} events, last_block_number={last_block_number}")
        return events
    except Exception as e:
        logger.error(f"Failed to fetch logs: {e}")
        return []

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
async def set_webhook_with_retry(bot_app: Application) -> bool:
    webhook_url = f"https://{APP_URL}/webhook"
    logger.info(f"Attempting to set webhook: {webhook_url}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://{APP_URL}/health", timeout=10) as response:
                if response.status != 200:
                    raise Exception(f"Health check failed: {response.status}")
        await bot_app.bot.delete_webhook(drop_pending_updates=True)
        await bot_app.bot.set_webhook(webhook_url, allowed_updates=["message", "channel_post"])
        webhook_info = await bot_app.bot.get_webhook_info()
        if webhook_info.url != webhook_url:
            logger.error(f"Webhook URL mismatch: expected {webhook_url}, got {webhook_info.url}")
            return False
        logger.info(f"Webhook set successfully: {webhook_url}")
        return True
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")
        return False

async def polling_fallback(bot_app: Application) -> None:
    global polling_task
    logger.info("Starting polling fallback")
    while True:
        try:
            if not bot_app.running:
                await bot_app.initialize()
                await bot_app.start()
                await bot_app.updater.start_polling(
                    poll_interval=3,
                    timeout=10,
                    drop_pending_updates=True,
                    error_callback=lambda e: logger.error(f"Polling error: {e}")
                )
                logger.info("Polling started successfully")
                while True:
                    await asyncio.sleep(60)
            else:
                logger.warning("Bot already running")
                while polling_task and not polling_task.done():
                    await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            await asyncio.sleep(10)
        finally:
            if bot_app.running and polling_task:
                try:
                    await bot_app.updater.stop()
                    await bot_app.stop()
                    logger.info("Polling stopped")
                except Exception as e:
                    logger.error(f"Error stopping polling: {e}")

async def send_message_with_retry(bot, chat_id: str, message: str, image_url: str, parse_mode: str = 'Markdown', max_retries: int = 3, delay: int = 2) -> bool:
    for i in range(max_retries):
        try:
            logger.info(f"Attempt {i+1}/{max_retries} to send message to chat {chat_id}")
            async with aiohttp.ClientSession() as session:
                async with session.head(image_url, timeout=5) as head_response:
                    if head_response.status != 200:
                        raise Exception(f"Image URL inaccessible, status {head_response.status}")
            await bot.send_photo(chat_id=chat_id, photo=image_url, caption=message, parse_mode=parse_mode)
            logger.info(f"Successfully sent message to chat {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message (attempt {i+1}/{max_retries}): {e}")
            if i == max_retries - 1:
                await bot.send_message(chat_id, f"{message}\n\n⚠️ Image unavailable", parse_mode='Markdown')
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
        pets_price = get_pets_price_from_geckoterminal(timestamp) or 0.00003886
        bnb_price = get_bnb_price_from_geckoterminal(timestamp) or 600
        value_wei = int(event['data'], 16) if event['data'].startswith('0x') else 0
        pets_amount = value_wei / 1e18
        usd_value = pets_amount * pets_price
        bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
        method = "Add Public Listing V2" if event['topics'][0] == ADD_PUBLIC_LISTING_V2_SELECTOR else "Settle Public Listing V2"
        metadata = await fetch_nft_metadata(event['tokenId'])
        image_url = metadata['image']
        nft_name = metadata['name']
        tx_url = f"https://bscscan.com/tx/{tx_hash}"
        if method == "Add Public Listing V2":
            message = (
                f"🔥 *New Listing Notification* 🔥\n\n"
                f"1x {nft_name}\n"
                f"Listed for: {pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)\n"
                f"Get it on the Marketplace 🎁 now before it's gone! 👀\n\n"
                f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
            )
        else:  # Settle Public Listing V2
            message = (
                f"🌸 *3D NFT Sold!* 🌸\n\n"
                f"Sold: 1x {nft_name}\n"
                f"Sold for: {pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)\n"
                f"Worth: 0.13 BNB (${usd_value:.2f})\n\n"
                f"📦 [Marketplace]({MARKETPLACE_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
            )
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
        except asyncio.CancelledError:
            logger.info("Monitoring task cancelled")
            break
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
    logger.info(f"Received /start command from chat {chat_id}")
    active_chats.add(str(chat_id))
    await context.bot.send_message(chat_id=chat_id, text="👋 Welcome to NFT Marketplace Tracker! Use /track to start event alerts.")

async def track(update: Update, context) -> None:
    global is_monitoring_enabled, monitoring_task
    chat_id = update.effective_chat.id
    logger.info(f"Received /track command from chat {chat_id}")
    if not is_admin(update):
        logger.warning(f"Unauthorized /track attempt from chat {chat_id}")
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    if is_monitoring_enabled:
        await context.bot.send_message(chat_id=chat_id, text="🚀 Tracking already enabled")
        return
    is_monitoring_enabled = True
    active_chats.add(str(chat_id))
    monitoring_task = asyncio.create_task(monitor_events(context))
    logger.info("Event monitoring started via /track command")
    await context.bot.send_message(chat_id=chat_id, text="🚖 Tracking started. Notifications will include NFT images.")

async def stop(update: Update, context) -> None:
    global is_monitoring_enabled, monitoring_task
    chat_id = update.effective_chat.id
    logger.info(f"Received /stop command from chat {chat_id}")
    if not is_admin(update):
        logger.warning(f"Unauthorized /stop attempt from chat {chat_id}")
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
    active_chats.discard(str(chat_id))
    await context.bot.send_message(chat_id=chat_id, text="🛑 Stopped")

async def stats(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /stats command from chat {chat_id}")
    if not is_admin(update):
        logger.warning(f"Unauthorized /stats attempt from chat {chat_id}")
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Fetching latest marketplace stats")
    try:
        events = await fetch_logs()
        if not events:
            await context.bot.send_message(chat_id=chat_id, text="🚫 No events found")
            return
        latest_listing = next((e for e in reversed(events) if e['topics'][0] == ADD_PUBLIC_LISTING_V2_SELECTOR), None)
        latest_sale = next((e for e in reversed(events) if e['topics'][0] == SETTLE_PUBLIC_LISTING_V2_SELECTOR), None)
        message_parts = ["📊 *Latest Marketplace Stats*"]
        if latest_listing:
            metadata = await fetch_nft_metadata(latest_listing['tokenId'])
            pets_amount = int(latest_listing['data'], 16) / 1e18
            timestamp = latest_listing['timeStamp']
            pets_price = get_pets_price_from_geckoterminal(timestamp) or 0.00003886
            bnb_price = get_bnb_price_from_geckoterminal(timestamp) or 600
            usd_value = pets_amount * pets_price
            bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
            message_parts.append(f"\n🔥 *New Listing: {metadata['name']}*\nListed for: {pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)")
        if latest_sale:
            metadata = await fetch_nft_metadata(latest_sale['tokenId'])
            pets_amount = int(latest_sale['data'], 16) / 1e18
            timestamp = latest_sale['timeStamp']
            pets_price = get_pets_price_from_geckoterminal(timestamp) or 0.00003886
            bnb_price = get_bnb_price_from_geckoterminal(timestamp) or 600
            usd_value = pets_amount * pets_price
            bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
            message_parts.append(f"\n🌸 *Latest Sale: {metadata['name']}*\nSold for: {pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)")
        message = "\n".join(message_parts) + f"\n\n📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
        await send_message_with_retry(context.bot, chat_id, message, NFT_IMAGE_URL if not (latest_listing or latest_sale) else (await fetch_nft_metadata(latest_listing['tokenId'] if latest_listing else latest_sale['tokenId']))['image'])
    except Exception as e:
        logger.error(f"Error in /stats: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Failed to fetch stats: {str(e)}")

async def help_command(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /help command from chat {chat_id}")
    if not is_admin(update):
        logger.warning(f"Unauthorized /help attempt from chat {chat_id}")
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "🆘 Commands:\n\n"
            "/start - Start bot\n"
            "/track - Enable alerts\n"
            "/stop - Disable alerts\n"
            "/stats - Show latest marketplace stats\n"
            "/test - Test notifications\n"
            "/help - This help\n"
        ),
        parse_mode='Markdown'
    )

async def status(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /status command from chat {chat_id}")
    if not is_admin(update):
        logger.warning(f"Unauthorized /status attempt from chat {chat_id}")
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 *Status:* {'Enabled' if is_monitoring_enabled else 'Disabled'}",
        parse_mode='Markdown'
    )

async def debug(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /debug command from chat {chat_id}")
    if not is_admin(update):
        logger.warning(f"Unauthorized /debug attempt from chat {chat_id}")
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    status = {
        'monitoringEnabled': is_monitoring_enabled,
        'lastBlockNumber': last_block_number,
        'recentErrors': recent_errors[-5:],
        'apiStatus': {
            'bscWeb3': bool(w3.is_connected())
        },
        'activeChats': list(active_chats),
        'pollingActive': polling_task is not None and not polling_task.done()
    }
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 Debug:\n```json\n{json.dumps(status, indent=2)}\n```",
        parse_mode='Markdown'
    )

async def test(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /test command from chat {chat_id}")
    if not is_admin(update):
        logger.warning(f"Unauthorized /test attempt from chat {chat_id}")
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Generating test events (listing and sale)...")
    try:
        timestamp = int(time.time())
        pets_price = get_pets_price_from_geckoterminal(timestamp) or 0.00003886
        bnb_price = get_bnb_price_from_geckoterminal(timestamp) or 600
        image_url = NFT_IMAGE_URL  # Using placeholder for test
        nft_name = "Test NFT"

        # Test Listing
        test_pets_amount = random.randint(1000000, 5000000)
        usd_value = test_pets_amount * pets_price
        bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
        listing_message = (
            f"🔥 *New Listing Notification* 🔥\n\n"
            f"1x {nft_name}\n"
            f"Listed for: {test_pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)\n"
            f"Get it on the Marketplace 🎁 now before it's gone! 👀\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
        )
        success = await send_message_with_retry(context.bot, chat_id, listing_message, image_url)
        if not success:
            await context.bot.send_message(chat_id=chat_id, text="🚫 Listing test failed: Unable to send notification")
            return

        # Test Sale
        test_pets_amount = random.randint(1000000, 5000000)
        usd_value = test_pets_amount * pets_price
        bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
        sale_message = (
            f"🌸 *3D NFT Sold!* 🌸\n\n"
            f"Sold: 1x {nft_name}\n"
            f"Sold for: {test_pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)\n"
            f"Worth: 0.13 BNB (${usd_value:.2f})\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
        )
        success = await send_message_with_retry(context.bot, chat_id, sale_message, image_url)
        if success:
            await context.bot.send_message(chat_id=chat_id, text="✅ Both tests successful")
        else:
            await context.bot.send_message(chat_id=chat_id, text="🚫 Sale test failed: Unable to send notification")
    except Exception as e:
        logger.error(f"Test error: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Test failed: {str(e)}")

# FastAPI routes
app = FastAPI()

@app.get("/health")
async def health_check():
    logger.info("Health check endpoint called")
    try:
        if not w3.is_connected():
            logger.error("Web3 is not connected")
            raise Exception("Web3 connection failed")
        return {"status": "Connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")

@app.get("/webhook")
async def webhook_get():
    logger.info("Received GET webhook request")
    raise HTTPException(status_code=405, detail="Method Not Allowed")

@app.post("/webhook")
async def webhook(request: Request):
    logger.info("Webhook received update")
    try:
        data = await request.json()
        logger.debug(f"Webhook data: {json.dumps(data, indent=2)}")
        update = Update.de_json(data, bot_app.bot)
        if update:
            await bot_app.process_update(update)
            logger.info("Webhook update processed successfully")
            return {"status": "OK"}
        else:
            logger.error("Invalid update data received")
            raise HTTPException(status_code=400, detail="Invalid update data")
    except Exception as e:
        logger.error(f"Webhook processing failed: {e}")
        recent_errors.append({"time": datetime.now().isoformat(), "error": str(e)})
        if len(recent_errors) > 5:
            recent_errors.pop(0)
        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {e}")

# Lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitoring_task, polling_task, bot_app
    logger.info("Starting bot application")
    try:
        await initialize_last_block_number()
        bot_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        bot_app.add_handler(CommandHandler("start", start))
        bot_app.add_handler(CommandHandler("track", track))
        bot_app.add_handler(CommandHandler("stop", stop))
        bot_app.add_handler(CommandHandler("stats", stats))
        bot_app.add_handler(CommandHandler("help", help_command))
        bot_app.add_handler(CommandHandler("status", status))
        bot_app.add_handler(CommandHandler("debug", debug))
        bot_app.add_handler(CommandHandler("test", test))
        await bot_app.initialize()
        commands = [
            BotCommand(command="start", description="Start the bot"),
            BotCommand(command="track", description="Start tracking events"),
            BotCommand(command="stop", description="Stop tracking events"),
            BotCommand(command="stats", description="Show latest marketplace stats"),
            BotCommand(command="test", description="Test notifications"),
            BotCommand(command="help", description="Show commands"),
            BotCommand(command="status", description="Check bot status"),
            BotCommand(command="debug", description="Show debug info")
        ]
        logger.info(f"Setting bot commands: {[cmd.to_dict() for cmd in commands]}")
        try:
            await bot_app.bot.set_my_commands(commands)
            logger.info("Bot commands set successfully")
        except Exception as e:
            logger.error(f"Failed to set bot commands: {e}")
            logger.warning("Continuing without custom commands; use /start to interact")
        if not await set_webhook_with_retry(bot_app):
            logger.info("Webhook setup failed, starting polling")
            polling_task = asyncio.create_task(polling_fallback(bot_app))
        else:
            logger.info("Webhook mode active")
        yield
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise
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
            if polling_task:
                polling_task.cancel()
                try:
                    await polling_task
                except asyncio.CancelledError:
                    logger.info("Polling task cancelled")
                polling_task = None
            if bot_app and bot_app.running:
                await bot_app.stop()
                await bot_app.shutdown()
                try:
                    await bot_app.bot.delete_webhook(drop_pending_updates=True)
                    logger.info("Webhook deleted")
                except Exception as e:
                    logger.error(f"Failed to delete webhook: {e}")
            logger.info("Bot shutdown completed")
        except Exception as e:
            logger.error(f"Shutdown error: {str(e)}")

app = FastAPI(lifespan=lifespan)

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Uvicorn server on port {PORT}")
    try:
        uvicorn.run(app, host="0.0.0.0", port=PORT)
    except Exception as e:
        logger.error(f"Uvicorn error: {e}")
        raise
