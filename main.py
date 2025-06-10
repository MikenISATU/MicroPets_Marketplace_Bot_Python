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
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from web3 import Web3
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from datetime import datetime
import aiohttp
import threading
from bs4 import BeautifulSoup

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

# Load environment variables
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
BSCSCAN_API_KEY = os.getenv('BSCSCAN_API_KEY')
BNB_RPC_URL = os.getenv('BNB_RPC_URL')
CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS')
ADMIN_USER_ID = os.getenv('ADMIN_USER_ID')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
ALPHA_CHAT_ID = os.getenv('ALPHA_CHAT_ID')
MARKET_CHAT_ID = os.getenv('MARKET_CHAT_ID')
PORT = int(os.getenv('PORT', 8080))
COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY', '')
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', 60))
BSC_URL = os.getenv('BSC_URL', 'https://bscscan.com/token/0x2466858ab5edad0bb597fe9f008f568b00d25fe3')

# Validate environment variables
missing_vars = []
for var, name in [
    (TELEGRAM_BOT_TOKEN, 'TELEGRAM_BOT_TOKEN'),
    (BSCSCAN_API_KEY, 'BSCSCAN_API_KEY'),
    (BNB_RPC_URL, 'BNB_RPC_URL'),
    (CONTRACT_ADDRESS, 'CONTRACT_ADDRESS'),
    (ADMIN_USER_ID, 'ADMIN_USER_ID'),
    (TELEGRAM_CHAT_ID, 'TELEGRAM_CHAT_ID')
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

# Initialize active chats
active_chats: Set[str] = {TELEGRAM_CHAT_ID}
if ALPHA_CHAT_ID:
    active_chats.add(ALPHA_CHAT_ID)
if MARKET_CHAT_ID:
    active_chats.add(MARKET_CHAT_ID)

logger.info(f"Environment loaded successfully. PORT={PORT}, Active chats={active_chats}")

# Constants
BASE_URL = "https://element.market/collections/micropetsnewerabnb-5414f1c9?search[toggles][0]=ALL"
ADD_PUBLIC_LISTING_SELECTOR = "0xe7bab167"  # addPublicListing(uint256,uint256)
SETTLE_PUBLIC_LISTING_SELECTOR = "0x018bd58e"  # settlePublicListing(uint256)
NFT_IMAGE_URL = "https://media.giphy.com/media/3o7bu3X8f7wY5zX9K0/giphy.gif"
MARKETPLACE_LINK = "https://pets.micropets.io/marketplace"
CHART_LINK = "https://www.dextools.io/app/en/bnb/pair-explorer/0x4bdece4e422fa015336234e4fc4d39ae6dd75b01?t=1749434278227"
MERCH_LINK = "https://micropets.store/"
BUY_PETS_LINK = "https://pancakeswap.finance/swap?outputCurrency=0x2466858ab5edAd0BB597FE9f008F568B00d25Fe3"

# In-memory data
posted_events: Set[str] = set()
last_block_number: Optional[int] = None
is_monitoring_enabled: bool = False
monitoring_task: Optional[asyncio.Task] = None
recent_errors: List[Dict] = []
file_lock = threading.Lock()
bot_app = None

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

# Initialize last_block_number
async def initialize_last_block_number():
    global last_block_number
    try:
        last_block_number = w3.eth.block_number
        logger.info(f"Initialized last_block_number to {last_block_number}")
    except Exception as e:
        logger.error(f"Failed to initialize last_block_number: {e}")

# Helper functions
def get_gif_url(category: str) -> str:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(BASE_URL, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        gif_elements = soup.find_all('img', src=lambda x: x and x.endswith('.gif'))
        if gif_elements:
            return random.choice(gif_elements)['src']
        logger.warning("No GIFs found on the page, using fallback")
        return NFT_IMAGE_URL
    except Exception as e:
        logger.error(f"Failed to scrape GIF URL: {e}")
        return NFT_IMAGE_URL

def shorten_address(address: str) -> str:
    return f"{address[:6]}...{address[-4:]}" if address and Web3.is_address(address) else ''

def load_posted_transactions() -> Set[str]:
    try:
        with file_lock:
            if not os.path.exists('posted_transactions.txt'):
                return set()
            with open('posted_transactions.txt', 'r') as f:
                return set(line.strip() for line in f if line.strip())
    except Exception as e:
        logger.warning(f"Could not load posted_transactions.txt: {e}")
        return set()

def log_posted_transaction(transaction_hash: str) -> None:
    try:
        with file_lock:
            with open('posted_transactions.txt', 'a') as f:
                f.write(f"{transaction_hash}\n")
    except Exception as e:
        logger.error(f"Failed to write to posted_transactions.txt: {e}")

@retry(wait=wait_exponential(multiplier=2, min=4, max=20))
def get_pets_price(timestamp: int) -> float:
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
        price = float(price_str)
        if price <= 0:
            raise ValueError("Geckoterminal returned non-positive price")
        logger.info(f"$PETS price from GeckoTerminal: ${price:.10f} at {datetime.fromtimestamp(timestamp)}")
        return price
    except Exception as e:
        logger.error(f"GeckoTerminal $PETS price fetch failed: {e}")
        return 0.00003886

@retry(wait=wait_exponential(multiplier=2, min=4, max=20))
def get_bnb_price(timestamp: int) -> float:
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
        price = float(price_str)
        if price <= 0:
            raise ValueError("Geckoterminal returned non-positive price")
        logger.info(f"BNB price from Geckoterminal: ${price:.2f} at {datetime.fromtimestamp(timestamp)}")
        return price
    except Exception as e:
        logger.error(f"GeckoTerminal BNB price fetch failed: {e}")
        return 600.0

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
            "address": Web3.to_checksum_address(CONTRACT_ADDRESS)
        })
        events = [
            {
                'transactionHash': log['transactionHash'].hex(),
                'blockNumber': log['blockNumber'],
                'topics': [topic.hex() for topic in log['topics']],
                'timeStamp': w3.eth.get_block(log['blockNumber'])['timestamp'],
                'value': log.get('data', '0x0'),
                'tokenId': log['topics'][1].hex() if len(log['topics']) > 1 else '0'
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
        recent_errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
        if len(recent_errors) > 5:
            recent_errors.pop(0)
        return []

async def send_message_with_retry(context: ContextTypes.DEFAULT_TYPE, chat_id: str, message: str, image_url: str = None, parse_mode: str = 'Markdown', max_retries: int = 3, delay: int = 2) -> bool:
    for i in range(max_retries):
        try:
            logger.info(f"Attempt {i+1}/{max_retries} to send message to chat {chat_id}")
            if image_url:
                async with aiohttp.ClientSession() as session:
                    async with session.head(image_url, timeout=5) as head_response:
                        if head_response.status != 200:
                            logger.error(f"Image URL inaccessible, status {head_response.status}: {image_url}")
                            raise Exception(f"Image URL inaccessible, status {head_response.status}")
                await context.bot.send_photo(chat_id=chat_id, photo=image_url, caption=message, parse_mode=parse_mode)
            else:
                await context.bot.send_message(chat_id=chat_id, text=message, parse_mode=parse_mode)
            logger.info(f"Successfully sent message to chat {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message (attempt {i+1}/{max_retries}): {e}")
            if i == max_retries - 1:
                await context.bot.send_message(chat_id=chat_id, text=f"{message}\n\n⚠️ {'Image unavailable' if image_url else 'Message sending failed'}")
                return False
            await asyncio.sleep(delay)
    return False

async def process_event(context: ContextTypes.DEFAULT_TYPE, event: Dict, chat_id: str = TELEGRAM_CHAT_ID) -> bool:
    global posted_events
    try:
        tx_hash = event['transactionHash']
        if tx_hash in posted_events:
            logger.info(f"Skipping already posted event: {tx_hash}")
            return False
        timestamp = event['timeStamp']
        pets_price = get_pets_price(timestamp)
        bnb_price = get_bnb_price(timestamp)
        value_wei = int(event['value'], 16) if event['value'].startswith('0x') else 0
        pets_amount = value_wei / 1e18
        usd_value = pets_amount * pets_price
        bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
        method = "Add Public Listing" if event['topics'][0] == ADD_PUBLIC_LISTING_SELECTOR else "Settle Public Listing"
        image_url = get_gif_url("Sale" if method == "Settle Public Listing" else "Listing")
        tx_url = f"https://bscscan.com/tx/{tx_hash}"
        wallet_address = w3.eth.get_transaction(tx_hash)['to']
        if method == "Add Public Listing":
            message = (
                f"🔥 *New MicroPets NFT Listing* 🔥\n\n"
                f"Listed for: {pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)\n"
                f"Lister: {shorten_address(wallet_address)}\n"
                f"[🔍 View on BscScan]({tx_url})\n\n"
                f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
            )
        else:
            message = (
                f"🌸 *MicroPets NFT Sold!* 🌸\n\n"
                f"Sold for: {pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)\n"
                f"Buyer: {shorten_address(wallet_address)}\n"
                f"[🔍 View on BscScan]({tx_url})\n\n"
                f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
            )
        success = await send_message_with_retry(context, chat_id, message, image_url)
        if success:
            posted_events.add(tx_hash)
            log_posted_transaction(tx_hash)
            logger.info(f"Processed event {tx_hash} for chat {chat_id}")
            if chat_id == ALPHA_CHAT_ID:
                await asyncio.sleep(60)
                await send_message_with_retry(context, MARKET_CHAT_ID, message, image_url)
                await send_message_with_retry(context, TELEGRAM_CHAT_ID, message, image_url)
            return True
        return False
    except Exception as e:
        logger.error(f"Error processing event {event.get('transactionHash', 'unknown')}: {e}")
        return False

async def monitor_events(context: ContextTypes.DEFAULT_TYPE) -> None:
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
                await process_event(context, event, ALPHA_CHAT_ID if ALPHA_CHAT_ID else TELEGRAM_CHAT_ID)
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
    return str(update.effective_user.id) == ADMIN_USER_ID

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /start command from user {update.effective_user.id} in chat {chat_id}")
    active_chats.add(str(chat_id))
    await context.bot.send_message(chat_id=chat_id, text="👋 Welcome to MicroPets Marketplace Tracker! Use /track to start NFT alerts.")

async def track(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global is_monitoring_enabled, monitoring_task
    chat_id = update.effective_chat.id
    logger.info(f"Received /track command from user {update.effective_user.id} in chat {chat_id}")
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    if is_monitoring_enabled:
        await context.bot.send_message(chat_id=chat_id, text="🚀 Tracking already enabled")
        return
    is_monitoring_enabled = True
    active_chats.add(str(chat_id))
    monitoring_task = asyncio.create_task(monitor_events(context))
    logger.info("Event monitoring started via /track command")
    await context.bot.send_message(chat_id=chat_id, text="🚖 Tracking started. Notifications will include images.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global is_monitoring_enabled, monitoring_task
    chat_id = update.effective_chat.id
    logger.info(f"Received /stop command from user {update.effective_user.id} in chat {chat_id}")
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
    active_chats.discard(str(chat_id))
    await context.bot.send_message(chat_id=chat_id, text="🛑 Stopped")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /stats command from user {update.effective_user.id} in chat {chat_id}")
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Fetching marketplace stats")
    try:
        latest_block = w3.eth.block_number
        blocks_per_day = 24 * 60 * 60 // 3
        start_block = max(0, latest_block - (14 * blocks_per_day))
        events = await fetch_logs(startblock=start_block, endblock=latest_block)
        if not events:
            await context.bot.send_message(chat_id=chat_id, text="🚫 No events found in the last 2 weeks")
            return
        timestamp = int(time.time())
        pets_price = get_pets_price(timestamp)
        bnb_price = get_bnb_price(timestamp)
        add_count = sum(1 for e in events if e['topics'][0] == ADD_PUBLIC_LISTING_SELECTOR)
        settle_count = sum(1 for e in events if e['topics'][0] == SETTLE_PUBLIC_LISTING_SELECTOR)
        total_pets = sum(int(e['value'], 16) / 1e18 for e in events if e['topics'][0] in [ADD_PUBLIC_LISTING_SELECTOR, SETTLE_PUBLIC_LISTING_SELECTOR])
        usd_value = total_pets * pets_price
        bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
        message = (
            f"📊 *MicroPets Marketplace Stats (Last 2 Weeks)*\n\n"
            f"🔥 New Listings: {add_count}\n"
            f"🌸 Sales: {settle_count}\n"
            f"💰 Total $PETS: {total_pets:,.0f} (${usd_value:.2f}/{bnb_value:.3f} BNB)\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
        )
        await send_message_with_retry(context, chat_id, message, NFT_IMAGE_URL)
    except Exception as e:
        logger.error(f"Error in /stats: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Failed to fetch stats: {str(e)}")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /help command from user {update.effective_user.id} in chat {chat_id}")
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "🆘 *Commands:*\n\n"
            "/start - Start bot\n"
            "/track - Enable NFT alerts\n"
            "/stop - Disable alerts\n"
            "/stats - View marketplace stats\n"
            "/status - Check tracking status\n"
            "/test - Test listing and sale notifications\n"
            "/nov - Test without image\n"
            "/debug - Debug info\n"
            "/help - This message\n"
        ),
        parse_mode='Markdown'
    )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /status command from user {update.effective_user.id} in chat {chat_id}")
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 *Status:* {'Enabled' if is_monitoring_enabled else 'Disabled'}",
        parse_mode='Markdown'
    )

async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /debug command from user {update.effective_user.id} in chat {chat_id}")
    status = {
        'monitoringEnabled': is_monitoring_enabled,
        'lastBlockNumber': last_block_number,
        'recentErrors': recent_errors[-5:],
        'apiStatus': {
            'bscWeb3': bool(w3.is_connected()),
        },
        'activeChats': list(active_chats),
        'botRunning': bot_app.running if bot_app else False
    }
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 Debug:\n```json\n{json.dumps(status, indent=2)}\n```",
        parse_mode='Markdown'
    )

async def test(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /test command from user {update.effective_user.id} in chat {chat_id}")
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Generating test events (listing and sale)...")
    try:
        timestamp = int(time.time())
        pets_price = get_pets_price(timestamp)
        bnb_price = get_bnb_price(timestamp)
        image_url = get_gif_url('Sale')
        # Test Listing
        test_tx_hash = f"0xTestListing{uuid.uuid4().hex[:16]}"
        test_pets_amount = random.randint(1000000, 5000000)
        usd_value = test_pets_amount * pets_price
        bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
        listing_message = (
            f"🔥 *New MicroPets NFT Listing* Test\n\n"
            f"Listed for: {test_pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)\n"
            f"Lister: {shorten_address('0x' + ''.join(random.choices('0123456789abcdef', k=40)))}\n"
            f"[🔍 View](https://bscscan.com/tx/{test_tx_hash})\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
        )
        success = await send_message_with_retry(context, chat_id, listing_message, image_url)
        if not success:
            await context.bot.send_message(chat_id=chat_id, text="🚫 Listing test failed: Unable to send notification")
            return
        # Test Sale
        test_tx_hash = f"0xTestSale{uuid.uuid4().hex[:16]}"
        test_pets_amount = random.randint(1000000, 5000000)
        usd_value = test_pets_amount * pets_price
        bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
        sale_message = (
            f"🌸 *MicroPets NFT Sold!* Test\n\n"
            f"Sold for: {test_pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)\n"
            f"Buyer: {shorten_address('0x' + ''.join(random.choices('0123456789abcdef', k=40)))}\n"
            f"[🔍 View](https://bscscan.com/tx/{test_tx_hash})\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
        )
        success = await send_message_with_retry(context, chat_id, sale_message, image_url)
        if success:
            await context.bot.send_message(chat_id=chat_id, text="✅ Both tests successful")
        else:
            await context.bot.send_message(chat_id=chat_id, text="🚫 Sale test failed: Unable to send notification")
    except Exception as e:
        logger.error(f"Test error: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Test failed: {str(e)}")

async def no_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /nov command from user {update.effective_user.id} in chat {chat_id}")
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Testing event (no image)")
    try:
        timestamp = int(time.time())
        test_tx_hash = f"0xTestNoV{uuid.uuid4().hex[:16]}"
        test_pets_amount = random.randint(1000000, 5000000)
        pets_price = get_pets_price(timestamp)
        bnb_price = get_bnb_price(timestamp)
        usd_value = test_pets_amount * pets_price
        bnb_value = usd_value / bnb_price if bnb_price > 0 else 0
        message = (
            f"🌸 *MicroPets NFT Sold!* Test\n\n"
            f"Sold for: {test_pets_amount:,.0f} $PETS (${usd_value:.2f}/{bnb_value:.3f} BNB)\n"
            f"Buyer: {shorten_address('0x' + ''.join(random.choices('0123456789abcdef', k=40)))}\n"
            f"[🔍 View](https://bscscan.com/tx/{test_tx_hash})\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) | 📈 [Chart]({CHART_LINK}) | 🛍 [Merch]({MERCH_LINK}) | 💰 [Buy $PETS]({BUY_PETS_LINK})"
        )
        await send_message_with_retry(context, chat_id, message)
        await context.bot.send_message(chat_id=chat_id, text="✅ No-image test successful")
    except Exception as e:
        logger.error(f"/nov error: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Test failed: {str(e)}")

# FastAPI app
app = FastAPI()

# FastAPI routes
@app.get("/health")
async def health_check():
    logger.info("Health check endpoint called")
    try:
        if not w3.is_connected():
            logger.error("Web3 is not connected")
            raise Exception("Web3 connection failed")
        if not bot_app or not bot_app.bot:
            logger.error("Telegram bot not initialized")
            raise Exception("Telegram bot not initialized")
        return {"status": "Connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unavailable: {str(e)}")

# Lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot_app
    logger.info("Starting bot application")
    try:
        await initialize_last_block_number()
        posted_events.update(load_posted_transactions())
        bot_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        # Register command handlers
        handlers = [
            CommandHandler("start", start),
            CommandHandler("track", track),
            CommandHandler("stop", stop),
            CommandHandler("stats", stats),
            CommandHandler("help", help_command),
            CommandHandler("status", status),
            CommandHandler("debug", debug),
            CommandHandler("test", test),
            CommandHandler("nov", no_video)
        ]
        for handler in handlers:
            bot_app.add_handler(handler)
            logger.info(f"Registered handler for command /{handler.commands[0]}")
        # Set bot commands
        commands = [
            BotCommand("start", "Start the bot"),
            BotCommand("track", "Start tracking events"),
            BotCommand("stop", "Stop tracking events"),
            BotCommand("stats", "Show marketplace stats"),
            BotCommand("help", "Show commands"),
            BotCommand("status", "Check bot status"),
            BotCommand("debug", "Show debug info"),
            BotCommand("test", "Test event notification"),
            BotCommand("nov", "Test event without image")
        ]
        await bot_app.initialize()
        await bot_app.start()
        await bot_app.updater.start_polling(
            poll_interval=3,
            timeout=10,
            drop_pending_updates=True,
            error_callback=lambda e: logger.error(f"Polling error: {e}")
        )
        logger.info("Polling started successfully")
        await bot_app.bot.set_my_commands(commands)
        logger.info("Bot commands set successfully")
        yield
    except Exception as e:
        logger.error(f"Startup error: {e}")
        recent_errors.append({"time": datetime.now().isoformat(), "error": str(e)})
        raise
    finally:
 goede logger.info("Initiating bot shutdown...")
        try:
            if monitoring_task:
                monitoring_task.cancel()
                try:
                    await monitoring_task
                except asyncio.CancelledError:
                    logger.info("Monitoring task cancelled")
            if bot_app and bot_app.running:
                await bot_app.updater.stop()
                await bot_app.stop()
                await bot_app.shutdown()
                logger.info("Bot shutdown completed")
        except Exception as e:
            logger.error(f"Shutdown error: {str(e)}")

# Attach lifespan to FastAPI app
app = FastAPI(lifespan=lifespan)

# Main entry point
if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Uvicorn server on port {PORT}")
    try:
        uvicorn.run(app, host="0.0.0.0", port=PORT, reload=False)
    except Exception as e:
        logger.error(f"Uvicorn startup failed: {e}")
        raise
