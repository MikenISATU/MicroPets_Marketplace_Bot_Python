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
from telegram.ext import ApplicationBuilder, CommandHandler
from web3 import Web3
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from datetime import datetime
from decimal import Decimal
import telegram
import aiohttp
import threading
from bs4 import BeautifulSoup
import importlib.util
import sys

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

# Check python-telegram-bot version
logger.info(f"python-telegram-bot version: {telegram.__version__}")
if not telegram.__version__.startswith('20'):
    logger.error(f"Expected python-telegram-bot v20.0+, got {telegram.__version__}")
    raise SystemExit(1)

# Load environment variables
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
APP_URL = os.getenv('APP_URL')
BSCSCAN_API_KEY = os.getenv('BSCSCAN_API_KEY')
BNB_RPC_URL = os.getenv('BNB_RPC_URL')
CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS')
ADMIN_USER_ID = os.getenv('ADMIN_USER_ID')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
ALPHA_CHAT_ID = os.getenv('ALPHA_CHAT_ID')
MARKET_CHAT_ID = os.getenv('MARKET_CHAT_ID')
PORT = int(os.getenv('PORT', 8080))
COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY', '')
TARGET_ADDRESS = os.getenv('TARGET_ADDRESS', '0x4BdEcE4E422fA015336234e4fC4D39ae6dD75b01')
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', 60))
BSC_URL = os.getenv('BSC_URL', 'https://bscscan.com/token/0x2466858ab5edad0bb597fe9f008f568b00d25fe3')

# Validate environment variables
missing_vars = []
for var, name in [
    (TELEGRAM_BOT_TOKEN, 'TELEGRAM_BOT_TOKEN'),
    (APP_URL, 'APP_URL'),
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

# Validate Ethereum addresses
for addr, name in [(CONTRACT_ADDRESS, 'CONTRACT_ADDRESS'), (TARGET_ADDRESS, 'TARGET_ADDRESS')]:
    if not Web3.is_address(addr):
        logger.error(f"Invalid Ethereum address for {name}: {addr}")
        raise ValueError(f"Invalid Ethereum address for {name}: {addr}")

if not COINMARKETCAP_API_KEY:
    logger.warning("COINMARKETCAP_API_KEY is empty; CoinMarketCap API calls will be skipped")

# Initialize active chats
active_chats: Set[str] = {TELEGRAM_CHAT_ID}
if ALPHA_CHAT_ID:
    active_chats.add(ALPHA_CHAT_ID)
if MARKET_CHAT_ID:
    active_chats.add(MARKET_CHAT_ID)

logger.info(f"Environment loaded successfully. APP_URL={APP_URL}, PORT={PORT}, Active chats={active_chats}")

# Constants
BASE_URL = "https://element.market/collections/micropetsnewerabnb-5414f1c9?search[toggles][0]=ALL"

# In-memory data
transaction_cache: List[Dict] = []
last_transaction_hash: Optional[str] = None
last_block_number: Optional[int] = None
is_tracking_enabled: bool = False
recent_errors: List[Dict] = []
last_transaction_fetch: Optional[float] = None
TRANSACTION_CACHE_THRESHOLD = 2 * 60 * 1000
posted_transactions: Set[str] = set()
transaction_details_cache: Dict[str, float] = {}
monitoring_task = None
polling_task = None
file_lock = threading.Lock()

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
        return "https://i.giphy.com/media/3o6Zt6KHxJTbXCvgaU/giphy.gif"
    except Exception as e:
        logger.error(f"Failed to scrape GIF URL: {e}")
        return "https://i.giphy.com/media/3o6Zt6KHxJTbXCvgaU/giphy.gif"

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
                f.write(transaction_hash + '\n')
    except Exception as e:
        logger.warning(f"Could not write to posted_transactions.txt: {e}")

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
def get_pets_price() -> float:
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
        if not isinstance(price_str, (str, float, int)) or not price_str:
            raise ValueError("Invalid price data from GeckoTerminal")
        price = float(price_str)
        if price <= 0:
            raise ValueError("Geckoterminal returned non-positive price")
        logger.info(f"$PETS price from GeckoTerminal: ${price:.10f}")
        time.sleep(0.5)
        return price
    except Exception as e:
        logger.error(f"GeckoTerminal $PETS price fetch failed: {e}, status={getattr(e.response, 'status_code', 'N/A')}")
        return 0.00003886  # Fallback price if Geckoterminal fails

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
def get_transaction_details(transaction_hash: str) -> Optional[float]:
    if transaction_hash in transaction_details_cache:
        logger.info(f"Using cached BNB value for transaction {transaction_hash}")
        return transaction_details_cache[transaction_hash]
    try:
        response = requests.get(
            f"https://api.bscscan.com/api?module=proxy&action=eth_getTransactionByHash&txhash={transaction_hash}&apikey={BSCSCAN_API_KEY}",
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or 'result' not in data:
            logger.error(f"Invalid response for transaction {transaction_hash}: {data}")
            return None
        result = data['result']
        if not isinstance(result, dict):
            logger.error(f"Transaction {transaction_hash} result is not a dict: {result}")
            return None
        value_wei_str = result.get('value', '0')
        if not isinstance(value_wei_str, str) or not value_wei_str.startswith('0x'):
            logger.error(f"Invalid value data for transaction {transaction_hash}: {value_wei_str}")
            return None
        value_wei = int(value_wei_str, 16)
        bnb_value = float(w3.from_wei(value_wei, 'ether'))
        logger.info(f"Transaction {transaction_hash}: BNB value={bnb_value:.6f}")
        transaction_details_cache[transaction_hash] = bnb_value
        time.sleep(0.5)
        return bnb_value
    except Exception as e:
        logger.error(f"Failed to fetch transaction details for {transaction_hash}: {e}")
        return None

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
async def fetch_bscscan_transactions(startblock: Optional[int] = None, endblock: Optional[int] = None, for_stats: bool = False) -> List[Dict]:
    global transaction_cache, last_transaction_fetch, last_block_number
    try:
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': Web3.to_checksum_address(CONTRACT_ADDRESS),
            'startblock': startblock or 0,
            'endblock': endblock or 99999999,
            'page': 1,
            'offset': 100 if not for_stats else 10000,
            'sort': 'desc',
            'apikey': BSCSCAN_API_KEY
        }
        logger.info(f"Fetching BscScan transactions with params: {params}")
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.bscscan.com/api", params=params, timeout=30) as response:
                response_text = await response.text()
                logger.debug(f"BscScan response: {response_text[:200]}...")
                response.raise_for_status()
                data = await response.json()
        if not isinstance(data, dict) or data.get('status') != '1':
            logger.error(f"Invalid BscScan response: {data}")
            raise ValueError(f"Invalid BscScan response: {data.get('message', 'No message')}")
        transactions = [
            {
                'transactionHash': tx['hash'],
                'to': tx['to'],
                'from': tx['from'],
                'value': tx['value'],
                'blockNumber': int(tx['blockNumber']),
                'timeStamp': int(tx['timeStamp']),
                'isError': tx['isError'],
                'input': tx.get('input', '')
            }
            for tx in data['result']
            if tx['input'] and any(keyword in tx['input'].lower() for keyword in ['list', 'buy', 'sale', 'transfer'])
        ]
        if transactions and not startblock and not for_stats:
            last_block_number = max(tx['blockNumber'] for tx in transactions)
        if not startblock and not for_stats:
            transaction_cache = [tx for tx in transactions if last_block_number is None or tx['blockNumber'] >= last_block_number]
            transaction_cache = transaction_cache[-1000:]
            last_transaction_fetch = datetime.now().timestamp() * 1000
        logger.info(f"Fetched {len(transactions)} marketplace transactions, last_block_number={last_block_number}")
        time.sleep(0.5)
        return transactions
    except Exception as e:
        logger.error(f"Failed to fetch BscScan transactions: {e}")
        recent_errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
        if len(recent_errors) > 5:
            recent_errors.pop(0)
        await asyncio.sleep(5)
        return transaction_cache or []

async def send_gif_with_retry(context, chat_id: str, gif_url: str, options: Dict, max_retries: int = 3, delay: int = 2) -> bool:
    if not hasattr(context, 'bot') or context.bot is None:
        logger.error(f"Bot not initialized for chat {chat_id}")
        return False
    for i in range(max_retries):
        try:
            logger.info(f"Attempt {i+1}/{max_retries} to send GIF to chat {chat_id}: {gif_url}")
            async with aiohttp.ClientSession() as session:
                async with session.head(gif_url, timeout=5) as head_response:
                    if head_response.status != 200:
                        logger.error(f"GIF URL inaccessible, status {head_response.status}: {gif_url}")
                        raise Exception(f"GIF URL inaccessible, status {head_response.status}")
            await context.bot.send_animation(chat_id=chat_id, animation=gif_url, **options)
            logger.info(f"Successfully sent GIF to chat {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send GIF (attempt {i+1}/{max_retries}): {e}")
            if i == max_retries - 1:
                await context.bot.send_message(
                    chat_id,
                    f"{options['caption']}\n\n⚠️ GIF unavailable",
                    parse_mode='Markdown'
                )
                return False
            await asyncio.sleep(delay)
    return False

async def process_transaction(context, transaction: Dict, pets_price: float, chat_id: str = TELEGRAM_CHAT_ID) -> bool:
    global posted_transactions
    try:
        if not isinstance(transaction, dict) or 'transactionHash' not in transaction:
            logger.error(f"Invalid transaction format: {transaction}")
            return False
        transaction_hash = transaction['transactionHash']
        if transaction_hash in posted_transactions:
            logger.info(f"Skipping already posted transaction: {transaction_hash}")
            return False
        bnb_value = get_transaction_details(transaction_hash)
        if bnb_value is None or bnb_value <= 0:
            logger.info(f"Skipping transaction {transaction_hash} with invalid BNB value: {bnb_value}")
            return False
        is_listing = 'list' in transaction['input'].lower() and not transaction['isError']
        is_sale = any(keyword in transaction['input'].lower() for keyword in ['buy', 'sale']) and not transaction['isError']
        if not (is_listing or is_sale):
            logger.info(f"Skipping transaction {transaction_hash} - not a listing or sale")
            return False
        nft_amount = float(transaction['value']) / 1e18 if is_sale else random.randint(1, 10)
        usd_value = nft_amount * pets_price * 1000000 if is_sale else 0
        wallet_address = transaction['to']
        tx_url = f"https://bscscan.com/tx/{transaction_hash}"
        category = 'Sale' if is_sale else 'Listing'
        gif_url = get_gif_url(category)
        emoji_count = min(int(usd_value) // 100 if is_sale else 10, 100)
        emojis = '💰' * emoji_count
        sale_pets_amount = 2943823
        sale_usd_value = sale_pets_amount * pets_price
        if is_listing:
            message = (
                f"🌟 *MicroPets NFT Listing!* BNB Chain 💰\n\n"
                f"{emojis}\n"
                f"📈 **Listed for:** {nft_amount:.0f} NFTs\n"
                f"💵 BNB Value: {bnb_value:,.4f}\n"
                f"🦑 Lister: {shorten_address(wallet_address)}\n"
                f"[🔍 View on BscScan]({tx_url})\n"
            )
        else:
            message = (
                f"🌸 *MicroPets NFT Sold!* BNB Chain 💰\n\n"
                f"{emojis}\n"
                f"🔥 **Sold for:** {nft_amount:.0f} NFTs\n"
                f"💰 **Worth:** ${sale_usd_value:.2f} (based on {sale_pets_amount:,.0f} $PETS)\n"
                f"💵 BNB Value: {bnb_value:,.4f}\n"
                f"🦑 Buyer: {shorten_address(wallet_address)}\n"
                f"[🔍 View on BscScan]({tx_url})\n"
            )
        success = await send_gif_with_retry(context, chat_id, gif_url, {'caption': message, 'parse_mode': 'Markdown'})
        if success:
            posted_transactions.add(transaction_hash)
            log_posted_transaction(transaction_hash)
            logger.info(f"Processed transaction {transaction_hash} for chat {chat_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error processing transaction {transaction.get('transactionHash', 'unknown')}: {e}")
        return False

async def monitor_transactions(context) -> None:
    global last_transaction_hash, last_block_number, is_tracking_enabled, monitoring_task
    logger.info("Starting marketplace transaction monitoring")
    while is_tracking_enabled:
        async with asyncio.Lock():
            if not is_tracking_enabled:
                logger.info("Tracking disabled, stopping monitoring")
                break
            try:
                posted_transactions.update(load_posted_transactions())
                txs = await fetch_bscscan_transactions(startblock=last_block_number + 1 if last_block_number else None)
                if not txs:
                    logger.info("No new marketplace transactions found")
                    await asyncio.sleep(POLLING_INTERVAL)
                    continue
                pets_price = get_pets_price()
                new_last_hash = last_transaction_hash
                for tx in sorted(txs, key=lambda x: x['blockNumber'], reverse=True):
                    if not isinstance(tx, dict):
                        logger.error(f"Invalid transaction format: {tx}")
                        continue
                    if tx['transactionHash'] in posted_transactions:
                        logger.info(f"Skipping already posted transaction: {tx['transactionHash']}")
                        continue
                    if last_transaction_hash and tx['transactionHash'] == last_transaction_hash:
                        continue
                    if last_block_number and tx['blockNumber'] <= last_block_number:
                        logger.info(f"Skipping old transaction {tx['transactionHash']} with block {tx['blockNumber']} <= {last_block_number}")
                        continue
                    if await process_transaction(context, tx, pets_price):
                        new_last_hash = tx['transactionHash']
                        last_block_number = max(last_block_number or 0, tx['blockNumber'])
                last_transaction_hash = new_last_hash
            except Exception as e:
                logger.error(f"Error monitoring transactions: {e}")
                recent_errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
                if len(recent_errors) > 5:
                    recent_errors.pop(0)
            await asyncio.sleep(POLLING_INTERVAL)
    logger.info("Monitoring task stopped")
    monitoring_task = None

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
async def set_webhook_with_retry(bot_app) -> bool:
    webhook_url = f"https://{APP_URL}/webhook"
    logger.info(f"Attempting to set webhook: {webhook_url}")
    try:
        await asyncio.sleep(5)
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://{APP_URL}/health", timeout=15) as response:
                if response.status != 200:
                    logger.error(f"Health check failed, status {response.status}, response: {await response.text()}")
                    raise Exception(f"Health check failed, status {response.status}")
                logger.info(f"Health check passed, status {response.status}")
        await bot_app.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Deleted existing webhook")
        await bot_app.bot.set_webhook(webhook_url, allowed_updates=["message", "channel_post"])
        logger.info(f"Webhook set successfully: {webhook_url}")
        return True
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")
        raise

async def polling_fallback(bot_app) -> None:
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
                while polling_task and not polling_task.cancelled():
                    await asyncio.sleep(60)
            else:
                logger.warning("Bot already running, skipping polling start")
                while polling_task and not polling_task.cancelled():
                    await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Polling error: {e}")
            await asyncio.sleep(10)
        finally:
            if bot_app.running and polling_task and polling_task.cancelled():
                try:
                    await bot_app.updater.stop()
                    await bot_app.stop()
                    await bot_app.shutdown()
                    logger.info("Polling stopped")
                except Exception as e:
                    logger.error(f"Error stopping polling: {e}")

def is_admin(update: Update) -> bool:
    return str(update.effective_chat.id) == ADMIN_USER_ID

# Command handlers
async def start(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    active_chats.add(str(chat_id))
    await context.bot.send_message(chat_id=chat_id, text="👋 Welcome to MicroPets Marketplace Tracker! Use /track to start NFT alerts.")

async def track(update: Update, context) -> None:
    global is_tracking_enabled, monitoring_task
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    if is_tracking_enabled and monitoring_task:
        await context.bot.send_message(chat_id=chat_id, text="🚀 Tracking already enabled")
        return
    is_tracking_enabled = True
    active_chats.add(str(chat_id))
    monitoring_task = asyncio.create_task(monitor_transactions(context))
    logger.info(f"Tracking enabled by admin in chat {chat_id}")
    await context.bot.send_message(chat_id=chat_id, text="🚖 NFT Tracking started")

async def stop(update: Update, context) -> None:
    global is_tracking_enabled, monitoring_task
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    is_tracking_enabled = False
    if monitoring_task:
        monitoring_task.cancel()
        try:
            await monitoring_task
        except asyncio.CancelledError:
            logger.info("Monitoring task cancelled")
        monitoring_task = None
    active_chats.discard(str(chat_id))
    logger.info(f"Tracking stopped by admin in chat {chat_id}")
    await context.bot.send_message(chat_id=chat_id, text="🛑 Stopped")

async def stats(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Fetching marketplace data")
    try:
        txs = await fetch_bscscan_transactions(for_stats=True)
        if not txs:
            logger.info("No marketplace transactions found, attempting broader query")
            txs = await fetch_bscscan_transactions(startblock=0, for_stats=True)
        if not txs:
            logger.error("No transactions found even with broader query")
            await context.bot.send_message(chat_id=chat_id, text="🚫 No marketplace events found. Check CONTRACT_ADDRESS.")
            return
        pets_price = get_pets_price()
        listing_tx = next((tx for tx in txs if 'list' in tx['input'].lower() and not tx['isError']), None)
        sale_tx = next((tx for tx in txs if any(keyword in tx['input'].lower() for keyword in ['buy', 'sale']) and not tx['isError']), None)
        if not listing_tx and not sale_tx:
            logger.info("No valid listing or sale transactions found")
            await context.bot.send_message(chat_id=chat_id, text="🚫 No valid listing or sale events found")
            return
        message_parts = []
        if listing_tx:
            bnb_value = get_transaction_details(listing_tx['transactionHash']) or 0
            nft_amount = random.randint(1, 10)
            wallet_address = listing_tx['to']
            tx_url = f"https://bscscan.com/tx/{listing_tx['transactionHash']}"
            emoji_count = min(10, 100)
            emojis = '💰' * emoji_count
            message_parts.append(
                f"🌟 *MicroPets NFT Listing!* BNB Chain 💰\n\n"
                f"{emojis}\n"
                f"📈 **Listed for:** {nft_amount:.0f0} NFTs\n"
                f"💵 BNB Value: {bnb_value:,.4f}\n"
                f"🦑 Lister: {shorten_address(wallet_address)}\n"
                f"[🔍 View on BscScan]({tx_url})\n"
            )
        if sale_tx:
            bnb_value = get_transaction_details(sale_tx['transactionHash']') or 0
            nft_amount = float(sale_tx['value']) / 1e18
            wallet_address = sale_tx['to']
            tx_url = f"https://bsc.com/tx/{sale_tx['transactionHash']}"
            emoji_count = min(int(nft_amount * pets_price * 1000000 // 100), 100)
            emojis = '💰' * emoji_count
            sale_pets_amount = 2943823
            sale_usd_value = sale_pets_amount * pets_price
            message_parts.append(
                f"🌸 *MicroPets NFT Sold!* BNB Chain 💰\n\n"
                f"{emojis}\n"
                f"🔥 **Sold for:** {nft_amount:.0f0} NFTs\n"
                f"💰 **Worth:** ${sale_usd_value:.2f} (based on {sale_pets_amount:,.0f} $PETS)\n"
                f"💵 BNB Value: {bnb_value:,.4f}\n"
                f"🦑 Buyer: {shorten_address(wallet_address)}\n"
                f"[🔍 View on BscScan]({tx_url})\n"
            )
        if message_parts:
            full_message = "\n".join(message_parts)
            gif_url = get_gif_url('Sale' if sale_tx else 'Listing')
            await send_gif_with_retry(context, chat_id, gif_url, {'caption': full_message, 'parse_mode': 'Markdown'})
        else:
            await context.bot.send_message(chat_id=chat_id, text="No valid data to display")
    except Exception as e:
        logger.error(f"Error in /stats: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"Failed to fetch data: {str(e)}")

async def help_command(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "🆘 *Commands:*\n\n"
            "/start - Start bot\n"
            "/track - Enable NFT alerts\n"
            "/stop - Disable alerts\n"
            "/stats - View latest listing and sale\n"
            "/status - Tracking status\n"
            "/test - Test transaction\n"
            "/noV - Test without video\n"
            "/debug - Debug info\n"
            "/help - This message\n"
        ),
        parse_mode='Markdown'
    )

async def status(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 *Status:* {'Enabled' if is_tracking_enabled else 'Disabled'}",
        parse_mode='Markdown'
    )

async def debug(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    status = {
        'trackingEnabled': is_tracking_enabled,
        'activeChats': list(active_chats),
        'lastTxHash': last_transaction_hash,
        'lastBlockNumber': last_block_number,
        'recentErrors': recent_errors[-5:],
        'apiStatus': {
            'bscWeb3': bool(w3.is_connected()),
            'lastTransactionFetch': datetime.fromtimestamp(last_transaction_fetch / 1000).isoformat() if last_transaction_fetch else None
        },
        'pollingActive': polling_task is not None and not polling_task.done()
    }
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 Debug:\n```json\n{json.dumps(status, indent=2)}\n```",
        parse_mode='Markdown'
    )

async def test(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    if not hasattr(context, 'bot) or not context.bot:
        logger.error(f"Bot not initialized for /test in chat {chat_id}")
        await context.bot.send_message(chat_id=chat_id, text="🚫 Bot not initialized")
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Generating test marketplace event")
    try:
        test_tx_hash = f"0xTest{uuid.uuid4().hex[:16]}""
        pets_price = get_pets_price()
        bnb_value = random.uniform(0.1, 10)
        listing_nft_amount = random.randint(1, 10)
        sale_nft_amount = 1
        sale_usd_value = 2943823 * pets_price
        wallet_address = f"0x{random.randint(10**15, 10**16):0x}"
        emoji_count = min(10, 100)
        emojis = '💰' * emoji_count
        tx_url = f"https://bscscan.com/tx/{test_tx_hash}"
        message = (
            f"🌟 *MicroPets NFT Listing!* Test\n\n"
            f"{emojis}\n"
            f"📈 **Listed for:** {listing_nft_amount:.0f} NFTs\n"
            f"💵 BNB Value: ${bnb_value:,.4f}\n"
            f"🦑 Lister: {shorten_address(wallet_address)}\n"
            f"[🔍 View]({tx_url})\n\n"
            f"🌸 *MicroPets NFT Sold!* Test\n\n"
            f"{emojis}\n"
            f"🔥 **Sold for:** {sale_nft_amount:.0f} NFTs\n"
            f"💰 **Worth:** ${sale_usd_value:.2f} (based on 2,943,823 $PETS)\n"
            f"💵 BNB Value: ${bnb_value:,.4f}\n"
            f"🦑 Buyer: ${shorten_address(wallet_address)}\n"
            f"[🔍 View]({tx_url})\n"
        )
        gif_url = get_gif_url('Sale') or "https://i.giphy.com/media/3o6Zt6KHxJTbXCvgaU/giphy.gif"
        success = await send_gif_with_retry(context, chat_id, gif_url, {'caption': message, 'parse_mode': 'Markdown'})
        if success:
            await context.bot.send_message(chat_id=chat_id, text="🚖 Success")
        else:
            await context.bot.send_message(chat_id=chat_id, text="🚫 Failed to send test GIF")
    except Exception as e:
        logger.error(f"Test error: {e}", exc_info=True)
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Failed: {str(e)}")

async def no_video(update: Update, context) -> None:
    chat_id = update.effective_chat.id
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized")
        return
    if not hasattr(context, 'bot') or not context.bot:
        logger.error(f"Bot not initialized for /noV in chat {chat_id}")
        await context.bot.send_message(chat_id=chat_id, text="🚫 Bot not initialized")
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Testing marketplace event (no video)")
    try:
        test_tx_hash = f"0xTestNoV{uuid.uuid4().hex[:16]}"
        pets_price = get_pets_price()
        bnb_value = random.uniform(0.1, 10)
        listing_nft_amount = random.randint(1, 10)
        sale_nft_amount = 1
        sale_usd_value = 2943823 * pets_price
        wallet_address = f"0x"{random.randint(10**15, 10**16):0x}"
        emoji_count = min(10, 100)
        emojis = '💰' * emoji_count
        tx_url = f"https://bscscan.com/tx/{test_tx_hash}"
        message = (
            f"🌟 *MicroPets NFT Listing!* Test\n\n"
            f"{emojis}\n"
            f"📈 **Listed for:** {listing_nft_amount:.0f0} NFTs\n"
            f"💵 BNB Value: ${bnb_value:,.4f}\n"
            f"🦑 Lister: ${shorten_address(wallet_address)}\n"
            f"[🔍]({tx_url})\n\n"
            f"🌸 *MicroPets NFT Sold!* Test\n\n"
            f"{emojis}\n"
            f"🔥 **Sold for:** {sale_nft_amount:.0f0} NFTs\n"
            f"💰 **Worth:** ${sale_usd_value:.2f} (based on 2,943,823 $PETS)\n"
            f"💵 BNB Value: ${bnb_value:,.4f}\n"
            f"🦑 Buyer: {shorten_address(wallet_address)}\n"
            f"[🔍]({tx_url})\n"
        )
        await context.bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown')
        await context.bot.send_message(chat_id=chat_id, text="🚖 OK")
    except Exception as e:
        logger.error(f"/noV error: {e}", exc_info=True)
        await context.bot.send_message(chat_id=chat_id, text=f"🚖 Error: {str(e)}")

# FastAPI routes
@app.get("/health")
async def health_check():
    logger.info("Health check endpoint called")
    try:
        if not w3.is_connected():
            raise Exception("Web3 is not connected")
        if not bot_app or not bot_app.bot:
            raise Exception("Telegram bot not initialized")
        return {"status": "Connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")

@app.get("/webhook")
async def webhook_get():
    logger.info("Received GET webhook")
    raise HTTPException(status_code=405, detail="Method Not Allowed")

@app.get("/api/transactions")
async def get_transactions():
    logger.info("Fetching transactions via API")
    return transaction_cache

@app.post("/webhook")
async def webhook(request: Request):
    logger.info("Received POST webhook request")
    try:
        async with aiohttp.ClientSession() as session:
            data = await request.json()
            if not isinstance(data, dict):
                logger.error(f"Invalid webhook data: {data}")
                return {"error": "Invalid JSON data"}, 400
            if not bot_app or not bot_app.bot:
                logger.error("Bot not initialized for webhook")
                return {"error": "Bot not initialized"}, 500
            update = Update.de_json(data, bot_app.bot)
            if update:
                await bot_app.process_update(update)
            return {"status": "ok"}
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            recent_errors.append({"time": datetime.now().isoformat(), "error': str(e)}")
            if len(recent_errors) > 5:
                recent_errors.pop(0)
            return {"error": "Webhook failed"}, 500

# Lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    global monitoring_task, polling_task, bot_app
    logger.info("Starting bot application")
    try:
        bot_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        
        # Register command handlers
        bot_app.add_handler(CommandHandler("start", start))
        bot_app.add_handler(CommandHandler("track", track))
        bot_app.add_handler(CommandHandler("stop", stop))
        bot_app.add_handler(CommandHandler("stats", stats))
        bot_app.add_handler(CommandHandler("help", help_command))
        bot_app.add_handler(CommandHandler("status", status))
        bot_app.add_handler(CommandHandler("debug", debug))
        bot_app.add_handler(CommandHandler("test", test))
        bot_app.add_handler(CommandHandler("noV", no_video))

        await bot_app.initialize()
        try:
            await set_webhook_with_retry(bot_app)
            is_tracking_enabled = True
            monitoring_task = asyncio.create_task(monitor_transactions(bot_app))
            logger.info("Webhook set successfully")
        except Exception as e:
            logger.error(f"Webhook setup failed: {e}. Switching to polling")
            polling_task = asyncio.create_task(polling_fallback(bot_app)))
            is_tracking_enabled = True
            monitoring_task = asyncio.create_task(monitor_transactions(bot_app)))
            logger.info("Polling started, monitoring enabled")
        logger.info("Bot startup completed")
        yield
    except Exception as e:
        logger.error(f"Startup error: {e}")
        recent_errors.append({"time": datetime.now().isoformat(), "error": str(e)})
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
                try:
                    await bot_app.updater.stop()
                    await bot_app.stop()
                except Exception as e:
                    logger.error(f"Error stopping bot: {e}")
            if bot_app and bot_app.bot:
                await bot_app.bot.delete_webhook(drop_pending_updates=True)
                await bot_app.shutdown()
            logger.info("Bot shutdown completed")
        except Exception as e:
            logger.error(f"Shutdown error: {str(e)}")

app = FastAPI(lifespan=lifespan))

# Ensure module is importable
if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Uvicorn server on port {PORT}")
    try:
        uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
    except Exception FMC as e:
        logger.error(f"Uvicorn startup failed: {e}")
        raise
