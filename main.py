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
from fastapi import FastAPI, HTTPException
from telegram import Update, BotCommand, InputFile
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from web3 import Web3
from web3.logs import STRICT
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from datetime import datetime
import aiohttp
import threading
from bs4 import BeautifulSoup
import io
from collections import defaultdict

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
BSCSCAN_API_KEY = os.getenv('BSCSCAN_API_KEY')
BNB_RPC_URL = os.getenv('BNB_RPC_URL')
ALCHEMY_API_KEY = os.getenv('ALCHEMY_API_KEY', '5IyUyaJBrZq9eBDKxarcQEkkeBlfUOG_')
CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS', '0x2cCFDC83BbdD8bbd2979416620ADB2344B2Cb672')
ADMIN_CHAT_ID = os.getenv('ADMIN_USER_ID')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
ALPHA_CHAT_ID = os.getenv('ALPHA_CHAT_ID')
MARKET_CHAT_ID = os.getenv('MARKET_CHAT_ID')
PORT = int(os.getenv('PORT', 8080))
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', 60))

# Validate environment variables
missing_vars = []
for var, name in [
    (TELEGRAM_BOT_TOKEN, 'TELEGRAM_BOT_TOKEN'),
    (BSCSCAN_API_KEY, 'BSCSCAN_API_KEY'),
    (BNB_RPC_URL, 'BNB_RPC_URL'),
    (ALCHEMY_API_KEY, 'ALCHEMY_API_KEY'),
    (CONTRACT_ADDRESS, 'CONTRACT_ADDRESS'),
    (ADMIN_CHAT_ID, 'ADMIN_USER_ID'),
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

logger.info(f"Environment loaded successfully. PORT={PORT}")

# Constants
BASE_URL = "https://element.market/collections/micropetsnewerabnb-5414f1c9?search[toggles][0]=ALL"
FALLBACK_GIF = "https://media.giphy.com/media/3o7bu3X8f7wY5zX9K0/giphy.gif"
PETS_AMOUNT = 2943823  # Fixed PETS amount for sales
MARKETPLACE_LINK = "https://pets.micropets.io/marketplace"
CHART_LINK = "https://www.dextools.io/app/en/bnb/pair-explorer/0x4bdece4e422fa015336234e4fc4d39ae6dd75b01?t=1749434278227"
MERCH_LINK = "https://micropets.store/"
BUY_PETS_LINK = "https://pancakeswap.finance/swap?outputCurrency=0x2466858ab5edAd0BB597FE9f008F568B00d25Fe3"
ALCHEMY_API = "https://bnb-mainnet.g.alchemy.com/nft/v3"
IPFS_GATEWAYS = [
    'https://cloudflare-ipfs.com/ipfs/',
    'https://ipfs.io/ipfs/',
    'https://gateway.pinata.cloud/ipfs/'
]

# ERC-721 ABI for Transfer event
ERC721_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": True, "name": "tokenId", "type": "uint256"}
        ],
        "name": "Transfer",
        "type": "event"
    }
]

# In-memory data
posted_transactions: Set[str] = set()
last_block_number: Optional[int] = None
is_tracking_enabled: bool = False
monitoring_task: Optional[asyncio.Task] = None
recent_errors: List[Dict] = []
file_lock = threading.Lock()
bot_app = None
metadata_cache: Dict[str, tuple[Dict, float]] = {}  # {token_id: (metadata, expiry_timestamp)}
latest_transactions: Dict[str, Dict] = defaultdict(dict)  # {type: {tx_hash: transaction}}

# Initialize Web3
try:
    w3 = Web3(Web3.HTTPProvider(f'https://bnb-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}', request_kwargs={'timeout': 60}))
    if not w3.is_connected():
        raise Exception("Primary RPC URL connection failed")
    logger.info("Successfully initialized Web3 with Alchemy RPC")
except Exception as e:
    logger.error(f"Failed to initialize Web3 with primary URL: {e}")
    w3 = Web3(Web3.HTTPProvider('https://bsc-dataseed2.binance.org', request_kwargs={'timeout': 60}))
    if not w3.is_connected():
        logger.error("Fallback RPC URL connection failed")
        raise ValueError("Both primary and fallback Web3 connections failed")
    logger.info("Web3 initialized with fallback")

contract = w3.eth.contract(address=Web3.to_checksum_address(CONTRACT_ADDRESS), abi=ERC721_ABI)

# Helper functions
async def initialize_last_block_number():
    global last_block_number
    try:
        last_block_number = w3.eth.block_number
        logger.info(f"Initialized last_block_number to {last_block_number}")
    except Exception as e:
        logger.error(f"Failed to initialize last_block_number: {e}")

def ipfs_to_http(ipfs_url: str, gateways: List[str] = IPFS_GATEWAYS) -> str:
    try:
        cid = ipfs_url.replace('ipfs://', '')
        return gateways[0] + cid
    except Exception as e:
        logger.error(f"Failed to convert IPFS URL {ipfs_url}: {e}")
        return None

@retry(wait=wait_exponential(multiplier=2, min=5, max=20), stop=stop_after_attempt(2))
async def fetch_nft_metadata(token_id: str) -> Optional[Dict]:
    """Fetch NFT metadata using Alchemy."""
    try:
        headers = {'accept': 'application/json'}
        url = f"{ALCHEMY_API}/{ALCHEMY_API_KEY}/getNFTMetadata?contractAddress={CONTRACT_ADDRESS}&tokenId={token_id}&refreshCache=false"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                metadata = {
                    'name': data.get('title') or data.get('metadata', {}).get('name'),
                    'image': data.get('media', [{}])[0].get('gateway') or data.get('metadata', {}).get('image'),
                    'animation_url': data.get('metadata', {}).get('animation_url'),
                    'attributes': data.get('metadata', {}).get('attributes', [])
                }
                if metadata['image'] and metadata['image'].startswith('ipfs://'):
                    metadata['image'] = ipfs_to_http(metadata['image'])
                if metadata['animation_url'] and metadata['animation_url'].startswith('ipfs://'):
                    metadata['animation_url'] = ipfs_to_http(metadata['animation_url'])
                metadata_cache[token_id] = (metadata, time.time() + 24 * 3600)  # Cache for 24 hours
                logger.info(f"Fetched metadata for token {token_id}")
                return metadata
    except Exception as e:
        logger.error(f"Failed to fetch metadata for token {token_id}: {e}")
        return None

async def download_media(url: str) -> Optional[io.BytesIO]:
    """Download media to memory."""
    for gateway in IPFS_GATEWAYS:
        try:
            if url.startswith('ipfs://'):
                url = url.replace('ipfs://', gateway)
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as response:
                    if response.status != 200:
                        logger.error(f"Media download failed, status {response.status}: {url}")
                        continue
                    data = io.BytesIO(await response.read())
                    data.name = 'nft.mp4' if url.endswith('.mp4') else 'nft.jpg'
                    logger.info(f"Downloaded media from {url}")
                    return data
        except Exception as e:
            logger.error(f"Failed to download media from {gateway}: {e}")
            continue
    return None

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
            return random.choice([elem['src'] for elem in gif_elements])
        logger.warning("No GIFs found, using fallback")
        return FALLBACK_GIF
    except Exception as e:
        logger.error(f"Failed to scrape GIF URL: {e}")
        return FALLBACK_GIF

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
        price = float(price_str)
        if price <= 0:
            raise ValueError("Geckoterminal returned non-positive price")
        logger.info(f"$PETS price from GeckoTerminal: ${price:.10f}")
        return price
    except Exception as e:
        logger.error(f"GeckoTerminal $PETS price fetch failed: {e}")
        return 0.00003886

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
def get_bnb_price() -> float:
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
        logger.info(f"BNB price from Geckoterminal: ${price:.2f}")
        return price
    except Exception as e:
        logger.error(f"GeckoTerminal BNB price fetch failed: {e}")
        return 600.0

async def get_token_id_from_tx(tx_hash: str) -> Optional[str]:
    """Parse Transfer event from transaction receipt to get token ID."""
    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        logs = contract.events.Transfer().process_receipt(receipt, errors=STRICT)
        if logs:
            return str(logs[0]['args']['tokenId'])
        logger.warning(f"No Transfer event found in tx {tx_hash}")
        return None
    except Exception as e:
        logger.error(f"Failed to parse token ID from tx {tx_hash}: {e}")
        return None

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
async def fetch_nft_sales(from_block: Optional[int] = None) -> List[Dict]:
    """Fetch NFT sales using Alchemy."""
    try:
        headers = {'accept': 'application/json'}
        params = {
            'contractAddress': CONTRACT_ADDRESS,
            'limit': 100,
            'order': 'desc'
        }
        if from_block:
            params['fromBlock'] = hex(from_block)
        url = f"{ALCHEMY_API}/{ALCHEMY_API_KEY}/getNFTSales"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=30) as response:
                response.raise_for_status()
                data = await response.json()
                sales = [
                    {
                        'transactionHash': sale.get('transactionHash'),
                        'buyer': sale.get('buyerAddress'),
                        'seller': sale.get('sellerAddress'),
                        'tokenId': str(sale.get('tokenId')),
                        'price': int(sale.get('price', {}).get('value', 0)),
                        'blockNumber': int(sale.get('blockNumber', 0)),
                        'timestamp': int(sale.get('timestamp', 0))
                    }
                    for sale in data.get('nftSales', [])
                    if sale.get('buyerAddress') and sale.get('tokenId')
                ]
                logger.info(f"Fetched {len(sales)} NFT sales from Alchemy")
                return sales
    except Exception as e:
        logger.error(f"Failed to fetch NFT sales: {e}")
        recent_errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
        return []

@retry(wait=wait_exponential(multiplier=2, min=4, max=20), stop=stop_after_attempt(3))
async def fetch_bscscan_transactions(startblock: Optional[int] = None, endblock: Optional[int] = None) -> List[Dict]:
    global last_block_number
    try:
        if not startblock and last_block_number:
            startblock = last_block_number + 1
        params = {
            'module': 'account',
            'action': 'txlist',
            'address': Web3.to_checksum_address(CONTRACT_ADDRESS),
            'startblock': startblock or 0,
            'endblock': endblock or 99999999,
            'page': 1,
            'offset': 100,
            'sort': 'desc',
            'apikey': BSCSCAN_API_KEY
        }
        response = requests.get("https://api.bscscan.com/api", params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or data.get('status') != '1':
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
            if tx['to'].lower() == CONTRACT_ADDRESS.lower() and not tx['isError']
        ]
        if transactions:
            last_block_number = max(tx['blockNumber'] for tx in transactions)
        logger.info(f"Fetched {len(transactions)} transactions, last_block_number={last_block_number}")
        return transactions
    except Exception as e:
        logger.error(f"Failed to fetch BscScan transactions: {e}")
        recent_errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
        return []

async def send_nft_media(context: ContextTypes.DEFAULT_TYPE, chat_id: str, metadata: Dict, caption: str, max_retries: int = 3, delay: int = 2) -> bool:
    """Send NFT video or image to Telegram."""
    for i in range(max_retries):
        try:
            logger.info(f"Attempt {i+1}/{max_retries} to send media to chat {chat_id}")
            media_url = metadata.get('animation_url') or metadata.get('image')
            if not media_url:
                logger.error("No media URL available")
                return await send_gif_with_retry(context, chat_id, get_gif_url('NFT'), caption)
            media_data = await download_media(media_url)
            if not media_data:
                logger.error(f"Failed to download media: {media_url}")
                return await send_gif_with_retry(context, chat_id, get_gif_url('NFT'), caption)
            logger.info(f"Sending media message with caption length: {len(caption)}")
            if media_url.endswith('.mp4'):
                await context.bot.send_video(chat_id=chat_id, video=InputFile(media_data), caption=caption, parse_mode='MarkdownV2')
            else:
                await context.bot.send_photo(chat_id=chat_id, photo=InputFile(media_data), caption=caption, parse_mode='MarkdownV2')
            logger.info(f"Successfully sent media to chat {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send media (attempt {i+1}/{max_retries}): {e}")
            if i == max_retries - 1:
                return await send_gif_with_retry(context, chat_id, get_gif_url('NFT'), caption)
            await asyncio.sleep(delay)
    return False

async def send_gif_with_retry(context: ContextTypes.DEFAULT_TYPE, chat_id: str, gif_url: str, caption: str, max_retries: int = 3, delay: int = 2) -> bool:
    for i in range(max_retries):
        try:
            logger.info(f"Attempt {i+1}/{max_retries} to send GIF to chat {chat_id}: {gif_url}")
            async with aiohttp.ClientSession() as session:
                async with session.head(gif_url, timeout=5) as head_response:
                    if head_response.status != 200:
                        logger.error(f"GIF URL inaccessible, status {head_response.status}: {gif_url}")
                        gif_url = FALLBACK_GIF
            logger.info(f"Sending GIF message with caption length: {len(caption)}")
            await context.bot.send_animation(chat_id=chat_id, animation=gif_url, caption=caption, parse_mode='MarkdownV2')
            logger.info(f"Successfully sent GIF to chat {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send GIF (attempt {i+1}/{max_retries}): {e}")
            if i == max_retries - 1:
                await context.bot.send_message(chat_id, f"{caption}\n\n⚠️ GIF unavailable", parse_mode='MarkdownV2')
                return False
            await asyncio.sleep(delay)
    return False

async def process_transaction(context: ContextTypes.DEFAULT_TYPE, transaction: Dict, is_sale: bool, token_id: Optional[str] = None, chat_id: str = TELEGRAM_CHAT_ID) -> bool:
    global posted_transactions, latest_transactions
    try:
        tx_hash = transaction['transactionHash']
        if tx_hash in posted_transactions:
            logger.info(f"Skipping already posted transaction: {tx_hash}")
            return False
        pets_price = get_pets_price()
        bnb_price = get_bnb_price()
        value_wei = int(transaction.get('value', '0')) if transaction.get('value', '0').isdigit() else 0
        bnb_value = value_wei / 1e18
        listing_pets_amount = random.randint(1000000, 5000000)
        listing_usd_value = listing_pets_amount * pets_price
        sale_usd_value = PETS_AMOUNT * pets_price
        wallet_address = transaction.get('buyer', transaction.get('to')) if is_sale else transaction.get('from')
        tx_url = f"https://bscscan.com/tx/{tx_hash}"
        category = 'Sale' if is_sale else 'Listing'
        # Fetch metadata
        if not token_id:
            token_id = await get_token_id_from_tx(tx_hash)
        metadata = metadata_cache.get(token_id, (None, 0))[0] if token_id and metadata_cache.get(token_id, (None, 0))[1] > time.time() else await fetch_nft_metadata(token_id) if token_id else None
        nft_name = metadata.get('name', 'Unknown NFT') if metadata else 'Unknown NFT'
        rarity = next((attr['value'] for attr in metadata.get('attributes', []) if attr['trait_type'] == 'Rarity Ranking'), 'Unknown') if metadata else 'Unknown'
        multiplier = next((attr['value'] for attr in metadata.get('attributes', []) if attr['trait_type'] == 'Staking Multiplier'), 'Unknown') if metadata else 'Unknown'
        emoji_count = min(int(sale_usd_value // 100) if is_sale else 10, 100)
        emojis = '💰' * emoji_count
        if is_listing:
            message = (
                f"🔥 *New 3D NFT New Era Listing* 🔥\n\n"
                f"**NFT:** {nft_name} \\(Rarity: {rarity}, Multiplier: {multiplier}\\)\n\n"
                f"**Listed for:** {listing_pets_amount:,.0f} \\$PETS \\(${listing_usd_value:.2f}\\)\n\n"
                f"Listed by: {shorten_address(wallet_address)}\n\n"
                f"Get it on the Marketplace 🎁\\! Join our Alpha Group for 60s early alerts\\! 👀\n\n\n"
                f"📦 [Marketplace]({MARKETPLACE_LINK}) \\| 📈 [Chart]({CHART_LINK}) \\| 🛍 [Merch]({MERCH_LINK}) \\| 💰 [Buy \\$PETS]({BUY_PETS_LINK})"
            )
            latest_transactions[category][tx_hash] = {
                'wallet': wallet_address,
                'pets_amount': listing_pets_amount,
                'usd_value': listing_usd_value,
                'bnb_value': 0,
                'timestamp': transaction['timeStamp'],
                'token_id': token_id,
                'nft_name': nft_name,
                'rarity': rarity,
                'multiplier': multiplier
            }
        else:
            message = (
                f"🌸 *3D NFT New Era Sold!* 🌸\n\n"
                f"**NFT:** {nft_name} \\(Rarity: {rarity}, Multiplier: {multiplier}\\)\n\n"
                f"{emojis}\n\n"
                f"🔥 **Sold For:** {PETS_AMOUNT:,.0f} \\$PETS\n\n"
                f"💰 **Worth:** \\${sale_usd_value:.2f}\n\n"
                f"💵 BNB Value: {bnb_value:.4f}\n\n"
                f"🦑 Buyer: {shorten_address(wallet_address)}\n"
                f"[🔍 View on BscScan]({tx_url})\n\n\n"
                f"📦 [Marketplace]({MARKETPLACE_LINK}) \\| 📈 [Chart]({CHART_LINK}) \\| 🛍 [Merch]({MERCH_LINK}) \\| 💰 [Buy \\$PETS]({BUY_PETS_LINK})"
            )
            latest_transactions[category][tx_hash] = {
                'wallet': wallet_address,
                'pets_amount': PETS_AMOUNT,
                'usd_value': sale_usd_value,
                'bnb_value': bnb_value,
                'timestamp': transaction['timestamp'] if 'timestamp' in transaction else transaction['timeStamp'],
                'token_id': token_id,
                'nft_name': nft_name,
                'rarity': rarity,
                'multiplier': multiplier
            }
        success = await send_nft_media(context, chat_id, metadata or {}, message)
        if success:
            posted_transactions.add(tx_hash)
            log_posted_transaction(tx_hash)
            logger.info(f"Processed transaction {tx_hash} for chat {chat_id}")
            # Send to additional chat IDs
            for extra_chat_id in [ALPHA_CHAT_ID, MARKET_CHAT_ID]:
                if extra_chat_id and extra_chat_id != chat_id:
                    await send_nft_media(context, extra_chat_id, metadata or {}, message)
            return True
        return False
    except Exception as e:
        logger.error(f"Error processing transaction {tx_hash}: {e}")
        return False

async def monitor_transactions(context: ContextTypes.DEFAULT_TYPE) -> None:
    global last_block_number, is_tracking_enabled, monitoring_task
    logger.info("Starting transaction monitoring")
    while is_tracking_enabled:
        try:
            # Fetch sales from Alchemy
            sales = await fetch_nft_sales(from_block=last_block_number + 1 if last_block_number else None)
            for sale in sorted(sales, key=lambda x: x['blockNumber'], reverse=True):
                if sale['transactionHash'] in posted_transactions:
                    continue
                await process_transaction(context, sale, is_sale=True, token_id=sale['tokenId'])
            # Fetch transactions from BscScan
            transactions = await fetch_bscscan_transactions(startblock=last_block_number + 1 if last_block_number else None)
            for tx in sorted(transactions, key=lambda x: x['blockNumber'], reverse=True):
                if tx['transactionHash'] in posted_transactions:
                    continue
                is_sale = int(tx['value']) > 0
                await process_transaction(context, tx, is_sale=is_sale)
            await asyncio.sleep(POLLING_INTERVAL)
        except asyncio.CancelledError:
            logger.info("Monitoring task cancelled")
            break
        except Exception as e:
            logger.error(f"Error monitoring transactions: {e}")
            recent_errors.append({'time': datetime.now().isoformat(), 'error': str(e)})
            if len(recent_errors) > 5:
                recent_errors.pop(0)
            await asyncio.sleep(POLLING_INTERVAL)
    logger.info("Monitoring task stopped")
    monitoring_task = None

def is_admin(update: Update) -> bool:
    return str(update.effective_user.id) == ADMIN_CHAT_ID

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /start command from user {update.effective_user.id} in chat {chat_id}")
    await context.bot.send_message(chat_id=chat_id, text="👋 Welcome to MicroPets Marketplace Tracker\\! Use /track to start NFT alerts\\.", parse_mode='MarkdownV2')

async def track(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global is_tracking_enabled, monitoring_task
    chat_id = update.effective_chat.id
    logger.info(f"Received /track command from user {update.effective_user.id} in chat {chat_id}")
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized", parse_mode='MarkdownV2')
        return
    if is_tracking_enabled:
        await context.bot.send_message(chat_id=chat_id, text="🚀 Tracking already enabled", parse_mode='MarkdownV2')
        return
    is_tracking_enabled = True
    monitoring_task = asyncio.create_task(monitor_transactions(context))
    await context.bot.send_message(chat_id=chat_id, text="🚖 Tracking started", parse_mode='MarkdownV2')

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global is_tracking_enabled, monitoring_task
    chat_id = update.effective_chat.id
    logger.info(f"Received /stop command from user {update.effective_user.id} in chat {chat_id}")
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized", parse_mode='MarkdownV2')
        return
    is_tracking_enabled = False
    if monitoring_task:
        monitoring_task.cancel()
        try:
            await monitoring_task
        except asyncio.CancelledError:
            logger.info("Monitoring task cancelled")
        monitoring_task = None
    await context.bot.send_message(chat_id=chat_id, text="🛑 Stopped", parse_mode='MarkdownV2')

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /stats command from user {update.effective_user.id} in chat {chat_id}")
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized", parse_mode='MarkdownV2')
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Fetching marketplace stats", parse_mode='MarkdownV2')
    try:
        sales = await fetch_nft_sales()
        transactions = await fetch_bscscan_transactions()
        pets_price = get_pets_price()
        bnb_price = get_bnb_price()
        bsc_sales = [tx for tx in transactions if int(tx['value']) > 0]
        listings = [tx for tx in transactions if int(tx['value']) == 0]
        total_usd = sum(PETS_AMOUNT * pets_price for _ in bsc_sales) + sum(int(sale['price']) / 1e18 * pets_price for sale in sales)
        total_bnb = total_usd / bnb_price if bnb_price > 0 else 0
        latest_sale = max(latest_transactions['Sale'].items(), key=lambda x: x[1]['timestamp'], default=(None, {}))
        latest_listing = max(latest_transactions['Listing'].items(), key=lambda x: x[1]['timestamp'], default=(None, {}))
        sale_details = (
            f"**Latest Sale:**\n\n"
            f"**NFT:** {latest_sale[1].get('nft_name', 'Unknown')} \\(Rarity: {latest_sale[1].get('rarity', 'Unknown')}, Multiplier: {latest_sale[1].get('multiplier', 'Unknown')}\\)\n"
            f"🦑 Buyer: {shorten_address(latest_sale[1].get('wallet', ''))}\n"
            f"🔥 Sold For: {latest_sale[1].get('pets_amount', 0):,.0f} \\$PETS \\(${latest_sale[1].get('usd_value', 0):.2f}\\)\n"
            f"💵 BNB Value: {latest_sale[1].get('bnb_value', 0):.4f}\n\n"
        ) if latest_sale[0] else "No sales yet\n\n"
        listing_details = (
            f"**Latest Listing:**\n\n"
            f"**NFT:** {latest_listing[1].get('nft_name', 'Unknown')} \\(Rarity: {latest_listing[1].get('rarity', 'Unknown')}, Multiplier: {latest_listing[1].get('multiplier', 'Unknown')}\\)\n"
            f"Listed by: {shorten_address(latest_listing[1].get('wallet', ''))}\n"
            f"**Listed for:** {latest_listing[1].get('pets_amount', 0):,.0f} \\$PETS \\(${latest_listing[1].get('usd_value', 0):.2f}\\)\n\n"
        ) if latest_listing[0] else "No listings yet\n\n"
        token_id = latest_sale[1].get('token_id') or latest_listing[1].get('token_id')
        metadata = metadata_cache.get(token_id, (None, 0))[0] if token_id and metadata_cache.get(token_id, (None, 0))[1] > time.time() else await fetch_nft_metadata(token_id) if token_id else None
        message = (
            f"📊 *NFT Marketplace Stats \\(Recent Transactions\\)*\n\n"
            f"🔥 **New Listings:** {len(listings)}\n\n"
            f"🌸 **Sales:** {len(sales) + len(bsc_sales)}\n\n"
            f"💰 **Total \\$PETS:** {(len(bsc_sales) * PETS_AMOUNT + sum(int(sale['price']) / 1e18 for sale in sales)):,.0f} \\(\\$ {total_usd:.2f}/{total_bnb:.3f} BNB\\)\n\n"
            f"{sale_details}"
            f"{listing_details}"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) \\| 📈 [Chart]({CHART_LINK}) \\| 🛍 [Merch]({MERCH_LINK}) \\| 💰 [Buy \\$PETS]({BUY_PETS_LINK})"
        )
        logger.info(f"Sending stats message with length: {len(message)}")
        await send_nft_media(context, chat_id, metadata or {}, message)
    except Exception as e:
        logger.error(f"Error in /stats: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Failed to fetch stats: {str(e)}", parse_mode='MarkdownV2')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /help command from user {update.effective_user.id} in chat {chat_id}")
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"🆘 *Commands:*\n\n"
            f"/start - Start bot\n"
            f"/track - Enable NFT alerts\n"
            f"/stop - Disable alerts\n"
            f"/stats - View marketplace stats\n"
            f"/status - Check tracking status\n"
            f"/test - Test notification\n"
            f"/nov - Test without image\n"
            f"/debug - Debug info\n"
            f"/help - This message\n"
        ),
        parse_mode='MarkdownV2'
    )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /status command from user {update.effective_user.id} in chat {chat_id}")
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 *Status:* {'Enabled' if is_tracking_enabled else 'Disabled'}",
        parse_mode='MarkdownV2'
    )

async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /debug command from user {update.effective_user.id} in chat {chat_id}")
    status = {
        'trackingEnabled': is_tracking_enabled,
        'lastBlockNumber': last_block_number,
        'recentErrors': recent_errors[-5:],
        'apiStatus': {'bscWeb3': bool(w3.is_connected())},
        'botRunning': bot_app.running if bot_app else False,
        'metadataCache': list(metadata_cache.keys())
    }
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🔍 Debug:\n```json\n{json.dumps(status, indent=2)}\n```",
        parse_mode='MarkdownV2'
    )

async def test(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /test command from user {update.effective_user.id} in chat {chat_id}")
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized", parse_mode='MarkdownV2')
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Generating test notifications \\(listing and sale\\)", parse_mode='MarkdownV2')
    try:
        pets_price = get_pets_price()
        bnb_price = get_bnb_price()
        bnb_value = random.uniform(0.01, 0.1)
        listing_pets_amount = random.randint(1000000, 5000000)
        listing_usd_value = listing_pets_amount * pets_price
        sale_usd_value = PETS_AMOUNT * pets_price
        wallet_address = f"0x{'{:040x}'.format(random.randint(0, 2**160))}"
        token_id = '130'  # Use sample token ID for test
        metadata = await fetch_nft_metadata(token_id)
        nft_name = metadata.get('name', 'Test NFT') if metadata else 'Test NFT'
        rarity = next((attr['value'] for attr in metadata.get('attributes', []) if attr['trait_type'] == 'Rarity Ranking'), 'Test') if metadata else 'Test'
        multiplier = next((attr['value'] for attr in metadata.get('attributes', []) if attr['trait_type'] == 'Staking Multiplier'), 'Test') if metadata else 'Test'

        # Test Listing
        test_tx_hash = f"0xTestListing{uuid.uuid4().hex[:16]}"
        listing_message = (
            f"🔥 *New 3D NFT New Era Listing* Test 🔥\n\n"
            f"**NFT:** {nft_name} \\(Rarity: {rarity}, Multiplier: {multiplier}\\)\n\n"
            f"**Listed for:** {listing_pets_amount:,.0f} \\$PETS \\(${listing_usd_value:.2f}\\)\n\n"
            f"Listed by: {shorten_address(wallet_address)}\n\n"
            f"Get it on the Marketplace 🎁\\! Join our Alpha Group for 60s early alerts\\! 👀\n\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) \\| 📈 [Chart]({CHART_LINK}) \\| 🛍 [Merch]({MERCH_LINK}) \\| 💰 [Buy \\$PETS]({BUY_PETS_LINK})"
        )
        success = await send_nft_media(context, chat_id, metadata or {}, listing_message)
        if not success:
            await context.bot.send_message(chat_id=chat_id, text="🚫 Listing test failed: Unable to send notification", parse_mode='MarkdownV2')
            return

        # Test Sale
        test_tx_hash = f"0xTestSale{uuid.uuid4().hex[:16]}"
        emoji_count = min(int(sale_usd_value // 100), 100)
        emojis = '💰' * emoji_count
        sale_message = (
            f"🌸 *3D NFT New Era Sold!* Test 🌸\n\n"
            f"**NFT:** {nft_name} \\(Rarity: {rarity}, Multiplier: {multiplier}\\)\n\n"
            f"{emojis}\n\n"
            f"🔥 **Sold For:** {PETS_AMOUNT:,.0f} \\$PETS\n\n"
            f"💰 **Worth:** \\${sale_usd_value:.2f}\n\n"
            f"💵 BNB Value: {bnb_value:.4f}\n\n"
            f"🦑 Buyer: {shorten_address(wallet_address)}\n"
            f"[🔍 View on BscScan](https://bscscan.com/tx/{test_tx_hash})\n\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) \\| 📈 [Chart]({CHART_LINK}) \\| 🛍 [Merch]({MERCH_LINK}) \\| 💰 [Buy \\$PETS]({BUY_PETS_LINK})"
        )
        success = await send_nft_media(context, chat_id, metadata or {}, sale_message)
        if success:
            await context.bot.send_message(chat_id=chat_id, text="✅ Both tests successful", parse_mode='MarkdownV2')
        else:
            await context.bot.send_message(chat_id=chat_id, text="🚫 Sale test failed: Unable to send notification", parse_mode='MarkdownV2')
    except Exception as e:
        logger.error(f"Test error: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Test failed: {str(e)}", parse_mode='MarkdownV2')

async def no_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    logger.info(f"Received /nov command from user {update.effective_user.id} in chat {chat_id}")
    if not is_admin(update):
        await context.bot.send_message(chat_id=chat_id, text="🚫 Unauthorized", parse_mode='MarkdownV2')
        return
    await context.bot.send_message(chat_id=chat_id, text="⏳ Testing notification without image", parse_mode='MarkdownV2')
    try:
        test_tx_hash = f"0xTestNoV{uuid.uuid4().hex[:16]}"
        pets_price = get_pets_price()
        bnb_price = get_bnb_price()
        bnb_value = random.uniform(0.01, 0.1)
        usd_value = PETS_AMOUNT * pets_price
        wallet_address = f"0x{'{:040x}'.format(random.randint(0, 2**160))}"
        emoji_count = min(int(usd_value // 100), 100)
        emojis = '💰' * emoji_count
        message = (
            f"🌸 *3D NFT New Era Sold!* Test 🌸\n\n"
            f"**NFT:** Test NFT \\(Rarity: Test, Multiplier: Test\\)\n\n"
            f"{emojis}\n\n"
            f"🔥 **Sold For:** {PETS_AMOUNT:,.0f} \\$PETS\n\n"
            f"💰 **Worth:** \\${usd_value:.2f}\n\n"
            f"💵 BNB Value: {bnb_value:.4f}\n\n"
            f"🦑 Buyer: {shorten_address(wallet_address)}\n"
            f"[🔍 View on BscScan](https://bscscan.com/tx/{test_tx_hash})\n\n\n"
            f"📦 [Marketplace]({MARKETPLACE_LINK}) \\| 📈 [Chart]({CHART_LINK}) \\| 🛍 [Merch]({MERCH_LINK}) \\| 💰 [Buy \\$PETS]({BUY_PETS_LINK})"
        )
        logger.info(f"Sending /nov message with length: {len(message)}")
        await context.bot.send_message(chat_id=chat_id, text=message, parse_mode='MarkdownV2')
        await context.bot.send_message(chat_id=chat_id, text="✅ Test successful", parse_mode='MarkdownV2')
    except Exception as e:
        logger.error(f"/nov error: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"🚫 Test failed: {str(e)}", parse_mode='MarkdownV2')

# FastAPI app
app = FastAPI()

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
        raise HTTPException(status_code=503, detail=f"Service unavailable: {str(e)}")

# Lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot_app
    logger.info("Starting bot application")
    try:
        await initialize_last_block_number()
        posted_transactions.update(load_posted_transactions())
        bot_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
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
        logger.info("Initiating bot shutdown...")
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

app = FastAPI(lifespan=lifespan)

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Uvicorn server on port {PORT}")
    try:
        uvicorn.run(app, host="0.0.0.0", port=PORT)
    except Exception as e:
        logger.error(f"Uvicorn startup failed: {e}")
        raise
