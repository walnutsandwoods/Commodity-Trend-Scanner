# multi_ema_scanner.py
import yfinance as yf
import pandas as pd
import time  # Module for sleep
import os
import json
import asyncio
import datetime  # Import the whole datetime module
import pytz
from dotenv import load_dotenv

from alerts import send_telegram_alert_sync, get_telegram_config

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STOCK_LIST_FILE = os.path.join(SCRIPT_DIR, 'nifty500.txt')
STATE_FILE = os.path.join(SCRIPT_DIR, 'scanner_state.json')
RESULTS_FILE = os.path.join(SCRIPT_DIR, 'scan_results.json')
HISTORICAL_FILE = os.path.join(SCRIPT_DIR, 'historical_alerts.json')
TIMEZONE = pytz.timezone('Asia/Kolkata')
MARKET_START = datetime.time(9, 15)
MARKET_END = datetime.time(15, 30)
SCAN_INTERVAL_MINUTES = 15
BATCH_SIZE = 100
USE_MACD = True
MIN_PRICE = 100
VOL_MULTIPLIER = 1.5

def read_stock_symbols():
    try:
        with open(STOCK_LIST_FILE, 'r', encoding='utf-8') as f:
            symbols = [s.strip() + ".NS" for s in f.read().split(',') if s.strip()]
        return symbols
    except FileNotFoundError:
        print(f"Error: {STOCK_LIST_FILE} not found.")
        return []

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f)

def log_results(results):
    try:
        with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(results, f)
        if os.path.exists(HISTORICAL_FILE):
            with open(HISTORICAL_FILE, 'r', encoding='utf-8') as f:
                hist = json.load(f)
        else:
            hist = []
        hist.append(results)
        with open(HISTORICAL_FILE, 'w', encoding='utf-8') as f:
            json.dump(hist, f)
    except Exception as e:
        print(f"Error logging results: {e}")

def scan_symbols(symbols):
    print(f"Scanning at {datetime.datetime.now(tz=TIMEZONE)}")
    state = load_state()
    new_bullish = []
    new_bearish = []
    strengthening = []
    total = len(symbols)
    
    for i in range(0, total, BATCH_SIZE):
        batch = symbols[i:i + BATCH_SIZE]
        try:
            data = yf.download(batch, period="2mo", interval="1h", progress=False, auto_adjust=True)
            if data.empty:
                print(f"No data for batch {i//BATCH_SIZE + 1}")
                continue
            for symbol in batch:
                try:
                    df = data.xs(symbol, level=1, axis=1) if len(batch) > 1 else data
                    close = df['Close'].dropna()
                    if len(close) < 56: continue
                    latest_price = close.iloc[-1]
                    if latest_price < MIN_PRICE: continue

                    ema9 = close.ewm(span=9, adjust=False).mean()
                    ema21 = close.ewm(span=21, adjust=False).mean()
                    ema55 = close.ewm(span=55, adjust=False).mean()
                    prev9, curr9 = ema9.iloc[-2], ema9.iloc[-1]
                    prev21, curr21 = ema21.iloc[-2], ema21.iloc[-1]
                    curr55 = ema55.iloc[-1]

                    macd_line = 0
                    if USE_MACD:
                        ema12 = close.ewm(span=12, adjust=False).mean()
                        ema26 = close.ewm(span=26, adjust=False).mean()
                        macd_line = ema12.iloc[-1] - ema26.iloc[-1]

                    # Volume as confirming factor (not requirement)
                    vol_surge = False
                    vol_above_avg = False
                    vol_info = ""
                    
                    vol = df['Volume'].dropna()
                    if len(vol) >= 23:
                        last3_avg = vol.iloc[-3:].mean()
                        prev20_avg = vol.iloc[-23:-3].mean()
                        vol_surge = last3_avg > (1.5 * prev20_avg)
                        vol_above_avg = last3_avg > prev20_avg
                        vol_info = " 📈" if vol_surge else " ↗️" if vol_above_avg else ""

                    open_price = df['Open'].iloc[-1]
                    stock_name = symbol.replace('.NS', '')
                    curr_spread = abs(curr9 - curr21) / curr21

                    # MAIN CONDITIONS (Volume removed from mandatory)
                    if (prev9 <= prev21 and curr9 > curr21 and curr21 > curr55 and 
                        open_price > curr9 and (not USE_MACD or macd_line > 0)):
                        
                        signal_strength = "STRONG" if vol_surge else "MODERATE" if vol_above_avg else "WEAK"
                        
                        if stock_name not in state or state[stock_name]['type'] != 'bullish':
                            new_bullish.append(f"{stock_name} ({signal_strength}{vol_info})")
                            state[stock_name] = {'type': 'bullish', 'first_price': latest_price, 
                                               'scan_count': 1, 'last_spread': curr_spread, 
                                               'timestamp': str(datetime.datetime.now(tz=TIMEZONE))}
                        else:
                            prev_price = state[stock_name]['first_price']
                            prev_spread = state[stock_name]['last_spread']
                            if latest_price > prev_price * 1.02 and curr_spread > prev_spread * 1.1:
                                strengthening.append(f"{stock_name} (up {(latest_price/prev_price-1)*100:.1f}%, div widening)")
                            state[stock_name]['scan_count'] += 1
                            state[stock_name]['last_spread'] = curr_spread

                    elif (prev9 >= prev21 and curr9 < curr21 and curr21 < curr55 and 
                          open_price < curr9 and (not USE_MACD or macd_line < 0)):
                          
                        signal_strength = "STRONG" if vol_surge else "MODERATE" if vol_above_avg else "WEAK"
                        
                        if stock_name not in state or state[stock_name]['type'] != 'bearish':
                            new_bearish.append(f"{stock_name} ({signal_strength}{vol_info})")
                            state[stock_name] = {'type': 'bearish', 'first_price': latest_price, 
                                               'scan_count': 1, 'last_spread': curr_spread, 
                                               'timestamp': str(datetime.datetime.now(tz=TIMEZONE))}
                        else:
                            prev_price = state[stock_name]['first_price']
                            prev_spread = state[stock_name]['last_spread']
                            if latest_price < prev_price * 0.98 and curr_spread > prev_spread * 1.1:
                                strengthening.append(f"{stock_name} (down {(1-latest_price/prev_price)*100:.1f}%, div widening)")
                            state[stock_name]['scan_count'] += 1
                            state[stock_name]['last_spread'] = curr_spread

                except Exception as e:
                    print(f"Error processing {symbol}: {e}")
        except Exception as e:
            print(f"Error downloading data for batch {i//BATCH_SIZE + 1}: {e}")
    
    save_state(state)

    messages = []
    if new_bullish:
        messages.append("📈 New Bullish EMA Alignment:\n" + "\n".join(f"• {s}" for s in new_bullish))
    if new_bearish:
        messages.append("📉 New Bearish EMA Alignment:\n" + "\n".join(f"• {s}" for s in new_bearish))
    if strengthening:
        messages.append("🚀 Strengthening Momentum:\n" + "\n".join(f"• {s}" for s in strengthening))

    for msg in messages:
        send_telegram_alert_sync(msg)

    results = {'timestamp': str(datetime.datetime.now(tz=TIMEZONE)), 'new_bullish': new_bullish,
               'new_bearish': new_bearish, 'strengthening': strengthening}
    log_results(results)

    if not messages:
        print("No signals.")

def main():
    bot_token, chat_id = get_telegram_config()
    if not bot_token or not chat_id:
        print("Telegram configuration failed. Check .env file.")
        return
    symbols = read_stock_symbols()
    if not symbols:
        return
    send_telegram_alert_sync("🔄 MultiEMA Scanner Started - 9/21/55 + Vol + MACD on 1h")
    while True:
        try:
            now = datetime.datetime.now(tz=TIMEZONE)  # Fixed: datetime.datetime.now
            if now.weekday() < 5 and MARKET_START <= now.time() <= MARKET_END:
                scan_symbols(symbols)
            else:
                print(f"Outside market hours: {now}")
            time.sleep(SCAN_INTERVAL_MINUTES * 60)
        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()