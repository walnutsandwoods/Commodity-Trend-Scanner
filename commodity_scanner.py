# commodity_scanner.py
import yfinance as yf
import pandas as pd
import time
import os
import json
import datetime
import pytz
from dotenv import load_dotenv

from alerts import send_telegram_alert_sync, get_telegram_config

load_dotenv()

# Timeframe progression rules - WAIT times between timeframes
TIMEFRAME_WAIT_MINUTES = {
    '5m': {'15m': 10},   # Wait 10 min after 5m detection to check 15m
    '15m': {'30m': 15},  # Wait 15 min after 15m detection to check 30m  
    '30m': {'1h': 30},   # Wait 30 min after 30m detection to check 1h
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, 'commodity_config.json')
STATE_FILE = os.path.join(SCRIPT_DIR, 'commodity_state.json')
ALERTS_FILE = os.path.join(SCRIPT_DIR, 'commodity_alerts.json')
TIMEZONE = pytz.timezone('Asia/Kolkata')

def is_commodity_market_active():
    """Check if commodity markets are active (24/5 with weekend check)"""
    now_utc = datetime.datetime.now(pytz.UTC)
    et_timezone = pytz.timezone('US/Eastern')
    now_et = now_utc.astimezone(et_timezone)
    
    # Markets are active Monday-Friday, nearly 24 hours
    # Only exclude weekends
    if now_et.weekday() < 5:  # Monday 0 - Friday 4
        return True
    
    # For weekends, check if it's Sunday evening (markets open at 6PM ET Sunday)
    if now_et.weekday() == 6:  # Sunday
        if now_et.hour >= 18:  # 6PM ET Sunday onwards
            return True
    
    return False

def get_market_status():
    """Get detailed market status"""
    now_utc = datetime.datetime.now(pytz.UTC)
    et_timezone = pytz.timezone('US/Eastern')
    now_et = now_utc.astimezone(et_timezone)
    now_ist = now_utc.astimezone(pytz.timezone('Asia/Kolkata'))
    
    is_active = is_commodity_market_active()
    status = "🟢 ACTIVE" if is_active else "🔴 CLOSED"
    
    info = f"""
📊 Market Status: {status}
⏰ US Eastern Time: {now_et.strftime('%Y-%m-%d %H:%M:%S')}
⏰ Indian Standard Time: {now_ist.strftime('%Y-%m-%d %H:%M:%S')}
📅 Day: {now_et.strftime('%A')}
🎯 Scanning: {'YES' if is_active else 'NO'}
"""
    return info, is_active

def initialize_files():
    """Initialize data files if they don't exist"""
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'w') as f:
            json.dump({}, f)
    
    if not os.path.exists(ALERTS_FILE):
        with open(ALERTS_FILE, 'w') as f:
            json.dump([], f)

def load_config():
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        # Default config if file is missing
        return {
            "commodities": [
                {"symbol": "GC=F", "name": "Gold"},
                {"symbol": "SI=F", "name": "Silver"}, 
                {"symbol": "NG=F", "name": "Natural Gas"},
                {"symbol": "CL=F", "name": "Crude Oil"}
            ],
            "timeframes": ["5m", "10m", "15m", "30m", "1h"],
            "ema_periods": [9, 21],
            "scan_interval": 300,
            "min_volume": 1000
        }

def load_state():
    try:
        if os.path.exists(STATE_FILE) and os.path.getsize(STATE_FILE) > 0:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Error loading state: {e}")
        return {}

def save_state(state):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"Error saving state: {e}")

def log_alert(alert_data):
    try:
        if os.path.exists(ALERTS_FILE) and os.path.getsize(ALERTS_FILE) > 0:
            with open(ALERTS_FILE, 'r') as f:
                alerts = json.load(f)
        else:
            alerts = []
        
        alerts.append(alert_data)
        
        with open(ALERTS_FILE, 'w') as f:
            json.dump(alerts, f, indent=2)
    except Exception as e:
        print(f"Error logging alert: {e}")

def check_ema_crossover(df, ema_fast=9, ema_slow=21):
    """Check for EMA crossover and return trend direction"""
    if len(df) < ema_slow + 1:
        return None
    
    try:
        df = df.copy()
        df['EMA_Fast'] = df['Close'].ewm(span=ema_fast, adjust=False).mean()
        df['EMA_Slow'] = df['Close'].ewm(span=ema_slow, adjust=False).mean()
        
        # Get current and previous values (convert to float)
        curr_fast = float(df['EMA_Fast'].iloc[-1])
        curr_slow = float(df['EMA_Slow'].iloc[-1])
        prev_fast = float(df['EMA_Fast'].iloc[-2])
        prev_slow = float(df['EMA_Slow'].iloc[-2])
        
        print(f"    EMA Comparison: Fast={round(curr_fast, 2)} vs Slow={round(curr_slow, 2)}")
        
        # Check for bullish crossover
        if prev_fast <= prev_slow and curr_fast > curr_slow:
            print("    🟢 BULLISH CROSSOVER DETECTED!")
            return "bullish"
        # Check for bearish crossover
        elif prev_fast >= prev_slow and curr_fast < curr_slow:
            print("    🔴 BEARISH CROSSOVER DETECTED!")
            return "bearish"
        
        print("    ⚪ No crossover (trend continues)")
        return None
        
    except Exception as e:
        print(f"Error in EMA calculation: {e}")
        return None

def analyze_commodity(commodity, timeframe):
    """Analyze single commodity on specific timeframe"""
    try:
        # Yahoo Finance supported intervals
        supported_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '1h', '1d']
        if timeframe not in supported_intervals:
            print(f"  ❌ Timeframe {timeframe} not supported by Yahoo Finance")
            return None
            
        # Download data - using 5 days for shorter timeframes
        days_needed = 5 if timeframe in ['1m', '2m', '5m', '15m'] else 10
        print(f"  Downloading {commodity['name']} ({timeframe})...")
        
        data = yf.download(commodity['symbol'], period=f"{days_needed}d", interval=timeframe, progress=False)
        
        if data.empty or len(data) < 22:  # Need enough data for EMAs
            print(f"  ❌ Insufficient data for {commodity['name']} on {timeframe}")
            return None
            
        # Show latest price
        latest_price = data['Close'].iloc[-1] if 'Close' in data else 'N/A'
        print(f"  Latest price: {latest_price}")
            
        trend = check_ema_crossover(data)
        print(f"  Trend detected: {trend}")
        return trend
        
    except Exception as e:
        print(f"❌ Error analyzing {commodity['name']} on {timeframe}: {e}")
        return None

def scan_commodities():
    market_status, is_active = get_market_status()
    print(market_status)
    
    if not is_active:
        print("⏸️ Markets closed - skipping scan")
        return []
    
    print(f"🔍 Scanning commodities at {datetime.datetime.now(tz=TIMEZONE)}")
    
    config = load_config()
    state = load_state()
    current_time = str(datetime.datetime.now(tz=TIMEZONE))
    
    alerts = []
    
    for commodity in config['commodities']:
        symbol = commodity['symbol']
        name = commodity['name']
        
        print(f"Analyzing {name}...")
        
        # Check for existing active trend
        active_trend_key = None
        active_trend_data = None
        
        for trend_type in ['bullish', 'bearish']:
            trend_key = f"{symbol}_{trend_type}"
            if trend_key in state:
                active_trend_key = trend_key
                active_trend_data = state[trend_key]
                print(f"  Found active {trend_type} trend up to {active_trend_data['max_timeframe']}")
                break
        
        # Determine which timeframes to scan
        timeframes_to_scan = []
        
        if not active_trend_key:
            # No active trend - only scan 5m for new trends
            timeframes_to_scan = ['5m']
            print("  No active trend - scanning 5m only for new trends")
        else:
            # Active trend exists - check if we should scan higher timeframe
            current_tf = active_trend_data['max_timeframe']
            current_index = config['timeframes'].index(current_tf)
            
            if current_index < len(config['timeframes']) - 1:
                next_tf = config['timeframes'][current_index + 1]
                
                # Check if enough time has passed
                detection_time = datetime.datetime.fromisoformat(active_trend_data['first_detected'].replace('Z', '+00:00'))
                time_passed = (datetime.datetime.now(detection_time.tzinfo) - detection_time).total_seconds() / 60
                
                wait_minutes = TIMEFRAME_WAIT_MINUTES.get(current_tf, {}).get(next_tf, 10)
                
                if time_passed >= wait_minutes:
                    timeframes_to_scan = [current_tf, next_tf]  # Check current + next
                    print(f"  Time passed: {time_passed:.1f}min, waiting: {wait_minutes}min - scanning {current_tf} and {next_tf}")
                else:
                    timeframes_to_scan = [current_tf]  # Only check current to confirm
                    print(f"  Time passed: {time_passed:.1f}min, waiting: {wait_minutes}min - scanning {current_tf} only")
            else:
                # Already at highest timeframe
                timeframes_to_scan = [current_tf]
                print(f"  At highest timeframe {current_tf} - scanning for confirmation")
        
        # Scan only the determined timeframes
        for timeframe in timeframes_to_scan:
            print(f"  Scanning {timeframe}...")
            trend = analyze_commodity(commodity, timeframe)
            
            if trend:
                if not active_trend_key:
                    # NEW TREND DETECTED
                    alert_msg = f"🎯 NEW {trend.upper()} CROSSOVER: {name} on {timeframe}"
                    alerts.append(alert_msg)
                    print(f"  {alert_msg}")
                    
                    # Save to state
                    state_key = f"{symbol}_{trend}"
                    state[state_key] = {
                        'first_detected': current_time,
                        'max_timeframe': timeframe,
                        'trend_strength': 1,
                        'last_updated': current_time
                    }
                    
                else:
                    # Existing trend - check if progressing
                    current_tf = active_trend_data['max_timeframe']
                    current_index = config['timeframes'].index(current_tf)
                    new_index = config['timeframes'].index(timeframe)
                    
                    if new_index > current_index:
                        # Progressed to higher timeframe
                        alert_msg = f"🚀 {name} {trend.upper()} trend progressing to {timeframe}"
                        alerts.append(alert_msg)
                        print(f"  {alert_msg}")
                        
                        # Update state
                        state[active_trend_key]['max_timeframe'] = timeframe
                        state[active_trend_key]['last_updated'] = current_time
                    
                    else:
                        # No trend detected
                        if active_trend_key and timeframe == active_trend_data['max_timeframe']:
                            # Trend faded at current level
                            # FIX: Use active_trend_key.split() instead of active_trend_data.split()
                            trend_type = active_trend_key.split('_')[1]  # Extract 'bullish' or 'bearish'
                            alert_msg = f"⚠️ {name} {trend_type} trend faded at {timeframe}"
                            alerts.append(alert_msg)
                            print(f"  {alert_msg}")
                            
                            # Remove from state
                            del state[active_trend_key]
    
    save_state(state)
    
    # Send alerts
    for alert in alerts:
        print(f"Alert: {alert}")
        send_telegram_alert_sync(alert)
        log_alert({
            'timestamp': current_time,
            'message': alert
        })
    
    if not alerts:
        print("No commodity trend alerts.")
    
    return alerts

def main():
    bot_token, chat_id = get_telegram_config()
    if not bot_token or not chat_id:
        print("Telegram configuration failed. Check .env file.")
        return
    
    # Initialize data files
    initialize_files()
    
    config = load_config()
    
    send_telegram_alert_sync("🔄 Commodity Progressive Trend Scanner Started - 24/5 Mode")
    print("Commodity Scanner Started. Monitoring: Gold, Silver, Natural Gas, Crude Oil")
    print("🕒 Scanning Mode: 24/5 (Continuous during weekdays)")
    
    while True:
        try:
            market_status, is_active = get_market_status()
            print(market_status)
            
            if is_active:
                print("🟢 Markets ACTIVE - Scanning...")
                scan_commodities()
                sleep_time = config.get('scan_interval', 300)  # 5 minutes during active
            else:
                print("🔴 Markets CLOSED (Weekend) - Sleeping...")
                sleep_time = 1800  # 30 minutes during weekends
            
            print(f"💤 Sleeping for {sleep_time//60} minutes...")
            time.sleep(sleep_time)
                
        except Exception as e:
            print(f"Commodity scanner error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()