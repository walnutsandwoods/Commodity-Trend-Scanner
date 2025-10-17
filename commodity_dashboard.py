# commodity_dashboard.py
import streamlit as st
import json
import os
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Commodity Trend Scanner", layout="wide")
st.title("🛢️ Commodity Progressive Trend Scanner")
st.write("Dashboard is working! Data files will appear when scanner runs.")

COMMODITY_HIST_FILE = 'commodity_alerts.json'
COMMODITY_STATE_FILE = 'commodity_state.json'

if os.path.exists(COMMODITY_HIST_FILE):
    with open(COMMODITY_HIST_FILE, 'r') as f:
        alerts = json.load(f)
    
    if alerts:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        total_alerts = len(alerts)
        bullish_alerts = sum(1 for a in alerts if "BULLISH" in a['message'])
        bearish_alerts = sum(1 for a in alerts if "BEARISH" in a['message'])
        today_alerts = sum(1 for a in alerts if datetime.now().strftime('%Y-%m-%d') in a['timestamp'])
        
        with col1:
            st.metric("Total Alerts", total_alerts)
        with col2:
            st.metric("Bullish Signals", bullish_alerts)
        with col3:
            st.metric("Bearish Signals", bearish_alerts)
        with col4:
            st.metric("Today's Alerts", today_alerts)
        
        # Latest alerts
        st.subheader("🔔 Recent Alerts")
        recent_alerts = alerts[-15:]  # Last 15 alerts
        
        for alert in reversed(recent_alerts):
            timestamp = alert['timestamp']
            message = alert['message']
            
            # Color-coded alerts
            if "NEW BULLISH" in message:
                st.success(f"**🟢 {timestamp}** - {message}")
            elif "BULLISH trend progressing" in message:
                st.info(f"**🔷 {timestamp}** - {message}")
            elif "NEW BEARISH" in message:
                st.error(f"**🔴 {timestamp}** - {message}")
            elif "BEARISH trend progressing" in message:
                st.warning(f"**🟠 {timestamp}** - {message}")
            elif "faded" in message:
                st.warning(f"**⚫ {timestamp}** - {message}")
            else:
                st.write(f"**⚪ {timestamp}** - {message}")
        
        # Active trends with progress bars
        if os.path.exists(COMMODITY_STATE_FILE):
            with open(COMMODITY_STATE_FILE, 'r') as f:
                state = json.load(f)
            
            if state:
                st.subheader("📈 Active Progressive Trends")
                
                timeframes = ["5m", "10m", "15m", "30m", "1h"]
                
                for key, data in state.items():
                    commodity_symbol = key.split('_')[0]
                    trend_type = key.split('_')[1]
                    current_tf = data['max_timeframe']
                    current_index = timeframes.index(current_tf)
                    
                    # Progress visualization
                    progress = (current_index + 1) / len(timeframes)
                    
                    if trend_type == 'bullish':
                        st.success(f"""
                        **{commodity_symbol} - 🟢 BULLISH Trend**
                        📊 Progress: {current_tf} ({progress:.0%})
                        🕒 Started: {data['first_detected']}
                        """)
                    else:
                        st.error(f"""
                        **{commodity_symbol} - 🔴 BEARISH Trend**
                        📊 Progress: {current_tf} ({progress:.0%})
                        🕒 Started: {data['first_detected']}
                        """)
    else:
        st.info("No commodity alerts recorded yet.")
else:
    st.warning("Commodity scanner data not found. Start the commodity scanner first.")

if st.button("🔄 Refresh"):
    st.rerun()

st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")