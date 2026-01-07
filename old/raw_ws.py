import websocket
import json
from datetime import datetime, timedelta
# from services import insert_ohlc_data_api, insert_spot_ltp_api
import time
import threading

import pytz
ist = pytz.timezone("Asia/Kolkata")

# Divya's credentials
# client_id = '1100449732'
# access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NzYxMzk2LCJpYXQiOjE3Njc2NzQ5OTYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwNDQ5NzMyIn0.xYRXr46pwv7zuGql5BNCf13gQwhpzNKJwD5VoT_XrYjY4CdAoFM5a8a-PQ2RxKwrwoUE4MwL19P6VEFVqoFFFQ"

client_id = '1100465668' #raja sir id
access_token ='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NzU5NjUzLCJpYXQiOjE3Njc2NzMyNTMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwNDY1NjY4In0.VD94mxWH_iI2A0AzfKVI-7OA9QisCEdZqkqrtC5vhe8MMnuLK4Qt5RwIO2c2-gEDMCK24bCGLSVefY2n6HUpsw'


# WebSocket URL (v2)
WS_URL = f"wss://api-feed.dhan.co?version=2&token={access_token}&clientId={client_id}&authType=2"

# Define instruments
INSTRUMENTS = {
    '25': 'BANKNIFTY',
    '13': 'NIFTY50',
    '51': 'SENSEX',
    '27': 'NIFTYFIN',
    '442': 'MIDCAP'
}

# Subscription request format (based on Dhan docs)
SUBSCRIPTION_CODE = 15  # Ticker data
EXCHANGE_SEGMENT = 1    # NSE F&O

# Tracking data
current_candle = {}
current_interval_start = {}
candles = {}
ws = None
is_connected = False


def round_down_time_3min(dt):
    """Round timestamp to 3-minute blocks"""
    minute_block = (dt.minute // 3) * 3
    return dt.replace(minute=minute_block, second=0, microsecond=0)


def on_open(ws):
    """Called when WebSocket connection is established"""
    global is_connected
    is_connected = True
    print("✅ WebSocket Connected!")
    
    # Subscribe to instruments
    subscription_request = {
        "RequestCode": SUBSCRIPTION_CODE,
        "InstrumentCount": len(INSTRUMENTS),
        "InstrumentList": [
            {
                "ExchangeSegment": EXCHANGE_SEGMENT,
                "SecurityId": security_id
            }
            for security_id in INSTRUMENTS.keys()
        ]
    }
    
    print(f"📡 Subscribing to {len(INSTRUMENTS)} instruments...")
    print(f"Subscription: {json.dumps(subscription_request, indent=2)}")
    ws.send(json.dumps(subscription_request))
    print("✅ Subscription sent!")


def on_message(ws, message):
    """Called when message is received from WebSocket"""
    try:
        data = json.loads(message)
        
        # Debug: Print raw message
        print(f"📥 Raw: {data}")
        
        # Check if it's ticker data
        if data.get('type') != 'Ticker Data':
            print(f"ℹ️ Non-ticker message: {data}")
            return
        
        security_id = str(data.get('security_id', ''))
        
        if not security_id or security_id not in INSTRUMENTS:
            print(f"⚠️ Unknown security_id: {security_id}")
            return
        print('data qwe:123',data)
        if 'LTP' not in data or 'LTT' not in data:
            print(f"⚠️ Missing LTP/LTT: {data}")
            return
        
        ltp = float(data['LTP'])
        ltt_time_str = data['LTT']  # "HH:MM:SS"
        instrument_name = INSTRUMENTS[security_id]
        
        # Get current time
        now_ist = datetime.now(ist)
        
        # Check market hours
        start_time = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
        end_time = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
        
        if not (start_time <= now_ist <= end_time):
            return
        
        # Parse tick time
        tick_time = datetime.strptime(ltt_time_str, "%H:%M:%S").time()
        tick_date = now_ist.date()
        ts = datetime.combine(tick_date, tick_time)
        ts = ist.localize(ts) if ts.tzinfo is None else ts.astimezone(ist)
        
        # Insert spot LTP
        try:
            # insert_spot_ltp_api(
            #     token=security_id,
            #     timestamp=ts.isoformat(),
            #     ltp=ltp
            # )
            ''
            print(f"✅ [{instrument_name}] LTP: {ltp:.2f} @ {ltt_time_str}")
        except Exception as e:
            print(f"❌ [{instrument_name}] LTP insert error: {e}")
        
        # Candle processing
        interval_start = round_down_time_3min(ts)
        
        # Initialize tracking
        if security_id not in current_interval_start:
            current_interval_start[security_id] = None
        if security_id not in current_candle:
            current_candle[security_id] = None
        if security_id not in candles:
            candles[security_id] = []
        
        # New candle?
        if (current_interval_start[security_id] is None or
                interval_start > current_interval_start[security_id]):
            
            # Save completed candle
            if current_candle[security_id] is not None:
                completed = current_candle[security_id]
                try:
                    # insert_ohlc_data_api(
                    #     symbol=security_id,
                    #     timeframe='3_MIN',
                    #     timestamp=completed['start_time'],
                    #     open=completed['open'],
                    #     high=completed['high'],
                    #     low=completed['low'],
                    #     close=completed['close'],
                    #     volume=0
                    # )
                    ''
                    print(f"🕯️ [{instrument_name}] Candle: O:{completed['open']:.2f} H:{completed['high']:.2f} L:{completed['low']:.2f} C:{completed['close']:.2f} @ {completed['start_time']}")
                    candles[security_id].append(completed)
                except Exception as e:
                    print(f"❌ [{instrument_name}] Candle insert error: {e}")
            
            # Start new candle
            current_interval_start[security_id] = interval_start
            current_candle[security_id] = {
                'start_time': interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                'open': ltp,
                'high': ltp,
                'low': ltp,
                'close': ltp
            }
            print(f"🆕 [{instrument_name}] New Candle @ {interval_start.strftime('%H:%M:%S')}")
        
        else:
            # Update running candle
            candle = current_candle[security_id]
            candle['high'] = max(candle['high'], ltp)
            candle['low'] = min(candle['low'], ltp)
            candle['close'] = ltp
    
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}, Message: {message}")
    except Exception as e:
        print(f"❌ Error processing message: {e}")


def on_error(ws, error):
    """Called when WebSocket error occurs"""
    global is_connected
    is_connected = False
    print(f"❌ WebSocket Error: {error}")


def on_close(ws, close_status_code, close_msg):
    """Called when WebSocket connection is closed"""
    global is_connected
    is_connected = False
    print(f"🔌 WebSocket Closed: {close_status_code} - {close_msg}")


def connect_websocket():
    """Connect to WebSocket"""
    global ws
    
    print(f"🔌 Connecting to: {WS_URL[:50]}...")
    
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Run WebSocket in a separate thread
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()
    
    return ws


def main():
    """Main loop"""
    print("📊 Dhan Market Feed - Direct WebSocket")
    print(f"📡 Monitoring: {', '.join(INSTRUMENTS.values())}")
    print(f"👤 Client ID: {client_id}")
    print("-" * 60)
    
    retry_count = 0
    max_retries = 10
    
    while retry_count < max_retries:
        try:
            # Connect
            connect_websocket()
            
            # Wait for connection
            time.sleep(3)
            
            if not is_connected:
                print("⚠️ Connection failed, retrying...")
                retry_count += 1
                time.sleep(10 * retry_count)
                continue
            
            # Keep alive
            print("🔄 Market feed running... Press Ctrl+C to stop")
            while is_connected:
                time.sleep(1)
            
            # If disconnected, retry
            print("🔄 Reconnecting...")
            retry_count += 1
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            
            # Save remaining candles
            for security_id, candle in current_candle.items():
                if candle is not None:
                    instrument_name = INSTRUMENTS.get(security_id, security_id)
                    try:
                        # insert_ohlc_data_api(
                        #     symbol=security_id,
                        #     timeframe='3_MIN',
                        #     timestamp=candle['start_time'],
                        #     open=candle['open'],
                        #     high=candle['high'],
                        #     low=candle['low'],
                        #     close=candle['close'],
                        #     volume=0
                        # )
                        ''
                        print(f"💾 [{instrument_name}] Final candle saved")
                    except Exception as e:
                        print(f"❌ [{instrument_name}] Final save error: {e}")
            
            if ws:
                ws.close()
            break
        
        except Exception as e:
            print(f"❌ Main error: {e}")
            retry_count += 1
            time.sleep(10)
    
    print("✅ Shutdown complete")


if __name__ == "__main__":
    main()