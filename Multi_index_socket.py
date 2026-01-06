from dhanhq import DhanContext, MarketFeed
import json
from datetime import date, datetime, timedelta
from services import insert_ohlc_data_csv, insert_ohlc_data_api, insert_spot_ltp_api
import time

import pytz
ist = pytz.timezone("Asia/Kolkata")

client_id = '1100465668'
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NzU5NjUzLCJpYXQiOjE3Njc2NzMyNTMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwNDY1NjY4In0.VD94mxWH_iI2A0AzfKVI-7OA9QisCEdZqkqrtC5vhe8MMnuLK4Qt5RwIO2c2-gEDMCK24bCGLSVefY2n6HUpsw"

dhan_context = DhanContext(client_id, access_token)

# Define instruments with readable names
instruments = [
    (MarketFeed.NSE_FNO, 25, MarketFeed.Ticker),   # Bank Nifty
    (MarketFeed.NSE_FNO, 13, MarketFeed.Ticker),   # Nifty 50
    (MarketFeed.NSE_FNO, 51, MarketFeed.Ticker),   # Sensex
    (MarketFeed.NSE_FNO, 27, MarketFeed.Ticker),   # Nifty Fin
    (MarketFeed.NSE_FNO, 442, MarketFeed.Ticker),  # Midcap Nifty
]

# Instrument names for logging
INSTRUMENT_NAMES = {
    '25': 'BANKNIFTY',
    '13': 'NIFTY50',
    '51': 'SENSEX',
    '27': 'NIFTYFIN',
    '442': 'MIDCAP'
}

version = "v2"


def round_down_time_3min(dt):
    """Round timestamp to 3-minute blocks"""
    minute_block = (dt.minute // 3) * 3
    return dt.replace(minute=minute_block, second=0, microsecond=0)


# Separate tracking for each instrument
current_candle = {}
current_interval_start = {}
candles = {}

print(f"📊 Monitoring {len(instruments)} instruments: {', '.join(INSTRUMENT_NAMES.values())}")

retry_delay = 5
max_retry_delay = 60
retry_count = 0


while True:
    try:
        data = MarketFeed(dhan_context, instruments, version)
        print("🚀 Starting Market Feed...")

        retry_delay = 5
        retry_count = 0

        while True:
            now_ist = datetime.now(ist)
            start_time = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
            end_time = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

            if not (start_time <= now_ist <= end_time):
                print(f"⏰ Waiting for Market Hours (9:15 AM – 3:30 PM)... Current time: {now_ist.strftime('%H:%M:%S')}")
                time.sleep(60)
                continue

            data.run_forever()
            
            # Process all available ticks in buffer
            ticks_processed = 0
            while True:
                try:
                    response = data.get_data()
                    
                    # No more data in buffer
                    if response is None:
                        break
                    
                    # Extract security_id from response
                    security_id = str(response.get('security_id', ''))
                    
                    if not security_id:
                        print(f"⚠️ Missing security_id in response: {response}")
                        continue
                    
                    if 'LTP' not in response or 'LTT' not in response:
                        print(f"⚠️ Missing LTP/LTT for {INSTRUMENT_NAMES.get(security_id, security_id)}: {response}")
                        continue

                    ltp = float(response['LTP'])
                    ltt_time_str = response['LTT']  # "HH:MM:SS"
                    
                    instrument_name = INSTRUMENT_NAMES.get(security_id, security_id)
                    ticks_processed += 1

                    # Parse tick time
                    tick_time = datetime.strptime(ltt_time_str, "%H:%M:%S").time()
                    tick_date = now_ist.date()
                    ts = datetime.combine(tick_date, tick_time)
                    ts = ist.localize(ts) if ts.tzinfo is None else ts.astimezone(ist)

                    # Insert spot LTP for this instrument
                    try:
                        insert_spot_ltp_api(
                            token=security_id,
                            timestamp=ts.isoformat(),
                            ltp=ltp
                        )
                        print(f"✅ [{instrument_name}] LTP: {ltp:.2f} @ {ltt_time_str}")
                    except Exception as e:
                        print(f"❌ [{instrument_name}] Spot LTP insert error: {e}")

                    # Determine the candle interval
                    interval_start = round_down_time_3min(ts)

                    # Initialize tracking for this instrument if needed
                    if security_id not in current_interval_start:
                        current_interval_start[security_id] = None
                    if security_id not in current_candle:
                        current_candle[security_id] = None
                    if security_id not in candles:
                        candles[security_id] = []

                    # Check if we need to start a new candle
                    if (current_interval_start[security_id] is None or
                            interval_start > current_interval_start[security_id]):

                        # Save the completed candle to database
                        if current_candle[security_id] is not None:
                            completed = current_candle[security_id]
                            try:
                                insert_ohlc_data_api(
                                    symbol=security_id,
                                    timeframe='3_MIN',
                                    timestamp=completed['start_time'],
                                    open=completed['open'],
                                    high=completed['high'],
                                    low=completed['low'],
                                    close=completed['close'],
                                    volume=0
                                )
                                print(f"🕯️ [{instrument_name}] Candle Saved: O:{completed['open']:.2f} H:{completed['high']:.2f} L:{completed['low']:.2f} C:{completed['close']:.2f} @ {completed['start_time']}")
                                
                                # Store in memory
                                candles[security_id].append(completed)

                            except Exception as e:
                                print(f"❌ [{instrument_name}] Insert DB Error: {e}")

                        # Start a new candle for this instrument
                        current_interval_start[security_id] = interval_start
                        current_candle[security_id] = {
                            'start_time': interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                            'open': ltp,
                            'high': ltp,
                            'low': ltp,
                            'close': ltp
                        }
                        print(f"🆕 [{instrument_name}] New Candle Started @ {interval_start.strftime('%H:%M:%S')}")

                    else:
                        # Update the current running candle
                        candle = current_candle[security_id]
                        candle['high'] = max(candle['high'], ltp)
                        candle['low'] = min(candle['low'], ltp)
                        candle['close'] = ltp

                except Exception as e:
                    print(f"⚠️ Error processing tick: {e}")
                    continue
            
            # Log batch processing info
            if ticks_processed > 0:
                print(f"📦 Processed {ticks_processed} ticks in this batch")
            
            # Small delay before next batch
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
        
        # Save any remaining candles
        for security_id, candle in current_candle.items():
            if candle is not None:
                instrument_name = INSTRUMENT_NAMES.get(security_id, security_id)
                try:
                    insert_ohlc_data_api(
                        symbol=security_id,
                        timeframe='3_MIN',
                        timestamp=candle['start_time'],
                        open=candle['open'],
                        high=candle['high'],
                        low=candle['low'],
                        close=candle['close'],
                        volume=0
                    )
                    print(f"💾 [{instrument_name}] Final Candle Saved")
                except Exception as e:
                    print(f"❌ [{instrument_name}] Final candle save error: {e}")
        
        break

    except Exception as e:
        retry_count += 1
        print(f"❌ Main Loop Error: {e}")
        print(f"⏳ Retry {retry_count}: Waiting {retry_delay} seconds...")

        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay)
        continue