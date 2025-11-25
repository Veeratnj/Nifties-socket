from dhanhq import DhanContext, MarketFeed
import json
from datetime import date, datetime, timedelta
from services import insert_ohlc_data_csv, insert_ohlc_data_api, insert_spot_ltp_api
import time

import pytz
ist = pytz.timezone("Asia/Kolkata")


client_id='1100465668'
access_token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY0MDg3OTY4LCJpYXQiOjE3NjQwMDE1NjgsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwNDY1NjY4In0.Q0ybjom2j6mFlKtsTDuTB7ZUK-2wFweaivfUvX8h9h0t-S2XspT2XfBntTYfdcaDYCIpZkYDKolgL2eaDBsclQ"

dhan_context = DhanContext(client_id, access_token)

security_id = '25'
instruments = [
    (MarketFeed.IDX, security_id, MarketFeed.Ticker),
]

version = "v2"

def round_down_time_3min(dt):
    """Round down timestamp to 3-minute intervals (0, 3, 6, 9, 12... minutes)"""
    minute_block = (dt.minute // 3) * 3
    return dt.replace(minute=minute_block, second=0, microsecond=0)

current_candle = {}
current_interval_start = {}
candles = {}
print(instruments)

# Retry configuration
retry_delay = 5  # Initial retry delay in seconds
max_retry_delay = 60  # Maximum retry delay in seconds
retry_count = 0

while True:
    try:
        data = MarketFeed(dhan_context, instruments, version)
        print("Starting Market Feed...")
        
        # Reset retry delay on successful connection
        retry_delay = 5
        retry_count = 0
        
        while True:
            now = datetime.now()
            start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
            end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
            print(now)
            if not(start_time <= now <= end_time):
                print("📈 Market hours (9:15 AM - 3:30 PM). Waiting...")
                time.sleep(60)  # Wait 1 minute before checking again
                continue
            data.run_forever()
            # print("Market Feed running...")
            # print(dir(data.get_data))
            response = data.get_data()
            print(f"Response: {response}")
            stock_name = 'BANK_NIFTY'

            if 'LTP' in response and 'LTT' in response:
                ltp = response['LTP']
                ts_epoch = response['LTT']
                with open("condition_log.txt", "a") as f:
                    f.write(f"{str(response)}\n")

                # Ensure dicts are initialized
                if security_id not in current_interval_start:
                    current_interval_start[security_id] = None
                if security_id not in current_candle:
                    current_candle[security_id] = None
                if security_id not in candles:
                    candles[security_id] = []

                # Convert LTT ("12:03:50") to IST datetime
                time_obj = datetime.strptime(ts_epoch, "%H:%M:%S").time()
                ts = ist.localize(datetime.combine(date.today(), time_obj))

                # Insert spot LTP data
                try:
                    insert_spot_ltp_api(
                        token=security_id,
                        timestamp=ts.isoformat(),
                        ltp=ltp
                    )
                except Exception as e:
                    print(f"Spot LTP insert error: {e}")

                interval_start = round_down_time_3min(ts)
                print(f"Timestamp: {ts}, Interval start: {interval_start}")

                # Create new candle if new interval
                if current_interval_start[security_id] is None or interval_start > current_interval_start[security_id]:
                    if current_candle[security_id] is not None:
                        candles[security_id].append(current_candle[security_id])
                        completed_candle = current_candle[security_id]
                        try:
                            insert_ohlc_data_api(
                                symbol=security_id,
                                timeframe='3_MIN',
                                timestamp=completed_candle['start_time'],
                                open=completed_candle['open'],
                                high=completed_candle['high'],
                                low=completed_candle['low'],
                                close=completed_candle['close'],
                                volume=0  # Volume not tracked in current implementation
                            )
                            print(f"Inserted candle: {completed_candle}")
                        except Exception as e:
                            print(f"Insert DB error: {e}")

                    current_interval_start[security_id] = interval_start
                    current_candle[security_id] = {
                        'start_time': interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                        'open': ltp,
                        'high': ltp,
                        'low': ltp,
                        'close': ltp
                    }
                else:
                    # Update current candle
                    candle = current_candle[security_id]
                    candle['high'] = max(candle['high'], ltp)
                    candle['low'] = min(candle['low'], ltp)
                    candle['close'] = ltp
            else:
                print(f"Missing data for token {security_id}, response: {response}")
                continue

    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
        break
    except Exception as e:
        retry_count += 1
        error_msg = str(e)
        
        # Check if it's a rate limiting error
        if "429" in error_msg or "Too Many Requests" in error_msg:
            print(f"⚠️  Rate limit hit (HTTP 429). Retry #{retry_count}")
        else:
            print(f"❌ Main loop error: {e}")
        
        # Exponential backoff: double the delay each time, up to max
        print(f"⏳ Waiting {retry_delay} seconds before retry...")
        time.sleep(retry_delay)
        
        # Increase delay for next retry (exponential backoff)
        retry_delay = min(retry_delay * 2, max_retry_delay)
        
        continue
