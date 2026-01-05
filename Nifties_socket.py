from dhanhq import DhanContext, MarketFeed
import json
from datetime import date, datetime, timedelta
from services import insert_ohlc_data_csv, insert_ohlc_data_api, insert_spot_ltp_api
import time

import pytz
ist = pytz.timezone("Asia/Kolkata")

client_id = '1100465668'
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NjY1MTU5LCJpYXQiOjE3Njc1Nzg3NTksInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwNDY1NjY4In0.mqIdNumndRSjgedlS_hojTzqeA-tgRN7ldKlbQhUF-eeEnZgmnbceimjT9LkcWC1LdY_-3doU-iJgrFGBtrPKQ"

dhan_context = DhanContext(client_id, access_token)

security_id = '25'  # BANKNIFTY Spot ID
instruments = [
    (MarketFeed.NSE_FNO, security_id, MarketFeed.Ticker),
]

version = "v2"


def round_down_time_3min(dt):
    """Round timestamp to 3-minute blocks"""
    minute_block = (dt.minute // 3) * 3
    return dt.replace(minute=minute_block, second=0, microsecond=0)


current_candle = {}
current_interval_start = {}
candles = {}

print(instruments)

retry_delay = 5
max_retry_delay = 60
retry_count = 0


while True:
    try:
        data = MarketFeed(dhan_context, instruments, version)
        print("Starting Market Feed...")

        retry_delay = 5
        retry_count = 0

        while True:

            now_ist = datetime.now(ist)
            start_time = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
            end_time = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)

            # print(now_ist)

            if not (start_time <= now_ist <= end_time):
                print("📈 Waiting for Market Hours (9:15 AM – 3:30 PM)...")
                time.sleep(60)
                continue

            data.run_forever()
            response = data.get_data()
            # print(f"Response: {response}")

            if 'LTP' not in response or 'LTT' not in response:
                print(f"Missing data for token {security_id}, response: {response}")
                continue

            ltp = response['LTP']
            ltt_time_str = response['LTT']  # "HH:MM:SS"

            # Log for debugging
            # with open("condition_log.txt", "a") as f:
            #     f.write(f"{str(response)}\n")

            # --- TICK TIMESTAMP FIX (FINAL SOLUTION) ---

            # Parse tick time
            tick_time = datetime.strptime(ltt_time_str, "%H:%M:%S").time()

            # Get today’s date from IST clock
            tick_date = now_ist.date()

            

            # Combine date + tick time
            ts = datetime.combine(tick_date, tick_time).astimezone(ist)

            # Insert spot LTP
            try:
                insert_spot_ltp_api(
                    token=security_id,
                    timestamp=ts.isoformat(),
                    ltp=ltp
                )
            except Exception as e:
                print(f"Spot LTP insert error: {e}")

            # Determine the candle interval
            interval_start = round_down_time_3min(ts)
            # print(f"Tick TS: {ts}, Interval Start: {interval_start}")

            # Initialize
            if security_id not in current_interval_start:
                current_interval_start[security_id] = None
            if security_id not in current_candle:
                current_candle[security_id] = None
            if security_id not in candles:
                candles[security_id] = []

            # New candle?
            if (current_interval_start[security_id] is None or
                    interval_start > current_interval_start[security_id]):

                # Save old candle
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
                        print(f"Inserted Candle: {completed}")

                    except Exception as e:
                        print(f"Insert DB Error: {e}")

                # Start new candle
                current_interval_start[security_id] = interval_start
                current_candle[security_id] = {
                    'start_time': interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                    'open': ltp,
                    'high': ltp,
                    'low': ltp,
                    'close': ltp
                }

            else:
                # Update running candle
                candle = current_candle[security_id]
                candle['high'] = max(candle['high'], ltp)
                candle['low'] = min(candle['low'], ltp)
                candle['close'] = ltp

    except KeyboardInterrupt:
        print("\n🛑 Shutting down gracefully...")
        break

    except Exception as e:
        retry_count += 1
        print(f"❌ Main Loop Error: {e}")
        print(f"⏳ Waiting {retry_delay} seconds...")

        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay)
        continue
