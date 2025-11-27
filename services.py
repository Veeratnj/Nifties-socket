import csv
import os
from datetime import datetime
from pathlib import Path
import dotenv
import requests

dotenv.load_dotenv()

BASE_URL = os.getenv("BASE_URL")


def insert_ohlc_data_csv(symbol, timeframe, timestamp, open, high, low, close, volume):
    """
    Store OHLC candle data to CSV file.
    
    Args:
        symbol (str): Symbol name (e.g., "NIFTY")
        timeframe (str): Timeframe interval (e.g., "5_MIN")
        timestamp (str): Timestamp in ISO format (e.g., "2025-11-25T13:00:00")
        open (float): Opening price
        high (float): Highest price
        low (float): Lowest price
        close (float): Closing price
        volume (int): Trading volume
    """
    # Create data directory if it doesn't exist
    data_dir = Path('candle_data')
    data_dir.mkdir(exist_ok=True)
    
    # Create filename based on symbol and current date
    today = datetime.now().strftime('%Y-%m-%d')
    filename = data_dir / f'candles_{symbol}_{today}.csv'
    
    # Check if file exists to determine if we need to write headers
    file_exists = filename.exists()
    
    # Prepare the data row
    row = {
        'timestamp': timestamp,
        'symbol': symbol,
        'timeframe': timeframe,
        'open': open,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
        'recorded_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Write to CSV
    with open(filename, 'a', newline='') as csvfile:
        fieldnames = ['timestamp', 'symbol', 'timeframe', 'open', 'high', 'low', 'close', 'volume', 'recorded_at']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write header only if file is new
        if not file_exists:
            writer.writeheader()
        
        writer.writerow(row)
    
    print(f"✅ OHLC data saved to {filename}")


def insert_ohlc_data_api(symbol, timeframe, timestamp, open, high, low, close, volume):
    """
    Insert OHLC historical data via API endpoint.
    
    Args:
        symbol (str): Symbol name (e.g., "25")
        timeframe (str): Timeframe interval (e.g., "5_MIN")
        timestamp (str): Timestamp in ISO format (e.g., "2025-11-25T13:00:00")
        open (float): Opening price
        high (float): Highest price
        low (float): Lowest price
        close (float): Closing price
        volume (int): Trading volume
    
    Returns:
        dict: Response from the API or error information
    """
    try:
        response = requests.post(
            f"{BASE_URL}/api/tick/insert-ohlc",
            json={
                "symbol": symbol,
                "timeframe": timeframe,
                "timestamp": timestamp,
                "open": open,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume
            }
        )
        if response.status_code in [200, 201]:
            print(f"✅ OHLC data inserted successfully for {symbol}")
            return response.json()
        else:
            print(f"❌ Failed to insert OHLC data: {response.text}")
            return {"error": response.text, "status_code": response.status_code}
    except Exception as e:
        print(f"❌ Error inserting OHLC data: {e}")
        return {"error": str(e)}


def insert_spot_ltp_api(token, timestamp, ltp):
    """
    Insert spot LTP (Last Traded Price) data via API endpoint.
    
    Args:
        token (str): Token from symbol_master (e.g., "26000")
        timestamp (str): Timestamp in ISO format (e.g., "2025-11-25T14:52:31+05:30")
        ltp (float): Last traded price
    
    Returns:
        dict: Response from the API or error information
    """
    try:
        # Extract trade_date from timestamp (YYYY-MM-DD format)
        if isinstance(timestamp, str):
            trade_date = timestamp.split('T')[0] if 'T' in timestamp else timestamp.split(' ')[0]
        else:
            trade_date = datetime.now().strftime('%Y-%m-%d')
        
        response = requests.post(
            f"{BASE_URL}/api/tick/insert-spot-ltp",
            json={
                "symbol_id": int(token),  # API expects symbol_id as integer
                "trade_date": trade_date,  # API expects trade_date in YYYY-MM-DD format
                "timestamp": timestamp,
                "ltp": float(ltp)
            }
        )
        if response.status_code in [200, 201]:
            print(f"✅ Spot LTP inserted successfully for token {token}")
            return response.json()
        else:
            print(f"❌ Failed to insert spot LTP: {response.text}")
            return {"error": response.text, "status_code": response.status_code}
    except Exception as e:
        print(f"❌ Error inserting spot LTP: {e}")
        return {"error": str(e)}