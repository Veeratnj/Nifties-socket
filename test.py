import ast
import pandas as pd
from datetime import datetime, date
import pytz

input_file = "condition_log.txt"
output_file = "3min_ohlc.csv"

# Timezone
IST = pytz.timezone("Asia/Kolkata")

# Replace with actual date if required
today = date.today()

ticks = []

with open(input_file, "r") as f:
    for line in f:
        try:
            data = ast.literal_eval(line.strip())

            # Combine date + LTT
            dt = datetime.strptime(
                f"{today} {data['LTT']}",
                "%Y-%m-%d %H:%M:%S"
            )

            # Localize to IST
            dt = IST.localize(dt)

            ticks.append({
                "datetime": dt,
                "ltp": float(data["LTP"])
            })

        except:
            continue


df = pd.DataFrame(ticks)
df = df.sort_values("datetime")
df.set_index("datetime", inplace=True)

# Create 3-minute candle
ohlc = df["ltp"].resample("3T").agg(
    open="first",
    high="max",
    low="min",
    close="last"
).dropna()

# Add token column
ohlc["token"] = 25

# Final timestamp format for DB
ohlc.index = ohlc.index.strftime("%Y-%m-%d %H:%M:%S%z")

# Save CSV
ohlc.to_csv(output_file)

print("Saved:", output_file)


