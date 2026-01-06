from dhanhq import DhanContext, MarketFeed




# Define and use your dhan_context if you haven't already done so like below:
dhan_context = DhanContext("1100449732","eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NzY2NTcyLCJpYXQiOjE3Njc2ODAxNzIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwNDQ5NzMyIn0.u0kjiYRQQ1mTo2ZDcUNtHnZgjML6dArZS3HYKYW1SXQsZd1GS-BeLSuAWX-mIq5qwFLk4LR0V3-WRnJ-ahcUQQ")

# Structure for subscribing is (exchange_segment, "security_id", subscription_type)

instruments = [(MarketFeed.NSE, "1333", MarketFeed.Ticker),   # Ticker - Ticker Data
    (MarketFeed.NSE, "1333", MarketFeed.Quote),     # Quote - Quote Data
    (MarketFeed.NSE, "1333", MarketFeed.Full),      # Full - Full Packet
    (MarketFeed.NSE, "11915", MarketFeed.Ticker),
    (MarketFeed.NSE, "11915", MarketFeed.Full)]

version = "v2"          # Mention Version and set to latest version 'v2'

# In case subscription_type is left as blank, by default Ticker mode will be subscribed.

try:
    data = MarketFeed(dhan_context, instruments, version)
    while True:
        data.run_forever()
        response = data.get_data()
        print(response)

except Exception as e:
    print(e)