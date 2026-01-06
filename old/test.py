from dhanhq import DhanContext, MarketFeed


# client_id = '1100449732'   # Divya ID
# access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NzYxMzk2LCJpYXQiOjE3Njc2NzQ5OTYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwNDQ5NzMyIn0.xYRXr46pwv7zuGql5BNCf13gQwhpzNKJwD5VoT_XrYjY4CdAoFM5a8a-PQ2RxKwrwoUE4MwL19P6VEFVqoFFFQ"

client_id = '1100465668' #raja sir id
access_token ='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NzU5NjUzLCJpYXQiOjE3Njc2NzMyNTMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAwNDY1NjY4In0.VD94mxWH_iI2A0AzfKVI-7OA9QisCEdZqkqrtC5vhe8MMnuLK4Qt5RwIO2c2-gEDMCK24bCGLSVefY2n6HUpsw'



# Define and use your dhan_context if you haven't already done so like below:
dhan_context = DhanContext(client_id,access_token)

# Structure for subscribing is (exchange_segment, "security_id", subscription_type)

instruments = [
    (MarketFeed.IDX, "13", MarketFeed.Ticker),  
    (MarketFeed.IDX, "25", MarketFeed.Ticker),  
    (MarketFeed.IDX, "27", MarketFeed.Ticker),  
    (MarketFeed.IDX, "51", MarketFeed.Ticker),  
    (MarketFeed.IDX, "442", MarketFeed.Ticker),  
   
    ]



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