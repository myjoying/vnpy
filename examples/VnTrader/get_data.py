from jaqs.data import DataApi
phone = "15009296563"
token = "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1MjY5MDUyODQ4NDQiLCJpc3MiOiJhdXRoMCIsImlkIjoiMTUwMDkyOTY1NjMifQ.G7iuy8aDdNvq6CQzdKMGS7MhnpSg7aVuvCSUpPNhIEk"

api = DataApi(addr='tcp://data.quantos.org:8910')
result, msg = api.login(phone, token) 
print(result, msg)
print("########")
df, msg = api.bar("cu1806.SHF", start_time="09:56:00", end_time="13:56:00",
                  trade_date="2017-08-23", fields="open,high,low,last,volume", freq="5M")


print(df, msg)