import json,urllib.request
import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    user="python",
    passwd="python",
    database="stocktracker"
)

mycursor = db.cursor(buffered=True)

tickers = ["TSLA", "COKE", "AMZN", "GOOG", "MSFT"]
#tickers = ["AAPL"]
apiKey = "FIYD4XDQPMXOSUH8"

'''
#Populate Daily
for ticker in tickers:
    data = json.loads(urllib.request.urlopen("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&symbol=%s&apikey=%s" % (ticker, apiKey)).read())
    print(data)
    timeData = data['Time Series (Daily)']

    for date in timeData:
        open = data['Time Series (Daily)'][date]['1. open']
        high = data['Time Series (Daily)'][date]['2. high']
        low = data['Time Series (Daily)'][date]['3. low']
        close = data['Time Series (Daily)'][date]['4. close']
        volume = data['Time Series (Daily)'][date]['5. volume']

        id = ticker + date

        try:
            sql = "INSERT INTO stockdata (ID, Ticker, Date, Open, High, Low, Close, Volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (id, ticker, date, open, high, low, close, volume)
            mycursor.execute(sql, val)
            db.commit()
        except Exception as ex:
            print(ex.__class__.__name__)
            print(ex)
'''

'''
for ticker in tickers:
    try:
        #Get last 50
        sql = "SELECT Close FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc LIMIT 50" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        #Get average of 50
        avg50 = 0
        for close in result:
            avg50 = avg50 + close[0]
        avg50 = avg50 / 50

        #Get last 200
        sql = "SELECT Close FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc LIMIT 200" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        #Get average if 200
        avg200 = 0
        for close in result:
            avg200 = avg200 + close[0]
        avg200 = avg200 / 200
        print("Average of 50 is: %d, Average of 200 is: %d" % (avg50, avg200))
    except Exception as ex:
        print(ex)
'''
ticker="MSFT"
days = 5
sql = "SELECT Close FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc" % ticker
mycursor.execute(sql)
result = mycursor.fetchall()
arr = []
for i in range(len(result), days-1, -1):
    daySum = 0
    for j in range(days):
        daySum = daySum + result[i - j - 1][0]
    arr.insert(0, daySum/days)
print(arr)