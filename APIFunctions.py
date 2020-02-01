import mysql.connector
import json,urllib.request
#Globals
db = mysql.connector.connect(
    host="localhost",
    user="python",
    passwd="python",
    database="stocktracker"
)
apiKey = "FIYD4XDQPMXOSUH8"
mycursor = db.cursor(buffered=True)
#Function to update Daily Stocks in DB by ticker name. Returns # of inserted rows
def updateDailyStockDbByTicker(ticker):
    # Populate Daily
    rowsInserted = 0
    data = json.loads(urllib.request.urlopen(
        "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&symbol=%s&apikey=%s" % (
            ticker, apiKey)).read())
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
            rowsInserted = rowsInserted + 1
        except Exception as ex:
            print(ex.__class__.__name__)
            print(ex)
    return rowsInserted

#Function to determine if a stock is in the DB. Returns boolean
def isDailyStockInDb(ticker):
    try:
        #Get last 50
        sql = "SELECT * FROM stockdata WHERE Ticker='%s' LIMIT 1" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        if (len(result) > 0):
            return True
        else:
            return False
    except Exception as ex:
        print(ex)

#Function to calculate provide 50, 200 averages for mean revision. Returns Array
def meanRevisionCalculator(ticker):
    try:
        # Get last 50
        sql = "SELECT Close FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc LIMIT 50" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        # Get average of 50
        avg50 = 0
        for close in result:
            avg50 = avg50 + close[0]
        avg50 = avg50 / 50

        # Get last 200
        sql = "SELECT Close FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc LIMIT 200" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        # Get average if 200
        avg200 = 0
        for close in result:
            avg200 = avg200 + close[0]
        avg200 = avg200 / 200
        return [avg50, avg200]
    except Exception as ex:
        print(ex)

#Function to calculate moving day average. Returns array
def movingDayAverage(ticker, days):
    sql = "SELECT Close FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc" % ticker
    mycursor.execute(sql)
    result = mycursor.fetchall()
    arr = []
    for i in range(len(result), days-1, -1):
        daySum = 0
        for j in range(days):
            daySum = daySum + result[i - j - 1][0]
        arr.append(daySum/days)
    return arr