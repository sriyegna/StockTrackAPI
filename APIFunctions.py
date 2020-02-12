import mysql.connector
import json,urllib.request
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import atexit
from apscheduler.schedulers.background import BackgroundScheduler

#Globals
apiKey = "FIYD4XDQPMXOSUH8"

# Function to generate a unique DB connection
# If you use one global variable, concurrent calls will return an SSL error
def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="python",
        passwd="python",
        database="stocktracker"
    )
    return db


#Function to update Daily Stocks in DB by ticker name. Returns # of inserted rows
def updateDailyStockDbByTicker(ticker):
    try:
        #Create DB connection and buffered cursor
        db = connectToDb()
        mycursor = db.cursor(buffered=True)

        # Get latest and check if we can update last 100 or all records
        sql = "SELECT * FROM stockdata WHERE Ticker='%s' ORDER BY Date DESC LIMIT 1" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        date = result[0][2]
        # Calculate date from 70 days ago as a ballpark. If our last data is within the last 70 days, we can request a smaller data set
        right_now_70_days_ago = datetime.today() - timedelta(days=70)
        outputSize = "full"
        # If we are within 70 days, request compact data set
        if (date > right_now_70_days_ago):
            outputSize = "compact"

        # Make the API call to Time_series_daily
        rowsInserted = 0
        data = json.loads(urllib.request.urlopen(
            "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=%s&symbol=%s&apikey=%s" % (
                outputSize, ticker, apiKey)).read())
        timeData = data['Time Series (Daily)']

        # For all results, parse the data and for each set of data, insert it into the database
        for date in timeData:
            open = data['Time Series (Daily)'][date]['1. open']
            high = data['Time Series (Daily)'][date]['2. high']
            low = data['Time Series (Daily)'][date]['3. low']
            close = data['Time Series (Daily)'][date]['4. close']
            volume = data['Time Series (Daily)'][date]['5. volume']
            id = ticker + date
            try:
                # Create an insert statement with the above data and commit it.
                sql = "INSERT INTO stockdata (ID, Ticker, Date, Open, High, Low, Close, Volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                val = (id, ticker, date, open, high, low, close, volume)
                mycursor.execute(sql, val)
                db.commit()
                rowsInserted = rowsInserted + 1
            except Exception as ex:
                #print(ex.__class__.__name__)
                print(ex)
        # Close DB and cursor connection
        mycursor.close()
        db.close()
        return rowsInserted
    except Exception as ex:
        print(ex)

#Function to determine if a given ticker is in the DB. Returns boolean
def isDailyStockInDb(ticker):
    try:
        # Create DB and Cursor connection
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        # Create SQL Select to get one row from database for the given ticker
        sql = "SELECT * FROM stockdata WHERE Ticker='%s' LIMIT 1" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        # Close the DB and cursor connection
        mycursor.close()
        db.close()
        # Return true if we got a row return, false if we did not.
        if (len(result) > 0):
            return True
        else:
            return False
    except Exception as ex:
        print(ex)

#Function to calculate provide 50, 200 averages for mean revision. Returns Array
def meanRevisionCalculator(ticker):
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
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
        mycursor.close()
        db.close()
        return [avg50, avg200]
    except Exception as ex:
        print(ex)

#Function to calculate moving day average. Returns array
def movingDayAverage(ticker, days, fromDate, toDate):
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT Close, Date FROM stockdata WHERE Ticker='%s' AND Date > '%s' AND Date < '%s' ORDER BY Date Desc" % (ticker, fromDate, toDate)
        mycursor.execute(sql)
        result = mycursor.fetchall()
        arr = []
        histStockData = []
        for i in range(len(result), days-1, -1):
            histStockData.append(result[i-1][0])
            daySum = 0
            for j in range(days):
                if (j == 0):
                    startDate = result[i - j - 1][1]
                elif (j == days - 1):
                    endDate = result[i - j - 1][1]
                daySum = daySum + result[i - j - 1][0]
            arr.append([daySum/days, startDate.strftime("%Y-%m-%d"), endDate.strftime("%Y-%m-%d")])
        #Calculate n day regression
        print(arr[len(arr) - 1])
        m = (arr[len(arr) - 1][0] - arr[len(arr) - days - 1][0]) / days
        b = arr[len(arr) - 1][0] - (days * m)
        returnArr = [arr, m, b, histStockData]
        mycursor.close()
        db.close()
        return returnArr
    except Exception as ex:
        print(ex)


def isDailyStockUpToDate(ticker):
    curDate = datetime.date(datetime.now())
    dayOfWeek = curDate.weekday()
    if (dayOfWeek == 6):
        curDate = curDate - timedelta(days=2)
    elif (dayOfWeek == 5):
        curDate = curDate - timedelta(days=1)

    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT Date FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc LIMIT 1" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        sqlDate = result[0][0]
        mycursor.close()
        db.close()
        if (curDate.strftime("%Y-%m-%d") == sqlDate.strftime("%Y-%m-%d")):
            return True
        else:
            return False
    except Exception as ex:
        print(ex)

def getHistoricalData(ticker):
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT Date, Close FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        closeArr = []
        for res in result:
            closeArr.insert(0, [(res[0]).strftime("%Y-%m-%d"), res[1]])
        mycursor.close()
        db.close()
        return closeArr
    except Exception as ex:
        print(ex)

def getLatestStocksFromDb():
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT Distinct Ticker FROM stockdata ORDER BY Ticker"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        resultArr = []
        for res in result:
            sql = "SELECT * FROM stockdata WHERE Ticker='%s' ORDER BY Date DESC LIMIT 1" % res[0]
            mycursor.execute(sql)
            result = mycursor.fetchall()

            stockArr = []
            for i in range(0, len(result[0])):
                if (i == 2):
                    stockArr.append(result[0][i].strftime("%Y-%m-%d"))
                else:
                    stockArr.append(result[0][i])
            resultArr.append(stockArr)
        mycursor.close()
        db.close()
        return resultArr
    except Exception as ex:
        print(ex)

def getPreviousDayStockFromDb():
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT Distinct Ticker FROM stockdata ORDER BY Ticker"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        resultArr = []
        for res in result:
            sql = "SELECT * FROM stockdata WHERE Ticker='%s' ORDER BY Date DESC LIMIT 2" % res[0]
            mycursor.execute(sql)
            result = mycursor.fetchall()

            stockArr = []
            for i in range(0, len(result[1])):
                if (i == 2):
                    stockArr.append(result[1][i].strftime("%Y-%m-%d"))
                else:
                    stockArr.append(result[1][i])
            resultArr.append(stockArr)
        mycursor.close()
        db.close()
        return resultArr
    except Exception as ex:
        print(ex)

def getSAndP500():
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT * FROM stockdata WHERE Ticker='SPX' ORDER BY Date Desc LIMIT 1"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        mycursor.close()
        db.close()
        return result
    except Exception as ex:
        print(ex)


def updateAllStocksInDb():
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT Distinct Ticker FROM stockdata ORDER BY Ticker"
        mycursor.execute(sql)
        result = mycursor.fetchall()
        for res in result:
            updateDailyStockDbByTicker(res[0])
        mycursor.close()
        db.close()
    except Exception as ex:
        print(ex)


scheduler = BackgroundScheduler()
#scheduler.add_job(func=updateAllStocksInDb, trigger="interval", minutes=15)
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())