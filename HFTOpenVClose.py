import mysql.connector
from threading import Thread

def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="python",
        passwd="python",
        database="stocktracker"
    )
    return db

def getSQLData(sqlString):
    db = connectToDb()
    mycursor = db.cursor(buffered=True)
    sql = sqlString
    mycursor.execute(sql)
    result = mycursor.fetchall()
    mycursor.close()
    db.close()
    return result;

def calculateCloseAverage(tickerName):
    sqlString = ("SELECT Open, Close FROM stockdata WHERE ticker='%s' ORDER BY Date DESC" % tickerName)
    tickerData = getSQLData(sqlString)
    # print(tickerData)
    sum = 0
    for i in range(0, len(tickerData)):
        sum = sum + ((tickerData[i][1] / tickerData[i][0]) - 1)

    sum = sum / (len(tickerData))
    print("Ticker: " + tickerName + " Average: " + str(sum))


result = getSQLData("SELECT Ticker FROM stockdata GROUP BY Ticker")

for ticker in result:
    thread = Thread(target = calculateCloseAverage, args = (ticker[0],))
    thread.start()