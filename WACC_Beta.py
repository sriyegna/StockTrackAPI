from requests import get
import mysql.connector
import json,urllib.request
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from numpy.polynomial.polynomial import polyfit



#Global
alphaapiKey = "FIYD4XDQPMXOSUH8"
WACC = []
index = ["SPX"]

ticker = ['AAPL']

def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="root",
        database="stocktracker"
    )
    return db

def Beta (ticker, index):
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT CLOSE, DATE FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc LIMIT 1300" %index
        mycursor.execute(sql)
        result = np.array(mycursor.fetchall())
        index_date = result[:,1]
        index_close = result[:,0]
        sql = "SELECT CLOSE, DATE FROM stockdata WHERE Ticker='%s' ORDER BY Date Desc LIMIT 1300" % ticker
        mycursor.execute(sql)
        result = np.array(mycursor.fetchall())
        ticker_date = result[:,1]
        ticker_close = result[:,0]

        #Data Normalization & Calculating Returns
        ticker_arr = []
        index_arr = []
        for i in range(len(index_date)):
            for j in range(len(ticker_date)):
                if ticker_date[j] == index_date[i]:
                    index_arr.append(index_close[i])
                    ticker_arr.append(ticker_close[j])
        ticker_return = []
        index_return = []
        for i in range(len(index_arr)-1):
            ticker_return.append(ticker_arr[i]/ticker_arr[i+1] - 1)
            index_return.append(index_arr[i]/index_arr[i+1] - 1)

        #Line of best fit
        index_return = np.array(index_return)
        b, beta = polyfit(index_return, ticker_return, 1)

        #plot
        #plt.plot(index_return, ticker_return, '.')
        #plt.plot(index_return, b + float(beta) * index_return, '-')
        #plt.show()

        mycursor.close()
        db.close()
        return beta
    except Exception as ex:
        print(ex)

def WACC_Cal(ticker):
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT * FROM annualdata WHERE Ticker='%s' ORDER BY Year Desc LIMIT 1" %ticker
        mycursor.execute(sql)
        mycursor.execute(sql)
        results = (mycursor.fetchall())[0]
        ticker_TotalDebt = results[6] + results[7]
        ticker_CEquity = results[8]
        ticker_taxrate = results[11] / results[12]
        ticker_interestexp = results[20]


        #Cost of Debt Section
        if ticker_interestexp < 0:
            rd = 0.07
        else:
            rd = ticker_interestexp / ticker_TotalDebt

        #Cost of Equity Section
        beta = Beta(ticker,index[0])
        rf = 0.02
        rm = 0.12
        re = rf + beta * (rm - rf)

        #WACC
        Total_Capital = ticker_TotalDebt + ticker_CEquity
        WACC = re * (ticker_CEquity/Total_Capital) + rd * (1 - ticker_taxrate) * (ticker_TotalDebt / Total_Capital)
        return WACC

    except Exception as ex:
        print(ex)

#beta = Beta(ticker[0],index[0])
WACC = WACC_Cal(ticker[0])
print(WACC)

