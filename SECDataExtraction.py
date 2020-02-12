from requests import get
import mysql.connector
import numpy as np
import json,urllib.request
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd

#Notes
#rowArr
#0 cashcashequivalentsandshortterminvestments
#1 commonstock
#2 ebit
#3 grossprofit
#4 incomebeforetaxes
#5 incometaxes
#6 netincome
#7 operatingprofit
#8 totallongtermdebt
#9 totalrevenue
#10 totalshorttermdebt
#11 capitalexpenditures
#12 cfdepreciationamortization
#13 changeincurrentassets
#14 changeincurrentliabilities
#15 workingCapital
#16 eBITDA
#17 taxRate
#18 interestExpense


#Globals
EdgarKey = 'a4585b035f8bb44392e348073ec85ca2'
fieldsToGather = ["cashcashequivalentsandshortterminvestments", "commonstock", "ebit", "grossprofit", "incomebeforetaxes", "incometaxes", "netincome", "operatingprofit", "totallongtermdebt",
                  "totalrevenue", "totalshorttermdebt", "capitalexpenditures", "cfdepreciationamortization", "changeincurrentassets", "changeincurrentliabilities"]



def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="root",
        database="stocktracker"
    )
    return db



url = 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
exchanges = ['NASDAQ', 'AMEX', 'NYSE']
df = pd.concat([pd.read_csv(url.format(ex)) for ex in exchanges]).dropna(how='all', axis=1)
df = df.rename(columns=str.lower).set_index('symbol').drop('summary quote', axis=1).dropna(subset=['marketcap'], axis='rows')
df = df[~df.index.duplicated()]
tickers = df.index
#print(tickers)
#tickers = ["AAPL"]

for ticker in tickers:
    try:
        url = 'https://datafied.api.edgar-online.com/v2/corefinancials/ann?Appkey=a4585b035f8bb44392e348073ec85ca2&fields=BalanceSheetConsolidated%2CIncomeStatementConsolidated&primarysymbols=' + ticker + '&numperiods=12&activecompanies=false&deleted=false&sortby=primarysymbol%20asc&debug=false'
        data_BSC = get(url).json()
        url = ' https://datafied.api.edgar-online.com/v2/corefinancials/ann?Appkey=2d2d5cb64a16d58eb8d7eac5a2100090&fields=CashFlowStatementConsolidated&primarysymbols=' + ticker + '&numperiods=12&activecompanies=false&deleted=false&sortby=primarysymbol%20asc&debug=false'
        data_CFSC = get(url).json()
        print(data_BSC['result']['totalrows'])
        print(data_CFSC['result']['totalrows'])
        if (data_BSC['result']['totalrows'] == data_CFSC['result']['totalrows']):
            #Same # of rows of data from both calls
            arrayLen = data_CFSC['result']['totalrows']
        for i in range(arrayLen):
            rowArr = []
            for j in range(len(data_BSC['result']['rows'][i]['values'])):
                fieldName = data_BSC['result']['rows'][i]['values'][j]['field']
                if (fieldName in fieldsToGather):
                    rowArr.append(data_BSC['result']['rows'][i]['values'][j]['value'])
            for j in range(len(data_CFSC['result']['rows'][i]['values'])):
                fieldName = data_CFSC['result']['rows'][i]['values'][j]['field']
                if (fieldName in fieldsToGather):
                    rowArr.append(data_CFSC['result']['rows'][i]['values'][j]['value'])
            workingCapital = rowArr[13] - rowArr[14]
            eBITDA = rowArr[2] + rowArr[12]
            taxRate = rowArr[5] / rowArr[4]
            interestExpense = rowArr[7] - rowArr[4]
            rowArr.extend([workingCapital, eBITDA, taxRate, interestExpense])


            try:
                db = connectToDb()
                mycursor = db.cursor(buffered=True)

                Temp_Year = int(datetime.now().year) - 1 - i
                id = ticker + str(Temp_Year)
                sql = "INSERT INTO annualdata (Id, Ticker, Year, Total_Revenue, Gross_Profit, Net_Income, ST_Debt, LT_Debt, C_Equity, EBIT, EBITDA, Income_Tax, Income_Before_Tax, Operating_Profits, Dep_Amort, ChangeInAsset, ChangeInLiab, WorkingCap, CAPEX, Cash, Interest_Expense) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                val = (id, ticker, Temp_Year, rowArr[9], rowArr[3], rowArr[6], rowArr[10], rowArr[8], rowArr[1], rowArr[2], rowArr[16], rowArr[5], rowArr[4], rowArr[7], rowArr[12], rowArr[13], rowArr[14], rowArr[15], rowArr[11], rowArr[0], rowArr[18])
                mycursor.execute(sql, val)
                db.commit()
                mycursor.close()
                db.close()
            except Exception as ex:
                print(ex)
    except Exception as ex:
        print(ex)









