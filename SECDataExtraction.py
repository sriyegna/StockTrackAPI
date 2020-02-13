from requests import get
import mysql.connector
import json,urllib.request
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
#Globals
EdgarKey = 'a4585b035f8bb44392e348073ec85ca2'
fieldsToGather = ["cashcashequivalentsandshortterminvestments", "commonstock", "ebit", "grossprofit", "incomebeforetaxes", "incometaxes", "netincome", "operatingprofit", "totallongtermdebt",
                  "totalrevenue", "totalshorttermdebt", "capitalexpenditures", "cfdepreciationamortization", "changeincurrentassets", "changeincurrentliabilities"]

def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="python",
        passwd="python",
        database="stocktracker"
    )
    return db

url = 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
exchanges = ['NASDAQ', 'AMEX', 'NYSE']
df = pd.concat([pd.read_csv(url.format(ex)) for ex in exchanges]).dropna(how='all', axis=1)
df = df.rename(columns=str.lower).set_index('symbol').drop('summary quote', axis=1).dropna(subset=['marketcap'], axis='rows')
df = df[~df.index.duplicated()]
tickers = df.index


for ticker in tickers:
    try:
        url = 'https://datafied.api.edgar-online.com/v2/corefinancials/ann?Appkey=a4585b035f8bb44392e348073ec85ca2&fields=BalanceSheetConsolidated%2CIncomeStatementConsolidated&primarysymbols=' + ticker + '&numperiods=12&activecompanies=false&deleted=false&sortby=primarysymbol%20asc&debug=false'
        data_BSC = get(url).json()
        url = ' https://datafied.api.edgar-online.com/v2/corefinancials/ann?Appkey=2d2d5cb64a16d58eb8d7eac5a2100090&fields=CashFlowStatementConsolidated&primarysymbols=' + ticker + '&numperiods=12&activecompanies=false&deleted=false&sortby=primarysymbol%20asc&debug=false'
        data_CFSC = get(url).json()
        arrayLen = min(data_BSC['result']['totalrows'], data_CFSC['result']['totalrows'])
        for i in range(arrayLen):
            rowDict = {
                "cashcashequivalentsandshortterminvestments": 0,
                "commonstock": 0,
                "ebit": 0,
                "grossprofit": 0,
                "incomebeforetaxes": 0,
                "incometaxes": 0,
                "netincome": 0,
                "operatingprofit": 0,
                "totallongtermdebt": 0,
                "totalrevenue": 0,
                "totalshorttermdebt": 0,
                "capitalexpenditures": 0,
                "cfdepreciationamortization": 0,
                "changeincurrentassets": 0,
                "changeincurrentliabilities": 0,
                "workingcapital": 0,
                "ebitda": 0,
                "taxrate": 0,
                "interestexpense": 0
            }
            for j in range(len(data_BSC['result']['rows'][i]['values'])):
                fieldName = data_BSC['result']['rows'][i]['values'][j]['field']
                if (fieldName in fieldsToGather):
                    #rowArr.append(data_BSC['result']['rows'][i]['values'][j]['value'])
                    rowDict[fieldName] = data_BSC['result']['rows'][i]['values'][j]['value']
            for j in range(len(data_CFSC['result']['rows'][i]['values'])):
                fieldName = data_CFSC['result']['rows'][i]['values'][j]['field']
                if (fieldName in fieldsToGather):
                    #rowArr.append(data_CFSC['result']['rows'][i]['values'][j]['value'])
                    rowDict[fieldName] = data_CFSC['result']['rows'][i]['values'][j]['value']
            rowDict["workingcapital"] = rowDict["changeincurrentassets"] - rowDict["changeincurrentliabilities"]
            rowDict["ebitda"] = rowDict["ebit"] + rowDict["cfdepreciationamortization"]
            if (rowDict["incomebeforetaxes"] != 0):
                rowDict["taxrate"] = rowDict["incometaxes"] / rowDict["incomebeforetaxes"]
            rowDict["interestexpense"] = rowDict["operatingprofit"] - rowDict["incomebeforetaxes"]
            try:
                db = connectToDb()
                mycursor = db.cursor(buffered=True)
                Temp_Year = int(datetime.now().year) - 1 - i
                id = ticker + str(Temp_Year)
                print(id)
                sql = "INSERT INTO annualdata (Id, Ticker, Year, Total_Revenue, Gross_Profit, Net_Income, ST_Debt, LT_Debt, C_Equity, EBIT, EBITDA, Income_Tax, Income_Before_Tax, Operating_Profits, Dep_Amort, ChangeInAsset, ChangeInLiab, WorkingCap, CAPEX, Cash, Interest_Expense) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                val = (id, ticker, Temp_Year, rowDict["totalrevenue"], rowDict["grossprofit"], rowDict["netincome"], rowDict["totalshorttermdebt"], rowDict["totallongtermdebt"], rowDict["commonstock"], rowDict["ebit"], rowDict["ebitda"], rowDict["incometaxes"], rowDict["incomebeforetaxes"], rowDict["operatingprofit"], rowDict["cfdepreciationamortization"], rowDict["changeincurrentassets"], rowDict["changeincurrentliabilities"], rowDict["workingcapital"], rowDict["capitalexpenditures"], rowDict["cashcashequivalentsandshortterminvestments"], rowDict["interestexpense"])
                mycursor.execute(sql, val)
                db.commit()
                mycursor.close()
                db.close()
            except Exception as ex:
                pass
                # print(ex)
    except Exception as ex:
        pass
        # print(ex)









