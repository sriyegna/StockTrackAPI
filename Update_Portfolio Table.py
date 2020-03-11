from requests import get
import mysql.connector
import matplotlib as plt
import numpy as np
from numpy.polynomial.polynomial import polyfit
import json,urllib.request
import pandas as pd
import time

# Globals
apiKey = "16b6de16a73966165b3306c35d54fcb6"
index = ['SPX']

def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="root",
        database="stocktracker"
    )
    return db

def UpdateSectorGroup (ticker):
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT * FROM stockprofile where ticker = '%s'" %ticker
        mycursor.execute(sql)
        Ticker_Profile = mycursor.fetchall()[0]
        mycursor.close()

        Ticker_Sector = Ticker_Profile[3]
        print(Ticker_Sector)
        mycursor = db.cursor(buffered=True)
        sql = "SELECT Sector_Group FROM sector_definition where Industry = '%s'" %Ticker_Sector
        mycursor.execute(sql)
        Sector_Group = mycursor.fetchall()[0][0]
        db.close()
        return Sector_Group
    except Exception as ex:
        print(ex)

def UpdateTaxRate(ticker, apiKey):
    try:

        ticker_taxrate = 0.00
        url = 'https://datafied.api.edgar-online.com/v2/corefinancials/ann?Appkey=' + apiKey + '&fields=FinancialRatioData&primarysymbols=' + ticker + '&numperiods=5&activecompanies=false&deleted=false&sortby=primarysymbol%20asc&debug=false'
        results = (get(url).json())['result']['rows']
        for j in range(len(results)):
            Data_ratio = results[j]['values']
            for i in range(len(Data_ratio)):
                if Data_ratio[i]['field'] == 'taxrate':
                    ticker_taxrate = ticker_taxrate + Data_ratio[i]['value']

        ticker_taxrate = ticker_taxrate / (len(results))

        if ticker_taxrate <= 0.00 or ticker_taxrate == None:
            ticker_taxrate = 0.271 #average effective tax rate in US

        print(ticker_taxrate)
        return ticker_taxrate

    except Exception as ex:
        print(ex)

#execute results

db = connectToDb()
mycursor = db.cursor(buffered=True)
sql = "SELECT Ticker FROM stockprofile"
mycursor.execute(sql)
result = mycursor.fetchall()
# Close the DB and cursor connection
mycursor.close()
db.close()

x = 0

for ticker in result:
    ticker = ticker[0]
    print(ticker)
    Sector_Group = UpdateSectorGroup(ticker)
    Ticker_TaxRate = UpdateTaxRate(ticker, apiKey)
    x = x + 1
    if x == 4:
        time.sleep(5)
        x = 0
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "UPDATE stockprofile SET TaxRate = '%s', Sector_Group = '%s' Where Ticker = '%s'" %(Ticker_TaxRate, Sector_Group, ticker)
        print(sql)
        mycursor.execute(sql)
        db.commit()
        mycursor.close()
        db.close()
    except Exception as ex:
        print(ex)

        #UPDATE stocktracker.stockprofile SET TaxRate='0.025' WHERE Ticker ='1';
