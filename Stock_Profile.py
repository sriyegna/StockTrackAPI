from requests import get
import mysql.connector
import matplotlib as plt
import numpy as np
from numpy.polynomial.polynomial import polyfit
import json,urllib.request



# Globals
apiKey = "a4585b035f8bb44392e348073ec85ca2"
index = ['SPX']

def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="root",
        database="stocktracker"
    )
    return db

def CompanyProfile (ticker, apiKey):
    try:
        url = 'https://datafied.api.edgar-online.com/v2/companies?primarysymbols=%s&appkey=%s' % (ticker, apiKey)
        data_Companies = (get(url).json())['result']['rows'][0]['values']
        name = ""
        sicValue = ""
        for i in range(len(data_Companies)):
            if data_Companies[i]['field'] == 'companyname':
                name = data_Companies[i]['value']
            if data_Companies[i]['field'] == 'sicdescription':
                sicValue = data_Companies[i]['value']
        return name, sicValue
    except Exception as ex:
        print(ex)

def CompanyRatios (ticker, apiKey):
    try:
        url = 'https://datafied.api.edgar-online.com/v2/corefinancials/ann?Appkey=' + apiKey + '&fields=ValuationRatiosMini&primarysymbols=' + ticker + '&numperiods=1&activecompanies=false&deleted=false&sortby=primarysymbol%20asc&debug=false'
        Data_ratio = (get(url).json())['result']['rows'][0]['values']
        priceEarningsSfy = float(0.0)
        priceEarningsTtm = float(0.0)
        priceBookFy = float(0.0)
        for i in range(len(Data_ratio)):
            fieldValue = (Data_ratio[i])
            if fieldValue['field'] == "priceearningsfy":
                priceEarningsSfy = fieldValue['value']
            if fieldValue['field'] == "priceearningsttm":
                priceEarningsTtm = fieldValue['value']
            if fieldValue['field'] == "pricebookfy":
                priceBookFy = fieldValue['value']
        return priceEarningsSfy, priceEarningsTtm, priceBookFy
    except Exception as ex:
        print(ex)

def EBITDAx (ticker):
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT * FROM annualdata WHERE Ticker='%s' ORDER BY Year ASC" % ticker
        mycursor.execute(sql)
        result = mycursor.fetchall()
        mycursor.close()
        numOfAverages = len(result) - 1
        EBITDAGrowth = 0
        if (numOfAverages != 0):
            for i in range(numOfAverages):
                if result[i][10] !=0:
                    EBITDAGrowth = EBITDAGrowth + (result[i+1][10] / result[i][10] - 1)
            EBITDAGrowth = EBITDAGrowth / numOfAverages
        else:
            EBITDAGrowth = None

        # Get multiple FCF
        multiEBITDA = 0.0
        if (result[numOfAverages][10] != 0):
                multiEBITDA = (result[numOfAverages][6] + result[numOfAverages][7] + result[numOfAverages][8]) / result[numOfAverages][10]
        else:
            multiEBITDA = None

        multiFCF = 0.0
        FCFmargin = 0.0
        url = 'https://datafied.api.edgar-online.com/v2/corefinancials/ann?Appkey=' + apiKey + '&fields=FinancialRatioData&primarysymbols=' + ticker + '&numperiods=1&activecompanies=false&deleted=false&sortby=primarysymbol%20asc&debug=false'
        Data_ratio_2 = (get(url).json())['result']['rows'][0]['values']
        for i in range(len(Data_ratio_2)):
            if Data_ratio_2[i]['field'] == 'freecashflowmargin':
                FCFmargin = Data_ratio_2[i]['value']
        if ((result[numOfAverages][3] != 0) and (FCFmargin != 0)):
            multiFCF = (result[numOfAverages][6] + result[numOfAverages][7] + result[numOfAverages][8]) / (result[numOfAverages][3] * FCFmargin)
        else:
            multiFCF = None

        return multiEBITDA, EBITDAGrowth, multiFCF
    except Exception as ex:
        print(ex)



def Beta (ticker, index):
    try:
        beta = 1.5 #keeping the default high to avoid risk of not having the beta value
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT CLOSE, DATE FROM alldata WHERE Ticker='%s' ORDER BY Date Desc LIMIT 1300" %index
        mycursor.execute(sql)
        result = np.array(mycursor.fetchall())
        index_date = result[:,1]
        index_close = result[:,0]
        sql = "SELECT CLOSE, DATE FROM alldata WHERE Ticker='%s' ORDER BY Date Desc LIMIT 1300" % ticker
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
        ticker_interestexp = results[20]

        #Tax Rate
        ticker_taxrate = 0.271 #average effective tax rate in US
        url = 'https://datafied.api.edgar-online.com/v2/corefinancials/ann?Appkey=' + apiKey + '&fields=FinancialRatioData&primarysymbols=' + ticker + '&numperiods=1&activecompanies=false&deleted=false&sortby=primarysymbol%20asc&debug=false'
        Data_ratio = (get(url).json())['result']['rows'][0]['values']
        for i in range(len(Data_ratio)):
            if Data_ratio[i]['field'] == 'taxrate':
                ticker_taxrate = Data_ratio[i]['value']

        #Cost of Debt Section
        if (ticker_interestexp <= 0) or (ticker_TotalDebt <= 0):
            rd = 0.07
        else:
            rd = ticker_interestexp / ticker_TotalDebt
        #Cost of Equity Section
        beta = Beta(ticker,index[0])
        if beta is None:
            beta = 1.5 #assuming a high risk rate due to beta does not exist
        else:
            beta = float(beta)
        print(beta)
        rf = 0.02
        rm = 0.12
        re = rf + beta * (rm - rf)
        #WACC
        Total_Capital = ticker_TotalDebt + ticker_CEquity

        WACC = 0.0
        if (Total_Capital != 0):
                WACC = re * (ticker_CEquity/Total_Capital) + rd * (1 - ticker_taxrate) * (ticker_TotalDebt / Total_Capital)
                WACC = float(WACC)
        else:
            WACC = None
        return WACC, beta
    except Exception as ex:
        print(ex)

#execute results

db = connectToDb()
mycursor = db.cursor(buffered=True)
sql = "SELECT Ticker FROM annualdata GROUP BY Ticker"
mycursor.execute(sql)
result = mycursor.fetchall()
# Close the DB and cursor connection
mycursor.close()
db.close()

for ticker in result:
    print(ticker)
    name, sicValue = CompanyProfile(ticker[0], apiKey)
    priceEarningsSfy, priceEarningsTtm, priceBookFy = CompanyRatios(ticker[0], apiKey)
    multiEBITDA, EBITDAGrowth, multiFCF = EBITDAx(ticker[0])
    WACC, Ticker_Beta = (WACC_Cal(ticker[0]))
    #Ticker_Beta = float(Beta(ticker[0], index[0]))
    print(type(Ticker_Beta))

    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "INSERT INTO stockprofile (Ticker, Name, Sector, PE, PE_TTM, PB, EBITDA_Growth, EBITDA_Multiple, FCF_Multiple, Beta, WACC) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (ticker[0], name, sicValue, priceEarningsSfy, priceEarningsTtm, priceBookFy, EBITDAGrowth, multiEBITDA, multiFCF, Ticker_Beta, WACC)
        print(val)
        mycursor.execute(sql, val)
        db.commit()
        mycursor.close()
        db.close()
    except Exception as ex:
        print(ex)
