from requests import get
import mysql.connector
import json,urllib.request

# Globals
apiKey = "a4585b035f8bb44392e348073ec85ca2"

def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="python",
        passwd="python",
        database="stocktracker"
    )
    return db

db = connectToDb()
mycursor = db.cursor(buffered=True)
sql = "SELECT Ticker FROM annualdata GROUP BY Ticker"
mycursor.execute(sql)
result = mycursor.fetchall()
# Close the DB and cursor connection
mycursor.close()
db.close()

for ticker in result:
    # Values from Link 1 - Company name, sector
    # url = 'https://datafied.api.edgar-online.com/v2/companies?primarysymbols=%s&appkey=%s' % (ticker[0], apiKey)
    # data_Companies = get(url).json()
    # companyName = data_Companies['result']['rows'][0]['values'][1]
    # name = ""
    # if (companyName['field'] != "companyname"):
    #     print("companyname for %s is not at index 1" % ticker[0])
    # else:
    #     name = companyName['value']
    #
    # sicDescriptionData = data_Companies['result']['rows'][0]['values'][8]
    # sicValue = ""
    # if (sicDescriptionData['field'] != "sicdescription"):
    #     print("sicdescription for %s is not at index 8" % ticker[0])
    # else:
    #     sicValue = sicDescriptionData['value']
        
    # Values form Link 2 - P/E FY, PE/TTM, P/B
    # url = 'https://datafied.api.edgar-online.com/v2/corefinancials/ann?Appkey=' + apiKey + '&fields=ValuationRatiosMini&primarysymbols=' + ticker[0] + '&numperiods=1&activecompanies=false&deleted=false&sortby=primarysymbol%20asc&debug=false'
    # data_Companies = get(url).json()
    # print(data_Companies['result']['rows'][0]['values'])
    # result = data_Companies['result']['rows'][0]['values']
    # for fieldValue in result:
    #     priceEarningsSfy = 0.0
    #     priceEarningsTtm = 0.0
    #     priceBookFy = 0.0
    #     if fieldValue['field'] == "priceearningsfy":
    #         priceEarningsSfy = fieldValue['value']
    #     if fieldValue['field'] == "priceearningsttm":
    #         priceEarningsTtm = fieldValue['value']
    #     if fieldValue['field'] == "pricebookfy":
    #         priceBookFy = fieldValue['value']

    # Get EBITDA Growth
    db = connectToDb()
    mycursor = db.cursor(buffered=True)
    sql = "SELECT * FROM annualdata WHERE Ticker='%s' ORDER BY Year DESC" % ticker[0]
    mycursor.execute(sql)
    result = mycursor.fetchall()
    mycursor.close()
    db.close()
    numOfAverages = len(result) - 1
    sumAvg = 0
    for i in range(numOfAverages):
        if (result[i+1][10] == 0):
            numOfAverages = numOfAverages - 1
            continue;
        sumAvg = sumAvg + (result[i][10]/result[i+1][10] - 1)
    if (numOfAverages != 0):
        sumAvg = sumAvg / numOfAverages
    else:
        sumAvg = None

    # Get multiple EBITDA
    multiEBITDA = 0
    if (result[0][10] != 0):
        multiEBITDA = result[0][6] + result[0][7] + result[0][8]/result[i][10]
    else:
        multiEBITDA = None
