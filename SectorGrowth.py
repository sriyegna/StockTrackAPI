from requests import get
import json,urllib.request
import mysql.connector
import pandas as pd

# Globals
apiKey = "FIYD4XDQPMXOSUH8"


data = json.loads(urllib.request.urlopen(
            "https://www.alphavantage.co/query?function=SECTOR&apikey=%s" % (apiKey)).read())

rankI = data['Rank I: 5 Year Performance']

for sector in rankI:
    cagr = ((1 + float(rankI[sector][:-1])/100) ** (1/5)) -1
    print(sector + str(cagr))