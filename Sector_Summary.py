from requests import get
import mysql.connector
import matplotlib as plt
import numpy as np
from numpy.polynomial.polynomial import polyfit
import json,urllib.request
import pandas as pd

#Global_Variable
fieldsToGather = ['PE', 'PE_TTM', 'PB', 'Growth', 'EBITDA_Multiple', 'FCF_Multiple', 'Beta', 'WACC']

Sector_Data = pd.read_excel(r'C:\Users\ashaik29\Desktop\ML\SECSectorMapping.xlsx', header=0)
Sector_Groups = Sector_Data['Office']
Sector_Groups = list(dict.fromkeys(Sector_Groups))
Sector_Industrys = Sector_Data['Industry']

def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="root",
        database="stocktracker"
    )
    return db

db = connectToDb()
mycursor = db.cursor(buffered=True)
sql = "SELECT *FROM stockprofile"
mycursor.execute(sql)
result = mycursor.fetchall()
# Close the DB and cursor connection
mycursor.close()
db.close()

for group in Sector_Groups:
    Group_Data = [] #Rows(8) are as followed: PE, PE_TTM, PB, EBITDA_Growth, EBITDA_Multiple, FCF_Multiple, Beta, WACC
    for industry in (Sector_Data['Industry'][Sector_Data['Office'] == group]):
        for i in range(len(result)):
            if result[i][2].upper() == industry:
                Group_Data.append(result[i][3:11])

    df = pd.DataFrame(Group_Data, columns=fieldsToGather)
    sector_summary = [str(group)]
    for i in range(len(fieldsToGather)):
        arr = df[fieldsToGather[i]]
        Q1 = arr.quantile(0.25)
        Q3 = arr.quantile(0.75)
        IQR = Q3 - Q1
        arr = arr[~((arr < (Q1 - IQR)) | (arr > (Q3 + IQR)))]
        sector_summary.append(float(arr.mean()))

    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "INSERT INTO sector_summary (Sector_Group, PE, PE_TTM, PB, Growth, EBITDA_Multiple, FCF_Multiple, Beta, WACC) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        print(sector_summary)
        mycursor.execute(sql, sector_summary)
        db.commit()
        mycursor.close()
        db.close()
    except Exception as ex:
        print(ex)

