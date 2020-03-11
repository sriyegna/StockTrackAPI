from requests import get
import mysql.connector
import matplotlib as plt
import numpy as np
from numpy.polynomial.polynomial import polyfit
import json,urllib.request
import pandas as pd


ticker = ['VNET']
growth = None
year = 4
Input_WACC = 0.15

def connectToDb():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="root",
        database="stocktracker"
    )
    return db


def Valuation_Inputs (ticker):

    #Gather Inputs
    try:
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT CLOSE FROM alldata where ticker = '%s' ORDER BY DATE DESC LIMIT 1" %ticker
        mycursor.execute(sql)
        Ticker_Price = mycursor.fetchall()[0][0]
        mycursor.close()

        mycursor = db.cursor(buffered=True)
        sql = "SELECT * FROM stockprofile where ticker = '%s'" %ticker
        mycursor.execute(sql)
        Ticker_Profile = mycursor.fetchall()[0]
        mycursor.close()
        Sector_Group = Ticker_Profile[1]

        mycursor = db.cursor(buffered=True)
        sql = "SELECT * FROM portfolio_data where Ticker='%s' ORDER BY Year Desc" %ticker
        mycursor.execute(sql)
        Ticker_Data = mycursor.fetchall()[0]
        mycursor.close()

        #Sector_Comp:
        if Sector_Group == None:
            mycursor = db.cursor(buffered=True)
            sql = "SELECT *FROM sector_summary"
            mycursor.execute(sql)
            Sector_Profile = mycursor.fetchall()
            mycursor.close()
            Sector_Profile = pd.DataFrame(Sector_Profile).mean(axis=0)

        else:
            mycursor = db.cursor(buffered=True)
            sql = "SELECT *FROM sector_summary Where Sector_Group = %s" %Sector_Group
            mycursor.execute(sql)
            Sector_Profile = mycursor.fetchall()
            mycursor.close()

        db.close()

        return Ticker_Price, Ticker_Profile, Ticker_Data, Sector_Profile

    except Exception as ex:
        print(ex)


def DCF_Valuation(ticker, growth, Input_WACC, year, Type_GP_EM = 'GP'):
    try:
        #Gather inputs:
        Ticker_Price, Ticker_Profile, Ticker_Data, Sector_Profile = Valuation_Inputs(ticker)

        #Inputs
        Sector_Group = Ticker_Profile[1]
        Calc_CAGR = Ticker_Profile[7]
        Exit_Multiple = Ticker_Profile[8]
        Calc_WACC = Ticker_Profile[11]
        Tax_Rate = Ticker_Profile[12]

        EBIT = Ticker_Data[9]
        Dep_Amort = Ticker_Data[14]
        Working_Cap = Ticker_Data[15] + Ticker_Data[16]
        CAPEX = Ticker_Data[18]

        Total_Debt = Ticker_Data[6] + Ticker_Data[7]
        Equity = Ticker_Data[8]
        Cash = Ticker_Data[19]
        Shares_Outstanding = Equity / Ticker_Price

        Sector_Growth = Sector_Profile[4]

        #DCF Valuation
        FCF = EBIT * (1 - Tax_Rate) + Dep_Amort + Working_Cap + CAPEX

        #WACC
        if Input_WACC != None:
            WACC = Input_WACC
        else:
            if Calc_WACC >  Sector_Profile[4]:
                WACC = Calc_WACC
            else:
                WACC = 0.20

        #Initial Growth
        if growth != None:
            CAGR = growth
        else:
            if Calc_CAGR > -0.125:
                CAGR = Calc_CAGR
            else:
                CAGR = 0.07 #1/2 market rate

        #Financials Future Projections
        EBIT, Disc_EBIT = Projection_Calcultation(CAGR, Sector_Growth, year, WACC, EBIT)
        Dep_Amort, Disc_Dep_Amort = Projection_Calcultation(CAGR, Sector_Growth, year, WACC, Dep_Amort)
        CAPEX, Disc_CAPEX = Projection_Calcultation(CAGR, Sector_Growth, year, WACC, CAPEX)
        Working_Cap, Disc_Working_Cap = Projection_Calcultation(CAGR, Sector_Growth, year, WACC, Working_Cap)
        FCF, Disc_FCF = Projection_Calcultation(CAGR, Sector_Growth, year, WACC, FCF)

        EBITDA = []
        Disc_EBITDA = []
        for i in range(len(EBIT)):
            EBITDA.append(EBIT[i] + Dep_Amort[i])
            Disc_EBITDA.append(Disc_EBIT[i] + Disc_Dep_Amort[i])

        #Terminal Value:
        TV = Terminal_Value(FCF, EBITDA, WACC, Sector_Growth, Exit_Multiple, year, Type_GP_EM)

        EV = sum(Disc_FCF) - Disc_FCF[0] + TV
        EQ_Value = EV - Total_Debt + Cash
        Inst_Value = EQ_Value / Shares_Outstanding

        return EV, EQ_Value, Inst_Value

    except Exception as ex:
        print(ex)


def Terminal_Value(FCF, EBITDA, WACC, CAGR, Exit_Multiple, year, Type_GP_EM = 'GP'):
    Type_GP_EM = Type_GP_EM.upper()
    if Type_GP_EM == 'GP':
        TV = (abs(FCF[-1]) *  (CAGR) + FCF[-1]) / (WACC - CAGR)
        disc = ((1 + WACC) ** (year * 2 + 1))
        TV = TV /disc

    else:
        TV = Exit_Multiple * EBITDA[-1]
        disc = ((1 + WACC) ** (year * 2 + 1))
        TV = TV / disc

    return TV

def Projection_Calcultation(CAGR, Sector_Growth, year, WACC, value):
    row = []
    disc_row = []
    row.append(value)
    disc_row.append(value)
    #First stage growth
    for i in range(year):
        arr = abs(row[-1]) * CAGR + row[-1]
        row.append(arr)
        arr = arr / ((1 + WACC) ** (i + 1))
        disc_row.append(arr)

    #Second Stage Growth
    Change_Growth = (Sector_Growth - CAGR) / year
    for i in range(year):
        CAGR = CAGR + Change_Growth
        arr = abs(row[-1]) * CAGR + row[-1]
        row.append(arr)
        arr = arr / (1 + WACC) ** (year + i + 1)
        disc_row.append(arr)

    return row, disc_row

Result = DCF_Valuation(ticker[0], growth, Input_WACC, year, 'Exit')

print(Result)

#PE
#PE_TTM
#PB
#EBITDA Multiple

def Comparable_Valuation (ticker):
    try:
        #Gather Inputs:
        Ticker_Price, Ticker_Profile, Ticker_Data, Sector_Profile = Valuation_Inputs(ticker)

        #Ticker Values
        Earnings = Ticker_Data[5]
        Equity = Ticker_Data[8]
        Shares_Outstanding = Equity / Ticker_Price
        EBITDA = Ticker_Data[9] + Ticker_Data[14]
        Total_Debt = Ticker_Data[6] + Ticker_Data[7]
        Cash = Ticker_Data[19]

        Valuation_Values = [Earnings / Shares_Outstanding, Equity, EBITDA]


        #Comp_Valuations
        fieldstogather = ['PE, PB, EBITDA_Multiple']

        #Find Similar Companies
        Industry = Ticker_Profile[3]
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT * FROM stockprofile where Sector = '%s'" %Industry
        mycursor.execute(sql)
        result = mycursor.fetchall()
        mycursor.close()
        db.close()

        cols = [4, 6, 8]
        industry_valuation = []
        i = 0
        for col in cols:
            arr = []
            for j in range(len(result)):
                arr.append(result[j][col])

            arr = pd.DataFrame(arr)
            Q1 = arr.quantile(0.25)
            Q3 = arr.quantile(0.75)
            IQR = Q3 - Q1
            arr = arr[~((arr < (Q1 - IQR)) | (arr > (Q3 + IQR)))]
            arr = arr / Valuation_Values[i]
            industry_valuation.append(float(arr.mean()))
            i = i + 1

        #Sector_Group
        Sector = Ticker_Profile[1]
        print(Sector)
        db = connectToDb()
        mycursor = db.cursor(buffered=True)
        sql = "SELECT * FROM stockprofile where Sector = '%s'" %Sector
        mycursor.execute(sql)
        result = mycursor.fetchall()
        mycursor.close()
        db.close()

        if Sector != None:
            cols = [4, 6, 8]
            sector_valuation = []
            i = 0
            for col in cols:
                arr = []
                for j in range(len(result)):
                    arr.append(result[j][col])

                arr = pd.DataFrame(arr)
                Q1 = arr.quantile(0.25)
                Q3 = arr.quantile(0.75)
                IQR = Q3 - Q1
                arr = arr[~((arr < (Q1 - IQR)) | (arr > (Q3 + IQR)))]
                arr = arr / Valuation_Values[i]
                sector_valuation.append(float(arr.mean()))
                i = i + 1
        else:
            sector_valuation = None

        return industry_valuation, sector_valuation

    except Exception as ex:
        print(ex)
