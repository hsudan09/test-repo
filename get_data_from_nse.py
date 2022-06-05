import json
import gspread
from nsepy.history import get_price_list
import pandas as pd
import datetime
from datetime import date
import requests

stock_list = [
        'ACC',
        'AMBUJACEM',
        'AARTIIND',
        'ALKYLAMINE',
        'APOLLOHOSP',
        'ASIANPAINT',
        'AXISBANK',
        'BAJAJ-AUTO',
        'BAJFINANCE',
        'BERGEPAINT',
        'BHARTIARTL',
        'BRITANNIA',
        'BIOCON',
        'BPCL',
        'ZYDUSLIFE',
        'CHOLAFIN',
        'BSE',
        'CDSL',
        'CIPLA',
        'COLPAL',
        'CONCOR',
        'DABUR',
        'DEEPAKNTR',
        'DIVISLAB',
        'DMART',
        'DRREDDY',
        'EICHERMOT',
        'GODREJCP',
        'GRASIM',
        'HAVELLS',
        'HCLTECH',
        'HDFC',
        'HDFCAMC',
        'HDFCLIFE',
        'HEROMOTOCO',
        'ICICIGI',
        'INDIGO',
        'HINDALCO',
        'HINDUNILVR',
        'ICICIBANK',
        'IDEA',
        'IEX',
        'IGL',
        'INDUSINDBK',
        'INFY',
        'ITC',
        'JSWSTEEL',
        'JUBLFOOD',
        'KOTAKBANK',
        'LALPATHLAB',
        'LT',
        'LUPIN',
        'LAURUSLABS',
        'LTI',
        'M&M',
        'MARUTI',
        'MARICO',
        'MCDOWELL-N',
        'MUTHOOTFIN',
        'NAM-INDIA',
        'NAUKRI',
        'PETRONET',
        'NMDC',
        'POWERGRID',
        'NAVINFLUOR',
        'PIDILITIND',
        'PIIND',
        'RELAXO',
        'RELIANCE',
        'SAIL',
        'SBICARD',
        'SBILIFE',
        'SBIN',
        'SIEMENS',
        'SUNPHARMA',
        'TATACONSUM',
        'TATAMOTORS',
        'TATASTEEL',
        'TCS',
        'TECHM',
        'TITAN',
        'TORNTPHARM',
        'ULTRACEMCO',
        'UPL',
        'WIPRO',
        'YESBANK'
        ]

def getDailyPrices():
    dt=date(2022,3,25)
    #dt = datetime.datetime.now()
    t = dt.strftime('%Y%m%d')
    print(t)
    prices = get_price_list(dt)
    df = prices[['SYMBOL', 'OPEN', 'HIGH', 'LOW', 'CLOSE']]
    selected_stocks = df[df['SYMBOL'].isin(stock_list)]
    selected_stocks = selected_stocks.rename(columns={
    'SYMBOL': 'symbol',
    'OPEN': 'open',
    'HIGH': 'high',
    'LOW': 'low',
    'CLOSE': 'close'
    })
    selected_stocks['date'] = t
    print("Returning dataframe")
    return selected_stocks

def writeToSheet(df,rng):
    client = gspread.service_account(filename='credentials.json')
    sheet = client.open("SSTAutomated").get_worksheet(0)
    sheet.update(rng, df.values.tolist())
    print("Write Completed")

def getNextDayRange():
    client = gspread.service_account(filename='credentials.json')
    sheet = client.open("SSTAutomated").get_worksheet(0)
    df = pd.DataFrame(sheet.get_all_records())
    row_count = len(df.index)
    start_range = row_count + 2
    end_range = start_range + 87
    rng = "A"+str(start_range)+":F"+str(end_range)
    return rng


def sendMessage(message):
    base_url = 'https://api.telegram.org/bot1697547636:AAFAWIdhkcyoRWG1SRRXsFVqbTZ2Pizy4zg/sendMessage?chat_id=-504975420&text={}'.format(message)
    requests.get(base_url)


if __name__ == "__main__":
    r = getNextDayRange()
    df = getDailyPrices()
    print(df)

'''
def lambda_handler(event, context):
    
    #r = getNextDayRange()
    #df = getDailyPrices()
    #writeToSheet(df,r)
    #sendMessage("Data Loaded to Google Sheet from NSE - SUCC")
    
    try:
        r = getNextDayRange()
        df = getDailyPrices()
        writeToSheet(df,r)
        sendMessage("Data Loaded to Google Sheet from NSE - SUCC")
    except:
        sendMessage("Getting Data from NSE using Python - FAILED")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
'''