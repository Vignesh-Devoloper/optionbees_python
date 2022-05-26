import requests
from datetime import datetime
import mysql.connector
import pandas as pd
import schedule
import time
import sys

# Nimblerest Access request data
accessKey = '9ca07521-938f-425c-abda-8edda85d1f22'
exchange = 'NFO'
periodicity = 'Minute'
period = '1'

def db_push():
    print("Process Start in ", datetime.now())
    t1 = datetime.now()
    url = "http://nimblerest.lisuns.com:4531/getexchangesnapshot/?accessKey=%s&exchange=%s&periodicity=%s&period=%s" % (accessKey, exchange, periodicity, period)

    payload = {}
    headers = {}
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        print("Error : ",e,", Please Contact admin")
    Flag = True
    while(Flag):
        if response.text == 'Authentication request received. Try request data in next moment.':
            time.sleep(1)
            response = requests.request("GET", url, headers=headers, data=payload)
            print("Error! Delaying Request 1 Second")
        else:
            Flag = False

    response_data = response.json()
    current_datetime = datetime.now()

    API_fetched_ltt = datetime.fromtimestamp(response_data['EXCHANGESNAPSHOTITEMS'][0]['LASTTRADETIME'] / 1000)
    data = response_data['EXCHANGESNAPSHOTITEMS'][0]['SNAPSHOTITEMS']

    df = pd.DataFrame(data)

    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="amadmin",
        password="amAdmin$2021.08",
        database="optionbees"
    )

    mycursor = mydb.cursor()

    for i in range(len(df)):
        created_by = 'CODIFI_BOT'
        INSTRUMENTIDENTIFIER = df['INSTRUMENTIDENTIFIER'][i].split('_')
        Instrument_Type = INSTRUMENTIDENTIFIER[0]
        Instrument_Name = INSTRUMENTIDENTIFIER[1]
        Instrument_Date = datetime.strptime(INSTRUMENTIDENTIFIER[2], '%d%b%Y')
        lt_time = datetime.fromtimestamp(df['LASTTRADETIME'][i] / 1000)
        if len(INSTRUMENTIDENTIFIER) > 3:
            Instrument_Option_type = INSTRUMENTIDENTIFIER[3]
            Instrument_Strike_price = INSTRUMENTIDENTIFIER[4]
        else:
            Instrument_Option_type = ''
            Instrument_Strike_price = ''
        # sql = "INSERT INTO api_fetch_data (created_on,created_by,Exchange,Instrument_identifier,Last_trade_time,Traded_qty,Open_intrest,Open,High,Low,Close,Token_number,Instrument_type,Symbol_Name,Expiry_Date,Option_type,Strike_Price,API_fetched_ltt) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
        # val = (
        # current_datetime, created_by, df['EXCHANGE'][i], df['INSTRUMENTIDENTIFIER'][i], lt_time, df['TRADEDQTY'][i],
        # df['OPENINTEREST'][i], df['OPEN'][i], df['HIGH'][i], df['LOW'][i], df['CLOSE'][i], df['TOKENNUMBER'][i],
        # Instrument_Type, Instrument_Name, Instrument_Date, Instrument_Option_type, Instrument_Strike_price,
        # API_fetched_ltt)
        sql = "INSERT INTO gdfl_table (token,open,high,low,close,volume,oi,ltp,updated_time,created_on,created_by) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
        val = (df['TOKENNUMBER'][i],df['OPEN'][i],df['HIGH'][i],df['LOW'][i],df['CLOSE'][i],df['TRADEDQTY'][i],df['OPENINTEREST'][i],df['CLOSE'][i],API_fetched_ltt,current_datetime, created_by)
        mycursor.execute(sql % val)
        mydb.commit()
    mycursor.close()
    mydb.close()
    t2 = datetime.now()    # Process Completed Time
    print("Process Completed in ",datetime.now())
    print("Took ",(t2-t1),"Seconds to complete")

def gdfl_push():
    print("Process Start in ", datetime.now())
    t1 = datetime.now()
    url = "http://nimblerest.lisuns.com:4531/getexchangesnapshot/?accessKey=%s&exchange=%s&periodicity=%s&period=%s" % (accessKey, exchange, periodicity, period)

    payload = {}
    headers = {}
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        print("Error : ",e,", Please Contact admin")
    Flag = True
    while(Flag):
        if response.text == 'Authentication request received. Try request data in next moment.':
            time.sleep(1)
            response = requests.request("GET", url, headers=headers, data=payload)
            print("Error! Delaying Request 1 Second")
        else:
            Flag = False

    response_data = response.json()
    current_datetime = datetime.now()

    API_fetched_ltt = datetime.fromtimestamp(response_data['EXCHANGESNAPSHOTITEMS'][0]['LASTTRADETIME'] / 1000)
    data = response_data['EXCHANGESNAPSHOTITEMS'][0]['SNAPSHOTITEMS']

    df = pd.DataFrame(data)

    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="amadmin",
        password="amAdmin$2021.08",
        database="optionbees"
    )

    mycursor = mydb.cursor()

    for i in range(len(df)):
        created_by = 'CODIFI_BOT'
        sql = "INSERT INTO api_fetch_data (token,open,high,low,close,volume,oi) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
        val = (df['TOKENNUMBER'][i], df['OPEN'][i], df['HIGH'][i], df['LOW'][i], df['CLOSE'][i], df['TRADEDQTY'][i],df['OPENINTEREST'][i],API_fetched_ltt,current_datetime, created_by)
        mycursor.execute(sql % val)
        mydb.commit()
    mycursor.close()
    mydb.close()
    t2 = datetime.now()    # Process Completed Time
    print("Process Completed in ",datetime.now())
    print("Took ",(t2-t1),"Seconds to complete")


def trigger():
    db_push()
    schedule.every(1).minutes.do(db_push)

def stop_trigger():
    print("System Exit at ",datetime.now())
    sys.exit()

trigger()
# schedule.every().day.at("09:15").do(trigger)
# schedule.every().day.at('15:30').do(stop_trigger)

while True:
    schedule.run_pending()
    time.sleep(1)
