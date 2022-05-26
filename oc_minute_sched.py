import requests
from datetime import datetime
import mysql.connector
import pandas as pd
import time,sched
import sys
from utilities.redis import save_minute_snap
from config.config import Props


schedule_processor = sched.scheduler(time.time, time.sleep)

def db_push(schedule_processor_second):
    print("Process Start in ", datetime.now())
    t1 = datetime.now()
    url = Props.GDFL_REST_URL
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
        host=Props.DB_HOST,
        user=Props.DB_USER,
        password=Props.DB_PASSWORD,
        database=Props.DB_SCHEMA
    )

    mycursor = mydb.cursor()

    for i in range(len(df)):
        created_by = 'CODIFI_BOT'
        # INSTRUMENTIDENTIFIER = df['INSTRUMENTIDENTIFIER'][i].split('_')
        # Instrument_Type = INSTRUMENTIDENTIFIER[0]
        # Instrument_Name = INSTRUMENTIDENTIFIER[1]
        # Instrument_Date = datetime.strptime(INSTRUMENTIDENTIFIER[2], '%d%b%Y')
        # lt_time = datetime.fromtimestamp(df['LASTTRADETIME'][i] / 1000)
        # if len(INSTRUMENTIDENTIFIER) > 3:
        #     Instrument_Option_type = INSTRUMENTIDENTIFIER[3]
        #     Instrument_Strike_price = INSTRUMENTIDENTIFIER[4]
        # else:
        #     Instrument_Option_type = ''
        #     Instrument_Strike_price = ''
        # sql = "INSERT INTO api_fetch_data (created_on,created_by,Exchange,Instrument_identifier,Last_trade_time,Traded_qty,Open_intrest,Open,High,Low,Close,Token_number,Instrument_type,Symbol_Name,Expiry_Date,Option_type,Strike_Price,API_fetched_ltt) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
        # val = (
        # current_datetime, created_by, df['EXCHANGE'][i], df['INSTRUMENTIDENTIFIER'][i], lt_time, df['TRADEDQTY'][i],
        # df['OPENINTEREST'][i], df['OPEN'][i], df['HIGH'][i], df['LOW'][i], df['CLOSE'][i], df['TOKENNUMBER'][i],
        # Instrument_Type, Instrument_Name, Instrument_Date, Instrument_Option_type, Instrument_Strike_price,
        # API_fetched_ltt)
        data = {
            "token": df['TOKENNUMBER'][i],
            "open": str(df['OPEN'][i]),
            "high": str(df['HIGH'][i]),
            "low": str(df['LOW'][i]),
            "close": str(df['CLOSE'][i]),
            "ltp": str(df['CLOSE'][i]),
            "volume": str(df['TRADEDQTY'][i]),
            "oi": str(df['OPENINTEREST'][i]),
            "timestamp": str(int(datetime.timestamp(API_fetched_ltt)))
        }
        save_minute_snap(data)
        sql = "INSERT INTO gdfl_table (token,open,high,low,close,volume,oi,ltp,updated_time,created_on,created_by) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
        val = (df['TOKENNUMBER'][i],df['OPEN'][i],df['HIGH'][i],df['LOW'][i],df['CLOSE'][i],df['TRADEDQTY'][i],df['OPENINTEREST'][i],df['CLOSE'][i],API_fetched_ltt,current_datetime, created_by)
        mycursor.execute(sql % val)
        mydb.commit()
    mycursor.close()
    mydb.close()
    t2 = datetime.now()    # Process Completed Time
    print("Process Completed in ",datetime.now())
    print("Took ",(t2-t1),"Seconds to complete")
    delta = 60 - datetime.now().second
    schedule_processor_second.enter(delta, 1, db_push, (schedule_processor_second,))

schedule_processor.enter(0, 1, db_push, (schedule_processor,))
schedule_processor.run()


