from flask import Flask, request
import mysql.connector
# from mysql.connector.errors import Error
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import redis
import json
# Defined Library functions
from config.config import Props
from utilities.redis import redis_get_token_list,redis_get_key_value

app = Flask(__name__)

def current_future_expiry(symbol,expiry):
    token = symbol+"_0_XX"
    redis_token = redis_get_token_list(token)
    redis_token_list = [token.decode('utf-8').split('_') for token in redis_token]
    redis_token_list_filter = []
    for i in range(len(redis_token_list)):
        if redis_token_list[i][0] == symbol:
            redis_token_list_filter.append(redis_token_list[i])
    redis_fut_exp_list = [datetime.fromtimestamp(int(dt[3])) for dt in redis_token_list_filter]
    required_fut_expy = expiry
    for i in range(len(redis_fut_exp_list)):
        if redis_fut_exp_list[i].month == expiry.month:
            required_fut_expy = redis_fut_exp_list[i]
    return required_fut_expy

@app.route("/server_check",methods=['POST'])
def server_check():
    return "Connected"

@app.route("/get_underlying",methods=['POST'])
def get_underlying():
    mydb = mysql.connector.connect(
        host=Props.DB_HOST,
        user=Props.DB_USER,
        password=Props.DB_PASSWORD,
        database=Props.DB_SCHEMA
    )

    mycursor = mydb.cursor()
    try:
        try:
            mycursor.execute("SELECT DISTINCT underlying FROM tbl_previous_day_data")
            # NB : you won't get an IntegrityError when reading
        except (mysql.connector.Error, mysql.connector.Warning) as e:
            # print(e)
            return {'stat':'Not_ok','emsg':str(e)}
        try:
            db_underlying_fetched_data = mycursor.fetchall()
        except TypeError as e:
            # print(e)
            return {'stat':'Not_ok','emsg':str(e)}
    finally:
        mydb.close()

    list_underlying = [db_underlying_fetched_data[i][0] for i in range(len(db_underlying_fetched_data))]


    return {'list_underlying': list_underlying , 'stat':'ok'}

@app.route("/get_expiry_underlying",methods=['POST'])
def get_expiry_underlying():
    data = request.get_json()
    # print(data)
    mydb = mysql.connector.connect(
        host=Props.DB_HOST,
        user=Props.DB_USER,
        password=Props.DB_PASSWORD,
        database=Props.DB_SCHEMA
    )

    mycursor = mydb.cursor()
    try:
        try:
            mycursor.execute("SELECT  distinct expiry_date FROM tbl_previous_day_data where underlying = '%s';"%data['underlying'])
            # NB : you won't get an IntegrityError when reading
        except (mysql.connector.Error, mysql.connector.Warning) as e:
            # print(e)
            return {'stat': 'Not_ok', 'emsg': str(e)}
        try:
            db_underlying_expiry_fetched_data = mycursor.fetchall()
        except TypeError as e:
            # print(e)
            return {'stat': 'Not_ok', 'emsg': str(e)}
    finally:
        mydb.close()
    # print(type(db_underlying_expiry_fetched_data[0][0]))
    # list_underlying_expiry = [datetime.strptime(db_underlying_expiry_fetched_data[i][0],'%Y-%m-%d %H:%M:%S').strftime("%Y%b%d").upper() for i in range(len(db_underlying_expiry_fetched_data))]
    list_underlying_expiry = [datetime.strptime(db_underlying_expiry_fetched_data[i][0], '%Y-%m-%d %H:%M:%S') for i in range(len(db_underlying_expiry_fetched_data))]

    current_month_expiry = None
    for i in range(len(list_underlying_expiry)):
        if list_underlying_expiry[i].strftime('%m') == datetime.now().strftime('%m') and list_underlying_expiry[i].strftime('%Y') == datetime.now().strftime('%Y'):
            current_month_expiry = list_underlying_expiry[i].strftime('%d%b%y').upper()
    next_month_expiry = 0
    for i in range(len(list_underlying_expiry)):
        if list_underlying_expiry[i].strftime('%m') == (datetime.now() + relativedelta(months=1)).strftime('%m') and list_underlying_expiry[i].strftime('%Y') == (datetime.now() + relativedelta(months=1)).strftime('%Y'):
            next_month_expiry = list_underlying_expiry[i].strftime('%d%b%y').upper()
    next_three_expiry = []
    if data['underlying'].upper() == 'NIFTY' or data['underlying'].upper() == 'BANKNIFTY' or data['underlying'].upper() == 'FINNIFTY' or data['underlying'].upper() == 'MIDCPNIFTY':
        for i in range(len(list_underlying_expiry)):
            if list_underlying_expiry[i].strftime('%m') == datetime.now().strftime('%m') and list_underlying_expiry[i].strftime('%Y') == datetime.now().strftime('%Y') and current_month_expiry != list_underlying_expiry[i].strftime('%d%b%y').upper():
                next_three_expiry.append(list_underlying_expiry[i].strftime('%d%b%y').upper())
            if len(next_three_expiry) < 3:
                if list_underlying_expiry[i].strftime('%m') == (datetime.now() + relativedelta(months=1)).strftime('%m') and list_underlying_expiry[i].strftime('%Y') == (datetime.now() + relativedelta(months=1)).strftime('%Y') and next_month_expiry != list_underlying_expiry[i].strftime('%d%b%y').upper():
                    next_three_expiry.append(list_underlying_expiry[i].strftime('%d%b%y').upper())
                if len(next_three_expiry) >= 3:
                    break
    next_three_expiry.append(current_month_expiry)
    next_three_expiry.append(next_month_expiry)
    return {
            "underlying":data['underlying'].upper(),
            "underlying_expiry":next_three_expiry,
            "stat":"Ok"
            }

@app.route("/get_strike_data",methods=['POST'])
def get_strike_data():
    t1 = datetime.now()
    data = request.get_json()
    symbol = data['underlying']
    expiry = datetime.strptime(data['expiry'],'%d%b%y')
    interval = data['interval']
    # print(symbol,expiry,interval)
    # mydb = mysql.connector.connect(
    #     host=Props.DB_HOST,
    #     user=Props.DB_USER,
    #     password=Props.DB_PASSWORD,
    #     database=Props.DB_SCHEMA
    # )
    #
    # mycursor = mydb.cursor()
    # try:
    #     try:
    #         mycursor.execute("SELECT  * FROM tbl_previous_day_data where underlying = '%s' and expiry_date= '%s';" % (symbol, expiry))
    #         # NB : you won't get an IntegrityError when reading
    #     except (mysql.connector.Error, mysql.connector.Warning) as e:
    #         # print(e)
    #         return {'stat': 'Not_ok', 'emsg': str(e)}
    #     try:
    #         db_data = mycursor.fetchall()
    #         field_names = [i[0] for i in mycursor.description]
    #     except TypeError as e:
    #         # print(e)
    #         return {'stat': 'Not_ok', 'emsg': str(e)}
    # finally:
    #     mydb.close()
    # df_data = pd.DataFrame(db_data, columns=field_names)
    # underlying_fut_script = df_data[(df_data['underlying'] == symbol) & (df_data['expiry_date'] == expiry) & (
    #             df_data['option_type'] == 'XX')].values.tolist()
    # token_underlying_fut_script = ''
    # if len(underlying_fut_script) != 0:
    #     token_underlying_fut_script = underlying_fut_script[0][5]
    # spot = 16200
    # min_spot_value = str(spot - (interval * lot_size))
    # max_spot_value = str(spot + (interval * lot_size))
    # total_strike_data = df_data.loc[(df_data['strike_price'] >= min_spot_value) & (df_data['strike_price'] <= max_spot_value)]

    redis_token = redis_get_token_list(symbol)
    redis_token_list = [data.decode('utf-8').split('_') for data in redis_token]
    redis_token_list_df = pd.DataFrame(redis_token_list)
    t2 = datetime.now()
    fut_expiry = current_future_expiry(symbol,expiry)
    underlying_fut_script_key = symbol + '_0_XX_' + str(int(datetime.timestamp(fut_expiry)))

    underlying_fut_script_value = redis_get_key_value(underlying_fut_script_key)
    print("Check :", underlying_fut_script_key)
    underlying_fut_script = json.loads(underlying_fut_script_value.decode('utf-8'))
    token_underlying_fut_script = ''
    if underlying_fut_script:
        token_underlying_fut_script = underlying_fut_script['token']
    t3 = datetime.now()

    if symbol.upper() == "BANKNIFTY":
        lot_size = 100
        spot = 34300
    else:
        lot_size =int(underlying_fut_script['lot_size'])
        spot = 16050
    min_spot_value = str(spot - (interval * lot_size))
    max_spot_value = str(spot + (interval * lot_size))
    strike_list = redis_token_list_df[(redis_token_list_df[0] == symbol) & (redis_token_list_df[2] != 'XX') & (redis_token_list_df[3] == str(int(datetime.timestamp(expiry))))]
    total_strike_data_keys = strike_list.loc[(strike_list[1] >= min_spot_value) & (strike_list[1] <= max_spot_value)].values.tolist()

    total_strike_data_json = []
    t4 = datetime.now()
    for i in range(len(total_strike_data_keys)):
        keys = total_strike_data_keys[i][0] + '_' + total_strike_data_keys[i][1] + '_' + total_strike_data_keys[i][2] + '_' + total_strike_data_keys[i][3]
        key_value = redis_get_key_value(keys)
        # print(key_value,type(key_value))
        total_strike_data_json.append(json.loads(key_value.decode('utf-8')))
    total_strike_data = pd.DataFrame(total_strike_data_json)

    lot_size = total_strike_data.values.tolist()[0][5]
    tick_size = total_strike_data.values.tolist()[0][6]
    exch_seg = total_strike_data.values.tolist()[0][3]
    t5 = datetime.now()
    strikes = []
    distinct_strike_price = total_strike_data['strike_price'].drop_duplicates().tolist()

    for i in range(len(distinct_strike_price)):
        call = total_strike_data[(total_strike_data['strike_price'] == distinct_strike_price[i]) & (
                    total_strike_data['option_type'] == 'CE')].to_dict('records')
        put = total_strike_data[(total_strike_data['strike_price'] == distinct_strike_price[i]) & (
                    total_strike_data['option_type'] == 'PE')].to_dict('records')

        data = redis_get_token_list(call[0]['token']) #r.execute_command('KEYS *%s*' % call[0]['token'])
        data = [data.decode('ascii').split('_') for data in data]

        if len(data) > 0:
            keys_df = pd.DataFrame(data)
            # print("keys_df :",keys_df)
            if 2 in keys_df:
                keys_df = keys_df[(keys_df[2] !='PE')&(keys_df[2] !='CE')&(keys_df[2] !='XX')]
            # print("keys_df :", keys_df)
            pd.to_datetime(keys_df[1], unit='s')
            to_get_key_list = keys_df.sort_values(by=1).values.tolist()[len(keys_df) - 1]
            to_get_key = to_get_key_list[0] + '_' + to_get_key_list[1]
            to_get_key_value = redis_get_key_value(to_get_key)
            # print("key :",to_get_key,type(to_get_key_value))
            redis_call_get = to_get_key_value.decode('ascii').replace("'",'"')
            if len(redis_call_get) > 0:
                # print("Redis Call -------->",redis_call_get)
                call_gdfl_data = json.loads(redis_call_get)
            else:
                call_gdfl_data = {"oi":"","ltp":""}
        else:
            call_gdfl_data = {"oi":"","ltp":""}

        call_data = {
            "oi": call_gdfl_data['oi'],
            "token": call[0]['token'],
            "pdc": call[0]['previous_day_close'],
            "ltp": call_gdfl_data['ltp'],
            "pdoi": call[0]['previous_day_oi'],
            "forInsName": call[0]['formatted_ins_name']
        }
        data = redis_get_token_list(put[0]['token']) # r.execute_command('KEYS *%s*' % put[0]['token'])
        data = [data.decode('ascii').split('_') for data in data]

        if len(data) > 0:
            keys_df = pd.DataFrame(data)
            pd.to_datetime(keys_df[1], unit='s')
            to_get_key_list = keys_df.sort_values(by=1).values.tolist()[len(keys_df) - 1]
            to_get_key = to_get_key_list[0] + '_' + to_get_key_list[1]
            redis_put_get = redis_get_key_value(to_get_key).decode('ascii').replace("'",'"')
            if len(redis_put_get) > 0:
                put_gdfl_data = json.loads(redis_put_get)
            else:
                put_gdfl_data = {"oi": "", "ltp": ""}
        else:
            put_gdfl_data = {"oi":"","ltp":""}
        put_data = {
            "oi": put_gdfl_data['oi'],
            "token": put[0]['token'],
            "pdc": put[0]['previous_day_close'],
            "ltp": put_gdfl_data['ltp'],
            "pdoi": put[0]['previous_day_oi'],
            "forInsName": put[0]['formatted_ins_name']
        }
        data = {"strikeprice": distinct_strike_price[i], "PE": put_data, "CE": call_data}
        strikes.append(data)
    # fut_token_ltp=''
    # gdfl_fut_token_filter = gdfl_df[gdfl_df['token'] == token_underlying_fut_script]
    # if len(gdfl_fut_token_filter) != 0:
    #     latest_number = len(gdfl_fut_token_filter) - 1
    #     fut_token_ltp = gdfl_fut_token_filter.values.tolist()[latest_number][8]
    data = redis_get_token_list(token_underlying_fut_script) # r.execute_command('KEYS *%s*' % token_underlying_fut_script)
    data = [data.decode('ascii').split('_') for data in data]
    t6 = datetime.now()
    if len(data) > 0:
        keys_df = pd.DataFrame(data)
        pd.to_datetime(keys_df[1], unit='s')
        to_get_key_list = keys_df.sort_values(by=1).values.tolist()[len(keys_df) - 1]
        to_get_key = to_get_key_list[0] + '_' + to_get_key_list[1]
        redis_fut_get = redis_get_key_value(to_get_key).decode('ascii').replace("'",'"')
        if len(redis_fut_get) > 0:
            future_gdfl_data = json.loads(redis_fut_get)
        else:
            future_gdfl_data = {"ltp":""}
    else:
        future_gdfl_data = {"ltp":""}
    t7 = datetime.now()
    send_data = {
        "stat": "Ok",
        "result": {
            "lotsize": lot_size,
            "ticksize": tick_size,
            "exchange": exch_seg,
            "spotLTP": "",
            "spotToken": "",
            "futLTP": future_gdfl_data['ltp'],
            "futToken": token_underlying_fut_script,
            "data": {}
        }

    }
    send_data['result']['data'] = strikes
    t8 = datetime.now()
    print("Total Time taken to process :",t8-t1)
    print("Token Fetch :",t2-t1)
    print("underlying_fut_script:",t3-t2)
    print("Strike Key List  : ",t4-t3)
    print("Strike Key Value list :",t5-t4)
    print("Strike Value json :",t6-t5)
    print("Fut:",t7-t6)
    print("Data:",t8-t7)
    # print(send_data)
    return send_data