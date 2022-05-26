import requests
from datetime import datetime,timedelta,date
from pathlib import Path
import zipfile
import pandas as pd
import mysql.connector
import math
from dateutil.relativedelta import relativedelta

from utilities.CalendarUtil import check_holiday
from config.config import Props
from utilities.redis import save_pdc_snap

def file_download():
    # Setting date to download Bhav and Contract master
    check_file_date = check_holiday(datetime.now())
    print("Is %s is Holiday? %s"%(datetime.now().strftime("%d%b%Y").upper(),'Yes' if check_file_date['Holiday'] else 'No'))
    file_date = check_file_date['Date'].strftime("%d%b%Y").upper()
    check_yesterday_date = check_holiday(datetime.now()- timedelta(days=1))
    print("Yesterday %s is Holiday? %s" % ((datetime.now()- timedelta(days=1)).strftime("%d%b%Y").upper(), 'Yes' if check_yesterday_date['Holiday'] else 'No'))
    yesterday_date = check_yesterday_date['Date']

    yesterday_file_date = yesterday_date.strftime("%d%b%Y").upper()
    yesterday_file_month = yesterday_date.strftime("%b").upper()
    yesterday_file_year = yesterday_date.strftime('%Y')
    print(file_date,yesterday_file_date)
    # Setting Path to store file
    path_to_file_contract = Props.PATH_CONTRACT_MASTER+'%s_NSE_FO.csv'%file_date
    path_contract = Path(path_to_file_contract)
    path_to_file_bhav = Props.PATH_BHAV_ZIP+'%s_BHAV_COPY.csv.zip'%yesterday_file_date
    path_bhav = Path(path_to_file_bhav)

    # Download Contract Master File
    if path_contract.is_file():
        print("Contract File Already Exist")
        pass
    else:
        url = 'https://api.zebull.in/rest/Contract/Master/%s/NSE_FO/NSE_FO.csv'%file_date
        r = requests.get(url, allow_redirects=True)
        open(path_to_file_contract, 'wb').write(r.content)
    # Download Bhav copy file and extract it
    if path_bhav.is_file():
        print("Bhav File Already Exist")
        pass
    else:
        url = 'https://www1.nseindia.com/content/historical/DERIVATIVES/%s/%s/fo%sbhav.csv.zip'%(yesterday_file_year,yesterday_file_month,yesterday_file_date)
        r = requests.get(url, allow_redirects=True)
        open(path_to_file_bhav, 'wb').write(r.content)

        with zipfile.ZipFile(path_to_file_bhav, 'r') as zip_ref:
            zip_ref.extractall(Props.PATH_BHAV_CSV)
    if path_contract.is_file() and path_bhav.is_file():
        return {'stat':'ok','msg':'File Downloaded successfully'}
    else:
        return {'stat': 'not_ok', 'msg': 'File not Downloaded properly. Please contact the admin'}

def previous_day_db_push():
    # Set date to get contract and bhav copy file data
    check_file_status =file_download()
    if check_file_status['stat'] == 'ok':
        file_date = datetime.now().strftime("%d%b%Y").upper()
        yesterday_date = date.today() - timedelta(days=1)
        yesterday_file_date = yesterday_date.strftime("%d%b%Y").upper()

        # Contract master and bhav copy file path
        contract_file_path = Props.PATH_CONTRACT_MASTER+'%s_NSE_FO.csv'%file_date
        bhav_file_path = Props.PATH_BHAV_CSV+'/fo%sbhav.csv'%yesterday_file_date

        # Set DataFrame of bhav and contract file
        bhav_df = pd.read_csv(bhav_file_path)
        contract_df = pd.read_csv(contract_file_path)

        # Change the date format in both data frame to merge
        bhav_df['EXPIRY_DT'] = pd.to_datetime(bhav_df['EXPIRY_DT'], format='%d-%b-%Y')
        contract_df['expiry_date'] = pd.to_datetime(contract_df['expiry_date'],format='%Y-%m-%d')

        # Rename bhav file column name
        bhav_df.rename(columns = {'SYMBOL':'symbol', 'INSTRUMENT':'instrument_type', 'EXPIRY_DT':'expiry_date', 'OPTION_TYP':'option_type','STRIKE_PR':'strike_price'}, inplace = True)

        # Filter Required Column in Dataframe
        previous_day = pd.merge(contract_df,bhav_df,on=['instrument_type','symbol','expiry_date','strike_price','option_type'],how='left')

        # Insert previous day data in DB
        mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="amadmin",
            password="amAdmin$2021.08",
            database="optionbees"
        )
        mycursor = mydb.cursor()
        created_on =datetime.now()
        created_by = "CODIFI_BOT"

        for i in range(len(previous_day)):
            underlying = previous_day['symbol'][i]
            if previous_day['option_type'][i] != 'XX':
                formatted_ins_name = previous_day['symbol'][i] + ' ' + previous_day['expiry_date'][i].strftime("%d%b").upper() + ' ' + str(round(previous_day['strike_price'][i])) + ' ' + previous_day['option_type'][i]
                trading_symbol = previous_day['symbol'][i]+ previous_day['expiry_date'][i].strftime("%d%b%y").upper() + str(round(previous_day['strike_price'][i])) + previous_day['option_type'][i]
            else:
                formatted_ins_name = previous_day['symbol'][i] + ' ' + previous_day['expiry_date'][i].strftime("%d%b").upper() + ' FUT'
                trading_symbol = previous_day['symbol'][i]  + previous_day['expiry_date'][i].strftime("%d%b%y").upper() + 'FUT'
            token = previous_day['token'][i]
            instrument_type = previous_day['instrument_type'][i]
            lot_size = previous_day['lot_size'][i]
            tick_size = previous_day['tick_size'][i]
            option_type = previous_day['option_type'][i]
            expiry_date = previous_day['expiry_date'][i]
            exchange_segment = previous_day['exchange_segment'][i]
            strike_price = str(round(float(previous_day['strike_price'][i])))
            if math.isnan(previous_day['CONTRACTS'][i]):
                volume = previous_day['CONTRACTS'][i]
            else:
                volume = str(round(float(previous_day['CONTRACTS'][i])))
            if math.isnan(previous_day['CLOSE'][i]):
                previous_day_close = previous_day['CLOSE'][i]
            else:
                previous_day_close = str(float(previous_day['CLOSE'][i]) * 100)
            previous_day_oi = previous_day['OPEN_INT'][i]
            sql = "INSERT INTO tbl_previous_day_data (underlying,trading_symbol,formatted_ins_name,exch_seg,token,lot_size,tick_size,instrument_type,expiry_date,strike_price,option_type,volume,pdc,pdoi,created_on,created_by) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
            val = (
            underlying, trading_symbol, formatted_ins_name,exchange_segment, token, lot_size, tick_size, instrument_type, expiry_date,
            strike_price, option_type, volume, previous_day_close, previous_day_oi, created_on, created_by)
            mycursor.execute(sql % val)
        mydb.commit()
        mycursor.close()
        mydb.close()
    else:
        return {'stat':'not_ok','msg':'There is an issue in file download, Please check'}

def optmized_previous_day_db_push():
    # Set date to get contract and bhav copy file data
    check_file_status =file_download()
    if check_file_status['stat'] == 'ok':
        file_date = check_holiday(datetime.now())['Date'].strftime("%d%b%Y").upper()
        yesterday_date = check_holiday(date.today() - timedelta(days=1))['Date']
        yesterday_file_date = yesterday_date.strftime("%d%b%Y").upper()
        # Contract master and bhav copy file path
        contract_file_path = Props.PATH_CONTRACT_MASTER + '%s_NSE_FO.csv' % file_date
        bhav_file_path = Props.PATH_BHAV_CSV + '/fo%sbhav.csv' % yesterday_file_date

        # Set DataFrame of bhav and contract file
        contract_df = pd.read_csv(contract_file_path)
        bhav_df = pd.read_csv(bhav_file_path)
        bhav_df['EXPIRY_DT'] = pd.to_datetime(bhav_df['EXPIRY_DT'], format='%d-%b-%Y')

        contract_df['strike_price'] = contract_df['strike_price'].replace([-1.0], 0.0)
        contract_df['expiry_date'] = pd.to_datetime(contract_df['expiry_date'], format='%Y-%m-%d')
        bhav_df.rename(columns={'SYMBOL': 'symbol', 'INSTRUMENT': 'instrument_type', 'EXPIRY_DT': 'expiry_date',
                                'OPTION_TYP': 'option_type', 'STRIKE_PR': 'strike_price'}, inplace=True)
        today_contract_master_df = pd.merge(contract_df, bhav_df,
                                            on=['instrument_type', 'symbol', 'expiry_date', 'strike_price',
                                                'option_type'], how='left')
        pd.set_option('display.max_columns', None)
        nifty_bnifty_expiry_list = today_contract_master_df[
            (today_contract_master_df['symbol'] == 'BANKNIFTY') | (today_contract_master_df['symbol'] == 'NIFTY')][
            'expiry_date'].drop_duplicates().dt.strftime('%Y-%m-%d').to_list()
        fin_midc_nifty_expiry_list = today_contract_master_df[
            (today_contract_master_df['symbol'] == 'FINNIFTY') | (today_contract_master_df['symbol'] == 'MIDCPNIFTY')][
            'expiry_date'].drop_duplicates().dt.strftime('%Y-%m-%d').to_list()
        optstk_expiry_list = today_contract_master_df[today_contract_master_df['instrument_type'] == 'OPTSTK'][
            'expiry_date'].drop_duplicates().dt.strftime('%Y-%m-%d').to_list()
        for init in range(0, 3):
            current_month_expiry = None
            next_month_expiry = None
            current_month = datetime.now().strftime('%m')
            current_month_year = datetime.now().strftime('%Y')
            next_month = (datetime.now() + relativedelta(months=1)).strftime('%m')
            next_month_year = (datetime.now() + relativedelta(months=1)).strftime('%Y')

            if init == 0:
                expiry_list = nifty_bnifty_expiry_list
            if init == 1:
                expiry_list = fin_midc_nifty_expiry_list
            if init == 2:
                expiry_list = optstk_expiry_list
            for i in range(len(expiry_list)):
                if datetime.strptime(expiry_list[i], '%Y-%m-%d').strftime("%m") == current_month and datetime.strptime(
                        expiry_list[i], '%Y-%m-%d').strftime("%Y") == current_month_year:
                    current_month_expiry = expiry_list[i]
                if datetime.strptime(expiry_list[i], '%Y-%m-%d').strftime("%m") == next_month and datetime.strptime(
                        expiry_list[i], '%Y-%m-%d').strftime("%Y") == next_month_year:
                    next_month_expiry = expiry_list[i]
            next_three_month_expiry = []
            if i != 2:
                for i in range(len(expiry_list)):
                    if len(next_three_month_expiry) == 3:
                        break
                    else:
                        if (expiry_list[i] != current_month_expiry) and (expiry_list[i] != next_month_expiry):
                            next_three_month_expiry.append(expiry_list[i])
            next_three_month_expiry.append(current_month_expiry)
            next_three_month_expiry.append(next_month_expiry)
            if init == 0:
                filter_b_nifty_df = today_contract_master_df[(today_contract_master_df['symbol'] == 'NIFTY') | (
                            today_contract_master_df['symbol'] == 'BANKNIFTY')]
                b_nifty_df = filter_b_nifty_df[filter_b_nifty_df.expiry_date.isin(next_three_month_expiry)]
            if init == 1:
                filter_fin_midc_nifty_df = today_contract_master_df[
                    (today_contract_master_df['symbol'] == 'FINNIFTY') | (
                                today_contract_master_df['symbol'] == 'MIDCPNIFTY')]
                fin_midc_nifty_df = filter_fin_midc_nifty_df[
                    filter_fin_midc_nifty_df.expiry_date.isin(next_three_month_expiry)]
            if init == 2:
                filter_optstk_df = today_contract_master_df[
                    (today_contract_master_df['instrument_type'] == 'OPTSTK') | (
                                today_contract_master_df['instrument_type'] == 'FUTSTK')]
                optstk_df = filter_optstk_df[filter_optstk_df.expiry_date.isin(next_three_month_expiry)]
        previous_day = pd.concat([b_nifty_df, fin_midc_nifty_df, optstk_df]).to_dict('records')
        # Insert previous day data in DB
        mydb = mysql.connector.connect(
            host=Props.DB_HOST,
            user=Props.DB_USER,
            password=Props.DB_PASSWORD,
            database=Props.DB_SCHEMA
        )
        mycursor = mydb.cursor()
        created_on =datetime.now()
        created_by = "CODIFI_BOT"
        for i in range(len(previous_day)):
            underlying = previous_day[i]['symbol']
            if previous_day[i]['option_type'] != 'XX':
                formatted_ins_name = previous_day[i]['symbol'] + ' ' + previous_day[i]['expiry_date'].strftime("%d%b").upper() + ' ' + str(round(previous_day[i]['strike_price'])) + ' ' + previous_day[i]['option_type']
                trading_symbol = previous_day[i]['symbol']+ previous_day[i]['expiry_date'].strftime("%d%b%y").upper() + str(round(previous_day[i]['strike_price'])) + previous_day[i]['option_type']
            else:
                formatted_ins_name = previous_day[i]['symbol'] + ' ' + previous_day[i]['expiry_date'].strftime("%d%b").upper() + ' FUT'
                trading_symbol = previous_day[i]['symbol'] + previous_day[i]['expiry_date'].strftime("%d%b%y").upper() + 'FUT'
            token = previous_day[i]['token']
            instrument_type = previous_day[i]['instrument_type']
            lot_size = previous_day[i]['lot_size']
            tick_size = str(int(previous_day[i]['tick_size'])/100)
            option_type = previous_day[i]['option_type']
            expiry_date = previous_day[i]['expiry_date']
            exchange_segment = previous_day[i]['exchange_segment']
            strike_price = str(round(float(previous_day[i]['strike_price'])))
            if math.isnan(previous_day[i]['CONTRACTS']):
                volume = previous_day[i]['CONTRACTS']
            else:
                volume = str(round(float(previous_day[i]['CONTRACTS'])))
            if math.isnan(previous_day[i]['CLOSE']):
                previous_day_close = previous_day[i]['CLOSE']
            else:
                previous_day_close = str(float(previous_day[i]['CLOSE']) * 100)
            previous_day_oi = previous_day[i]['OPEN_INT']
            sql = "INSERT INTO tbl_previous_day_data (underlying,trading_symbol,formatted_ins_name,exch_seg,token,lot_size,tick_size,instrument_type,expiry_date,strike_price,option_type,volume,pdc,pdoi,created_on,created_by) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
            val = (
            underlying, trading_symbol, formatted_ins_name,exchange_segment, token, lot_size, tick_size, instrument_type, expiry_date,
            strike_price, option_type, volume, previous_day_close, previous_day_oi, created_on, created_by)
            redis_data_push = {
                "underlying": underlying,
                "trading_symbol": trading_symbol,
                "formatted_ins_name":formatted_ins_name,
                "exchange_segment": exchange_segment,
                "token": token,
                "lot_size": lot_size,
                "tick_size": tick_size,
                "instrument_type":instrument_type,
                "expiry_date":str(int(datetime.timestamp(expiry_date))),
                "strike_price":strike_price,
                "option_type":option_type,
                "volume":volume,
                "previous_day_close":previous_day_close,
                "previous_day_oi":previous_day_oi,
                "created_on":str(int(datetime.timestamp(created_on)))
            }
            mycursor.execute(sql % val)
            save_pdc_snap(redis_data_push)
        mydb.commit()

        mycursor.close()
        mydb.close()
    else:
        return {'stat':'not_ok','msg':'There is an issue in file download, Please check'}



optmized_previous_day_db_push()
