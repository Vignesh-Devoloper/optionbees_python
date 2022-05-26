import redis
import json

redis_connect = redis.Redis(
    host='localhost',
    port='7000')


def save_minute_snap(data):
    key = data['token']+'_%s'%(data['timestamp'])
    redis_connect.set(key, json.dumps(data).encode('utf-8'))
    return 'Ok'

def save_pdc_snap(data):
    print(data['underlying']+'_%s'%(data['expiry_date']))
    key = data['underlying']+'_'+data['strike_price']+'_'+data['option_type']+'_%s'%(data['expiry_date'])
    redis_connect.set(key, json.dumps(data).encode('utf-8'))
    return 'Ok'

def redis_get_token_list(token):
    redis_token = redis_connect.execute_command('KEYS *%s*' % token)
    return redis_token

def redis_get_key_value(key):
    redis_key_value = redis_connect.get(key)
    return redis_key_value

