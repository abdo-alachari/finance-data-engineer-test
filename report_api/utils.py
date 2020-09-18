import pandas as pd
from sqlalchemy import create_engine
import psycopg2, json


connection = create_engine('postgresql://postgres:pwd@172.22.0.2:5432/postgres')

def monthly_report(user_id):
    json_data   = dict()
    monthly_query = """
    SELECT transaction_type,
           To_char(transaction_date, 'Month') AS transaction_month,
           Sum(amount) as monthly_amount
    FROM   transactions.reconciliated_transactions
    WHERE  uuid = '{}' 
    GROUP  BY transaction_type,transaction_month
    """.format(user_id)

    data = pd.read_sql(monthly_query, connection)

    json_string = data.to_json(orient='records')
    tmp = json.loads(json_string)
    json_data[user_id] = tmp
    return json_data



def daily_report(user_id):
    json_data   = dict()
    daily_query = """
    SELECT transaction_type,
           transaction_date::date::text AS transaction_day,
           Sum(amount) as daily_amount
    FROM   transactions.reconciliated_transactions
    WHERE  uuid = '{}' 
    GROUP  BY transaction_type,transaction_day
    """.format(user_id)

    data = pd.read_sql(daily_query, connection)

    json_string = data.to_json(orient='records')
    tmp = json.loads(json_string)
    json_data[user_id] = tmp
    return json_data



def total_report(user_id):   
    json_data   = dict()       
    total_query = """
    SELECT transaction_type,
           Sum(amount) as total_amount
    FROM   transactions.reconciliated_transactions
    WHERE  uuid = '{}' 
    GROUP  BY transaction_type
    """.format(user_id)

    data = pd.read_sql(total_query, connection)

    json_string = data.to_json(orient='records')
    tmp = json.loads(json_string)
    json_data[user_id] = tmp
    return json_data