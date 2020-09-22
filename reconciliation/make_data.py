
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import random
import uuid
import time

# https://stackoverflow.com/a/553320
def str_time_prop(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def random_date(start, end, size):
    unique_dates = set()
    while len(unique_dates) < size : 
        unique_dates.add(str_time_prop(start, end, "%Y-%m-%d %H:%M:%S", random.random())) 
    return list(unique_dates)

def random_uid(size):
    unique_uid = set()
    while len(unique_uid) < size : 
        unique_uid.add(str(uuid.uuid1())) 
    return list(unique_uid)

clients = random_uid(2000)
transactions_size = 10000
client_transactions  = pd.DataFrame(columns=['uuid', 'transaction_type', 'transaction_id', 'transaction_date', 'amount'])
client_transactions['uuid'] = random.choices(clients,k=transactions_size)
client_transactions['transaction_type'] = random.choices(['deposit', 'withdrawal'],k=transactions_size)
client_transactions['amount'] = np.random.uniform(5,1000,transactions_size )
client_transactions['transaction_id'] = random.sample(range(99999999, 9999999999), transactions_size)
client_transactions['transaction_date'] = random_date("2020-04-01 13:30:00", "2020-09-17 16:50:00", transactions_size)
client_transactions['transaction_date'] = pd.to_datetime(client_transactions['transaction_date'])


conn = psycopg2.connect(host='localhost',port="5432",user='postgres',database='postgres', password='pwd') 
cursor = conn.cursor()
cursor.execute("CREATE SCHEMA IF NOT EXISTS transactions;")
conn.commit()
cursor.close()
conn.close()

connection = create_engine('postgresql://postgres:pwd@localhost:5432/postgres')


# Make some noise

statement_size = 7000
bank_statement = client_transactions.sample(7000, random_state=1)
bank_statement['transaction_date'] = client_transactions.transaction_date.dt.strftime('%d%m%Y')
change = bank_statement.sample(337).index
bank_statement.loc[change,'amount'] = bank_statement.loc[change,'amount'] - np.random.uniform(0.01,0.05,337 )
change = bank_statement.sample(337).index
bank_statement.to_excel('bank_statement.xlsx', index=False)

bank_statement.to_excel('bank_statement.xlsx', index=False)


text_change = client_transactions.sample(500).index
text_change = client_transactions[(client_transactions.index.isin(text_change))
                    & (client_transactions.transaction_type == 'deposit') ].index
client_transactions.loc[text_change,'transaction_type'] = "déposit"

text_change = client_transactions.sample(500).index
text_change = client_transactions[(client_transactions.index.isin(text_change))
                    & (client_transactions.transaction_type == 'withdrawal') ].index
client_transactions.loc[text_change,'transaction_type'] = "with-drawal"


client_transactions.to_sql('client_transactions', connection, schema='transactions', if_exists='replace')

