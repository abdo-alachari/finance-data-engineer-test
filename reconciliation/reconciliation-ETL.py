import pandas as pd
from sqlalchemy import create_engine

connection = create_engine('postgresql://postgres:pwd@172.22.0.2:5432/postgres')



client_transactions = pd.read_sql("select * from transactions.client_transactions", connection)

bank_statement = pd.read_excel('bank_statement.xlsx')

matching = client_transactions.merge(bank_statement, on =  ['uuid', 'transaction_type', 'transaction_id', 'amount'], how='inner')
matching.drop(columns=  'transaction_date_y', inplace=True)
matching.rename(columns= { 'transaction_date_x' : 'transaction_date'}, inplace=True)


matching.to_sql('reconciliated_transactions', connection, schema='transactions', index=False, if_exists='replace')

