import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from Levenshtein import ratio

def percentage(percent, whole):
  return (percent * whole) / 100.0


# text score tolerance
text_tolerance = 0.8

# tolerance in percantage
tolerance = 0.1

date_format = "%d-%m-%Y"

bank_file_date_format = "%d%m%Y"

connection = create_engine('postgresql://postgres:pwd@localhost:5432/postgres')

# laod comany transaction
client_transactions = pd.read_sql("select * from transactions.client_transactions", connection)

# load bank transactions
bank_statement = pd.read_excel('bank_statement.xlsx')


# first transform date format for both sources
client_transactions['transaction_date'] = client_transactions.transaction_date.dt.strftime(date_format)
bank_statement['transaction_date'] = pd.to_datetime(bank_statement['transaction_date'], format=bank_file_date_format).dt.strftime(date_format)

# merge data based on user id / transaction id / date
matching = client_transactions.merge(bank_statement, how = 'outer', on = ['uuid', "transaction_id", "transaction_date"])

matching.rename(columns={ 'transaction_type_x' : 'company_transaction_type', 
                'amount_x':'company_amount',
                'transaction_type_y' : 'bank_transaction_type',
                         'amount_y':'bank_amount'}, inplace = True)

# fill textual data na with space
matching.company_transaction_type.fillna('', inplace=True)
matching.bank_transaction_type.fillna('',inplace=True)


# caluclate score transactions type text 
matching["transaction_score"] = matching.apply(lambda x: ratio(x["company_transaction_type"], x["bank_transaction_type"]), axis=1)


# fill numerical data na with 0
matching.bank_amount.fillna(0, inplace=True)
matching.company_amount.fillna(0, inplace=True)


# caluclate amount error 
matching['amount_error'] = np.abs(matching.company_amount - matching.bank_amount)
matching['amount_allowed'] = matching.amount_error.apply(lambda x : percentage(tolerance, x))


reconciliated_transactions = matching[(matching.transaction_score >=text_tolerance )&(matching.amount_error <=matching.amount_allowed) ].copy()


reconciliated_transactions.rename(columns = {'bank_transaction_type': 'transaction_type', "bank_amount": 'amount' }, inplace=True)
reconciliated_transactions['transaction_date'] = pd.to_datetime(reconciliated_transactions['transaction_date'])

reconciliated_transactions = reconciliated_transactions[['uuid', 'transaction_type', 'transaction_id', 'transaction_date', 'amount' ]]
# store reconciliated transactions to use it later for reporting
reconciliated_transactions.to_sql('reconciliated_transactions', connection, schema='transactions', index=False, if_exists='replace')