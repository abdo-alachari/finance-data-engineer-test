# finance-data-engineer-test


### Requirements
- python3 >=3.5
- postgres >=9.5
- jupyter notbook/Lab

before everything
- > pip3 install -r requirements

## Task 1 :

SQL analysis : analysis/queries_LAB.ipynb


## Task 2 : 
1. reconciliation 

```
cd reconciliation
python3 make_data.py 
python3 reconciliation-ETL.py 
```



2. report_api  

```
cd report_api
python3 api.py
```


example : 

total api 
```
curl --location --request GET 'http://0.0.0.0:4043/total-report?user_id=47c269e4-f99e-11ea-99c7-8c8590230075'
```

result 
```
{
    "47c269e4-f99e-11ea-99c7-8c8590230075": [
        {
            "total_amount": 89757.0,
            "transaction_type": "deposit"
        },
        {
            "total_amount": 157046.0,
            "transaction_type": "withdrawal"
        }
    ]
}
```


# Notes:
* I've chosen Python as a programming language because it is the universal language of data guys 

* I could create sample data using SQL, however, I've chosen Python because it is easy and has more options

* All solutions are applicable  on large amounts of data: pandas module exists in spark libraries

* I've made 3 API (daily, monthly, total), I could make only one, but on  a production environment one API could be overloaded

* the service consumes data from a reconciliated table

* the reconciliattion is made as below

    - first, unify the date format for both sources
    - join the source by transaction id UUID and date 
    - calculate the score of  similarity between description ( using Levenshtein ratio)
    - calculate the difference between bank statement amount and company amount
    - calculate the allowed error for each record
    - filter data by allowed error and the chosen score