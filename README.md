# Trustly Case

Project github -> [link](https://github.com/AbnerHenriq/case-trustly/tree/main) 

## Stack:

Transformation: dbt
Database: postgresql

# 1. Model It Your Way 

## Deliverables

The final models are `fct_transactions` and `dim_merchants` used to create all queries.

### Transactions
![Transactions](transactions_lineage.png)

### Merchants:
![Merchants](merchants_lineage.png)

#  Data Quality:

- I applied some tests to try to capture some errors and apply some transformations. What is worth mentioning is:
    - Test merchant rows were removed (id = 3)
    - There are 500 transactions that do not have sessions. Session 0 is assigned to the transactions, but does not exist in the sessions.
    - The session table has duplicity, functioning more like session_steps than a session table.

![Data Quality](data_quality.png)

##  Questions

Todos as perguntas podem ser encontradas no arquivo [questions.md](dbt-trustly/questions/questions.md)


# 2 - Suggest It Your Way 