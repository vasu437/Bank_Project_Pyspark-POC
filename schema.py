from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime, date
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BankingDataModel") \
    .getOrCreate()

# 1. CUSTOMERS Schema
customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("customer_since", DateType(), True),
    StructField("customer_type", StringType(), True)
])

# 2. ACCOUNTS Schema
accounts_schema = StructType([
    StructField("account_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("account_number", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DecimalType(15, 2), True),
    StructField("open_date", DateType(), True),
    StructField("close_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("branch_id", IntegerType(), True)
])

# 3. TRANSACTIONS Schema
transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("account_id", IntegerType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DecimalType(15, 2), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("description", StringType(), True),
    StructField("related_account_id", IntegerType(), True),
    StructField("transaction_status", StringType(), True)
])

# 4. BRANCHES Schema
branches_schema = StructType([
    StructField("branch_id", IntegerType(), False),
    StructField("branch_name", StringType(), True),
    StructField("branch_code", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("manager_id", IntegerType(), True)
])

# 5. EMPLOYEES Schema
employees_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("branch_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("position", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("hire_date", DateType(), True),
    StructField("salary", DecimalType(10, 2), True),
    StructField("manager_id", IntegerType(), True)
])

# 6. LOANS Schema
loans_schema = StructType([
    StructField("loan_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("loan_type", StringType(), True),
    StructField("loan_amount", DecimalType(15, 2), True),
    StructField("interest_rate", DecimalType(5, 3), True),
    StructField("term_months", IntegerType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("approved_by", IntegerType(), True)
])

# 7. LOAN_PAYMENTS Schema
loan_payments_schema = StructType([
    StructField("payment_id", IntegerType(), False),
    StructField("loan_id", IntegerType(), True),
    StructField("due_date", DateType(), True),
    StructField("payment_date", DateType(), True),
    StructField("amount_due", DecimalType(10, 2), True),
    StructField("amount_paid", DecimalType(10, 2), True),
    StructField("principal_amount", DecimalType(10, 2), True),
    StructField("interest_amount", DecimalType(10, 2), True),
    StructField("status", StringType(), True)
])

# 8. CREDIT_CARDS Schema
credit_cards_schema = StructType([
    StructField("card_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("card_number", StringType(), True),
    StructField("card_type", StringType(), True),
    StructField("credit_limit", DecimalType(10, 2), True),
    StructField("current_balance", DecimalType(10, 2), True),
    StructField("issue_date", DateType(), True),
    StructField("expiry_date", DateType(), True),
    StructField("status", StringType(), True)
])

# 9. CARD_TRANSACTIONS Schema
card_transactions_schema = StructType([
    StructField("card_transaction_id", IntegerType(), False),
    StructField("card_id", IntegerType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("transaction_type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True)
])

# 10. ACCOUNT_BENEFICIARIES Schema
account_beneficiaries_schema = StructType([
    StructField("beneficiary_id", IntegerType(), False),
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("relationship", StringType(), True),
    StructField("allocation_percentage", DecimalType(5, 2), True),
    StructField("added_date", DateType(), True)
])