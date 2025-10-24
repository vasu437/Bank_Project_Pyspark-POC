def analyze_account_aging(accounts_df, transactions_df):
    """Analyze account dormancy and aging patterns"""
    from pyspark.sql import functions as F
    
    # Last transaction per account
    last_transactions = transactions_df.groupBy("account_id") \
        .agg(F.max("transaction_date").alias("last_transaction_date"))
    
    account_aging = accounts_df.join(last_transactions, "account_id", "left") \
        .withColumn("days_since_last_transaction",
                   F.datediff(F.current_date(), "last_transaction_date")) \
        .withColumn("days_since_account_open",
                   F.datediff(F.current_date(), "open_date")) \
        .withColumn("aging_category",
                   F.when(F.col("days_since_last_transaction").isNull(), "NO_TRANSACTIONS")
                    .when(F.col("days_since_last_transaction") > 365, "DORMANT")
                    .when(F.col("days_since_last_transaction") > 180, "INACTIVE")
                    .when(F.col("days_since_last_transaction") > 90, "LOW_ACTIVITY")
                    .otherwise("ACTIVE")) \
        .withColumn("account_age_category",
                   F.when(F.col("days_since_account_open") > 3650, "VETERAN")
                    .when(F.col("days_since_account_open") > 1825, "ESTABLISHED")
                    .when(F.col("days_since_account_open") > 365, "MATURE")
                    .otherwise("NEW"))
    
    aging_summary = account_aging.groupBy("aging_category", "account_age_category") \
        .agg(
            F.count("*").alias("account_count"),
            F.sum("balance").alias("total_balance"),
            F.avg("balance").alias("avg_balance")
        ) \
        .withColumn("balance_percentage", 
                   F.col("total_balance") / F.sum("total_balance").over(Window.partitionBy()))
    
    return aging_summary