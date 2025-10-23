def segment_customers_by_transaction_pattern(transactions_df, accounts_df, customers_df):
    """Segment customers based on transaction frequency, amount patterns, and account activity"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Calculate transaction metrics per customer
    customer_transactions = transactions_df.join(accounts_df, "account_id") \
        .groupBy("customer_id") \
        .agg(
            F.count("*").alias("total_transactions"),
            F.avg("amount").alias("avg_transaction_amount"),
            F.stddev("amount").alias("std_transaction_amount"),
            F.sum("amount").alias("total_transaction_volume"),
            F.countDistinct("transaction_type").alias("unique_transaction_types")
        )
    
    # Add account metrics
    customer_accounts = accounts_df.groupBy("customer_id") \
        .agg(
            F.count("*").alias("total_accounts"),
            F.sum("balance").alias("total_balance"),
            F.avg("balance").alias("avg_balance")
        )
    
    # Combine metrics and create segments
    customer_segments = customer_transactions.join(customer_accounts, "customer_id") \
        .withColumn("transaction_frequency_segment",
                   F.when(F.col("total_transactions") > 100, "HIGH")
                    .when(F.col("total_transactions") > 50, "MEDIUM")
                    .otherwise("LOW")) \
        .withColumn("balance_segment",
                   F.when(F.col("total_balance") > 100000, "WEALTHY")
                    .when(F.col("total_balance") > 50000, "AFFLUENT")
                    .otherwise("STANDARD"))
    
    return customer_segments