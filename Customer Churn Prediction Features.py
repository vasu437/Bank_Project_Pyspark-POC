def create_churn_prediction_features(customers_df, accounts_df, transactions_df):
    """Create features for customer churn prediction without ML modeling"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Account inactivity features
    recent_transactions = transactions_df.groupBy("account_id") \
        .agg(F.max("transaction_date").alias("last_transaction_date"))
    
    account_activity = accounts_df.join(recent_transactions, "account_id", "left") \
        .withColumn("days_since_last_transaction",
                   F.datediff(F.current_date(), F.col("last_transaction_date"))) \
        .withColumn("is_inactive", F.col("days_since_last_transaction") > 90)
    
    # Customer-level churn indicators
    customer_churn_features = account_activity.groupBy("customer_id") \
        .agg(
            F.count("*").alias("total_accounts"),
            F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_accounts"),
            F.sum(F.when(F.col("is_inactive") == True, 1).otherwise(0)).alias("inactive_accounts"),
            F.avg("balance").alias("avg_balance"),
            F.sum("balance").alias("total_balance"),
            F.min("days_since_last_transaction").alias("min_days_inactive"),
            F.max("days_since_last_transaction").alias("max_days_inactive")
        ) \
        .withColumn("inactive_ratio", F.col("inactive_accounts") / F.col("total_accounts")) \
        .withColumn("churn_risk_score",
                   F.when(F.col("inactive_ratio") > 0.7, "HIGH")
                    .when(F.col("inactive_ratio") > 0.4, "MEDIUM")
                    .when(F.col("max_days_inactive") > 180, "HIGH")
                    .otherwise("LOW"))
    
    return customer_churn_features