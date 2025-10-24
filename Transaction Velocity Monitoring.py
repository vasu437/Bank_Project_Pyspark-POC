def monitor_transaction_velocity(transactions_df, accounts_df):
    """Monitor transaction frequency for suspicious patterns"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    transaction_velocity = transactions_df.join(accounts_df, "account_id") \
        .withColumn("transaction_hour", F.hour("transaction_date")) \
        .withColumn("transaction_date_only", F.to_date("transaction_date")) \
        .groupBy("account_id", "transaction_date_only") \
        .agg(
            F.count("*").alias("daily_transaction_count"),
            F.sum("amount").alias("daily_transaction_amount"),
            F.avg("amount").alias("avg_transaction_amount"),
            F.countDistinct("transaction_type").alias("unique_transaction_types")
        ) \
        .withColumn("rolling_avg_7d", 
                   F.avg("daily_transaction_count").over(
                       Window.partitionBy("account_id")
                       .orderBy("transaction_date_only")
                       .rowsBetween(-6, 0)
                   )) \
        .withColumn("velocity_alert",
                   F.when(F.col("daily_transaction_count") > F.col("rolling_avg_7d") * 3, "HIGH_VELOCITY")
                    .when(F.col("daily_transaction_amount") > 10000, "LARGE_AMOUNT")
                    .otherwise("NORMAL")) \
        .filter(F.col("velocity_alert") != "NORMAL")
    
    return transaction_velocity