def detect_transaction_anomalies(transactions_df, accounts_df):
    """Detect anomalous transactions using statistical methods"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Calculate transaction patterns per account
    account_window = Window.partitionBy("account_id")
    
    transaction_anomalies = transactions_df.join(accounts_df, "account_id") \
        .withColumn("account_avg_amount", F.avg("amount").over(account_window)) \
        .withColumn("account_std_amount", F.stddev("amount").over(account_window)) \
        .withColumn("z_score", 
                   (F.col("amount") - F.col("account_avg_amount")) / F.col("account_std_amount")) \
        .withColumn("is_anomaly", 
                   F.when(
                       (F.abs(F.col("z_score")) > 3) | 
                       (F.col("amount") > F.col("balance") * 0.5), True
                   ).otherwise(False)) \
        .filter(F.col("is_anomaly") == True) \
        .select("transaction_id", "account_id", "amount", "transaction_type", 
                "z_score", "balance", "account_avg_amount")
    
    return transaction_anomalies