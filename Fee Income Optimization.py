def optimize_fee_income(transactions_df, accounts_df, customers_df):
    """Analyze fee structures and optimization opportunities"""
    from pyspark.sql import functions as F
    
    fee_analysis = transactions_df.join(accounts_df, "account_id") \
        .join(customers_df, "customer_id") \
        .withColumn("estimated_fee",
                   F.when(F.col("transaction_type") == "WITHDRAWAL", 
                         F.greatest(F.lit(2.0), F.col("amount") * 0.01))
                    .when(F.col("transaction_type") == "TRANSFER", 
                         F.greatest(F.lit(1.5), F.col("amount") * 0.005))
                    .when(F.col("transaction_type") == "PAYMENT", 
                         F.greatest(F.lit(1.0), F.col("amount") * 0.003))
                    .otherwise(0.0)) \
        .withColumn("fee_category",
                   F.when(F.col("estimated_fee") > 10, "HIGH_FEE")
                    .when(F.col("estimated_fee") > 5, "MEDIUM_FEE")
                    .when(F.col("estimated_fee") > 1, "LOW_FEE")
                    .otherwise("NO_FEE")) \
        .groupBy("customer_type", "account_type", "fee_category") \
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("estimated_fee").alias("total_fee_income"),
            F.avg("estimated_fee").alias("avg_fee_per_transaction"),
            F.sum("amount").alias("total_transaction_volume")
        ) \
        .withColumn("fee_to_volume_ratio",
                   F.col("total_fee_income") / F.col("total_transaction_volume")) \
        .withColumn("optimization_opportunity",
                   F.when((F.col("fee_to_volume_ratio") < 0.01) & (F.col("transaction_count") > 100), "INCREASE_FEES")
                    .when((F.col("fee_to_volume_ratio") > 0.05) & (F.col("transaction_count") < 50), "REDUCE_FEES")
                    .otherwise("OPTIMAL"))
    
    return fee_analysis