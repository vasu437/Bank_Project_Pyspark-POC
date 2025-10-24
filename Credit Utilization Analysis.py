def analyze_credit_utilization(credit_cards_df, customers_df, card_transactions_df):
    """Analyze credit card utilization patterns and trends"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    current_utilization = credit_cards_df.filter(F.col("status") == "ACTIVE") \
        .withColumn("utilization_rate", 
                   F.col("current_balance") / F.col("credit_limit")) \
        .withColumn("utilization_category",
                   F.when(F.col("utilization_rate") > 0.8, "HIGH_RISK")
                    .when(F.col("utilization_rate") > 0.5, "MEDIUM_RISK")
                    .when(F.col("utilization_rate") > 0.3, "LOW_RISK")
                    .otherwise("HEALTHY"))
    
    # Monthly utilization trends
    monthly_utilization = card_transactions_df \
        .join(credit_cards_df, "card_id") \
        .withColumn("transaction_month", F.date_trunc("month", "transaction_date")) \
        .groupBy("card_id", "transaction_month") \
        .agg(F.sum("amount").alias("monthly_spend")) \
        .join(credit_cards_df.select("card_id", "credit_limit"), "card_id") \
        .withColumn("monthly_utilization", F.col("monthly_spend") / F.col("credit_limit")) \
        .groupBy("card_id") \
        .agg(
            F.avg("monthly_utilization").alias("avg_monthly_utilization"),
            F.stddev("monthly_utilization").alias("utilization_volatility"),
            F.max("monthly_utilization").alias("peak_utilization")
        )
    
    utilization_analysis = current_utilization.join(monthly_utilization, "card_id") \
        .join(customers_df, "customer_id") \
        .withColumn("utilization_trend",
                   F.when(F.col("utilization_volatility") > 0.3, "VOLATILE")
                    .when(F.col("peak_utilization") > 0.9, "PEAK_HIGH")
                    .when(F.col("avg_monthly_utilization") > 0.7, "CONSISTENT_HIGH")
                    .otherwise("STABLE"))
    
    return utilization_analysis