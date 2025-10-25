def create_loan_default_features(loans_df, loan_payments_df, accounts_df, customers_df):
    """Create features for default prediction without ML modeling"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Payment behavior features
    payment_features = loan_payments_df.groupBy("loan_id") \
        .agg(
            F.sum(F.when(F.col("status") == "OVERDUE", 1).otherwise(0)).alias("total_overdue_payments"),
            F.avg(F.datediff(F.coalesce("payment_date", F.current_date()), "due_date")).alias("avg_days_delayed"),
            F.max(F.datediff(F.coalesce("payment_date", F.current_date()), "due_date")).alias("max_days_delayed"),
            F.sum("amount_paid").alias("total_amount_paid"),
            F.sum("amount_due").alias("total_amount_due")
        ) \
        .withColumn("payment_completion_ratio", 
                   F.col("total_amount_paid") / F.col("total_amount_due"))
    
    # Customer financial features
    customer_financials = accounts_df.groupBy("customer_id") \
        .agg(
            F.sum("balance").alias("total_balance"),
            F.avg("balance").alias("avg_balance"),
            F.count("*").alias("total_accounts")
        )
    
    default_features = loans_df.join(payment_features, "loan_id") \
        .join(customer_financials, "customer_id") \
        .join(customers_df, "customer_id") \
        .withColumn("loan_to_balance_ratio", 
                   F.col("loan_amount") / F.col("total_balance")) \
        .withColumn("default_risk_score",
                   (F.col("total_overdue_payments") * 0.3 + 
                    F.col("max_days_delayed") * 0.2 + 
                    F.col("loan_to_balance_ratio") * 0.3 + 
                    F.when(F.col("payment_completion_ratio") < 0.8, 0.2).otherwise(0))) \
        .withColumn("default_probability_category",
                   F.when(F.col("default_risk_score") > 0.7, "HIGH")
                    .when(F.col("default_risk_score") > 0.5, "MEDIUM_HIGH")
                    .when(F.col("default_risk_score") > 0.3, "MEDIUM")
                    .when(F.col("default_risk_score") > 0.1, "LOW")
                    .otherwise("VERY_LOW"))
    
    return default_features