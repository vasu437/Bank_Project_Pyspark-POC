def assess_loan_risk(loans_df, loan_payments_df, customers_df, accounts_df):
    """Assess loan risk based on payment history and customer financials"""
    from pyspark.sql import functions as F
    
    # Calculate payment performance
    payment_performance = loan_payments_df.groupBy("loan_id") \
        .agg(
            F.count("*").alias("total_payments"),
            F.sum(F.when(F.col("status") == "OVERDUE", 1).otherwise(0)).alias("overdue_payments"),
            F.avg(F.col("amount_paid") / F.col("amount_due")).alias("payment_ratio"),
            F.max(F.datediff(F.col("payment_date"), F.col("due_date"))).alias("max_days_late")
        )
    
    # Combine with loan and customer data
    loan_risk_assessment = loans_df.join(payment_performance, "loan_id") \
        .join(accounts_df, ["customer_id", "account_id"]) \
        .withColumn("risk_score",
                   F.when(F.col("overdue_payments") > 5, "HIGH_RISK")
                    .when(F.col("payment_ratio") < 0.8, "MEDIUM_RISK")
                    .when(F.col("max_days_late") > 30, "MEDIUM_RISK")
                    .otherwise("LOW_RISK")) \
        .withColumn("delinquency_rate", F.col("overdue_payments") / F.col("total_payments"))
    
    return loan_risk_assessment