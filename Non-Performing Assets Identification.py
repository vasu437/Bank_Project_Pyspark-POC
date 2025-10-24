def identify_non_performing_assets(loans_df, loan_payments_df, accounts_df):
    """Identify and categorize non-performing loans and accounts"""
    from pyspark.sql import functions as F
    
    # Loan delinquency analysis
    loan_delinquency = loan_payments_df.filter(F.col("status") == "OVERDUE") \
        .groupBy("loan_id") \
        .agg(
            F.count("*").alias("overdue_payments"),
            F.max(F.datediff(F.current_date(), "due_date")).alias("max_days_overdue"),
            F.sum("amount_due").alias("total_overdue_amount")
        )
    
    npa_identification = loans_df.join(loan_delinquency, "loan_id", "left") \
        .join(accounts_df, "account_id") \
        .withColumn("is_npa", 
                   F.when(
                       (F.col("max_days_overdue") > 90) |
                       (F.col("overdue_payments") > 3) |
                       (F.col("status") == "DEFAULTED"), True
                   ).otherwise(False)) \
        .withColumn("npa_category",
                   F.when(F.col("max_days_overdue") > 180, "LOSS")
                    .when(F.col("max_days_overdue") > 90, "DOUBTFUL")
                    .when(F.col("max_days_overdue") > 60, "SUBSTANDARD")
                    .otherwise("STANDARD")) \
        .filter(F.col("is_npa") == True) \
        .groupBy("npa_category", "loan_type") \
        .agg(
            F.count("*").alias("npa_count"),
            F.sum("loan_amount").alias("npa_amount"),
            F.avg("max_days_overdue").alias("avg_days_overdue")
        )
    
    return npa_identification