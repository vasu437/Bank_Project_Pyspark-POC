def assess_interest_rate_risk(loans_df, accounts_df):
    """Calculate sensitivity of loan portfolio to interest rate changes"""
    from pyspark.sql import functions as F
    
    interest_rate_risk = loans_df.filter(F.col("status") == "ACTIVE") \
        .withColumn("rate_change_impact_1pct", 
                   F.col("loan_amount") * F.col("interest_rate") * 0.01 * F.col("term_months") / 12) \
        .withColumn("rate_change_impact_2pct", 
                   F.col("loan_amount") * F.col("interest_rate") * 0.02 * F.col("term_months") / 12) \
        .groupBy("loan_type") \
        .agg(
            F.count("*").alias("loan_count"),
            F.sum("loan_amount").alias("total_loan_amount"),
            F.avg("interest_rate").alias("avg_interest_rate"),
            F.sum("rate_change_impact_1pct").alias("total_impact_1pct"),
            F.sum("rate_change_impact_2pct").alias("total_impact_2pct"),
            F.percentile_approx("interest_rate", 0.5).alias("median_rate")
        ) \
        .withColumn("portfolio_sensitivity", 
                   F.col("total_impact_1pct") / F.col("total_loan_amount")) \
        .withColumn("risk_category",
                   F.when(F.col("portfolio_sensitivity") > 0.1, "HIGH")
                    .when(F.col("portfolio_sensitivity") > 0.05, "MEDIUM")
                    .otherwise("LOW"))
    
    return interest_rate_risk