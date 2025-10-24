def analyze_collateral_coverage(loans_df, accounts_df, customers_df):
    """Assess loan collateral adequacy and coverage ratios"""
    from pyspark.sql import functions as F
    
    # Simplified collateral valuation (in real scenario, this would come from collateral table)
    collateral_coverage = loans_df.filter(F.col("status") == "ACTIVE") \
        .join(accounts_df, ["customer_id", "account_id"]) \
        .withColumn("estimated_collateral_value",
                   F.when(F.col("loan_type") == "MORTGAGE", F.col("loan_amount") * 1.2)
                    .when(F.col("loan_type") == "AUTO", F.col("loan_amount") * 0.8)
                    .when(F.col("loan_type") == "PERSONAL", F.col("loan_amount") * 0.5)
                    .otherwise(F.col("loan_amount") * 0.3)) \
        .withColumn("collateral_coverage_ratio",
                   F.col("estimated_collateral_value") / F.col("loan_amount")) \
        .withColumn("coverage_adequacy",
                   F.when(F.col("collateral_coverage_ratio") > 1.5, "EXCESS")
                    .when(F.col("collateral_coverage_ratio") > 1.2, "ADEQUATE")
                    .when(F.col("collateral_coverage_ratio") > 1.0, "MINIMAL")
                    .otherwise("INADEQUATE")) \
        .withColumn("risk_adjusted_coverage",
                   F.col("collateral_coverage_ratio") * 
                   F.when(F.col("loan_type") == "MORTGAGE", 1.0)
                    .when(F.col("loan_type") == "AUTO", 0.8)
                    .when(F.col("loan_type") == "PERSONAL", 0.5)
                    .otherwise(0.3))
    
    portfolio_coverage = collateral_coverage.groupBy("loan_type", "coverage_adequacy") \
        .agg(
            F.count("*").alias("loan_count"),
            F.sum("loan_amount").alias("total_loan_amount"),
            F.avg("collateral_coverage_ratio").alias("avg_coverage_ratio"),
            F.min("collateral_coverage_ratio").alias("min_coverage_ratio")
        )
    
    return portfolio_coverage