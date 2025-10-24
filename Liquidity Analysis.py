def analyze_bank_liquidity(accounts_df, loans_df, transactions_df):
    """Assess bank's liquidity position across different time horizons"""
    from pyspark.sql import functions as F
    
    # Current liquidity (instant access)
    current_liquidity = accounts_df.filter(
        (F.col("status") == "ACTIVE") & 
        (F.col("account_type").isin("SAVINGS", "CHECKING"))
    ).agg(F.sum("balance").alias("total_liquid_assets"))
    
    # Projected outflows (next 30 days)
    projected_outflows = transactions_df.filter(
        (F.col("transaction_type").isin("WITHDRAWAL", "PAYMENT")) &
        (F.col("transaction_date") >= F.current_date()) &
        (F.col("transaction_date") <= F.date_add(F.current_date(), 30))
    ).agg(F.sum("amount").alias("projected_outflows_30d"))
    
    # Loan commitments
    loan_commitments = loans_df.filter(F.col("status") == "ACTIVE") \
        .agg(F.sum("loan_amount").alias("total_loan_commitments"))
    
    liquidity_analysis = current_liquidity.crossJoin(projected_outflows) \
        .crossJoin(loan_commitments) \
        .withColumn("liquidity_ratio", 
                   F.col("total_liquid_assets") / F.col("projected_outflows_30d")) \
        .withColumn("coverage_ratio",
                   F.col("total_liquid_assets") / F.col("total_loan_commitments")) \
        .withColumn("liquidity_status",
                   F.when(F.col("liquidity_ratio") > 2.0, "EXCELLENT")
                    .when(F.col("liquidity_ratio") > 1.5, "ADEQUATE")
                    .when(F.col("liquidity_ratio") > 1.0, "MINIMAL")
                    .otherwise("INADEQUATE"))
    
    return liquidity_analysis