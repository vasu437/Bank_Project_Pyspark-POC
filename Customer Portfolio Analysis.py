def analyze_customer_portfolios(customers_df, accounts_df, loans_df, credit_cards_df):
    """Analyze complete financial portfolios for customers"""
    from pyspark.sql import functions as F
    
    # Account portfolio
    account_portfolio = accounts_df.filter(F.col("status") == "ACTIVE") \
        .groupBy("customer_id", "account_type") \
        .agg(F.sum("balance").alias("total_balance_by_type")) \
        .groupBy("customer_id") \
        .pivot("account_type") \
        .sum("total_balance_by_type") \
        .fillna(0)
    
    # Loan portfolio
    loan_portfolio = loans_df.filter(F.col("status") == "ACTIVE") \
        .groupBy("customer_id") \
        .agg(
            F.count("*").alias("active_loans"),
            F.sum("loan_amount").alias("total_loan_amount"),
            F.avg("interest_rate").alias("avg_interest_rate")
        )
    
    # Credit card portfolio
    card_portfolio = credit_cards_df.filter(F.col("status") == "ACTIVE") \
        .groupBy("customer_id") \
        .agg(
            F.count("*").alias("active_cards"),
            F.sum("credit_limit").alias("total_credit_limit"),
            F.sum("current_balance").alias("total_card_balance")
        )
    
    # Combine all portfolios
    customer_portfolio = customers_df \
        .join(account_portfolio, "customer_id", "left") \
        .join(loan_portfolio, "customer_id", "left") \
        .join(card_portfolio, "customer_id", "left") \
        .fillna(0) \
        .withColumn("total_assets", 
                   F.coalesce(F.col("SAVINGS"), F.lit(0)) + 
                   F.coalesce(F.col("CHECKING"), F.lit(0))) \
        .withColumn("total_liabilities", 
                   F.col("total_loan_amount") + F.col("total_card_balance")) \
        .withColumn("net_worth", F.col("total_assets") - F.col("total_liabilities")) \
        .withColumn("leverage_ratio", 
                   F.when(F.col("total_assets") > 0, 
                         F.col("total_liabilities") / F.col("total_assets"))
                    .otherwise(0))
    
    return customer_portfolio