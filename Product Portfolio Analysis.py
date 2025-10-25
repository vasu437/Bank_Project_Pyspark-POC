def analyze_product_portfolio(accounts_df, loans_df, credit_cards_df, transactions_df):
    """Analyze performance across different banking products"""
    from pyspark.sql import functions as F
    
    # Account product analysis
    account_products = accounts_df.groupBy("account_type") \
        .agg(
            F.count("*").alias("total_accounts"),
            F.sum("balance").alias("total_balance"),
            F.avg("balance").alias("avg_balance"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg(F.datediff(F.current_date(), "open_date")).alias("avg_account_age_days")
        )
    
    # Loan product analysis
    loan_products = loans_df.groupBy("loan_type") \
        .agg(
            F.count("*").alias("total_loans"),
            F.sum("loan_amount").alias("total_loan_amount"),
            F.avg("loan_amount").alias("avg_loan_amount"),
            F.avg("interest_rate").alias("avg_interest_rate")
        )
    
    # Credit card product analysis
    card_products = credit_cards_df.groupBy("card_type") \
        .agg(
            F.count("*").alias("total_cards"),
            F.sum("credit_limit").alias("total_credit_limit"),
            F.avg("current_balance").alias("avg_balance"),
            F.avg("credit_limit").alias("avg_credit_limit")
        )
    
    # Transaction volume by product type
    transaction_products = transactions_df.join(accounts_df, "account_id") \
        .groupBy("account_type") \
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_transaction_volume"),
            F.avg("amount").alias("avg_transaction_amount")
        )
    
    product_portfolio = account_products.join(transaction_products, "account_type", "left") \
        .withColumn("profitability_score",
                   (F.col("total_balance") * 0.4 + 
                    F.col("total_transaction_volume") * 0.3 + 
                    F.col("unique_customers") * 0.3)) \
        .withColumn("product_performance_tier",
                   F.when(F.col("profitability_score") > F.percentile_approx("profitability_score", 0.8).over(Window.partitionBy()), "STAR")
                    .when(F.col("profitability_score") > F.percentile_approx("profitability_score", 0.5).over(Window.partitionBy()), "CORE")
                    .when(F.col("profitability_score") > F.percentile_approx("profitability_score", 0.2).over(Window.partitionBy()), "SUPPORTING")
                    .otherwise("NICHE"))
    
    return product_portfolio