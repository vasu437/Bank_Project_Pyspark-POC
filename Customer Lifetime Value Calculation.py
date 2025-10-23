def calculate_customer_lifetime_value(customers_df, accounts_df, transactions_df, loans_df, credit_cards_df):
    """Calculate comprehensive customer lifetime value"""
    from pyspark.sql import functions as F
    
    # Revenue from loans (interest)
    loan_revenue = loans_df.filter(F.col("status") == "ACTIVE") \
        .withColumn("estimated_interest_revenue", 
                   F.col("loan_amount") * F.col("interest_rate") * F.col("term_months") / 12) \
        .groupBy("customer_id") \
        .agg(F.sum("estimated_interest_revenue").alias("total_loan_revenue"))
    
    # Revenue from credit cards (assumed fees)
    card_revenue = credit_cards_df.filter(F.col("status") == "ACTIVE") \
        .withColumn("estimated_card_revenue", F.col("current_balance") * 0.18) \
        .groupBy("customer_id") \
        .agg(F.sum("estimated_card_revenue").alias("total_card_revenue"))
    
    # Transaction fees (assumed)
    transaction_fees = transactions_df.join(accounts_df, "account_id") \
        .groupBy("customer_id") \
        .agg((F.count("*") * 0.10).alias("transaction_fees"))  # $0.10 per transaction
    
    # Combine all revenue streams
    customer_lifetime_value = customers_df \
        .join(loan_revenue, "customer_id", "left") \
        .join(card_revenue, "customer_id", "left") \
        .join(transaction_fees, "customer_id", "left") \
        .fillna(0) \
        .withColumn("total_lifetime_value", 
                   F.col("total_loan_revenue") + F.col("total_card_revenue") + F.col("transaction_fees")) \
        .withColumn("customer_value_tier",
                   F.when(F.col("total_lifetime_value") > 10000, "PREMIUM")
                    .when(F.col("total_lifetime_value") > 5000, "GOLD")
                    .when(F.col("total_lifetime_value") > 1000, "SILVER")
                    .otherwise("STANDARD"))
    
    return customer_lifetime_value