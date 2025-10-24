def identify_cross_selling_opportunities(customers_df, accounts_df, loans_df, credit_cards_df):
    """Identify customers likely to need additional banking products"""
    from pyspark.sql import functions as F
    
    # Current product holdings
    product_holdings = customers_df \
        .join(accounts_df.groupBy("customer_id")
              .agg(F.count("*").alias("account_count")), "customer_id", "left") \
        .join(loans_df.filter(F.col("status") == "ACTIVE")
              .groupBy("customer_id")
              .agg(F.count("*").alias("active_loan_count")), "customer_id", "left") \
        .join(credit_cards_df.filter(F.col("status") == "ACTIVE")
              .groupBy("customer_id")
              .agg(F.count("*").alias("active_card_count")), "customer_id", "left") \
        .fillna(0)
    
    cross_sell_opportunities = product_holdings \
        .withColumn("missing_products", 
                   F.when((F.col("account_count") == 0), "CHECKING_ACCOUNT")
                    .when((F.col("active_loan_count") == 0) & (F.col("account_count") > 0), "PERSONAL_LOAN")
                    .when((F.col("active_card_count") == 0) & (F.col("account_count") > 1), "CREDIT_CARD")
                    .when((F.col("active_loan_count") > 0) & (F.col("active_card_count") == 0), "CREDIT_CARD")
                    .otherwise("PREMIUM_SERVICES")) \
        .withColumn("opportunity_score",
                   F.when(F.col("account_count") == 0, 0.9)
                    .when(F.col("active_loan_count") == 0, 0.7)
                    .when(F.col("active_card_count") == 0, 0.6)
                    .otherwise(0.4)) \
        .withColumn("priority_level",
                   F.when(F.col("opportunity_score") > 0.8, "HIGH")
                    .when(F.col("opportunity_score") > 0.6, "MEDIUM")
                    .otherwise("LOW"))
    
    return cross_sell_opportunities