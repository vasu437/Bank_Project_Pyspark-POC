def detect_credit_card_fraud_patterns(card_transactions_df, credit_cards_df, customers_df):
    """Identify potential fraud patterns in credit card transactions"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Calculate transaction patterns per card
    card_window = Window.partitionBy("card_id").orderBy("transaction_date")
    
    fraud_patterns = card_transactions_df.join(credit_cards_df, "card_id") \
        .withColumn("time_since_last_tx", 
                   F.unix_timestamp("transaction_date") - 
                   F.lag(F.unix_timestamp("transaction_date")).over(card_window)) \
        .withColumn("amount_deviation", 
                   F.abs(F.col("amount") - F.avg("amount").over(card_window)) / 
                   F.stddev("amount").over(card_window)) \
        .withColumn("unusual_location", 
                   F.when(F.col("city") != F.lag("city").over(card_window), 1).otherwise(0)) \
        .filter(
            (F.col("amount") > F.col("credit_limit") * 0.8) |  # High amount relative to limit
            (F.col("time_since_last_tx") < 300) |  # Rapid successive transactions
            (F.col("amount_deviation") > 3) |  # Unusually high amount
            (F.col("unusual_location") == 1)  # Different location from previous
        )
    
    return fraud_patterns