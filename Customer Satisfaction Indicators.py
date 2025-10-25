def create_satisfaction_indicators(customers_df, accounts_df, transactions_df, loans_df):
    """Create proxy metrics for customer satisfaction"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Account activity indicators
    activity_indicators = transactions_df.join(accounts_df, "account_id") \
        .groupBy("customer_id") \
        .agg(
            F.count("*").alias("total_transactions"),
            F.avg("amount").alias("avg_transaction_size"),
            F.countDistinct("transaction_type").alias("product_usage_variety")
        )
    
    # Relationship depth indicators
    relationship_depth = accounts_df.groupBy("customer_id") \
        .agg(
            F.count("*").alias("total_products"),
            F.sum("balance").alias("total_relationship_balance"),
            F.avg(F.datediff(F.current_date(), "open_date")).alias("avg_relationship_length")
        )
    
    # Service quality indicators (simplified)
    service_quality = transactions_df.join(accounts_df, "account_id") \
        .withColumn("transaction_hour", F.hour("transaction_date")) \
        .withColumn("is_business_hours", 
                   F.when((F.col("transaction_hour") >= 9) & (F.col("transaction_hour") <= 17), 1)
                    .otherwise(0)) \
        .groupBy("customer_id") \
        .agg(
            F.avg(F.col("is_business_hours")).alias("business_hours_usage_ratio"),
            F.countDistinct("transaction_date").alias("active_days")
        )
    
    satisfaction_indicators = customers_df \
        .join(activity_indicators, "customer_id", "left") \
        .join(relationship_depth, "customer_id", "left") \
        .join(service_quality, "customer_id", "left") \
        .fillna(0) \
        .withColumn("satisfaction_score",
                   (F.col("total_products") * 0.2 + 
                    F.col("total_transactions") * 0.15 + 
                    F.col("product_usage_variety") * 0.15 + 
                    F.col("total_relationship_balance") * 0.2 + 
                    F.col("business_hours_usage_ratio") * 0.1 + 
                    F.col("active_days") * 0.2)) \
        .withColumn("satisfaction_level",
                   F.when(F.col("satisfaction_score") > 80, "VERY_SATISFIED")
                    .when(F.col("satisfaction_score") > 60, "SATISFIED")
                    .when(F.col("satisfaction_score") > 40, "NEUTRAL")
                    .when(F.col("satisfaction_score") > 20, "DISSATISFIED")
                    .otherwise("VERY_DISSATISFIED")) \
        .withColumn("retention_risk",
                   F.when(F.col("satisfaction_level").isin("VERY_DISSATISFIED", "DISSATISFIED"), "HIGH")
                    .when(F.col("satisfaction_level") == "NEUTRAL", "MEDIUM")
                    .otherwise("LOW"))
    
    return satisfaction_indicators