def analyze_service_levels(transactions_df, accounts_df, loans_df):
    """Analyze transaction processing times and service levels"""
    from pyspark.sql import functions as F
    
    service_level_analysis = transactions_df \
        .withColumn("processing_time_hours", 
                   (F.unix_timestamp("transaction_date") - 
                    F.unix_timestamp(F.lag("transaction_date").over(
                        Window.partitionBy("account_id").orderBy("transaction_date")))) / 3600) \
        .filter(F.col("processing_time_hours").isNotNull()) \
        .join(accounts_df, "account_id") \
        .groupBy("account_type", "transaction_type") \
        .agg(
            F.avg("processing_time_hours").alias("avg_processing_time"),
            F.percentile_approx("processing_time_hours", 0.95).alias("p95_processing_time"),
            F.count("*").alias("transaction_count")
        ) \
        .withColumn("service_level_agreement",
                   F.when(F.col("p95_processing_time") < 1, "WITHIN_SLA")
                    .when(F.col("p95_processing_time") < 4, "NEAR_SLA")
                    .otherwise("BREACH_SLA")) \
        .withColumn("performance_rating",
                   F.when(F.col("avg_processing_time") < 0.5, "EXCELLENT")
                    .when(F.col("avg_processing_time") < 2, "GOOD")
                    .when(F.col("avg_processing_time") < 6, "ACCEPTABLE")
                    .otherwise("POOR"))
    
    return service_level_analysis