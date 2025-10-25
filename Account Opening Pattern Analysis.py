def analyze_account_opening_patterns(accounts_df, customers_df, branches_df):
    """Analyze seasonal and demographic account opening patterns"""
    from pyspark.sql import functions as F
    
    opening_patterns = accounts_df.join(customers_df, "customer_id") \
        .join(branches_df, "branch_id") \
        .withColumn("opening_year", F.year("open_date")) \
        .withColumn("opening_month", F.month("open_date")) \
        .withColumn("opening_quarter", F.quarter("open_date")) \
        .withColumn("customer_age", 
                   F.floor(F.datediff("open_date", "date_of_birth") / 365.25)) \
        .withColumn("age_group",
                   F.when(F.col("customer_age") < 25, "18-24")
                    .when(F.col("customer_age") < 35, "25-34")
                    .when(F.col("customer_age") < 45, "35-44")
                    .when(F.col("customer_age") < 55, "45-54")
                    .when(F.col("customer_age") < 65, "55-64")
                    .otherwise("65+")) \
        .groupBy("opening_year", "opening_quarter", "age_group", "customer_type", "account_type") \
        .agg(
            F.count("*").alias("accounts_opened"),
            F.avg("balance").alias("avg_initial_balance"),
            F.sum("balance").alias("total_initial_deposits")
        ) \
        .withColumn("growth_rate", 
                   (F.col("accounts_opened") - F.lag("accounts_opened").over(
                       Window.partitionBy("age_group", "account_type").orderBy("opening_year", "opening_quarter"))) /
                   F.lag("accounts_opened").over(Window.partitionBy("age_group", "account_type").orderBy("opening_year", "opening_quarter")))
    
    return opening_patterns