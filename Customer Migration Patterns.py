def analyze_customer_migration(accounts_df, transactions_df, customers_df, branches_df):
    """Analyze customer movement between branches and products"""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Branch migration patterns
    branch_migration = accounts_df.join(customers_df, "customer_id") \
        .withColumn("next_branch", 
                   F.lead("branch_id").over(Window.partitionBy("customer_id").orderBy("open_date"))) \
        .filter(F.col("next_branch").isNotNull() & (F.col("branch_id") != F.col("next_branch"))) \
        .groupBy("branch_id", "next_branch") \
        .agg(
            F.count("*").alias("migration_count"),
            F.avg("balance").alias("avg_balance_at_migration"),
            F.countDistinct("customer_id").alias("unique_customers")
        ) \
        .withColumn("net_migration", 
                   F.col("migration_count") - F.coalesce(
                       F.lag("migration_count").over(Window.partitionBy("branch_id").orderBy("next_branch")), F.lit(0)))
    
    # Product migration patterns
    product_migration = accounts_df.join(customers_df, "customer_id") \
        .withColumn("next_product", 
                   F.lead("account_type").over(Window.partitionBy("customer_id").orderBy("open_date"))) \
        .filter(F.col("next_product").isNotNull() & (F.col("account_type") != F.col("next_product"))) \
        .groupBy("account_type", "next_product") \
        .agg(
            F.count("*").alias("product_switch_count"),
            F.avg(F.datediff(F.lead("open_date").over(Window.partitionBy("customer_id").orderBy("open_date")), "open_date")).alias("avg_time_to_switch")
        )
    
    migration_analysis = branch_migration.join(
        product_migration, 
        branch_migration.branch_id == product_migration.account_type,  # Simplified join for demo
        "left"
    )
    
    return migration_analysis