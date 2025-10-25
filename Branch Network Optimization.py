def optimize_branch_network(branches_df, accounts_df, transactions_df, customers_df):
    """Analyze branch performance for network optimization decisions"""
    from pyspark.sql import functions as F
    
    branch_performance = branches_df \
        .join(accounts_df.groupBy("branch_id")
              .agg(F.count("*").alias("total_accounts"),
                   F.sum("balance").alias("total_deposits")), "branch_id", "left") \
        .join(transactions_df.join(accounts_df, "account_id")
              .groupBy("branch_id")
              .agg(F.count("*").alias("total_transactions"),
                   F.sum("amount").alias("transaction_volume")), "branch_id", "left") \
        .join(customers_df.join(accounts_df, "customer_id")
              .groupBy("branch_id")
              .agg(F.countDistinct("customer_id").alias("unique_customers")), "branch_id", "left") \
        .fillna(0) \
        .withColumn("deposits_per_customer", 
                   F.col("total_deposits") / F.col("unique_customers")) \
        .withColumn("transactions_per_customer", 
                   F.col("total_transactions") / F.col("unique_customers")) \
        .withColumn("efficiency_score",
                   (F.col("total_deposits") * 0.4 + 
                    F.col("transaction_volume") * 0.3 + 
                    F.col("unique_customers") * 0.3)) \
        .withColumn("optimization_recommendation",
                   F.when(F.col("efficiency_score") < F.percentile_approx("efficiency_score", 0.25).over(Window.partitionBy()), "CONSOLIDATE")
                    .when(F.col("efficiency_score") > F.percentile_approx("efficiency_score", 0.75).over(Window.partitionBy()), "EXPAND")
                    .otherwise("MAINTAIN"))
    
    return branch_performance