def analyze_branch_performance(branches_df, accounts_df, employees_df, transactions_df):
    """Comprehensive branch performance analysis"""
    from pyspark.sql import functions as F
    
    # Account metrics per branch
    branch_accounts = accounts_df.groupBy("branch_id") \
        .agg(
            F.count("*").alias("total_accounts"),
            F.sum("balance").alias("total_deposits"),
            F.avg("balance").alias("avg_account_balance"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
    
    # Employee metrics
    branch_employees = employees_df.groupBy("branch_id") \
        .agg(
            F.count("*").alias("total_employees"),
            F.avg("salary").alias("avg_salary"),
            F.countDistinct("position").alias("diverse_positions")
        )
    
    # Transaction volume
    branch_transactions = transactions_df.join(accounts_df, "account_id") \
        .groupBy("branch_id") \
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_transaction_volume"),
            F.avg("amount").alias("avg_transaction_size")
        )
    
    # Combine all metrics
    branch_performance = branches_df \
        .join(branch_accounts, "branch_id", "left") \
        .join(branch_employees, "branch_id", "left") \
        .join(branch_transactions, "branch_id", "left") \
        .fillna(0) \
        .withColumn("efficiency_ratio", 
                   F.col("total_transaction_volume") / F.col("total_employees")) \
        .withColumn("deposit_growth_score", 
                   F.col("total_deposits") / F.col("total_customers")) \
        .withColumn("overall_performance_score",
                   (F.col("efficiency_ratio") * 0.4 + 
                    F.col("deposit_growth_score") * 0.3 + 
                    F.col("avg_account_balance") * 0.3))
    
    return branch_performance