def calculate_employee_performance(employees_df, loans_df, accounts_df, branches_df):
    """Calculate comprehensive employee performance metrics"""
    from pyspark.sql import functions as F
    
    # Loans approved by employee
    loans_approved = loans_df.groupBy("approved_by") \
        .agg(
            F.count("*").alias("loans_approved"),
            F.sum("loan_amount").alias("total_loan_volume"),
            F.avg("loan_amount").alias("avg_loan_size")
        )
    
    # Accounts managed (assuming relationship managers)
    accounts_managed = accounts_df.groupBy("branch_id") \
        .agg(F.count("*").alias("accounts_in_branch"))
    
    employee_performance = employees_df \
        .join(loans_approved, employees_df.employee_id == loans_approved.approved_by, "left") \
        .join(accounts_managed, "branch_id") \
        .join(branches_df, "branch_id") \
        .fillna(0) \
        .withColumn("performance_score",
                   (F.col("loans_approved") * 0.4 + 
                    F.col("total_loan_volume") * 0.3 + 
                    F.col("salary") * 0.3)) \
        .withColumn("efficiency_ratio", 
                   F.col("total_loan_volume") / F.col("accounts_in_branch")) \
        .withColumn("performance_tier",
                   F.when(F.col("performance_score") > F.avg("performance_score").over(Window.partitionBy()), "TOP")
                    .when(F.col("performance_score") < F.avg("performance_score").over(Window.partitionBy()), "LOW")
                    .otherwise("AVERAGE"))
    
    return employee_performance