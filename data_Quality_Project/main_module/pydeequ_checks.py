import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
from pydeequ.checks import *
import pandas as pd
from pydeequ.verification import *
from pyspark.sql import SparkSession  
import pydeequ


# Initialize Spark session
spark = (SparkSession
.builder
.config("spark.jars.packages", pydeequ.deequ_maven_coord)
.config("spark.jars.excludes", pydeequ.f2j_maven_coord)
.getOrCreate())



# Read the CSV file from your desktop
csv_file = "data_Quality_Project/main_module/customer_details.csv"
df = pd.read_csv(csv_file)
spark_df = spark.createDataFrame(df)
check = Check(spark, CheckLevel.Warning, "Review Check")


checkResult = VerificationSuite(spark) \
    .onData(spark_df) \
    .addCheck(
        check.hasSize(lambda x: x >= 3)\
        .isComplete("name")  # Test Case 1: Verify that the name field is not empty
        .isComplete("account_number")  # Test Case 2: Ensure account number is not empty
        .isComplete("date_of_birth")  # Test Case 3: Ensure date of birth is not empty
        .isContainedIn("gender", ["Male", "Female"])  # Test Case 4: Check valid gender values
        .hasPattern("account_number", r"^[A-Za-z0-9]{13}$")  # Account number pattern check
        .hasMin("date_of_birth", "2003-01-01")  # Minimum date of birth (18 years ago)
    ) \
    .run()

checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()


# Stop the Spark session
spark.stop()
