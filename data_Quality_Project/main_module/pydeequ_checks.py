import pandas as pd
from pydeequ.analyzers import *
from pydeequ.verification import VerificationSuite
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()

# Read the CSV file from your desktop
csv_file = '/path/to/your/desktop/customer_details.csv'
df = pd.read_csv(csv_file)

# Create a PyDeequ DataFrame
deequ_df = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        Check(spark)
        .isComplete("name")  # Test Case 1: Verify that the name field is not empty
        .isComplete("account_number")  # Test Case 2: Ensure account number is not empty
        .isComplete("date_of_birth")  # Test Case 3: Ensure date of birth is not empty
        .isContainedIn("gender", ["Male", "Female"])  # Test Case 4: Check valid gender values
        .hasPattern("account_number", r"^[A-Za-z0-9]{13}$")  # Account number pattern check
        .hasMin("date_of_birth", "2003-01-01")  # Minimum date of birth (18 years ago)
    ) \
    .run()

# Print the results
for result in deequ_df.checkResults():
    print(f"Check '{result.check().name()}' passed: {result.status() == CheckStatus.Success}")

# Stop the Spark session
spark.stop()
