# Read CSV File without Header and Schema
sample_df = spark.read.csv('/FileStore/tables/tutorials/sample.csv')
sample_df.show()
sample_df.printSchema()

# Read CSV file with Header
sample_df = spark.read.option("header",True).csv('/FileStore/tables/tutorials/sample.csv')
sample_df.show()
sample_df.printSchema()


# Read CSV file with Header and Schema
sample_df = spark.read.option("header",True).option("inferSchema",True).csv('/FileStore/tables/tutorials/sample.csv')
sample_df.show()

#Print Schema
sample_df.printSchema()
sample_df.dtypes

# Read Sample data
sample_df.head()
sample_df.first()
sample_df.take(3)

# Describe 
sample_df.describe("Emp_Id").show()
sample_df.describe("Emp_Id","Salary(k)").show()
sample_df.describe().show()

#Explain
sample_df.explain(True)
