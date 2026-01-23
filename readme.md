# List of tuples:
data = [("Data Engineer","RM 6500"),("Design Engineer","RM 4400")]
df = spark.createDataFrame(data, ["Jobs","Salary"])
display(df)

1) Create dataframe:
spark.createDataFrame

