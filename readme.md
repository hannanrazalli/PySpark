# List of tuples:
data = [("Data Engineer","RM 6500"),("Design Engineer","RM 4400")]
df = spark.createDataFrame(data, ["Jobs","Salary"])
display(df)

1) Create dataframe:
spark.createDataFrame

data = [('John', 21), ('Amy', 25), ('Anita', 41), ('Rohan', 25), ('Maria', 37)]
df = spark.createDataFrame(data) *Create dataframe*
df.show() *Print dataframe*