# First commit - 18/1/2026
# pip install pyspark

Step 1:
- Download & install JDK (Java Development Kit)
- https://www.oracle.com/asean/java/technologies/downloads/#jdk25-windows

Step 2:
Install VS Code
Add Python extension

Step 3:
- go to spark.apache.org
- download .tgz file (eg: spark-4.1.1-bin-hadoop3.tgz)
- Extract > Put file in C:\spark (eg: C:\spark\bin, C:\spark\conf)

Step 4:
- Go to https://github.com/cdarlint/winutils.
- Open any version file (eg: hadoop-3.3.6/bin)
- Download winutils.exe (eg: https://github.com/cdarlint/winutils/blob/master/hadoop-3.3.6/bin/winutils.exe)
- Put winutils.exe file in C:\spark\bin

Step 5:
- Open environment variables:

- System variable > New:
    Variable name: JAVA_HOME
    Variable value: C:\Program Files\Java\jdk-25.0.2

- System variable > Path > Edit > New > paste: %JAVA_HOME%\bin

- System variable > New:
    Variable name: SPARK_HOME
    Variable value: C:\spark

- System variable > New:
    Variable name: HADOOP_HOME
    Variable value: C:\spark

Step 6:
- Verify by closing all CMD and open new CMD.
- Type: spark-shell

Step 7:
- Open VS Code > Terminal (Ctrl + `)
- pip install pyspark
- pip install pyspark findspark