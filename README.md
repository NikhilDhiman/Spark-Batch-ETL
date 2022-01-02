# Spark-Batch-ETL
 Spark batch ETL pipeline to read data from HDFS, transform and load it into target dimensions and facts on MYSQL
 
 ## Broadly following task was performed on this project-
 1. Reading the csv data from HDFS.
 2. Transforming the data according to the given target schema using Spark-scala. 
 3. This transformed data is to be loaded to HDFS.
 4. Creating the MySQL tables according to the given schema.
 5. Loading the data from HDFS to MySQL tables using Sqoop.
