# Spark-Batch-ETL
 Spark batch ETL pipeline to read data from HDFS, transform and load it into target dimensions and facts on MYSQL
 
 # Broadly following task was performed on this project-
 1. Reading the csv data from HDFS.
 2. Transforming the data according to the given target schema using Spark-scala. 
 3. This transformed data is to be loaded to HDFS.
 4. Creating the MySQL tables according to the given schema.
 5. Loading the data from HDFS to MySQL tables using Sqoop.

# Dataset Used
Worked on Danish ATM Transactions Data Set in this project.
This dataset comprises around 2.5 million records of withdrawal data along with weather information at the time of the transactions from around 113 ATMs from the year 2017.
The actual data set is divided into two part files, both amounting to about 503 MB in total. 
There is also a data dictionary present along with the data set, which defines all of the 34 columns.
This data set contains various types of transactional data as well as the weather data at the time of the transaction, such as:
1.Transaction Date and Time: Year, month, day, weekday, hour
2.Status of the ATM: Active or inactive
3.Details of the ATM: ATM ID, manufacturer name along with location details such as longitude, latitude, street name, street number and zip code
4.The weather of the area near the ATM during the transaction: Location of measurement such as longitude, latitude, city name along with the type of weather, temperature, pressure, wind speed, cloud and so on
5.Transaction details: Card type, currency, transaction/service type, transaction amount and error message (if any)

Spar Nord Bank has published this dataset at Kaggle under Database Contents License (DbCL) v1.0 — Open Data Commons.
Link to dataset:- https://www.kaggle.com/sparnord/danish-atm-transactions![04ccb28b-37a3-4c38-ac7a-1cd556327670-Dimension Model](https://user-images.githubusercontent.com/30123312/147868862-4e77ed1f-a370-456b-a502-c0868b9efc7a.jpg)
