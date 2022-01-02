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
Link to dataset:- https://www.kaggle.com/sparnord/danish-atm-transactions

# Target Dimension Model
For this project, we will need four dimension tables and one fact table. They are as follows:
1. ATM dimension - This dimension will have the data related to the various ATMs present in the dataset along with the ATM number(ATM ID in the original dataset), ATM manufacturer and a reference to the ATM location and is very important for solving analytical queries related where ATM data will be used.

2. Location dimension - This is a very important dimension containing all the location data including location name, street name, street number, zip code and even the latitude and longitude. This information will be very important for solving problems related to the particular location at which a transaction took place and can help banks in things like pinpointing locations where ATMs where demand is higher as compared to other locations. Combined with weather data in the transaction table, this can be used to further do analysis such as how weather affects the demand at ATMs at a particular location.

3. Date dimension - This is another very important dimension which is almost always present where data such as transactional data is being dealt with. This dimension includes fields such as the full date and time timestamp, year, month, day, hour as well as the weekday for a transaction. This all can help in analysing the transaction behaviour with respect to the time at which the transaction took place and also how the transaction activity varies between weekdays and weekends.

 4. Card type dimension - This dimension has the information about the particular card type with which a particular transaction took place. This can help in performing analysis on how the number of transactions varies with respect to each different card type.
 
 5. Transaction fact - This is the actual fact table for the data set which contains all of the numerical data such as the currency of the transaction, service, transaction amount, message code and text as well as weather info such as description, weather id etc.
 
 ![04ccb28b-37a3-4c38-ac7a-1cd556327670-Dimension Model](https://user-images.githubusercontent.com/30123312/147868891-138d700c-d63b-4d9b-8257-fb63ba1e3b49.jpg)
 
 
# Environment Used:
1. Hadoop version : 2.10.1
2. Java Version: 1.8.0
3. Eclipse Version: 2019-12(4.14.0)
4. Spark Version: 2.4.7
5. Scala Version: 2.11.0
6. Sqoop Version: 1.4.7

# Submiting job to spark submit in cluser mode:
spark-submit --class ATM_ETL --master yarn --deploy-mode cluster atmETL.jar /user/itv000943/atm/atmData.csv /user/itv000943/atmETL_output

# Loading data to MySQL using Sqoop:
creating Location dinemsion Table:

create table itv000943_Din_Location( location_id Bigint, location varchar(50), streatname varchar(255), street_number int, zipcode int, lat Decimal(10,3), lon Decimal(10,3), Primary key (location_id));
    
Loading Data: 
sqoop export --connect "jdbc:mysql://ms.itversity.com:3306/retail_export" --username retail_user --password itversity --table itv000943_Din_Location --export-dir /user/itv000943/atmETL_output/ATM_Data_location/part-00000-3bf87172-d01d-4b7d-8633-937c0b3e6338-c000.csv --fields-terminated-by ","

