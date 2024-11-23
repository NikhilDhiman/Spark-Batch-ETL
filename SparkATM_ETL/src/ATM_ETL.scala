import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.functions.udf
import java.time.Month
import java.util.TimeZone
import java.util.Calendar
import java.text.SimpleDateFormat

object ATM_ETL {
  
  def main(args: Array[String])={
    
    //Check if input file path and output file path is provided or not.
    if (args.length != 2){
      println("Two arguments are required")
      System.exit(1)
    }
    
    //Set Logger level to ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)
  
    //Creating SparkSession Instance
    val spark = new SparkSession.Builder().appName("Spark ETL").getOrCreate()
    
    processData(spark, args(0), args(1))
    
    spark.close()
  }
  
  /*
  Responsibilities:- This functions loads the data into dataframe and after processing, save the resultent dataframe to output directory.
  Parameter:- Instance of SparkSession, Input path and output path.
   */
  def processData(spark: SparkSession, inputPath: String, outputPath: String) = {
    
  //Raw DataFrame schema using StructType
  val schema =  StructType(List(
    StructField("year", IntegerType, true),
    StructField("month", StringType, true),
    StructField("day", IntegerType, true),
    StructField("weekday", StringType, true),
    StructField("hour", IntegerType, true),
    StructField("atm_status", StringType, true),
    StructField("atm_id", IntegerType, true),
    StructField("atm_manufacturer", StringType, true),
    StructField("atm_location", StringType, true),
    StructField("atm_streetname", StringType, true),
    StructField("atm_street_number", IntegerType, true),
    StructField("atm_zipcode", IntegerType, true),
    StructField("atm_lat", DoubleType, true),
    StructField("atm_lon", DoubleType, true),
    StructField("currency", StringType, true),
    StructField("card_type", StringType, true),
    StructField("service", StringType, true),
    StructField("message_code", IntegerType, true),
    StructField("message_text", StringType, true),
    StructField("weather_lat", DoubleType, true),
    StructField("weather_lon", DoubleType, true),
    StructField("weather_city_id", IntegerType, true),
    StructField("weather_city_name", StringType, true),
    StructField("temp", DoubleType, true),
    StructField("pressure", IntegerType, true),
    StructField("humidity", IntegerType, true),
    StructField("wind_speed", IntegerType, true),
    StructField("wind_deg", IntegerType, true),
    StructField("rain_3h", DoubleType, true),
    StructField("clouds_all", IntegerType, true),
    StructField("weather_id", IntegerType, true),
    StructField("weather_main", StringType, true),
    StructField("weather_description", StringType, true),
    StructField("transaction_amount", DoubleType, true)
  ))

  //Reading CSV data into DataFrame with defined schema
  val rawDataDF = spark.read.option("header", true).schema(schema).csv(inputPath)


  //Creating DataFrame for location dimension.

  //Selection only columns which are required for location dimension.
  val locationDF = rawDataDF.select(col("atm_location").alias("location"),
  col("atm_streetname").alias("streetname"),
  col("atm_street_number").alias("street_number"),
  col("atm_zipcode").alias("zipcode"),
  col("atm_lat").alias("lat"),
  col("atm_lon").alias("lon"))

  //Removing duplicate records.
  val locationUniqueDF = locationDF.distinct()

  //Adding a unique location_id from each location row.
  val indexedLocationDF = locationUniqueDF.withColumn("location_id", monotonically_increasing_id())
    .select("location_id", "location", "streetname", "street_number", "zipcode", "lat", "lon")



  //Creating DataFrame for card type dimension.

  //Selection only columns which are required for card type dimension.
  val cardTypeDF = rawDataDF.select(col("card_type"))

  //Removing duplicate records.
  val distinctCardTypeDF = cardTypeDF.distinct()

  //Adding a unique card_type_id from each location row.
  val indexedCardTypeDF = distinctCardTypeDF.withColumn("card_type_id", monotonically_increasing_id())
    .select("card_type_id", "card_type")



  //Creating DataFrame for ATM dimension.

  //Selection only columns which are required for card type dimension and adding alias to each column.
  val atmeDF = rawDataDF.select(col("atm_id").alias("atm_number"),
    col("atm_location").alias("location"),
    col("atm_manufacturer").alias("atm_manufacturer"),
    col("atm_streetname").alias("streetname"),
    col("atm_street_number").alias("street_number"),
    col("atm_zipcode").alias("zipcode"),
    col("atm_lat").alias("lat"),
    col("atm_lon").alias("lon"))

  //Removing duplicate records.
  val distinctAtmDF = atmeDF.distinct()

  /*
  Joinig the ATM dimension with Location dimension on columns lat and lon so that
  ATM dimension and Location dimension have 1:1 relation on location id and selecting required columns after join
  */
  val atmWithLocationIdDF = distinctAtmDF.join(indexedLocationDF, List("lat", "lon"), "left")
    .select("atm_number", "atm_manufacturer", "location_id")

  //Adding a unique atm_id from each ATM dimension row.
  val atmDinDF = atmWithLocationIdDF.withColumn("atm_id", monotonically_increasing_id())
    .select("atm_id", "atm_number", "atm_manufacturer", "location_id")


  //Creating DataFrame for Date dimension.

  //Defining a UDF for getDateFormated function.
  val formateDateUDF = udf((year: Int, mon: String, day: Int, hours: Int) => getDateFormated(year: Int, mon: String, day: Int, hours: Int ))

  //Selection only columns which are required for Date dimension.
  val dateDF = rawDataDF.select("year", "month", "day", "weekday", "hour")

  //Removing duplicate rows
  val distinctDateDF = dateDF.distinct()

  //Creating a new column with name 'full_date' and determining values using formateDateUDF.
  val newDateDf =  distinctDateDF.withColumn("full_date", formateDateUDF(col("year"), col("month"), col("day"), col("hour")))

  //Adding a unique atm_id from each Date dimension row.
  val indexedDateDF = newDateDf.withColumn("date_id", monotonically_increasing_id())
    .select("date_id", "full_date", "year", "month", "day", "weekday", "hour")



  //Creating Fact Table

  //Adding alias name to each Dataframe.
  val rawDataAliasDF = rawDataDF.alias("rawDataDF")
  val dimDateDF = indexedDateDF.alias("dimDataFD")
  val dimAtmDF = atmDinDF.alias("dimAtmDF")
  val dimCardTypeDF = indexedCardTypeDF.alias("dimCardTypeDF")
  val dimLocationDF =  indexedLocationDF.alias("dimLocationDF")

  //Replacing Date dimension columns with date id in raw dataframe.
  val replaceDateWithDateIdDF = rawDataAliasDF.join(dimDateDF, List("year", "month", "day", "weekday", "hour"), "left")
    .select("rawDataDf.*", "dimDataFD.date_id").drop("year", "month", "day", "weekday", "hour").alias("replaceDateWithDateIdDF")

  //Replacing Card type dimension columns with card tpe id in raw dataframe.
  val replaceCardWithCardIdDF = replaceDateWithDateIdDF.join(dimCardTypeDF, List("card_type"), "left")
    .select("replaceDateWithDateIdDF.*", "dimCardTypeDF.card_type_id").drop("card_type").alias("replaceCardWithCardIdDF")

  //Replacing Location dimension columns with Location id in raw dataframe.
  val replaceLocationWithLocationIdDF =  replaceCardWithCardIdDF.withColumnRenamed("atm_location", "location")
    .withColumnRenamed("atm_streetname", "streetname")
    .withColumnRenamed("atm_street_number", "street_number")
    .withColumnRenamed("atm_zipcode", "zipcode")
    .withColumnRenamed("atm_lat", "lat")
    .withColumnRenamed("atm_lon", "lon")
    .join(dimLocationDF, List("location", "streetname", "zipcode", "lat", "lon"))
    .select("replaceCardWithCardIdDF.*", "dimLocationDF.location_id")
    .drop("location", "street_number","streetname", "zipcode", "lat", "lon")
    .alias("replaceLocationWithLocationIdDF")

  //Replacing ATM dimension columns with ATM id in raw dataframe.
  val replaceAtmWithAtmIdDf = replaceLocationWithLocationIdDF.withColumnRenamed("atm_id", "atm_number")
    .join(dimAtmDF, List("atm_number", "atm_manufacturer", "location_id"), "left")
    .select("replaceLocationWithLocationIdDF.*", "dimAtmDF.atm_id")
    .drop("atm_number", "atm_manufacturer")
    .alias("replaceAtmWithAtmIdDf")

  //Removing duplicate rows.
  val distinctTransDF = replaceAtmWithAtmIdDf.distinct()

  //Adding a unique trans_id from each transaction dimension row.
  val transDF = distinctTransDF.withColumn("trans_id", monotonically_increasing_id())

  //Selecting columns in a format that is required.
  val transFactDF = transDF.select("trans_id", "atm_id", "date_id", "card_type_id", "location_id", "atm_status",
  "currency", "service", "transaction_amount", "message_code", "message_text", "rain_3h", "clouds_all", "weather_id", "weather_main", "weather_description")

  //Saving the fact data frame
  transFactDF.coalesce(1).write.format("csv").save(outputPath + "/ATM_Data_fact")
  
  //Saving the location data frame
  dimLocationDF.coalesce(1).write.format("csv").save(outputPath + "/ATM_Data_location")
  
  //Saving the ATM data frame
  dimAtmDF.coalesce(1).write.format("csv").save(outputPath + "/ATM_Data_atm")
  
  //Saving the Card data frame
  dimCardTypeDF.coalesce(1).write.format("csv").save(outputPath + "/ATM_Data_card")
  
  //Saving the Date data frame
  dimDateDF.coalesce(1).write.format("csv").save(outputPath + "/ATM_Data_date")

  }
  
  /*
  Responsibilities:- To convert string name of the month to the month number.
  Parameter:- Name of month in String.
  Return:- Number representation of month in Integer.
   */
  private def getMonthNumber(monthName: String) = Month.valueOf(monthName.toUpperCase).getValue

  /*
  Responsibilities:- To convert year, month, day and hours to format 'yyyy-MM-dd HH:mm:ss'.
  Parameter:- year as Integer, month name as String, day as Integer and hours as Integer.
  Return:- Formated date as 'yyyy-MM-dd HH:mm:ss' in String.
   */
  def getDateFormated(year: Int, mon: String, day: Int, hours: Int ): String= {
    val monthInt =  getMonthNumber(monthName = mon) - 1
    val c = Calendar.getInstance(TimeZone.getTimeZone("IST"))
    c.set(year, monthInt, day, hours, 0, 0)
    val format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format1.format(c.getTime)
  }
}
