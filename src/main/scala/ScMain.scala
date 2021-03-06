import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.FileInputStream
import java.util.Properties

object ScMain extends App {

  val sparkSession = SparkSession.builder()
    .appName("Executor for m06_sparkbasics_jvm_azure")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  val prop = new Properties()
  prop.load(new FileInputStream("src/main/resources/creds.properties"))
  sparkSession.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
  sparkSession.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  sparkSession.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",s"${prop.getProperty("client.id")}")
  sparkSession.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",s"${prop.getProperty("client.secret")}")
  sparkSession.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net","https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

  // weather
  val weatherDf = sparkSession.
    read.parquet("abfs://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather")

  // hotels
  val hotelsDf = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("abfs://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels")


  /** =Task 1:  =
   * Check hotels data on incorrect (null) values (Latitude & Longitude).
   * For incorrect values map (Latitude & Longitude) from OpenCage Geocoding
   * API in job on fly (Via REST API). */
  val cleansedHotelDS = HotelFilter.cleanAndEnhanceHotelsDf(hotelsDf, sparkSession, prop.getProperty("geocoder.api.key"))
  cleansedHotelDS.write.mode(SaveMode.Overwrite).parquet("results/cleaned_hotels")

  val cleansedHotelWithGeoHashDS = GeoHashAppender.addGeoHashToHotels(cleansedHotelDS, sparkSession)
  cleansedHotelWithGeoHashDS.write.mode(SaveMode.Overwrite).parquet("results/cleaned_hotels_with_geoHash")
  // Todo:  10k rows limit used for the whether table to avoid large computation delays
  val weatherWithGeoHashDS = GeoHashAppender.addGeoHashToWeather(weatherDf.limit(10000), sparkSession)
  weatherWithGeoHashDS.write.mode(SaveMode.Overwrite).parquet("results/weather_with_geoHash")


  /** =Task 3: =
   * Left join weather and hotels data by generated 4-characters
   * geohash (avoid data multiplication and make you job idempotent) */
  val joinCondition = cleansedHotelWithGeoHashDS.col("GeoHash") === weatherWithGeoHashDS.col("weather_GeoHash")
  val resultDf = cleansedHotelWithGeoHashDS.join(weatherWithGeoHashDS, joinCondition, "left_outer")
  resultDf.repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", ";")
    .csv("results/final_enriched_data")
}