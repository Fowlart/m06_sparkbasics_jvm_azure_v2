import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Objects

class GeoHashAppender extends java.io.Serializable {

  def addGeoHashToHotels(cleansedHotelDf: DataFrame, ss: SparkSession): DataFrame = {
    import ss.implicits._

    cleansedHotelDf.as[ConvertedHotel].rdd.map(location => {

      val latitude = java.lang.Double.parseDouble(location.Latitude)
      val longitude = java.lang.Double.parseDouble(location.Longitude)

      ConvertedHotelWithGeoHash(
        location.Id,
        location.Name,
        location.Country,
        location.City,
        location.Address,
        location.Latitude,
        location.Longitude,
        GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 4)
      )
    }).toDF("Id", "Name", "Country", "City", "Address", "Latitude", "Longitude", "GeoHash")
  }


  def addGeoHashToWeather(weatherDf: DataFrame, ss: SparkSession): DataFrame = {
    import ss.implicits._

    weatherDf.as[WeatherRecord].rdd
      .filter(weatherRecord => {
        if (Objects.nonNull(weatherRecord.lat) && Objects.nonNull(weatherRecord.lng) && Objects.nonNull(weatherRecord.avg_tmpr_c)) {
          try {
            java.lang.Double.parseDouble(weatherRecord.lat)
            java.lang.Double.parseDouble(weatherRecord.lng)
            true
          }
          catch {
            case ex: NumberFormatException => false
          }
        }
        else false
      })
      .map(weatherRecord => {
        WeatherRecordWithGeoHash(weatherRecord.lng,
          weatherRecord.lat,
          weatherRecord.avg_tmpr_f,
          weatherRecord.avg_tmpr_c,
          weatherRecord.wthr_date,
          weatherRecord.year,
          weatherRecord.month,
          weatherRecord.day,
          GeoHash.geoHashStringWithCharacterPrecision( java.lang.Double.parseDouble(weatherRecord.lat),  java.lang.Double.parseDouble(weatherRecord.lng), 4)
        )
      })
      .toDF("lng", "lat", "avg_tmpr_f", "avg_tmpr_c", "wthr_date", "year", "month", "day", "weather_GeoHash")
  }
}