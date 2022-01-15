import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.Objects

object GeoHashAppender extends {

  def addGeoHashToHotels(cleansedHotelDf: Dataset[ConvertedHotel], ss: SparkSession): Dataset[ConvertedHotelWithGeoHash] = {

    import ss.implicits._

    cleansedHotelDf.map(location => {

      val latitude = java.lang.Double.parseDouble(location.latitude)
      val longitude = java.lang.Double.parseDouble(location.longitude)

      ConvertedHotelWithGeoHash(
        location.id,
        location.name,
        location.country,
        location.city,
        location.address,
        location.latitude,
        location.longitude,
        GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 4)
      )
    })
  }


  def addGeoHashToWeather(weatherDf: DataFrame, ss: SparkSession): Dataset[WeatherRecordWithGeoHash] = {
    import ss.implicits._

    weatherDf.as[WeatherRecord].filter(weatherRecord => {
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
  }
}