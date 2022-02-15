import com.byteowls.jopencage.JOpenCageGeocoder
import com.byteowls.jopencage.model.JOpenCageForwardRequest
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HotelFilter {

  def retrieveCoordinates(street: String,
                          city: String,
                          countryCode: String,
                          geoCoderApiKey: String) = {
    val jOpenCageGeocoder = new JOpenCageGeocoder(geoCoderApiKey)
    val request = new JOpenCageForwardRequest(s"$street, $city")
    request.setRestrictToCountryCode(countryCode.toLowerCase)
    val response = jOpenCageGeocoder.forward(request);
    response.getFirstPosition
  }

  def cleanAndEnhanceHotelsDf(rowHotels: DataFrame, ss: SparkSession, apiKey: String): Dataset[ConvertedHotel] = {

    import ss.implicits._

    val hotelsRDD = rowHotels.as[Hotel]

    hotelsRDD.map(row => {

      if (row.latitude.isEmpty
        || row.longitude.isEmpty
        || "NA".equals(row.latitude.get)
        || "NA".equals(row.longitude.get)) {

        val retrievedCoordinates = retrieveCoordinates(row.address, row.city, row.country, apiKey)

        ConvertedHotel(
          row.id,
          row.name,
          row.country,
          row.city,
          row.address,
          retrievedCoordinates.getLat.doubleValue().toString,
          retrievedCoordinates.getLng.doubleValue().toString)
      }
      else
        ConvertedHotel(
          row.id,
          row.name,
          row.country,
          row.city,
          row.address,
          row.latitude.get,
          row.longitude.get)
    })
  }
}
