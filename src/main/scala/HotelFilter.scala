import ScMain.sparkSession
import com.byteowls.jopencage.JOpenCageGeocoder
import com.byteowls.jopencage.model.JOpenCageForwardRequest
import org.apache.spark.sql.{DataFrame, SparkSession}

class HotelFilter extends java.io.Serializable {

  def retrieveCoordinates(street: String,
                          city: String,
                          countryCode: String) = {
    val jOpenCageGeocoder = new JOpenCageGeocoder("5f189b638d024369ac7318e4eedb9a5c")
    val request = new JOpenCageForwardRequest(s"$street, $city")
    request.setRestrictToCountryCode(countryCode.toLowerCase)
    val response = jOpenCageGeocoder.forward(request);
    response.getFirstPosition
  }

  def cleanAndEnhanceHotelsDf(rowHotels: DataFrame, ss: SparkSession): DataFrame = {

    import ss.implicits._

    val hotelsRDD = rowHotels.as[Hotel].rdd

    hotelsRDD.map(row => {

      if (row.Latitude.isEmpty
        || row.Longitude.isEmpty
        || "NA".equals(row.Latitude.get)
        || "NA".equals(row.Longitude.get) ) {

        val retrievedCoordinates = retrieveCoordinates(row.Address, row.City, row.Country)

        ConvertedHotel(
          row.Id,
          row.Name,
          row.Country,
          row.City,
          row.Address,
          retrievedCoordinates.getLat.doubleValue().toString,
          retrievedCoordinates.getLng.doubleValue().toString)
      }
      else
        ConvertedHotel(
          row.Id,
          row.Name,
          row.Country,
          row.City,
          row.Address,
          row.Latitude.get,
          row.Longitude.get)
    }).toDF("Id", "Name", "Country", "City", "Address", "Latitude", "Longitude")
  }
}
