package org.sia.chapter03App

import com.vividsolutions.jts.geom.Envelope
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.formatMapper.WktReader
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.example.{SpatialRDDUtil, SqlDataFrameUtil}

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("GeoSparkRunnableExample") // Change this to a proper name
    conf.setMaster("local[*]") // Delete this if run in cluster mode
    // Enable GeoSpark custom Kryo serializer
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    //    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)


    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    //    val sc = new SparkContext(conf)
    val sc = sparkSession.sparkContext

    //    val pointRDDInputLocation = "/Download/checkin.csv"
    //    val pointRDDOffset = 0 // The point long/lat starts from Column 0
    //    val pointRDDSplitter = FileDataSplitter.CSV
    //    val pointCarryOtherAttributes = true // Carry Column 2 (hotel, gas, bar...)
    //    var pointObjectRDD = new PointRDD(sc, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, pointCarryOtherAttributes)
    //
    //
    //    val polygonRDDInputLocation = "/Download/checkin.csv"
    //    val polygonRDDStartOffset = 0 // The coordinates start from Column 0
    //    val polygonRDDEndOffset = 8 // The coordinates end at Column 8
    //    val polygonRDDSplitter = FileDataSplitter.CSV
    //    val polygonCarryOtherAttributes = true // Carry Column 10 (hotel, gas, bar...)
    //    var polygonObjectRDD = new PolygonRDD(sc, polygonRDDInputLocation, polygonRDDStartOffset, polygonRDDEndOffset, polygonRDDSplitter, polygonCarryOtherAttributes)
    //
    //    val inputLocation = "/Download/checkin.csv"
    //    val wktColumn = 0 // The WKT string starts from Column 0
    //    val allowTopologyInvalidGeometries = true // Optional
    //    val skipSyntaxInvalidGeometries = false // Optional
    //    val spatialRDD = WktReader.readToGeometryRDD(sc, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)

    val shapefileInputLocation = "/home/yuanjk/data/shp/places"

    val spatialRDD = SpatialRDDUtil.readShpFile(sparkSession, shapefileInputLocation)

    SpatialRDDUtil.envelopeQuery(117.050, 118.490, 38.367, 39.450, spatialRDD)

    SqlDataFrameUtil.envelopeQuery(117.050, 38.367, 118.490, 39.450, spatialRDD, sparkSession)
  }

}
