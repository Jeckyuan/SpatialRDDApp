package org.example

import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD

object SpatialRDDUtil {

  def readShpFile(sparkSession: SparkSession, shpFile: String): SpatialRDD[geom.Geometry] = {
    //If the file you are reading contains non-ASCII characters you'll need to explicitly set the encoding
    System.setProperty("geospark.global.charset", "utf8")

    val spatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shpFile)

    println("field names: " + spatialRDD.fieldNames)
    println("first geometry: " + spatialRDD.rawSpatialRDD.rdd.first())
    println("CRS transformation: " + spatialRDD.getCRStransformation)
    spatialRDD.analyze()
    println("boundary envelope: " + spatialRDD.boundaryEnvelope)

    return spatialRDD
  }

  //117.050, 118.490, 38.367, 39.450  tianjin
  def envelopeQuery(minX: Double, maxX: Double, minY: Double, maxY: Double, spatialRDD: SpatialRDD[Geometry]): Unit = {
    val rangeQueryWindow = new Envelope(minX, maxX, minY, maxY)
    val considerBoundaryIntersection = false // Only return gemeotries fully covered by the window
    val usingIndex = false
    var queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, considerBoundaryIntersection, usingIndex)

    println("results size: " + queryResult.collect().size())
    queryResult.rdd.foreach(f => println(f))
  }

}
