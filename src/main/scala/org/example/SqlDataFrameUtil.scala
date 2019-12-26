package org.example

import com.vividsolutions.jts.geom
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

object SqlDataFrameUtil {

  //117.050, 38.367, 118.490, 39.450  tianjin
  def envelopeQuery(minX: Double, minY: Double, maxX: Double, maxY: Double, spatialRDD: SpatialRDD[Geometry], sparkSession: SparkSession): Unit = {
    // This function will register GeoSpark User Defined Type, User Defined Function and optimized join query strategy
    GeoSparkSQLRegistrator.registerAll(sparkSession)
    // spatial RDD to raw data frame
    val rawDf = Adapter.toDf(spatialRDD, sparkSession)
    println("raw data frame schema: ")
    rawDf.printSchema()
    // raw data frame to spatial data frame
    rawDf.createOrReplaceTempView("rawdf")
    var spatialDf = sparkSession.sql(
      """
        |SELECT ST_GeomFromWKT(geometry) AS geometry, osm_id, name, type, population
        |FROM rawdf
      """.stripMargin)
    println("spatial data frame schema: ")
    spatialDf.printSchema()
    //
    spatialDf.createOrReplaceTempView("spatialdf")
    println("source data: ")
    spatialDf.show(3, true)

    val sb = new StringBuilder()
    sb.append("SELECT * FROM").append(" spatialdf")
    sb.append(" WHERE ST_Contains(")
    sb.append("ST_PolygonFromEnvelope(")
      .append(minX).append(", ").append(minY).append(", ").append(maxX).append(", ").append(maxY).append(")")
    sb.append(", geometry)")

    //    spatialDf = sparkSession.sql(
    //      """|SELECT *
    //         |FROM spatialdf
    //         |WHERE ST_Contains(ST_PolygonFromEnvelope(117.050, 38.367, 118.490, 39.450), geometry)
    //      """.stripMargin)
    spatialDf = sparkSession.sql(sb.toString())
    spatialDf.createOrReplaceTempView("spatialdf")
    println("result data: ")
    spatialDf.show()
  }

}
