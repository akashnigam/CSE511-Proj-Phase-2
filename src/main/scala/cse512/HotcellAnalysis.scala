package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  pickupInfo.createOrReplaceTempView("pickupInfo")

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  var numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  val temp = spark.sql("select x,y,z from pickupInfo where x >= " + minX + " and x <= " +
    maxX + " and y >= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + 
    maxZ + " order by z,y,x")
  temp.createOrReplaceTempView("selectedCells")
  //temp.show()

  val temp1 = spark.sql("select x,y,z, count(*) as hotCells from selectedCells group by x,y,z order by 3,2,1")
  temp1.createOrReplaceTempView("SelectedCellsWithHotness")
  //temp1.show()

  spark.udf.register("find_square_val", (x: Int) => ((HotcellUtils.find_square_val(x))))
  val squared_hot_cells = spark.sql("select sum(find_square_val(hotCells)) as squared_hot_cells from SelectedCellsWithHotness")
  squared_hot_cells.createOrReplaceTempView("squared_hot_cells")
  //squared_hot_cells.show()

  val total_hotness = spark.sql("select sum(hotCells) as sumHotCells from SelectedCellsWithHotness")
  total_hotness.createOrReplaceTempView("total_hotness")
  //total_hotness.show()

  val mean_hotness = (total_hotness.first.getLong(0).toDouble / numCells)
  //println(mean_hotness)

  var sq_hotcells_by_numCells = (squared_hot_cells.first().getDouble(0) / numCells)
  var mean_squared = (mean_hotness.toDouble * mean_hotness.toDouble)
  var sd = scala.math.sqrt(sq_hotcells_by_numCells - mean_squared)

  spark.udf.register("numberOfNeighbours", (minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, x: Int, y: Int, z: Int) => ((HotcellUtils.numberOfNeighboursFound(x, y, z, minX, minY, minZ, maxX, maxY, maxZ))))

  val NeighbourCells = spark.sql("select numberOfNeighbours(" + minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + ", s1.x, s1.y, s1.z) as neighbourCount,"
      + "s1.x as x, s1.y as y, s1.z as z, sum(s2.hotCells) as counthcells "
      + "from SelectedCellsWithHotness as s1, SelectedCellsWithHotness as s2 "
      + "where (s2.x = s1.x+1 or s2.x = s1.x or s2.x = s1.x-1) "
      + "and (s2.y = s1.y+1 or s2.y = s1.y or s2.y = s1.y-1) "
      + "and (s2.z = s1.z+1 or s2.z = s1.z or s2.z = s1.z-1) "
      + "group by s1.z, s1.y, s1.x "
      + "order by s1.z, s1.y, s1.x")
  NeighbourCells.createOrReplaceTempView("NeighbourCells")
  //NeighbourCells.show()

  spark.udf.register("calculateZScore", (neighbourCellCount: Int, counthcells: Int, n: Int, mean: Double, sd: Double) => ((HotcellUtils.calculateZScore(neighbourCellCount, counthcells, n, mean, sd))))

  pickupInfo = spark.sql("select x,y,z from (select calculateZScore(neighbourCount, counthcells, "+ numCells + "," + mean_hotness + ", " + sd + ") as zScore, x, y, z from NeighbourCells order by zScore desc)");
  pickupInfo.createOrReplaceTempView("zScore")
  pickupInfo.show()
  
  return pickupInfo // YOU NEED TO CHANGE THIS PART
}
}
