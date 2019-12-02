package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def find_square_val(x:Int):Double=
  {
    return (x*x).toDouble;
  }

  
  def calculateZScore(neighbourCellCount: Int, counthcells: Int, n: Int, mean: Double, sd: Double): Double =
  {
    val numerator = (counthcells.toDouble - (mean*neighbourCellCount).toDouble).toDouble

    val denom1 = ((n.toDouble * neighbourCellCount.toDouble) - (neighbourCellCount.toDouble * neighbourCellCount.toDouble) / (n.toDouble - 1.0).toDouble).toDouble
    val denomerator = sd * math.sqrt(denom1).toDouble
    
    return (numerator / denomerator).toDouble
  }

  def numberOfNeighbours(minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, x: Int, y: Int, z: Int): Int = 
  {
    var count = 0;
    
    if (x == minX || x == maxX) {
      count += 1;
    }
    if (y == minY || y == maxY) {
      count += 1;
    }
    if (z == minZ || z == maxZ) {
      count += 1;
    }

    if (count == 1) {
      return 17;
    } else if (count == 2) {
      return 11;
    } else if (count == 3) {
      return 7;
    }

    return 26;

    /*if (count == 0) {
      return 26;
    }
    else {
      if (count == 1) {
        return 17;
      }

      if (count == 2) {
        return 11;
      }

      //if (count == 3){
        return 7;
      //}
    }*/

  }

  def numberOfNeighboursFound(x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int =
  {
    var count = 0

    if (x == minX || x == maxX) {
      count += 1
    }

    if (y == minY || y == maxY) {
      count += 1
    }

    if (z == minZ || z == maxZ) {
      count += 1
    }

    if (count == 1) {
      return 17;
    } else if (count == 2) {
      return 11;
    } else if (count == 3) {
      return 7;
    }

    return 26;
  }

  // YOU NEED TO CHANGE THIS PART
}
