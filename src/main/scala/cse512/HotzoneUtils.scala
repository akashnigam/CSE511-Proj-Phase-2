package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART

    //parsed coordinates of rectangle and points in arrays
    var rectangle = new Array[String](4)
    rectangle = queryRectangle.split(",")
    var point = new Array[String](2)
    point = pointString.split(",")

    //get the coordinates of rectangle
    var r_x1 = rectangle(0).trim.toDouble
    var r_y1 = rectangle(1).trim.toDouble
    var r_x2 = rectangle(2).trim.toDouble
    var r_y2 = rectangle(3).trim.toDouble

    //get the coordinates of point
    var p_x = point(0).trim.toDouble
    var p_y = point(1).trim.toDouble

    //calculate min and max coordinates
    var minx,miny,maxx,maxy=0.0
    minx= if (r_x1 <= r_x2) r_x1 else r_x2
    miny= if (r_y1 <= r_y2) r_y1 else r_y2
    maxx= if (r_x1 <= r_x2) r_x2 else r_x1
    maxy= if (r_y1 <= r_y2) r_y2 else r_y1

    //check for point to lie in rectangle
    if(p_x >= minx && p_x <= maxx && p_y >= miny && p_y <= maxy)
      return true
    return false
    //return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
