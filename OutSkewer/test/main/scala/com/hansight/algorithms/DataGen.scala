package com.hansight.algorithms

import scala.collection.mutable.ListBuffer

/*************************************************
  * Created by FU_Yong on 2014/10/28.
  * E-mail: fybluebird@gmail.com
  *
  *            Data Generator
  *
  ***********************************************/

class DataGen {

  /*************************************************

    *   Sin Data Generator
    *
    * @param n       : number of data points want to get
    * @param length  : horizontal range
    * @param height  : vertical coordinate range
    * @param medial  : symmetry axis
    * @param radius  : half thickness of the data distribution
    * @param noise   : the noise ratio
    *
    *     eg :
    *            * *    ------
    *          *     *  height
    *         *-------*-------*-- medial
    *                  *     *      n      is 11
    *                    * *        radius is 0
    *        |    length     |      noise  is 0
    *
    *
    **************************************************/
  def sinData( n      : Int = 250,
               length : Double = 2.0*Math.PI,
               height : Double = 1.0,
               radius : Double = 0.5,
               medial : Double = 0.0,
               noise  : Double = 0.1) : ListBuffer[Double] = {

    var data       = ListBuffer[Double]()
    var noise_mark = .0

    for ( i <- 1 to n ) {
      data += math.random * length
    }
    data = data.sorted

    var idx = 0
    for ( i <-  data ) {
      noise_mark = math.random
      if (noise_mark > noise)
        data update (idx, math.round((height * math.sin(i) + medial
          + (math.random * radius*2 - radius)) * 100.0) / 100.0)
      else
        data update (idx, math.round((height * math.sin(i) + medial
          + (math.random * radius*2 - radius)) * 100.0) / (math.random * 33))
      idx += 1
    }
    data
  }
}

object DataGenTest {
  def main
  (args: Array[String]) {
    val out = new DataGen
    println(out.sinData())
  }
}
