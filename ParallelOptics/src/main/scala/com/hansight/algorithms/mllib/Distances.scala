package  com.hansight.algorithms.mllib

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by FU_Yong on 2015/01/26.
 * E-mail: fybluebird@gmail.com
 *
 * if want to get other type of distance, can write a distance calculate function using similar
 * input and output format, and add it into function get[T].
 *
 * NOTICE : distance.checkpoint() is necessary and important after distance calculation
 *
 */
object Distances {

  def get[T] ( points            : RDD[T],
               coordinateValues  : T,
               distanceType : String) : RDD[Double] = (points, coordinateValues, distanceType) match {

    case (points : RDD[Array[Double]], coordinateValues : Array[Double], "EuclideanDistance") =>
      getEuclideanDistance(points, coordinateValues)

    case (points : RDD[String], coordinateValues : String, "LenvenshteinDistance") =>
      getLenvenshteinDistance(points, coordinateValues)

    case (points : RDD[String], coordinateValues : String, "URL_LenvenshteinDistance") =>
      getURL_LenvenshteinDistance(points, coordinateValues)
  }

  object LenvenshteinDistance {
    private def lowerOfThree(f : Double, s : Double, t : Double) : (Double, Int) = {
      val min = Math.min(f, s)
      var idx = 0
      if (f > s) {
        if (t > s)
          idx = 1
        else
          idx = 2
      }
      else {
        if (t <= f)
          idx = 2
      }
      (Math.min(min, t), idx)
    }
    private def lenvenshteinDistance ( array1 : Array[String],
                                       array2 : Array[String]) : (Double, Array[Int]) = {
      var dis   = Double.MaxValue
      val n     = array1.length
      val m     = array2.length
      val alley = ArrayBuffer[Int]()
      if (m == 0) {
        dis = n * 1.5
        alley ++= Array.fill(n)(-1)
      }
      else if (n == 0) {
        dis = m * 1.5
        alley ++= Array.fill(m)(1)
      }
      else {
        val matrix = Array.tabulate(n+1, m+1)((n,m) => n*1.5 + m*1.5)
        for (i <- 1 to n; j <- 1 to m) {
          if ( array1(i-1) == array2(j-1) )
            matrix(i)(j) = lowerOfThree(matrix(i-1)(j)+1.5, matrix(i)(j-1)+1.5, matrix(i-1)(j-1))._1
          else
            matrix(i)(j) = lowerOfThree(matrix(i-1)(j)+1.5, matrix(i)(j-1)+1.5, matrix(i-1)(j-1)+1.0)._1
        }
        dis     = matrix(n)(m)
        var tn  = n
        var tm  = m
        var i   = 0
        while ( tn > 0 & tm > 0 ) {
          val ta    = lowerOfThree(matrix(tn-1)(tm), matrix(tn)(tm-1), matrix(tn-1)(tm-1))
          ta._2 match {
            case 0 => alley += -1; tn -= 1
            case 1 => alley += 1 ; tm -= 1;
            case 2 => alley += 0 ; tn -= 1; tm -= 1
          }
          i += 1
        }
      }
      (dis, alley.toArray.reverse)
    }
    def dis (array1 : Array[String], array2 : Array[String]) : (Double, Array[Int]) = {
      val dis   = lenvenshteinDistance(array1, array2)
      ((dis._1 - dis._2.filter(_!=0).length*0.5) / dis._2.length.toDouble, dis._2)
    }
  }

  private def getURL_LenvenshteinDistance ( points           : RDD[String],
                                            coordinateValues : String) : RDD[Double] = {
    val distance = points.map{ p =>
      assert(p.charAt(0) == '/', """The first char of URL is not '/'""")
      val URLs1     = p.split("/")                 drop 1
      val URLs2     = coordinateValues.split("/")  drop 1
      val URLAlley  = LenvenshteinDistance.dis(URLs1, URLs2)._2
      var URLDis    = .0
      val arrayT    = Array(Array[String](), Array[String]())
      var indexT    = Array(0, 0)
      for ( alley <- URLAlley ) {
        alley match {
          case 0 =>
            arrayT(0)  = URLs1(indexT(0)).toArray.map(_.toString)
            arrayT(1)  = URLs2(indexT(1)).toArray.map(_.toString)
            indexT     = indexT.map(_+1)
            URLDis    += LenvenshteinDistance.dis(arrayT(0), arrayT(1))._1
          case 1 =>
            arrayT(1)  = URLs2(indexT(1)).toArray.map(_.toString)
            indexT(1) += 1
            URLDis    += LenvenshteinDistance.dis(arrayT(0), arrayT(1))._1
          case -1 =>
            arrayT(0)  = URLs1(indexT(0)).toArray.map(_.toString)
            indexT(0) += 1
            URLDis    += LenvenshteinDistance.dis(arrayT(0), arrayT(1))._1
        }
      }
      if (URLAlley.length != 0)
        URLDis /= URLAlley.length
      URLDis
    }
    distance.checkpoint()
    distance
  }

  private def getLenvenshteinDistance ( points           : RDD[String],
                                        coordinateValues : String) : RDD[Double] = {
    val distance = points.map{ p =>
      val array1  = p.toArray.map(_.toString)
      val array2  = coordinateValues.toArray.map(_.toString)
      LenvenshteinDistance.dis(array1, array2)._1
    }
    distance.checkpoint()
    distance
  }


  private def getEuclideanDistance ( points            : RDD[Array[Double]],
                                     coordinateValues  : Array[Double]) : RDD[Double] = {
    val distance = points.map{ p =>
      Math.sqrt(p.zip(coordinateValues).map{x => Math.pow(x._1-x._2, 2)}.sum)
    }
    distance.checkpoint()
    distance
  }
}

