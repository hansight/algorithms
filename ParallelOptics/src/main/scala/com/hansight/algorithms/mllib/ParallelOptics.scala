package  com.hansight.algorithms.mllib

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created by FU_Yong on 2015/01/26.
 * E-mail: fybluebird@gmail.com
 *
 * ParallelOptics is using Spark to parallel the distance calculate, and sequence update steps for OPTICS.
 *
 * e.g.
 * val opt = New ParallelOptics(minPts, radius)
 * val out = opt.run(data)
 *
 *  @param minPts : minimum density in OPTICS. If in a range radius the points number is bigger than or equal minPoints
 *                  they ara treated as belong a same cluster
 *  @param radius : the largest range for OPTICS to find minimum density
 *
 *         data   : RDD[T], here T is Array[Double] or String with URL format like "/a/b/c"
 *         out    : RDD[Point], witch records the necessary information to get clusters
 *
 *         e.g. to get a cluster with param epsilon ( epsilon <= radius )
 *         val flag = 0
 *         for ( i <- out ) {
 *            if (i.reachDis > epsilon) {
 *                if (i.coreDis > epsilon) {
 *                    data(i.id.toInt) is noise
 *                }
 *                else {
 *                    flag += 1
 *                    data(i.id.toInt) is belong to cluster(flag)
 *                }
 *            }
 *            else
 *              data(i.id.toInt) is belong to cluster(flag)
 *         }
 *
 * OPTICS part of ParallelOptics is reference from this paper :
 * http://cucis.ece.northwestern.edu/publications/pdf/PatPal13.pdf
 * and in procedure OPTICS part ParallelOptics has a small fixes. That is the first point add into output sequence is
 * must be a core point, but in this paper it used the first point its checked.
 *
 */
class ParallelOptics (minPts : Int, radius : Double) extends Serializable with Logging {

  /**
   *
   * Point is a container to record the OPTICS information for all points
   *
   * @param id_         : points index in original data
   * @param information : points name and something others we want to record in it
   */
  class Point ( id_         : Long,
                information : String = "" ) extends Serializable {
    val id        = id_
    var info      = information
    var reachDis  = Double.MaxValue
    var coreDis   = Double.MaxValue
    var processed = false
    var opticsId  = Long.MaxValue
    var notCore   = false

    def getP : Point = this
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
    private def lenvenshteinDistance (array1 : Array[String],
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

  private def getLenvenshteinDistance (points           : RDD[String],
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

  /**
   * ************************************************************************************************
   *
   *                                    NOTICE  NOTICE  NOTICE
   *
   * if want to get other type of distance, can write a distance calculate function using similar
   * input and output format
   *
   * ************************************************************************************************
   *
   * it used to calculate the distance between processing point and all input points
   *
   * @param obj : original points data and processing points' data
   * @return    : distance between processing points and all points
   */
  private def getDistance(obj : Any) : RDD[Double] = obj match {
    case (points : RDD[String], coordinateValues : String) =>
      getLenvenshteinDistance (points, coordinateValues)
    case (points : RDD[Array[Double]], coordinateValues : Array[Double]) =>
      getEuclideanDistance (points, coordinateValues)
  }

  private def getEuclideanDistance (points            : RDD[Array[Double]],
                                    coordinateValues  : Array[Double]) : RDD[Double] = {
    val distance = points.map{ p =>
      Math.sqrt(p.zip(coordinateValues).map{x => Math.pow(x._1-x._2, 2)}.sum)
    }
    distance.checkpoint()
    distance
  }

  /**
   *
   * update output sequence points
   *
   * @param points      : the final output of ParallelOptics
   * @param distance    : distance between processing points and all points
   * @param id          : the index of processing points in original data
   * @param opticsId    : output order of points
   * @param hasNeighbor : is in neighbor sequence still has points
   * @return            : undated points
   */
  private def update ( points      : RDD[Point],
                       distance    : RDD[Double],
                       id          : Long,
                       opticsId    : Long,
                       hasNeighbor : Boolean = false) : (RDD[Point], Boolean)= {
    val neiNum  = distance.filter{x => x<=radius}.count()
    var points_ = points
    val pointsT = points.zip(distance)
    if (neiNum > minPts) {
      val coreNei = distance.takeOrdered(minPts+1)
      val coreDis = coreNei(minPts)
      points_ = pointsT.map { p =>
        // Core point, set its output serial number as opticsId
        if (p._1.id == id) {
          p._1.coreDis   = coreDis
          p._1.opticsId  = opticsId
          p._1.processed = true
        }
        // Neighbor points, because one of they could be the next output, set there serial number as opticsId + 1,
        if (p._2<=radius & p._1.id!=id & !p._1.processed) {
          if (p._2 < coreDis)
            p._1.reachDis = Math.min(p._1.reachDis, coreDis)
          else
            p._1.reachDis = Math.min(p._1.reachDis, p._2)
          p._1.opticsId = opticsId + 1
        }
        // Updating the serial number of previous core points' neighbor
        if (p._2>radius & p._1.opticsId==opticsId & !p._1.processed)
          p._1.opticsId = opticsId + 1
        p._1.getP
      }
    }
    else {
      points_ = pointsT.map { p =>
        // Mark p is not core point
        if (p._1.id == id) {
          p._1.notCore = true
          // if p is already a neighbor point, mark it as processed, and output it
          if (hasNeighbor) {
            p._1.opticsId  = opticsId
            p._1.processed = true
          }
        }
        // Updating the serial number of previous core points' neighbor
        if (hasNeighbor & p._1.opticsId==opticsId & !p._1.processed & p._1.id!=id)
          p._1.opticsId = opticsId + 1
        p._1.getP
      }
    }
    points_.checkpoint()
    (points_, neiNum>minPts | hasNeighbor)
  }

  /**
   *
   * @param points  : input data, org.apache.spark.rdd.RDD[T]
   * @tparam T      : in this version, T is Array[Double] or String with URL format like "/a/b/c"
   * @return        : a sequence of class Point, it records the necessary information to get clusters
   */
  def run[T](points : RDD[T]) : RDD[Point] = {

    assert(minPts >= 0, "minPts smaller than 0")

    val initStartTime   = System.nanoTime()

    val pointsT         = points.zipWithIndex()
    // here can add a String information for each Point when call new Point(id, information)
    var points_         = pointsT.map{p => new Point(p._2)}
    var opticsId : Long = 0

    points.persist()
    pointsT.persist()
    points_.persist()

    while (points_.filter( p => !p.processed & !p.notCore ).count != 0 ) {

      var hasOut   = true
      var point    = points_.filter( p => !p.processed & !p.notCore ).first()
      var distance = getDistance(points, pointsT.filter{ p => p._2 == point.id }.first()._1)
      val temp     = update(points_, distance, point.id, opticsId)
      points_      = temp._1
      hasOut       = temp._2
      if (hasOut)
        opticsId  += 1

      while ( points_.filter( p => p.opticsId == opticsId).count > 0 ) {
        val hasNeighbor = true
        point           = points_.filter( p => !p.processed & p.opticsId==opticsId).sortBy(p => p.reachDis ).first()
        distance        = getDistance(points, pointsT.filter{ p => p._2 == point.id }.first()._1)
        points_         = update(points_, distance, point.id, opticsId, hasNeighbor)._1
        opticsId       += 1
      }
    }

    points_ = points_.sortBy(p => p.opticsId)

    points.unpersist()
    pointsT.unpersist()
    points_.unpersist()

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(s"POptics run took " + "%.3f".format(initTimeInSeconds) + " seconds.")

    points_
  }
}
