package  com.hansight.algorithms.mllib

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD


/**
 * Created by FU_Yong on 2015/01/26.
 * E-mail: fybluebird@gmail.com
 *
 * ParallelOptics is using Spark to parallel the distance calculate, and sequence update steps for OPTICS.
 *
 * how to use it:
 * step 1: creat a Spark context
 * val conf = new SparkConf().setMaster("local[*]").setAppName("POptics").set("spark.default.parallelism", "4")
 * val sc   = new SparkContext(conf)
 * sc.setCheckpointDir("checkpoint")
 *
 * setp 2: run ParalleOptics
 * val opt = New ParallelOptics(minPts, radius)
 * val out = opt.run(data)
 *
 *  @param minPts       : minimum density in OPTICS. If in a range radius the points number is bigger than or equal minPoints
 *                        they ara treated as belong a same cluster
 *  @param radius       : the largest range for OPTICS to find minimum density
 *  @param distanceType : like "EuclideanDistance", "LenvenshteinDistance"... more details see object Distance
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
class ParallelOptics ( minPts       : Int,
                       radius       : Double,
                       distanceType : String = "EuclideanDistance") extends Serializable with Logging {

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

  /**
   *
   * it used to calculate the distance between processing point and all input points
   *
   * @param points            : original all points
   * @param coordinateValues  : processing point
   * @param distanceType      : defined in object Distance, like "EuclideanDistance", "LenvenshteinDistance"...
   * @tparam T                : according to the type of the input points, like String, Array[Double]...
   * @return                  : distance between processing point and all input points
   */
  private def getDistance[T]( points           : RDD[T],
                              coordinateValues : T,
                              distanceType     : String) : RDD[Double] = {
    Distances.get(points, coordinateValues, distanceType)
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
      var distance = getDistance(points, pointsT.filter{ p => p._2 == point.id }.first()._1, distanceType)
      val temp     = update(points_, distance, point.id, opticsId)
      points_      = temp._1
      hasOut       = temp._2
      if (hasOut)
        opticsId  += 1

      while ( points_.filter( p => p.opticsId == opticsId).count > 0 ) {
        val hasNeighbor = true
        point           = points_.filter( p => !p.processed & p.opticsId==opticsId).sortBy(p => p.reachDis ).first()
        distance        = getDistance(points, pointsT.filter{ p => p._2 == point.id }.first()._1, distanceType)
        points_         = update(points_, distance, point.id, opticsId, hasNeighbor)._1
        opticsId       += 1
      }
    }

    points_ = points_.sortBy(p => p.opticsId)

    points.unpersist()
    pointsT.unpersist()
    points_.unpersist()

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(s"ParallelOptics run took " + "%.3f".format(initTimeInSeconds) + " seconds.")

    points_
  }
}
