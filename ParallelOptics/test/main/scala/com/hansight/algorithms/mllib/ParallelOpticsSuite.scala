package  com.hansight.algorithms.mllib

import java.io.File

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite
import scala.collection.mutable.ListBuffer

/**
 * Created by FU_Yong on 2015/01/26.
 * E-mail: fybluebird@gmail.com
 *
 */

class ParallelOpticsSuite extends FunSuite {

  val conf = new SparkConf().setMaster("local[*]").setAppName("POptics").set("spark.default.parallelism", "4")
  val sc   = new SparkContext(conf)
  sc.setCheckpointDir("checkpoint")

  class P[T](flag_ : Int, data_ : T) {
    val flag     = flag_
    val data     = data_
  }

  def getCluster[T]( optResult  : Array[_<:ParallelOptics#Point],
                     data       : Array[T],
                     r          : Double ) : (Int, Int, ListBuffer[ListBuffer[P[T]]]) = {
    var flag  = 0
    var noise = 0
    val lb    = ListBuffer[ListBuffer[P[T]]]()
    val lbn   = ListBuffer[P[T]]()
    val lb_   = ListBuffer[P[T]]()
    for ( i <- optResult ) {
      if (i.reachDis > r) {
        if (i.coreDis > r) {
          noise += 1
          lbn   += new P(-1, data(i.id.toInt))
        }
        else {
          flag += 1
          if (lb_.length > 0)
            lb += lb_.clone()
          lb_ --= lb_
          lb_  += new P(flag, data(i.id.toInt))
        }
      }
      else
        lb_  += new P(flag, data(i.id.toInt))
    }
    if (lb_.length > 0)
      lb += lb_.clone()
    if (lbn.length > 0)
      lb += lbn
    (flag, noise, lb)
  }

  def deleteFolder(name : String = "checkpoint") : Unit = {
    val file  = new File(name)
    if (!file.isFile) {
      val files = file.listFiles()
      for (f <- files)
        deleteFolder(f.getAbsolutePath)
      file.delete()
    }
    else
      file.delete()
  }

  test ("single point with EuclideanDistance") {

    val data  = Array(
      Array(1.0, 2.0, 6.0)
    )
    val data_ = sc.parallelize(data)

    // noise must be 0, cluster must be 1
    var opt = new ParallelOptics(0, Double.MaxValue, "EuclideanDistance")
    var out = opt.run(data_).collect()
    var clu = getCluster(out, data, 10000)
    assert(clu._1            == 1)
    assert(clu._2            == 0)
    assert(clu._3.length     == 1)
    assert(clu._3(0)(0).flag == 1)

    // noise must be 1, cluster must be 0
    opt = new ParallelOptics(1, -2, "EuclideanDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 1)
    assert(clu._1            == 0)
    assert(clu._2            == 1)
    assert(clu._3.length     == 1)
    assert(clu._3(0)(0).flag == -1)
    opt = new ParallelOptics(3, 100, "EuclideanDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 50)
    assert(clu._1            == 0)
    assert(clu._2            == 1)
    assert(clu._3.length     == 1)
    assert(clu._3(0)(0).flag == -1)
  }

  test ("single point with URL_LenvenshteinDistance") {
    val data  = Array(
      "/a/b/c"
    )
    val data_ = sc.parallelize(data)

    // noise must be 0, cluster must be 1
    var opt = new ParallelOptics(0, 3, "URL_LenvenshteinDistance")
    var out = opt.run(data_).collect()
    var clu = getCluster(out, data, 2)
    assert(clu._1            == 1)
    assert(clu._2            == 0)
    assert(clu._3.length     == 1)
    assert(clu._3(0)(0).flag == 1)

    // noise must be 1, cluster must be 0
    opt = new ParallelOptics(1, -2, "URL_LenvenshteinDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 1)
    assert(clu._1            == 0)
    assert(clu._2            == 1)
    assert(clu._3.length     == 1)
    assert(clu._3(0)(0).flag == -1)
    opt = new ParallelOptics(3, 10, "URL_LenvenshteinDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 1.1)
    assert(clu._1            == 0)
    assert(clu._2            == 1)
    assert(clu._3.length     == 1)
    assert(clu._3(0)(0).flag == -1)

  }

  test ("no distinct points with EuclideanDistance") {
    val data  = Array(
      Array(1.0, 2.0, 3.0),
      Array(1.0, 2.0, 3.0),
      Array(1.0, 2.0, 3.0)
    )
    val data_ = sc.parallelize(data)

    // noise must be 0, cluster must be 1
    // lb = [[cluster_1]]
    var opt = new ParallelOptics(1, Double.MaxValue, "EuclideanDistance")
    var out = opt.run(data_).collect()
    var clu = getCluster(out, data, 0)
    assert(clu._1           == 1)
    assert(clu._2           == 0)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 3)
    assert(clu._3(0).forall(x => x.flag == 1))

    // noise must be 3, cluster must be 0
    // lb = [[noise]]
    opt = new ParallelOptics(1, -2, "EuclideanDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 0)
    assert(clu._1           == 0)
    assert(clu._2           == 3)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 3)
    assert(clu._3(0).forall(x => x.flag == -1))
    opt = new ParallelOptics(Int.MaxValue, Double.MaxValue, "EuclideanDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 99999.0)
    assert(clu._1           == 0)
    assert(clu._2           == 3)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 3)
    assert(clu._3(0).forall(x => x.flag == -1))

  }

  test ("no distinct points with URL_LenvenshteinDistance") {
    val data  = Array(
      "/apple/bear/bee",
      "/apple/bear/bee",
      "/apple/bear/bee"
    )
    val data_ = sc.parallelize(data)

    // noise must be 0, cluster must be 1
    // lb = [[cluster_1]]
    var opt = new ParallelOptics(1, Double.MaxValue, "URL_LenvenshteinDistance")
    var out = opt.run(data_).collect()
    var clu = getCluster(out, data, 0)
    assert(clu._1           == 1)
    assert(clu._2           == 0)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 3)
    assert(clu._3(0).forall(x => x.flag == 1))

    // noise must be 3, cluster must be 0
    // lb = [[noise]]
    opt = new ParallelOptics(1, -2, "URL_LenvenshteinDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 0)
    assert(clu._1           == 0)
    assert(clu._2           == 3)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 3)
    assert(clu._3(0).forall(x => x.flag == -1))
    opt = new ParallelOptics(Int.MaxValue, Double.MaxValue, "URL_LenvenshteinDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 1.414)
    assert(clu._1           == 0)
    assert(clu._2           == 3)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 3)
    assert(clu._3(0).forall(x => x.flag == -1))

  }

  test ("one noise points with EuclideanDistance") {
    val data  = Array(
      Array(1.0, 2.0, 3.0),
      Array(2.0, 2.0, 3.0),
      Array(1.0, 2.0, 2.0),
      Array(-10.0, 20.0, -30.0)
    )
    val data_ = sc.parallelize(data)

    // noise must be 0, cluster must be 1
    // lb = [[cluster_1]]
    var opt = new ParallelOptics(2, Double.MaxValue, "EuclideanDistance")
    var out = opt.run(data_).collect()
    var clu = getCluster(out, data, 3578654.0)
    assert(clu._1           == 1)
    assert(clu._2           == 0)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 4)
    assert(clu._3(0).forall(x => x.flag == 1))

    // noise must be 1, cluster must be 1
    // lb = [[cluster_1],[noise]]
    clu = getCluster(out, data, 3)
    assert(clu._1           == 1)
    assert(clu._2           == 1)
    assert(clu._3.length    == 2)
    assert(clu._3(0).length == 3)
    assert(clu._3(1).length == 1)
    assert(clu._3(0).forall(x => x.flag == 1))
    assert(clu._3(1)(0).flag == -1)
    assert(clu._3(1)(0).data == data(3))

    // noise must be 4, cluster must be 0
    // lb = [[noise]]
    clu = getCluster(out, data, 0)
    assert(clu._1           == 0)
    assert(clu._2           == 4)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 4)
    assert(clu._3(0).forall(x => x.flag == -1))
    opt = new ParallelOptics(5, Double.MaxValue, "EuclideanDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 14527561.0)
    assert(clu._1           == 0)
    assert(clu._2           == 4)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 4)
    assert(clu._3(0).forall(x => x.flag == -1))

  }

  test ("one noise points with URL_LenvenshteinDistance") {
    val data  = Array(
      "/apple/bear/bee",
      "/apple/pear/bee",
      "/apple/bear/be",
      "/"
    )
    val data_ = sc.parallelize(data)

    // noise must be 0, cluster must be 1
    // lb = [[cluster_1]]
    var opt = new ParallelOptics(2, Double.MaxValue, "URL_LenvenshteinDistance")
    var out = opt.run(data_).collect()
    var clu = getCluster(out, data, 1.0)
    assert(clu._1           == 1)
    assert(clu._2           == 0)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 4)
    assert(clu._3(0).forall(x => x.flag == 1))

    // noise must be 1, cluster must be 1
    // lb = [[cluster_1],[noise]]
    clu = getCluster(out, data, 0.5)
    assert(clu._1           == 1)
    assert(clu._2           == 1)
    assert(clu._3.length    == 2)
    assert(clu._3(0).length == 3)
    assert(clu._3(1).length == 1)
    assert(clu._3(0).forall(x => x.flag == 1))
    assert(clu._3(1)(0).flag == -1)
    assert(clu._3(1)(0).data == data(3))

    // noise must be 4, cluster must be 0
    // lb = [[noise]]
    clu = getCluster(out, data, 0)
    assert(clu._1           == 0)
    assert(clu._2           == 4)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 4)
    assert(clu._3(0).forall(x => x.flag == -1))
    opt = new ParallelOptics(5, Double.MaxValue, "URL_LenvenshteinDistance")
    out = opt.run(data_).collect()
    clu = getCluster(out, data, 7.77)
    assert(clu._1           == 0)
    assert(clu._2           == 4)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 4)
    assert(clu._3(0).forall(x => x.flag == -1))

  }


  test ("random big data") {
    def randn(mean : Double = 0, std : Double = 1) : Double = {
      val z = Math.sqrt(-2.0 * Math.log(1.0 - Math.random)) * Math.cos(2.0 * Math.PI * Math.random)
      mean + std * z
    }
    val data  = Array.fill(150, 4)(randn(-300, 50)) ++ Array.fill(200, 4)(randn(0, 50)) ++ Array.fill(150, 4)(randn(300, 50))
    val data_ = sc.parallelize(data, 100)

    // noise must be 0, cluster must be 1
    // lb = [[cluster_1]]
    val opt = new ParallelOptics(20, Double.MaxValue, "EuclideanDistance")
    val out = opt.run(data_).collect()
    var clu = getCluster(out, data, 31415926.0)
    assert(clu._1           == 1)
    assert(clu._2           == 0)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 500)
    assert(clu._3(0).forall(x => x.flag == 1))

    // noise must be 1000, cluster must be 0
    // lb = [[noise]]
    clu = getCluster(out, data, 0)
    assert(clu._1           == 0)
    assert(clu._2           == 500)
    assert(clu._3.length    == 1)
    assert(clu._3(0).length == 500)
    assert(clu._3(0).forall(x => x.flag == -1))

    // cluster must be 3
    // lb = [[cluster_1],[cluster_2],[cluster_3],[noise]]
    clu = getCluster(out, data, 100)
    assert(clu._1        == 3, " cluster " + clu._1)
    assert(clu._2        >= 0)
    assert(clu._3.length >= 3)
    assert(clu._3(0).forall(x => x.flag == 1))
    assert(clu._3(1).forall(x => x.flag == 2))
    assert(clu._3(2).forall(x => x.flag == 3))
    if (clu._2 > 0) {
      assert(clu._3(0).length + clu._3(1).length + clu._3(2).length + clu._3(3).length == 500)
      assert(clu._3(3).forall(x => x.flag == -1))
    }

    deleteFolder()

  }
}

