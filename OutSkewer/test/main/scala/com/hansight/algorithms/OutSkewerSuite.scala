package com.hansight.algorithms

import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

/**
 *
 * Created by FU_Yong on 2014/10/27.
 * E-mail: fybluebird@gmail.com
 *
 */
class OutSkewerSuite extends FunSuite {

  test ("no distinct data") {

    val x = ListBuffer(0.73, 0.73, 0.73)

    val os = new OutSkewer
    os detectOutliers x
    assert(os.getNo.forall(i => i == 1))
    assert(os.getMaybe.forall(i => i == 0))
    assert(os.getYes.forall(i => i == 0))
    os clear()

    os detectOutliersDynamically x
    assert(os.getNo.forall(i => i == 1))
    assert(os.getMaybe.forall(i => i == 0))
    assert(os.getYes.forall(i => i == 0))
    os clear()
  }

  test ("one outlier") {

    val x = ListBuffer(0.11, 0.73, 0.73)

    val os = new OutSkewer
    os detectOutliers x
    assert(os.getNo(0) == 0 & os.getNo(1) == 1 & os.getNo(2) == 1)
    assert(os.getMaybe.forall(i => i == 0))
    assert(os.getYes(0) == 1 & os.getYes(1) == 0 & os.getYes(2) == 0)
    os clear()

    os detectOutliersDynamically x
    assert(os.getNo(0) == 0 & os.getNo(1) == 1 & os.getNo(2) == 1)
    assert(os.getMaybe.forall(i => i == 0))
    assert(os.getYes(0) == 1 & os.getYes(1) == 0 & os.getYes(2) == 0)
    os clear()
  }

  test ("one maybe outliere")  {

    val x = ListBuffer(0.72, 0.71, 0.73)

    val os = new OutSkewer
    os detectOutliers x
    assert(os.getNo(0) == 1 & os.getNo(1) == 0 & os.getNo(2) == 1)
    assert(os.getMaybe(0) == 0 & os.getMaybe(1) == 1 & os.getMaybe(2) == 0)
    assert(os.getYes.forall(i => i == 0))
    os clear()

    os detectOutliersDynamically x
    assert(os.getNo(0) == 1 & os.getNo(1) == 0 & os.getNo(2) == 1)
    assert(os.getMaybe(0) == 0 & os.getMaybe(1) == 1 & os.getMaybe(2) == 0)
    assert(os.getYes.forall(i => i == 0))
    os clear()
  }

  test ("random big data") {

    val dg = new DataGen
    val x  = dg.sinData(n=999)

    val os = new OutSkewer
    os detectOutliers x
    assert(os.getMaybe.zip(os.getNo).map(i => i._1+i._2)
      .zip(os.getYes).map(i => i._1+i._2)
      .forall(i => i==1))
    os clear()

    os detectOutliersDynamically(x, 16)
    assert(os.getMaybe.zip(os.getNo).map(i => i._1+i._2)
      .zip(os.getYes).map(i => i._1+i._2)
      .forall(i => i==1))
    os clear()

    os detectOutliersDynamically(x, 512)
    assert(os.getMaybe.zip(os.getNo).map(i => i._1+i._2)
      .zip(os.getYes).map(i => i._1+i._2)
      .forall(i => i==1))
    os clear()
  }

  test ("remove outliers") {

    val x  = ListBuffer(0.1, 0.11, 0.1)

    val os = new OutSkewer
    val x_ = os removeElementAtOutliers(x, 0.5)

    assert(x_.size == 2)
  }

}

