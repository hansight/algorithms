package com.hansight.algorithms

import scala.collection.mutable.ListBuffer

/****
 * Created by FU_Yong on 2014/10/27.
 * E-mail: fybluebird@gmail.com
 *
 * Outskewer: Using Skewness to Spot Outliers in Samples and Time Series
 * from http://sheymann.github.io/outskewer/
 *
 * this is a translation from .R to .scala
 *
 * #!/usr/bin/Rscript
#
# OUTSKEWER detect outliers in numeric samples and time series.
#
# S. Heymann, M. Latapy, and C. Magnien. "Outskewer: Using Skewness to Spot
# Outliers in Samples and Time Series", submitted to ASONAM'12.
#
# Version: 1.0   (2012 April)
#
# Copyright (C) 2012 S閎astien Heymann <sebastien.heymann@lip6.fr>
# see LICENSE.txt
# see http://outskewer.sebastien.pro
#
# Usage: source("path/to/outskewer.R")
#
# DEBUG mode:
# options(error=utils::recover)
 */

class OutSkewer {

  private var x               = ListBuffer[Double]()
  private var yes             = ListBuffer[Int]()
  private var maybe           = ListBuffer[Int]()
  private var no              = ListBuffer[Int]()
  private var skew_signature  = ListBuffer[Double]()
  private var unknown         = false

  def getX              : ListBuffer[Double]  = x
  def getYes            : ListBuffer[Int]     = yes
  def getMaybe          : ListBuffer[Int]     = maybe
  def getNo             : ListBuffer[Int]     = no
  def getSkew_signature : ListBuffer[Double]  = skew_signature
  def getUnknown        : Boolean             = unknown

  def clear() {
    x               --=  x
    yes             --=  yes
    maybe           --=  maybe
    no              --=  no
    skew_signature  --=  skew_signature
    unknown           =  false
  }

  /***************************************************************
  Skewness <- function(x, verbose = FALSE) {
    # Compute the sample skewness for a distribution of values.
    #
    # Args:
    #   x: Vector of numbers whose skewness is to be calculated.
    #   verbose: If TRUE, prints sample skewness; if not, not. Default is FALSE.
    #
    # Returns:
    #   The skewness of x.
    if (missing(x))
      stop("Argument x is missing.")
    if (is.null(x))
    stop("Argument x should not be null.")

    n <- length(x)
    if (n < 3)
      skew <- NA
    else {
      m <- mean(x)
      s <- sd(x)
      skew <- (n / ((n-1) * (n-2))) * sum(((x - m)/s)^^3)
    }
    if (verbose)
      cat("Sample skewness = ", skew, ".\n", sep = "")
    return(skew)
  }

  ***************************************************************/

  def skewness (x : ListBuffer[Double]) = {
    var skew = Double.NaN
    val n    = x.size

    assert(x.nonEmpty, "Argument x should not be null.")

    if (n >= 3) {

      val m = x.sum / n

      val s =  Math.sqrt(x.foldLeft(.0)((s, i) => s + (i-m)*(i-m)) / n)

      skew = n.toDouble / ((n-1.0) * (n-2.0)) *
        x.foldLeft(.0)((skew, i) => skew + Math.pow((i-m)/s, 3.0))
    }
    skew
  }

  /*********************************************************************
  SkewnessSignature <- function(x) {
    # Compute half of the skewness signature for a distribution of values.
    #
    # Args:
    #   x: Vector of numbers whose skewness signature is to be calculated.
    #
    # Returns:
    #   The half of the skewness signature of x.
    if (missing(x))
        stop("Argument x is missing.")
    if (is.null(x))
        stop("Argument x should not be null.")

    i <- 1
    j <- length(x)
    half.n <- floor(length(x) / 2)
    signature <- rep(NA, half.n)
    h <- half.n

    skew <- Skewness(x)
    # Sort with index.return returns a sorted x in sorted$x and the
    # indices of the sorted values from the original x in sorted$ix
    sorted <- sort(x, decreasing=FALSE, index.return=TRUE)
    new.x <- x[sorted$ix[i:j]]
    while (h > 0 && !is.na(skew)) {
        signature[h] <- skew
        h <- h - 1
        if (skew > 0) {
            # Remove max from x
            j <- j - 1
        }
        else {
            # Remove min from x
            i <- i + 1
        }
        new.x <- x[sorted$ix[i:j]]
        skew <- Skewness(new.x)
    }
    return(signature)
  }
  *********************************************************************/

  def skewnessSignature (x : ListBuffer[Double]) : ListBuffer[Double] = {
    val j = x.size
    var h = j / 2
    val signature = ListBuffer.fill(h)(Double.NaN)

    assert(x.nonEmpty, "Argument x should not be null.")

    var skew = skewness(x)
    var tx   = x.sorted
    while ( h>0 && !skew.isNaN) {
      signature update (h-1, skew)
      h -= h
      if (skew > 0)
        tx = tx dropRight 1
      else
        tx = tx drop 1
      skew = skewness(tx)
    }
    signature
  }

  /*********************************************************************
  IsNotPStable <- function(skew.signature, n, p) {
    # Do we reject H_0 : "the skewness signature is p-stable"?
    #
    # Args:
    #   skew.signature: Vector of half of the skewness signature to be tested.
    #   n: Size of the original vector of values.
    #   p: Number between 0 and 1, proportion of the extremal values.
    #
    # Returns:
    #   The skewness signature of x.
    if (missing(skew.signature))
      stop("Argument skew.signature is missing.")
    if (missing(n))
      stop("Argument n is missing.")
    if (missing(p))
      stop("Argument p is missing.")
    if (n <= 0)
      stop("Argument n must be positive.")
    if (is.null(skew.signature))
    stop("Argument skew.signature should not be null.")

    m <- 0
    # Count the number of skewess values until |skew| > p
    if (length(skew.signature) != 0) {
      for (i in seq(along=skew.signature)) {
        skew <- skew.signature[i]
        if (abs(skew) > p)
          break
        m <- m + 1
      }
    }
    reject_h0 <- ( (m / n) < p)  # Decision
    return(reject_h0)
  }
  *********************************************************************/

  private def isNotPStable (skew_signature : ListBuffer[Double],
                            n              : Int,
                            p              : Double) : Boolean = {
    var reject_h0: Boolean = false

    assert(skew_signature.nonEmpty, "Argument skew_signature should not be null.")
    assert( n >= 0, "Argument n must be positive.")

    var m     = 0
    var break = false
    var i     = 0
    while (!break && i<skew_signature.size) {
      if (Math.abs(skew_signature(i)) > p)
        break = true
      else
        m += 1
      i += 1
    }
    reject_h0 = (m.toDouble / n.toDouble) < p
    reject_h0
  }

  /*********************************************************************
  Findt <- function(x, skew.signature) {
    # Compute 0.5 - t.
    #
    # Args:
    #   x: Vector of numbers whose largest skewness threshold is to be calculated.
    #   skew.signature: Vector of half of the skewness signature of x.
    #
    # Returns:
    #   The largest skewness threshold of x.
    n <- length(x)
    if (n < 3)
      stop("Argument x should contain more than 2 element.")

    from <- length(skew.signature) / n
    by_ <- 1 / n
    return(FindP(x, skew.signature, n, from=from, to=by_, by_=-by_))
  }
  FindT <- function(x, skew.signature) {
    # Compute 0.5 - T.
    #
    # Args:
    #   x: Vector of numbers whose smallest skewness threshold is to be calculated.
    #   skew.signature: Vector of half of the skewness signature of x.
    #
    # Returns:
    #   The smallest skewness threshold of x.
    n <- length(x)
    if (n < 3)
      stop("Argument x should contain more than 2 element.")

    to <- length(skew.signature) / n
    by_ <- 1 / n
    return(FindP(x, skew.signature, n, from=by_, to=to, by_=by_))
  }
  *********************************************************************/

  private class ListP {
    var t        = Double.NaN
    var rejected = true
  }
  private def find_tOrT (x              : ListBuffer[Double],
                         skew_signature : ListBuffer[Double],
                         is_t           : Boolean) : ListP = {
    var lp = new ListP
    val n  = x.size

    assert( n > 2, "Argument x should contain more than 2 element.")

    val ft = skew_signature.size.toDouble / n.toDouble
    val by_  = 1.0 / n.toDouble
    if (is_t)
      lp = findP(x, skew_signature, n, ft, by_, -by_)
    else
      lp = findP(x, skew_signature, n, by_, ft, by_)
    lp
  }

  /*********************************************************************
  FindP <- function(x, skew.signature, n, from, to, by_) {
    # Find the first skewness value where
    # the skewness signature of 'x' is p-stable,
    # starting skewness from 'from' to 'to'.
    #
    # Args:
    #   x: Vector of numeric values.
    #   skew.signature: Vector being half of the skewness signature of x.
    #   n: Length of x.
    #   from: largest skewness value possible.
    #   to: smallest skewness value possible.
    #   by_: gap between two skewness values.
    #
    # Returns:
    #   The first skewness value where the skewness signature of 'x' is p-stable,
    #   and a boolean of value TRUE if no value where found.
    if (missing(x))
      stop("Argument x is missing.")
    if (missing(skew.signature))
      stop("Argument skew.signature is missing.")
    if (missing(n))
      stop("Argument n is missing.")
    if (missing(from))
      stop("Argument from is missing.")
    if (missing(to))
      stop("Argument to is missing.")
    if (missing(by_))
      stop("Argument by_ is missing.")
    if (is.null(x))
    stop("Argument x should not be null.")
    if (is.null(skew.signature))
    stop("Argument skew.signature should not be null.")

    p <- from
    rejected <- TRUE

    for (i in seq(from, to, by_)) {
      reject_h0 <- IsNotPStable(skew.signature, n, i)
      if (!reject_h0) { # if s is p-stable
        p <- i
        rejected <- FALSE
        break
      }
    }

    return(list(t = p,
      rejected = rejected ))
  }
  *********************************************************************/

  private def findP ( x              : ListBuffer[Double],
                      skew_signature : ListBuffer[Double],
                      n              : Int, from : Double,
                      to             : Double, by_ : Double) : ListP = {
    val lp = new ListP

    assert(skew_signature.nonEmpty, "Argument skew_signature should not be null.")
    assert(x.nonEmpty, "Argument x should not be null.")

    var p        = from
    var rejected = true
    var loop     = from
    var break    = false

    while (loop <= to & !break) {
      val reject_h0 = isNotPStable(skew_signature, n, loop)
      if (!reject_h0) {
        p        = loop
        rejected = false
        break    = true
      }
      loop += by_
    }

    lp.t = p
    lp.rejected = rejected
    lp
  }

  /*********************************************************************
  RemoveOutliers <- function(x, p) {
    # Remove outliers in the vector 'x'.
    #
    # Args:
    #   x: Vector of numeric values.
    #   p: Number between 0 and 1, proportion of extremal values to remove.
    #
    # Returns:
    #   The vector of values without a proportion p of extremal values.
    if (missing(x))
      stop("Argument x is missing.")
    if (missing(p))
      stop("Argument p is missing.")
    if (is.null(x))
    stop("Argument x should not be null.")

    # Sort with index.return returns a sorted x in sorted$x and the
    # indices of the sorted values from the original x in sorted$ix
    sorted <- sort(x, decreasing=FALSE, index.return=TRUE)
    i <- 1
    j <- length(x)
    new.x <- x[sorted$ix[i:j]]
    skew <- Skewness(x)
    while (!is.na(skew) && abs(skew) > p) {
      if( skew > 0 ) {
        # Remove max from x
          j <- j - 1
        new.x <- x[sorted$ix[i:j]]
      }
      else {
        # Remove min from x
          i <- i + 1
        new.x <- x[sorted$ix[i:j]]
      }
      skew <- Skewness(new.x)
    }
    return(new.x)
  }
  *********************************************************************/

  def removeElementAtOutliers (x : ListBuffer[Double],
                               p : Double) : ListBuffer[Double] = {

    assert(x.nonEmpty, "Argument x should not be null.")

    var tx   = x.sorted
    var skew = skewness(x)

    while (!skew.isNaN & Math.abs(skew) > p) {
      if (skew > 0)
        tx = tx dropRight 1
      else
        tx = tx drop 1
      skew = skewness(tx)
    }
    tx
  }

  /*********************************************************************
  DetectOutliers <- function(x, verbose=FALSE) {
    # Compute outlier scores ('yes', 'maybe', 'no', 'unknown')
    # on the entire data set.
    #
    # Args:
    #   df_: Vector of numeric values.
    #   verbose: If TRUE, prints t and T; if not, not. Default is FALSE.
    #
    # Returns:
    #   The data frame with outliers score computed, and the skewness signature.
    if (missing(x))
      stop("Argument x is missing.")
    if (is.null(x))
    stop("Argument x should not be null.")

    # Prepare data frame
    init <- rep.int(0, length(x))
    df_ <- data.frame(x = x, yes = init, maybe = init, no = init, unknown = init)

    # Compute skewness thresholds
    skew.signature <- SkewnessSignature(x)
    res.high <- Findt(x, skew.signature)
    res.low <- FindT(x, skew.signature)
    t_ <- res.low$t
    T_ <- res.high$t

    if (verbose)
      cat("T = ", T_, "\nt = ", t_, "\n", sep = "")

    # Apply outlier status
    if (res.high$rejected && res.low$rejected) {
      df_$unknown <- 1
    }
    else {
      # Sort with index.return returns a sorted x in sorted$x and the
      # indices of the sorted values from the original x in sorted$ix
      sorted <- sort(x, decreasing=FALSE, index.return=TRUE)
      new.x <- x
      i <- 1
      j <- length(x)
      skew <- Skewness(x)
      below.t <- FALSE
      while (!is.na(skew) && !below.t) {
        if (verbose)
          cat("|skew| = ", abs(skew), "\nmax = ", max(new.x), "\nmin = ", min(new.x), "\n", sep = "")

        if (abs(skew) <= t_) {
          below.t <- TRUE
        }
        colname <- 'maybe'
        if (abs(skew) > T_) {
          colname <- 'yes'
        }
        if (skew > 0) {
          # Remove max from x
            df_[df_$x==sorted$x[j], colname] <- 1
          j <- j - 1
        }
        else {
          # Remove min from x
            df_[df_$x==sorted$x[i], colname] <- 1
          i <- i + 1
        }
        new.x <- x[sorted$ix[i:j]]
        skew <- Skewness(new.x)
      }
      df_[df_$yes != 1 & df_$maybe != 1, 'no'] <- 1
    }
    return(list(df=df_, skew.signature=skew.signature))
  }
  *********************************************************************/

  private class Data_frame {
    var x               = ListBuffer[Double]()
    var yes             = ListBuffer[Int]()
    var maybe           = ListBuffer[Int]()
    var no              = ListBuffer[Int]()
    var skew_signature  = ListBuffer[Double]()
    var unknown         = false
  }
  def detectOutliers (x : ListBuffer[Double]) {
    val df_             = detectOutliers(x, new Data_frame)
    this.x              = df_.x
    this.yes            = df_.yes
    this.maybe          = df_.maybe
    this.no             = df_.no
    this.unknown        = df_.unknown
    this.skew_signature = df_.skew_signature
  }
  private def detectOutliers ( x   : ListBuffer[Double],
                               df_ : Data_frame) : Data_frame = {
    val j  = x.size
    var tx = x clone()

    assert(x.nonEmpty, "Argument x should not be null.")

    val init = ListBuffer.fill(j)(0)

    df_.x       = x    clone()
    df_.yes     = init clone()
    df_.maybe   = init clone()
    df_.no      = init clone()
    df_.unknown = false

    val skew_signature  = skewnessSignature(tx)
    tx                  = x                     clone()
    df_.skew_signature  = skew_signature        clone()

    val is_t      = true
    val res_high  = find_tOrT(x, skew_signature, is_t)
    val res_low   = find_tOrT(x, skew_signature, !is_t)
    val t_        = res_low.t
    val T_        = res_high.t

    if (res_high.rejected & res_low.rejected)
      df_.unknown = true
    else {
      tx          = tx.sorted
      var skew    = skewness(tx)
      var below_t = false
      while (!skew.isNaN & !below_t) {
        if (math.abs(skew) <= t_)
          below_t = true
        var idx   = -1
        var temp  = .0
        if (skew > 0) {
          temp = tx(tx.size - 1)
          tx = tx dropRight 1
        }
        else {
          temp = tx(0)
          tx = tx drop 1
        }
        idx = df_.x.indexOf(temp)
        while (idx >= 0) {
          if (math.abs(skew) > T_) {
            df_.yes.update(idx, 1)
            df_.maybe.update(idx, 0)
          }
          else {
            df_.maybe.update(idx, 1)
            df_.yes.update(idx, 0)
          }
          idx = df_.x.indexOf(temp, idx + 1)
        }
        skew = skewness(tx)
      }
      var loop = 0
      while (loop < j) {
        assert((df_.maybe(loop) + df_.yes(loop)) != 2)
        if (df_.maybe(loop) != 1 & df_.yes(loop) != 1)
          df_.no.update(loop, 1)
        loop += 1
      }
    }
    df_
  }

  /*********************************************************************
  DetectOutliersDynamically <- function(df_, w=100) {
    # Compute outlier scores ('yes', 'maybe', 'no', 'unknown')
    # on the data set using a sliding time window.
    #
    # Args:
    #   df_: Data frame of values as rows and 'x', 't' as columns.
    #
    # Returns:
    #   The data frame with outliers score computed.
      init <- rep.int(0, nrow(df_))
    df_$yes <- init
    df_$maybe <- init
    df_$no <- init
    df_$unknown <- init
    for (i in (w + 1):(nrow(df_) + 1)) {
      subset.df <- df_[(i - w):(i - 1), ]
      subset.df <- DetectOutliersDF(subset.df)
      df_[(i - w):(i - 1), ] <- subset.df
    }
    return(df_)
  }
  *********************************************************************/

  def detectOutliersDynamically (x : ListBuffer[Double],
                                 w : Int = 100) {
    val init = ListBuffer.fill(x.size)(0)

    this.x       = x     clone()
    this.yes     = init  clone()
    this.maybe   = init  clone()
    this.no      = init  clone()
    this.unknown = false

    if (x.size <= w)
      detectOutliers(x)
    else {
      var loop = w
      var tx   = x clone()
      while (loop <= x.size) {
        val df_ = detectOutliers(tx.take(w), new Data_frame)
        if (loop == x.size) {
          for (lop <- 0 until w) {
            this.yes    update  ( loop-w+lop, df_.yes(lop)  )
            this.maybe  update  ( loop-w+lop, df_.maybe(lop))
            this.no     update  ( loop-w+lop, df_.no(lop)   )
          }
        }
        else {
          this.yes    update  ( loop-w, df_.yes(0)  )
          this.maybe  update  ( loop-w, df_.maybe(0))
          this.no     update  ( loop-w, df_.no(0)   )
        }
        if (df_.unknown)
          this.unknown = true
        loop += 1
        tx    = tx drop 1
      }
    }
  }

  /*******************************************************************
   *                          print result                           *
   *******************************************************************/

  def print_result()
  {
    println("YES: ")
    var loop = 0
    while (loop < this.x.size) {
      if (this.yes(loop) == 1)
        println(this.x(loop)+ " "*(10-this.x(loop).toString.length) + "index: " + loop)
      loop += 1
    }
    println("MAYBE: ")
    loop = 0
    while (loop < this.x.size) {
      if (this.maybe(loop) == 1)
        println(this.x(loop)+ " "*(10-this.x(loop).toString.length) + "index: " + loop)
      loop += 1
    }
    if (unknown) println("Can not find OutSkewer on this kind of data")
  }
}
