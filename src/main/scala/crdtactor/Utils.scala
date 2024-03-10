package crdtactor

import ch.qos.logback.classic.{Level as LBLevel, LoggerContext as LBLoggerContext}
import org.apache.pekko.actor.Address
import org.apache.pekko.cluster.UniqueAddress
import org.apache.pekko.cluster.ddata.SelfUniqueAddress
import org.slf4j.{Logger as SLLogger, LoggerFactory as SLLoggerFactory}
import smile.math.MathEx.median
import smile.plot.swing.boxplot

import java.awt.image.BufferedImage
import java.io.File
import java.util.concurrent.ThreadLocalRandom
import javax.imageio.ImageIO
import scala.collection.mutable

object Utils {

  // State which is safe to use from different threads, uses inefficient locking mechanism
  class SynchronizedState {
    private val state = scala.collection.mutable.Map.empty[Any, Any]
    private val lock = new Object

    def get[K, V](key: K): Option[V] = lock.synchronized:
      state.get(key).asInstanceOf[Option[V]]

    def put[K, V](key: K, value: V): Unit = lock.synchronized:
      state.put(key, value)

    def remove[K, V](key: K): Option[V] = lock.synchronized:
      state.remove(key).asInstanceOf[Option[V]]

    def getAll[K, V](): scala.collection.immutable.Map[K, V] =
      lock.synchronized:
        state.asInstanceOf[scala.collection.mutable.Map[K, V]].toMap
  }

  // Global state which can be shared between the actors
  // Note: this is an "anti-pattern", use it only for the bootstrapping process
  final val GLOBAL_STATE = new SynchronizedState()
  final val RANDOM_MESSAGE_DELAY = 50
  final val CRDT_SYNC_PERIOD = 500

  // Pessimistic total estimated delay for testing purposes
  final val RANDOM_BC_DELAY_SAFE = (RANDOM_MESSAGE_DELAY + 10) * 20

  private val r = new scala.util.Random
  def randomString(): String =
    r.nextString(2)

  def randomInt(): Int =
    r.nextInt(16)

  // Set the logger level which is used by Pekko
  def setLoggerLevel(level: String): Unit =
    val loggerContext: LBLoggerContext =
      SLLoggerFactory.getILoggerFactory().asInstanceOf[LBLoggerContext]
    val rootLogger: ch.qos.logback.classic.Logger =
      loggerContext.getLogger(SLLogger.ROOT_LOGGER_NAME)
    level match
      case "DEBUG" => rootLogger.setLevel(LBLevel.DEBUG)
      case "INFO"  => rootLogger.setLevel(LBLevel.INFO)
      case "WARN"  => rootLogger.setLevel(LBLevel.WARN)
      case "ERROR" => rootLogger.setLevel(LBLevel.ERROR)
      case "OFF"   => rootLogger.setLevel(LBLevel.OFF)
      case _ => throw new IllegalArgumentException(s"Unknown log level: $level")

  // Creates a unique address for use as node identifiers in the CRDT library
  // Example:
  // val selfNode = Utils.nodeFactory()
  // crdtstate.put(selfNode, key, value)
  def nodeFactory() =
    SelfUniqueAddress(
      UniqueAddress(
        Address("crdtactor", "crdt"),
        ThreadLocalRandom.current.nextLong()
      )
    )
  def writeToCsv(
      data: Seq[(String, String, String | Long)],
      filename: String
  ): Unit =
    val rows = data.map(x => s"${x._1},${x._2},${x._3}")
    val csv = rows.mkString("\n")
    val file = new java.io.File(filename)
    val bw = new java.io.BufferedWriter(new java.io.FileWriter(file))
    bw.write(csv)
    bw.close()

  def calculateResponseTimesDiffs(
      responseTimes: mutable.Map[String, Long],
      requestTimes: mutable.Map[String, Long]
  ): Seq[(String, String, Long)] = {
    // Sort by opId
    val responseTimesSorted = responseTimes.toSeq.sortBy(_._2)
    responseTimesSorted.map { case (opId, responseTime) =>
      // Split opId
      val (actorNr, opNr, innerOpNr) = opId.split(" - ") match
        case Array(a, b)    => (a, b, 0L)
        case Array(a, b, c) => (a, b, c)
        case _              => println(s"Unknown opId $opId"); ("", "", "")
      requestTimes.get(opId) match
        case None =>
          println(s"WARNING: No match for op $opId")
          (
            actorNr,
            opNr + "-" + innerOpNr,
            0L
          ) // This should not happen, but this hack would create a bias if it did
        case Some(requestTime) =>
          (actorNr, opNr + "-" + innerOpNr, responseTime - requestTime)
    }
  }

  def printStats(label: String, responseDiffs: Seq[Long]): Unit = {
    val responseDiffsList = responseDiffs.toList
    val mean = responseDiffsList.sum / responseDiffsList.size.max(1)
    val variance = responseDiffsList
      .map(x => math.pow(x - mean, 2))
      .sum / responseDiffsList.size
    val stdDev = math.sqrt(variance)
    println(s"$label - Mean: $mean, Variance: $variance, StdDev: $stdDev")

    val med = median(responseDiffs.map(_.toDouble).toArray)
    println(s"$label - Median response time: $med ms")
  }

  def saveBoxPlot(
      data: Array[Array[Double]],
      labels: Array[String],
      yLabel: String,
      filename: String
  ): Unit = {
    val canvas = boxplot(data, labels)
    canvas.setAxisLabels("xlabel", yLabel)
    // Export canvas as image
    val image: BufferedImage = canvas.toBufferedImage(800, 600)

    val outputFile = new File(filename)
    ImageIO.write(image, "png", outputFile)
  }
}
