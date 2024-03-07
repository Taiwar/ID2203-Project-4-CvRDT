package crdtactor

import crdtactor.CRDTActorV4.*
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class PerformanceTestFutures
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike {

  trait StoreSystem {
    val N_ACTORS = 4

    Utils.setLoggerLevel("INFO")

    // Create the actors
    val actors = (0 until N_ACTORS).map { i =>
      // Spawn the actor and get its reference (address)
      val actorRef = spawn(Behaviors.setup[Command] { ctx =>
        Behaviors.withTimers(timers => new CRDTActorV4(i, ctx, timers))
      })
      i -> actorRef
    }.toMap

    // Write actor addresses into the global state
    actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))

    // Set leader (BLE mock)
    actors.foreach((_, actorRef) => actorRef ! Leader(actors(0)))

    Thread.sleep(50) // Wait for actors to be ready
  }

  "Performance test" must {

    "no overhead equal writes and reads" in new StoreSystem {
      val TEST_TIME = 5000
      var TESTING = true
      implicit val context: ExecutionContextExecutor =
        ExecutionContext.fromExecutor(
          Executors.newFixedThreadPool(N_ACTORS * 2)
        )

      // For each actor, create a thread sending writes and reads with equal frequency
      val probes = (0 until N_ACTORS).map { i =>
        val probe = createTestProbe[Command]()
        probe
      }
      // Get current time
      val start = System.currentTimeMillis()

      println("TEST: Building requesters")
      val requesters = (0 until N_ACTORS).map { i =>
        Future {
          val putRequestTimes = mutable.Map[String, Long]()
          val getRequestTimes = mutable.Map[String, Long]()

          val actorRef = actors(i)
          var j = 0
          while (TESTING) {
            val opId = s"$i - $j"
            val pId = "p" + opId
            val gId = "g" + opId

            val putRequestTime = System.currentTimeMillis()
            actorRef ! Put(pId, Utils.randomString(), j, probes(i).ref)
            putRequestTimes.put(pId, putRequestTime)

            val getRequestTime = System.currentTimeMillis()
            actorRef ! Get(gId, Utils.randomString(), probes(i).ref)
            getRequestTimes.put(gId, getRequestTime)
            j += 1
            Thread.sleep(0, 5000) // 0.05 ms
          }
          (putRequestTimes, getRequestTimes)
        }
      }

      println("TEST: Building readers")
      // For each actor, create a thread reading from the thread probe
      val readers = (0 until N_ACTORS).map { i =>
        Future {
          val putResponseTimes = mutable.Map[String, Long]()
          val getResponseTimes = mutable.Map[String, Long]()
          while (TESTING) {
            probes(i).receiveMessage(TEST_TIME.millis) match {
              case PutResponse(pId, _) =>
                putResponseTimes.put(pId, System.currentTimeMillis())
              case GetResponse(gId, _, _) =>
                getResponseTimes.put(gId, System.currentTimeMillis())
              case _ => println(s"Unexpected message")
            }
          }
          (putResponseTimes, getResponseTimes)
        }
      }

      // Stop futures after TEST_TIME
      println("TEST: Sleeping")
      Thread.sleep(TEST_TIME)
      // soft shutdown
      TESTING = false
      println("TEST: Stopping")
      // hard interrupt
      // requesters.foreach(_.interrupt())
      // readers.foreach(_.interrupt())

      // Collect results
      val responseTimes
          : Seq[(mutable.Map[String, Long], mutable.Map[String, Long])] =
        Await.result(
          Future.sequence(readers),
          scala.concurrent.duration.Duration.Inf
        )
      println("TEST: Responses collected")
      val requestTimes
          : Seq[(mutable.Map[String, Long], mutable.Map[String, Long])] =
        Await.result(
          Future.sequence(requesters),
          scala.concurrent.duration.Duration.Inf
        )
      println("TEST: Requests collected")

      val end = System.currentTimeMillis()
      // Merge put and get request and response times from every future
      val putRequestTimes = requestTimes.map(_._1).reduce(_ ++ _)
      val getRequestTimes = requestTimes.map(_._2).reduce(_ ++ _)
      val putResponseTimes = responseTimes.map(_._1).reduce(_ ++ _)
      val getResponseTimes = responseTimes.map(_._2).reduce(_ ++ _)

      // Log total time and response count
      val requests = putRequestTimes.size + getRequestTimes.size
      val responses = putResponseTimes.size + getResponseTimes.size
      println(s"Total time: ${end - start} ms")
      println(s"Total requests: $requests")
      println(s"Total responses: $responses")
      println(s"Total unmatched requests: ${requests - responses}")
      // Calculate average ops per second
      val ops = responses / ((end - start) / 1000)
      println(s"Average ops per second: $ops")

      // Calculate average response time for puts
      var noMatch = 0
      val putResponseTimesDiffs = putResponseTimes.map {
        case (pId, responseTime) =>
          putRequestTimes.get(pId) match
            case None =>
              println(s"WARNING: No match for put $pId")
              noMatch += 1
              0L // This should not happen, but this hack would create a bias if it did
            case Some(requestTime) =>
              responseTime - requestTime
      }
      val putResponseTimeAvg =
        putResponseTimesDiffs.sum / putResponseTimesDiffs.size.max(1)
      println(s"Average response time for puts: $putResponseTimeAvg ms")

      val getResponseTimesDiffs = getResponseTimes.map {
        case (gId, responseTime) =>
          getRequestTimes.get(gId) match
            case None =>
              println(s"WARNING: No match for get $gId")
              noMatch += 1
              0L // This should not happen, but this hack would create a bias if it did
            case Some(requestTime) =>
              responseTime - requestTime
      }

      val getResponseTimeAvg =
        getResponseTimesDiffs.sum / getResponseTimesDiffs.size.max(1)
      println(s"Average response time for gets: $getResponseTimeAvg ms")

      println(s"Total unmatched requests (should be 0): $noMatch")

      // Prepare data for put requests
      val putCsvRows = putResponseTimesDiffs.map(_.toString)
      val putCsv = putCsvRows.mkString("\n")
      val putFilename = "evaluation/data/put_performance_test.csv"
      val putFile = new java.io.File(putFilename)
      val putBw = new java.io.BufferedWriter(new java.io.FileWriter(putFile))
      putBw.write(putCsv)
      putBw.close()

      // Prepare data for get requests
      val getCsvRows = getResponseTimesDiffs.map(_.toString)
      val getCsv = getCsvRows.mkString("\n")
      val getFilename = "evaluation/data/get_performance_test.csv"
      val getFile = new java.io.File(getFilename)
      val getBw = new java.io.BufferedWriter(new java.io.FileWriter(getFile))
      getBw.write(getCsv)
      getBw.close()

    }

  }
}
