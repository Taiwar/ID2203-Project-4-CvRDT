package crdtactor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.mutable

class PerformanceTest extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  import CRDTActorV4.*

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

      // For each actor, create a thread sending writes and reads with equal frequency
      val probes = (0 until N_ACTORS).map { i =>
        val probe = createTestProbe[Command]()
        probe
      }
      val putRequestTimes = mutable.Map[String, Long]()
      val getRequestTimes = mutable.Map[String, Long]()
      val requesters = (0 until N_ACTORS).map { i =>
        new Thread(() => {
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
            Thread.sleep(0, 50000) // 0.05 ms
          }
          println(s"Requesting $i done")
        })
      }

      // For each actor, create a thread reading from the thread probe
      var responses = 0
      val putResponseTimes = mutable.Map[String, Long]()
      val getResponseTimes = mutable.Map[String, Long]()
      val readers = (0 until N_ACTORS).map { i =>
        new Thread(() => {
          while (TESTING) {
            probes(i).receiveMessage() match {
              case PutResponse(pId, _) =>
                putResponseTimes.put(pId, System.currentTimeMillis())
              case GetResponse(gId, _, _) =>
                getResponseTimes.put(gId, System.currentTimeMillis())
              case _ => println(s"BAD: Unexpected message")
            }
            responses += 1
          }
          println(s"Reading $i done")
        })
      }

      // Run requesters and readers for 20 seconds
      // Get current time
      val start = System.currentTimeMillis()
      requesters.foreach(_.start())
      readers.foreach(_.start())
      Thread.sleep(TEST_TIME)
      // soft shutdown
      TESTING = false
      // hard interrupt
      // requesters.foreach(_.interrupt())
      // readers.foreach(_.interrupt())
      val end = System.currentTimeMillis()

      // Wait for stabilization
      Thread.sleep(50)

      // Log total time and response count
      println(s"Total time: ${end - start} ms")
      println(s"Total responses: $responses")
      // Calculate average ops per second
      val ops = responses / ((end - start) / 1000)
      println(s"Average ops per second: $ops")

      // Calculate average response time for puts
      // For every key in putResponseTimes, calculate the difference between the time of the response and the time of the request
      var noMatch = 0
      val putResponseTimesDiffs = putResponseTimes.map {
        case (pId, responseTime) =>
          putRequestTimes.get(pId) match
            case None =>
              noMatch += 1
              0L // TODO: This creates a bias in the average
            case Some(requestTime) =>
              responseTime - requestTime
      }
      println(s"No match: $noMatch")
      val putResponseTimeAvg =
        putResponseTimesDiffs.sum / putResponseTimesDiffs.size
      println(s"Average response time for puts: $putResponseTimeAvg ms")

      // Calculate unanswered puts and gets
      val unansweredPuts = putRequestTimes.size - putResponseTimes.size
      val unansweredGets = getRequestTimes.size - getResponseTimes.size
      println(s"Unanswered: ${unansweredPuts + unansweredGets}")
    }

  }
}
