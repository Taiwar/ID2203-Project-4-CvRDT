package crdtactor

import crdtactor.CRDTActorV4.*
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.MailboxSelector
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.*
import scala.concurrent.duration.*
import scala.util.Random

class PerformanceTest
    extends ScalaTestWithActorTestKit("""
CRDTKVStore {
  CRDTActor {
    mailbox-type = "org.apache.pekko.dispatch.UnboundedControlAwareMailbox"
  }
}
""")
    with AnyWordSpecLike {

  trait StoreSystem {
    // Configuration
    val TEST_TIME = 60000
    val REQUEST_WAIT_NS = 75000 // 0.75 ms
    val REQUEST_WAIT_NS_VARIANCE = 25000 // 0.25 ms
    val N_ACTORS = 4

    // Shared variable between threads, do not edit manually
    var TESTING = true
    implicit val context: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(
        Executors.newFixedThreadPool(N_ACTORS * 2)
      )

    Utils.setLoggerLevel("WARN")

    // Create the actors
    val props = MailboxSelector.fromConfig("CRDTKVStore.CRDTActor")

    val actors = (0 until N_ACTORS).map { i =>
      // Spawn the actor and get its reference (address)
      val actorRef = spawn(
        Behaviors.setup[Command] { ctx =>
          Behaviors.withTimers(timers => new CRDTActorV4(i, ctx, timers))
        },
        s"CRDTActor-$i",
        props
      )
      i -> actorRef
    }.toMap

    // Write actor addresses into the global state
    actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))

    // Set leader (BLE mock)
    actors.foreach((_, actorRef) => actorRef ! Leader(actors(0)))

    Thread.sleep(50) // Wait for actors to be ready
  }

  "Performance test" must {

    "basic all regular ops" in new StoreSystem {
      // For each actor, create a thread sending writes and reads with equal frequency
      val probes = (0 until N_ACTORS).map { i =>
        val probe = createTestProbe[Command]()
        probe
      }
      // Get current time
      val start = System.currentTimeMillis()

      println("Building requesters")
      // TODO: Remove the gets and just keep puts for simplicity
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
            Thread.sleep(0, REQUEST_WAIT_NS)
          }
          (putRequestTimes, getRequestTimes)
        }
      }

      println("Building readers")
      // For each actor, create a thread reading from the thread probe
      val readers = (0 until N_ACTORS).map { i =>
        Future {
          val putResponseTimes = mutable.Map[String, Long]()
          val getResponseTimes = mutable.Map[String, Long]()
          while (TESTING) {
            try {
              probes(i).receiveMessage(TEST_TIME.millis) match {
                case PutResponse(pId, _) =>
                  putResponseTimes.put(pId, System.currentTimeMillis())
                case GetResponse(gId, _, _) =>
                  getResponseTimes.put(gId, System.currentTimeMillis())
                case _ => println(s"Reader: Unexpected message")
              }
            } catch {
              case e: Exception => println(s"Reader: Timed out $e")
            }
          }
          (putResponseTimes, getResponseTimes)
        }
      }

      // Stop futures after TEST_TIME
      println("Letting futures run")
      Thread.sleep(TEST_TIME)
      // soft shutdown
      TESTING = false
      println("Stopping")

      // Collect results
      readers.foreach(_.recover({ case e => println(s"Reader: Error $e") }))
      val seq = Future.sequence(readers)
      seq.recover({ case e => println(s"Reader sequence: Error $e") })
      val responseTimes
          : Seq[(mutable.Map[String, Long], mutable.Map[String, Long])] =
        Await.result(
          Future.sequence(readers),
          scala.concurrent.duration.Duration.Inf
        )
      println("Responses collected")
      val requestTimes
          : Seq[(mutable.Map[String, Long], mutable.Map[String, Long])] =
        Await.result(
          Future.sequence(requesters),
          scala.concurrent.duration.Duration.Inf
        )
      println("Requests collected")

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

      // Write total responses and time to file
      Utils.writeToCsv(
        Seq(
          ("Total requests", "Total responses", "Total time (ms)"),
          (requests.toString, responses.toString, (end - start).toString)
        ),
        "evaluation/data/basic_performance_test.totals.csv"
      )

      // Calculate average response time for puts
      val putResponseTimesDiffs: Seq[(String, String, Long)] =
        Utils.calculateResponseTimesDiffs(putResponseTimes, putRequestTimes)
      Utils.printStats("Put", putResponseTimesDiffs.map(_._3))

      val getResponseTimesDiffs: Seq[(String, String, Long)] =
        Utils.calculateResponseTimesDiffs(getResponseTimes, getRequestTimes)
      Utils.printStats("Get", getResponseTimesDiffs.map(_._3))

      // Prepare data for put requests
      Utils.writeToCsv(
        putResponseTimesDiffs,
        "evaluation/data/basic_put_performance_test.csv"
      )

      Utils.writeToCsv(
        getResponseTimesDiffs,
        "evaluation/data/basic_get_performance_test.csv"
      )
    }

    "basic all atomic ops (PUT)" in new StoreSystem {
      // For each actor, create a thread sending writes and reads with equal frequency
      val probes = (0 until N_ACTORS).map { i =>
        val probe = createTestProbe[Command]()
        probe
      }
      // Get current time
      val start = System.currentTimeMillis()

      println("Building requesters")
      val requesters = (0 until N_ACTORS).map { i =>
        Future {
          val requestTimes = mutable.Map[String, Long]()

          val actorRef = actors(i)
          var j = 0
          while (TESTING) {
            val opId = s"$i - $j"

            val requestTime = System.currentTimeMillis()
            val p = Put("p" + opId, Utils.randomString(), j, probes(i).ref)
            actorRef ! Atomic(opId, Array(p), probes(i).ref)
            requestTimes.put(opId, requestTime)
            j += 1
            Thread.sleep(0, REQUEST_WAIT_NS)
          }
          requestTimes
        }
      }

      println("Building readers")
      // For each actor, create a thread reading from the thread probe
      val readers = (0 until 4).map { i =>
        Future {
          val responseTimes = mutable.Map[String, Long]()
          while (TESTING) {
            probes(i).receiveMessage(TEST_TIME.millis) match {
              case AtomicResponse(opId, _) =>
                responseTimes.put(opId, System.currentTimeMillis())
              case m => println(s"Reader: Unexpected message $m")
            }
          }
          responseTimes
        }
      }

      // Stop futures after TEST_TIME
      println("Letting futures run")
      Thread.sleep(TEST_TIME)
      // soft shutdown
      TESTING = false
      println("Stopping")

      // Collect results
      readers.foreach(_.recover({ case e => println(s"Reader: Error $e") }))
      val seq = Future.sequence(readers)
      seq.recover({ case e => println(s"Reader sequence: Error $e") })
      val responseTimesMap: Seq[mutable.Map[String, Long]] =
        Await.result(
          seq,
          scala.concurrent.duration.Duration.Inf
        )
      println("Responses collected")
      val requestTimesMap: Seq[mutable.Map[String, Long]] =
        Await.result(
          Future.sequence(requesters),
          scala.concurrent.duration.Duration.Inf
        )
      println("Requests collected")

      val end = System.currentTimeMillis()
      // Fetch store state with new probe
      val probe = createTestProbe[Command]()
      actors(0) ! GetState(probe.ref)
      val state = probe.receiveMessage(10.seconds)
      println(s"State size at end: ${state.asInstanceOf[State].state.size}")

      // Merge request and response times from every future
      val requestTimes = requestTimesMap.reduce(_ ++ _)
      val responseTimes = responseTimesMap.reduce(_ ++ _)

      // Log total time and response count
      val requests = requestTimes.size
      val responses = responseTimes.size
      println(s"Total time: ${end - start} ms")
      println(s"Total requests: $requests")
      println(s"Total responses: $responses")
      println(s"Total unmatched requests: ${requests - responses}")
      // Calculate average ops per second
      val ops = responses / ((end - start) / 1000)
      println(s"Average ops per second: $ops")

      // Write total responses and time to file
      Utils.writeToCsv(
        Seq(
          ("Total requests", "Total responses", "Total time (ms)"),
          (requests.toString, responses.toString, (end - start).toString)
        ),
        "evaluation/data/atomic_performance_test.totals.csv"
      )

      // Calculate average response time for puts
      val responseTimesDiffs: Seq[(String, String, Long)] =
        Utils.calculateResponseTimesDiffs(responseTimes, requestTimes)
      Utils.printStats("Atomic", responseTimesDiffs.map(_._3))

      // Prepare data for put requests
      Utils.writeToCsv(
        responseTimesDiffs,
        "evaluation/data/atomic_performance_test.csv"
      )
    }

    "mixed ops" in new StoreSystem {
      val PUT_PROB = 0.5
      val ATOMIC_PROB = 0
      val BATCH_SIZE = 1
      // Utils.setLoggerLevel("INFO")

      // For each actor, create a thread sending writes and reads with equal frequency
      val probes = (0 until N_ACTORS).map { i =>
        val probe = createTestProbe[Command]()
        probe
      }

      val it = Iterator.continually(math.random)

      // Get current time
      val start = System.currentTimeMillis()
      println("Building requesters")
      var atomic = 0
      var puts = 0
      var gets = 0
      val requesters = (0 until N_ACTORS).map { i =>
        Future {
          val requestTimes = mutable.Map[String, Long]()

          val actorRef = actors(i)
          var j = 0
          while (TESTING) {
            val opId = s"$i - $j"

            val requestTime = System.currentTimeMillis()
            val op = if (it.next() <= ATOMIC_PROB) {
              // Generate BATCH_SIZE amount of inner ops
              val innerOps = (0 until BATCH_SIZE).map { k =>
                val kOpId = s"$i - $j - $k"
                if (it.next() <= PUT_PROB) {
                  puts += 1
                  requestTimes.put(kOpId, requestTime)
                  Put(kOpId, Utils.randomString(), j, probes(i).ref)
                } else {
                  gets += 1
                  requestTimes.put(kOpId, requestTime)
                  Get(kOpId, Utils.randomString(), probes(i).ref)
                }
              }
              atomic += 1
              Atomic(
                opId,
                innerOps,
                probes(i).ref
              )
            } else {
              requestTimes.put(opId, requestTime)
              if (it.next() <= PUT_PROB) {
                puts += 1
                Put(opId, Utils.randomString(), j, probes(i).ref)
              } else {
                gets += 1
                Get(opId, Utils.randomString(), probes(i).ref)
              }
            }

            actorRef ! op
            j += 1
            val randSleep =
              Random.between(
                REQUEST_WAIT_NS - REQUEST_WAIT_NS_VARIANCE,
                REQUEST_WAIT_NS + REQUEST_WAIT_NS_VARIANCE
              )
            Thread.sleep(
              0,
              randSleep * BATCH_SIZE
            ) // Assumption: Total amount of ops requests per second is the same
          }
          requestTimes
        }
      }

      println("Building readers")
      // For each actor, create a thread reading from the thread probe
      val readers = (0 until N_ACTORS).map { i =>
        Future {
          val responseTimes = mutable.Map[String, Long]()
          while (TESTING) {
            probes(i).receiveMessage() match {
              case AtomicResponse(_, innerOps) =>
                for ((_, op) <- innerOps) {
                  op match {
                    case PutResponse(opId, _) =>
                      responseTimes.put(opId, System.currentTimeMillis())
                    case GetResponse(opId, _, _) =>
                      responseTimes.put(opId, System.currentTimeMillis())
                    case m => println(s"Reader: Unexpected message $m")
                  }
                }
              case PutResponse(opId, _) =>
                responseTimes.put(opId, System.currentTimeMillis())
              case GetResponse(opId, _, _) =>
                responseTimes.put(opId, System.currentTimeMillis())
              case m => println(s"Reader: Unexpected message $m")
            }
          }
          responseTimes
        }.recover({ case e: Exception =>
          println(s"Reader: Error $e")
          mutable.Map.empty[String, Long]
        })
      }

      // Stop futures after TEST_TIME
      println("Letting futures run")
      Thread.sleep(TEST_TIME)
      // soft shutdown
      TESTING = false
      println("Stopping")

      // Collect results
      val responseTimesMap: Seq[mutable.Map[String, Long]] =
        Await.result(
          Future.sequence(readers),
          scala.concurrent.duration.Duration.Inf
        )
      println("Responses collected")
      val requestTimesMap: Seq[mutable.Map[String, Long]] =
        Await.result(
          Future.sequence(requesters),
          scala.concurrent.duration.Duration.Inf
        )
      println("Requests collected")

      val end = System.currentTimeMillis()
      // Fetch store state with new probe
      val probe = createTestProbe[Command]()
      actors(0) ! GetState(probe.ref)
      val state = probe.receiveMessage(10.seconds)
      println(s"State size at end: ${state.asInstanceOf[State].state.size}")

      // Merge request and response times from every future
      val requestTimes = requestTimesMap.reduce(_ ++ _)
      val responseTimes = responseTimesMap.reduce(_ ++ _)

      // Log actual operation load
      println(s"Atomic: $atomic")
      println(s"Puts: $puts")
      println(s"Gets: $gets")

      // Log total time and response count
      val requests = requestTimes.size
      val responses = responseTimes.size

      println(s"Total time: ${end - start} ms")
      println(s"Total requests: $requests")
      println(s"Total responses: $responses")
      println(s"Total unmatched requests: ${requests - responses}")
      // Calculate average ops per second
      val ops = responses / ((end - start) / 1000)
      println(s"Average ops per second: $ops")

      // Write total responses and time to file
      Utils.writeToCsv(
        Seq(
          ("Total requests", "Total responses", "Total time (ms)"),
          (requests.toString, responses.toString, (end - start).toString)
        ),
        s"evaluation/data/a$ATOMIC_PROB-p$PUT_PROB-b${BATCH_SIZE}_atomic_performance_test.totals.csv"
      )

      // Calculate average response time for puts
      val responseTimesDiffs: Seq[(String, String, Long)] =
        Utils.calculateResponseTimesDiffs(responseTimes, requestTimes)
      Utils.printStats(s"$ATOMIC_PROB Atomic", responseTimesDiffs.map(_._3))

      // Prepare data for put requests
      Utils.writeToCsv(
        responseTimesDiffs,
        s"evaluation/data/a$ATOMIC_PROB-p$PUT_PROB-b${BATCH_SIZE}_atomic_performance_test.csv"
      )
    }

  }
}
