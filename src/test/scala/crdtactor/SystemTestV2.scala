package crdtactor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike
import concurrent.duration.DurationInt

class SystemTestV2 extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  import CRDTActorV2.*

  trait StoreSystem {
    val N_ACTORS = 8

    Utils.setLoggerLevel("INFO")

    // Create the actors
    val actors = (0 until N_ACTORS).map { i =>
      // Spawn the actor and get its reference (address)
      val actorRef = spawn(Behaviors.setup[CRDTActorV2.Command] { ctx =>
        Behaviors.withTimers(timers => new CRDTActorV2(i, ctx, timers))
      })
      i -> actorRef
    }.toMap

    // Write actor addresses into the global state
    actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))

    // Start the actors
    actors.foreach((_, actorRef) => actorRef ! CRDTActorV2.Start())
  }

  "The system" must {

    "have eventually consistent state after CRDT actions (basic)" in new StoreSystem {
      val probe = createTestProbe[Command]()

      // Create randomized key value tuples
      val keyValues =
        (0 until N_ACTORS).map(_ => (Utils.randomString(), Utils.randomInt()))

      // Send random put messages to all actors
      actors.foreach((i, actorRef) =>
        actorRef ! Put(keyValues(i)._1, keyValues(i)._2, probe.ref)
      )
      var responses = (0 until N_ACTORS).map(_ => probe.receiveMessage())
      responses.foreach {
        case putMsg: PutResponse =>
          putMsg should not be null
        case msg =>
          fail("Unexpected message: " + msg)
      }

      // Wait for sync messages (assuming partially synchronous system)
      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE)

      // Send a get message for each key to each actor and verify the responses
      keyValues.foreach { case (key, value) =>
        actors.foreach((_, actorRef) => actorRef ! Get(key, probe.ref))
        responses = (0 until N_ACTORS).map(_ => probe.receiveMessage())
        responses.foreach {
          case getMsg: GetResponse =>
            getMsg.value shouldEqual value
          case msg =>
            fail("Unexpected message: " + msg)
        }
      }

    }

    "not guarantee sequentially consistent state after non-atomic actions (sanity check)" in new StoreSystem {
      val probe = createTestProbe[Command]()

      // Simulate a bank transfer

      // Set up
      actors(0) ! Put("a", 100, probe.ref)
      actors(0) ! Put("b", 200, probe.ref)
      probe.receiveMessage()
      probe.receiveMessage()
      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE)

      // Actor 0 reads value of b
      actors(0) ! Get("a", probe.ref)
      val a = probe.receiveMessage().asInstanceOf[GetResponse].value
      actors(0) ! Get("b", probe.ref)
      val b = probe.receiveMessage().asInstanceOf[GetResponse].value
      // Actor 0 deducts 50 from b
      actors(0) ! Put("b", b - 50, probe.ref)
      // Actor 0 adds 50 to a
      actors(0) ! Put("a", a + 50, probe.ref)
      probe.receiveMessage()
      probe.receiveMessage()
      Thread.sleep(
        0
      ) // If we set this to a high enough number, the test would fail because we would get the correct state

      // Actor 1 reads values of a and b
      actors(1) ! Get("a", probe.ref)
      val a1 = probe.receiveMessage().asInstanceOf[GetResponse].value
      actors(1) ! Get("b", probe.ref)
      val b1 = probe.receiveMessage().asInstanceOf[GetResponse].value

      // Log the values
      println(
        s"Intended from actor 0: a: ${a + 50} b: ${b - 50}; Received at actor 1: a1: $a1, b1: $b1"
      )

      // a1 should be 150 and b1 should be 50 if the state was sequentially consistent
      a1 should not be 150
      b1 should not be 150
    }

    "have sequentially consistent state after atomic actions" in new StoreSystem {
      val probe = createTestProbe[Command]()

      // Simulate a bank transfer

      // Set up
      actors(0) ! Put("a", 100, probe.ref)
      actors(0) ! Put("b", 200, probe.ref)
      probe.receiveMessage()
      probe.receiveMessage()
      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE)

      // Actor 0 reads value of a and b
      actors(0) ! Atomic(
        Array(Get("a", probe.ref), Get("b", probe.ref)),
        probe.ref
      )
      val results =
        probe.receiveMessage().asInstanceOf[AtomicResponse].responses
      // Get tuple where first element is the key and second is the value
      val a = results.find(_._1 == "a").get._2.asInstanceOf[GetResponse].value
      val b = results.find(_._1 == "b").get._2.asInstanceOf[GetResponse].value
      // Actor 0 deducts 50 from b and adds 50 to a
      actors(0) ! Atomic(
        Array(Put("b", b - 50, probe.ref), Put("a", a + 50, probe.ref)),
        probe.ref
      )
      probe.receiveMessage() match {
        case AtomicResponse(responses) =>
        // Do nothing
        case msg =>
          fail("Unexpected message: " + msg)
      }

      // Actor 1 reads values of a and b
      actors(1) ! Get("a", probe.ref)
      val a1 = probe.receiveMessage().asInstanceOf[GetResponse].value
      actors(1) ! Get("b", probe.ref)
      val b1 = probe.receiveMessage().asInstanceOf[GetResponse].value

      // Log the values
      println(
        s"Intended from actor 0: a: ${a + 50} b: ${b - 50}; Received at actor 1: a1: $a1, b1: $b1"
      )

      // a1 and b2 should be 150
      a1 shouldEqual 150
      b1 shouldEqual 150
    }

    "have working heartbeat handling" in new StoreSystem {
      // Create a probe for the failure detector and the actor
      val probeCRDT = createTestProbe[crdtactor.CRDTActorV2.Command]()
      val probeFailureDetector = createTestProbe[ActorFailureDetectorV1.Command]()

      // Spawn the failure detector and give it a name
      val failureDetector = spawn(Behaviors.setup[ActorFailureDetectorV1.Command] { ctx =>
        Behaviors.withTimers(timers => new ActorFailureDetectorV1(0, ctx, timers))
      }, "failureDetector")

      Thread.sleep(1000)

//      println("Sending the heartbeat")
//
//      // Send a Heartbeat message to the actor
//      actors(0) ! CRDTActorV2.Heartbeat(failureDetector)

//      println("Expecting the heartbeat ack")

//      println("Sleeping for 5 seconds")

      // Expect a HeartbeatAck message from the actor to the failure detector
//      probeFailureDetector.expectMessage(ActorFailureDetectorV1.HeartbeatAck(actors(0)))
//
//      // Expect a HeartbeatAck message from the actor to the failure detector
//      probeCRDT.expectMessage(CRDTActorV2.Heartbeat(failureDetector))

//      Thread.sleep(10000)
//      // Send a Heartbeat message to the actor
//      actors(0) ! Heartbeat(ActorFailureDetectorV1.Heartbeat(probe.ref))
//
//      // Expect a HeartbeatAck message from the actor
//      probe.expectMessage(ActorFailureDetectorV1.HeartbeatAck(actorRef))
//
//      // Expect no message for the duration of the timeout interval
//      probe.expectNoMessage(ActorFailureDetectorV1.delta)
//
//      // Send a Timeout message to the actor
//      actorRef ! ActorFailureDetectorV1.Timeout
//
//      // Expect a MortalityNoticeWrapper message from the actor
//      probe.expectMessageType[ActorFailureDetectorV1.MortalityNoticeWrapper]

      // Receive one message
//      probe.receiveMessage()

//      // Send a Timeout message to the actor
//      actors(0) ! ActorFailureDetectorV1.Timeout
//
//      // Expect a MortalityNoticeWrapper message from the actor
//      probe.expectMessageType[ActorFailureDetectorV1.MortalityNoticeWrapper]
    }

  }
}
