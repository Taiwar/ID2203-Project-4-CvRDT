package crdtactor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

class SystemTestV3 extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  import CRDTActorV3.*

  trait StoreSystem {
    val N_ACTORS = 8

    Utils.setLoggerLevel("INFO")

    // Create the actors
    val actors = (0 until N_ACTORS).map { i =>
      // Spawn the actor and get its reference (address)
      val actorRef = spawn(Behaviors.setup[CRDTActorV3.Command] { ctx =>
        Behaviors.withTimers(timers => new CRDTActorV3(i, ctx, timers))
      })
      i -> actorRef
    }.toMap

    // Write actor addresses into the global state
    actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))
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

      // Either a1 and b1 are both 150 or both are not 150
      (a1 == 150 && b1 == 150) || (a1 != 150 && b1 != 150) shouldEqual true

      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE)
      // Eventually, actor 2 reads new values of a and b
      actors(2) ! Get("a", probe.ref)
      val a2 = probe.receiveMessage().asInstanceOf[GetResponse].value
      actors(2) ! Get("b", probe.ref)
      val b2 = probe.receiveMessage().asInstanceOf[GetResponse].value

      // Log the values
      println(
        s"Intended from actor 0: a: ${a + 50} b: ${b - 50}; Received at actor 2: a2: $a2, b2: $b2"
      )

      // Either a1 and b1 are both 150 or both are not 150
      (a2 == 150 && b2 == 150) shouldEqual true
    }

  }
}
