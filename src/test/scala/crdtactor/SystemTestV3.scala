package crdtactor

import org.apache.pekko.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

import scala.util.Random

class SystemTestV3 extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  import crdtactor.CRDTActorV3.*

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

    // Set leader (BLE mock)
    actors.foreach((_, actorRef) => actorRef ! CRDTActorV3.Leader(actors(0)))

    Thread.sleep(50) // Wait for actors to be ready

    val probe: TestProbe[Command] = createTestProbe[Command]()
  }

  "The system" must {

    "have eventually consistent state after CRDT actions (basic)" in new StoreSystem {
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
      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE + Utils.CRDT_SYNC_PERIOD)

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
      actors(0) ! Put("b", b.get - 50, probe.ref)
      // Actor 0 adds 50 to a
      actors(0) ! Put("a", a.get + 50, probe.ref)
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
        s"Intended from actor 0: a: ${a.get + 50} b: ${b.get - 50}; Received at actor 1: a1: $a1, b1: $b1"
      )

      // a1 should be 150 and b1 should be 50 if the state was sequentially consistent
      a1 should not be 150
      b1 should not be 150
    }

    "have sequentially consistent state after atomic actions" in new StoreSystem {
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
        Array(Put("b", b.get - 50, probe.ref), Put("a", a.get + 50, probe.ref)),
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
        s"Intended from actor 0: a: ${a.get + 50} b: ${b.get - 50}; Received at actor 1: a1: $a1, b1: $b1"
      )

      // Either a1 and b1 are both 150 or both are not 150
      (a1.get == 150 && b1.get == 150) || (a1.get != 150 && b1.get != 150) shouldEqual true

      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE)
      // Eventually, actor 2 reads new values of a and b
      actors(2) ! Get("a", probe.ref)
      val a2 = probe.receiveMessage().asInstanceOf[GetResponse].value
      actors(2) ! Get("b", probe.ref)
      val b2 = probe.receiveMessage().asInstanceOf[GetResponse].value

      // Log the values
      println(
        s"Intended from actor 0: a: ${a.get + 50} b: ${b.get - 50}; Received at actor 2: a2: $a2, b2: $b2"
      )

      // Either a1 and b1 are both 150 or both are not 150
      (a2.get == 150 && b2.get == 150) shouldEqual true
    }

    "have sequentially consistent state after concurrent atomic actions" in new StoreSystem {
      // Setup key a to be 0
      actors(0) ! Put("a", 0, probe.ref)
      // Wait for sync
      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE)

      // Send put messages to random actors for 2 seconds on a separate thread
      val putThread = new Thread(() => {
        val start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < 2000) {
          val actorRef = actors(Random.between(0, N_ACTORS))
          // Send two atomic puts
          actorRef ! Atomic(
            Array(Put("a", 1, probe.ref), Put("a", 2, probe.ref)),
            probe.ref
          )
          Thread.sleep(25)
        }
      })
      // Start put thread
      putThread.start()

      val start = System.currentTimeMillis()
      while (System.currentTimeMillis() - start < 2000) {
        val actorRef = actors(Random.between(0, N_ACTORS))
        actorRef ! Get("a", probe.ref)
        // Check if response is GetResponse and get value
        probe.receiveMessage() match {
          case GetResponse(key, value) =>
            // value should be 2 or 0
            key shouldEqual "a"
            // Log value
            println(s"Read value: $value")
            value should (equal(2) or equal(0))
          case msg => ()
        }
        Thread.sleep(100)
      }

      // Create new probe with empty inbox
      val probe2: TestProbe[Command] = createTestProbe[Command]()

      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE)
      // Eventually, and actor reads new value of a
      actors(0) ! Get("a", probe2.ref)
      val a = probe2.receiveMessage().asInstanceOf[GetResponse].value
      println(s"Final value of a: $a")

      // a should be 2
      a shouldEqual 2
    }

    "should execute updates inconsistently if not executed in atomic batch" in new StoreSystem {
      // Setup key a to be max
      val max = 5
      actors(0) ! Put("a", max, probe.ref)
      actors(0) ! Put("b", max, probe.ref)
      // Wait for sync
      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE)

      val decrementA =
        UpdateWithContext(
          "a",
          "b",
          (a, b) => if (a.get > 0 || b.get > 0) a.get - 1 else a.get,
          probe.ref
        )
      val decrementB =
        UpdateWithContext(
          "b",
          "a",
          (b, a) => if (a.get > 0 || b.get > 0) b.get - 1 else b.get,
          probe.ref
        )

      // Randomly send increments and decrements to actors
      val updateThread = new Thread(() => {
        val start = System.currentTimeMillis()
        while (
          System
            .currentTimeMillis() - start < Utils.CRDT_SYNC_PERIOD * max * 1.2
        ) {
          val actorRef = actors(Random.between(0, N_ACTORS))
          // Simulate decrements arriving at different times
          actorRef ! decrementA
          Thread.sleep(Utils.CRDT_SYNC_PERIOD)
          actorRef ! decrementB
          Thread.sleep(5)
        }
      })
      // Start update thread
      updateThread.start()

      // When reading, a should always be between 0 and 10
      val start = System.currentTimeMillis()
      val receiveProbe = createTestProbe[Command]()
      var inconsistent = false
      while (
        System.currentTimeMillis() - start < Utils.CRDT_SYNC_PERIOD * max * 1.2
      ) {
        val actorRef = actors(Random.between(0, N_ACTORS))
        actorRef ! Get("a", receiveProbe.ref)
        // Check if response is GetResponse and get value
        receiveProbe.receiveMessage() match {
          case GetResponse(key, a) =>
            actorRef ! Get("b", receiveProbe.ref)
            val b =
              receiveProbe.receiveMessage().asInstanceOf[GetResponse].value

            println(s"Read values: a: ${a.get}, b: ${b.get}")
            // set inconsistent to true if a or be are lower than 0
            if (a.get < 0 || b.get < 0) inconsistent = true
          case msg => ()
        }
        Thread.sleep(25)
      }
      // a and b should be inconsistent at some point
      inconsistent shouldEqual true
    }

    "should execute updates consistently if executed in atomic batch" in new StoreSystem {
      // Setup key a to be max
      val max = 5
      actors(0) ! Put("a", max, probe.ref)
      actors(0) ! Put("b", max, probe.ref)
      // Wait for sync
      Thread.sleep(Utils.RANDOM_BC_DELAY_SAFE)

      val decrementA =
        UpdateWithContext(
          "a",
          "b",
          (a, b) => if (a.get > 0 || b.get > 0) a.get - 1 else a.get,
          probe.ref
        )
      val decrementB =
        UpdateWithContext(
          "b",
          "a",
          (b, a) => if (a.get > 0 || b.get > 0) b.get - 1 else b.get,
          probe.ref
        )

      // Randomly send increments and decrements to actors
      val updateThread = new Thread(() => {
        val start = System.currentTimeMillis()
        while (
          System
            .currentTimeMillis() - start < Utils.CRDT_SYNC_PERIOD * max * 1.2
        ) {
          val actorRef = actors(Random.between(0, N_ACTORS))
          // Send both decrements in atomic
          actorRef ! Atomic(
            Array(decrementA, decrementB),
            probe.ref
          )
          Thread.sleep(25)
        }
      })
      // Start update thread
      updateThread.start()

      // When reading, a should always be between 0 and 10
      val start = System.currentTimeMillis()
      val receiveProbe = createTestProbe[Command]()
      while (
        System.currentTimeMillis() - start < Utils.CRDT_SYNC_PERIOD * max * 1.2
      ) {
        val actorRef = actors(Random.between(0, N_ACTORS))
        actorRef ! Get("a", receiveProbe.ref)
        // Check if response is GetResponse and get value
        receiveProbe.receiveMessage() match {
          case GetResponse(key, a) =>
            actorRef ! Get("b", receiveProbe.ref)
            val b =
              receiveProbe.receiveMessage().asInstanceOf[GetResponse].value

            println(s"Read values: a: ${a.get}, b: ${b.get}")
            // b and a should always be greater than 0
            a.get should (be >= 0 and be <= max)
            b.get should (be >= 0 and be <= max)
          case msg => ()
        }
        Thread.sleep(25)
      }
    }

  }
}
