package crdtactor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

class SystemTest extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  import CRDTActor.*

  trait StoreSystem {
    val N_ACTORS = 8

    Utils.setLoggerLevel("INFO")

    // Create the actors
    val actors = (0 until N_ACTORS).map { i =>
      // Spawn the actor and get its reference (address)
      val actorRef = spawn(Behaviors.setup[CRDTActor.Command] { ctx =>
        new CRDTActor(i, ctx)
      })
      i -> actorRef
    }.toMap

    // Write actor addresses into the global state
    actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))

    // Start the actors
    actors.foreach((_, actorRef) => actorRef ! CRDTActor.Start)
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
      Thread.sleep(100)

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

    "have sequentially consistent state after atomic actions" in new StoreSystem {
      // TODO: Implement this test
      // Read up on sequential consistency and how to test it
      // May need to add artificial delays to the actors to simulate network latency
        val probe = createTestProbe[Command]()
        val r = scala.util.Random

        // Create a sequence of key value tuples
        val sequence = (0 until N_ACTORS).map(i => (i, Utils.randomString(), Utils.randomInt()))

        // Print the sequence
        println("Sequence: " + sequence)

        // Send put messages to all actors in sequence
        sequence.foreach { case (seq, key, value) =>
          actors.foreach((_, actorRef) =>
            // Add a random chance the message is not sent to the actor
            // if (r.nextInt(100) > 50) actorRef ! Put(key, value, probe.ref)
            actorRef ! Put(key, value, probe.ref)
          )

          // Wait for all actors to process the put before proceeding to the next
          var responses = (0 until N_ACTORS).map(_ => probe.receiveMessage())
          responses.foreach {
            case putMsg: PutResponse =>
              putMsg should not be null
            case msg =>
              fail("Unexpected message: " + msg)
          }
        }

        // Wait for sync messages (assuming partially synchronous system)
        Thread.sleep(100)

        // Send a get message for each key to each actor and verify the responses
        sequence.foreach { case (_, key, value) =>
          actors.foreach((_, actorRef) => actorRef ! Get(key, probe.ref))
          var responses = (0 until N_ACTORS).map(_ => probe.receiveMessage())
          responses.foreach {
            case getMsg: GetResponse =>
              //print the get message
              println("Get message: " + getMsg)
              getMsg.value shouldEqual value
            case msg =>
              fail("Unexpected message: " + msg)
          }
        }
      }
    }
}
