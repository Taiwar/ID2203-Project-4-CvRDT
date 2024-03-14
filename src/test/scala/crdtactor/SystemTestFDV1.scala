package crdtactor

import crdtactor.ActorFailureDetectorV1.*
import org.apache.pekko.actor.PoisonPill
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

class SystemTestFDV1 extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  import CRDTActorV2.*

  trait StoreSystem {
    val N_ACTORS = 3

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

  // TODO: Write a test for the failure detector
  "The system" must {

    "have working heartbeat receiving and acknowledging" in new StoreSystem {
//      // Create a probe for the failure detector and the actor
//      val probeCRDT = createTestProbe[crdtactor.CRDTActorV2.Command]()
//      val probeFailureDetector = createTestProbe[ActorFailureDetector.Command]()
//
//      // Spawn the failure detector and give it a name
//      //      val failureDetector = spawn(Behaviors.setup[ActorFailureDetector.Command] { ctx =>
//      //        Behaviors.withTimers(timers => new ActorFailureDetector(0, ctx, timers))
//      //      }, "failureDetector")
//
//      // Start the failure detector
//      probeFailureDetector ! ActorFailureDetector.Start(probeCRDT.ref)
//
//      // wait for the next heartbeat
//      probeCRDT.expectNoMessage(500.millis)
//
//      // Send heartbeat to the actor
//      probeCRDT.ref ! CRDTActorV2.Heartbeat(probeFailureDetector.ref, false)
//
//      // Loop through 100 heartbeats
//      for (_ <- 1 to 100) {
//        // Actor should receive the heartbeat from the failure detector
//        probeCRDT.expectMessage(CRDTActorV2.Heartbeat(probeFailureDetector.ref, false))
//
//        // Send the heartbeat ack to the failure detector
//        probeFailureDetector.ref ! ActorFailureDetector.HeartbeatAck(actorRef)
//
//        // Expect a heartbeat from the failure detector
//        probeFailureDetector.expectMessage(ActorFailureDetector.HeartbeatAck(actorRef))
//
//        // wait for the next heartbeat
//        probeCRDT.expectNoMessage(500.millis)
//
//        // Send heartbeat to the actor
//        probeCRDT.ref ! CRDTActorV2.Heartbeat(probeFailureDetector.ref, false)
      }
    }

    "have correct failed node detection in moment of timeout" in new StoreSystem {
//      // Create a probe for the failure detector and the actor
//      val probeCRDT = createTestProbe[crdtactor.CRDTActorV2.Command]()
//      val probeFailureDetector = createTestProbe[ActorFailureDetector.Command]()
//
//      // Spawn the failure detector and give it a name
//      val failureDetector = spawn(Behaviors.setup[ActorFailureDetector.Command] { ctx =>
//        Behaviors.withTimers(timers => new ActorFailureDetector(0, ctx, timers))
//      }, "failureDetector")
//
//      // Start the failure detector
//      failureDetector ! ActorFailureDetector.Start(probeCRDT.ref)
//
//
//
//      // wait for the next heartbeat
//      probeCRDT.expectNoMessage(500.millis)
//
//      // Loop through 100 heartbeats
////      for (_ <- 1 to 10) {
//        // Actor should receive the heartbeat from the failure detector
//        probeCRDT.expectMessage(CRDTActorV2.Heartbeat(failureDetector, false))
//
//        println ("Received heartbeat")
//
//        // Send the heartbeat ack to the failure detector
//        probeFailureDetector.ref ! ActorFailureDetector.HeartbeatAck(actorRef)
//
//        println ("Sent heartbeat ack")
//
//        // Expect a heartbeat from the failure detector
//        probeFailureDetector.expectMessage(ActorFailureDetector.HeartbeatAck(actorRef))
//
//        println ("Received heartbeat ack")
//
//        // wait for the next heartbeat
//        probeCRDT.expectNoMessage(500.millis)
//
//        println ("Waiting for next heartbeat")
//
//        // Send heartbeat to the actor
////        probeCRDT.ref ! CRDTActorV2.Heartbeat(failureDetector, false)
//
//        println ("Sent heartbeat")
//
//        // First expect a heartbeat from the crdt actor (The one we send the previous line)
//        probeCRDT.expectMessage(CRDTActorV2.Heartbeat(failureDetector, false))
//
//        // Wait for the timeout
////        probeCRDT.expectNoMessage(550.millis)
//
//        // Actor should receive the timeout message
////        probeCRDT.expectMessage(CRDTActorV2.MortalityNotice(probeCRDT.ref))
//
//        println ("Received timeout")

      // Failure detector should detect the failure

//
//      // Actor should receive the timeout message
//      probeCRDT.expectMessage(CRDTActorV2.MortalityNotice(probeCRDT.ref))

      // Send the heartbeat ack to the failure detector
//      probeFailureDetector.ref ! ActorFailureDetector.HeartbeatAck(actorRef)
//
//      // Expect a heartbeat from the failure detector
//      probeFailureDetector.expectMessage(ActorFailureDetector.HeartbeatAck(actorRef))
//
//      // wait for the next heartbeat
//      probeCRDT.expectNoMessage(500.millis)
//
//      // Send heartbeat to the actor
//      probeCRDT.ref ! CRDTActorV2.Heartbeat(failureDetector, false)

      // Wait for the timeout
//      probeCRDT.expectMessage(CRDTActorV2.Timeout)
    }

    "have working heartbeat system" in new StoreSystem {

      Thread.sleep(3000)

      // Kill the first actor
      actors(0) ! Die



      Thread.sleep(10000)

    }
}
