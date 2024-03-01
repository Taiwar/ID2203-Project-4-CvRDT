package crdtactor

import crdtactor.ActorFailureDetector.*
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

class SystemTestFDV1 extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  import CRDTActorV2.*

  trait StoreSystem {
    val actorRef = spawn(Behaviors.setup[CRDTActorV2.Command] { ctx =>
      Behaviors.withTimers(timers => new CRDTActorV2(1000, ctx, timers))
    })

    // Write actor addresses into the global state
    Utils.GLOBAL_STATE.put(1000, actorRef)

    // Start the actors
    actorRef ! CRDTActorV2.Start
  }

  "The system" must {

    "have working heartbeat receiving and acknowledging" in new StoreSystem {
      // Create a probe for the failure detector and the actor
      val probeCRDT = createTestProbe[crdtactor.CRDTActorV2.Command]()
      val probeFailureDetector = createTestProbe[ActorFailureDetector.Command]()

      // Spawn the failure detector and give it a name
      val failureDetector = spawn(Behaviors.setup[ActorFailureDetector.Command] { ctx =>
        Behaviors.withTimers(timers => new ActorFailureDetector(0, ctx, timers))
      }, "failureDetector")

      // Start the failure detector
      failureDetector ! ActorFailureDetector.Start(probeCRDT.ref)

      // wait for the next heartbeat
      probeCRDT.expectNoMessage(500.millis)

      // Loop through 100 heartbeats
      for (_ <- 1 to 10) {
        // Actor should receive the heartbeat from the failure detector
        probeCRDT.expectMessage(CRDTActorV2.Heartbeat(failureDetector))

        // Send the heartbeat ack to the failure detector
        probeFailureDetector.ref ! ActorFailureDetector.HeartbeatAck(actorRef)

        // Expect a heartbeat from the failure detector
        probeFailureDetector.expectMessage(ActorFailureDetector.HeartbeatAck(actorRef))

        // wait for the next heartbeat
        probeCRDT.expectNoMessage(500.millis)

        // Send heartbeat to the actor
        probeCRDT.ref ! CRDTActorV2.Heartbeat(failureDetector)
      }
    }

  }
}
