package crdtactor

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.scalatest.wordspec.AnyWordSpecLike


class CRDTActorTest extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  import CRDTActor.*

  // Example test to get testing working
  "A CRDT actor" must {

    "send back state when sent ReadState" in {
      val probe = createTestProbe[Command]()
      val crdtActor = spawn(Behaviors.setup[CRDTActor.Command] { ctx => new CRDTActor(1, ctx) })

      crdtActor ! ReadState(probe.ref)
      val response = probe.receiveMessage()

      response match {
        case stateMsg: StateMsg =>
          stateMsg.state should not be null
        case _ =>
          fail("Unexpected message")
      }

    }

  }
}