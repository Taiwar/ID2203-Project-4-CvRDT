package crdtactor

import org.apache.pekko.actor.typed.scaladsl.adapter.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object Bootstrap {

  // Startup the actors and execute the workload
  def apply(): Unit =
    val N_ACTORS = 8

    Utils.setLoggerLevel("INFO")

    val system = ActorSystem("CRDTActor")

    // Create the actors
    val actors = (0 until N_ACTORS).map { i =>
      // Create the actor and give it a name
      val name = s"CRDTActor-$i"

      // Spawn the actor and get its reference (address)
      val actorRef = system.spawn(
        Behaviors.setup[CRDTActor.Command] { ctx => new CRDTActor(i, ctx) },
        name
      )
      i -> actorRef
    }.toMap

    // Write actor addresses into the global state
    actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))

    // Start the actors
    actors.foreach((_, actorRef) => actorRef ! CRDTActor.Start)

    // Sleep for a few seconds, then quit :)
    Thread.sleep(5000)

    // Force quit
    System.exit(0)
}
