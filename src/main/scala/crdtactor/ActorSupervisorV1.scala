package crdtactor

import crdtactor.ActorSupervisorV1.Command
import crdtactor.CRDTActorV4.Leader
import org.apache.pekko.actor.typed.{ActorRef, Behavior, Signal, Terminated}
import org.apache.pekko.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}

object ActorSupervisorV1 {
  // Define the commands for the supervisor
  sealed trait Command
  case class createActor() extends Command
}

class ActorSupervisorV1(
                        id : Int,
                        cxt: ActorContext[ActorSupervisorV1.Command],
                        timers: TimerScheduler[ActorSupervisorV1.Command]
                       ) extends AbstractBehavior[ActorSupervisorV1.Command](cxt) {
  import ActorSupervisorV1._

  private var childActor: ActorRef[CRDTActorV4.Command] = _

  // Hack to get the actor references of the other actors, check out `lazy val`
  // Careful: make sure you know what you are doing if you are editing this code
  private lazy val everyone =
    Utils.GLOBAL_STATE.getAll[Int, ActorRef[CRDTActorV4.Command]]()

  // TODO: Add actual state transfer
  private def createActor(): ActorRef[CRDTActorV4.Command] = {
    cxt.log.info("Creating a new actor...")
    val actor = context.spawn(Behaviors.setup[CRDTActorV4.Command] { ctx =>
      Behaviors.withTimers(timers => new CRDTActorV4(id, ctx, timers))
    }, "childActor")
    startActor(actor)
    context.watch(actor)
    actor
  }

  private def startActor(actor: ActorRef[CRDTActorV4.Command]): Unit = {
    // Write actor addresses into the global state with id and actorRef
    Utils.GLOBAL_STATE.put(id, actor)

    // Set leader (BLE mock)
    // Get first actor reference
    val leader = everyone.values.head
    actor ! Leader(leader)

    // Start the failure detector
    actor ! CRDTActorV4.StartFailureDetector(true)
  }

  override def onMessage(msg: Command): Behavior[Command] = msg match {
    case createActor() =>
      childActor = createActor()
      Behaviors.same
  }

  override def onSignal: PartialFunction[Signal, Behavior[Command]] = {
    case Terminated(actorRef) if actorRef == childActor =>
      context.log.info("Child actor stopped, creating a new one...")
      childActor = createActor()
      Behaviors.same
  }
}