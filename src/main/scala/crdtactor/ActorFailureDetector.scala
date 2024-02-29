package crdtactor

import ActorFailureDetector.Command
import crdtactor.CRDTActorV2.MortalityNotice
import org.apache.pekko.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.{LWWMap, ReplicatedDelta}

import scala.concurrent.duration.DurationInt

object ActorFailureDetector {
  // The type of messages that the actor can handle
  sealed trait Command

  // Triggers the actor to start the computation (do this only once!)
  case class Start(from: ActorRef[CRDTActorV2.Command]) extends Command

  case class StateMsg(state: ddata.LWWMap[String, Int]) extends Command

  // Mortality Notice
  case class MortalityNotice(from: ActorRef[CRDTActorV2.Command]) extends Command

  // New message type to hold a MortalityNotice from CRDTActorV2
  case class MortalityNoticeWrapper(mortalityNotice: CRDTActorV2.MortalityNotice) extends Command

  // Key-Value Ops

  // Timer
  private case object Timeout extends Command
  private case object TimerKey

  // Heartbeat
  case class Heartbeat() extends Command

  case class HeartbeatAck(from: ActorRef[CRDTActorV2.Command]) extends Command

  // Time

  // Time between heartbeats interval (Gamma γ)
  val gamma = 50.millis

  // Timeout interval (Delta δ)
  val delta = 100.millis

  // Total wait time (time T)
  val T = gamma + delta

  // The sender of the start message (actor reference)
  var messageSender: Option[ActorRef[CRDTActorV2.Command]] = None
}

import crdtactor.ActorFailureDetector.*

// The actor implementation of the (perfect) failure detector
// Note: the current implementation assumes perfect failure detection and does not
// handle network partitions or other failure scenarios
class ActorFailureDetector(
                   // id is the unique identifier of the actor, ctx is the actor context
                   id: Int,
                   ctx: ActorContext[Command],
                   timers: TimerScheduler[Command]
                   //faiureDetector: ActorRef[ActorFailureDetector.Command]
                 ) extends AbstractBehavior[Command](ctx) {

  // The CRDT state of this actor, mutable var as LWWMap is immutable
  private var crdtstate = ddata.LWWMap.empty[String, Int]

  // The CRDT address of this actor/node, used for the CRDT state to identify the nodes
  private val selfNode = Utils.nodeFactory()

  // Hack to get the actor references of the other actors, check out `lazy val`
  // Careful: make sure you know what you are doing if you are editing this code
  private lazy val others =
    Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]()

  private val locks = scala.collection.mutable.Map[String, Int]()
  private val commandBuffer = scala.collection.mutable.Queue[Command]()

  private var dirty = false

  // Start the failure detector timer
  private def startFailureDetectorTimer(): Unit = {
    // Start the failure detector timer
    timers.startTimerWithFixedDelay(
      TimerKey,
      Heartbeat(),
      gamma
    )
  }

  // Create timer to handle timeout
  private def startTimeoutTimer(): Unit = {
    // TODO: Check if this is the best way to handle timeout
    timers.startSingleTimer(TimerKey, Timeout, T)
  }

  private def handleHeartbeatAck() : Unit = {
    // TODO: Check if timers.cancel & restart is needed
    timers.cancel(TimerKey)
    startFailureDetectorTimer() // Reset the timer
  }

  // Note: you probably want to modify this method to be more efficient
  private def broadcastAndResetDeltas(): Unit =
    // Broadcast the delta to all other actors and reset the delta
    // CRDT-Delta: https://pekko.apache.org/docs/pekko/current//typed/distributed-data.html#delta-crdt
    val deltaOption = crdtstate.delta

    // The deltaOption is an instance of Option,
    // a Scala container type that can either hold a value (Some)
    // or no value (None). This is used to represent the presence
    // or absence of a value, providing a safer alternative to null references.
    deltaOption match
      case None => ()
      case Some(delta) =>
        crdtstate = crdtstate.resetDelta // May be omitted
        others.foreach { //
          (name, actorRef) =>
            Thread.sleep(Utils.RANDOM_BC_DELAY)
//            actorRef !
              // Send the delta to the other actors
//              DeltaMsg(ctx.self, delta)
        }
        dirty = false

  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case Start(from) =>
      ctx.log.info(s"FailureDetector-$id started")

      // Store the sender of the start message
      messageSender = Some(from)

      // Start the failure detector timer
      startFailureDetectorTimer()
      Behaviors.same

    // Handle case if no ack is received
    case Timeout =>
      ctx.log.info(s"FailureDetector-$id: Timeout")
      // Inform others about the timeout (death) of agent
      // TODO: Replace detect with suspect and request ack from every other process
      // TODO: Check if wrapper is needed
      for (actor <- others.values) {
        actor ! MortalityNoticeWrapper(CRDTActorV2.MortalityNotice(messageSender.get))
      }
      Behaviors.same

    // Heartbeat handling
    case Heartbeat() =>
      ctx.log.info(s"FailureDetector-$id: Received heartbeat")
      // Send the heartbeat message to the sender
      messageSender.get ! CRDTActorV2.Heartbeat(ctx.self)
      // Start timeout timer
      startTimeoutTimer()
      Behaviors.same

    case HeartbeatAck(from) =>
      ctx.log.info(s"FailureDetector-$id: Received heartbeat ack")
      // Handle ack from the sender
      handleHeartbeatAck()
      Behaviors.same

  Behaviors.same
}
