package crdtactor

import ActorFailureDetector.Command
import org.apache.pekko.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.{LWWMap, ReplicatedDelta}

import scala.concurrent.duration.DurationInt
import scala.util.Random

object ActorFailureDetector {
  // The type of messages that the actor can handle
  sealed trait Command

  // Triggers the actor to start the computation (do this only once!)
  case class Start(from: ActorRef[CRDTActorV2.Command]) extends Command

  case class StateMsg(state: ddata.LWWMap[String, Int]) extends Command

  // Mortality Notice
  case class MortalityNotice(from: ActorRef[CRDTActorV2.Command]) extends Command

  // GetIdResponse
  case class GetIdResponse(id: Int) extends Command

  // New message type to hold a MortalityNotice from CRDTActorV2
//  case class MortalityNoticeWrapper(mortalityNotice: CRDTActorV2.MortalityNotice) extends Command

  // Key-Value Ops

  // Timeout
  case class Timeout(actorId: Int) extends Command
  private case object TimerKey

  // Heartbeat
  case class Heartbeat() extends Command

  case class HeartbeatAck(from: ActorRef[CRDTActorV2.Command]) extends Command

  // Time

  // Time between heartbeats interval (Gamma γ)
  val gamma = 1000.millis

  // Timeout interval (Delta δ)
  val delta = 500.millis

  // Random time difference between nodes (Epsilon ε)
  val epsilon = 100.millis

  // TODO: Check if this is needed
  // Total wait time (time T)
  val T = gamma + delta

  // The sender of the start message (actor reference)
  var messageSender: Option[ActorRef[CRDTActorV2.Command]] = None

  // Add a flag to indicate whether the Timeout message should be processed
  private var processTimeout = true
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
                 ) extends AbstractBehavior[Command](ctx) {

  // The CRDT state of this actor, mutable var as LWWMap is immutable
  private var crdtstate = ddata.LWWMap.empty[String, Int]

  // The CRDT address of this actor/node, used for the CRDT state to identify the nodes
  private val selfNode = Utils.nodeFactory()

  // Hack to get the actor references of the other actors, check out `lazy val`
  // Careful: make sure you know what you are doing if you are editing this code
  private lazy val others = Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]().filter(_._1 != id)

  // A map with all the other actors and a boolean if they are alive
  private val aliveActors = scala.collection.mutable.Map[Int, Boolean]()

  // Start the failure detector timer (heartbeat)
  private def startFailureDetectorTimer(): Unit = {

    timers.startTimerWithFixedDelay(
      id, // Use the provided id or default to the actor's id
      ActorFailureDetector.Heartbeat(),
      gamma // Time between heartbeats interval (Gamma γ)
    )
  }

  // Start the failure detector timer (timeout)
  private def startTimeoutTimer(actorId: Int): Unit = {
    // Start the failure detector timer (heartbeat) with a random delay
    val variance = 10.millis + Random.nextInt((epsilon.toMillis.toInt - 10) + 1).millis
    val timoutTime = T + variance

    ctx.log.info(s"FailureDetector-$id: Starting timeout timer for actor $actorId with timeout $timoutTime")

    timers.startTimerWithFixedDelay(
      actorId, // Use the provided id or default to the actor's id
      ActorFailureDetector.Timeout(actorId),
      timoutTime // Time between heartbeats interval (Gamma γ)
    )
  }

  // Stop the timer
  private def stopTimer(id: Int): Unit = {
    timers.cancel(id)
  }

  private def handleHeartbeatAck() : Unit = {
    processTimeout = false
    // TODO: Check if timers.cancel & restart is needed
    timers.cancel(TimerKey)
    startTimeoutTimer(id) // Reset the timer
  }

  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case Start(from) =>
      ctx.log.info(s"FailureDetector-$id started")

      // Start the failure detector timer
      startFailureDetectorTimer()

      // Setup ids for all actors
      for ((actorId, actor) <- others) {
        // Add actor to the aliveActors map
        aliveActors += (actorId -> true)
      }
      Behaviors.same

    // TODO: Remove this if not needed
    case GetIdResponse(actorId) =>
      ctx.log.info(s"FailureDetector-$id: Received idResponse")
      Behaviors.same

    // Only handle timeout if the actor is still alive
    case Timeout(actorId: Int) if aliveActors(actorId) =>
      ctx.log.info(s"FailureDetector-$id: Timeout")
      // Set aliveActors to false
      aliveActors += (actorId -> false)

      // TODO: Check best way to handle this
      // Stop the timer
      stopTimer(actorId)

      // TODO: Notify the application that the actor is dead
      // Send the MortalityNotice to all other actors
      for (actor <- aliveActors.keys) {
          if (aliveActors(actor)) {
            // Get the actor from the 'others' map
            others.get(actor) match {
              case Some(actor: ActorRef[CRDTActorV2.Command]) =>
                // Send Heartbeat to the actor
                actor ! CRDTActorV2.MortalityNotice(actor)
              case _ =>
              // Skip the actor if it is not found or not of the correct type
            }
          }
      }
      Behaviors.same

    // Heartbeat handling
    case Heartbeat() =>
      ctx.log.info(s"FailureDetector-$id: Sending heartbeat")
      // Send the heartbeat to all other actors
      for (actorId <- aliveActors.keys if aliveActors(actorId)) {
        // Get the actor from the 'others' map
        others.get(actorId) match {
          case Some(actor: ActorRef[CRDTActorV2.Command]) =>
            // Send Heartbeat to the actor
            actor ! CRDTActorV2.Heartbeat(ctx.self, false)
          case _ =>
          // Skip the actor if it is not found or not of the correct type
        }
      }
      Behaviors.same

    // Handle HeartbeatAck from CRDTActorV2
    case HeartbeatAck(from) =>
      ctx.log.info(s"FailureDetector-$id: Received heartbeat ack")

      // Filter the actor from the others
      others.find(_._2 == from) match {
        case Some((actorId, actor)) =>
          // Update the aliveActors map if needed
          if (!aliveActors(actorId)) {
            // Set aliveActors to true
            aliveActors += (actorId -> true)
          }

          // Restart timemout timer
          startTimeoutTimer(actorId)
        case None =>
          ctx.log.info(s"FailureDetector-$id: Actor not found")
      }

      Behaviors.same

    case MortalityNotice(from) =>
      ctx.log.info(s"FailureDetector-$id: Received MortalityNotice")

      others.find(_._2 == from) match {
        case Some((actorId, actor)) =>
          // Handle MortalityNotice message from CRDTActorV2
          aliveActors += (actorId -> false)

          ctx.log.info(s"FailureDetector-$id: AliveActors: $aliveActors")
        case None =>
          ctx.log.info(s"FailureDetector-$id: Actor not found")
      }

      Behaviors.same

  Behaviors.same
}
