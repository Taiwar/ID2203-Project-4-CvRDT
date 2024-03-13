package crdtactor

import crdtactor.ActorFailureDetectorV2.Command
import org.apache.pekko.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.{LWWMap, ReplicatedDelta}

import scala.concurrent.duration.DurationInt
import scala.util.Random

object ActorFailureDetectorV2 {

  // The type of messages that the actor can handle
  sealed trait Command

  // Triggers the actor to start the computation (do this only once!)
  case class Start(from: ActorRef[CRDTActorV4.Command], revived: Boolean) extends Command

  case class StateMsg(state: ddata.LWWMap[String, Int]) extends Command

  // Mortality Notice
  case class MortalityNotice(from: ActorRef[CRDTActorV4.Command]) extends Command

  // GetIdResponse
  case class GetIdResponse(id: Int) extends Command

  // Key-Value Ops

  private case class AliveActorsResponse(aliveActors: Map[Int, Boolean]) extends Command
  // Timeout
  case class Timeout(actorId: Int) extends Command

  private case object TimerKey

  // Heartbeat
  case class Heartbeat() extends Command

  case class HeartbeatAck(from: ActorRef[CRDTActorV4.Command]) extends Command

  // Join
  case class JoinRequest(newActor: ActorRef[CRDTActorV4.Command]) extends Command

  case class GetAliveActors(from: ActorRef[Command]) extends Command

  case object StartFD extends Command

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
  var mainActor: Option[ActorRef[CRDTActorV4.Command]] = None

  // Add a flag to indicate whether the Timeout message should be processed
  private var processTimeout = true
}

import crdtactor.ActorFailureDetectorV2.*

// The actor implementation of the (perfect) failure detector
// Note: the current implementation assumes perfect failure detection and does not
// handle network partitions or other failure scenarios
class ActorFailureDetectorV2(
                   // id is the unique identifier of the actor, ctx is the actor context
                   id: Int,
                   ctx: ActorContext[Command],
                   timers: TimerScheduler[Command]
                 ) extends AbstractBehavior[Command](ctx) {

  // The CRDT state of this actor, mutable var as LWWMap is immutable
  private var crdtstate = ddata.LWWMap.empty[String, Int]

  // The CRDT address of this actor/node, used for the CRDT state to identify the nodes
  private val selfNode = Utils.nodeFactory()

  // Define the variable
  var others: Map[Int, ActorRef[Command]] = Map()

  // Define a method to update the variable
  def refreshOthers(): Unit = {
    others = Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]().filter(_._1 != id)
  }

  // A map with all the other actors and a boolean if they are alive
  private val aliveActors = scala.collection.mutable.Map[Int, Boolean]()

  // TODO: Check if this is needed
  // A set of actors that are not synchronized
  private val notSynchronizedActors = scala.collection.mutable.Set[ActorRef[CRDTActorV4.Command]]()

  // Start the failure detector timer (heartbeat)
  private def startFailureDetectorTimer(): Unit = {

    timers.startTimerWithFixedDelay(
      id, // Use the provided id or default to the actor's id
      ActorFailureDetectorV2.Heartbeat(),
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
      ActorFailureDetectorV2.Timeout(actorId),
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
//    timers.cancel(TimerKey)
    startTimeoutTimer(id) // Reset the timer
  }

  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case Start(from, revived) =>
      ctx.log.info(s"FailureDetector-$id started by ${from.path.name}")

      // Store the sender of the start message
      mainActor = Some(from)

      // Refresh the actor references from the global state
      refreshOthers()

      // If the actor is revived, get latest aliveActors from the leader
      if (revived) {
        // TODO: Actually get the leader
        // Get the latest aliveActors from the leader
        others.get(0) match {
          case Some(actor: ActorRef[CRDTActorV4.Command]) =>
            ctx.log.info(s"FailureDetector-$id: Sending GetAliveActors to actor ${actor.path.name}")
            actor ! CRDTActorV4.GetAliveActors(ctx.self)
//            ctx.scheduleOnce(1000.millis, actor, CRDTActorV4.GetAliveActors(ctx.self))
          case _ =>
            ctx.log.info(s"FailureDetector-$id: No actors found in 'others' map")
        }

      } else {
        // Start the failure detector timer
        startFailureDetectorTimer()

        // Setup ids for all actors
        for ((actorId, actor) <- others) {
          // Add actor to the aliveActors map
          aliveActors += (actorId -> true)
        }
      }

      Behaviors.same

    // TODO: Remove this if not needed
    case GetIdResponse(actorId) =>
      ctx.log.info(s"FailureDetector-$id: Received idResponse")
      Behaviors.same

    // Only handle timeout if the actor is still alive
    case Timeout(actorId: Int) =>
      ctx.log.info(s"FailureDetector-$id: Timeout for actor $actorId")
      // Set aliveActors to false
      aliveActors += (actorId -> false)

      var deadActor: Option[ActorRef[CRDTActorV4.Command]] = None

      // Get the actor from the 'others' map or skip the rest of the code
      others.get(actorId) match {
        case Some(actor: ActorRef[CRDTActorV4.Command]) =>
          deadActor = Some(actor)

          // TODO: Check best way to handle this
          // Stop the timer
          stopTimer(actorId)

          // TODO: Notify the application that the actor is dead
          // Send the MortalityNotice to all other actors
          for (actor <- aliveActors.keys) {
            if (aliveActors(actor)) {
              // Get the actor from the 'others' map
              others.get(actor) match {
                case Some(actor: ActorRef[CRDTActorV4.Command]) =>
                  // Send Heartbeat to the actor
                  actor ! CRDTActorV4.MortalityNotice(deadActor.get)
                case _ =>
                  ctx.log.info(s"FailureDetector-$id: Actor not found")
              }
            }
          }
        case _ =>
          ctx.log.info(s"FailureDetector-$id: Actor not found")
      }

      Behaviors.same

    // Heartbeat handling
    case Heartbeat() =>
      ctx.log.info(s"FailureDetector-$id: Sending heartbeat")

      ctx.log.info(s"FailureDetector-$id: AliveActors: $aliveActors")

      // TODO: Check if needed
      // Update the others map
      refreshOthers()

      for ((actorId, actor) <- others) {
        if (!aliveActors.contains(actorId)) {
          // Add actor to the aliveActors map
          aliveActors += (actorId -> true)
        }
      }

      // Send the heartbeat to all other actors
      for (actorId <- aliveActors.keys if aliveActors(actorId)) {
        // Get the actor from the 'others' map
        others.get(actorId) match {
          case Some(actor: ActorRef[CRDTActorV4.Command]) =>
            // Send Heartbeat to the actor
            actor ! CRDTActorV4.Heartbeat(ctx.self, false)
          case _ =>
          // Skip the actor if it is not found or not of the correct type
        }
      }
      Behaviors.same

    // Handle HeartbeatAck from CRDTActorV4
    case HeartbeatAck(from) =>
      ctx.log.info(s"FailureDetector-$id: Received heartbeat ack from ${from.path.name}")

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
          ctx.log.info(s"FailureDetector-$id: Actor not found while handling HeartbeatAck")
      }

      Behaviors.same

    case MortalityNotice(from) =>
      ctx.log.info(s"FailureDetector-$id: Received MortalityNotice")

      others.find(_._2 == from) match {
        case Some((actorId, actor)) =>
          // Handle MortalityNotice message from CRDTActorV4
          aliveActors += (actorId -> false)

          ctx.log.info(s"FailureDetector-$id: AliveActors: $aliveActors")
        case None =>
          ctx.log.info(s"FailureDetector-$id: Actor not found")
      }
      Behaviors.same

    case JoinRequest(newActor) =>
      ctx.log.info(s"FailureDetector-$id: Received JoinRequest from ${newActor.path.name}")

      // Refresh the actor references
      refreshOthers()

      ctx.log.info(s"FailureDetector-$id: Updated others: $others")
      // Get the id of the new actor from the others
      others.find(_._2 == newActor) match {
        case Some((actorId, actor)) =>
          // Add the new actor to the aliveActors map
          aliveActors += (actorId -> true)

          ctx.log.info(s"FailureDetector-$id: Updated aliveActors: $aliveActors")
        case None =>
          ctx.log.info(s"FailureDetector-$id: Actor not found")
      }
      Behaviors.same

    // Update the aliveActors map with the latest aliveActors from the leader
    case AliveActorsResponse(aliveActors) =>
      ctx.log.info(s"FailureDetector-$id: Received AliveActorsResponse")

      aliveActors match
        case map: Map[Int, Boolean] =>

          // Update the aliveActors map
          this.aliveActors ++= aliveActors

          // Remove our own id from the map
          this.aliveActors -= id

          ctx.log.info(s"FailureDetector-$id: Get new AliveActors from the leader: $aliveActors")

      // After updating the aliveActors map, send join requests to the actors that are alive
      for (actorId <- aliveActors.keys if aliveActors(actorId)) {
        // Get the actor from the 'others' map
        others.get(actorId) match {
          case Some(actor: ActorRef[CRDTActorV4.Command]) =>
            // Send JoinRequest to the actor
//            actor ! CRDTActorV4.RequestToJoin(mainActor.get)
          case _ =>
            // Skip the actor if it is not found or not of the correct type
            ctx.log.info(s"FailureDetector-$id: Actor not found")
        }
      }

      // Wait a little bit before starting the failure detector timer
      ctx.scheduleOnce(800.millis, ctx.self, StartFD)

      Behaviors.same

    case GetAliveActors(from) =>
      ctx.log.info(s"FailureDetector-$id: Received GetAliveActors")

      // Create copy of the aliveActors map
      var actors = this.aliveActors.toMap

      // Remove our own id from the map
      actors -= id

      // Add sender to the aliveActors map
      actors += (id -> true)

      // Send the aliveActors to the failure detector from the actor
      from ! AliveActorsResponse(actors)

      Behaviors.same

    case StartFD =>
      ctx.log.info(s"FailureDetector-$id: Starting failure detector timer!!!!")
      startFailureDetectorTimer()
      Behaviors.same

  Behaviors.same
}
