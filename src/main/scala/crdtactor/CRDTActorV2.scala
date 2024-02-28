package crdtactor

import org.apache.pekko.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.{LWWMap, ReplicatedDelta}

import scala.concurrent.duration.DurationInt

object CRDTActorV2 {
  // The type of messages that the actor can handle
  sealed trait Command

  // Messages containing the CRDT delta state exchanged between actors
  case class DeltaMsg(from: ActorRef[Command], delta: ReplicatedDelta)
    extends Command

  case class RequestSync(from: ActorRef[Command]) extends Command

  case class GatherLocks(keys: Iterable[String], from: ActorRef[Command])
    extends Command

  case class ReleaseLocks(keys: Iterable[String], from: ActorRef[Command])
    extends Command

  // Triggers the actor to start the computation (do this only once!)
  case object Start extends Command

  // For testing: Messages to read the current state of the CRDT
  case class ReadState(from: ActorRef[Command]) extends Command

  case class StateMsg(state: ddata.LWWMap[String, Int]) extends Command

  // Key-Value Ops
  case class Put(key: String, value: Int, from: ActorRef[Command])
    extends Command

  case class PutResponse(key: String) extends Command

  case class Get(key: String, from: ActorRef[Command]) extends Command

  case class GetResponse(key: String, value: Int) extends Command

  case class Atomic(commands: Iterable[Command], from: ActorRef[Command])
    extends Command

  case class AtomicResponse(responses: Iterable[(String, Command)])
    extends Command

  // Timer
  private case object Timeout extends Command
  private case object TimerKey
}

import crdtactor.CRDTActorV2.*

// The actor implementation of the CRDT actor that uses a LWWMap CRDT to store the state
class CRDTActorV2(
                   // id is the unique identifier of the actor, ctx is the actor context
                   id: Int,
                   ctx: ActorContext[Command],
                   timers: TimerScheduler[CRDTActorV2.Command]
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

  // Start timer to periodically broadcast the delta
  timers.startTimerWithFixedDelay(
    TimerKey,
    Timeout,
    50.millis
  )

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
            actorRef !
              // Send the delta to the other actors
              DeltaMsg(ctx.self, delta)
        }
        dirty = false

  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case Start =>
      ctx.log.info(s"CRDTActor-$id started")
      // ctx.self ! ConsumeOperation // start consuming operations
      Behaviors.same

    case Put(key, value, from) =>
      ctx.log.info(s"CRDTActor-$id: Consuming operation $key -> $value")
      // Check lock for key
      if (locks.getOrElse(key, 0) > 0) {
        commandBuffer.enqueue(Put(key, value, from))
        return Behaviors.same
      }
      crdtstate = crdtstate.put(selfNode, key, value)
      dirty = true
      ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
      from ! PutResponse(key)
      Behaviors.same

    case Timeout =>
      // Work through and empty buffer
      while (commandBuffer.nonEmpty) {
        val command = commandBuffer.dequeue()
        ctx.self ! command
      }
      if (!dirty) return Behaviors.same
      broadcastAndResetDeltas()
      Behaviors.same

    case Get(key, from) =>
      ctx.log.info(s"CRDTActor-$id: Sending value of $key to ${from.path.name}")
      // Check lock for key
      if (locks.getOrElse(key, 0) > 0) {
        commandBuffer.enqueue(Get(key, from))
        return Behaviors.same
      }
      from ! GetResponse(key, crdtstate.get(key).getOrElse(0))
      Behaviors.same

    case DeltaMsg(from, delta) =>
      ctx.log.info(s"CRDTActor-$id: Received delta from ${from.path.name}")
      // Merge the delta into the local CRDT state
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      Behaviors.same

    case RequestSync(from) =>
      ctx.log.info(s"CRDTActor-$id: Sending delta to ${from.path.name}")
      Thread.sleep(Utils.RANDOM_BC_DELAY)
      // Only send delta if is not empty
      if (crdtstate.delta.isDefined) {
        from ! DeltaMsg(ctx.self, crdtstate.delta.get)
      }
      // TODO: If we wait for responses from this, we should send an empty one here instead of nothing
      Behaviors.same

    case GatherLocks(keys, from) =>
      ctx.log.info(s"CRDTActor-$id: Consuming gather locks operation $keys")
      keys.foreach { key =>
        locks(key) = locks.getOrElse(key, 0) + 1
      }
      // TODO: Response?
      Behaviors.same

    case ReleaseLocks(keys, from) =>
      ctx.log.info(s"CRDTActor-$id: Consuming release locks operation $keys")
      keys.foreach { key =>
        locks(key) = locks.getOrElse(key, 0) - 1
      }
      Behaviors.same

    case Atomic(commands, from) =>
      ctx.log.info(s"CRDTActor-$id: Consuming atomic operation $commands")
      // Get all used keys from commands
      val keys = commands.flatMap {
        case Put(key, _, _) => Some(key)
        case Get(key, _)    => Some(key)
        case _              => None
      }

      // Gather locks
      others.foreach { (_, actorRef) =>
        Thread.sleep(Utils.RANDOM_BC_DELAY)
        actorRef ! GatherLocks(keys, ctx.self)
      }

      // Wait for locks
      // TODO Wait for responses
      Thread.sleep(100)

      // Request sync
      others.foreach { (_, actorRef) =>
        Thread.sleep(Utils.RANDOM_BC_DELAY)
        actorRef ! RequestSync(ctx.self)
      }

      // Wait for responses
      // TODO Wait for responses
      Thread.sleep(100)

      // Execute commands
      val responses = commands.map {
        case Put(key, value, _) =>
          crdtstate = crdtstate.put(selfNode, key, value)
          dirty = true
          (key, PutResponse(key))
        case Get(key, _) =>
          (key, GetResponse(key, crdtstate.get(key).getOrElse(0)))
        case _ => throw new Exception("Unexpected command")
      }
      // Send deltas
      broadcastAndResetDeltas()

      // Release locks
      others.foreach { (_, actorRef) =>
        Thread.sleep(Utils.RANDOM_BC_DELAY)
        actorRef ! ReleaseLocks(keys, ctx.self)
      }
      // Wait for responses
      // TODO Wait for responses
      Thread.sleep(100)

      from ! AtomicResponse(responses)
      Behaviors.same

    case ReadState(from) =>
      ctx.log.info(s"CRDTActor-$id: Sending state to ${from.path.name}")
      from ! StateMsg(crdtstate)
      Behaviors.same

  Behaviors.same
}
