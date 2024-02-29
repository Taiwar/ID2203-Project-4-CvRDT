package crdtactor

import org.apache.pekko.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.{LWWMap, ReplicatedDelta}

import scala.concurrent.duration.DurationInt

object CRDTActorV3 {
  // The type of messages that the actor can handle

  sealed trait Command
  sealed trait Request extends Command
  sealed trait Indication extends Command

  // External messages

  // For testing: Messages to read the current state of the CRDT
  case class GetState(from: ActorRef[Command]) extends Request

  case class State(state: ddata.LWWMap[String, Int]) extends Indication

  // Key-Value Ops
  case class Put(key: String, value: Int, from: ActorRef[Command])
      extends Request

  case class PutResponse(key: String) extends Indication

  case class Get(key: String, from: ActorRef[Command]) extends Request

  case class GetResponse(key: String, value: Int) extends Indication

  case class Atomic(commands: Iterable[Command], from: ActorRef[Command])
      extends Request

  case class AtomicResponse(responses: Iterable[(String, Command)])
      extends Indication

  // Internal messages

  // Internal forwarding of atomic messages
  case class ForwardAtomic(
      origin: ActorRef[Command],
      commands: Iterable[Command],
      from: ActorRef[Command]
  ) extends Command

  // Periodic delta messages
  private case class DeltaMsg(from: ActorRef[Command], delta: ReplicatedDelta)
      extends Command

  // Combines lock gathering and sync
  private case class Prepare(
      timestamp: (Int, Int),
      keys: Iterable[String],
      from: ActorRef[Command]
  ) extends Command

  // Combines lock gathering and sync
  private case class PrepareResponse(
      timestamp: (Int, Int),
      from: ActorRef[Command],
      delta: Option[ReplicatedDelta]
  ) extends Command

  private case class Commit(
      timestamp: (Int, Int),
      from: ActorRef[Command]
  ) extends Command

  private case object Timeout extends Command
  private case object TimerKey

  // Ballot leader election messages
  case class Leader(leader: ActorRef[Command]) extends Command
}

import crdtactor.CRDTActorV3.*

// The actor implementation of the CRDT actor that uses a LWWMap CRDT to store the state
class CRDTActorV3(
    // id is the unique identifier of the actor, ctx is the actor context
    id: Int,
    ctx: ActorContext[Command],
    timers: TimerScheduler[CRDTActorV3.Command]
) extends AbstractBehavior[Command](ctx) {

  // The CRDT state of this actor, mutable var as LWWMap is immutable
  private var crdtstate = ddata.LWWMap.empty[String, Int]

  // The CRDT address of this actor/node, used for the CRDT state to identify the nodes
  private val selfNode = Utils.nodeFactory()

  // Hack to get the actor references of the other actors, check out `lazy val`
  // Careful: make sure you know what you are doing if you are editing this code
  private lazy val others =
    Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]()

  private var leader: Option[ActorRef[Command]] = None

  private var time = (id, 0)
  private val pendingTransactionLocks =
    scala.collection.mutable.Map[(Int, Int), Int]()
  private val pendingTransactions =
    scala.collection.mutable.Map[(Int, Int), ForwardAtomic]()
  // TODO: Remove locks if leader dies
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
    // Handle leader
    case Leader(l) =>
      ctx.log.info(s"CRDTActor-$id: Consuming leader - ${l.path.name}")
      leader = Some(l)
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

    case Prepare(timestamp, keys, from) =>
      ctx.log.info(s"CRDTActor-$id: Consuming Prepare operation $timestamp")
      // TODO: Do we need to keep track of pending things here?
      keys.foreach { key =>
        locks(key) = locks.getOrElse(key, 0) + 1
      }
      // We don't need to execute the transaction ourselves like in full 2PC, for SC it's enough to get the result sent to us later
      from ! PrepareResponse(timestamp, ctx.self, crdtstate.delta)
      Behaviors.same

    case PrepareResponse(timestamp, _, delta) =>
      ctx.log.info(
        s"CRDTActor-$id: Consuming PrepareResponse operation $timestamp"
      )

      // Increment transaction tracker
      pendingTransactionLocks(timestamp) =
        pendingTransactionLocks(timestamp) + 1

      // Merge the delta into the local CRDT state
      delta match
        case Some(d) =>
          crdtstate = crdtstate.mergeDelta(d.asInstanceOf)
        case None => ()

      // If we got all responses, commit
      if (pendingTransactionLocks(timestamp) == others.size) {
        ctx.log.info(
          s"CRDTActor-$id: All locks acquired for transaction $timestamp"
        )
        // Get commands from pending transactions
        val tx = pendingTransactions(timestamp)

        // Execute commands in tx
        val responses = tx.commands.map {
          case Put(key, value, _) =>
            crdtstate = crdtstate.put(selfNode, key, value)
            dirty = true
            (key, PutResponse(key))
          case Get(key, _) =>
            (key, GetResponse(key, crdtstate.get(key).getOrElse(0)))
          case _ =>
            throw new Exception(
              "Unexpected command"
            ) // TODO: Handle this better instead of crashing
        }

        // Remove transaction from pending
        pendingTransactionLocks -= timestamp
        pendingTransactions -= timestamp

        // Respond to client
        tx.origin ! AtomicResponse(responses)

        // Send commit to all others to unlock keys
        others.foreach { (_, actorRef) =>
          Thread.sleep(Utils.RANDOM_BC_DELAY)
          actorRef ! Commit(timestamp, ctx.self)
        }
      }
      Behaviors.same

    // Handle commit
    case Commit(timestamp, from) =>
      ctx.log.info(s"CRDTActor-$id: Consuming Commit operation $timestamp")
      // Unlock keys
      pendingTransactionLocks -= timestamp
      pendingTransactions -= timestamp
      // TODO: We should only unlock keys of that transaction
      locks.foreach { case (key, value) =>
        locks(key) = locks.getOrElse(key, 0) - 1
      }
      Behaviors.same

    case Atomic(commands, from) =>
      ctx.log.info(s"CRDTActor-$id: Consuming atomic operation $commands")
      // If we are not the leader, forward to leader
      leader match
        case Some(l) =>
          l ! ForwardAtomic(from, commands, ctx.self)
        case None =>
          // TODO: Respond with error
          ctx.log.info(s"CRDTActor-$id: No leader")
      Behaviors.same

    case ForwardAtomic(origin, commands, from) =>
      // Create new unique tid
      time = (time._1, time._2 + 1)
      val timestamp = time

      // Add new transaction to transactions
      pendingTransactionLocks += (timestamp -> 0)
      pendingTransactions += (timestamp -> ForwardAtomic(
        origin,
        commands,
        from
      ))

      // Get all used keys from commands
      val keys = commands.flatMap {
        case Put(key, _, _) => Some(key)
        case Get(key, _)    => Some(key)
        case _              => None
      }

      // TODO: Check if we already have locks on some keys and queue the transaction if so

      // Lock own keys
      keys.foreach { key =>
        locks(key) = locks.getOrElse(key, 0) + 1
      }

      // Send prepare to others
      others.foreach { (_, actorRef) =>
        Thread.sleep(Utils.RANDOM_BC_DELAY)
        actorRef ! Prepare(timestamp, keys, ctx.self)
      }
      Behaviors.same

    // Only used for testing
    case GetState(from) =>
      ctx.log.info(s"CRDTActor-$id: Sending state to ${from.path.name}")
      from ! State(crdtstate)
      Behaviors.same

  Behaviors.same
}