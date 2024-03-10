package crdtactor

import org.apache.pekko.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.{LWWMap, ReplicatedDelta}
import org.apache.pekko.dispatch.ControlMessage

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

val USE_DUAL_BUFFER = true

object CRDTActorV4 {
  // The type of messages that the actor can handle

  sealed trait Command
  sealed trait Request extends Command
  sealed trait Indication extends Command

  case class DelayedMessage(message: Command) extends Command
  // External messages
  // For testing: Messages to read the current state of the CRDT
  case class GetState(from: ActorRef[Command])
      extends ControlMessage
      with Command

  case class State(state: ddata.LWWMap[String, Int]) extends Indication

  // Key-Value Ops
  case class Put(opId: String, key: String, value: Int, from: ActorRef[Command])
      extends Request

  case class PutResponse(opId: String, key: String) extends Indication

  case class Get(opId: String, key: String, from: ActorRef[Command])
      extends Request

  case class GetResponse(opId: String, key: String, value: Option[Int])
      extends Indication

  case class UpdateWithContext(
      opId: String,
      key: String,
      contextKey: String,
      update: (Option[Int], Option[Int]) => Int,
      from: ActorRef[Command]
  ) extends Request

  case class UpdateWithContextResponse(opId: String, key: String)
      extends Indication

  case class Atomic(
      opId: String,
      commands: Iterable[Command],
      from: ActorRef[Command]
  ) extends Request

  case class AtomicResponse(
      opId: String,
      responses: Iterable[(String, Command)]
  ) extends Indication

  case class MortalityNotice(actor: ActorRef[Command]) extends Command

  case object Die extends Command
  case class AtomicAbort(opId: String) extends Indication

  // Error responses

  case class UnknownCommandResponse() extends Indication

  case class NoLeaderResponse() extends Indication

  // Internal messages

  // Internal forwarding of atomic messages
  case class ForwardAtomic(
      opId: String,
      origin: ActorRef[Command],
      commands: Iterable[Command],
      from: ActorRef[Command]
  ) extends Command

  // Periodic delta messages
  private case class DeltaMsg(from: ActorRef[Command], delta: ReplicatedDelta)
      extends ControlMessage
      with Command

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

  private case object Timeout extends ControlMessage with Command
  private case object TimerKey

  // Ballot leader election messages
  case class Leader(leader: ActorRef[Command]) extends Command

  // Failure detector
  case class StartFailureDetector(revived: Boolean) extends Command

  // Heartbeat
  case class Heartbeat(from: ActorRef[ActorFailureDetectorV2.Command], delay: Boolean) extends Command

  case class HeartbeatAck(from: ActorRef[Command]) extends Command

  case class RequestToJoin(from: ActorRef[Command]) extends Command
}

import crdtactor.CRDTActorV4.*

// The actor implementation of the CRDT actor that uses a LWWMap CRDT to store the state
class CRDTActorV4(
    // id is the unique identifier of the actor, ctx is the actor context
    id: Int,
    ctx: ActorContext[Command],
    timers: TimerScheduler[CRDTActorV4.Command]
) extends AbstractBehavior[Command](ctx) {

  // The CRDT state of this actor, mutable var as LWWMap is immutable
  private var crdtstate = ddata.LWWMap.empty[String, Int]

  // The CRDT address of this actor/node, used for the CRDT state to identify the nodes
  private val selfNode = Utils.nodeFactory()

  // Hack to get the actor references of the other actors, check out `lazy val`
  // Careful: make sure you know what you are doing if you are editing this code
  private lazy val everyone =
    Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]()

  // Make copy of everyone and remove self
  private lazy val others = everyone - id

  // The leader of the system

  private var leader: Option[ActorRef[Command]] = None

  private var time = (id, 0)
  private val pendingTransactionLocks =
    mutable.Map[(Int, Int), Iterable[String]]()

  private val locks = mutable.Map[String, Int]()
  private val commandBuffer = mutable.Queue[Command]()
  private val atomicCommandBuffer = mutable.Queue[Command]()
  private var dirty = false

  // Random number generator
  val r = scala.util.Random

  // Failure detector
  var failureDetector: ActorRef[ActorFailureDetectorV2.Command] = _

  // Leader state
  private val pendingTransactionAgreement =
    mutable.Map[(Int, Int), Int]()
  private val pendingTransactions =
    mutable.Map[(Int, Int), ForwardAtomic]()
  private var transactionWorking = false

  // Start timer to periodically broadcast the delta
  timers.startTimerWithFixedDelay(
  TimerKey,
  Timeout,
  Utils.CRDT_SYNC_PERIOD.milliseconds
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
            actorRef !
              // Send the delta to the other actors
              DelayedMessage(DeltaMsg(ctx.self, delta))
        }
        dirty = false

  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    // Receive DelayedMessage and send the internal message to ourselves after a delay
    case DelayedMessage(message) =>
      // Schedule the message to be sent to ourselves after Utils.RANDOM_MESSAGE_DELAY
      timers.startSingleTimer(
        message,
        message,
        Utils.RANDOM_MESSAGE_DELAY.milliseconds
      )
      Behaviors.same

    // Handle leader
    case Leader(l) =>
      ctx.log.debug(s"CRDTActor-$id: Consuming leader - ${l.path.name}")
      // If no leader before, nothing to do
      if (leader.isEmpty)
        ctx.log.warn(s"CRDTActor-$id: Starting system with new leader")
        if (l == ctx.self)
          ctx.log.warn(s"CRDTActor-$id: I'm the new leader!")
        leader = Some(l)
        return Behaviors.same
      // If leader is replacing old one, we need to abort ongoing transactions
      if (leader.get != l)
        ctx.log.warn(s"CRDTActor-$id: Leader change detected")
        // If we're now leader, we need to abort ongoing atomic commands
        if (l == ctx.self)
          ctx.log.warn(s"CRDTActor-$id: I'm the new leader")
          // Send abort to all atomic commands in pendingTransactions
          pendingTransactions.foreach { case (_, tx) =>
            tx.origin ! AtomicAbort(tx.opId)
          }
        // Clear pending transactions
        pendingTransactionAgreement.clear()
        pendingTransactions.clear()
        // Unlock all locks
        locks.clear()
      leader = Some(l)
      Behaviors.same

    case Timeout =>
      // Don't send deltas if we're currently executing a transaction, but queue a delta for later
      if (transactionWorking) {
        ctx.self ! Timeout
        return Behaviors.same
      }
      // Send deltas only if dirty
      if (!dirty) return Behaviors.same
      broadcastAndResetDeltas()
      // Print if leader
      if (leader.isDefined && leader.get == ctx.self) {
        ctx.log.info(s"Leader-$id: Sending delta")
      }
      Behaviors.same

    case Put(opId, key, value, from) =>
      // Check lock for key
      if (locks.getOrElse(key, 0) > 0) {
        commandBuffer.enqueue(Put(opId, key, value, from))
        return Behaviors.same
      }
      ctx.log.debug(s"CRDTActor-$id: Executing PUT $key -> $value")
      // TODO: Could we use a logical clock and avoid the need for synchronized clocks?
      crdtstate = crdtstate.put(selfNode, key, value)
      dirty = true
      ctx.log.debug(s"CRDTActor-$id: CRDT state: $crdtstate")
      from ! PutResponse(opId, key)
      Behaviors.same

    case Get(opId, key, from) =>
      // Check lock for key
      if (locks.getOrElse(key, 0) > 0) {
        commandBuffer.enqueue(Get(opId, key, from))
        return Behaviors.same
      }
      ctx.log.debug(s"CRDTActor-$id: Executing GET $key")
      from ! GetResponse(opId, key, crdtstate.get(key))
      Behaviors.same

    case UpdateWithContext(opId, key, contextKey, update, from) =>
      // Check lock for key
      if (locks.getOrElse(key, 0) > 0) {
        commandBuffer.enqueue(
          UpdateWithContext(opId, key, contextKey, update, from)
        )
        return Behaviors.same
      }
      ctx.log.debug(s"CRDTActor-$id: Executing UpdateWithContext $key")
      val current = crdtstate.get(key)
      val context = crdtstate.get(contextKey)
      val next = update(current, context)
      // Put next to store if different
      if (current.isDefined && current.get != next) {
        crdtstate = crdtstate.put(selfNode, key, next)
        dirty = true
      }
      from ! UpdateWithContextResponse(opId, key)
      Behaviors.same

    case DeltaMsg(from, delta) =>
      ctx.log.debug(s"CRDTActor-$id: Received delta from ${from.path.name}")
      // Merge the delta into the local CRDT state
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      Behaviors.same

    case Prepare(timestamp, keys, from) =>
      ctx.log.debug(s"CRDTActor-$id: Consuming Prepare operation $timestamp")
      // Only respond if source is leader
      if (leader.isEmpty || leader.get != from) {
        ctx.log.warn(s"CRDTActor-$id: Not leader")
        return Behaviors.same
      }

      pendingTransactionLocks += (timestamp -> keys)
      keys.foreach { key =>
        locks(key) = 1
      }
      // We don't need to execute the transaction on every node like in full 2PC, for SC it's enough to get the result sent to us by the leader later
      from ! DelayedMessage(
        PrepareResponse(timestamp, ctx.self, crdtstate.delta)
      )
      Behaviors.same

    case PrepareResponse(timestamp, _, delta) =>
      ctx.log.debug(
        s"Leader-$id: Consuming PrepareResponse operation $timestamp"
      )

      // Increment agreement tracker
      pendingTransactionAgreement(timestamp) =
        pendingTransactionAgreement(timestamp) + 1

      // Merge the delta into the local CRDT state
      delta match
        case Some(d) =>
          crdtstate = crdtstate.mergeDelta(d.asInstanceOf)
        case None => ()

      // If we got all responses, commit
      // TODO: See if a majority would also suffice
      if (pendingTransactionAgreement(timestamp) == others.size) {
        ctx.log.info(
          s"Leader-$id: All locks acquired for transaction $timestamp"
        )
        // Get commands from pending transactions
        val tx = pendingTransactions(timestamp)
        transactionWorking = true

        // Execute commands in tx
        val responses = tx.commands.map {
          case Put(opId, key, value, _) =>
            crdtstate = crdtstate.put(selfNode, key, value)
            dirty = true
            (key, PutResponse(opId, key))
          case Get(opId, key, _) =>
            (key, GetResponse(opId, key, crdtstate.get(key)))
          case UpdateWithContext(opId, key, contextKey, update, _) =>
            val current = crdtstate.get(key)
            val context = crdtstate.get(contextKey)
            ctx.log.debug(
              s"Leader-$id: (Atomic) Executing UpdateWithContext $current with context $context"
            )
            val next = update(current, context)
            if (current.isDefined && current.get != next) {
              crdtstate = crdtstate.put(selfNode, key, next)
              dirty = true
            }
            (key, UpdateWithContextResponse(opId, key))
          case _ =>
            ctx.log.warn(s"Leader-$id: Unknown command")
            ("unknown", UnknownCommandResponse())
        }
        transactionWorking = false

        // Remove transaction from pending
        pendingTransactionAgreement -= timestamp
        pendingTransactions -= timestamp

        // Respond to client
        tx.origin ! AtomicResponse(tx.opId, responses)

        // Send commit to all others to unlock keys
        everyone.foreach { (_, actorRef) =>
          actorRef ! DelayedMessage(Commit(timestamp, ctx.self))
        }
      }
      Behaviors.same

    // Handle commit
    case Commit(timestamp, from) =>
      ctx.log.debug(s"CRDTActor-$id: Consuming Commit operation $timestamp")
      if (leader.isEmpty || leader.get != from) {
        ctx.log.warn(s"CRDTActor-$id: Not leader")
        return Behaviors.same
      }

      // Unlock keys
      val keys = pendingTransactionLocks(timestamp)
      keys.foreach { key =>
        locks(key) = 0
      }
      pendingTransactionLocks -= timestamp

      // Work through and empty buffer
      ctx.log.debug(s"CRDTActor-$id: Executing buffered commands")
      while (commandBuffer.nonEmpty) {
        val command = commandBuffer.dequeue()
        ctx.self ! command
      }
      // Queue atomic commands after regular commands
      while (atomicCommandBuffer.nonEmpty) {
        val command = atomicCommandBuffer.dequeue()
        ctx.self ! command
      }
      Behaviors.same

    case Atomic(opId, commands, from) =>
      ctx.log.debug(s"CRDTActor-$id: Consuming atomic operation $commands")
      // If we are not the leader, forward to leader
      leader match
        case Some(l) =>
          l ! DelayedMessage(ForwardAtomic(opId, from, commands, ctx.self))
        case None =>
          ctx.log.warn(s"CRDTActor-$id: No leader")
          from ! NoLeaderResponse()
      Behaviors.same

    case ForwardAtomic(opId, origin, commands, from) =>
      // If we're not leader, reply with abort
      if (leader.isEmpty || leader.get != ctx.self) {
        ctx.log.warn(
          s"CRDTActor-$id: Not leader, cannot handle atomic operation"
        )
        origin ! AtomicAbort(opId)
        return Behaviors.same
      }

      // Get all used keys from commands
      val keys = commands.flatMap {
        case Put(opId, key, _, _) => Some(key)
        case Get(opId, key, _)    => Some(key)
        case _                    => None
      }

      // Check if we already have locks on some keys and queue the transaction if so
      if (keys.exists(key => locks.getOrElse(key, 0) > 0)) {
        ctx.log.debug(
          s"Leader-$id: Queuing transaction due to locks"
        )
        if (USE_DUAL_BUFFER) {
          atomicCommandBuffer.enqueue(
            ForwardAtomic(opId, origin, commands, from)
          )
        } else {
          commandBuffer.enqueue(Atomic(opId, commands, from))
        }
        return Behaviors.same
      }

      // Create new unique tid
      time = (time._1, time._2 + 1)
      val timestamp = time

      ctx.log.info(s"Leader-$id: Starting transaction $timestamp")

      // Add new transaction to transactions
      pendingTransactionAgreement += (timestamp -> 0)
      pendingTransactions += (timestamp -> ForwardAtomic(
        opId,
        origin,
        commands,
        from
      ))

      // Do own "Prepare" now, if we just send commit to ourselves we could accept another transaction before we have the locks
      pendingTransactionLocks += (timestamp -> keys)
      keys.foreach { key =>
        locks(key) = 1
      }

      // Send prepare to others
      others.foreach { (_, actorRef) =>
        actorRef ! DelayedMessage(Prepare(timestamp, keys, ctx.self))
      }
      Behaviors.same

    // Only used for testing
    case GetState(from) =>
      ctx.log.debug(s"CRDTActor-$id: Sending state to ${from.path.name}")
      from ! State(crdtstate)
      Behaviors.same

    // Start the failure detector for this actor
    case StartFailureDetector(revived) =>
      ctx.log.info(s"CRDTActor-$id: Starting failure detector for CRDTActor-$id")
      // Randomize the id of the failure detector
//      val fdId = r.nextInt(100) + id

      // Spawn the failure detector and give it a name
      failureDetector = ctx.spawn(Behaviors.setup[crdtactor.ActorFailureDetectorV2.Command] { ctx =>
        Behaviors.withTimers(timers => new ActorFailureDetectorV2(id, ctx, timers))
      }, s"failure-detector-${id.toString}")

      failureDetector ! ActorFailureDetectorV2.Start(ctx.self, revived)
      Behaviors.same

    // Heartbeat handling
    case Heartbeat(from, delay) =>
      ctx.log.info(s"CRDTActor-$id: Received heartbeat")

      if (delay) {
        ctx.log.info(s"CRDTActor-$id: Delaying heartbeat ack for 600ms")
        ctx.scheduleOnce(600.millis, ctx.self, Heartbeat(from, false))
      } else {
        ctx.log.info(s"CRDTActor-$id: Sending heartbeat ack to ${from.path.name}")
        // Send ack back to the sender
        from ! ActorFailureDetectorV2.HeartbeatAck(ctx.self)
      }

      Behaviors.same

    case HeartbeatAck(from) =>
      ctx.log.info(s"CRDTActor-$id: Received heartbeat ack")
      Behaviors.same

    case MortalityNotice(actor) =>
      ctx.log.info(s"CRDTActor-$id: Received mortality notice that ${actor.path.name} has stopped responding")
      // Update the failure detector with the new state
      failureDetector ! ActorFailureDetectorV2.MortalityNotice(actor)
      Behaviors.same


    case RequestToJoin(from) =>
      ctx.log.info(s"CRDTActor-$id: Received request to join from ${from.path.name}")
      // Update the failure detector with the new state
      failureDetector ! ActorFailureDetectorV2.JoinRequest(from)
      Behaviors.stopped

    case Die =>
      context.log.info("Received PoisonPill, stopping...")
      Behaviors.stopped

  Behaviors.same
}
