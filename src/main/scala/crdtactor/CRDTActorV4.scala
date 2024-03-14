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

  case class testProbe(from: ActorRef[Command]) extends Command

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

  case class AbortOperationsTimer(actor: ActorRef[Command]) extends Command

  case object Die extends Command
  case class AtomicAbort(opId: String) extends Indication

  case class LeaderAbort(reason: String) extends Indication

  case class AbortAtomicOperations() extends Command

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
      from: ActorRef[Command],
      ref: (String, ActorRef[Command])
  ) extends Command

  // Combines lock gathering and sync
  private case class PrepareResponse(
      timestamp: (Int, Int),
      from: ActorRef[Command],
      delta: Option[ReplicatedDelta]
  ) extends Command

  private case class Commit(
      timestamp: (Int, Int),
      from: ActorRef[Command],
      opId: String
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

  case class GetAliveActors(from: ActorRef[ActorFailureDetectorV2.Command]) extends Command
}

import crdtactor.CRDTActorV4.*

// The actor implementation of the CRDT actor that uses a LWWMap CRDT to store the state
class CRDTActorV4(
    // id is the unique identifier of the actor, ctx is the actor context
    id: Int,
    ctx: ActorContext[Command],
    timers: TimerScheduler[CRDTActorV4.Command]
) extends AbstractBehavior[Command](ctx) {

  private var testActor = Option.empty[ActorRef[Command]]

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
  // Track ongoing transactions in case of leader failure
  private val pendingTransactionRefs =
    mutable.Map[String, ActorRef[Command]]()
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

  // Print debug messages
  private def debugMsg(msg: String) = {
    context.log.info(s"CRDTActor-$id: $msg")
  }

  private def debugFD(msg: String) = {
//    context.log.info(s"CRDTActor-$id: $msg")
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
            actorRef !
              // Send the delta to the other actors
              DelayedMessage(DeltaMsg(ctx.self, delta))
        }
        dirty = false

  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case testProbe(from) =>
      debugMsg(s"CRDTActor-$id: Storing test probe to actor")
      testActor = Some(from)
      Behaviors.same

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

          // Send Leader abort to test probe
          testActor match
            case Some(actor) =>
              actor ! LeaderAbort("Aborted due to leader change")
            case None => ()
          debugMsg(s"Leader-$id: Aborting ongoing atomic commands due to leader change ($pendingTransactionRefs)")
          // Send abort to all atomic commands in pendingTransactionRefs
          pendingTransactionRefs.foreach { case (opId, origin) =>
            origin ! AtomicAbort(opId)
          }
        // Clear pending transactions
        pendingTransactionAgreement.clear()
        pendingTransactions.clear()
        pendingTransactionRefs.clear()
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
        debugMsg(s"Leader-$id: Sending delta")
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

    case Prepare(timestamp, keys, from, (opId, origin)) =>
      ctx.log.debug(s"CRDTActor-$id: Consuming Prepare operation $timestamp")
      // Only respond if source is leader
      if (leader.isEmpty || leader.get != from) {
        ctx.log.warn(s"CRDTActor-$id: Not leader")
        return Behaviors.same
      }

      pendingTransactionLocks += (timestamp -> keys)
      pendingTransactionRefs += (opId -> origin)
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
      if (pendingTransactionAgreement(timestamp) == others.size) {
        debugMsg(
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

        debugMsg(s"Leader-$id: Executed transaction $timestamp with pendingTransactionRefs $pendingTransactionRefs")

        // Remove transaction from pending
        pendingTransactionAgreement -= timestamp
        pendingTransactions -= timestamp
        pendingTransactionRefs -= tx.opId

        // Respond to client
        tx.origin ! AtomicResponse(tx.opId, responses)

        // Send commit to all others to unlock keys
        everyone.foreach { (_, actorRef) =>
          actorRef ! DelayedMessage(Commit(timestamp, ctx.self, tx.opId))
        }
      }
      Behaviors.same

    // Handle commit
    case Commit(timestamp, from, opId) =>
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
      // Remove transaction from pending
      pendingTransactionLocks -= timestamp
      pendingTransactionRefs -= opId

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

      debugMsg(s"Leader-$id: Starting transaction $timestamp")

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
      pendingTransactionRefs += (opId -> origin)

      // Send prepare to others
      others.foreach { (_, actorRef) =>
        actorRef ! DelayedMessage(
          Prepare(timestamp, keys, ctx.self, (opId, origin))
        )
      }
      Behaviors.same

    // Only used for testing
    case GetState(from) =>
      ctx.log.debug(s"CRDTActor-$id: Sending state to ${from.path.name}")
      from ! State(crdtstate)
      Behaviors.same

//    case AtomicAbortAtomicAbort(opId) =>
//      ctx.log.debug(s"CRDTActor-$id: Consuming atomic abort operation")
//      Behaviors.same

    // Start the failure detector for this actor
    case StartFailureDetector(revived) =>
      debugFD(s"CRDTActor-$id: Starting failure detector for CRDTActor-$id")

      // Spawn the failure detector and give it a name
      failureDetector = ctx.spawn(Behaviors.setup[crdtactor.ActorFailureDetectorV2.Command] { ctx =>
        Behaviors.withTimers(timers => new ActorFailureDetectorV2(id, ctx, timers))
      }, s"failure-detector-${id.toString}")

      failureDetector ! ActorFailureDetectorV2.Start(ctx.self, revived)
      Behaviors.same

    // Heartbeat handling
    case Heartbeat(from, delay) =>
      debugFD(s"CRDTActor-$id: Received heartbeat")

      if (delay) {
        debugFD(s"CRDTActor-$id: Delaying heartbeat ack for 600ms")
        ctx.scheduleOnce(600.millis, ctx.self, Heartbeat(from, false))
      } else {
        debugFD(s"CRDTActor-$id: Sending heartbeat ack to ${from.path.name}")
        // Send ack back to the sender
        from ! ActorFailureDetectorV2.HeartbeatAck(ctx.self)
      }

      Behaviors.same

    case HeartbeatAck(from) =>
      debugFD(s"CRDTActor-$id: Received heartbeat ack")
      Behaviors.same

    case MortalityNotice(actor) =>
      debugMsg(s"CRDTActor-$id: Received mortality notice that ${actor.path.name} has stopped responding")

      // Update the failure detector with the new state
      failureDetector ! ActorFailureDetectorV2.MortalityNotice(actor)

      // If actor is/was the leader, we need to find a new leader
      // (Make next actor the leader)
      if (leader.isDefined && leader.get == actor) {
        debugMsg(s"CRDTActor-$id: Leader has died, finding new leader")

        // Get id of the previous leader
        val previousLeaderId = everyone.find(_._2 == actor).get._1

        // Get the next leader
        // TODO: Find safer way to get next leader
        val nextLeader = everyone.find(_._1 > previousLeaderId + 1)

        if (nextLeader.isDefined) {
          debugMsg(s"CRDTActor-$id: New leader is ${nextLeader.get._2.path.name}")

          everyone.foreach { (_, actorRef) =>
            if (actorRef != actor) {
              actorRef ! Leader(nextLeader.get._2)
            }
          }
        }
      } // If im the leader, we need to set a timer to abort ongoing transactions
      else if (leader.isDefined && leader.get == ctx.self) {
        debugMsg(s"CRDTActor-$id: Actor is dead, aborting ongoing transactions")

        // Set a timer to abort ongoing transactions
        ctx.scheduleOnce(500.millis, ctx.self, AbortOperationsTimer(actor))
      }

      Behaviors.same

    case AbortAtomicOperations() =>
      debugMsg(s"CRDTActor-$id: Received atomic abort, clearing locks/transactions/refs")

      // Send abort to all atomic commands in pendingTransactionRefs
      pendingTransactionRefs.foreach { case (opId, origin) =>
        origin ! AtomicAbort(opId)
      }

      // Clear pending transactions
      pendingTransactionAgreement.clear()
      pendingTransactions.clear()
      pendingTransactionRefs.clear()
      // Unlock all locks
      locks.clear()

      Behaviors.same

    case AbortOperationsTimer(actor) =>
      debugMsg(s"CRDTActor-$id: Timer expired, aborting ongoing transactions")

      // Send abort message to all actors except the dead one
      everyone.foreach { (_, actorRef) =>
        if (actorRef != actor) {
          actorRef ! AbortAtomicOperations()
        }
      }

      Behaviors.same

    case RequestToJoin(from) =>
      debugFD(s"CRDTActor-$id: Received request to join from ${from.path.name}")
      // Update the failure detector with the new state
      failureDetector ! ActorFailureDetectorV2.JoinRequest(from)
      Behaviors.stopped

    case GetAliveActors(from) =>
      debugFD(s"CRDTActor-$id: Received request to get alive actors from ${from.path.name}")
      // Update the failure detector with the new state
      failureDetector ! ActorFailureDetectorV2.GetAliveActors(from)
      Behaviors.same

    case Die =>
      context.log.info("Received PoisonPill, stopping...")
      Behaviors.stopped

  Behaviors.same
}
