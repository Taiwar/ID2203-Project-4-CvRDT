package crdtactor

import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.ReplicatedDelta
import org.apache.pekko.actor.typed.ActorRef

object CRDTActor {
  // The type of messages that the actor can handle
  sealed trait Command

  // Messages containing the CRDT delta state exchanged between actors
  case class DeltaMsg(from: ActorRef[Command], delta: ReplicatedDelta)
    extends Command

  // Triggers the actor to start the computation (do this only once!)
  case object Start extends Command

  // For testing: Messages to read the current state of the CRDT
  case class ReadState(from: ActorRef[Command]) extends Command

  case class StateMsg(state: ddata.LWWMap[String, Int]) extends Command

  // Key-Value Ops
  case class Put(key: String, value: Int, from: ActorRef[Command]) extends Command

  case class PutResponse(key: String) extends Command

  case class Get(key: String, from: ActorRef[Command]) extends Command

  case class GetResponse(key: String, value: Int) extends Command
}

import CRDTActor.*

// The actor implementation of the CRDT actor that uses a LWWMap CRDT to store the state
class CRDTActor(
  // id is the unique identifier of the actor, ctx is the actor context
                 id: Int,
                 ctx: ActorContext[Command]
               ) extends AbstractBehavior[Command](ctx) {

  // The CRDT state of this actor, mutable var as LWWMap is immutable
  private var crdtstate = ddata.LWWMap.empty[String, Int]

  // The CRDT address of this actor/node, used for the CRDT state to identify the nodes
  private val selfNode = Utils.nodeFactory()

  // Hack to get the actor references of the other actors, check out `lazy val`
  // Careful: make sure you know what you are doing if you are editing this code
  private lazy val others =
    Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]()

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
              DeltaMsg(ctx.self, delta)
        }

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
      crdtstate = crdtstate.put(selfNode, key, value)
      ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
      broadcastAndResetDeltas()
      from ! PutResponse(key)
      Behaviors.same

    case Get(key, from) =>
      ctx.log.info(s"CRDTActor-$id: Sending value of $key to ${from.path.name}")
      from ! GetResponse(key, crdtstate.get(key).getOrElse(0))
      Behaviors.same

    case DeltaMsg(from, delta) =>
      ctx.log.info(s"CRDTActor-$id: Received delta from ${from.path.name}")
      // Merge the delta into the local CRDT state
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      Behaviors.same

    case ReadState(from) =>
      ctx.log.info(s"CRDTActor-$id: Sending state to ${from.path.name}")
      from ! StateMsg(crdtstate)
      Behaviors.same

  Behaviors.same
}
