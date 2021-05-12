package com.wavesplatform.dex.actors

import akka.actor.{Actor, ActorContext, ActorRef}
import com.wavesplatform.dex.actors.WorkingStash.ContextualMessage
import kamon.Kamon
import kamon.trace.Span

import scala.collection.immutable.Queue

// Because the Akka's one doesn't work during actor start, e.g.: https://stackoverflow.com/questions/45305757/akka-unstashall-not-replaying-the-messages
trait WorkingStash {
  this: Actor =>

  private var stashedMessages = Queue.empty[(ActorRef, ContextualMessage)]

  def stash(message: Any)(implicit actorContext: ActorContext): Unit =
    stashedMessages = stashedMessages.enqueue(actorContext.sender() -> ContextualMessage(message, Kamon.currentSpan()))

  def unstashAll(): Unit = {
    stashedMessages.foreach { case (sender, ContextualMessage(msg, parentSpan)) =>
      val span = Kamon.spanBuilder("unstashed").asChildOf(parentSpan).start()
      Kamon.runWithSpan(span, finishSpan = true) {
        self.tell(msg, sender)
      }
    }
    stashedMessages = Queue.empty
  }

}

object WorkingStash {
  final private case class ContextualMessage(msg: Any, parentSpan: Span)
}
