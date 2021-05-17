package com.wavesplatform.dex.grpc.integration.clients.combined

import akka.actor.ActorSystem
import akka.actor.typed.BehaviorInterceptor.ReceiveTarget
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{Behaviors, StashBuffer}
import akka.actor.typed.{Behavior, BehaviorInterceptor, TypedActorContext}
import com.wavesplatform.dex.grpc.integration.clients.ControlledStream.SystemEvent
import com.wavesplatform.dex.grpc.integration.clients.blockchainupdates.{BlockchainUpdatesControlledStream, BlockchainUpdatesConversions}
import com.wavesplatform.dex.grpc.integration.clients.combined.CombinedStream.{Settings, Status}
import com.wavesplatform.dex.grpc.integration.clients.combined.CombinedStreamActor.CustomLoggerBehaviorInterceptor.LogMessageTemplate
import com.wavesplatform.dex.grpc.integration.clients.domain.WavesNodeEvent
import com.wavesplatform.dex.grpc.integration.clients.matcherext.{UtxEventConversions, UtxEventsControlledStream}
import com.wavesplatform.dex.grpc.integration.services.UtxEvent
import com.wavesplatform.events.api.grpc.protobuf.SubscribeEvent
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.reactive.subjects.ConcurrentSubject
import org.slf4j.Logger
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

// TODO move to Actors?
object CombinedStreamActor {
  sealed trait Message extends Product with Serializable

  sealed trait Command extends Message

  object Command {
    case class Start(fromHeight: Int) extends Command

    case class UpdateProcessedHeight(height: Int) extends Command

    case object Restart extends Command

    case object Continue extends Command

    case class ProcessUtxSystemEvent(evt: SystemEvent) extends Command
    case class ProcessBlockchainUpdatesSystemEvent(evt: SystemEvent) extends Command

    case class ProcessUtxEvent(evt: UtxEvent) extends Command
    case class ProcessBlockchainUpdatesEvent(evt: SubscribeEvent) extends Command
  }

  implicit final class PartialFunctionOps[A, B](val self: PartialFunction[A, B]) extends AnyVal {
    def toTotal(f: A => B): A => B = self.applyOrElse(_, f)
  }

  case class CustomLoggerOptions[T](logger: Logger, enabled: Boolean, level: Level, formatter: PartialFunction[T, String])

  class CustomLoggerBehaviorInterceptor[T](private val opts: CustomLoggerOptions[T])(implicit ct: ClassTag[T])
      extends BehaviorInterceptor[T, T] {

    import opts.logger
    private val formatter = opts.formatter.lift

    override def aroundReceive(ctx: TypedActorContext[T], msg: T, target: ReceiveTarget[T]): Behavior[T] = {
      log(LogMessageTemplate, msg)
      target(ctx, msg)
    }

    private def log(template: String, message: T): Unit =
      if (opts.enabled) formatter(message).foreach { message =>
        opts.level match {
          case Level.ERROR => logger.error(template, message)
          case Level.WARN => logger.warn(template, message)
          case Level.INFO => logger.info(template, message)
          case Level.DEBUG => logger.debug(template, message)
          case Level.TRACE => logger.trace(template, message)
          case other => throw new IllegalArgumentException(s"Unknown log level [$other].")
        }
      }

    override def isSame(other: BehaviorInterceptor[Any, Any]): Boolean = other match {
      case a: CustomLoggerBehaviorInterceptor[_] => a.opts == opts
      case _ => false
    }

  }

  object CustomLoggerBehaviorInterceptor {
    val LogMessageTemplate = "Got: {}"
  }

  def apply(
    settings: Settings,
    processedHeight: AtomicInteger,
    blockchainUpdates: BlockchainUpdatesControlledStream,
    utxEvents: UtxEventsControlledStream,
    status: ConcurrentSubject[Status, Status],
    output: ConcurrentSubject[WavesNodeEvent, WavesNodeEvent]
  )(implicit monixScheduler: Scheduler): Behavior[Command] = Behaviors.setup[Command] { context =>

    blockchainUpdates.systemStream.foreach(context.self ! Command.ProcessBlockchainUpdatesSystemEvent(_))
    blockchainUpdates.stream.foreach(context.self ! Command.ProcessBlockchainUpdatesEvent(_))

    utxEvents.systemStream.foreach(context.self ! Command.ProcessUtxSystemEvent(_))
    utxEvents.stream.foreach(context.self ! Command.ProcessUtxEvent(_))

    Behaviors.intercept(() =>
      new CustomLoggerBehaviorInterceptor[Command](CustomLoggerOptions(
        logger = context.log,
        enabled = true,
        level = Level.DEBUG,
        formatter = {
          case _: Command.ProcessBlockchainUpdatesEvent => "ProcessBlockchainUpdatesEvent"
          case x @ (_: Command.Start | _: Command.UpdateProcessedHeight | Command.Restart | Command.Continue | _: Command.ProcessUtxSystemEvent |
              _: Command.ProcessBlockchainUpdatesSystemEvent) => x.toString
        }
      ))
    ) {
      val ignore: Command => Behavior[Command] = _ => Behaviors.same
      def partial(f: PartialFunction[Command, Behavior[Command]]): PartialFunction[Command, Behavior[Command]] = f

      def logAndIgnore(text: String): Behavior[Command] = {
        context.log.warn(text)
        Behaviors.same[Command]
      }

      def closed: Behavior[Command] = {
        context.log.info("Now is Closed")
        status.onNext(Status.Closing(blockchainUpdates = true, utxEvents = true))
        Behaviors.receiveMessage[Command](x => logAndIgnore(s"Unexpected $x"))
      }

      def closing(utxEventsClosed: Boolean, blockchainUpdatesClosed: Boolean): Behavior[Command] = {
        context.log.info(s"Now is Closing(utx=$utxEventsClosed, bu=$blockchainUpdatesClosed)")
        status.onNext(Status.Closing(blockchainUpdates = blockchainUpdatesClosed, utxEvents = utxEventsClosed))
        Behaviors.receiveMessage[Command] {
          case Command.ProcessUtxSystemEvent(SystemEvent.Closed) =>
            if (utxEventsClosed) logAndIgnore("utx has already closed")
            else if (blockchainUpdatesClosed) closed
            else {
              blockchainUpdates.close()
              closing(utxEventsClosed = true, blockchainUpdatesClosed = false)
            }

          case Command.ProcessBlockchainUpdatesSystemEvent(SystemEvent.Closed) =>
            if (blockchainUpdatesClosed) logAndIgnore("bu has already closed")
            else if (utxEventsClosed) closed
            else {
              utxEvents.close()
              closing(utxEventsClosed = false, blockchainUpdatesClosed = true)
            }

          case _ => // Silently ignore, it's ok
            Behaviors.same
        }
      }

      val onClosedOrRestart: PartialFunction[Command, Behavior[Command]] = {
        case Command.ProcessUtxSystemEvent(SystemEvent.Closed) =>
          blockchainUpdates.close()
          closing(utxEventsClosed = true, blockchainUpdatesClosed = false)

        case Command.ProcessBlockchainUpdatesSystemEvent(SystemEvent.Closed) =>
          utxEvents.close()
          closing(utxEventsClosed = false, blockchainUpdatesClosed = true)

        case Command.Restart =>
          blockchainUpdates.stop() // Will be self-healed
          Behaviors.same
      }

      def quiet: Behavior[Command] = Behaviors.receiveMessage[Command] {
        partial {
          case Command.Continue =>
            utxEvents.start()
            starting(utxEventsStarted = false, blockchainUpdatesStarted = false)
        }
          .orElse(onClosedOrRestart)
          .toTotal(ignore)
      }

      def wait(): Behavior[Command] = {
        context.scheduleOnce(settings.restartDelay, context.self, Command.Continue)
        quiet
      }

      def startWithRollback(): Behavior[Command] = {
        context.log.info("Now is StartingWithRollback")
        output.onNext(WavesNodeEvent.RolledBack(WavesNodeEvent.RolledBack.To.Height(processedHeight.decrementAndGet())))
        wait()
      }

      def startWithoutRollback(): Behavior[Command] = {
        context.log.info("Now is StartingWithoutRollback")
        wait()
      }

      def starting(utxEventsStarted: Boolean, blockchainUpdatesStarted: Boolean): Behavior[Command] = {
        context.log.info(s"Now is Starting(utx=$utxEventsStarted, bu=$blockchainUpdatesStarted)")
        status.onNext(Status.Starting(blockchainUpdates = blockchainUpdatesStarted, utxEvents = utxEventsStarted))
        Behaviors.withStash[Command](Int.MaxValue) { stash =>
          Behaviors.receiveMessagePartial[Command] {
            partial {
              case Command.ProcessUtxSystemEvent(SystemEvent.BecameReady) =>
                if (utxEventsStarted) logAndIgnore("utx has already started")
                else if (blockchainUpdatesStarted) {
                  context.log.warn("Unexpected state: bu has already started")
                  becomeWorking(stash)
                } else {
                  blockchainUpdates.startFrom(processedHeight.get() + 1)
                  starting(utxEventsStarted = true, blockchainUpdatesStarted)
                }

              case Command.ProcessBlockchainUpdatesSystemEvent(SystemEvent.BecameReady) =>
                if (blockchainUpdatesStarted) logAndIgnore("bu has already started")
                else if (utxEventsStarted) becomeWorking(stash)
                else starting(utxEventsStarted, blockchainUpdatesStarted = true)

              case Command.ProcessUtxSystemEvent(SystemEvent.Stopped) =>
                // We don't need to stop blockchain updates, because we start it
                // only after receiving SystemEvent.BecameReady from UTX Stream
                stash.clear()
                startWithoutRollback()

              case Command.ProcessBlockchainUpdatesSystemEvent(SystemEvent.Stopped) =>
                utxEvents.stop()
                stopping(utxEventsStopped = false, blockchainUpdatesStopped = true)

              case cmd @ (Command.ProcessUtxEvent(_) | Command.ProcessBlockchainUpdatesEvent(_)) =>
                stash.stash(cmd)
                Behaviors.same
            }.orElse(onClosedOrRestart)
          }
        }
      }

      def working: Behavior[Command] = {
        context.log.info("Now is Working")
        status.onNext(Status.Working)
        Behaviors.receiveMessagePartial[Command] {
          partial {
            case Command.ProcessUtxSystemEvent(SystemEvent.Stopped) =>
              // See Starting + Stopped
              blockchainUpdates.stop()
              stopping(utxEventsStopped = true, blockchainUpdatesStopped = false)

            case Command.ProcessBlockchainUpdatesSystemEvent(SystemEvent.Stopped) =>
              // See utxEventsTransitions: Starting + Stopped
              utxEvents.stop()
              stopping(utxEventsStopped = false, blockchainUpdatesStopped = true)

            case Command.ProcessUtxEvent(evt) =>
              UtxEventConversions.toEvent(evt).foreach(output.onNext) // We log errors in the deep of toEvent
              Behaviors.same

            case Command.ProcessBlockchainUpdatesEvent(evt) =>
              evt.update.flatMap(BlockchainUpdatesConversions.toEvent) match {
                case Some(evt) =>
                  output.onNext(evt)
                  Behaviors.same
                case None =>
                  blockchainUpdates.requestNext()
                  logAndIgnore(s"Can't convert $evt to a domain event, asking next")
              }

            case Command.UpdateProcessedHeight(x) =>
              processedHeight.set(x)
              Behaviors.same

            case x => logAndIgnore(s"Unexpected $x")
          }.orElse(onClosedOrRestart)
        }
      }

      def becomeWorking(stash: StashBuffer[Command]): Behavior[Command] = stash.unstashAll(working)

      def stopping(utxEventsStopped: Boolean, blockchainUpdatesStopped: Boolean): Behavior[Command] = {
        context.log.info(s"Now is Stopping(utx=$utxEventsStopped, bu=$blockchainUpdatesStopped)")
        status.onNext(Status.Stopping(blockchainUpdates = blockchainUpdatesStopped, utxEvents = utxEventsStopped))
        Behaviors.receiveMessagePartial[Command] {
          partial {
            case Command.ProcessUtxSystemEvent(SystemEvent.Stopped) =>
              if (utxEventsStopped) logAndIgnore("utx has already stopped")
              else if (blockchainUpdatesStopped) startWithRollback()
              else {
                blockchainUpdates.stop()
                stopping(utxEventsStopped = true, blockchainUpdatesStopped)
              }

            case Command.ProcessBlockchainUpdatesSystemEvent(SystemEvent.Stopped) =>
              if (blockchainUpdatesStopped) logAndIgnore("bu has already stopped")
              else if (utxEventsStopped) startWithRollback()
              else {
                utxEvents.stop()
                stopping(utxEventsStopped = false, blockchainUpdatesStopped = true)
              }

            case Command.ProcessUtxSystemEvent(SystemEvent.BecameReady) =>
              utxEvents.stop()
              logAndIgnore("Unexpected event, stopping utx forcefully")

            case Command.ProcessBlockchainUpdatesSystemEvent(SystemEvent.BecameReady) =>
              blockchainUpdates.stop()
              logAndIgnore("Unexpected event, stopping bu forcefully")

            case _: Command.ProcessUtxEvent | _: Command.ProcessBlockchainUpdatesEvent => // Silently ignore, it's ok
              Behaviors.same
          }.orElse(onClosedOrRestart)
        }
      }

      // Initial
      Behaviors
        .receiveMessagePartial[Command] {
          case Command.Start(fromHeight) =>
            utxEvents.start()
            processedHeight.set(fromHeight - 1)
            starting(utxEventsStarted = false, blockchainUpdatesStarted = false)
        }
        .behavior
    }
  }

}

class AkkaCombinedStream(settings: Settings, blockchainUpdates: BlockchainUpdatesControlledStream, utxEvents: UtxEventsControlledStream)(implicit
  system: ActorSystem,
  monixScheduler: Scheduler
) extends CombinedStream {

  private val processedHeight = new AtomicInteger(0)

  private val statusStream = ConcurrentSubject.publish[Status]
  @volatile private var lastStatus: Status = Status.Starting()
  statusStream.doOnNext(x => Task { lastStatus = x }).lastL.runToFuture

  private val outputStream = ConcurrentSubject.publish[WavesNodeEvent]

  private val ref = system.spawn(
    CombinedStreamActor(settings, processedHeight, blockchainUpdates, utxEvents, statusStream, outputStream),
    "combined-stream"
  )

  override def startFrom(height: Int): Unit = ref ! CombinedStreamActor.Command.Start(height)

  override def restart(): Unit = ref ! CombinedStreamActor.Command.Restart

  override def currentStatus: CombinedStream.Status = lastStatus

  override def updateProcessedHeight(height: Int): Unit = ref ! CombinedStreamActor.Command.UpdateProcessedHeight(height)

  override def currentProcessedHeight: Int = processedHeight.get()

  override val stream: Observable[WavesNodeEvent] = outputStream
}
