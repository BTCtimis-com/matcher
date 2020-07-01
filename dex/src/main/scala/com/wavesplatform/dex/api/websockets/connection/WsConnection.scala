package com.wavesplatform.dex.api.websockets.connection

import java.util.concurrent.{ConcurrentLinkedQueue, ThreadLocalRandom}

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{CompletionStrategy, Materializer, OverflowStrategy}
import com.wavesplatform.dex.api.websockets._
import com.wavesplatform.dex.domain.utils.{LoggerFacade, ScorexLogging}
import org.slf4j.LoggerFactory
import play.api.libs.json.{JsError, JsSuccess, Json}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class WsConnection(uri: String, keepAlive: Boolean = true)(implicit system: ActorSystem, materializer: Materializer) extends ScorexLogging {

  private val testId: Int = ThreadLocalRandom.current().nextInt(10000)

  override protected lazy val log: LoggerFacade = LoggerFacade(LoggerFactory.getLogger(s"WsConnection[testId=$testId]"))

  log.info(s"""Connecting to Matcher WS API:
            |         URI = $uri
            |  Keep alive = $keepAlive""".stripMargin)

  import materializer.executionContext

  private val wsHandlerRef = system.actorOf(TestWsHandlerActor.props(testId, keepAlive))

  protected def stringifyClientMessage(cm: WsClientMessage): TextMessage.Strict =
    WsMessage.toStrictTextMessage(cm)(WsClientMessage.wsClientMessageWrites)

  // From test to server
  private val source: Source[TextMessage.Strict, ActorRef] = {
    val completionMatcher: PartialFunction[Any, CompletionStrategy] = { case akka.actor.Status.Success(_) => CompletionStrategy.draining }
    val failureMatcher: PartialFunction[Any, Throwable]             = { case Status.Failure(cause)        => cause }

    Source
      .actorRef[WsClientMessage](completionMatcher, failureMatcher, 10, OverflowStrategy.fail)
      .map(stringifyClientMessage)
      .mapMaterializedValue { source =>
        wsHandlerRef.tell(TestWsHandlerActor.AssignSourceRef, source)
        source
      }
  }

  private val messagesBuffer: ConcurrentLinkedQueue[WsServerMessage] = new ConcurrentLinkedQueue[WsServerMessage]()

  // From server to test
  private val sink: Sink[Message, Future[Done]] = Sink.foreach { x =>
    val rawMsg = x.asTextMessage.getStrictText
    Json.parse(rawMsg).validate[WsServerMessage] match {
      case JsSuccess(value, _) =>
        log.debug(s"Got $value")
        messagesBuffer.add(value)
        value match {
          case value: WsPingOrPong => wsHandlerRef ! value
          case _                   =>
        }

      case JsError(e) => log.error(s"Can't parse message: $rawMsg, $e")
    }
  }

  private val flow: Flow[Message, TextMessage.Strict, Future[Done]] = Flow.fromSinkAndSourceCoupled(sink, source).watchTermination() {
    case (_, f) =>
      f.onComplete {
        case Success(_) => log.info(s"WebSocket connection to $uri successfully closed")
        case Failure(e) => log.error(s"WebSocket connection to $uri closed with an error", e)
      }(materializer.executionContext)
      f
  }

  val (connectionResponse, closed) = Http().singleWebSocketRequest(WebSocketRequest(uri), flow)

  val connectionOpenedTs: Long                   = System.currentTimeMillis
  val connectionClosedTs: Future[Long]           = closed.map(_ => System.currentTimeMillis)
  val connectionLifetime: Future[FiniteDuration] = connectionClosedTs.map(cc => FiniteDuration(cc - connectionOpenedTs, MILLISECONDS))

  def messages: List[WsServerMessage] = messagesBuffer.iterator().asScala.toList
  def clearMessages(): Unit           = messagesBuffer.clear()

  def send(message: WsClientMessage): Unit = wsHandlerRef ! TestWsHandlerActor.SendToServer(message)

  def close(): Unit     = if (!isClosed) wsHandlerRef ! TestWsHandlerActor.CloseConnection
  def isClosed: Boolean = closed.isCompleted
}
