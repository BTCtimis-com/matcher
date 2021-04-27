package com.wavesplatform.dex.tool

import akka.http.scaladsl.server.Directives.mapInnerRoute
import akka.http.scaladsl.server.Route
import kamon.Kamon
import kamon.context.{BinaryPropagation, Context, Storage}
import kamon.trace.Span
import kamon.trace.Trace.SamplingDecision

import java.io.ByteArrayOutputStream
import scala.concurrent.{ExecutionContext, Future}

object KamonTraceUtils {

  def setSpanName(name: String): Unit =
    Kamon.currentSpan().name(name)

  def setSpanNameAndForceSamplingDecision(name: String): Unit = {
    Kamon.currentSpan().takeSamplingDecision() //forces span sample decision inferring
    Kamon.currentSpan().name(name)
  }

  def mkTracedRoute(operationName: String, forceSamplingDecision: Boolean = true)(route: Route): Route = mapInnerRoute { route => ctx =>
    if (forceSamplingDecision)
      setSpanNameAndForceSamplingDecision(operationName)
    else
      setSpanName(operationName)
    route(ctx)
  }(route)

  //https://github.com/kamon-io/Kamon/issues/829
  def propagateTraceCtxThroughCachedFuture[A](future: => Future[A])(implicit ec: ExecutionContext): Future[A] = {
    val span = mkIgnoredSpan()
    val scope = Kamon.storeContext(Kamon.currentContext().withEntry(Span.Key, span))

    try future.transform(
      res => {
        finishSpanAndContextScope(span, scope)
        res
      },
      err => {
        failSpanAndContextScope(span, scope, err)
        err
      }
    )
    catch {
      case e: Throwable =>
        failSpanAndContextScope(span, scope, e)
        throw e
    }
  }

  def runWithIgnoredSpan[A](f: => A): A = {
    val span = mkIgnoredSpan()
    Kamon.runWithSpan(span, finishSpan = true)(f)
  }

  def writeCtx(ctx: Context): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    Kamon.defaultBinaryPropagation().write(ctx, BinaryPropagation.ByteStreamWriter.of(out))
    out.toByteArray
  }

  def readCtx(input: Array[Byte]): Context =
    Kamon.defaultBinaryPropagation().read(BinaryPropagation.ByteStreamReader.of(input))

  lazy val DoNotSample: SamplingDecision = loadModule(mkSamplingDecisionPath("DoNotSample"))

  lazy val Sample: SamplingDecision = loadModule(mkSamplingDecisionPath("Sample"))

  lazy val FollowsFrom: Span.Link.Kind = loadModule("kamon.trace.Span$Link$Kind$FollowsFrom$")

  private def mkSamplingDecisionPath(name: String): String =
    "kamon.trace.Trace$SamplingDecision$" + name + "$"

  //for some reason accessing sampling decisions and links fails scalac
  //this is a hacky workaround
  private def loadModule[A](path: String): A =
    //https://stackoverflow.com/questions/8867766/scala-dynamic-object-class-loading
    Class.forName(path).getField("MODULE$").get(null).asInstanceOf[A]

  private def mkIgnoredSpan(): Span =
    Kamon.spanBuilder("ignored")
      .ignoreParentFromContext()
      .doNotTrackMetrics()
      .samplingDecision(DoNotSample)
      .traceId(Kamon.currentSpan().trace.id)
      .start()

  private def finishSpanAndContextScope(span: Span, scope: Storage.Scope): Unit = {
    span.finish()
    scope.close()
  }

  private def failSpanAndContextScope(span: Span, scope: Storage.Scope, throwable: Throwable): Unit = {
    span.fail(throwable.getMessage, throwable)
    span.finish()
    scope.close()
  }

}
