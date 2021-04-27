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

  def mkTracedRoute(operationName: String)(route: Route): Route =
    mapInnerRoute { route => ctx =>
      Kamon.currentSpan().name(operationName)
      Kamon.currentSpan().takeSamplingDecision() //force span sample decision inferring
      route(ctx)
    }(route)

  //https://github.com/kamon-io/Kamon/issues/829
  def propagateTraceCtxThroughCachedFuture[A](future: => Future[A])(implicit ec: ExecutionContext): Future[A] = {
    val span = Kamon.spanBuilder("ignored")
      .ignoreParentFromContext()
      .doNotTrackMetrics()
      .samplingDecision(DoNotSample)
      .traceId(Kamon.currentSpan().trace.id)
      .start()
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

  def writeCtx(ctx: Context): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    Kamon.defaultBinaryPropagation().write(ctx, BinaryPropagation.ByteStreamWriter.of(out))
    out.toByteArray
  }

  def readCtx(input: Array[Byte]): Context =
    Kamon.defaultBinaryPropagation().read(BinaryPropagation.ByteStreamReader.of(input))

  lazy val DoNotSample: SamplingDecision = loadClass(mkSamplingDecisionPath("DoNotSample"))

  lazy val Sample: SamplingDecision = loadClass(mkSamplingDecisionPath("Sample"))

  lazy val FollowsFrom: Span.Link.Kind = loadClass("kamon.trace.Span$Link$Kind$FollowsFrom$")

  private def mkSamplingDecisionPath(name: String): String =
    "kamon.trace.Trace$SamplingDecision$" + name + "$"

  //for some reason accessing sampling decisions and links fails scalac
  //this is a hacky workaround
  private def loadClass[A](path: String): A = {
    val classVal = Class.forName(path)
    val constructor = classVal.getDeclaredConstructor()
    val instance = constructor.newInstance().asInstanceOf[A]
    instance
  }

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
