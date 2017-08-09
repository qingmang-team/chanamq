package chana.mq.amqp.server.rest

import akka.actor.ActorSystem
import akka.cluster.sharding.ClusterSharding
import akka.event.Logging
import akka.pattern.ask
import akka.http.scaladsl.model.ContentType
import akka.http.scaladsl.model.HttpCharset
import akka.http.scaladsl.model.MediaType
import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer
import akka.util.Timeout
import chana.mq.amqp.entity.VhostEntity
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait AdminApi extends Directives {
  val system: ActorSystem
  implicit val materializer: ActorMaterializer
  import system.dispatcher
  private implicit val timeout: Timeout = system.settings.config.getInt("chana.mq.internal.timeout").seconds

  val log = Logging.getLogger(system, this)

  private val charset: HttpCharset = HttpCharset.custom("utf-8")
  private val mediaTYpe: MediaType.WithOpenCharset = MediaType.applicationWithOpenCharset("json")
  protected val JsonContentType = ContentType.WithCharset(mediaTYpe, charset)

  private def siteSharding = ClusterSharding(system).shardRegion(VhostEntity.typeName)

  def adminApi = (pathPrefix("admin")) {
    vhostApi
  }

  def vhostApi = pathPrefix("vhost") {
    path("put" / Segment ~ Slash.?) { vhost =>
      get {
        completeWithStatusCode {
          (siteSharding ? VhostEntity.Create(vhost))
        }
      }
    } ~ path("delete" / Segment ~ Slash.?) { vhost =>
      get {
        completeWithStatusCode {
          (siteSharding ? VhostEntity.Delete(vhost))
        }
      }
    }
  }

  private def completeWithStatusCode(f: Future[_]) = onComplete(f) {
    case Success(_) =>
      complete { StatusCodes.OK }
    case Failure(e) =>
      log.error(e, e.getMessage)
      complete { StatusCodes.InternalServerError }
  }
}
