package chana.mq.amqp.server.engine

import chana.mq.amqp.model.LongString
import java.io.UnsupportedEncodingException

object SaslMechanism {
  val supportedMechanisms = Set("PLAIN", "EXTERNAL")

  def getSaslMechanism(clientMechanism: String): Option[SaslMechanism] = {
    clientMechanism match {
      case "PLAIN"    => Some(new PlainMechanism())
      case "EXTERNAL" => Some(new ExternalMechanism())
      case _          => None
    }
  }

  def getSaslMechanism(clientMechanisms: Array[String]): Option[SaslMechanism] = {
    clientMechanisms map getSaslMechanism find (_.isDefined) flatten
  }
}
trait SaslMechanism {
  /**
   * The name of this mechanism (e.g. PLAIN)
   * @return the name
   */
  def name: String

  /**
   * Handle one round of challenge-response
   * @param challenge the challenge this round, or null on first round.
   * @param username name of user
   * @param password for username
   * @return response
   */
  def handleChallenge(challenge: LongString, username: String, password: String): LongString

  def handleResponse(response: Array[Byte]): Any
}

final class PlainMechanism extends SaslMechanism {
  def name = "PLAIN"

  private var isComplete = false

  def handleChallenge(challenge: LongString, username: String, password: String): LongString = {
    LongString.asLongString(s"\u0000${username}\u0000${password}")
  }

  def handleResponse(response: Array[Byte]) = {
    if (isComplete) {
      // return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, new IllegalStateException( "Multiple Authentications not permitted."));
    } else {
      isComplete = true
    }
    val authzidNullPos = findNullPosition(response, 0)
    if (authzidNullPos < 0) {
      //return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, new IllegalArgumentException( "Invalid PLAIN encoding, authzid null terminator not found"));
    }
    val authcidNullPos = findNullPosition(response, authzidNullPos + 1)
    if (authcidNullPos < 0) {
      //return new AuthenticationResult(AuthenticationResult.AuthenticationStatus.ERROR, new IllegalArgumentException( "Invalid PLAIN encoding, authcid null terminator not found"));
    }

    try {
      val username = new String(response, authzidNullPos + 1, authcidNullPos - authzidNullPos - 1, "UTF-8")
      // TODO: should not get pwd as a String but as a char array...
      val passwordLen = response.length - authcidNullPos - 1
      val password = new String(response, authcidNullPos + 1, passwordLen, "UTF-8")
      (username, password)
    } catch {
      case e: UnsupportedEncodingException =>
        throw new RuntimeException("JVM does not support UTF8", e)
    }

    //_usernamePasswordAuthenticationProvider.authenticate(_username, password);
  }

  private def findNullPosition(response: Array[Byte], startPosition: Int): Int = {
    var position = startPosition;
    while (position < response.length) {
      if (response(position) == 0) {
        return position
      }
      position += 1
    }
    -1
  }
}

final class ExternalMechanism extends SaslMechanism {
  def name = "EXTERNAL"

  def handleChallenge(challenge: LongString, username: String, password: String): LongString = {
    LongString.asLongString("")
  }

  def handleResponse(response: Array[Byte]) = ()
}
