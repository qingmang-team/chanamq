package chana.mq.amqp.model

object ErrorCodes {
  /** Indicates that the method completed successfully. */
  val REPLY_SUCCESS = 200

  /**
   * The client asked for a specific message that is no longer available. The message was delivered to another
   * client, or was purged from the queue for some other reason.
   */
  val NOT_DELIVERED = 310

  /**
   * The client attempted to transfer content larger than the server could accept at the present time.  The client
   * may retry at a later time.
   */
  val MESSAGE_TOO_LARGE = 311

  /**
   * When the exchange cannot route the result of a .Publish, most likely due to an invalid routing key. Only when
   * the mandatory flag is set.
   */
  val NO_ROUTE = 312

  /**
   * When the exchange cannot deliver to a consumer when the immediate flag is set. As a result of pending data on
   * the queue or the absence of any consumers of the queue.
   */
  val NO_CONSUMERS = 313

  /**
   * An operator intervened to close the connection for some reason. The client may retry at some later date.
   */
  val CONNECTION_FORCED = 320

  /** The client tried to work with an unknown virtual host or cluster. */
  val INVALID_PATH = 402

  /** The client attempted to work with a server entity to which it has no access due to security settings. */
  val ACCESS_REFUSED = 403

  /** The client attempted to work with a server entity that does not exist. */
  val NOT_FOUND = 404

  /**
   * The client attempted to work with a server entity to which it has no access because another client is
   * working with it.
   */
  val ALREADY_EXISTS = 405

  /** The client requested a method that was not allowed because some precondition failed. */
  val IN_USE = 406

  val INVALID_ROUTING_KEY = 407

  val REQUEST_TIMEOUT = 408

  val ARGUMENT_INVALID = 409

  /**
   * The client sent a malformed frame that the server could not decode. This strongly implies a programming error
   * in the client.
   */
  val FRAME_ERROR = 501

  /**
   * The client sent a frame that contained illegal values for one or more fields. This strongly implies a
   * programming error in the client.
   */
  val SYNTAX_ERROR = 502

  /**
   * The client sent an invalid sequence of frames, attempting to perform an operation that was considered invalid
   * by the server. This usually implies a programming error in the client.
   */
  val COMMAND_INVALID = 503

  /**
   * The client attempted to work with a channel that had not been correctly opened. This most likely indicates a
   * fault in the client layer.
   */
  val CHANNEL_ERROR = 504

  /**
   * The server could not complete the method because it lacked sufficient resources. This may be due to the client
   * creating too many of some type of entity.
   */
  val RESOURCE_ERROR = 506

  /**
   * The client tried to work with some entity in a manner that is prohibited by the server, due to security settings
   * or by some other criteria.
   */
  val NOT_ALLOWED = 530

  /** The client tried to use functionality that is not implemented in the server. */
  val NOT_IMPLEMENTED = 540

  /**
   * The server could not complete the method because of an internal error. The server may require intervention by
   * an operator in order to resume normal operations.
   */
  val INTERNAL_ERROR = 541

  val INVALID_ARGUMENT = 542

  /**
   * The client impl does not support the protocol version
   */
  val UNSUPPORTED_CLIENT_PROTOCOL_ERROR = 543
}
