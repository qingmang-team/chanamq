package chana.mq.amqp.server.engine

import java.util.concurrent.atomic.AtomicReference
import scala.collection
import scala.collection.mutable

trait TNode
trait Subscriber extends TNode
final case class Subscription(topic: String, subscriber: Subscriber)

trait QueueMatcher {

  /**
   * Subscribe adds the Subscriber to the topic and returns a Subscription.
   */
  def subscribe(topic: String, sub: Subscriber): Subscription

  /**
   * Unsubscribe removes the Subscription.
   */
  def unsubscribe(sub: Subscription)

  /**
   * Lookup returns the Subscribers for the given topic.
   */
  def lookup(topic: String): collection.Set[Subscriber]
}

final class DirectMatcher extends QueueMatcher {
  private val topicToSubscriptions = new mutable.HashMap[String, mutable.HashSet[Subscription]]()

  def subscribe(topic: String, sub: Subscriber) = {
    val subscription = Subscription(topic, sub)
    val subs = topicToSubscriptions.getOrElseUpdate(topic, new mutable.HashSet[Subscription]())
    subs += subscription
    subscription
  }

  def unsubscribe(sub: Subscription) {
    topicToSubscriptions.get(sub.topic) foreach { xs =>
      xs -= sub
    }
  }

  def lookup(topic: String): collection.Set[Subscriber] = {
    topicToSubscriptions.get(topic).map(xs => xs.map(_.subscriber)).getOrElse(Set())
  }
}

final class FanoutMatcher extends QueueMatcher {
  private val subscriptions = new mutable.HashSet[Subscription]()

  def subscribe(topic: String, sub: Subscriber) = {
    val subscription = Subscription(topic, sub)
    subscriptions += subscription
    subscription
  }

  def unsubscribe(sub: Subscription) {
    subscriptions -= sub
  }

  def lookup(topic: String): collection.Set[Subscriber] = {
    subscriptions.map(_.subscriber)
  }
}

object TrieMatcher {
  val DELIMITER = "\\."
  val WILDCARD = "*"
  val EMPTY = ""

  // --- simple test
  private case class TestSubscriber(name: String) extends Subscriber
  def main(args: Array[String]) {
    val m = new TrieMatcher()
    val s0 = TestSubscriber("s0")
    val s1 = TestSubscriber("s1")
    val s2 = TestSubscriber("s2")
    val s3 = TestSubscriber("tmp_541b184c-6213-487c-a254-f1a8e3d461ab")

    var res: collection.Set[Subscriber] = null

    val sub0 = m.subscribe("forex.*", s0)
    val sub1 = m.subscribe("*.usd", s0)
    val sub2 = m.subscribe("forex.eur", s0)
    val sub3 = m.subscribe("*.eur", s1)
    val sub4 = m.subscribe("forex.*", s1)
    val sub5 = m.subscribe("trade", s1)
    val sub6 = m.subscribe("*", s2)
    val sub7 = m.subscribe("quote", s3)

    // --- does not work
    //    val sub0 = Subscription("forex.*", TestSubscriber("s0"))
    //    val sub1 = Subscription("*.usd", TestSubscriber("s0"))
    //    val sub2 = Subscription("forex.eur", TestSubscriber("s0"))
    //    val sub3 = Subscription("*.eur", TestSubscriber("s1"))
    //    val sub4 = Subscription("forex.*", TestSubscriber("s1"))
    //    val sub5 = Subscription("trade", TestSubscriber("s1"))
    //    val sub6 = Subscription("*", TestSubscriber("s2"))
    //    val sub7 = Subscription("quote", TestSubscriber("tmp_541b184c-6213-487c-a254-f1a8e3d461ab"))

    println(m)

    res = m.lookup("forex.eur")
    assert(res.toList == List(s0, s1))
    res = m.lookup("forex")
    assert(res.toList == List(s2))
    res = m.lookup("trade.jpy")
    assert(res.toList == List())
    res = m.lookup("forex.jpy")
    assert(res.toList == List(s1, s0))
    res = m.lookup("trade")
    assert(res.toList == List(s1, s2))
    res = m.lookup("quote")
    println(res.toList)

    m.unsubscribe(sub0)
    m.unsubscribe(sub1)
    m.unsubscribe(sub2)
    m.unsubscribe(sub3)
    m.unsubscribe(sub4)
    m.unsubscribe(sub5)
    m.unsubscribe(sub6)

    println(m)
    res = m.lookup("forex.eur")
    assert(res.toList == List())
    res = m.lookup("forex")
    assert(res.toList == List())
    res = m.lookup("trade.jpy")
    assert(res.toList == List())
    res = m.lookup("forex.jpy")
    assert(res.toList == List())
    res = m.lookup("trade")
    assert(res.toList == List())

    //println(res.toList)
  }

  def newCSTrieMatcher(): TrieMatcher = {
    val root = INode(new AtomicReference(MainNode(CNode(mutable.Map()), null)))
    new TrieMatcher(root)
  }

  final case class INode(main: AtomicReference[MainNode])
  final case class MainNode(cNode: CNode, tNode: TNode)
  final case class Branch(subs: mutable.Map[Subscriber, AnyRef], var iNode: INode = null) { // TNode or _ ? 
    /**
     * updated returns a copy of this branch updated with the given I-node.
     */
    def updated(in: INode): Branch = {
      var subs = mutable.Map[Subscriber, AnyRef]()
      this.subs foreach {
        case (id, sub) =>
          subs += (id -> sub)
      }
      Branch(subs, in)
    }

    /**
     * removed returns a copy of this branch with the given Subscriber removed.
     */
    def removed(sub: Subscriber): Branch = {
      val subs = this.subs.filter(_._1 != sub)
      Branch(subs, this.iNode)
    }

    /**
     * subscribers returns the Subscribers for this branch.
     */
    def subscribers: collection.Set[Subscriber] = {
      this.subs.keySet
    }

  }

  final case class CNode(branches: mutable.Map[String, Branch]) {

    /**
     * inserted returns a copy of this C-node with the specified Subscriber inserted.
     */
    def inserted(words: Array[String], sub: Subscriber): CNode = {
      var branches = mutable.Map[String, Branch]()
      this.branches foreach {
        case (key, branch) =>
          branches += (key -> branch)
      }
      val br = if (words.length == 1) {
        Branch(mutable.Map(sub -> new AnyRef))
      } else {
        Branch(mutable.Map(), INode(new AtomicReference(MainNode(newCNode(words.slice(1, words.length), sub), null))))
      }
      branches += (words(0) -> br)
      CNode(branches)
    }

    /**
     * updated returns a copy of this C-node with the specified branch updated.
     */
    def updated(word: String, sub: Subscriber): CNode = {
      var branches = mutable.Map[String, Branch]()
      this.branches foreach {
        case (key, branch) =>
          branches += (key -> branch)
      }
      val newBranch = Branch(mutable.Map(sub -> new AnyRef))
      branches.get(word) match {
        case Some(br) =>
          br.subs foreach {
            case (id, sub) =>
              newBranch.subs += (id -> sub)
          }
          newBranch.iNode = br.iNode
        case None =>
      }
      branches += (word -> newBranch)
      CNode(branches)
    }

    /**
     * updatedBranch returns a copy of this C-node with the specified branch updated.
     */
    def updatedBranch(word: String, in: INode, br: Branch): CNode = {
      var branches = mutable.Map[String, Branch]()
      this.branches foreach {
        case (key, branch) =>
          branches += (key -> branch)
      }
      branches += (word -> br.updated(in))
      CNode(branches)
    }

    /**
     * removed returns a copy of this C-node with the Subscriber removed from the corresponding branch.
     */
    def removed(word: String, sub: Subscriber): CNode = {
      var branches = mutable.Map[String, Branch]()
      this.branches foreach {
        case (key, branch) =>
          branches += (key -> branch)
      }
      branches.get(word) match {
        case Some(br) =>
          val nbr = br.removed(sub)
          if (nbr.subs.size == 0 && nbr.iNode == null) {
            // remove the branch if it contains no subscribers and doesn't point anywhere.
            branches -= word
          } else {
            branches += (word -> nbr)
          }
        case None =>
      }
      CNode(branches)
    }

    /**
     * getBranches returns the branches for the given word. There are two possible branches:
     * exact match and single wildcard.
     */
    def getBranches(word: String): (Option[Branch], Option[Branch]) = {
      (this.branches.get(word), this.branches.get(WILDCARD))
    }

  }

  /**
   * newCNode creates a new C-node with the given subscription path.
   */
  def newCNode(words: Array[String], sub: Subscriber): CNode = {
    if (words.length == 1) {
      CNode(mutable.Map(words(0) -> Branch(mutable.Map(sub -> new AnyRef))))
    } else {
      val cNode = newCNode(words.slice(1, words.length), sub)
      val nin = INode(new AtomicReference(MainNode(cNode, null)))
      CNode(mutable.Map(words(0) -> Branch(mutable.Map(), nin)))
    }
  }

  /**
   * clean replaces an I-node's C-node with a copy that has any tombed I-nodes resurrected.
   */
  def clean(i: INode) {
    val main = i.main.get
    if (main.cNode != null) {
      i.main.compareAndSet(main, toCompressed(main.cNode))
    }
  }

  /**
   * cleanParent reads the main node of the parent I-node p and the current
   * I-node i and checks if the T-node below i is reachable from p. If i is no
   * longer reachable, some other thread has already completed the contraction.
   * If it is reachable, the C-node below p is replaced with its contraction.
   */
  def cleanParent(i: INode, parent: INode, parentsParent: INode, c: TrieMatcher, word: String) {
    val main = i.main.get
    val pMain = parent.main.get
    if (pMain.cNode != null) {
      pMain.cNode.branches.get(word) match {
        case Some(br) =>
          if (br.iNode != i) {
            return
          }
          if (main.tNode != null) {
            if (!contract(parentsParent, parent, i, c, pMain)) {
              cleanParent(parentsParent, parent, i, c, word)
            }
          }
        case None =>
      }
    }
  }

  /**
   * contract performs a contraction of the parent's C-node if possible. Returns
   * true if the contraction succeeded, false if it needs to be retried.
   */
  def contract(parentsParent: INode, parent: INode, i: INode, c: TrieMatcher, pMain: MainNode): Boolean = {
    val ncn = toCompressed(pMain.cNode)
    if (ncn.cNode.branches.size == 0 && parentsParent != null) {
      // if the compressed C-node has no branches, it and the I-node above it
      // should be removed. To do this, a CAS must occur on the parent I-node
      // of the parent to update the respective branch of the C-node below it
      // to point to nil.
      val ppMain = parentsParent.main.get
      for ((pKey, pBranch) <- ppMain.cNode.branches) {
        // find the branch pointing to the parent.
        if (pBranch.iNode == parent) {
          // update the branch to point to nil.
          val updated = ppMain.cNode.updatedBranch(pKey, null, pBranch)
          if (pBranch.subs.size == 0) {
            // if the branch has no subscribers, simply prune it.
            updated.branches -= pKey
          }
          // replace the main node of the parent's parent.
          return parentsParent.main.compareAndSet(ppMain, toCompressed(updated))
        }
      }
    } else {
      // otherwise, perform a simple contraction to a T-node.
      val cntr = c.toContracted(ncn.cNode, parent)
      val pMain = parent.main.get
      return parent.main.compareAndSet(pMain, cntr)
    }
    true
  }

  /**
   * toCompressed prunes any branches to tombed I-nodes and returns the
   * compressed main node.
   */
  def toCompressed(cn: CNode): MainNode = {
    var branches = mutable.Map[String, Branch]()
    cn.branches foreach {
      case (key, br) if !prunable(br) =>
        branches += (key -> br)
      case _ =>
    }
    MainNode(CNode(branches), null)
  }

  /**
   * prunable indicates if the branch can be pruned. A branch can be pruned if
   * it has no subscribers and points to nowhere or it has no subscribers and
   * points to a tombed I-node.
   */
  def prunable(br: Branch): Boolean = {
    if (br.subs.size > 0) {
      return false
    }
    if (br.iNode == null) {
      return true
    }
    val main = br.iNode.main.get
    main.tNode != null
  }

}
final class TrieMatcher(root: TrieMatcher.INode) extends QueueMatcher {
  import TrieMatcher._

  def this() = this(TrieMatcher.INode(new AtomicReference(TrieMatcher.MainNode(TrieMatcher.CNode(mutable.Map()), null))))

  def subscribe(topic: String, sub: Subscriber): Subscription = {
    val words = topic.split(DELIMITER)
    val root = this.root
    if (!iinsert(root, null, words, sub)) {
      subscribe(topic, sub)
    }
    Subscription(topic, sub)
  }

  def unsubscribe(sub: Subscription) {
    val words = sub.topic.split(DELIMITER)
    //	rootPtr = (*unsafe.Pointer)(unsafe.Pointer(&c.root))
    val root = this.root
    if (!this.iremove(root, null, null, words, 0, sub.subscriber)) {
      this.unsubscribe(sub)
    }
  }

  def lookup(topic: String): collection.Set[Subscriber] = {
    val words = topic.split(DELIMITER)
    //rootPtr = (*unsafe.Pointer)(unsafe.Pointer(&c.root))
    val root = this.root
    this.ilookup(root, null, words) match {
      case (result, true) => result
      case (_, false) =>
        //lookup(topic) // what is this?
        Set()
    }
  }

  def iinsert(i: INode, parent: INode, words: Array[String], sub: Subscriber): Boolean = {
    // linearization point.
    val main = i.main.get
    if (main.cNode != null) {
      val cn = main.cNode
      cn.branches.get(words(0)) match {
        case None =>
          // if the relevant branch is not in the map, a copy of the C-node
          // with the new entry is created. The linearization point is a
          // successful CAS.
          val ncn = MainNode(cn.inserted(words, sub), null)
          return i.main.compareAndSet(main, ncn)
        case Some(br) =>
          // if the relevant key is present in the map, its corresponding
          // branch is read.
          if (words.length > 1) {
            // if more than 1 word is present in the path, the tree must be
            // traversed deeper.
            if (br.iNode != null) {
              // if the branch has an I-node, iinsert is called recursively.
              return iinsert(br.iNode, i, words.slice(1, words.length), sub)
            }
            // otherwise, an I-node which points to a new C-node must be
            // added. The linearization point is a successful CAS.
            val nin = INode(new AtomicReference(MainNode(newCNode(words.slice(1, words.length), sub), null)))
            val ncn = MainNode(cn.updatedBranch(words(0), nin, br), null)
            return i.main.compareAndSet(main, ncn)
          }
          br.subs.get(sub) match {
            case Some(_) =>
              // already subscribed.
              return true
            case _ =>
              // insert the Subscriber by copying the C-node and updating the
              // respective branch. The linearization point is a successful CAS.
              val ncn = MainNode(cn.updated(words(0), sub), null)
              return i.main.compareAndSet(main, ncn)
          }
      }
    } else if (main.tNode != null) {
      clean(parent)
      return false
    } else {
      throw new RuntimeException("csTrie is in an invalid state")
    }
  }

  def iremove(i: INode, parent: INode, parentsParent: INode, words: Array[String], wordIdx: Int, sub: Subscriber): Boolean = {

    // linearization point.
    val main = i.main.get

    if (main.cNode != null) {
      val cn = main.cNode
      cn.branches.get(words(wordIdx)) match {
        case None =>
          // if the relevant word is not in the map, the subscription doesn't exist.
          return true
        case Some(br) =>
          // if the relevant word is present in the map, its corresponding
          // branch is read.
          if (wordIdx + 1 < words.length) {
            // if more than 1 word is present in the path, the tree must be
            // traversed deeper.
            if (br.iNode != null) {
              // if the branch has an I-node, iremove is called recursively.
              return this.iremove(br.iNode, i, parent, words, wordIdx + 1, sub)
            }
            // otherwise, the subscription doesn't exist.
            return true
          }
          br.subs.get(sub) match {
            case None =>
              // not subscribed.
              return true
            case Some(_) =>
              // remove the Subscriber by copying the C-node without it. A
              // contraction of the copy is then created. A successful CAS will
              // substitute the old C-node with the copied C-node, thus removing
              // the Subscriber from the trie - this is the linearization point.
              val ncn = cn.removed(words(wordIdx), sub)
              val cntr = toContracted(ncn, i)
              if (i.main.compareAndSet(main, cntr)) {
                if (parent != null) {
                  val main = i.main.get
                  if (main.tNode != null) {
                    cleanParent(i, parent, parentsParent, this, words(wordIdx - 1))
                  }
                }
                return true
              }
              return false
          }

      }
    } else if (main.tNode != null) {
      clean(parent)
      return false
    } else {
      throw new RuntimeException("csTrie is in an invalid state")
    }
  }

  /**
   * ilookup attempts to retrieve the Subscribers for the word path. True is
   * returned if the Subscribers were retrieved, false if the operation needs to
   * be retried.
   */
  def ilookup(i: INode, parent: INode, words: Array[String]): (Set[Subscriber], Boolean) = {
    // Linearization point.
    val main = i.main.get
    if (main.cNode != null) {
      // traverse exact-match branch and single-word-wildcard branch.
      val (exact, singleWC) = main.cNode.getBranches(words(0))
      //println(s"word(0) ${words(0)}")
      //println(s"cnode branches: ${main.cNode.branches}")
      //println(s"exact $exact, singleWC $singleWC")
      var subs = Map[Subscriber, AnyRef]()
      exact match {
        case Some(x) =>
          this.bLookup(i, parent, main, x, words) match {
            case (_, false) => return (null, false)
            case (s, true) =>
              s.foreach { sub =>
                subs += (sub -> new AnyRef)
              }
          }
        case None =>
      }
      singleWC match {
        case Some(x) =>
          this.bLookup(i, parent, main, x, words) match {
            case (_, false) => return (null, false)
            case (s, ok) =>
              s foreach { sub =>
                subs += (sub -> new AnyRef)
              }
          }
        case _ =>
      }
      return (subs.keySet, true)
    } else if (main.tNode != null) {
      clean(parent)
      return (null, false)
    } else {
      throw new RuntimeException("csTrie is in an invalid state")
    }
  }

  /**
   * bLookup attempts to retrieve the Subscribers from the word path along the
   * given branch. True is returned if the Subscribers were retrieved, false if
   * the operation needs to be retried.
   */
  def bLookup(i: INode, parent: INode, main: MainNode, b: Branch, words: Array[String]): (collection.Set[Subscriber], Boolean) = {
    if (words.length > 1) {
      // if more than 1 key is present in the path, the tree must be
      // traversed deeper.

      if (b.iNode == null) {
        // if the branch doesn't point to an I-node, no subscribers exist.
        (Set(), true)
      } else {
        // if the branch has an I-node, ilookup is called recursively.
        this.ilookup(b.iNode, i, words.slice(1, words.length))
      }
    } else {
      // retrieve the subscribers from the branch.
      (b.subscribers, true)
    }
  }

  /**
   * toContracted ensures that every I-node except the root points to a C-node
   * with at least one branch or a T-node. If a given C-node has no branches and
   * is not at the root level, a T-node is returned.
   */
  def toContracted(cn: CNode, parent: INode): MainNode = {
    if (this.root != parent && cn.branches.size == 0) {
      MainNode(null, new TNode {})
    } else {
      MainNode(cn, null)
    }
  }

  override def toString = root.toString
}
