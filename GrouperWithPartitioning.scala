import annotation.tailrec
import java.io._
import io.Source
import collection.parallel._
import math.BigDecimal
import math.BigDecimal.RoundingMode

// Implements a complicated algorithm that attempts to, in parallel, stream in only the Purchases that have an email that starts with the same letter.
// The idea here is to greatly reduce the size of the Iterable that is created in order to do the groupBy.
// Unfortunately, something is triggering the loading of all contents into a large LinkedList anyway. It is faster, but still uses the same amount of memory.

case class Purchase(email: String, price: BigDecimal)

object GrouperAppWithPartitioning {
  private val FILES_TO_READ = 25
  private val ALPHA_RANGE = ('a' to 'z')

  private val purchaseFiles = new File("Purchases").listFiles.toSeq.filterNot(_.getName.equals(".DS_Store"))
  private val emailsFromFile = getEmailsFromFile(purchaseFiles(0))

  /** Pull all the e-mails out into a lookup table to save memory later on by reusing the same string instances. */
  def getEmailsFromFile(file: File): Map[String, String] = {
    val emailTuples = Source.fromFile(file).getLines.map { s =>
      val email = new String(s.split(',')(1))
      (email, email)
    }
    emailTuples.toMap
  }

  def main(args: Array[String]) {
    println("Partitioning across files into letters from A to Z...")
    val startTime = System.currentTimeMillis
    val partitioned = partitionedAcrossFiles(filePartitions)
    val endTime = System.currentTimeMillis

    println("Calculating average purchase for each person in all files...")
    val startTime2 = System.currentTimeMillis
    val avgs = partitioned.map(_.toIterable.par.groupBy(_.email).mapValues(avgPurchases))
    val endTime2 = System.currentTimeMillis

    println("Finding biggest shopper...")
    val startTime3 = System.currentTimeMillis
    val biggestPurchaser = findMaxPurchaser(avgs)
    val endTime3 = System.currentTimeMillis

    println("The biggest shopper is " + biggestPurchaser._1 + " with an average spend of " + biggestPurchaser._2)

    println("Time to partitionAcrossFiles = " + (endTime - startTime) + "ms")
    println("Time to find avgs = " + (endTime2 - startTime2) + "ms")
    println("Time to find biggest shopper = " + (endTime3 - startTime3) + "ms")
    println("Total time = " + (endTime3 - startTime) + "ms")
  }

  /** Combines each partition in each file into a sequence of combined partitions, where each item iterates over all files within that letter partition. */
  def partitionedAcrossFiles(parts: ParSeq[Seq[Iterator[Purchase]]]): Seq[Iterator[Purchase]] = {
    for (alphabetIdx <- 0 until ALPHA_RANGE.size)
      yield parts.map(_(alphabetIdx)).reduce(_ ++ _)
  }

  /** Returns sequence where each item in the sequence contains the Purchases within that file, partitioned by A to Z. */
  def filePartitions: ParSeq[Seq[Iterator[Purchase]]] =
    purchaseFiles.take(FILES_TO_READ).par.map(file => partition(getPurchasesFromFile(file)))

  /** Get all purchases from the given file. Purchases are streamed in as needed. */
  def getPurchasesFromFile(file: File): Iterator[Purchase] =
    Source.fromFile(file).getLines.map { s =>
      val tokens = s.split(',')
      Purchase(emailsFromFile(tokens(1)), BigDecimal(tokens(3)))
    }

  /** Returns the purchases in a file partitioned by letters A to Z. */
  def partition(initialPurchases: Iterator[Purchase]): Seq[Iterator[Purchase]] = {

    @tailrec
    def partitionByLetter(purchases: Iterator[Purchase], acc: Seq[Iterator[Purchase]], letters: Seq[Char]): Seq[Iterator[Purchase]] = {
      if (letters.isEmpty || purchases.isEmpty) acc
      else {
        val (thoseMatchingLetter, thoseNotMatchingLetter) = purchases.span(_.email.startsWith(letters.head.toString))
        partitionByLetter(thoseNotMatchingLetter, acc :+ thoseMatchingLetter, letters.tail)
      }
    }
    
    partitionByLetter(initialPurchases, Vector(), ALPHA_RANGE)
  }

  /** Averages the given sequence of purchases. */
  def avgPurchases(purchases: ParIterable[Purchase]): BigDecimal =
    (purchases.map(_.price).sum / purchases.size).setScale(2, RoundingMode.HALF_UP)

  /** From all average purchases, find the shopper who spent the most on average. */
  def findMaxPurchaser(allAvgs: Seq[ParMap[String, BigDecimal]]): (String, BigDecimal) = {
    val largestByLetter = for (avgByLetter <- allAvgs if !avgByLetter.isEmpty) yield avgByLetter.maxBy(_._2)
    largestByLetter.maxBy(_._2)
  }

}