import annotation.tailrec
import java.io._
import io.Source
import collection.parallel._
import math.BigDecimal
import math.BigDecimal.RoundingMode

// Implements a straight forward algorithm that groups all purchases from all files by the e-mail to find the max average.
// Requires loading contents of all files into memory.

case class Purchase(email: String, price: BigDecimal)

object GrouperAppNoPartitioning {
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
    println("Calculating average purchase for each person in all files...")
    val startTime = System.currentTimeMillis
    val avgs = filePurchases.reduce(_ ++ _).toIterable.par.groupBy(_.email).mapValues(avgPurchases)
    val endTime = System.currentTimeMillis

    println("Finding biggest shopper...")
    val startTime2 = System.currentTimeMillis
    val biggestPurchaser = findMaxPurchaser(avgs)
    val endTime2 = System.currentTimeMillis

    println("The biggest shopper is " + biggestPurchaser._1 + " with an average spend of " + biggestPurchaser._2)

    println("Time to find avgs = " + (endTime - startTime) + "ms")
    println("Time to find biggest shopper = " + (endTime2 - startTime2) + "ms")
    println("Total time = " + (endTime2 - startTime) + "ms")
  }

  /** Returns sequence where each item in the sequence contains the Purchases within that file. */
  def filePurchases: ParSeq[Iterator[Purchase]] = purchaseFiles.take(FILES_TO_READ).par.map(getPurchasesFromFile)

  /** Get all purchases from the given file. Purchases are streamed in as needed. */
  def getPurchasesFromFile(file: File): Iterator[Purchase] =
    Source.fromFile(file).getLines.map { s =>
      val tokens = s.split(',')
      Purchase(emailsFromFile(tokens(1)), BigDecimal(tokens(3)))
    }

  /** Averages the given sequence of purchases. */
  def avgPurchases(purchases: ParIterable[Purchase]): BigDecimal =
    (purchases.map(_.price).sum / purchases.size).setScale(2, RoundingMode.HALF_UP)

  /** From all average purchases, find the shopper who spent the most on average. */
  def findMaxPurchaser(allAvgs: ParMap[String, BigDecimal]) = {
    allAvgs.maxBy(_._2)
  }

}