import annotation.tailrec
import java.io._
import io.Source
import collection.parallel._
import math.BigDecimal
import math.BigDecimal.RoundingMode

// Processes only 1 letter at a time but repeatedly parses the files in order to do so.
// This uses a lot less memory than the other approaches but is MUCH slower and greatly reduces parallelism.

case class Purchase(email: String, price: BigDecimal)

object GrouperAppByLetter {
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
    val startTotalTime = System.currentTimeMillis

    val maxPurchaserByLetters = for (letter <- ALPHA_RANGE) yield {

      println("Finding purchases for letter " + letter + "...")
      val purchasesByLetterByFile = getPurchasesFromFiles(letter)
      val purchasesByLetterAcrossFiles = purchasesByLetterByFile.reduce(_ ++ _)

      println("Calculating averages for those purchases...")
      val startTime2 = System.currentTimeMillis
      val avgs = purchasesByLetterAcrossFiles.toIterable.par.groupBy(_.email).mapValues(avgPurchases)
      val endTime2 = System.currentTimeMillis

      println("Finding biggest shopper for this letter...")
      findMaxPurchaser(avgs)
    }

    println("Finding biggest shopper across all letters...")
    val startTime4 = System.currentTimeMillis
    val biggestPurchaser = maxPurchaserByLetters.maxBy(_._2)
    val endTime4 = System.currentTimeMillis

    println("The biggest shopper is " + biggestPurchaser._1 + " with an average spend of " + biggestPurchaser._2)

    println("Total time = " + (endTime4 - startTotalTime) + "ms")
  }

  /** Returns sequence where each item in the sequence contains the Purchases within that file with an email that starts with the given letter. */
  def getPurchasesFromFiles(letter: Char): ParSeq[Iterator[Purchase]] =
    purchaseFiles.take(FILES_TO_READ).par.map(file => getPurchasesFromFile(file, letter))

  /**
   * Get all purchases from the given file for people who's email starts with the given letter.
   * Purchases are streamed in as needed.
   */
  def getPurchasesFromFile(file: File, letter: Char): Iterator[Purchase] = {
    val purchases = Source.fromFile(file).getLines.map { s =>
      val tokens = s.split(',')
      Purchase(emailsFromFile(tokens(1)), BigDecimal(tokens(3)))
    }
    // Use dropWhile/takewhile instead of something like filter to take advantage of the fact that the data is sorted A to Z and only parse what's necessary. */
    purchases.dropWhile(!_.email.startsWith(letter.toString)).takeWhile(_.email.startsWith(letter.toString))
  }

  /** Averages the given sequence of purchases. */
  def avgPurchases(purchases: ParIterable[Purchase]): BigDecimal =
    (purchases.map(_.price).sum / purchases.size).setScale(2, RoundingMode.HALF_UP)

  /** From all average purchases, find the shopper who spent the most on average. */
  def findMaxPurchaser(avgs: ParMap[String, BigDecimal]) = {
    if (!avgs.isEmpty) avgs.maxBy(_._2) else ("", BigDecimal(java.math.BigDecimal.ZERO))
  }

}