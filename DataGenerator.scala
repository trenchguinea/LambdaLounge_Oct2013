import io.Source
import collection.SortedSet
import java.io._
import java.math.BigDecimal
import java.util.Random

case class Customer(id: String, email: String)
case class Purchase(retailer: String, customer: Customer, cost: BigDecimal)

def getTokens(fileName: String, tokenPos: Int): Iterator[String] = {
  for (line <- Source.fromFile(fileName).getLines) yield line.split("\t")(tokenPos).trim
}

def camelCase(s: String) = Character.toUpperCase(s.head) + s.tail.toLowerCase

def writePurchases(purchases: Iterable[Purchase], fileName: String) {
  
  println("Writing purchases to " + fileName)

  val writer = new PrintWriter(new BufferedWriter(new FileWriter(fileName)))
  try {
    purchases.foreach { purchase =>
      writer.println(purchase.customer.id + "," + purchase.customer.email + "," + purchase.retailer + "," + purchase.cost)
    }
  } finally {
    writer.close()
  }
}

def encodeName(s: String): String = s.filter(Character.isLetterOrDigit)


val customers: List[Customer] = (for {
  firstName <- getTokens("firstNames.txt", 0)
  lastName <- getTokens("lastNames.txt", 0).toSet[String] // Must convert to something that allows for iterating through multiple times
} yield Customer(camelCase(firstName) + " " + camelCase(lastName), firstName.toLowerCase + "." + lastName.toLowerCase + "@gmail.com")).toList

def retailers: Iterator[String] = getTokens("retailerSource.txt", 1)

val costRnd = new Random(5)
val shopCntRnd = new Random(6)

retailers.foreach { retailer =>

  println("Building purchases for " + retailer)

  val purchases = for {
    customer <- customers
    shopCnt <- 1 to (shopCntRnd.nextInt(3) + 1)
  } yield Purchase(retailer, customer, BigDecimal.valueOf(costRnd.nextInt(25000), 2))

  writePurchases(purchases, "Purchases/" + encodeName(retailer) + ".txt")
}