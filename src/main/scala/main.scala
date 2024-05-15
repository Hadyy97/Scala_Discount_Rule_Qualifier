import scala.io.Source
import java.time.{Duration, Instant, LocalDate}
import java.time.format.DateTimeFormatter
import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.time.temporal.ChronoUnit

object QualifierEngine extends App{

  /* -------------------------------- Functions -------------------------------- */

  // Function to log events with timestamp, log level, and message
  def logEvent(logWriter: PrintWriter, file: File, logLevel: String, message: String): Unit = {
    logWriter.write(s"Timestamp: ${Instant.now()}\tLogLevel: ${logLevel}\tMessage: ${message}\n")
    logWriter.flush()
  }

  // Function to log order processing events
  def logOrder(logWriter: PrintWriter, file: File, status: String): Unit = {
    logEvent(logWriter, file, "Info/Debug", s"${status} processing an order")
  }

  // Function to log qualification events
  def logQualification(logWriter: PrintWriter, file: File, status: String): Unit = {
    logEvent(logWriter, file, "Info", s"${status} qualification")
  }

  // Function to log calculated discount
  def logDiscount(logWriter: PrintWriter, file: File, value: Double): Unit = {
    logEvent(logWriter, file, "Info", s"calculated discount: ${value}")
  }

  // Function to log order processing duration
  def logOrderProcessDuration(logWriter: PrintWriter, file: File, value: Long): Unit = {
    logEvent(logWriter, file, "Debug", s"Time taken to process order: ${value}ms")
  }

  // Function to convert string date to LocalDate
  def strDateConverter(dateString: String, pattern: String) = {
    val formatter = DateTimeFormatter.ofPattern(pattern)
    LocalDate.parse(dateString, formatter)
  }

  // Function to split product name and category
  def splitProductNameCategory(productName: String): (String,String) = {
    val nameParts = productName.split("-")
    if (nameParts.length > 1) (nameParts(0).trim, productName)
    else ("Unknown", nameParts(0))
  }

  // Function to process order and extract relevant information
  def processOrder(order: String) = {
    val orderParts = order.split(",")
    val orderDate = strDateConverter(orderParts(0), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    val expiryDate = strDateConverter(orderParts(2), "yyyy-MM-dd")
    val daysToExpiry = ChronoUnit.DAYS.between(orderDate, expiryDate).toInt
    val (productCategory, productName) = splitProductNameCategory(orderParts(1))
    val quantity = orderParts(3).toInt
    val unitPrice = orderParts(4).toDouble
    val channel = orderParts(5)
    val paymentMethod = orderParts(6)
    (orderDate,expiryDate, daysToExpiry, productCategory, productName, quantity,
      unitPrice, channel, paymentMethod)
  }

  // Functions to qualify orders based on different criteria
  def qualify23March(order: String): Boolean = {
    val date = processOrder(order)._1
    date.getDayOfMonth == 23 && date.getMonthValue == 3
  }

  def qualifyExpiryDays(order: String): Boolean = {
    val daysToExpiry = processOrder(order)._3
    daysToExpiry < 30
  }

  def qualifyCategory(order: String): Boolean = {
    val productCategory = processOrder(order)._4
    productCategory match {
      case "Wine" => true
      case "Cheese" => true
      case _ => false
    }
  }

  def qualifyVisa(order: String): Boolean = {
    val channel = processOrder(order)._9
    channel == "Visa"
  }

  def qualifyQuantity(order: String): Boolean = {
    val quantity = processOrder(order)._6
    quantity > 5
  }

  def qualifyApp(order: String): Boolean = {
    val channel = processOrder(order)._8
    channel == "App"
  }

  // Functions to calculate discounts based on different criteria
  def discount23March(order: String): Double = {
    0.5
  }

  def discountExpiryDays(order: String): Double = {
    val daysToExpiry = processOrder(order)._3
    (30.0 - daysToExpiry) / 100.0
  }

  def discountCategory(order: String): Double = {
    val productCategory = processOrder(order)._4
    productCategory match {
      case "Wine" => 0.05
      case "Cheese" => 0.1
      case _ => 0.0
    }
  }

  def discountVisa(order: String): Double = {
    0.05
  }

  def discountQuantity(order: String): Double = {
    val quantity = processOrder(order)._6
    quantity match {
      case x if x < 10 => 0.05
      case x if x < 15 => 0.07
      case _ => 0.1
    }
  }

  def discountApp(order: String): Double = {
    val channel = processOrder(order)._8
    val quantity = processOrder(order)._6

    def helper(qunatity: Int): Double = {
      val remainder = qunatity % 5
      if (remainder == 0) {
        qunatity / 100.0
      } else {
        (quantity + (5 - remainder)) / 100.0
      }
    }

    helper(quantity)
  }

  // List of qualification and discount functions
  val qualifyDiscountMap: List[((String) => Boolean, (String) => Double)] = List(
    (qualifyQuantity, discountQuantity),
    (qualifyCategory, discountCategory),
    (qualify23March, discount23March),
    (qualifyExpiryDays, discountExpiryDays),
    (qualifyApp, discountApp),
    (qualifyVisa, discountVisa)
  )

  // Function to apply rules, calculate discounts, and log events
  def ruleApplier(order: String, rules: List[((String) => Boolean, (String) => Double)], logWriter: PrintWriter, logFile: File): Double = {
    val startTimestamp = Instant.now()
    logOrder(logWriter, logFile, "Started")

    def applyRules(rules: List[((String) => Boolean, (String) => Double)], acc: List[Double]): List[Double] = {
      rules match {
        case Nil => acc
        case (condition, discount) :: tail =>
          if (condition(order)) {
            logQualification(logWriter, logFile, "Successful")
            val appliedDiscount = discount(order)
            logDiscount(logWriter, logFile, appliedDiscount)
            applyRules(tail, appliedDiscount :: acc)
          } else {
            logQualification(logWriter, logFile, "Failed")
            applyRules(tail, acc)
          }
      }
    }

    val discounts = applyRules(rules, Nil)
    // Select the top two discounts and calculate their average
    val topTwoDiscounts = discounts.sorted.reverse.take(2)
    val avgDiscount = if (topTwoDiscounts.nonEmpty) average(topTwoDiscounts) else 0
    logOrder(logWriter, logFile, "Ended")
    val endTimestamp = Instant.now()
    val orderProcessingDuration = Duration.between(startTimestamp, endTimestamp).toMillis
    logOrderProcessDuration(logWriter, logFile, orderProcessingDuration)

    if (avgDiscount > 0) avgDiscount else 0
  }

  // Function to calculate average of a list of numbers
  def average(nums: List[Double]): Double = {
    BigDecimal(nums.sum / nums.length).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  // Function to prepare data for writing to database
  def prepareForWriting(order: String, rules: List[((String) => Boolean, (String) => Double)], logWriter: PrintWriter, logFile: File) = {
    val initialProcessing = processOrder(order)
    val discount = ruleApplier(order, rules, logWriter, logFile)
    val totalDue = (initialProcessing._7 * initialProcessing._6) -
      (discount * initialProcessing._7 * initialProcessing._6)
    (
      initialProcessing._1,
      initialProcessing._2,
      initialProcessing._3,
      initialProcessing._4,
      initialProcessing._5,
      initialProcessing._6,
      initialProcessing._7,
      initialProcessing._8,
      initialProcessing._9,
      discount,
      totalDue
    )
  }

  // Function to write data to database
  def writeToDB(orderLines: List[String], rules: List[((String) => Boolean, (String) => Double)], logWriter: PrintWriter, logFile: File): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    val url = "jdbc:oracle:thin:@//localhost:1521/XE"
    val username = "HADY"
    val password = "123"

    val data = orderLines.map(
      order =>
        prepareForWriting(order, rules, logWriter, logFile)
    )

    val insertStatement =
      """
        |INSERT INTO orderLines (order_date, expiry_date, days_to_expiry, product_category,
        |                   product_name, quantity, unit_price, channel, payment_method,
        |                   discount, total_due)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver")
      connection = DriverManager.getConnection(url, username, password)
      logEvent(logWriter, logFile, "Debug", "Successfully Connected to Database")
      preparedStatement = connection.prepareStatement(insertStatement)

      data.foreach { case (orderDate, expiryDate, daysToExpiry, productCategory, productName, quantity,
      unitPrice, channel, paymentMethod, discount, totalDue) =>
        preparedStatement.setDate(1, Date.valueOf(orderDate.toString))
        preparedStatement.setDate(2, Date.valueOf(expiryDate.toString))
        preparedStatement.setInt(3, daysToExpiry)
        preparedStatement.setString(4, productCategory)
        preparedStatement.setString(5, productName)
        preparedStatement.setInt(6, quantity)
        preparedStatement.setDouble(7, unitPrice)
        preparedStatement.setString(8, channel)
        preparedStatement.setString(9, paymentMethod)
        preparedStatement.setDouble(10, discount)
        preparedStatement.setDouble(11, totalDue)

        preparedStatement.addBatch()
      }

      preparedStatement.executeBatch()
    } catch {
      case e: Exception =>
        logEvent(logWriter, logFile, "Error", s"Failed to close preparedStatement: ${e.getMessage}")
    } finally {
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
      logEvent(logWriter, logFile, "Info", "Successfully inserted into database")
      logEvent(logWriter, logFile, "Debug", "Closed database connection")
    }
  }

  // File for logging
  val logFile: File = new File("src/main/resources/logs.log")
  // PrintWriter for writing logs
  val logWriter = new PrintWriter(new FileOutputStream(logFile, true))
  // Start time of the program
  val programStartTime = Instant.now()
  // Log program start
  logEvent(logWriter, logFile, "Info/Debug", "Program Started")
  // Read order lines from file
  val orderLines = Source.fromFile("src/main/resources/TRX1000.csv").getLines().toList.tail
  // Write order data to database
  writeToDB(orderLines, qualifyDiscountMap, logWriter, logFile)
  // End time of the program
  val programWorkDuration = Duration.between(programStartTime, Instant.now()).toMillis
  // Log program finish
  logEvent(logWriter, logFile, "Info/Debug", "Program Finished")
  // Log program duration
  logEvent(logWriter, logFile, "Debug", s"Program took ${programWorkDuration}ms to run")
}
