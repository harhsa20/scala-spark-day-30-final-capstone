
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object BankingBatch {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Real-Time Banking Analytics - Batch")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("\n========== 1. READ TRANSACTIONS ==========")

    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/raw/transactions.csv")

    val accounts = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/reference/accounts.csv")

    val branches = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/reference/branches.csv")

    transactions.show(false)

    println("\n========== 2. DATA VALIDATION ==========")

    val invalidRecords = spark.sparkContext.longAccumulator(
      "Invalid Transaction Records"
    )

    val checkedTransactions = transactions.rdd.map { row =>
      val amount = try {
        Option(row.getAs[Any]("amount"))
          .map(_.toString.toDouble)
          .getOrElse(0.0)
      } catch {
        case _: Exception => 0.0
      }

      val transactionId = Option(row.getAs[String]("transaction_id"))
      val accountId = Option(row.getAs[String]("account_id"))
      val branchId = Option(row.getAs[String]("branch_id"))
      val transactionType =
        Option(row.getAs[String]("transaction_type"))

      val valid =
        transactionId.exists(_.nonEmpty) &&
        accountId.exists(_.nonEmpty) &&
        branchId.exists(_.nonEmpty) &&
        amount > 0 &&
        transactionType.exists(t =>
          t == "DEBIT" || t == "CREDIT"
        )

      if (!valid) invalidRecords.add(1L)

      (row, valid)
    }

    checkedTransactions.count()

    println(
      s"Invalid transaction records: ${invalidRecords.value}"
    )

    val validTransactions = transactions
      .filter(
        col("transaction_id").isNotNull &&
        col("account_id").isNotNull &&
        col("branch_id").isNotNull &&
        col("amount") > 0 &&
        col("transaction_type").isin("DEBIT", "CREDIT") &&
        to_date(col("transaction_date"), "yyyy-MM-dd").isNotNull
      )
      .withColumn(
        "transaction_date",
        to_date(col("transaction_date"), "yyyy-MM-dd")
      )

    println("\n========== 3. BROADCAST JOINS ==========")

    val enriched = validTransactions
      .join(broadcast(accounts), Seq("account_id"), "left")
      .join(broadcast(branches), Seq("branch_id"), "left")

    println("\n========== 4. UDF RISK CLASSIFICATION ==========")

    val riskUDF = udf((amount: Double) => {
      if (amount >= 100000) "HIGH"
      else if (amount >= 50000) "MEDIUM"
      else "LOW"
    })

    val classified = enriched
      .withColumn("amount_risk", riskUDF(col("amount").cast("double")))
      .withColumn(
        "signed_amount",
        when(col("transaction_type") === "CREDIT", col("amount"))
          .otherwise(-col("amount"))
      )

    println("\n========== 5. CACHE / PERSIST ==========")

    val persisted = classified.persist(StorageLevel.MEMORY_AND_DISK)

    println(s"Valid transactions: ${persisted.count()}")

    println("\n========== 6. WINDOW FUNCTIONS ==========")

    val accountWindow = Window
      .partitionBy("account_id")
      .orderBy("transaction_date", "transaction_id")
      .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
      )

    val windowResults = persisted
      .withColumn(
        "running_net_amount",
        sum(col("signed_amount")).over(accountWindow)
      )
      .withColumn(
        "account_transaction_number",
        row_number().over(
          Window.partitionBy("account_id")
            .orderBy("transaction_date", "transaction_id")
        )
      )

    windowResults.select(
      "transaction_id",
      "account_id",
      "customer_name",
      "transaction_date",
      "transaction_type",
      "amount",
      "amount_risk",
      "running_net_amount",
      "account_transaction_number"
    ).show(false)

    println("\n========== 7. SPARK SQL ANALYTICS ==========")

    persisted.createOrReplaceTempView("bank_transactions")

    val accountSummary = spark.sql("""
      SELECT
        account_id,
        customer_name,
        COUNT(*) AS transaction_count,
        ROUND(SUM(amount), 2) AS total_transaction_amount,
        ROUND(AVG(amount), 2) AS average_transaction_amount,
        MAX(amount) AS largest_transaction
      FROM bank_transactions
      GROUP BY account_id, customer_name
      ORDER BY total_transaction_amount DESC
    """)

    accountSummary.show(false)

    println("\n========== 8. BRANCH-WISE AGGREGATION ==========")

    persisted
      .groupBy("branch_id", "branch_name", "branch_risk")
      .agg(
        count("*").as("transaction_count"),
        sum("amount").as("total_amount"),
        max("amount").as("largest_transaction")
      )
      .orderBy(desc("total_amount"))
      .show(false)

    println("\n========== 9. DAILY TRANSACTION SUMMARY ==========")

    persisted
      .groupBy("transaction_date")
      .agg(
        count("*").as("transaction_count"),
        sum("amount").as("total_amount")
      )
      .orderBy("transaction_date")
      .show(false)

    println("\n========== 10. PHYSICAL EXECUTION PLAN ==========")

    accountSummary.explain("formatted")

    println("\n========== BATCH PROCESSING COMPLETED ==========")

    persisted.unpersist()
    spark.stop()
  }
}
