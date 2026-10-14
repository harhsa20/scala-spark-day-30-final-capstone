
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BankingStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Real-Time Banking Streaming")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")

    // Stateful streaming requires checkpointing.
    ssc.checkpoint("checkpoint/banking-streaming")

    println("========================================")
    println(" Banking Streaming Application Started")
    println(" Socket: localhost:9997")
    println(" Batch interval: 5 seconds")
    println(" Expected input: transactionId,accountId,branchId,amount,type")
    println("========================================")

    val lines = ssc.socketTextStream("localhost", 9997)

    val parsed = lines.map { line =>
      val fields = line.trim.split(",")

      if (fields.length == 5) {
        try {
          val transactionId = fields(0).trim
          val accountId = fields(1).trim
          val branchId = fields(2).trim
          val amount = fields(3).trim.toDouble
          val transactionType = fields(4).trim.toUpperCase

          if (
            transactionId.nonEmpty &&
            accountId.nonEmpty &&
            branchId.nonEmpty &&
            amount > 0 &&
            Set("DEBIT", "CREDIT").contains(transactionType)
          ) {
            Some((accountId, (1L, amount)))
          } else {
            None
          }
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }.flatMap(_.iterator)

    // Aggregate each account's transactions in each 5-second batch.
    val batchTotals = parsed
      .reduceByKey { (a, b) =>
        (a._1 + b._1, a._2 + b._2)
      }

    batchTotals.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n===== LIVE ACCOUNT TRANSACTIONS =====")
        rdd.collect().foreach { case (accountId, (count, amount)) =>
          println(
            s"Account: $accountId | Transactions: $count | " +
            s"Total Amount: $amount"
          )
        }
      } else {
        println("\nNo valid transactions received in this batch.")
      }
    }

    // Maintain cumulative transaction count and amount per account.
    val accountState = parsed
      .updateStateByKey[(Long, Double)] {
        (newValues: Seq[(Long, Double)],
         oldState: Option[(Long, Double)]) =>

          val newCount = newValues.map(_._1).sum
          val newAmount = newValues.map(_._2).sum

          val previous = oldState.getOrElse((0L, 0.0))

          Some((
            previous._1 + newCount,
            previous._2 + newAmount
          ))
      }

    accountState.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        println("\n===== CUMULATIVE ACCOUNT STATE =====")
        rdd.collect().foreach { case (accountId, (count, amount)) =>
          println(
            s"Account: $accountId | Cumulative Transactions: $count | " +
            s"Cumulative Amount: $amount"
          )
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
