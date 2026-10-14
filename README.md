# Day 30 — Real-Time Banking Analytics Platform

## Project Overview
A Scala and Apache Spark capstone project demonstrating batch analytics and real-time transaction processing for a banking use case.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.3
- Spark SQL
- Spark Core
- Spark Streaming (DStreams)
- sbt
- Java 17

## Batch Processing
- Read transaction, account, and branch CSV datasets.
- Validate transaction records.
- Join transactions with account and branch reference data using broadcast joins.
- Classify transaction amounts using a UDF.
- Use persistence to reuse processed data.
- Calculate account-wise running net amounts with window functions.
- Generate account-wise, branch-wise, and daily summaries.
- Inspect the Spark physical execution plan.

## Streaming Processing
- Receive transactions through a TCP socket on port 9997.
- Process transactions in five-second batches.
- Aggregate transaction counts and amounts by account.
- Maintain cumulative account state with `updateStateByKey`.
- Use checkpointing for stateful streaming.

## Sample Streaming Input
`T101,A101,B01,5000,DEBIT`

Input format: `transactionId,accountId,branchId,amount,type`

## Run Batch Application
```bash
sbt "runMain BankingBatch"
```

## Run Streaming Application
Start a socket sender in one terminal:

```bash
nc -lk 9997
```

Start Spark Streaming in another terminal:

```bash
sbt "runMain BankingStreaming"
```

Enter sample transactions in the socket terminal.

## Learning Outcomes
This project demonstrates batch processing, stateful streaming, Spark SQL, joins, window functions, data validation, persistence, and execution-plan analysis.

## Limitations
This is a local learning demonstration. The socket-based stream is not a durable message broker, and the project is not a production banking or fraud-detection system. Production deployments require durable ingestion, robust data-quality handling, security, monitoring, and reliable financial auditing.
