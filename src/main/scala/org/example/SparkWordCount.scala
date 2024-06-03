package org.example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkWordCount {
  def updateState(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val updatedCount = runningCount.getOrElse(0) + newValues.sum
    Some(updatedCount)
  }
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: CombinedTasks <directory> <outputDirectory>") // Throwing an error if user fails to input the required number of arguments
      System.exit(1)
    }

    // Initializing SparkConf and Streaming Context along with Batch Interval duration
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint(".")

    // Task A - Word Count

    //Tokenizing and filtering the words as per specifications
    val tokenized = ssc.textFileStream(args(0)).flatMap(line => line.split(" ")).filter(word => word.matches("[a-zA-Z]+") && word.length >= 3)
    //Counting takes place here
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    //--------------------------End of Task A-----------------------------//

    // Task B - Co-Occurrence Count
    val docStream = ssc.textFileStream(args(0)) // Reading input file from the input directory
    val wordPairStream = docStream.flatMap(eachLine => {
      val cleanedWords = eachLine.split("\\s+").filter(word => word.matches("[a-zA-Z]+") && word.length >= 3) // Tokenizing and filtering
      for {
        i <- 0 until cleanedWords.length
        j <- 0 until cleanedWords.length if i != j
      } yield (s"${cleanedWords(i)} ${cleanedWords(j)}", 1) // Looping through the text one after the other and yielding co-occurrences
    })
    val coMatrix = wordPairStream.reduceByKey(_ + _)

    //--------------------------End of Task B-----------------------------//

    // Task C - Co-occurrence Count using updateStateByKey
    val docStreamC = ssc.textFileStream(args(0))
    val wordPairStreamC = docStreamC.flatMap(eachLine => {
      val cleanedWordsC = eachLine.split("\\s+").filter(word => word.matches("[a-zA-Z]+") && word.length >= 3)
      for {
        i <- 0 until cleanedWordsC.length
        j <- 0 until cleanedWordsC.length if i != j
      } yield (s"${cleanedWordsC(i)} ${cleanedWordsC(j)}", 1)
    })
    val coMatrixC = wordPairStreamC.updateStateByKey[Int](updateState _) // Same as task B except updateStateByKey is used instead of reduceByKey

    //--------------------------End of Task C-----------------------------//

    // Saving the outputs to the appropriate directories

    // Initialize the counters (it'll be incremented for every new file streamed in
    var counterA = 1
    var counterB = 1
    var counterC = 1

    // Task A - Word Count
    wordCounts.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val taskADirectory = args(1) + s"/taskA-${counterA formatted "%03d"}"
        rdd.saveAsTextFile(taskADirectory)
        counterA += 1
      }
    }

    // Task B - Co-Occurrence Count
    coMatrix.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val taskBDirectory = args(1) + s"/taskB-${counterB formatted "%03d"}"
        rdd.saveAsTextFile(taskBDirectory)
        counterB += 1
      }
    }

    // Task C - Stateful Co-Occurrence Count
    coMatrixC.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val taskCDirectory = args(1) + s"/taskC-${counterC formatted "%03d"}"
        rdd.saveAsTextFile(taskCDirectory)
        counterC += 1
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}