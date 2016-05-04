package projects

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Peter on 5/3/2016.
  */
object spark_sort_by_word_length {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)

    //val input = sc.textFile("pride_and_prejudice.txt")
    val input = sc.textFile("dictionary.txt")   // foster's dictionary
    val words = input.flatMap(line => line.split(" ")).flatMap(line => line.split("-"))
    val result = words.map(word => (word, word.length) // Get word lengths
    ).reduceByKey { case (x, y) => (x) // Remove duplicates
    }.map { case (word, length) => (length, word) // swap key and value so length is the new key for sorting
    }.sortByKey(false, 1 // sorting descending by length
    ).map { case (length, word) => word } // remove the length part
    result.saveAsTextFile("outputDictionary")
  }
}
