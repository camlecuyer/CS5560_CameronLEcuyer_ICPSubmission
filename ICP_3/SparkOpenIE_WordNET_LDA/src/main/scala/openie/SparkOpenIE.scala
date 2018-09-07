package openie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mayanka on 27-Jun-16.
  */
object SparkOpenIE {

  def main(args: Array[String]) {
    // Configuration
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]") //.set("spark.executor.memory","3g").set("spark.driver.memory","2g")

    val sc = new SparkContext(sparkConf)

    // Turn off Info Logger for Console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //val input = sc.textFile("data/abstract_text/1.txt").map(line => {
    val input = sc.textFile("data/abstract_text/2.txt").map(line => {
    //val input = sc.textFile("data/abstract_text/3.txt").map(line => {
    //val input = sc.textFile("data/abstract_text/4.txt").map(line => {
    //val input = sc.textFile("data/abstract_text/5.txt").map(line => {
      //Getting OpenIE Form of the word using lda.CoreNLP

      val t=CoreNLP.returnTriplets(line)
      t
    })

    input.collect()
    //println(CoreNLP.returnTriplets("The dog has a lifespan of upto 10-12 years."))
   //println(input.collect().mkString("\n"))
  }
}
