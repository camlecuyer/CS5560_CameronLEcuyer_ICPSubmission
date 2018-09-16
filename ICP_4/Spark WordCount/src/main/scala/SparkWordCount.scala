

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Mayanka on 09-Sep-15.
 */
object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val inputf = sc.wholeTextFiles("abstract_text", 4)

    val wc = inputf.flatMap(line=>{line._2.split(" ")}).map(word=>(word,1))

    // example on how to refer within wholeTextFiles
    /*inputf.map(abs => {
      abs._1
      abs._2
    })*/

    //val input = sc.textFile("input", 4)

    //val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output=wc.reduceByKey(_+_)

    output.saveAsTextFile("output")

    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}

  }

}
