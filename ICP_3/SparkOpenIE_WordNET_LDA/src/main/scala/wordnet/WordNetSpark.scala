package wordnet

import org.apache.spark.{SparkConf, SparkContext}
import rita.RiWordNet

/**
  * Created by Mayanka on 26-06-2017.
  */
object WordNetSpark {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)


    //val data=sc.textFile("data/abstract_text/1.txt")
    //val data=sc.textFile("data/abstract_text/2.txt")
    //val data=sc.textFile("data/abstract_text/3.txt")
    val data=sc.textFile("data/abstract_text/4.txt")
    //val data=sc.textFile("data/abstract_text/5.txt")

    val dd=data.map(line=>{
      val wordnet = new RiWordNet("C:\\WordNet\\WordNet-3.0")
      val wordSet=line.split(" ")
      val synarr=wordSet.map(word=>{
        if(wordnet.exists(word))
          (word,getSynoymns(wordnet,word))
        else
          (word,null)
      })
      synarr
    })
    dd.collect().foreach(linesyn=>{
      linesyn.foreach(wordssyn=>{
        if(wordssyn._2 != null)
          println(wordssyn._1+":"+wordssyn._2.mkString(","))
      })
    })
  }
  def getSynoymns(wordnet:RiWordNet,word:String): Array[String] ={
    println(word)
    val pos=wordnet.getPos(word)
    println(pos.mkString(" "))
    val syn=wordnet.getAllSynonyms(word, pos(0), 10)
    syn
  }

}
