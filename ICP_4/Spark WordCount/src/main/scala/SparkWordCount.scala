import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import org.apache.spark.{SparkConf, SparkContext}
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP

import scala.collection.JavaConversions._
import rita.RiWordNet

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val inputf = sc.wholeTextFiles("abstract_text", 4)

    val lemmatized = inputf.map(line => lemmatize(line._2))
    //lemmatized.foreach(println)

    val flatLemma = lemmatized.flatMap(list => list)

    //val flatInput = inputf.flatMap(doc=>{doc._2.split(" ")})

    val wc = flatLemma.map(word =>(word._1,1))

    val wordnetCount = flatLemma.map(word => if(new RiWordNet("C:\\WordNet\\WordNet-3.0").exists(word._1)) (word._1,1) else (word._1, 0))
    val posCount = flatLemma.map(word => (word._2,1))

    //wordnetCount.foreach(println)
    val wNetCount = wordnetCount.reduceByKey(_+_)

    wNetCount.saveAsTextFile("outCount")

    val oPosCount = posCount.reduceByKey(_+_)

    oPosCount.saveAsTextFile("outPos")

    // example on how to refer within wholeTextFiles
    /*inputf.map(abs => {
      abs._1
      abs._2
    })*/

    //val input = sc.textFile("input", 4)

    //val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output = wc.reduceByKey(_+_)
    output.saveAsTextFile("output")

    //val o = output.collect()

    //var s:String="Words:Count \n"
    //o.foreach{case(word,count)=>{

      //s+=word+" : "+count+"\n"

   // }}
  }

  // code referenced from https://stackoverflow.com/questions/30222559/simplest-method-for-text-lemmatization-in-scala-and-spark
  def lemmatize(text: String): ListBuffer[(String, String)] = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val document = new Annotation(text)
    pipeline.annotate(document)

    //val lemmas = new ArrayBuffer[String]()
    val lemmas = ListBuffer.empty[(String, String)]
    val sentences = document.get(classOf[SentencesAnnotation])

    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      val pos = token.get(classOf[PartOfSpeechAnnotation])

      if (lemma.length > 2) {
        //lemmas += lemma.toLowerCase
        //lemmas += pos
        lemmas += ((lemma.toLowerCase, pos.toLowerCase))
      }
    }
    lemmas
  }
}
