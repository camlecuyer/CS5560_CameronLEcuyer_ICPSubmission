import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import org.apache.spark.{SparkConf, SparkContext}
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP

import scala.collection.JavaConversions._
import rita.RiWordNet
import scala.io.Source
import scala.collection.mutable.{ListBuffer}

object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val inputf = sc.wholeTextFiles("abstract_text", 4)

    val lemmatized = inputf.map(line => lemmatize(line._2))

    val flatLemma = lemmatized.flatMap(list => list)

    //val flatInput = inputf.flatMap(doc=>{doc._2.split(" ")})

    val wc = flatLemma.map(word =>(word._1,1))

    val wordnetCount = flatLemma.map(word => if(new RiWordNet("C:\\WordNet\\WordNet-3.0").exists(word._1)) (word._1,1) else (word._1, 0))
    val posCount = flatLemma.map(word => (word._2,1))

    val wNetCount = wordnetCount.reduceByKey(_+_)

    wNetCount.saveAsTextFile("outCount")

    val oPosCount = posCount.reduceByKey(_+_)

    oPosCount.saveAsTextFile("outPos")

    //val input = sc.textFile("input", 4)

    //val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output = wc.reduceByKey(_+_)
    output.saveAsTextFile("output")

    if (args.length < 2) {
      System.out.println("\n$ java RESTClientGet [Bioconcept] [Inputfile] [Format]")
      System.out.println("\nBioconcept: We support five kinds of bioconcepts, i.e., Gene, Disease, Chemical, Species, Mutation. When 'BioConcept' is used, all five are included.\n\tInputfile: a file with a pmid list\n\tFormat: PubTator (tab-delimited text file), BioC (xml), and JSON\n\n")
    }
    else
    {
      val Bioconcept = args(0)
      val Inputfile = args(1)
      var Format = "PubTator"
      if (args.length > 2) Format = args(2)

      val medWords = ListBuffer.empty[(String, String)]

      for (line <- Source.fromFile(Inputfile).getLines) {
        val data = get("https://www.ncbi.nlm.nih.gov/CBBresearch/Lu/Demo/RESTful/tmTool.cgi/" + Bioconcept + "/" + line + "/" + Format + "/")
        val lines = data.flatMap(line => {line.split("\n")}).drop(2)

        val words = lines.flatMap(word => {word.split("\t").drop(3).dropRight(1)}).toArray

        for(i <- 0 until words.length by 2)
        {
          if(i < words.length - 1)
          {
            val work = (words(i), words(i+1))
            medWords += work
          }

        }
      }

      val medData = sc.parallelize(medWords.toList)
      val flatMed = medData.map(word => (word._1, 1))
      val outMed = flatMed.reduceByKey(_+_)
      outMed.saveAsTextFile("outMed")
    }
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

  def get(url: String) = scala.io.Source.fromURL(url).getLines()
}
