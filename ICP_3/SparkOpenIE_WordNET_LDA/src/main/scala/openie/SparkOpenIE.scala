package openie

import scala.collection.JavaConverters._
import edu.stanford.nlp.simple
import edu.stanford.nlp.simple.Document
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source
import scala.collection.mutable.ListBuffer

/**
  * Created by Mayanka on 27-Jun-16.
  */
object SparkOpenIE {

  private val OUT_PATH = "C:\\Users\\CJ\\Box\\outputFiles_lite\\" //"C:\\Users\\camle\\Box\\outputFiles_lite\\"
  private val IN_PATH = "C:\\Users\\CJ\\Box\\data_lite\\" //"C:\\Users\\camle\\Box\\data_lite\\"

  def main(args: Array[String]) {
    // Configuration
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]") //.set("spark.executor.memory","3g").set("spark.driver.memory","2g")

    val sc = new SparkContext(sparkConf)

    // Turn off Info Logger for Console
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val input = sc.wholeTextFiles(IN_PATH + "abstract_text", 4).map(line => {
    //val input = sc.textFile("data/abstract_text/1.txt").map(line => {
    //val input = sc.textFile(IN_PATH + "abstract_text/1.txt").map(line => {
      //Getting OpenIE Form of the word using lda.CoreNLP

      //val t = CoreNLP.returnTriplets(line)
      val triples = returnTriplets(line._2)
      triples
    })

    val triplets = input.flatMap(line => line)

    val predicates = triplets.map(line => toCamelCase(line._2)).distinct()
    val subjects = triplets.map(line => line._1).distinct()

    //input.saveAsTextFile(OUT_PATH + "triplets")
    //println(CoreNLP.returnTriplets("The dog has a lifespan of upto 10-12 years."))
   //println(input.collect().mkString("\n"))

    // medical word retrieval
    if (args.length < 2) {
      System.out.println("\n$ java RESTClientGet [Bioconcept] [Inputfile] [Format]")
      System.out.println("\nBioconcept: We support five kinds of bioconcepts, i.e., Gene, Disease, Chemical, Species, Mutation. When 'BioConcept' is used, all five are included.\n\tInputfile: a file with a pmid list\n\tFormat: PubTator (tab-delimited text file), BioC (xml), and JSON\n\n")
    }
    else {
      val Bioconcept = args(0)
      val Inputfile = args(1)
      var Format = "PubTator"
      if (args.length > 2) Format = args(2)

      val medWords = ListBuffer.empty[(String, String)]

      // retieve ids and get data
      for (line <- Source.fromFile(Inputfile).getLines) {
        val data = get("https://www.ncbi.nlm.nih.gov/CBBresearch/Lu/Demo/RESTful/tmTool.cgi/" + Bioconcept + "/" + line + "/" + Format + "/")
        val lines = data.flatMap(line => {
          line.split("\n")
        }).drop(2)

        // drop unused parts of output
        val words = lines.map(word => word.split("\t").drop(3).dropRight(1).mkString(",")).toArray

        // places word and designation in tuple
        for (i <- 0 until (words.length - 1)) {
          val splitWord = words(i).split(",")
          val work = (splitWord.head, splitWord.last)
          medWords += work
        } // end loop
      } // end loop

      val medData = sc.parallelize(medWords.toList).map(line => (line._1.toLowerCase, line._2)).distinct()
      val medWordList = medData.map(line => line._1).toLocalIterator.toSet

      val medSubjects = subjects.map(line => if (medWordList.contains(line)) {
        toCamelCase(line)
      } else {
        ""
      }).distinct()

      //triplets.map(line => toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3)).saveAsTextFile(OUT_PATH + "triplets")
      //predicates.saveAsTextFile(OUT_PATH + "predicates")
      //subjects.map(line => toCamelCase(line)).saveAsTextFile(OUT_PATH + "subjects")
      medSubjects.saveAsTextFile(OUT_PATH + "medSubjects")
      medData.map(line => line._1).distinct().saveAsTextFile(OUT_PATH + "medWords")
    }
  }

  def toCamelCase(phrase: String): String = {
    var temp = phrase

    if(phrase.contains(" "))
      {
        val words = phrase.split(" ")

        for(i <- 1 until words.length)
        {
          words(i) = words(i).capitalize
        }

        temp = words.mkString("").replaceAll("[.]", "")
      }

    temp
  }

  def returnTriplets(sentence: String): List[(String, String, String)] = {
    val doc: Document = new Document(sentence.replaceAll("[',()]", ""))
    val lemma = ListBuffer.empty[(String, String, String)]

    for (sent: simple.Sentence <- doc.sentences().asScala.toList) { // Will iterate over two sentences

      val l = sent.openie()
      val data = l.iterator()

      var subject = ""
      var predicate = ""
      var obj = ""

      //println(sent)
      while(data.hasNext)
        {
          val temp = data.next()

          if((subject.length <= temp.first.length) && (obj.length < temp.third.length))
            {
              subject = temp.first
              predicate = temp.second
              obj = temp.third
            }
        }

      //println(subject + ", " + predicate + ", " + obj)

      if(subject != "")
        {
          lemma += ((subject.toLowerCase, predicate.toLowerCase, obj.toLowerCase))
        }
    }
    lemma.toList
  }

  // gets data for medical words from URL
  def get(url: String): Iterator[String] = scala.io.Source.fromURL(url).getLines()
}
