package openie

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import edu.stanford.nlp.simple
import edu.stanford.nlp.simple.Document
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
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

    // StopWords
    val stopwords = sc.textFile("data/stopwords.txt").collect()
    val stopwordBroadCast = sc.broadcast(stopwords)

    val input = sc.wholeTextFiles(IN_PATH + "abstract_text", 4).map(line => {
    //val input = sc.textFile("data/abstract_text/1.txt").map(line => {
    //val input = sc.textFile(IN_PATH + "abstract_text/1.txt").map(line => {
      //Getting OpenIE Form of the word using lda.CoreNLP

      //val t = CoreNLP.returnTriplets(line)
      val lemmas = lemmatize(line._2)

      var temp = ""

      lemmas.foreach(ele => temp += ele._1 + " ")

      val lemmaSent = temp.trim

      val triples = returnTriplets(lemmaSent, line._1.substring(line._1.lastIndexOf("/") + 1, line._1.length))
      (triples, lemmas)
    })

    val triples = input.flatMap(line => line._1)
    val lemmas = input.flatMap(line => line._2)

    val triplets = triples.map(line => {
      var subject = ""
      var obj = ""

      if(line._1.contains(" "))
      {
        val work = line._1.split(" ").filter(!stopwordBroadCast.value.contains(_))

        for(i <- 0 until (work.length - 1))
        {
          if(work(i).compareTo("ad") == 0)
          {
            work(i) = "alzheimers disease"
          }
        }

        subject = work.mkString(" ")
      }
      else
      {
        if(line._1.compareTo("ad") == 0)
        {
          subject = "alzheimers disease"
        }
        else
        {
          subject = line._1
        }
      }

      if(line._3.contains(" "))
      {
        val work = line._3.split(" ").filter(!stopwordBroadCast.value.contains(_))

        for(i <- 0 until (work.length - 1))
        {
          if(work(i).compareTo("ad") == 0)
          {
            work(i) = "alzheimers disease"
          }
        }

        obj = work.mkString(" ")
      }
      else
      {
        if(line._3.compareTo("ad") == 0)
        {
          obj = "alzheimers disease"
        }
        else
        {
          obj = line._3
        }
      }

      (subject, line._2, obj, line._4, line._5, line._6)
    })

    val predicates = triplets.map(line => toCamelCase(line._2)).distinct().filter(line => line != "")
    val subjects = triplets.map(line => toCamelCase(line._1)).distinct().filter(line => line != "")
    val objects = triplets.map(line => toCamelCase(line._3)).distinct().filter(line => line != "")

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

      // retrieve ids and get data
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

      val medData = sc.parallelize(medWords.toList).map(line => if (line._1.toLowerCase.compareTo(line._2.toLowerCase) != 0)
        {
          (toCamelCase(line._1.replaceAll("[']", "").toLowerCase), line._2.toLowerCase.capitalize)
        }
        else
        {
          (toCamelCase(line._1.toLowerCase), "Misc")
        }).distinct().filter(line => line._1.length > 1)

      val workMed = medData.filter(line => line._1.length > 2).toLocalIterator.toSet
      val workTriples = triplets.map(line => (toCamelCase(line._1), toCamelCase(line._2), toCamelCase(line._3)))

      val medSubjects = subjects.map(line => {
        var found : Boolean = false
        var subject = ""
        workMed.foreach(ele => if(line.toLowerCase.contains(ele._1.toLowerCase) && !found)
        {
          found = true
          subject = ele._2
        })

        if(found)
        {
          subject + "," + line
        }
        else
        {
          ""
        }
      }).distinct().filter(line => line != "")

      val medObjects = objects.map(line => {
        var found : Boolean = false
        var obj = ""
        workMed.foreach(ele => if(line.toLowerCase.contains(ele._1.toLowerCase) && !found)
        {
          found = true
          obj = ele._2
        })

        if(found)
        {
          obj + "," + line
        }
        else
        {
          ""
        }
      }).distinct().filter(line => line != "")

      val medTriplets = workTriples.map(line => {
        var found : Boolean = false
        workMed.foreach(ele => if((line._1.toLowerCase.contains(ele._1.toLowerCase) || line._3.toLowerCase.contains(ele._1.toLowerCase)) && !found)
        {
          found = true
        })

        if(found)
        {
          line._1 + "," + line._2 + "," + line._3 + "," + "Obj" //+ ";" + line._1
        }
        else
        {
          ""
        }
      }).distinct().filter(line => line != "")

      val medSubjectsWork = medSubjects.toLocalIterator.toList
      val medObjectsWork = medObjects.toLocalIterator.toList

      val tripSub = medTriplets.map(line => {
        var subj = "Subject"
        val item = line.split(",").head
        medSubjectsWork.foreach(ele => if(ele.contains(item)) {
          subj = ele.split(",").head
        })

        (subj, line, item)
      })

      val tripBoth = tripSub.map(line => {
        var obj = "Object"
        val item = line._2.split(",").drop(2).head
        medObjectsWork.foreach(ele => if(ele.contains(item)) {
          obj = ele.split(",").head
        })

        (line._2.split(",").drop(1).dropRight(1).head, line._1, line._3, obj, item, line._2)
      })

      val medFixed = tripBoth.map(line => if(line._2.compareTo("Subject") == 0 && line._4.compareTo("Object") == 0) {
        ("","","","","","")
      }
      else {
        line
      }).distinct().filter(line => line._1.compareTo("") != 0)

      System.out.println("MedTriplets: " + medTriplets.count)
      System.out.println("MedSubjects: " + medSubjects.count)
      System.out.println("MedObjects: " + medObjects.count)
      System.out.println("MedWords: " + medData.count)
      System.out.println("Subjects: " + subjects.count)
      System.out.println("Objects: " + objects.count)
      System.out.println("Predicates: " + predicates.count)
      System.out.println("FinalTriplets: " + medFixed.count)

      System.out.println("Triplets: " + triplets.map(line => line._6).sum())
      System.out.println("UniqueTriplets: " + triplets.count)

      triplets.map(line => line._5 + "," + line._4 + "," + toCamelCase(line._1) + ";" + toCamelCase(line._2) + ";" + toCamelCase(line._3) + ","+ line._6).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "triplets")
      predicates.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "predicates")
      subjects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "subjects")
      objects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "objects")
      medSubjects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medSubjects")
      medObjects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medObjects")
      medFixed.map(line => line._6).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medTriplets")
      medFixed.map(line => line._1 + "," + line._2 + "," + line._4 + ",Func").distinct().coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "tripBoth")
      medFixed.map(line => if(line._2.compareTo("Subject") == 0) {
        line._2 + "," + line._3
      }
      else if (line._4.compareTo("Object") == 0){
        line._4 + "," + line._5
      }
      else {
        ""
      }).distinct().filter(line => line.compareTo("") != 0).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "otherIndivid")
      medData.map(line => line._2 + ","+ line._1).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medWords")
    }
  }

  def toCamelCase(phrase: String): String = {
    var temp = phrase

    if(phrase.contains(" "))
      {
        val words = phrase.split(" ")

        for(i <- 1 until words.length)
        {
          if(words(i).length > 1)
          {
              words(i) = words(i).capitalize
          }
        }

        temp = words.mkString("_").replaceAll("[.]", "")
      }

    temp
  }

  def returnTriplets(sentence: String, docName: String): List[(String, String, String, String, String, Int)] = {
    val doc: Document = new Document(sentence.replaceAll("\\(.*?\\)","").replaceAll("[',%/]", "").replaceAll("\\s[0-9]+\\s", " "))
    val lemma = ListBuffer.empty[(String, String, String, String, String, Int)]

    for (sent: simple.Sentence <- doc.sentences().asScala.toList) { // Will iterate over two sentences

      val l = sent.openie()
      val data = l.iterator()

      var subject = ""
      var predicate = ""
      var obj = ""
      var count = 0

      while(data.hasNext)
        {
          val temp = data.next()
          count += 1

          if((subject.length <= temp.first.length) && (obj.length < temp.third.length))
            {
              subject = temp.first
              predicate = temp.second
              obj = temp.third
            }
        }

      lemma += ((subject.toLowerCase, predicate.toLowerCase, obj.toLowerCase, sent.toString, docName, count))
    }
    lemma.toList
  }

  // gets data for medical words from URL
  def get(url: String): Iterator[String] = scala.io.Source.fromURL(url).getLines()

  // lemmatizes input string
  // code referenced from https://stackoverflow.com/questions/30222559/simplest-method-for-text-lemmatization-in-scala-and-spark
  def lemmatize(text: String): List[(String, String)] = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val document = new Annotation(text.replaceAll("\\(.*?\\)", "").replaceAll("[',%/]", "").replaceAll("\\s[0-9]+\\s", " ").replaceAll("\\s[0-9]+[.][0-9]+\\s", " "))
    pipeline.annotate(document)

    val lemmas = ListBuffer.empty[(String, String)]
    val sentences = document.get(classOf[SentencesAnnotation])

    // for each token get the lemma and part of speech
    for (sentence <- sentences) {
      for (token <- sentence.get(classOf[TokensAnnotation]))
      {
        val lemma = token.get(classOf[LemmaAnnotation])
        val pos = token.get(classOf[PartOfSpeechAnnotation])

        if (lemma.length > 1) {
          lemmas += ((lemma.toLowerCase, pos.toLowerCase))
        } // end if
      }

      lemmas += ((".", ""))
    } // end loop
    lemmas.toList
  } // end lemmatize
}
