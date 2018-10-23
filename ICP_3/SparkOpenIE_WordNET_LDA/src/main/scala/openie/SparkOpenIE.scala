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
      val triples = returnTriplets(line._2, line._1.substring(line._1.lastIndexOf("/") + 1, line._1.length))
      triples
    })

    val triplets = input.flatMap(line => line)

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

      val medData = sc.parallelize(medWords.toList).map(line => if (line._1.compareTo(line._2) != 0)
        {
          (toCamelCase(line._1.replaceAll("[']", "")), line._2.toLowerCase.capitalize)
        }
        else
        {
          (toCamelCase(line._1), "Misc")
        }).distinct().filter(line => line._1.length > 1)
      //val medWordList = medData.map(line => line._1).toLocalIterator.toSet
      //val medWordListSingle = medData.map(line => if(line._1.contains(" ")) { line._1.split(" ")} else {line._1}).toLocalIterator.toSet

      /*val medSubjects = triplets.map(line => if(line._1.contains(" ") && line._3.contains(" "))
      {
        if (medWordList.contains(line._1) || line._1.split(" ").exists(medWordListSingle contains _)) {
          toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3)
        }
        else if(medWordList.contains(line._3) || line._3.split(" ").exists(medWordListSingle contains _))
        {
          toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3)
        }
        else
        {
          ""
        }
      }
      else if (line._1.contains(" "))
      {
        if (medWordList.contains(line._1) || line._1.split(" ").exists(medWordListSingle contains _)) {
          toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3)
        }
        else if(medWordList.contains(line._3))
        {
          toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3)
        }
        else
        {
          ""
        }
      }
      else if (line._3.contains(" "))
      {
        if (medWordList.contains(line._1)) {
          toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3)
        }
        else if(medWordList.contains(line._3) || line._3.split(" ").exists(medWordListSingle contains _))
        {
          toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3)
        }
        else
        {
          ""
        }
      }
      else
      {
        if (medWordList.contains(line._1)) {
          toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3)
        }
        else if (medWordList.contains(line._3))
        {
          toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3)
        }
        else
        {
          ""
        }
      }).distinct()*/
      val workSubjects = subjects.toLocalIterator.toSet
      val workObjects = objects.toLocalIterator.toSet
      val workTriples = triplets.map(line => (toCamelCase(line._1), toCamelCase(line._2), toCamelCase(line._3))).toLocalIterator.toSet

      val medSubjects = medData.filter(line => line._1.length > 2).map(line => {
        var found : Boolean = false
        var subject = ""
        workSubjects.foreach(ele => if(ele.toLowerCase.contains(line._1.toLowerCase))
        {
          found = true
          subject = ele
        })

        if(found)
        {
          line._2 + "," + subject
        }
        else
        {
            ""
        }
      }).distinct().filter(line => line != "")

      val medObjects = medData.filter(line => line._1.length > 2).map(line => {
        var found : Boolean = false
        var obj = ""
        workObjects.foreach(ele => if(ele.toLowerCase.contains(line._1.toLowerCase))
        {
          found = true
          obj = ele
        })

        if(found)
        {
          line._2 + "," + obj
        }
        else
        {
          ""
        }
      }).distinct().filter(line => line != "")

      val medTriplets = medData.filter(line => line._1.length > 2).map(line => {
        var found : Boolean = false
        var obj = ""
        workTriples.foreach(ele => if(ele._1.toLowerCase.contains(line._1.toLowerCase) || ele._3.toLowerCase.contains(line._1.toLowerCase))
        {
          found = true
          obj = ele._1 + "," + ele._2 + "," + ele._3
        })

        if(found)
        {
          obj + "," + "Obj" //+ ";" + line._1
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

      triplets.map(line => line._5 + ";" + line._4 + ";" + toCamelCase(line._1) + "," + toCamelCase(line._2) + "," + toCamelCase(line._3) + ";"+ line._6).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "triplets")
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

        temp = words.mkString("").replaceAll("[.]", "")
      }

    temp
  }

  def returnTriplets(sentence: String, docName: String): List[(String, String, String, String, String, Int)] = {
    val doc: Document = new Document(sentence.replaceAll("\\(.*?\\)","").replaceAll("[',%]", "").replaceAll("\\s[0-9]+\\s", " "))
    val lemma = ListBuffer.empty[(String, String, String, String, String, Int)]

    for (sent: simple.Sentence <- doc.sentences().asScala.toList) { // Will iterate over two sentences

      val l = sent.openie()
      val data = l.iterator()

      var subject = ""
      var predicate = ""
      var obj = ""
      var count = 0
      //println(sent)
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

      //println(subject + ", " + predicate + ", " + obj)

      lemma += ((subject.toLowerCase, predicate.toLowerCase, obj.toLowerCase, sent.toString, docName, count))
    }
    lemma.toList
  }

  // gets data for medical words from URL
  def get(url: String): Iterator[String] = scala.io.Source.fromURL(url).getLines()
}
