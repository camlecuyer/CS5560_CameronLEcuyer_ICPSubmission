import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import org.apache.spark.{SparkConf, SparkContext}
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.simple
import edu.stanford.nlp.simple.Document
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import rita.RiWordNet

import scala.collection.immutable.HashMap
import scala.io.Source
import scala.collection.mutable.ListBuffer

object SparkWordCount {

  private val OUT_PATH = "C:\\Users\\CJ\\Box\\projectOutput\\" //"C:\\Users\\camle\\Box\\projectOutput\\"
  private val IN_PATH = "C:\\Users\\CJ\\Box\\data\\" //"C:\\Users\\camle\\Box\\data\\"

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // retrieve data
    val inputf = sc.wholeTextFiles(IN_PATH + "abstract_text", 20).map(line => (line._1.substring(line._1.lastIndexOf("/") + 1, line._1.length), lemmatize(line._2)))
    val stopwords = sc.textFile(IN_PATH + "stopwords.txt").collect()
    val stopwordBroadCast = sc.broadcast(stopwords)

    val lemmaInput = inputf.map(line => {
      val lemmas = line._2

      var temp = ""

      lemmas.foreach(ele => temp += ele._1 + " ")

      val lemmaSent = temp.trim

      (line._1, lemmaSent)
    })

    val input = lemmaInput.map(line => {
      val triples = returnTriplets(line._2, line._1)
      triples
    })

    val triples = input.flatMap(line => line)

    val triplets = triples.map(line => {
      var subject = ""
      var obj = ""

      if(line._1.contains(" "))
      {
        val work = line._1.split(" ").filter(!stopwordBroadCast.value.contains(_))

        subject = work.mkString(" ")
      }
      else
      {
        subject = line._1
      }

      if(line._3.contains(" "))
      {
        val work = line._3.split(" ").filter(!stopwordBroadCast.value.contains(_))

        obj = work.mkString(" ")
      }
      else
      {
        obj = line._3
      }

      (subject, line._2, obj, line._4, line._5, line._6)
    }).cache()

    val predicates = triplets.map(line => line._2).distinct().filter(line => line != "")
    val subjects = triplets.map(line => line._1).distinct().filter(line => line != "")
    val objects = triplets.map(line => line._3).distinct().filter(line => line != "")

    // lemmatize the data
    val lemmatized = inputf.map(line => line._2).cache()
    val flatLemma = lemmatized.flatMap(list => list).map(line => if(line._1.compareTo(".") == 0) {
      ("", "")
    }
    else
    {
      line
    })

    val lemmatizedSeq = flatLemma.map(list => List(list._1))
    val ngram2LemmaSeq = lemmatized.map(line => {
      val ngrams = getNGrams(line.map(line => line._1).mkString(" ").replaceAll("[.]", ""), 2).map(list => list.mkString(" ")).toList
      ngrams
    })

    val ngram3LemmaSeq = lemmatized.map(line => {
      val ngrams = getNGrams(line.map(line => line._1).mkString(" ").replaceAll("[.]", ""), 3).map(list => list.mkString(" ")).toList
      ngrams
    })

    val ngram2Num = ngram2LemmaSeq.map(line => ("total", line.length))
    val ngram2Total = ngram2Num.reduceByKey(_+_)

    val ngram3Num = ngram3LemmaSeq.map(line => ("total", line.length))
    val ngram3Total = ngram3Num.reduceByKey(_+_)

    // count for all words and count for all parts of speech
    val wc = flatLemma.map(word =>(word._1 + "," + word._2, 1))
    val wcTotal = wc.count()
    val posCount = flatLemma.map(word => word._2).filter(!stopwordBroadCast.value.contains(_)).map(line => (line, 1))

    // count for wordnet words
    val wordnetCount = flatLemma.map(word => if(new RiWordNet("C:\\WordNet\\WordNet-3.0").exists(word._1)) (word._1 + "," + word._2, 1) else (word._1 + "," + word._2, 0)).reduceByKey(_+_)
    val wordnetCountTotal = wordnetCount.count()

    val medThread = new Thread(new Runnable {
      def run(): Unit = {
        // medical word retrieval
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

          // retieve ids and get data
          for (line <- Source.fromFile(Inputfile).getLines) {
            val data = get("https://www.ncbi.nlm.nih.gov/CBBresearch/Lu/Demo/RESTful/tmTool.cgi/" + Bioconcept + "/" + line + "/" + Format + "/")
            val lines = data.flatMap(line => {line.split("\n")}).drop(2)

            // drop unused parts of output
            val words = lines.map(word => word.split("\t").drop(3).dropRight(1).mkString(",")).toArray

            // places word and designation in tuple
            for(i <- 0 until (words.length - 1))
            {
              val splitWord = words(i).split(",")
              val work = (splitWord.head, splitWord.last)
              medWords += work
            } // end loop
          } // end loop

          val medWordData = sc.parallelize(medWords.toList)
          val flatMed = medWordData.map(word => (word._1.toLowerCase + ", " + word._2.toLowerCase, 1))
          val medType = medWordData.map(word => (word._2.toLowerCase, 1))
          val wordMed = medWordData.map(word => word._1.toLowerCase).distinct()
          val medSeq = wordMed.map(word => List(word))

          val tf_idf = TF_IDF(medSeq, sc)

          tf_idf.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outMedTFIDF")

          val outMed = flatMed.reduceByKey(_+_)
          outMed.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outMed")

          val outMedType = medType.reduceByKey(_+_)
          outMedType.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outMedType")

          val medData = medWordData.map(line => if (line._1.toLowerCase.compareTo(line._2.toLowerCase) != 0)
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
            var subj = "Other"
            val item = line.split(",").head
            medSubjectsWork.foreach(ele => if(ele.contains(item)) {
              subj = ele.split(",").head
            })

            (subj, line, item)
          })

          val tripBoth = tripSub.map(line => {
            var obj = "Other"
            val item = line._2.split(",").drop(2).head
            medObjectsWork.foreach(ele => if(ele.contains(item)) {
              obj = ele.split(",").head
            })

            (line._2.split(",").drop(1).dropRight(1).head, line._1, line._3, obj, item, line._2)
          })

          val medFixed = tripBoth.map(line => if(line._2.compareTo("Other") == 0 && line._4.compareTo("Other") == 0) {
            ("","","","","","")
          }
          else {
            line
          }).distinct().filter(line => line._1.compareTo("") != 0).cache()

          val outMedTotals = List("MedWordCount," + wordMed.count(), "MedTriplets," + medTriplets.count, "MedSubjects," + medSubjects.count,
            "MedObjects," + medObjects.count, "MedWords," + flatMed.count, "FinalTriplets," + medFixed.count)
          sc.parallelize(outMedTotals).map(line => line).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medTotals")

          medSubjects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medSubjects")
          medObjects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medObjects")
          medFixed.map(line => line._6).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medTriplets")
          medFixed.map(line => line._1 + "," + line._2 + "," + line._4 + ",Func").distinct().coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "tripBoth")
          medFixed.map(line => if(line._2.compareTo("Other") == 0) {
            line._2 + "," + line._3
          }
          else if (line._4.compareTo("Other") == 0){
            line._4 + "," + line._5
          }
          else {
            ""
          }).distinct().filter(line => line.compareTo("") != 0).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "otherIndivid")
          medData.map(line => line._2 + ","+ line._1).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medWords")
        } // end if
      } // end unit
    }) // end thread

    // start thread for medical data
    medThread.start()

    val tf_idf = TF_IDF(lemmatizedSeq, sc)
    tf_idf.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outLemmaTFIDF")

    val tf_idf_ngram2 = TF_IDF(ngram2LemmaSeq, sc)
    tf_idf_ngram2.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outNgram2TFIDF")

    val tf_idf_ngram3 = TF_IDF(ngram3LemmaSeq, sc)
    tf_idf_ngram3.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outNgram3TFIDF")

    val wNetCount = wordnetCount.reduceByKey(_+_)
    wNetCount.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outWordNetCount")

    val oPosCount = posCount.reduceByKey(_+_)
    oPosCount.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outPosCount")

    val output = wc.reduceByKey(_+_)
    output.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outCount")

    val outTotals = List("WordCount," + wcTotal, "NGram2Count," + ngram2Total.collect().head._2, "NGram3Count," + ngram3Total.collect().head._2, "WordNetCount," + wordnetCountTotal,
      "Subjects," + subjects.count, "Objects," + objects.count, "Predicates," + predicates.count, "Triplets," + triplets.map(line => line._6).sum(), "UniqueTriplets," + triplets.count)
    sc.parallelize(outTotals).map(line => line).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "totals")

    triplets.map(line => line._5 + "," + line._4 + "," + line._1 + ";" + line._2 + ";" + line._3 + ","+ line._6).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "triplets")
    triplets.map(line => line._1 + ";" + line._2 + ";" + line._3).coalesce(1, shuffle = true).filter(line => line != ";;").saveAsTextFile(OUT_PATH + "triples")
    predicates.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "predicates")
    subjects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "subjects")
    objects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "objects")
  } // end main

  // generates the NGrams based on the number input
  // reference: https://stackoverflow.com/questions/8258963/how-to-generate-n-grams-in-scala
  def getNGrams(sentence: String, n:Int): IndexedSeq[List[String]] = {
    val words = sentence
    //val ngrams = (for(i <- 2 to n) yield words.split(' ').sliding(i).map(word => word.toList)).flatten
    val ngrams = words.split(' ').sliding(n).map(word => word.toList).toIndexedSeq
    ngrams
  }

  // lemmatizes input string
  // code referenced from https://stackoverflow.com/questions/30222559/simplest-method-for-text-lemmatization-in-scala-and-spark
  def lemmatize(text: String): ListBuffer[(String, String)] = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val document = new Annotation(text.replaceAll("\\(.*?\\)", "").replaceAll("[/]", " ").replaceAll("[',%]", "").replaceAll("\\s[0-9]+\\s", " ").replaceAll("\\s[0-9]+[.][0-9]+\\s", " "))
    pipeline.annotate(document)

    val lemmas = ListBuffer.empty[(String, String)]
    val sentences = document.get(classOf[SentencesAnnotation])

    // for each token get the lemma and part of speech
    for (sentence <- sentences) {
      for (token <- sentence.get(classOf[TokensAnnotation]))
      {
        var lemma = token.get(classOf[LemmaAnnotation])
        val pos = token.get(classOf[PartOfSpeechAnnotation])

        if(lemma.toLowerCase().compareTo("ad") == 0)
        {
          lemma = "alzheimers disease"
        }

        if (lemma.length > 1) {
          lemmas += ((lemma.toLowerCase, pos.toLowerCase))
        } // end if
      }

      lemmas += ((".", ""))
    } // end loop
    lemmas
  } // end lemmatize

  // gets data for medical words from URL
  def get(url: String): Iterator[String] = scala.io.Source.fromURL(url).getLines()

  def TF_IDF(data: RDD[List[String]], sc: SparkContext): RDD[(String, Double)] = {
    //Creating an object of HashingTF Class
    val hashingTF = new HashingTF()

    //Creating Term Frequency of the document
    val tf = hashingTF.transform(data)
    tf.cache()

    val idf = new IDF().fit(tf)

    //Creating Inverse Document Frequency
    val tfidf = idf.transform(tf)

    val tfidfvalues = tfidf.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val values = ff(2).replace("]", "").replace(")", "").split(",")
      values
    })

    val tfidfindex = tfidf.flatMap(f => {
      val ff: Array[String] = f.toString.replace(",[", ";").split(";")
      val indices = ff(1).replace("]", "").replace(")", "").split(",")
      indices
    })

    val tfidfData = tfidfindex.zip(tfidfvalues)

    var hm = new HashMap[String, Double]

    tfidfData.collect().foreach(f => {
      hm += f._1 -> f._2.toDouble
    })

    val mapp = sc.broadcast(hm)

    val documentData = data.flatMap(_.toList)

    val dd = documentData.map(f => {
      val i = hashingTF.indexOf(f)
      val h = mapp.value
      (f, h(i.toString))
    })

    val dd1 = dd.distinct().sortBy(_._2, ascending = false)

    dd1
  } // end TF_IDF

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
  } // end toCamelCase

  def returnTriplets(sentence: String, docName: String): ListBuffer[(String, String, String, String, String, Int)] = {
    val doc: Document = new Document(sentence)
    val lemma = ListBuffer.empty[(String, String, String, String, String, Int)]

    for (sent: simple.Sentence <- doc.sentences().asScala.toList) {

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

      lemma += ((toCamelCase(subject.toLowerCase), toCamelCase(predicate.toLowerCase), toCamelCase(obj.toLowerCase), sent.toString, docName, count))
    }
    lemma
  } // end returnTriplets
}
