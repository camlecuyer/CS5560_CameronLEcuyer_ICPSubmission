import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import org.apache.spark.{SparkConf, SparkContext}
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.simple
import edu.stanford.nlp.simple.Document
import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import rita.RiWordNet

import scala.collection.immutable.HashMap
import scala.io.Source
import scala.collection.mutable.ListBuffer

object SparkWordCount {

  private val OUT_PATH = "C:\\Users\\CJ\\Box\\projectOutput\\"
  private val IN_PATH = "C:\\Users\\CJ\\Box\\data\\"

  def main(args: Array[String]) {

    // Get hadoop from winutils
    System.setProperty("hadoop.home.dir","C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // retrieve data and lemmatize it
    val inputf = sc.wholeTextFiles(IN_PATH + "abstract_text", 20).map(line => (line._1.substring(line._1.lastIndexOf("/") + 1, line._1.length), lemmatize(line._2))).cache()

    // retrieve stopwords
    val stopwords = sc.textFile(IN_PATH + "stopwords.txt").collect()
    val stopwordBroadCast = sc.broadcast(stopwords)

    // retrieve shcemawords
    val schemawords = sc.textFile(IN_PATH + "schema_words.txt").map(line => if(line.contains(",")) {
      line.split(",").toList
    }
    else {
      List(line)
    }).flatMap(line => line).collect()
    val schemawordBroadCast = sc.broadcast(schemawords)

    // get the lemmas and create a sentence for use with triplets
    val lemmaInput = inputf.map(line => {
      val lemmas = line._2

      var temp = ""

      lemmas.foreach(ele => temp += ele._1 + " ")

      val lemmaSent = temp.trim

      // pass in abstract name and lemma sentence
      (line._1, lemmaSent)
    })

    // get triplets
    val input = lemmaInput.map(line => {
      val triples = returnTriplets(line._2, line._1)
      triples
    })

    val triples = input.flatMap(line => line)

    // remove stop words from the triplets
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

      (toCamelCase(subject), toCamelCase(line._2), toCamelCase(obj), line._4, line._5, line._6)
    }).cache()

    // get predicates, subjects, and objects from triplets
    val predicates = triplets.map(line => line._2).distinct().filter(line => line != "")
    val subjects = triplets.map(line => line._1).distinct().filter(line => line != "")
    val objects = triplets.map(line => line._3).distinct().filter(line => line != "")

    // get counts for preidcates and entities (need to remove distinct keyword from sets)
    //subjects.union(objects).map(line => (line, 1)).reduceByKey(_+_).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "entityCount")
    //predicates.map(line => (line, 1)).reduceByKey(_+_).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "predCount")

    // get lemmatized the data
    val lemmatized = inputf.map(line => line._2).cache()
    val flatLemma = lemmatized.flatMap(list => list).filter(line => !stopwordBroadCast.value.contains(line._1))
    val lemmatizedSeq = flatLemma.map(list => List(list._1))

    // get bigrams
    val ngram2LemmaSeq = lemmatized.map(line => {
      val ngrams = getNGrams(line.map(line => line._1).filter(line => !stopwordBroadCast.value.contains(line)).mkString(" ").replaceAll("[.]", ""), 2).map(list => list.mkString(" ")).toList
      ngrams
    })

    // get trigrams
    val ngram3LemmaSeq = lemmatized.map(line => {
      val ngrams = getNGrams(line.map(line => line._1).filter(line => !stopwordBroadCast.value.contains(line)).mkString(" ").replaceAll("[.]", ""), 3).map(list => list.mkString(" ")).toList
      ngrams
    })

    // count the bigrams and trigrams
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

      // get tf-idf and word2vec for medwords
      val tf_idf = TF_IDF(medSeq, sc)
      tf_idf.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outMedTFIDF")

      val word2vec = new Word2Vec().setVectorSize(1000)

      val model = word2vec.fit(lemmatizedSeq)

      // reference: https://stackoverflow.com/questions/4089537/scala-catching-an-exception-within-a-map
      val wordVec = tf_idf.collect().map( word => try{Left((word._1, model.findSynonyms(word._1, 5)))}catch{case e: IllegalStateException => Right(e)})

      val (synonym, errors) = wordVec.partition {_.isLeft}
      val synonyms = synonym.map(_.left.get)

      val data = sc.parallelize(synonyms.map(word => word._1 + ":" + word._2.mkString(",")).toSeq)
      data.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outMedW2V")

      val outMed = flatMed.reduceByKey(_+_)
      outMed.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outMed")

      // output the count of medwords
      val outMedType = medType.reduceByKey(_+_)
      outMedType.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outMedType")

      // retag all medwords that do not have a proper tag
      val medData = medWordData.map(line => if (line._1.toLowerCase.compareTo(line._2.toLowerCase) != 0)
      {
        (toCamelCase(line._1.replaceAll("[']", "").toLowerCase), line._2.toLowerCase.capitalize)
      }
      else
      {
        (toCamelCase(line._1.toLowerCase), "Misc")
      }).distinct().filter(line => line._1.length > 1)

      val workMed = medData.filter(line => line._1.length > 2).toLocalIterator.toSet
      val workTriples = triplets.map(line => (line._1, line._2, line._3))

      // find all subjects with medwords
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

      // find all objects with medwords
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

      // find all entities that contain a schema word
      val schemaTemp = medObjects.union(medSubjects).map(line => line.split(",").drop(1).head)
      val ontSchema = schemaTemp.map(line => {
        var found : Boolean = false
        var schemaWord = "Other"
        schemawordBroadCast.value.foreach(ele => if(line.toLowerCase.replaceAll("[_]", " ").contains(ele.toLowerCase()) && !found) {
          schemaWord = ele
          found = true
        })

        schemaWord + "," + line
      }).cache()

      ontSchema.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "ontSchema")
      ontSchema.map(line => (line.split(",").head, 1)).reduceByKey(_+_).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "ontSchemaCount")

      // work set for schema triplets
      val ontSchemaWork = schemaTemp.map(line => {
        var found : Boolean = false
        var schemaWord = "Other"
        schemawordBroadCast.value.foreach(ele => if(line.toLowerCase.replaceAll("[_]", " ").contains(ele.toLowerCase()) && !found) {
          schemaWord = ele
          found = true
        })

        (schemaWord.replaceAll(" ", ""), line)
      }).cache().toLocalIterator.toSet

      // covert all work triplets that contain a medword into a string with Obj at the end
      val medTriplets = workTriples.map(line => {
        var found : Boolean = false
        workMed.foreach(ele => if((line._1.toLowerCase.contains(ele._1.toLowerCase) || line._3.toLowerCase.contains(ele._1.toLowerCase)) && !found)
        {
          found = true
        })

        if(found)
        {
          line._1 + "," + line._2 + "," + line._3 + "," + "Obj"
        }
        else
        {
          ""
        }
      }).distinct().filter(line => line != "")

      // matches the entities in the schema file with a triplet
      val schemaTriples = medTriplets.map(line => {
        var subj = "Other"
        val item = line.split(",").head
        var obj = "Other"
        val item2 = line.split(",").drop(2).head

        ontSchemaWork.foreach(ele => {
          if(ele._2.compareTo(item) == 0) {
            subj = ele._1
          }

          if(ele._2.compareTo(item2) == 0) {
            obj = ele._1
          }
        })

        (subj, item, line.split(",").drop(1).head, item2, obj)
      }).cache()

      schemaTriples.map(line => line._1 + "," + line._2 + "\n" + line._5 + "," + line._4).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "ontIndivid")
      schemaTriples.map(line => line._2 + "," + line._3 + "," + line._4 + ",Obj").coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "ontTriples")
      schemaTriples.map(line => line._3 + "," + line._1 + "," + line._5 + ",Func").coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "ontObjProp")

      val medSubjectsWork = medSubjects.toLocalIterator.toList
      val medObjectsWork = medObjects.toLocalIterator.toList

      // loop through triplets with medwords and find the BioNLP type for the subject
      val tripSub = medTriplets.map(line => {
        var subj = "Other"
        val item = line.split(",").head
        medSubjectsWork.foreach(ele => if(ele.contains(item)) {
          subj = ele.split(",").head
        })

        (subj, line, item)
      })

      // loop through triplets with medwords and subject type and find the BioNLP type for the object
      val tripBoth = tripSub.map(line => {
        var obj = "Other"
        val item = line._2.split(",").drop(2).head
        medObjectsWork.foreach(ele => if(ele.contains(item)) {
          obj = ele.split(",").head
        })

        (line._2.split(",").drop(1).dropRight(1).head, line._1, line._3, obj, item, line._2)
      })

      // remove any triplet that has no BioNLP tupe
      val medFixed = tripBoth.map(line => if(line._2.compareTo("Other") == 0 && line._4.compareTo("Other") == 0) {
        ("","","","","","")
      }
      else {
        line
      }).distinct().filter(line => line._1.compareTo("") != 0).cache()

      // gets tf-idf for medsubjects and medobjects (needs distinct removed from mebSubjects and medObjects to work correctly)
      //TF_IDF(medSubjects.map(line => List(line.split(",").drop(1).head.replaceAll("[_]", " "))), sc).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medSubTFIDF")
      //TF_IDF(medObjects.map(line => List(line.split(",").drop(1).head.replaceAll("[_]", " "))), sc).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medObjTFIDF")

      // holds totals for med items
      val outMedTotals = List("MedWordCount," + wordMed.count(), "MedTriplets," + medTriplets.count, "MedSubjects," + medSubjects.count,
        "MedObjects," + medObjects.count, "MedWords," + flatMed.count, "FinalTriplets," + medFixed.count)
      sc.parallelize(outMedTotals).map(line => line).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medTotals")

      // output data to files
      medSubjects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medSubjects")
      medObjects.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medObjects")
      medFixed.map(line => line._6).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "medTriplets")
      medFixed.map(line => line._1 + "," + line._2 + "," + line._4 + ",Func").distinct().coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "tripBoth")
      // outputs the non BioNLP type to separate file
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

    // gets tf-idf and word2vec for lemmatized data
    val tf_idf = TF_IDF(lemmatizedSeq, sc)
    tf_idf.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outLemmaTFIDF")

    val word2vec = new Word2Vec().setVectorSize(1000)

    val model = word2vec.fit(lemmatizedSeq)

    // reference: https://stackoverflow.com/questions/4089537/scala-catching-an-exception-within-a-map
    val wordVec = tf_idf.collect().map( word => try{Left((word._1, model.findSynonyms(word._1, 5)))}catch{case e: IllegalStateException => Right(e)})

    val (synonym, errors) = wordVec.partition {_.isLeft}
    val synonyms = synonym.map(_.left.get)

    val data = sc.parallelize(synonyms.map(word => word._1 + ":" + word._2.mkString(",")).toSeq)
    data.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outLemmaW2V")

    // get tf-idf for ngrams
    val tf_idf_ngram2 = TF_IDF(ngram2LemmaSeq, sc)
    tf_idf_ngram2.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outNgram2TFIDF")

    val tf_idf_ngram3 = TF_IDF(ngram3LemmaSeq, sc)
    tf_idf_ngram3.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outNgram3TFIDF")

    // outputs counts
    val wNetCount = wordnetCount.reduceByKey(_+_)
    wNetCount.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outWordNetCount")

    val oPosCount = posCount.reduceByKey(_+_)
    oPosCount.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outPosCount")

    val output = wc.reduceByKey(_+_)
    output.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outCount")

    // outputs totals for non medwrods
    val outTotals = List("WordCount," + wcTotal, "NGram2Count," + ngram2Total.collect().head._2, "NGram3Count," + ngram3Total.collect().head._2, "WordNetCount," + wordnetCountTotal,
      "Subjects," + subjects.count, "Objects," + objects.count, "Predicates," + predicates.count, "Triplets," + triplets.map(line => line._6).sum(), "UniqueTriplets," + triplets.count)
    sc.parallelize(outTotals).map(line => line).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "totals")

    // outputs triplet data
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

  // generates the TF-IDF for the input data
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

  // converts sentences from space separated to underscore separated and CamelCase
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

  // returns the longest triplet per sentence for each input file
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

        // gets the longest triplet based on length of subject and object
        if((subject.length <= temp.first.length) && (obj.length < temp.third.length))
        {
          subject = temp.first
          predicate = temp.second
          obj = temp.third
        }
      }

      lemma += ((subject.toLowerCase, predicate.toLowerCase, obj.toLowerCase, sent.toString, docName, count))
    }
    lemma
  } // end returnTriplets
}
