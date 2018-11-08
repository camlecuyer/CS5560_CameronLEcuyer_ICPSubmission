import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.util.Properties
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import org.apache.spark.{SparkConf, SparkContext}
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec}
import scala.collection.JavaConversions._
import rita.RiWordNet
import scala.collection.immutable.HashMap
import scala.io.Source
import scala.collection.mutable.ListBuffer

object SparkWordCount {

  private val REST_URL = "https://data.bioontology.org"
  private val API_KEY = ""
  private val mapper: ObjectMapper = new ObjectMapper

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // retrieve data
    val inputf = sc.wholeTextFiles("C:\\Users\\camle\\Box\\data\\abstract_text", 2)

    // lemmatize the data
    val lemmatized = inputf.map(line => lemmatize(line._2))
    val flatLemma = lemmatized.flatMap(list => list)
    val lemmatizedSeq = flatLemma.map(list => List(list._1).toSeq)

    // count for all words and count for all parts of speech
    val wc = flatLemma.map(word =>(word._1 + "," + word._2, 1))
    val wcTotal = flatLemma.map(word =>("total", 1))
    val posCount = flatLemma.map(word => (word._2, 1))

    // count for wordnet words
    val wordnetCount = flatLemma.map(word => if(new RiWordNet("C:\\WordNet\\WordNet-3.0").exists(word._1)) (word._1 + "," + word._2, 1) else (word._1 + "," + word._2, 0))
    val wordnetCountTotal = flatLemma.map(word => if(new RiWordNet("C:\\WordNet\\WordNet-3.0").exists(word._1)) ("total", 1) else ("total", 0))


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

          val medData = sc.parallelize(medWords.toList)
          val flatMed = medData.map(word => (word._1.toLowerCase + ", " + word._2.toLowerCase, 1))
          val medType = medData.map(word => (word._2.toLowerCase, 1))
          val wordMed = medData.map(word => word._1.toLowerCase)
          val medString = wordMed.collect().distinct.mkString(" ") // string for ontologies
          val medSeq = wordMed.map(word => List(word).toSeq)

          //Creating an object of HashingTF Class
          val hashingTF = new HashingTF()

          //Creating Term Frequency of the document
          val tf = hashingTF.transform(medSeq)
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

          val documentData = medSeq.flatMap(_.toList)

          val dd = documentData.map(f => {
            val i = hashingTF.indexOf(f)
            val h = mapp.value
            (f, h(i.toString))
          })

          val dd1 = dd.distinct().sortBy(_._2, false)

          val word2vec = new Word2Vec().setVectorSize(1000)

          val model = word2vec.fit(medSeq)

          // reference: https://stackoverflow.com/questions/4089537/scala-catching-an-exception-within-a-map
          val wordVec = dd1.collect().map( word => try{Left((word._1, model.findSynonyms(word._1, 5)))}catch{case e: IllegalStateException => Right(e)})

          val (synonym, errors) = wordVec.partition {_.isLeft}
          val synonyms = synonym.map(_.left.get)

          dd1.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outMedTFIDF")

          val data = sc.parallelize(synonyms.map(word => word._1 + ":" + word._2.mkString(",")).toSeq)
          data.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outMedW2V")

          val outMed = flatMed.reduceByKey(_+_)
          outMed.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outMed")

          val outMedType = medType.reduceByKey(_+_)
          outMedType.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outMedType")

          val textToAnnotate = URLEncoder.encode(medString, "ISO-8859-1")

          // Get just annotations of data for ontologies
          val urlParameters = "/annotator?text=" + textToAnnotate
          val annotations: JsonNode = jsonToNode(getOntologies(REST_URL + urlParameters))

          // get ontologies
          val ontData = annotations.map(annotation => getOntologyData(annotation.get("annotatedClass").get("links").get("self").asText))

          val ontWords = ontData.flatMap(list => list)
          val ontCount = ontWords.toList.map(list => (list._1, 1))
          val ontologies = ontWords.toList.map(list => (list._1, list._2)).distinct
          sc.parallelize(ontologies).saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outOnt")

          val outOnt = sc.parallelize(ontCount).reduceByKey(_+_)
          outOnt.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outOntCount")
        } // end if
      }
    })

    // start thread for medical data
    medThread.start

    //Creating an object of HashingTF Class
    val hashingTF = new HashingTF()

    //Creating Term Frequency of the document
    val tf = hashingTF.transform(lemmatizedSeq)
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

    val documentData = lemmatizedSeq.flatMap(_.toList)

    val dd = documentData.map(f => {
      val i = hashingTF.indexOf(f)
      val h = mapp.value
      (f, h(i.toString))
    })

    val dd1 = dd.distinct().sortBy(_._2, false)

    val word2vec = new Word2Vec().setVectorSize(1000)

    val model = word2vec.fit(lemmatizedSeq)

    // reference: https://stackoverflow.com/questions/4089537/scala-catching-an-exception-within-a-map
    val wordVec = dd1.collect().map( word => try{Left((word._1, model.findSynonyms(word._1, 5)))}catch{case e: IllegalStateException => Right(e)})

    val (synonym, errors) = wordVec.partition {_.isLeft}
    val synonyms = synonym.map(_.left.get)

    dd1.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outLemmaTFIDF")

    val data = sc.parallelize(synonyms.map(word => word._1 + ":" + word._2.mkString(",")).toSeq)
    data.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outLemmaW2V")

    val wNetCount = wordnetCount.reduceByKey(_+_)
    wNetCount.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outWordNetCount")

    val oPosCount = posCount.reduceByKey(_+_)
    oPosCount.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outPosCount")

    val output = wc.reduceByKey(_+_)
    output.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outCount")

    val outTotal = wcTotal.reduceByKey(_+_)
    outTotal.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outTotal")

    val outWordnetTotal = wordnetCountTotal.reduceByKey(_+_)
    outWordnetTotal.saveAsTextFile("C:\\Users\\camle\\Box\\outputFiles\\outWordnetTotal")
  } // end main

  // lemmatizes input string
  // code referenced from https://stackoverflow.com/questions/30222559/simplest-method-for-text-lemmatization-in-scala-and-spark
  def lemmatize(text: String): ListBuffer[(String, String)] = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val document = new Annotation(text)
    pipeline.annotate(document)
    /*val stopwords = List("a", "an", "the", "and", "are", "as", "at", "be", "by", "for", "from", "has", "in", "is",
      "it", "its", "of", "on", "that", "to", "was", "were", "will", "with", "''", "``")*/

    val lemmas = ListBuffer.empty[(String, String)]
    val sentences = document.get(classOf[SentencesAnnotation])

    // for each token get the lemma and part of speech
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      val pos = token.get(classOf[PartOfSpeechAnnotation])

      if (lemma.length > 1) {
        lemmas += ((lemma.toLowerCase, pos.toLowerCase))
      } // end if
    } // end loop
    lemmas
  } // end lemmatize

  // gets data for medical words from URL
  def get(url: String) = scala.io.Source.fromURL(url).getLines()

  // changes string to JsonNode
  def jsonToNode (json: String): JsonNode =
  {
    var root: JsonNode = null
    try
      root = mapper.readTree(json)
    catch {
      case e: Exception =>
        None
    }
    root
  }

  // retrieves ontologies
  def getOntologies(urlIn: String): String = {
    var line: String = null
    var result: String = ""

    try {
      val url = new URL(urlIn)
      val conn = url.openConnection.asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.setRequestProperty("Authorization", "apikey token=" + API_KEY)
      conn.setRequestProperty("Accept", "application/json")

      val rd = new BufferedReader(new InputStreamReader(conn.getInputStream))
      while({line = rd.readLine; line != null})
        result += line
      rd.close()
    } catch {
      case e: Exception =>
        None
    } // end try/catch

    result
  } // end getOntologies

  // parses the data for the individual ontologies
  def getOntologyData(urlIn: String): ListBuffer[(String, String)] = {
    val ontWords = ListBuffer.empty[(String, String)]
    var line: String = null
    var result: String = ""

    try {
      val url = new URL(urlIn)
      val conn = url.openConnection.asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.setRequestProperty("Authorization", "apikey token=" + API_KEY)
      conn.setRequestProperty("Accept", "application/json")

      val rd = new BufferedReader(new InputStreamReader(conn.getInputStream))
      while({line = rd.readLine; line != null})
        result += line
      rd.close()

      val classDetails: JsonNode = jsonToNode(result)

      if(classDetails != null)
      {
        ontWords += ((classDetails.get("prefLabel").asText.toLowerCase, classDetails.get("links").get("ontology").asText))
      } // end if
    } catch {
      case e: Exception =>
        None
    } // end try/catch
    ontWords
  } // end getOntologyData
}
