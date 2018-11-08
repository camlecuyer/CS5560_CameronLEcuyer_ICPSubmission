import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL, URLEncoder}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._
import scala.io.Source
import scala.collection.mutable.ListBuffer

object SparkWordCount {

  private val REST_URL = "https://data.bioontology.org"
  private val API_KEY = ""
  private val mapper: ObjectMapper = new ObjectMapper
  private val OUT_PATH = "C:\\Users\\CJ\\Box\\projectOutput\\"

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

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
      val wordMed = medData.map(word => word._1.toLowerCase)
      val medString = wordMed.distinct().top(100).mkString(" ") // string for ontologies

      val textToAnnotate = URLEncoder.encode(medString, "ISO-8859-1")

      // Get just annotations of data for ontologies
      val urlParameters = "/annotator?text=" + textToAnnotate
      val annotations: JsonNode = jsonToNode(getOntologies(REST_URL + urlParameters))

      // get ontologies
      val ontData = annotations.map(annotation => getOntologyData(annotation.get("annotatedClass").get("links").get("self").asText))

      val ontWords = ontData.flatMap(list => list)
      val ontCount = ontWords.toList.map(list => (list._1, 1))
      val ontologies = ontWords.toList.map(list => (list._1, list._2)).distinct
      sc.parallelize(ontologies).coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outOnt")

      val outOnt = sc.parallelize(ontCount).reduceByKey(_+_)
      outOnt.coalesce(1, shuffle = true).saveAsTextFile(OUT_PATH + "outOntCount")
    } // end if
  } // end main

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
