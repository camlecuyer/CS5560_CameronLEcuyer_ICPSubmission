import org.apache.spark.{SparkConf, SparkContext}
import rita.RiWordNet

object WordNetSpark {
  // this code takes the tokenized and lemmatized data and gives it to WordNet so WordNet can validate the words
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("WordNetSpark").setMaster("local[*]").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)
    val NUM = 11

    val data = sc.textFile("data/token_results/"+ NUM +"_token.txt")

    // counter for the number of words WordNet recognizes
    var count = 0

    // sends the lemmatized word to WordNet and displays the word and the count
    val dd = data.map(line=>{
      val wordnet = new RiWordNet("C:\\WordNet\\WordNet-3.0")
      val wordSet = line.split(" ")

      if(wordnet.exists(wordSet(1)))
        {
          count += 1
          println(wordSet(1) + " " + count)
        }
    })
    dd.collect()
  }
}
