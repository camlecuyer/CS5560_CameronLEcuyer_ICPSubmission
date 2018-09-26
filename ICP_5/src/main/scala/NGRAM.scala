
/**
  * Created by Mayanka on 19-06-2017.
  */
object NGRAM {

  def main(args: Array[String]): Unit = {
    val a = getNGrams("the bee is the bee of the bees",3)
    //a.foreach(f=>println(f.mkString(" ")))
    //a.foreach(f=> println(f.mkString(" ")))
    a.map(f => f.mkString(" ")).foreach(println)
  }

  def getNGrams(sentence: String, n:Int): IndexedSeq[List[String]] = {
    val words = sentence
    val ngrams = (for(i <- 1 to n) yield words.split(' ').sliding(i).map(word => word.toList)).flatMap(x => x)
    ngrams
  }

}


