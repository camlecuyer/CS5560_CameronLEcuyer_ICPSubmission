import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object Reasoning {
  private val IN_PATH = "C:\\Users\\camle\\Box\\projectOutput\\ontTriples\\part-00000"

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val input = sc.textFile(IN_PATH, 4)

    val triples = input.map(line => {
      val data = line.split(",").dropRight(1)

      (data.head, data.drop(1).head, data.drop(2).head)
    }).cache().toLocalIterator.toArray

    var reasonList: ListBuffer[String] = new ListBuffer[String]

    triples.foreach(ele => {
      triples.foreach(ele1 => if(ele._1.compareTo(ele1._3) == 0  && ele._3.compareTo(ele1._1) == 0 && ele._2.compareTo(ele1._2) == 0) {
        reasonList += "Sym:" + ele._1 + "," + ele._2 + "," + ele._3 + ";" + ele1._1 + "," + ele1._2 + "," + ele1._3
      })
    })

    triples.foreach(ele => {
      triples.foreach(ele1 => if(ele._1.compareTo(ele1._3) == 0  && ele._3.compareTo(ele1._1) == 0 && ele._2.compareTo(ele1._2) != 0) {
        reasonList += "Inv:" + ele._1 + "," + ele._2 + "," + ele._3 + ";" + ele1._1 + "," + ele1._2 + "," + ele1._3
      })
    })

    triples.foreach(ele => {
      triples.foreach(ele1 => if(ele._1.compareTo(ele1._3) != 0  && ele._3.compareTo(ele1._1) == 0 && ele._2.compareTo(ele1._2) == 0) {
        reasonList += "Tra:" + ele._1 + "," + ele._2 + "," + ele._3 + ";" + ele1._1 + "," + ele1._2 + "," + ele1._3
      })
    })

    triples.foreach(ele => {
      triples.foreach(ele1 => if(ele._1.compareTo(ele1._3) != 0  && ele._3.compareTo(ele1._1) == 0 && ele._2.compareTo(ele1._2) != 0) {
        reasonList += "Cha:" + ele._1 + "," + ele._2 + "," + ele._3 + ";" + ele1._1 + "," + ele1._2 + "," + ele1._3
      })
    })

    triples.foreach(ele => {
      triples.foreach(ele1 => if(ele._1.compareTo(ele1._3) == 0  && ele._3.compareTo(ele1._1) == 0 && ele._2.compareTo("beParent") == 0) {
        reasonList += "Asy:" + ele._1 + "," + ele._2 + "," + ele._3 + ";" + ele1._1 + "," + ele1._2 + "," + ele1._3
      })
    })

    triples.foreach(ele => {
      if(ele._1.compareTo(ele._3) == 0) {
        reasonList += "Irr:" + ele._1 + "," + ele._2 + "," + ele._3
      }
    })

    reasonList.toList.foreach(println)
  }
}
