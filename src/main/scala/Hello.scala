
import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json._

object Hello {
  def get_files(dir:String):Array[File] = {

    val d = new File(dir)

    if (!d.exists() || !d.isDirectory()) {
      return Array[File]()
    }

    return d.listFiles()
  }
  def main_(args: Array[String]): Unit = {
    var files = get_files("/home/darkgs/Dataset/Adressa/three_month")

    for (file <- files) {
      println(file.getAbsolutePath())
    }
  }
  def main___(args: Array[String]): Unit = {
    // Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word count")
    val sc = new SparkContext(conf)

    var files = get_files("/home/darkgs/Dataset/Adressa/three_month")

    for (file <- files) {
      var textFile = sc.textFile(file.getAbsolutePath())

      var textJSON = textFile.map(x => JSON.parseFull(x) match {
        case Some(x) => {
          val m:Map[String,String] = x.asInstanceOf[Map[String,String]]
          if (m.keySet.contains("userId")) ("userId", m("userId")) else Nil
        }
        case None => Nil
      }).filter(x => x != Nil)

      textJSON.take(5).foreach(x => println(x))
      /*
      .filter(x => x.asInstanceOf[Map[String,String]].getOrElse("city", "").equals("oslo"))
      */
    }

  }

  def main__(args: Array[String]): Unit = {
    // Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word count")
    val sc = new SparkContext(conf)

    var textFile = sc.textFile("/home/darkgs/Dataset/Adressa/three_month")

    println(textFile.count())
    textFile.take(5).foreach(x => println(x))
  }

  def find_best_url(event: Map[String,String]): String = {
    val url_keys:List[String] = List("url", "cannonicalUrl", "referrerUrl")
    val black_list:List[String] = List("http://adressa.no", "http://google.no", "http://facebook.com")

    var candidates:List[String] = List()

    url_keys.foreach(url_key =>
      if (event.keySet.contains(url_key)) {
        if (!black_list.contains(event(url_key))) {
          candidates = candidates :+ event(url_key)
        }
      }
    )

    var best_candidate:String = ""

    candidates.foreach(candidate =>
      if ((candidate.length() > best_candidate.length) &&
        (candidate.count(_ == '/') > 2)){
        best_candidate = candidate
      }
    )

    return if (best_candidate.length() > 0) best_candidate else null
  }

  def main(args: Array[String]): Unit = {

    // Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word count")
    val sc = new SparkContext(conf)

    var textFile = sc.textFile("/home/darkgs/Dataset/Adressa/one_week")

    textFile.map(x => JSON.parseFull(x) match {
      case Some(x) => {
        val event: Map[String, String] = x.asInstanceOf[Map[String, String]]

        val userId: String = event.getOrElse("userId", null) match {
          case x: String => x
          case _ => null
        }

        val best_url: String = find_best_url(event)

        val time: Int = event.getOrElse("time", 0) match {
          case x: Int => x
          case x: Double => x.toInt
          case x: String => Integer.getInteger(x, 0)
          case _ => 0
        }

        if ((userId != null) && (best_url != null)) {
          best_url
        } else {
          Nil
        }
      }
      case None => Nil
    }).filter(x => x != Nil)
      .distinct()
      .coalesce(1, true).saveAsTextFile("result")

    /*
    val sqlContext = new SQLContext(sc)

    var textFile = sc.textFile("/home/darkgs/Dataset/Adressa/three_month_test/test")

    textFile.map(x => sqlContext.read.json(x))
      .reduce
      */


    /*
    var testRDD = sc.parallelize(List("aaa", "bbb", "aaa", "c"))

    testRDD.map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .foreach(x => println(x))
      */
  }
}


