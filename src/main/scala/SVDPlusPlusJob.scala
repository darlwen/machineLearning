import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.lib.SVDPlusPlus
import org.apache.spark.storage.StorageLevel
import org.apache.spark.scheduler.InputFormatInfo
import org.slf4j.LoggerFactory

/**
 * Created by darlwen on 14-8-14.
 */

object SVDPlusPlusJob {
  protected val logger = Logger(LoggerFactory getLogger "SVDPlusPlus")

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkHdfs <file>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Simple Job SVDPlusPlus.")
    val svdppErr = 8.0
    val inputPath = args(0)
    val partitionNum = args(1).toInt
    val iterNum = args(2).toInt
    val topicNum = args(3).toInt
    val minValue = args(4).toDouble
    val maxValue = args(5).toDouble
    val segment = args(6).toString
    val outputPath = args(7).toString
    val param1 = args(8).toDouble
    val param2 = args(9).toDouble
    val param3 = args(10).toDouble
    val param4 = args(11).toDouble

    val conf = SparkHadoopUtil.get.newConfiguration()
    val sc = new SparkContext(sparkConf,
      InputFormatInfo.computePreferredLocations(
        Seq(new InputFormatInfo(conf, classOf[org.apache.hadoop.mapred.TextInputFormat], inputPath))
      ))

    val edges = sc.textFile(inputPath).map{ line =>
      val fields = line.split(segment)
      Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
    }.repartition(partitionNum)

    val svdPlusPlusconf = new SVDPlusPlus.Conf(topicNum, iterNum, minValue, maxValue, param1, param2, param3, param4) // 2 iterations
    var (graph, u) = SVDPlusPlus.run(edges, svdPlusPlusconf)
    graph.cache()
 /*
    val err = graph.vertices.collect().map{ case (vid, vd) =>
      if (vid % 2 == 1) vd._4 else 0.0
    }.reduce(_ + _) / graph.triplets.collect().size
 */

  val tol_err = graph.vertices.map{ case (vid, vd) =>
     if (vid % 2 == 1) vd._4 else 0.0
    }.reduce(_ + _)
  val tol_num = graph.triplets.count()
  logger.info("tol_err is: " + tol_err)
  logger.info("tol_num is: " + tol_num)
  val err = tol_err / tol_num
/*
  val err = graph.vertices.map{ case (vid, vd) =>
     if (vid % 2 == 1) vd._4 else 0.0
    }.reduce(_ + _) / graph.triplets.count()
*/
    logger.info("err is " + err)
  }

}
