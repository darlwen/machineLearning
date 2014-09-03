import com.typesafe.scalalogging.slf4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.lib.LDA
import org.apache.spark.storage.StorageLevel
import org.apache.spark.scheduler.InputFormatInfo
import org.slf4j.LoggerFactory

/**
 * Created by darlwen on 14-8-25.
 */

object GibbsSamplingLDAJob {

  protected val logger = Logger(LoggerFactory getLogger "GibbsSamplingLDAJob")

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkHdfs <file>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Simple Job GibbsSamplingLDA.")
    val inputPath = args(0)
    val partitionNum = args(1).toInt
    val iterNum = args(2).toInt
    val topicNum = args(3).toInt
    val alpha = args(4).toDouble
    val beta = args(5).toDouble
    val vocNum = args(6).toInt
    val segment = args(7).toString

    val conf = SparkHadoopUtil.get.newConfiguration()
    val sc = new SparkContext(sparkConf,
      InputFormatInfo.computePreferredLocations(
        Seq(new InputFormatInfo(conf, classOf[org.apache.hadoop.mapred.TextInputFormat], inputPath))
      ))

    val edges = sc.textFile(inputPath).map { line =>
      val fields = line.split(segment)
      Edge(fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toInt)
    }.repartition(partitionNum)

    val LDAconf = new LDA.Conf(topicNum, vocNum, alpha, beta, iterNum) // 2 iterations
    var graph = LDA.run(edges, LDAconf)
    graph.cache()

  }

}
