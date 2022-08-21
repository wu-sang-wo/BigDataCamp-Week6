import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//参考https://blog.csdn.net/nzbing/article/details/124210806

object index {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("bingbing").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    //调用Spark读取文件API,读取文件内容
    val project_path = file.getAbsolutePath() //获取项目的绝对路径。这里的级别是project的根目录
    val fileName = project_path + "\\index.txt" //在根目录的基础上加上文件的具体相对路径
    val wordRDD= sc.textFile(fileName)
    //使用flatMap进行分词后展开
      .flatMap {
        line =>
        //以.做分词需要加转义符
          val array = line.split("\\.", 2)
          val bookName = array(0)
          array(1).split("\"")(1).split(" ").map(word => (bookName, word))
      }

    val kvRDD= wordRDD.map(kv => (kv._2, kv._1)).map((_, 1))
      .reduceByKey((x,y) => x + y)
      .map{case ((k,v),cnt) => (k,(v,cnt))}
      .groupByKey() //只分组不聚合
      .collect()
      .foreach(println)
  }
}
