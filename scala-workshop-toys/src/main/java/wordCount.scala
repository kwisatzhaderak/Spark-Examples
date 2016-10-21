import org.apache.spark.{SparkConf, SparkContext}

/**
  * Toy example of how to do data processing in Scala with Spark
  * Created by wrendall on 4/28/16.
  */
object wordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val dat = sc.parallelize("/home/wrendall/wc")
    val threshold = 0

    //Split the documents into words
    val tokenized = dat.flatMap(_.split(" "))

    //Count word occurences
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    System.out.println(wordCounts.collect().mkString(","))

    //Filter out words with less than threshold occusrences
    val filtered = wordCounts.filter(_._2 >= threshold)

    //Count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    //Print results, make sure to use an "action" like collect to force spark to do work
    System.out.println(charCounts.collect().mkString(","))

  }
}


 /*
  * How to submit jobs to your local cluster:
  *
  * 1) Set up a local cluster, make sure you've installed spark and scala!
  * 2) Navigate to the spark directory and run ./sbin/start-master.sh
  * 3) Now you've got a master running at spark://sf100058.sofi.private:7077
  * 4) You can view (and kill) applications at http://localhost:8080/
  * 5) Now spin up a worker node, the master will send jobs to it.
  * 6) Run ./sbin/start-slave.sh spark://sf100058.sofi.private:7077
  * 7) Now you can submit jobs and the master will run them!
  * 8) ./bin/spark-submit --class wordCount --master spark://sf100058.sofi.private:7077 scala-toys.jar sample.txt 1
  * 9) You might run into a signed jar / manifest issue, fix in the future, but for now run the following:
  * 10) zip -d scala-toys.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF
  */
  */
  */
  */
