/**
  * Toy Scala example of support vector machine to classify high dimensional data
  * Created by wrendall on 4/28/16.
  */
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object svn {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Support Vector Machine"))

    //Pull out the first arg to see the mode of operation for SVM
    val mode = args(0)

    //Load some toy data
    val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    //Evaluate to see if real data was provided.
    if (args.length > 1) {
      val data = MLUtils.loadLibSVMFile(sc, args(1))
    }

    //Split the data set into training and test sets randomly
    val split = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = split(0).cache()
    val test = split(1)

    //Define a model based on the SVM with Stochastic Gradient Descent (helps to not get hung up at local minimas)
    val numIter = 200
    val l2param = 0.5

    val SoFisticatedAlgo = new SVMWithSGD
    SoFisticatedAlgo.optimizer.
      setNumIterations(numIter).
      setRegParam(l2param)
    //.setUpdater(new L1Updater)

    //run the model
    val model = SoFisticatedAlgo.run(training)

    //use the model to compute scores for the test data. Return a tuple with the prediction and the label
    val pred = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    //get metrics back
    val metrics = new BinaryClassificationMetrics(pred)
    val AreaUnderRecieverOperatingCharacteristic = metrics.areaUnderROC()

    System.out.println("RocAuc = " + AreaUnderRecieverOperatingCharacteristic)

  }
}

 /*
  * Dear Code Maintainer:
  * How to submit jobs to your local cluster:
  *
  * 1) Set up a local cluster, make sure you've installed spark and scala!
  * 2) Navigate to the spark directory and run ./sbin/start-master.sh
  * 3) Now you've got a master running at spark://sf100058.sofi.private:7077
  * 4) You can view (and kill) applications at http://localhost:8080/
  * 5) Now spin up a worker node, the master will send jobs to it.
  * 6) Run ./sbin/start-slave.sh spark://sf100058.sofi.private:7077
  * 7) Now you can submit jobs and the master will run them!
  * 8) ./bin/spark-submit --class svn --master spark://sf100058.sofi.private:7077 scala-toys.jar gaussian
  * 9) You might run into a signed jar / manifest issue, fix in the future, but for now run the following:
  * 10) zip -d scala-toys.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF
  */
  */
  */
  */