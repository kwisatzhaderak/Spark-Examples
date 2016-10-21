/**
  * Created by wrendall on 9/14/16.
  *
  * This class is intended to provide easy reconstruction of google analytics hit data into a parquet format
  * Data is collected by tracking pixels displayed on wp-landing and borrower / wealth app pages
  * Used as a counterbalance to application data and also a method for determining sources of traffic and ad viewthrus
  * Parquet formatted columnar data can then be read by the UnifiedTable.scala class
  * Along with PII, Attribute, and Internal Borrower, this provides the principal view of Marketing Data Mart
  *
  */

import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.auth.BasicAWSCredentials
import collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.io
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class ImportGoogleAnalytics {
  def main(args: Array[String]) {

    // Set up SC and such, but this will be available in the repl by default if running there
    val conf = new SparkConf().setAppName("ImportMerkle")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Get s3 GA Contents
    val request = new ListObjectsRequest()
    val bucket = "SCRUBBED"
    val prefix = "google-sync/hit_data_20"
    val destination = "s3://SCRUBBED/emr-output/google_agg.parquet"
    val pageLength = 30
    request.setBucketName(bucket)
    request.setPrefix(prefix)
    request.setMaxKeys(pageLength)
    def s3 = new AmazonS3Client()

    def mks3string(key: String): String = "s3n://" + bucket + "/" + key

    def import_csv(s3key: String): org.apache.spark.sql.DataFrame = {
      sqlContext.read.format("com.databricks.spark.csv").
        option("header", "true").
        option("inferSchema","true").
        load(s3key)
    }

    // Note that this method returns truncated data if longer than the "pageLength" above. You might need to deal with that.
    val objs = s3.listObjects(request)
    val obj_keys = objs.getObjectSummaries.map(_.getKey).toList

    // Turn s3 object keys into DataFrames
    val s3ns = obj_keys.map(key => mks3string(key) )

    val s3dfs = s3ns.map(s3path => import_csv(s3path))

    // Great! Now we have DataFrames!
    // Get the column names for each

    val has_all_cols = s3dfs.filter(df => df.columns.size == 89)

    val single_ga = has_all_cols.reduce((DataFrame, n) => DataFrame.unionAll(n))

    val visit_logs = single_ga.groupBy("borrowerId").agg(countDistinct('visitId).as("visits"))

    //Save to file
    visit_logs.write.format("parquet").save(destination)

    // register tables
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.sql("DROP TABLE IF EXISTS default.google_agg")
    visit_logs.saveAsTable("google_agg")
  }

}
