import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

// classes to represent rows in hive tables
case class Review(productId: String, userId: String, profileName: String, helpfulness: String, score: String, time: String, summary: String, text: String)
case class Movie(movieId: String, title: String, genres: String)
case class Rating(userId: String, movieId: String, rating: Double)
case class Tag(userId: String, movieId: String, tag: String)

// create hive tables
object Tables {
	def main (args: Array[String]) {
		
		// initialise spark context
		val conf = new SparkConf().setAppName("Tables")
		val sc = new SparkContext(conf)
		
		// initialise sql context
		val hiveContext = new HiveContext(sc)
		import hiveContext.implicits._

		// create 'reviews' table
		val mConf = new Configuration(sc.hadoopConfiguration)
		mConf.set("textinputformat.record.delimiter", "\n\n")
		val reviews = sc.newAPIHadoopFile("moviesample.txt", classOf[TextInputFormat], classOf[LongWritable], classOf[Text], mConf).map(_._2.toString)
		val records = reviews.map(record => record.split("\n"))
		val cleanRecords = records.map(record => record.map(field => field.split(": ", 2)(1)))
		val data = cleanRecords.map(r => Review(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7))).toDF()
		data.saveAsTable("reviews")
		
		// create table to map asin to movie title
		val metadata = hiveContext.read.json("/Users/liam/Downloads/metadata.json")
		metadata.registerTempTable("metadata")
		val asinTitles = hiveContext.sql("SELECT asin, title from metadata")
		asinTitles.saveAsTable("asinTitles")
		
		// create 'movies' table
		val moviesLines = sc.textFile("ml-latest/movies.csv")
		val moviesFields = moviesLines.map(l => l.split(","))
		val movies = moviesFields.map(l => Movie(l(0), l(1), l(2))).toDF()
		movies.saveAsTable("movies")
		
		// create 'ratings' table
		val ratingsLines = sc.textFile("ml-latest/ratings.csv")
		val ratingsFields = moviesLines.map(l => l.split(","))
		val ratings = moviesFields.map(l => Rating(l(0), l(1), l(2))).toDF()
		ratings.saveAsTable("ratings")
		
		// create 'tags' table
		val tagsLines = sc.textFile("ml-latest/tags.csv")
		val tagsFields = moviesLines.map(l => l.split(","))
		val tags = moviesFields.map(l => Tag(l(0), l(1), l(2))).toDF()
		tags.saveAsTable("tags")
	}
}