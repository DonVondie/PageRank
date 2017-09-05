import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.MutableList

/* Do not include any other Spark libraries.
 * e.g. import org.apache.spark.sql._ */

/* NoInOutCore
 * This program prints out the first 10 pages with no outlinks (ascending order
 * in index) and the first 10 pages with no inlinkns (ascending order in index
 * as well).
 * The only difference between Part4.scala in HW3 and NoInOutCore.scala is that
 * NoInOutCore runs on AWS. */

/*



*/


object NoInOutCore {
	
    val MASTER_ADDRESS = "ec2-52-38-138-36.us-west-2.compute.amazonaws.com" 
	
    val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
    val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
    val INPUT_DIR = HDFS_MASTER + "/noinout/input"

    def main(args: Array[String]) {
		
		Logger.getLogger("org").setLevel(Level.OFF)
       	Logger.getLogger("akka").setLevel(Level.OFF)
		
        var links_file  = INPUT_DIR + "/links-simple-sorted.txt"
        var titles_file = INPUT_DIR + "/titles-sorted.txt"
        var num_partitions = 20
		
        def cleanLinks(node:String, outlinks:String): (Int, List[Int]) = {
			val temp = outlinks.split(" ").filter(_ != "").map(v => v.toInt).toList
			(node.toInt, temp)
			}
		
		def inLinked(node: Int, links: List[Int]): List[(Int, Int)] = {
			var storage = MutableList[(Int, Int)]()
			links.foreach( v => storage += ((v, 1)))
			storage.toList
		}
		
		
		/*
        if (args.length != 0 && args.length != 2) {
            System.err.println("Usage: NoInOutCore <links-simple-sorted> <titles-sorted>")
            System.exit(1)
        }
        if (args.length == 2) {
            links_file = args(0)
            titles_file = args(1)
        }
		*/
		
		// For the input directory

        val conf = new SparkConf()
            .setMaster(SPARK_MASTER)
            .setAppName("NoInOutCore")
            //.set("spark.executor.memory", "")

        val sc = new SparkContext(conf)
		
		////////////////////////////////////////////
		
        val links = sc
        .textFile(links_file, num_partitions)
        .map{line => 
        val splitup = line.split(":") 
        (splitup(0), splitup(1))}
		.map{ case(k,v) => cleanLinks(k,v) }
        .distinct().groupByKey().cache()

        val titles = sc
        .textFile(titles_file, num_partitions)
        .zipWithIndex()
    	.map{case(k,v) => (v.toInt + 1, k)}.cache()
		
		///////////////////////////////////////////

        /* No Outlinks */
        val joined = links
        .rightOuterJoin(titles)
        .map{ case(k,v) => (k, v._1) }
    	.groupByKey().cache()
    	

    	val withoutLetters = joined
    	.map{ case(k,v) => (k, v.flatten)}
    	.filter{ case(k,v) => v.isEmpty }
			
			
    	val no_outlinks = withoutLetters
    	.leftOuterJoin(titles)
    	.map{ case(k,v) => (k, v._2)}
		.sortByKey()
		.take(10)
		
		
		println("\n[ NO OUTLINKS ]")
		no_outlinks.foreach(println)
		
		//////////////////////////////////////
		
		
		
        val start = links
    	.map{ case(k,v) => (k, v.toList)}  
    	.map{ case(k,v) => (k, v.flatten)} 
    	.flatMap{ case(k,v) => inLinked(k,v)}
    	.distinct().groupByKey().cache()
    	
    	
    	val no_inlinks = start
    	.rightOuterJoin(titles)
    	.filter{ case(k,v) => v._1 == None }
    	.map{ case(k,v) => (k, v._2) }
		.sortByKey()
		.take(10)
		
    	
        println("\n[ NO INLINKS ]")
        no_inlinks.foreach(println)
		
		
    }
}
