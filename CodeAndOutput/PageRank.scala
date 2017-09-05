


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.MutableList

/* Do not include any other Spark libraries.
 * e.g. import org.apache.spark.sql._ */

/* PageRank
 * This program prints out the 10 pages with the highest pagerank.
 * The only difference between Part5.scala in HW3 and PageRank.scala is that
 * PageRank runs on AWS. */

object PageRank {
    val MASTER_ADDRESS = "ec2-52-24-39-130.us-west-2.compute.amazonaws.com"
    val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
	val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
    val INPUT_DIR = HDFS_MASTER + "/pr/input"
	

    def main(args: Array[String]) {
		
		Logger.getLogger("org").setLevel(Level.OFF)
       	Logger.getLogger("akka").setLevel(Level.OFF)
		
        var links_file  = INPUT_DIR + "/links-simple-sorted.txt"
        var titles_file = INPUT_DIR + "/titles-sorted.txt"
        var num_partitions = 30
        var iters = 10
		
		/*
        if (args.length > 3) {
            System.err.println("Usage: PageRank <links-simple-sorted> <titles-sorted> <iterations>")
            System.exit(1)
        }
        if (args.length == 1) {
            iters = args(0).toInt
        }
        else if (args.length >= 2) {
            links_file = args(0)
            titles_file = args(1)
            if (args.length == 3) {
                iters = args(2).toInt
            }
        }
		*/

        val conf = new SparkConf()
            .setMaster(SPARK_MASTER)
            .setAppName("PageRank")
            //.set("spark.executor.memory", "")

        val sc = new SparkContext(conf)
	
		/////// Spark stuff /////////////////////
        def cleanLinks(node:String, outlinks:String): (Int, List[Int]) = {
			val temp = outlinks.split(" ").filter(_ != "").map(v => v.toInt).toList
			(node.toInt, temp)
			}
		
		def inLinked(node: Int, links: List[Int]): List[(Int, Int)] = {
			var storage = MutableList[(Int, Int)]()
			links.foreach( v => storage += ((v, 1)))
			storage.toList
		}

		////////////////////////////////////////////

        val links = sc
        .textFile(links_file, num_partitions)
        .map{line => 
        val splitup = line.split(":") 
        (splitup(0), splitup(1))}
		.map{ case(k,v) => cleanLinks(k,v) }
        .groupByKey().cache()

        val titles1 = sc
        .textFile(titles_file, num_partitions).cache()
		
		val titles = titles1
        .zipWithIndex()
    	.map{case(k,v) => (v.toInt + 1, k)}.cache()
		
		val N = titles.count
		
		val d = 0.85
		
		val alpha = (((1-d)/N)*100)
		
        var ranks = titles
        .mapValues{ v => (100/N.toDouble)}.cache()
		
        val joined = links
        .rightOuterJoin(titles)
        .map{ case(k,v) => (k, v._1) }
    	.groupByKey().cache()
		
    	val flattened = joined
        .map{ case(k,v) => (k, v.flatten)}
        .map{ case(k,v) => (k, v.flatten)}
        .map{ case(k,v) => (k, v.flatten)}
        .persist()

        /* PageRank */
		
		
		
		
		
        
        for (i <- 1 to iters) {
        
        
	        val generic = flattened
	        .map{ case(k,v) => (k, v.size)}.join(ranks).persist()
        
	        val withoutlinks = flattened
	        .flatMapValues{ v => v }
	        .map{case(k,v) => (v,k)}.groupByKey()
	        .map{case(k,v) => (k,v.toList)}
	        .flatMapValues{ v => v }
	    	.map{case(k,v) => (v,k)}.join(generic)
	    	.map{ case(k,v) => (v._1, (k, v._2))}
			.cache()

	    	val contribs = withoutlinks.map{ case(k, v) => 
	        	(k, v._2._2/v._2._1)}
	        	.reduceByKey(_ + _)
	        	.mapValues(alpha + d * _)
				.persist()
        	
			val ranks1 = ranks.subtractByKey(contribs).mapValues{ v => alpha} // the ones without incoming links

			ranks = contribs.union(ranks1)
            
        }
		
		val sum = ranks.values.sum

  		val x = ranks.map{case(k,v) => (k,((v/sum)*100))}
  		.takeOrdered(10)(Ordering[Double].reverse.on(x => x._2))
		
		x.foreach(println)
		
    }
}
