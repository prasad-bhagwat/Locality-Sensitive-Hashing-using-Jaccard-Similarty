import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import java.io._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

trait Config_params {
  val num_partitions  = 12
  val num_hash_funcs  = 12
  val num_buckets     = 673
  val hash_func_a     = 7
  val hash_func_b     = 3
  val threshold       = 0.5
}


object JaccardLSH extends Config_params {
  // Generating Characteristic matrix of size (users x List(movies))
  // eg. List(movies) = List(0, 1, 1, 0, 1, 0, 0)
  def gen_character_matrix(values: Iterable[Int], movie_length: Int, movie_list: List[Int]): Array[Int] = {
    // Initializing movie list with 0's
    val columns = ArrayBuffer.fill(movie_length)(0)
    // Looping over each movie
    for (movie <- values.toList) {
      // Storing only indexes of movies which are present instead of all 0 to n movies
      val index       = movie_list.indexOf(movie)
      columns(index)  = 1
    }
    columns.toArray
  }


  def gen_hash_signature_matrix(user_movie_list: Array[(Int, Array[Int])], user_length: Int, movie_length: Int): Array[Array[Int]] = {
    //Hash signatures matrix of size (users x hash_functions)
    val hash_signature_matrix = Array.fill(user_length, num_hash_funcs)(0)
    // Movie signatures matrix of size (hash_functions x movies)
    val movie_signature_matrix = Array.fill(num_hash_funcs, movie_length)(99999)
    // Looping over all the users
    var user = 0
    while (user < user_length) {
      // Generating hash signatures for all users
      var hash = 0
      while (hash < num_hash_funcs) {
        // Using hash function (ax + b) % m
        hash_signature_matrix(user)(hash) = (hash_func_a * user + hash_func_b * hash) % num_buckets
        hash += 1
      }

      // Looping over all the movies
      var movie = 0
      while (movie < movie_length) {
        // Checking occurance of "1" in the list of movies
        if (user_movie_list(user)._2(movie) == 1) {
          var hash_func = 0
          while (hash_func < num_hash_funcs) {
            if (hash_signature_matrix(user)(hash_func) < movie_signature_matrix(hash_func)(movie)) {
              movie_signature_matrix(hash_func)(movie) =
                hash_signature_matrix(user)(hash_func)
            }
            hash_func += 1
          }
        }
        movie += 1
      }
      user += 1
    }
    movie_signature_matrix
  }


  // LSH algorithm implementation for each of the bands
  def algorithm_LSH(movie_hash_partition_rdd: Iterator[Array[Int]]): Iterator[(Int, Int)] = {
    var candidate_pair_list       = ListBuffer[(Int, Int)]()
    val movie_hash_partition_list = movie_hash_partition_rdd.toArray
    // Transpose the list of lists to get matrix of size (hash_functions x movies)
    val hashed_movie = movie_hash_partition_list.transpose

    // Check if movies have similar signatures within the band, if Yes emit the tuple of candidate pair
    var index_1 = 0
    while (index_1 < hashed_movie.length) {
      var index_2 = index_1 + 1
      while (index_2 < hashed_movie.length) {
        if (hashed_movie(index_1) sameElements hashed_movie(index_2)) {
          candidate_pair_list.append((index_1, index_2))
        }
        index_2 += 1
      }
      index_1 += 1
    }
    candidate_pair_list.iterator
  }



  // Calculating Jaccard Similarity between candidate pairs
  def calculate_jaccard(candidate_pair: Iterator[(Int, Int)], transposed_characteristic_matrix: Array[Array[Int]], movie_list: List[Int]): Iterator[((Int, Int), Float)] = {
    val candidate_list        = candidate_pair.toList
    var candidate_pair_list   = ListBuffer[((Int, Int), Float)]()
    // Looping over all the candidates of Candidate pair list
    for (candidate <- candidate_list){
      // Creating summed of list of
      val summed_list = (transposed_characteristic_matrix(candidate._1), transposed_characteristic_matrix(candidate._2)).zipped.map(_ + _)
      // Calculating Jaccard Similarity between movies from the candidate pair
      var jaccard_similarity = summed_list.count(_ == 2).toFloat / (summed_list.count(_ == 1) + summed_list.count(_ == 2)).toFloat
      // Sorting the candidate pairs which are above given threshold
      if (jaccard_similarity >= threshold){
        if (movie_list(candidate._1) < movie_list(candidate._2)) {
          candidate_pair_list = candidate_pair_list :+ ((movie_list(candidate._1), movie_list(candidate._2)), jaccard_similarity)
        }else {
          candidate_pair_list = candidate_pair_list :+ ((movie_list(candidate._2), movie_list(candidate._1)), jaccard_similarity)
        }
      }
    }
    candidate_pair_list.iterator
  }

  // Main function
  def main(args: Array[String]) {
    val start_time = System.nanoTime()
    // Command Line Arguments
    val input_file = args(0)

    // Output filename generation
    val output_file_name    = "Prasad_Bhagwat_SimilarMovies_Jaccard.txt"
    val output_file         = new PrintWriter(new File(output_file_name))
    var output_result       = ""

    // Creating Spark Context
    val spark_config  = new SparkConf()
    val spark_context = new SparkContext(spark_config)
    spark_context.setLogLevel("WARN")

    // Reading file and extracting header
    val input           = spark_context.textFile(input_file).filter(x => ! x.contains("userId"))
    val data            = input.map( x => {
      val y = x.split(",")
      (y(0).toInt, y(1).toInt)
    }).groupByKey().persist()
    val user_list       = data.keys.distinct().collect().toList
    val user_length     = user_list.size
    val movie_list      = data.flatMap( x => x._2).distinct().collect().toList
    val movie_length    = movie_list.size

    // Original characteristic matrix
    val character_matrix            = data.mapValues( x => gen_character_matrix(x, movie_length, movie_list)).collect()
    val character_matrix_list       = character_matrix.map(element => element._2)
    val transposed_character_matrix = character_matrix_list.transpose

    // Generate Movie-Hash Signature matrix
    val movie_signature_matrix  = gen_hash_signature_matrix(character_matrix, user_length, movie_length)

    // Transforming Signature matrix into RDD and using mapPartitions on it using 4 partitions
    val movie_hash_rdd          = spark_context.parallelize(movie_signature_matrix.zipWithIndex.map( x => (x._2, x._1))).partitionBy(new HashPartitioner(num_partitions))

    // Converting tuples (key, [value1, value2....]) to list of values [value1, value2....] and performing LSH algorithm using given bands
    val recommender_output      = movie_hash_rdd.values.mapPartitions(x => algorithm_LSH(x)).distinct().persist()

    // Calculating actual Jaccard Similarity between the candidate pairs
    val actual_output           = recommender_output.mapPartitions(x => calculate_jaccard(x, transposed_character_matrix, movie_list)).distinct().sortByKey(true).collect()

    // Generating expected output of the form "Movie1, Movie2, Jaccard_Similarity"
    val temp_result             = actual_output.mkString("\n")
    val final_result            = temp_result.replace("(", "").replace(")", "").replace(",", ", ")

    // Writing results into output file
    output_file.write(final_result)
    output_file.close()

    // Printing time taken by program
    println("Time: "+ ((System.nanoTime() - start_time) / 1e9d).toInt + " sec")
  }
}
