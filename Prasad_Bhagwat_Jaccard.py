# Imports required for the program
from pyspark import SparkConf, SparkContext
import time
from operator import add
import sys


# Generating Characteristic matrix of size (users x List(movies))
# eg. List(movies) = [0, 1, 1, 0, 1, 0, 0]
def gen_characteristic_matrix(values, movie_length, movie_list):
    # Initializing movie list with 0's
    columns = [0] * movie_length
    # Looping over each movie
    for movie in values:
        # Storing only indexes of movies which are present instead of all 0 to n movies
        index           = movie_list.index(movie)
        columns[index]  = 1

    return columns


# Generating Signature matrix of size (hash_functions x movies)
def gen_hash_signature_matrix(user_movie_list, user_length, movie_length):
    global num_buckets, hash_func_a, hash_func_b, num_hash_funcs
    # Hash signatures matrix of size (users x hash_functions)
    hash_signature_matrix = [[0 for i in range(num_hash_funcs)] for j in range(user_length)]
    # Hash signatures matrix of size (hash_functions x movies)
    movie_signature_matrix = [[9999999 for i in range(movie_length)] for j in range(num_hash_funcs)]
    # Looping over all the users
    for user in range(user_length):
        # Generating hash signatures for all users
        for hash in range(num_hash_funcs):
            # Using hash function (ax + b) % m
            hash_signature_matrix[user][hash] = (hash_func_a * user + hash_func_b * hash) % num_buckets

    # Looping over all the users
    for user in range(user_length):
        # Looping over all the movies
        for movie in range(movie_length):
            # Checking occurance of "1" in the list of movies
             if user_movie_list[user][1][movie]:
                for hash_func in range(num_hash_funcs):
                    if hash_signature_matrix[user][hash_func] < movie_signature_matrix[hash_func][movie]:
                        movie_signature_matrix[hash_func][movie] = hash_signature_matrix[user][hash_func]

    return movie_signature_matrix


# LSH algorithm implementation for each of the bands
def algorithm_LSH(movie_hash_partition_rdd):
    movie_hash_partition_list   = list(movie_hash_partition_rdd)
    # Transpose the list of lists to get matrix of size (hash_functions x movies)
    hashed_movie = map(list, zip(*movie_hash_partition_list))
    # Check if movies have similar signatures within the band, if Yes emit the tuple of candidate pair
    for index_1 in range(len(hashed_movie)):
       for index_2 in range((index_1 + 1), len(hashed_movie)):
           if hashed_movie[index_1] == hashed_movie[index_2]:
               yield (index_1, index_2)


# Calculating Jaccard Similarity between candidate pairs
def calculate_jaccard(candidate_pair, transposed_characteristic_matrix, movie_list):
    global threshold
    candidate_list = list(candidate_pair)
    # Looping over all the candidates of Candidate pair list
    for candidate in candidate_list:
        # Creating summed up list for candidate movie1 and movie2 i.e. [1, 2, 3] + [4, 5, 6] = [5, 7, 9]
        summed_list = map(add, transposed_characteristic_matrix[candidate[0]], transposed_characteristic_matrix[candidate[1]])

        # Calculating Jaccard Similarity between movies from the candidate pair
        jaccard_similarity = float(summed_list.count(2)) / float(summed_list.count(1) + summed_list.count(2))
        # Sorting the candidate pairs which are above given threshold
        if jaccard_similarity >= threshold:
            if movie_list[candidate[0]] < movie_list[candidate[1]]:
                yield ((movie_list[candidate[0]], movie_list[candidate[1]]), jaccard_similarity)
            else:
                yield ((movie_list[candidate[1]], movie_list[candidate[0]]), jaccard_similarity)


# Main function
def main():
    # Recording time taken by program
    start_time = time.time()

    # Global variable declarations
    global num_buckets, hash_func_a, hash_func_b, threshold, num_hash_funcs

    # Command Line Arguments
    input_file          = sys.argv[1]

    # Output filename generation
    output_file_name    = "Prasad_Bhagwat_SimilarMovies_Jaccard.txt"
    output_file         = open(output_file_name, "w")

    # Creating Spark Context
    spark_config    = SparkConf()
    spark_context   = SparkContext(conf = spark_config)
    spark_context.setLogLevel("WARN")

    # Configuration parameters
    num_partitions  = 12
    num_hash_funcs  = 12
    num_buckets     = 673
    hash_func_a     = 7
    hash_func_b     = 3
    threshold       = 0.5

    # Reading file and extracting header
    input           = spark_context.textFile(input_file).filter(lambda x: "userId" not in x)#.coalesce(8)
    data            = input.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1]))).groupByKey().persist()
    user_list       = data.keys().distinct().collect()
    user_length     = len(user_list)
    movie_list      = data.flatMap(lambda x: x[1]).distinct().collect()
    movie_length    = len(movie_list)

    # Original characteristic matrix
    character_matrix            = data.mapValues(lambda x: gen_characteristic_matrix(x, movie_length, movie_list)).collect()
    character_matrix_list       = [element[1] for element in character_matrix]

    # Transpose characteristic matrix to give matrix of size (List(movies) x users)
    transposed_character_matrix = map(list, zip(*character_matrix_list))

    # Generate Movie-Hash Signature matrix
    movie_signature_matrix      = gen_hash_signature_matrix(character_matrix, user_length, movie_length)

    # Transforming Signature matrix into RDD and using mapPartitions on it using 4 partitions
    movie_hash_rdd      = spark_context.parallelize(enumerate(movie_signature_matrix)).partitionBy(num_partitions)

    # Converting tuples (key, [value1, value2....]) to list of values [value1, value2....] and performing LSH algorithm using given bands
    recommender_output  = movie_hash_rdd.values().mapPartitions(lambda x: algorithm_LSH(x)).distinct().persist()

    # Calculating Jaccard Similarity between the candidate pairs
    jaccard_similarity  = recommender_output.mapPartitions(lambda x: calculate_jaccard(x, transposed_character_matrix, movie_list)).distinct().sortByKey(True).collect()

    # Generating expected output of the form "Movie1, Movie2, Jaccard_Similarity"
    intermediate_result = str('\n'.join('%s, %s' % elements for elements in jaccard_similarity))
    output_result       = intermediate_result.replace("(", "").replace(")","")

    # Writing results into output file
    output_file.write(output_result)
    output_file.close()

    # Printing time taken by program
    print "Time:", int(time.time() - start_time), "sec"

# Entry point of the program
if __name__ == '__main__':
    main()
