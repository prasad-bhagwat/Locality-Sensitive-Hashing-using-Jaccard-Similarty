Locality Sensitive Hashing using Jaccard Similarity
=====================================================

### Enviroment versions required:

Spark: 2.2.1  
Python: 2.7  
Scala: 2.11

### Python command for executing LSH Algorithm using Jaccard Similarity

* * *

Exceuting Jaccard based LSH using _“Prasad\_Bhagwat\_Jaccard.py”_ file

    spark-submit --driver-memory 4G Prasad_Bhagwat_Jaccard.py <ratings file path>
    

where,  
_<ratings file path>_ corresponds to the absolute path of input _‘ratings.csv’_ file

Example usage of the above command is as follows:

     ~/Desktop/spark-2.2.1/bin/spark-submit --driver-memory 4G Prasad_Bhagwat_Jaccard.py ratings.csv
    

Note : _Output file_ named _‘Prasad\_Bhagwat\_SimilarMovies_Jaccard.txt’_ is generated at the location from where the _spark-submit_ is run.

**Task 1 Results using Python :**

### Scala command for executing LSH Algorithm using Jaccard Similarity

* * *

Exceuting Jaccard based LSH using _“Prasad\_Bhagwat\_JaccardLSH.jar”_ file

    spark-submit --driver-memory 4G --class JaccardLSH Prasad_Bhagwat_JaccardLSH.jar <ratings file path>
    

where,  
_<ratings file path>_ corresponds to the absolute path of input _‘ratings.csv’_ file

Example usage of the above command is as follows:

     ~/Desktop/spark-2.2.1/bin/spark-submit --driver-memory 4G --class JaccardLSH Prasad_Bhagwat_JaccardLSH.jar ratings.csv
    

Note : _Output file_ named _‘Prasad\_Bhagwat\_SimilarMovies_Jaccard.txt’_ is generated at the location from where the _spark-submit_ is run.
