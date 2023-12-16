from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

if __name__ == "__main__":
    # Create sparksession
    spark=SparkSession.\
        builder.appName("mongodb_test_2").\
            getOrCreate()

     # Write The data into MongoDB
    readUsers=spark.read.\
        format("com.mongodb.spark.sql.DefaultSource").\
            option("uri","mongodb://127.0.0.1/moviesdata.users").\
                load()
    
    readUsers.createGlobalTempView("readUsers")

    readUsers.printSchema()

    sqlDF=spark.sql("""
    SELECT occupation,count(user_id) as cnt_usr
    FROM users
    GROUP BY occupation
    ORDER BY cnt_usr DESC""")

    sqlDF.show()
