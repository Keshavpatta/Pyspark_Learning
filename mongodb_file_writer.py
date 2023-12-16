from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def parseInput(line):
    fields=line.split('|')
    return Row(user_id=int(fields[0]),age=int(fields[1]),gender=fields[2], occupation=fields[3],zip=fields[4])

if __name__ == "__main__":

    # Create sparksession
    spark=SparkSession.\
        builder.appName("mongodb_test").\
            getOrCreate()
    
    # Build RDD on top of user data file
    lines=spark.sparkContext.textFile("hdfs:///user/maria_dev/mongodb/movies.user")

    # creating new RDD by passing the parser function
    users=lines.map(parseInput)

    # convert RDD into a Dataframe
    usersDataset=spark.createDataFrame(users)

    # Write The data into MongoDB
    usersDataset.write.\
        format("com.mongodb.spark.sql.DefaultSource").\
            option("uri","mongodb://127.0.0.1/moviesdata.users").\
                mode('append').\
                    save()
