#Import Required Libraries
import pyspark

def spark_connection():
    #Create the Spark Session
    spark = pyspark.sql.SparkSession.builder.appName("Extract") \
        .config("spark.jars", "/workspaces/Project1/jars/postgresql-42.7.4.jar") \
        .getOrCreate()
    #Print to Validate if Spark Session is established
    print("Details of the jar file", spark.sparkContext._jsc.sc().listJars())
    return spark




def extract_movies_data(spark):
    #read table from Database using the spark session
    movie_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/Project01DB") \
        .option("dbtable", "movies") \
        .option("user", "postgres") \
        .option("password", "Hello@12345") \
        .option("driver", "org.postgresql.Driver").load()

    #Display the details of the Movies dataframe
    print("Data Frame is here", movie_df.show())
    return movie_df

def extract_users_data(spark):
    #read table from Database using the spark session
    user_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/Project01DB") \
        .option("dbtable", "users") \
        .option("user", "postgres") \
        .option("password", "Hello@12345") \
        .option("driver", "org.postgresql.Driver").load()

    #Display the details of the Users dataframe
    print("Data Frame is here", user_df.show())
    return user_df

def transform_data(movie_df, user_df):
    #Perform Transformation on the dataframes
    transformed_user_df = user_df.groupby("movie_id").mean("rating")

    #Display the details of the transformed Users dataframe
    print("Transformed User Data Frame is here", transformed_user_df.show())   

    movie_ratings_df = movie_df.join(transformed_user_df, movie_df.id == transformed_user_df.movie_id, "inner") 
    print("Movie Ratings Data Frame is here", movie_ratings_df.show())

def main():
    #Establish Spark Connection
    spark = spark_connection()

    #Extract Data from the Database
    movie_df = extract_movies_data(spark)
    user_df = extract_users_data(spark)

    #Transform the Extracted Data
    transform_data(movie_df, user_df)

if __name__ == "__main__":
    main()

