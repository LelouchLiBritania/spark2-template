package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )


    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val population = spark.read
      /*.option("multiline", true)
      .option("mode", "PERMISSIVE")*/
      .json("data/input/demographie_par_commune.json")
    population.show

    println("How many inhabitants has France?")
    population.select("Population")
      .agg(sum("Population")
      .as("Population"))
      .show()

    println("What are the top highly populated departments in France?")
    population.select("Population","Departement")
      .groupBy("Departement")
      .agg(sum("Population")
      .as("Population"))
      .orderBy(desc("Population"))
      .show()

    println("What are the top highly populated departments' name in France?")
    val popOrdered = population.select("Population","Departement")
      .groupBy("Departement")
      .agg(sum("Population")
      .as("Population"))
      .orderBy(desc("Population"))

    val departements = spark.read
      /*.option("multiline", true)
      .option("mode", "PERMISSIVE")*/
      .csv("data/input/departements.txt")
      .select($"_c0".as("name"),$"_c1".as("Departement"))

    popOrdered.join(departements,Seq("Departement")).show
  }

  def exec3(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val sample_07 = spark.read
      .option("delimiter", "\t").csv("data/input/sample_07")
      .select($"_c0".as("id"),$"_c1".as("name"),$"_c2".as("number_of_workers"),$"_c3".as("mean_salary"))
    sample_07.show

    val sample_08 = spark.read
      .option("delimiter", "\t").csv("data/input/sample_08")
      .select($"_c0".as("id"),$"_c1".as("name"),$"_c2".as("number_of_workers"),$"_c3".as("mean_salary"))
    sample_08.show

    println("Find top salaries in 2007 which are above 100k.")

    sample_07.where("mean_salary>100000").orderBy($"mean_salary".desc).show()

    println("Find salary growth sorted from 2007-08.")

    sample_07
      .join(sample_08,Seq("id"))
      .withColumn("growth",sample_08("mean_salary")-sample_07("mean_salary"))
      .orderBy($"growth".desc)
      .show()

    println("Find job loss among the top earnings from 2007-08.")

    sample_07.where("mean_salary>100000")
      .join(sample_08,Seq("id"))
      .withColumn("job_loss",sample_07("number_of_workers")-sample_08("number_of_workers"))
      .orderBy($"job_loss".desc)
      .where("job_loss > 0")
      .where(sample_08("mean_salary")>100000)
      .show()
  }

  def exec4(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println("How many unique levels of difficulties?")

    toursDF
      .select($"tourDifficulty")
      .distinct()
      .show()

    println("What is the min/max/average of tour prices?")

    toursDF
      .agg(min("tourPrice").as("Min"),max("tourPrice").as("Max"),avg("tourPrice").as("Average"))
      .show()

    println("What is the min/max/average of price for each level of difficulty?")

    toursDF
      .groupBy($"tourDifficulty")
      .agg(min("tourPrice").as("Min"),max("tourPrice").as("Max"),avg("tourPrice").as("Average"))
      .show()

    println("What is the min/max/average of price and duration for each level of difficulty?")

    toursDF
      .groupBy($"tourDifficulty")
      .agg(
        min("tourPrice").as("Min_price"),
        max("tourPrice").as("Max_price"),
        avg("tourPrice").as("Average_price"),
        min("tourLength").as("Min_length"),
        max("tourLength").as("Max_length"),
        avg("tourLength").as("Average_length"))
      .show()

    println("Display the top 10 \"tourTags\"")

    toursDF
      .select(explode($"tourTags"))
      .groupBy($"col")
      .count()
      .orderBy($"count".desc)
      .show(10)

    println("Relationship between top 10 \"tourTags\" and \"tourDifficulty\"")

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    println("What is the min/max/average of price in \"tourTags\" and \"tourDifficulty\" relationship, sorted by average?")

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty",$"tourPrice")
      .groupBy($"col", $"tourDifficulty")
      .agg(
        min("tourPrice").as("Min"),
        max("tourPrice").as("Max"),
        avg("tourPrice").as("Average"))
      .orderBy($"Average".desc)
      .show()
  }
}
