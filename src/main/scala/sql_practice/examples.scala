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
}
