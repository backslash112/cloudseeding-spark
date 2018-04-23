object Cells {
  import sys.process._
  import java.net.URL
  import java.io.File
  import scala.language.postfixOps
  
  import au.com.bytecode.opencsv.CSVParser
  import sqlContext.implicits._
  import org.apache.spark.sql.functions.round 

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  

  // Download the csv file from web
  def fileDownloader(url: String, filename: String) = {
      new URL(url) #> new File(filename) !!
  }
  fileDownloader("https://vincentarelbundock.github.io/Rdatasets/csv/Stat2Data/CloudSeeding2.csv", "CloudSeeding2.csv")


  // Define the class for reading the data from the csv file
  case class CloudSeeding(Period: String, Seeded: String, Season: String, TE: String, TW: String, NC: String,
   SC: String, NWC: String);

  // Read data
  val csv = sc.textFile("CloudSeeding2.csv")
  val headerAndRows = csv.map(line => line.split(",").map(_.trim))
  var header = headerAndRows.first
  val data = headerAndRows.filter(_(0) != header(0))
  val cloudSeeding  = data.map(p => CloudSeeding(p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8))).toDF()
  
  // Only select two columns
  val population = cloudSeeding.select("Season", "TE")//.orderBy("Season")
  population.show()


  // Resamping the data without replacement
  population.sample(withReplacement=false, fraction=0.25).show()


  val resample: (DataFrame) => DataFrame = (df:DataFrame) => 
    df.sample(withReplacement=true, fraction=1.0)
  
  val compute: (DataFrame) => DataFrame = (df:DataFrame) =>
    df.groupBy("Season")
      .agg(avg("TE").alias("avg"), 
           variance("TE").alias("variance"))
      .withColumn("avg", round($"avg", 2))
      .withColumn("variance", round($"variance", 2))
  

  // Run the resample method for 1000 times
  var result = resample(population)
  result = compute(result)
  result.show()
  for (a <- 1 to 999) {
    var temp = resample(population)
    temp = compute(temp)
    result = result.union(temp)
  }
  result.show()

  // Caculate the avg value
  result.groupBy("Season")
  .agg(avg("avg").alias("avg"), 
       avg("variance").alias("variance"))
  .withColumn("avg", round($"avg", 2))
  .withColumn("variance", round($"variance", 2))
  .show()
}
                  
