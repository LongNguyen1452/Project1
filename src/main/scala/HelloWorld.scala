//import org.apache.spark
//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.StdIn
import java.sql.DriverManager
import java.sql.Connection


object HelloWorld {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop")
    /*println("Hello  World")
    val conf = new SparkConf().setMaster("local").setAppName("ScalaSparkTest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")*/
    //val a = sc.parallelize(1 to 10)
    //print(a.reduce(_+_))
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark sql session")

    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/projectdb"
    val username = "lhnguyen38"
    val password = "p@ssword123"

    val connection:Connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()
    println("Enter username: ")
    val inputuser = StdIn.readLine()
    println("Enter password: ")
    val inputpw = StdIn.readLine()
    val checkuser = statement.executeQuery("select user_type from users where username="+"'"+inputuser+"'"+" AND pswrd="+"'"+inputpw+"'"+";")
    while(checkuser.next()){
      var usertype = checkuser.getString(1)
      if (usertype == "admin"){
        println("admin login successful")
        println("displaying tables visible to standard users and admin")
      }
      else if (usertype == "user"){
        println("user login successful")
        println("displaying tables visible to standard users")
      }
      else {
        println("invalid login... closing connection")
      }
      if (usertype == "admin" || usertype == "user"){
        spark.sql("drop table if exists mortal")
        spark.sql("create table IF NOT EXISTS mortal(Country String,Mortality_rate Float,Gender String,Year Int) row format delimited fields terminated by ','");
        spark.sql("LOAD DATA LOCAL INPATH 'input/InfantMortalityRate.csv' INTO TABLE mortal")
        //spark.sql("SELECT Country,round(mortality_rate,2) as Mortality_rate,Gender,Year FROM mortal").show(20)

        //1. Which countries have the highest infant mortality rate in the most recent year? (top 10)
        println("1. Which countries have the highest infant mortality rate in the most recent year? (top 10)")
        spark.sql("select Country,round(mortality_rate,2) as Mortality_rate,Year from mortal where year=2019 and Gender='Total' order by mortality_rate desc limit 10").show(false)


        //2. Which year had the highest infant mortality rate and for which country?
        println("2. Which year had the highest infant mortality rate and for which country?")
        spark.sql("select Country,round(Mortality_rate,2) as mortality_rate,Year from mortal where gender = 'Total' and mortality_rate<100 order by mortality_rate desc limit 1").show(false)


        //3. Which gender has a higher overall mortality rate?
        println("3. Which gender has a higher overall mortality rate?")
        spark.sql("select Gender,avg(mortality_rate) as average_mortality_rate from mortal where Gender != 'Total' group by Gender order by average_mortality_rate desc").show()


        //4. Which country had the top mortality rate each year?
        println("4. Which country had the top mortality rate each year?")
        spark.sql("select Country,round(Mortality_rate,2) as mortality_rate,Year from (select Country,Mortality_rate,Year,gender,rank() over (partition by year order by mortality_rate desc) as rank from mortal where gender='Total' and mortality_rate<100) temp where rank=1 order by year desc").show(false)


        //5. What's the global average mortality rate each year?
        println("5. What's the global average mortality rate each year?")
        spark.sql("select round(mortality_avg,2) as Mortality_rate,Year from (select avg(mortality_rate) as mortality_avg,year from mortal where gender='Total' group by year) order by year desc").show()

      }
      if (usertype == "admin"){
        //6. What's the predicted global mortality rate for 2022?
        println("6. What's the predicted global mortality rate for 2022? (admin view)")
        spark.sql("select '2022' as year, round(mortality_avg)+(-0.59*(2022-2019)) as mortality_rate from (select avg(mortality_rate) as mortality_avg from mortal where year=2019) limit 1").show()

        println("exporting outputs to json file...")
        spark.sql("select Country,round(mortality_rate,2) as Mortality_rate,Year from mortal where year=2019 and Gender='Total' order by mortality_rate desc limit 10").write.format("org.apache.spark.sql.json").mode("overwrite").save("output/problem1")
        spark.sql("select Country,round(Mortality_rate,2) as mortality_rate,Year from mortal where gender = 'Total' and mortality_rate<100 order by mortality_rate desc limit 1").write.format("org.apache.spark.sql.json").mode("overwrite").save("output/problem2")
        spark.sql("select Gender,avg(mortality_rate) as average_mortality_rate from mortal where Gender != 'Total' group by Gender order by average_mortality_rate desc").write.format("org.apache.spark.sql.json").mode("overwrite").save("output/problem3")
        spark.sql("select Country,round(Mortality_rate,2) as mortality_rate,Year from (select Country,Mortality_rate,Year,gender,rank() over (partition by year order by mortality_rate desc) as rank from mortal where gender='Total' and mortality_rate<100) temp where rank=1 order by year desc").write.format("org.apache.spark.sql.json").mode("overwrite").save("output/problem4")
        spark.sql("select round(mortality_avg,2) as Mortality_rate,Year from (select avg(mortality_rate) as mortality_avg,year from mortal where gender='Total' group by year) order by year desc").write.format("org.apache.spark.sql.json").mode("overwrite").save("output/problem5")
        spark.sql("select '2022' as year, round(mortality_avg)+(-0.59*(2022-2019)) as mortality_rate from (select avg(mortality_rate) as mortality_avg from mortal where year=2019) limit 1").write.format("org.apache.spark.sql.json").mode("overwrite").save("output/problem6")
        println("exporting job completed")
      }
    }
  }
}