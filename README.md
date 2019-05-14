# Introduction to Spark SQL

Ο παρακάτω οδηγός περιέχει κώδικα σε [scala](https://www.scala-lang.org/) και αφορά την έκδοση 1.6.1 του [Spark](https://spark.apache.org/). 

### Table of Contents
**[SQLContext](#sqlcontext)**<br>
**[DataFrame Creation](#dataframe-creation)**<br>
**[DataFrame Operations](#dataframe-operations)**<br>
**[SQL Queries](#sql-queries)**<br>
**[User Defined Functions](#user-defined-functions)**<br>
**[Lazy Evaluation](#lazy-evaluation)**<br>
**[Query Engine](#query-engine)**<br>
**[Caching](#caching)**<br>
**[Partitioning in DataFrames](#partitioning-in-dataframes)**<br>
**[DataFrames and RDDs](#dataframes-and-rdds)**<br>
**[Working with JSON data](#working-with-json-data)**<br>
**[Resources](#resources)**<br>

## SQLContext
Η δημιουργία ενός SQLContext αντικειμένου είναι απαραίτητη για την επεξεργασία δεδομένων με το Spark SQL.

Στο Spark shell το αντικείμενο αυτό δημιουργείται αυτόματα. Ωστόσο σε project με δικό μας κώδικα, πρέπει να δημιουργήσουμε μόνοι μας το SQLContext ως εξής:

```scala
val conf = new SparkConf().setAppName("analysis").setMaster("local[*]")
val sc: SparkContext = new SparkContext(conf)
val sqlContext = new SQLContext(sc)
import sqlContext.implicits._
```


## DataFrame Creation
Η βασική δομή επεξεργασίας δεδομένων στο Sparql SQL API είναι ένα DataFrame.

Μπορούμε να δημιουργήσουμε ένα DataFrame είτε διαβάζοντας απευθείας από κάποιο αρχείο, είτε να μετατρέψουμε ένα RDD σε DataFrame:

Αρχικά δημιουργούμε ένα RDD που περιέχει νούμερα από το 1 μέχρι το 50:
```scala
val rdd = sc.parallelize(1 to 50)
//rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0]
```

Στη συνέχεια, μετατρέπουμε το RDD σε DataFrame χρησιμοποιώντας τη μέθοδο `toDF` και περνώντας ως παράμετρο το όνομα της κολώνας από την οποία θα αποτελείται:
```scala
val df = rdd.toDF("value")
//df: org.apache.spark.sql.DataFrame = [value: int]
```

Με τη μέθοδο `show` μπορούμε να εμφανίσουμε τα περιεχόμενα του DataFrame:
```scala
df.show()       //By default εμφανίζει τα πρώτα 20 rows. Σημείωση: Στη scala μπορούμε να καλούμε μεθόδους και χωρίς το "()" πχ. df.show
//+-----+
//|value|
//+-----+
//|    1|
//|    2|
//|    3|
//|    4|
//|    5|
//|    6|
//|    7|
//|    8|
//|    9|
//|   10|
//|   11|
//|   12|
//|   13|
//|   14|
//|   15|
//|   16|
//|   17|
//|   18|
//|   19|
//|   20|
//+-----+
//only showing top 20 rows
```

```scala
df.show(50)     //Εδώ εμφανίζει τα 50 πρώτα rows
//+-----+
//|value|
//+-----+
//|    1|
//|    2|
//|    3|
//|    4|
//|    5|
//|    6|
//|    7|
//|    8|
//|    9|
//|   10|
//|   11|
//|   12|
//|   13|
//|   14|
//|   15|
//|   16|
//|   17|
//|   18|
//|   19|
//|   20|
//|   21|
//|   22|
//|   23|
//|   24|
//|   25|
//|   26|
//|   27|
//|   28|
//|   29|
//|   30|
//|   31|
//|   32|
//|   33|
//|   34|
//|   35|
//|   36|
//|   37|
//|   38|
//|   39|
//|   40|
//|   41|
//|   42|
//|   43|
//|   44|
//|   45|
//|   46|
//|   47|
//|   48|
//|   49|
//|   50|
//+-----+
```

```scala
df.printSchema  //Τυπώνει το σχήμα των δεδομένων του DataFrame
//root
// |-- value: integer (nullable = true)
```

Ένας άλλος τρόπος για να δημιουργήσουμε DataFrames, είναι μέσω των Datasets. Ένα Dataset μοιάζει με ένα RDD αφού μπορεί να εκτελέσει περίπου τα ίδια operations (`map`, `reduce`, κλπ). Επίσης στα Datasets (όπως και στα RDD) πρέπει να δηλώνουμε τον τύπο δεδομένων από τον οποίο αποτελούνται.

Αρχικά δημιουργούμε ένα DataSet το οποίο έχει πάντα έχει ένα συγκεκριμένο τύπο δεδομένων. Εδώ έχει τον τύπο `Int`:
```scala
val ds: Dataset[Int] = (1 to 50).toDS()		//By default, το Dataset θα δημιουργήσει μια κολώνα που θα της δώσει το όνομα "value"
//ds: org.apache.spark.sql.Dataset[Int] = [value: int]
ds.show
//+-----+
//|value|
//+-----+
//|    1|
//|    2|
//|    3|
//|    4|
//|    5|
//|    6|
//|    7|
//|    8|
//|    9|
//|   10|
//|   11|
//|   12|
//|   13|
//|   14|
//|   15|
//|   16|
//|   17|
//|   18|
//|   19|
//|   20|
//+-----+
//only showing top 20 rows
```

Ένα Dataset μπορούμε να το μετατρέψουμε σε DataFrame ξανά με τη μέθοδο `toDF`:
```scala
val df = ds.toDF()
//df: org.apache.spark.sql.DataFrame = [value: int]
df.show
//+-----+
//|value|
//+-----+
//|    1|
//|    2|
//|    3|
//|    4|
//|    5|
//|    6|
//|    7|
//|    8|
//|    9|
//|   10|
//|   11|
//|   12|
//|   13|
//|   14|
//|   15|
//|   16|
//|   17|
//|   18|
//|   19|
//|   20|
//+-----+
//only showing top 20 rows
```

Τα DataFrames είναι μια semi-structured δομή όπου τα δεδομένα οργανώνονται σε named columns. Ένα DataFrame μπορεί να έχει οποιοδήποτε πλήθος/τύπο στηλών.

Τα DataFrames είναι column-based data structures σε αντίθεση με τα RDD που είναι row-based. Έτσι, σχεδόν operations στα DataFrames γίνονται πάνω σε συγκεκριμένες στήλες και όχι σε ολόκληρο το row.

Επίσης μπορούμε να δημιουργήσουμε DataFrames διαβάζοντας κάποιο input αρχείο:
```scala
val dfJson = sqlContext.read.json("t.txt")  //Το "t.txt" είναι το path του json αρχείου που θέλουμε να διαβάσουμε από το τοπικό file system ή από το HDFS
```

Υποστηρίζονται τα ακόλουθα datasources:
```
json file, parquet file, text file, jdbc
```
## DataFrame Operations
Επειδή τα DataFrames είναι immutable, οποιαδήποτε αλλαγή κάνουμε σε αυτά, δημιουργεί ένα νέο DataFrame.

Το νέο DataFrame μπορούμε είτε να το εμφανίσουμε στην οθόνη απευθείας, είναι να το κρατήσουμε ως ξεχωριστή μεταβλητή στη μνήμη για περαιτέρω επεξεργασία.

Αρχικά χρειάζεται να κάνουμε import:
```scala
import org.apache.spark.sql.functions._
```

Φιλτράρισμα δεδομένων:
```scala
df.filter(col("value") > 20).show	//Η συνάρτηση col μας επιτρέπει να επιλέγουμε κολώνα με βάση το όνομά της.
//+-----+
//|value|
//+-----+
//|   21|
//|   22|
//|   23|
//|   24|
//|   25|
//|   26|
//|   27|
//|   28|
//|   29|
//|   30|
//|   31|
//|   32|
//|   33|
//|   34|
//|   35|
//|   36|
//|   37|
//|   38|
//|   39|
//|   40|
//+-----+
//only showing top 20 rows
```
Προσθήκη νέας κολώνας με όνομα `halfValue` η οποία θα πάρει σε κάθε γραμμή τη μισή τιμή της κολώνας `value`:
```scala
val df1 = df.withColumn("halfValue", col("value") / 2)		//Υποστηρίζονται όλα τα αιρθμητικά operations (+ - * / %)
//df1: org.apache.spark.sql.DataFrame = [value: int, halfValue: double]
df1.show
//+-----+---------+
//|value|halfValue|
//+-----+---------+
//|    1|      0.5|
//|    2|      1.0|
//|    3|      1.5|
//|    4|      2.0|
//|    5|      2.5|
//|    6|      3.0|
//|    7|      3.5|
//|    8|      4.0|
//|    9|      4.5|
//|   10|      5.0|
//|   11|      5.5|
//|   12|      6.0|
//|   13|      6.5|
//|   14|      7.0|
//|   15|      7.5|
//|   16|      8.0|
//|   17|      8.5|
//|   18|      9.0|
//|   19|      9.5|
//|   20|     10.0|
//+-----+---------+
//only showing top 20 rows
```
Επιλογή μερικών από τις κολώνες:
```scala
df1.select(col("value")).show
//+-----+
//|value|
//+-----+
//|    1|
//|    2|
//|    3|
//|    4|
//|    5|
//|    6|
//|    7|
//|    8|
//|    9|
//|   10|
//|   11|
//|   12|
//|   13|
//|   14|
//|   15|
//|   16|
//|   17|
//|   18|
//|   19|
//|   20|
//+-----+
//only showing top 20 rows
```
Μετονομασία μιας κολώνας:
```scala
df1.select(col("value").as("v")).show
```
Εμφάνιση της μέγιστης τιμής για μια κολώνα:
```scala
df1.select(max(col("value"))).show			//Υποστηρίζονται max, min, avg, stddev, sum, variance, first, last, και άλλα
//+----------+                                                                    
//|max(value)|
//+----------+
//|        50|
//+----------+
```
Ταξινόμηση δεδομένων σε αύξουσα ή φθίνουσα σειρά:
```scala
df1.orderBy(col("value").desc).show			//desc για φθίνουσα, asc για αύξουσα σειρά
```
Ομαδοποίηση των δεδομένων με βάση μια κολώνα και εξαγωγή στατιστικών ανά ομάδα:
```scala
val df2 = df1.withColumn("groups", col("value") % 10)
//df2: org.apache.spark.sql.DataFrame = [value: int, halfValue: double, groups: int]
df2.groupBy("groups").agg(max(col("halfValue"))).show
//+------+--------------+                                                         
//|groups|max(halfValue)|
//+------+--------------+
//|     0|          25.0|
//|     1|          20.5|
//|     2|          21.0|
//|     3|          21.5|
//|     4|          22.0|
//|     5|          22.5|
//|     6|          23.0|
//|     7|          23.5|
//|     8|          24.0|
//|     9|          24.5|
//+------+--------------+
```
Διαγραφή κολώνας από DataFrame:
```scala
df2.drop(col("halfValue")).show
//+-----+------+
//|value|groups|
//+-----+------+
//|    1|     1|
//|    2|     2|
//|    3|     3|
//|    4|     4|
//|    5|     5|
//|    6|     6|
//|    7|     7|
//|    8|     8|
//|    9|     9|
//|   10|     0|
//|   11|     1|
//|   12|     2|
//|   13|     3|
//|   14|     4|
//|   15|     5|
//|   16|     6|
//|   17|     7|
//|   18|     8|
//|   19|     9|
//|   20|     0|
//+-----+------+
//only showing top 20 rows
```
Σημείωση: Τα DataFrames είναι immutable. Δε διαγράφεται η τιμή από το `df2`. Δημιουργείται ένα νέο DataFrame χωρίς την κολώνα `halfValue`.

Υπάρχουν πολλά ακόμα operations που μπορούν να γίνουν πάνω σε DataFrames με τη μορφή συναρτήσεων.

Μια πλήρη λίστα για την έκδοση 1.6.1 μπορεί να βρεθεί εδώ: https://spark.apache.org/docs/1.6.1/api/java/index.html?org/apache/spark/sql/functions.html

Για τις νεότερες εκδόσεις του Spark (2.4.3), η λίστα είναι διαθέσιμη εδώ: https://spark.apache.org/docs/2.4.3/api/sql/index.html

## SQL Queries
Δίνεται η δυνατότητα να εκτελέσουμε SQL queries πάνω σε δεδομένα των DataFrames.

Το αποτέλεσμα ενός SQL Query είναι ένα νέο DataFrame.

Αρχικά κάνουμε register ένα υπάρχον DataFrame:
```scala
df2.registerTempTable("numbers")
```
Στη συνέχεια εκτελούμε SQL query στο registered DataFrame χρησιμοποιώντας το registered name:
```scala
sqlContext.sql("SELECT * FROM numbers WHERE halfValue < 10").show
//+-----+---------+------+
//|value|halfValue|groups|
//+-----+---------+------+
//|    1|      0.5|     1|
//|    2|      1.0|     2|
//|    3|      1.5|     3|
//|    4|      2.0|     4|
//|    5|      2.5|     5|
//|    6|      3.0|     6|
//|    7|      3.5|     7|
//|    8|      4.0|     8|
//|    9|      4.5|     9|
//|   10|      5.0|     0|
//|   11|      5.5|     1|
//|   12|      6.0|     2|
//|   13|      6.5|     3|
//|   14|      7.0|     4|
//|   15|      7.5|     5|
//|   16|      8.0|     6|
//|   17|      8.5|     7|
//|   18|      9.0|     8|
//|   19|      9.5|     9|
//+-----+---------+------+
```

## User Defined Functions
Υπάρχουν περιπτώσεις όπου οι διαθέσιμες συναρτήσεις δεν παρέχουν τη λειτουργικότητα που θέλουμε.

Το Spark παρέχει τη δυνατότητα να γράφουμε δικές μας συναρτήσεις που θα εκτελεστούν πάνω σε κολώνες των δεδομένων.

Η παρακάτω `udf` συνάρτηση επιστρέφει true όταν το τρίτο bit είναι 1 και false όταν είναι 0:
```scala
val bitFilter = udf((value: Int) => ((value >> 2) & 1) == 1)
//bitFilter: org.apache.spark.sql.UserDefinedFunction = UserDefinedFunction(<function1>,BooleanType,List(IntegerType))
```
Οι udf συναρτήσεις μπορούν να χρησιμοποιηθούν σαν μια οποιαδήποτε συνάρτηση:
```scala
val df3 = df2.filter(bitFilter(col("value")))
//df3: org.apache.spark.sql.DataFrame = [value: int, halfValue: double, groups: int]
df3.show
//+-----+---------+------+
//|value|halfValue|groups|
//+-----+---------+------+
//|    4|      2.0|     4|
//|    5|      2.5|     5|
//|    6|      3.0|     6|
//|    7|      3.5|     7|
//|   12|      6.0|     2|
//|   13|      6.5|     3|
//|   14|      7.0|     4|
//|   15|      7.5|     5|
//|   20|     10.0|     0|
//|   21|     10.5|     1|
//|   22|     11.0|     2|
//|   23|     11.5|     3|
//|   28|     14.0|     8|
//|   29|     14.5|     9|
//|   30|     15.0|     0|
//|   31|     15.5|     1|
//|   36|     18.0|     6|
//|   37|     18.5|     7|
//|   38|     19.0|     8|
//|   39|     19.5|     9|
//+-----+---------+------+
//only showing top 20 rows
```
Οι `udf` συναρτήσεις μπορούν να παίρνουν ως όρισμα μία ή περισσότερες κολώνες και μία ή περισσότερες constant τιμές.

Μπορούν να χρησιμοποιηθούν οπουδήποτε ως αντικατάσταση μιας κολώνας πχ: στα aggregation operations, στα filter operations, στα withColumn operations και άλλα.

## Lazy Evaluation
Όπως και στα RDDs, τα operations στα DataFrames γίνονται lazy evaluated. Αυτό σημαίνει ότι δεν εκτελούνται αμέσως, αλλά όταν θέλουμε να πάρουμε το αποτέλεσμα.

Τα operations που αναγκάζουν την εκτέλεση των υπολογισμών είναι τα παρακάτω:

Συγκέντρωση όλων των αποτελεσμάτων σε centralized δομή στον driver:
```scala
df.collect()
//res15: Array[org.apache.spark.sql.Row] = Array([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12], [13], [14], [15], [16], [17], [18], [19], [20], [21], [22], [23], [24], [25], [26], [27], [28], [29], [30], [31], [32], [33], [34], [35], [36], [37], [38], [39], [40], [41], [42], [43], [44], [45], [46], [47], [48], [49], [50])
```
Αποθήκευση των δεδομένων στο σκληρό δίσκο:
```scala
df.write.json("test.txt")	//Εδώ αποθηκεύονται τα δεδομένα σε Json μορφή
```
Καταμέτρηση του πλήθους των γραμμών:
```scala
df.count()
res16: Long = 50
```
Εμφάνιση των αποτελεσμάτων στην οθόνη:
```scala
df.show()
```

## Query Engine
Το Spark SQL API διαθέτει ένα query engine, το οποίο κατασκευάζει logical και physical query plans για την εκτέλεση των υπολογισμών.

Οι μέθοδοι `withColumn`, `filter`, `drop`, `groupBy`, `aggregate` δεν εκτελούν κάποια επεξεργασία στα δεδομένα, αλλά ενημερώνουν το logical query plan.

Όταν εκτελέσουμε μέθοδο που αναγκάζει τον υπολογισμό του αποτελέσματος (`collect`, `write`, `count`, `show`) τότε το logical plan γίνεται optimized και έπειτα μετατρέπεται σε physical plan.

Μπορούμε να δούμε τα πλάνα που κατασκευάζονται χρησιμοποιώντας την εντολή explain:
```scala
df2.explain(true)
//== Parsed Logical Plan ==
//'Project [*,('value % 10) AS groups#11]
//+- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//   +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double, groups: int
//Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//+- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//   +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//LocalRelation [value#3,halfValue#6,groups#11], [[1,0.5,1],[2,1.0,2],[3,1.5,3],[4,2.0,4],[5,2.5,5],[6,3.0,6],[7,3.5,7],[8,4.0,8],[9,4.5,9],[10,5.0,0],[11,5.5,1],[12,6.0,2],[13,6.5,3],[14,7.0,4],[15,7.5,5],[16,8.0,6],[17,8.5,7],[18,9.0,8],[19,9.5,9],[20,10.0,0],[21,10.5,1],[22,11.0,2],[23,11.5,3],[24,12.0,4],[25,12.5,5],[26,13.0,6],[27,13.5,7],[28,14.0,8],[29,14.5,9],[30,15.0,0],[31,15.5,1],[32,16.0,2],[33,16.5,3],[34,17.0,4],[35,17.5,5],[36,18.0,6],[37,18.5,7],[38,19.0,8],[39,19.5,9],[40,20.0,0],[41,20.5,1],[42,21.0,2],[43,21.5,3],[44,22.0,4],[45,22.5,5],[46,23.0,6],[47,23.5,7],[48,24.0,8],[49,24.5,9],[50,25.0,0]]
//
//== Physical Plan ==
//LocalTableScan [value#3,halfValue#6,groups#11], [[1,0.5,1],[2,1.0,2],[3,1.5,3],[4,2.0,4],[5,2.5,5],[6,3.0,6],[7,3.5,7],[8,4.0,8],[9,4.5,9],[10,5.0,0],[11,5.5,1],[12,6.0,2],[13,6.5,3],[14,7.0,4],[15,7.5,5],[16,8.0,6],[17,8.5,7],[18,9.0,8],[19,9.5,9],[20,10.0,0],[21,10.5,1],[22,11.0,2],[23,11.5,3],[24,12.0,4],[25,12.5,5],[26,13.0,6],[27,13.5,7],[28,14.0,8],[29,14.5,9],[30,15.0,0],[31,15.5,1],[32,16.0,2],[33,16.5,3],[34,17.0,4],[35,17.5,5],[36,18.0,6],[37,18.5,7],[38,19.0,8],[39,19.5,9],[40,20.0,0],[41,20.5,1],[42,21.0,2],[43,21.5,3],[44,22.0,4],[45,22.5,5],[46,23.0,6],[47,23.5,7],[48,24.0,8],[49,24.5,9],[50,25.0,0]]
```
Εδώ παρατηρούμε ότι το spark κάνει optimize το logical plan ώστε να εκτελέσει σε ένα βήμα όλους τους υπολογισμούς που γίνονται στο `df2`.
```scala
df3.explain(true)
//== Parsed Logical Plan ==
//'Filter UDF('value)
//+- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//   +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//      +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double, groups: int
//Filter if (isnull(value#3)) null else UDF(value#3)
//+- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//   +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//      +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11]
//+- Filter UDF(value#3)
//   +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Physical Plan ==
//Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11]
//+- Filter UDF(value#3)
//   +- LocalTableScan [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
```
Εδώ παρατηρούμε ότι το Spark δεν μπορεί να κάνει optimize το `udf` και να το ενσωματώσει σε ένα βήμα εκτέλεσης γιατί δεν έχει έλεγχο στον κώδικά του.

Τα UDFs παρόλο που διευκολύνουν την επεξεργασία των δεδομένων, θα πρέπει να αποφεύγονται όσο είναι δυνατό γιατί επηρεάζουν το performance της ανάλυσης.

## Caching
Κάθε φορά που εκτελούμε κάποια από τις μεθόδους που αναγκάζουν τον υπολογισμό του αποτελέσματος (`collect`, `write`, `count`, `show`) το Spark εκτελεί όλο το ερώτημα από το πρώτο του βήμα. Παράδειγμα:
```scala
val df5 = df3.filter(col("value") < 20)
df5.explain(true)
//== Parsed Logical Plan ==
//'Filter ('value < 20)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double, groups: int
//Filter (value#3 < 20)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11]
//+- Filter (UDF(value#3) && (value#3 < 20))
//   +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Physical Plan ==
//Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11]
//+- Filter (UDF(value#3) && (value#3 < 20))
//   +- LocalTableScan [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
```
```scala
val df6 = df3.filter(col("value") > 20)
df6.explain(true)
//== Parsed Logical Plan ==
//'Filter ('value > 20)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double, groups: int
//Filter (value#3 > 20)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11]
//+- Filter (UDF(value#3) && (value#3 > 20))
//   +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Physical Plan ==
//Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11]
//+- Filter (UDF(value#3) && (value#3 > 20))
//   +- LocalTableScan [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
```
Εδώ πραγματοποιεί την επεξεργασία του `df3` πρώτα για το `df5` και μετά ξανά υπολογίζει από την αρχή το `df3` για να το `df6`.

Μπορούμε να κρατήσουμε στη μνήμη τον υπολογισμό του `df3`, για να ξεκινάει από εκεί ο υπολογισμός:
```scala
val df4 = df3.cache()
//df4: df3.type = [value: int, halfValue: double, groups: int]
val df5 = df4.filter(col("value") < 20)
df5.explain(true)
//== Parsed Logical Plan ==
//'Filter ('value < 20)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double, groups: int
//Filter (value#3 < 20)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//Filter (value#3 < 20)
//+- InMemoryRelation [value#3,halfValue#6,groups#11], true, 10000, StorageLevel(true, true, false, true, 1), Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11], None
//
//== Physical Plan ==
//Filter (value#3 < 20)
//+- InMemoryColumnarTableScan [value#3,halfValue#6,groups#11], [(value#3 < 20)], InMemoryRelation [value#3,halfValue#6,groups#11], true, 10000, StorageLevel(true, true, false, true, 1), Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11], None
```
```scala
val df6 = df4.filter(col("value") > 20)
df6.explain(true)
//== Parsed Logical Plan ==
//'Filter ('value > 20)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double, groups: int
//Filter (value#3 > 20)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//Filter (value#3 > 20)
//+- InMemoryRelation [value#3,halfValue#6,groups#11], true, 10000, StorageLevel(true, true, false, true, 1), Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11], None
//
//== Physical Plan ==
//Filter (value#3 > 20)
//+- InMemoryColumnarTableScan [value#3,halfValue#6,groups#11], [(value#3 > 20)], InMemoryRelation [value#3,halfValue#6,groups#11], true, 10000, StorageLevel(true, true, false, true, 1), Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11], None
```
Προσοχή: το caching πρέπει να χρησιμοποιείται με προσοχή γιατί επηρεάζει τα optimizations που γίνονται στο query planning από το Spark.
Για παράδειγμα ο κώδικας που είχαμε πριν παράγει ένα optimized physical plan:
```scala
val ds: Dataset[Int] = (1 to 50).toDS()
val df = ds.toDF()
val df1 = df.withColumn("halfValue", col("value") / 2)
df1.explain(true)
//== Parsed Logical Plan ==
//'Project [*,('value / 2) AS halfValue#75]
//+- LocalRelation [value#73], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double
//Project [value#73,(cast(value#73 as double) / cast(2 as double)) AS halfValue#75]
//+- LocalRelation [value#73], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//LocalRelation [value#73,halfValue#75], [[1,0.5],[2,1.0],[3,1.5],[4,2.0],[5,2.5],[6,3.0],[7,3.5],[8,4.0],[9,4.5],[10,5.0],[11,5.5],[12,6.0],[13,6.5],[14,7.0],[15,7.5],[16,8.0],[17,8.5],[18,9.0],[19,9.5],[20,10.0],[21,10.5],[22,11.0],[23,11.5],[24,12.0],[25,12.5],[26,13.0],[27,13.5],[28,14.0],[29,14.5],[30,15.0],[31,15.5],[32,16.0],[33,16.5],[34,17.0],[35,17.5],[36,18.0],[37,18.5],[38,19.0],[39,19.5],[40,20.0],[41,20.5],[42,21.0],[43,21.5],[44,22.0],[45,22.5],[46,23.0],[47,23.5],[48,24.0],[49,24.5],[50,25.0]]
//
//== Physical Plan ==
//LocalTableScan [value#73,halfValue#75], [[1,0.5],[2,1.0],[3,1.5],[4,2.0],[5,2.5],[6,3.0],[7,3.5],[8,4.0],[9,4.5],[10,5.0],[11,5.5],[12,6.0],[13,6.5],[14,7.0],[15,7.5],[16,8.0],[17,8.5],[18,9.0],[19,9.5],[20,10.0],[21,10.5],[22,11.0],[23,11.5],[24,12.0],[25,12.5],[26,13.0],[27,13.5],[28,14.0],[29,14.5],[30,15.0],[31,15.5],[32,16.0],[33,16.5],[34,17.0],[35,17.5],[36,18.0],[37,18.5],[38,19.0],[39,19.5],[40,20.0],[41,20.5],[42,21.0],[43,21.5],[44,22.0],[45,22.5],[46,23.0],[47,23.5],[48,24.0],[49,24.5],[50,25.0]]
```
Αντίθετα ο παρακάτω κώδικας δεν παράγει το ίδιο optimization επειδή το caching δεν επιτρέπει τον υπολογισμό του αποτελέσματος σε ένα βήμα:
```scala
val ds: Dataset[Int] = (1 to 50).toDS()
val df = ds.toDF().cache()
val df1 = df.withColumn("halfValue", col("value") / 2)
df1.explain(true)
//== Parsed Logical Plan ==
//'Project [*,('value / 2) AS halfValue#85]
//+- LocalRelation [value#78], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double
//Project [value#78,(cast(value#78 as double) / cast(2 as double)) AS halfValue#85]
//+- LocalRelation [value#78], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//Project [value#78,(cast(value#78 as double) / 2.0) AS halfValue#85]
//+- InMemoryRelation [value#78], true, 10000, StorageLevel(true, true, false, true, 1), ConvertToUnsafe, None
//
//== Physical Plan ==
//Project [value#78,(cast(value#78 as double) / 2.0) AS halfValue#85]
//+- InMemoryColumnarTableScan [value#78], InMemoryRelation [value#78], true, 10000, StorageLevel(true, true, false, true, 1), ConvertToUnsafe, None
```

## Partitioning in DataFrames
Το Spark SQL API υποστηρίζει τα εξής 3 partitioning schemes: round robin partitioning, hash partitioning, range partitioning (το τελευταίο υποστηρίζεται από την έκδοση 2.3.0 και μετά).

Το round robin χρησιμοποιείται από το spark όταν ο προγραμματιστής καλέσει τη μέθοδο `repartition(N)` όπου το N είναι το πλήθος των partitions. Εδώ δεν ορίζεται πουθενά συγκεκριμένη κολώνα για το partitioning, και έτσι το spark μοιράζει equally τα records στα nodes με round robin. Πχ:
```scala
df3.repartition(3).explain(true)
//== Parsed Logical Plan ==
//Repartition 3, true
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double, groups: int
//Repartition 3, true
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//Repartition 3, true
//+- InMemoryRelation [value#3,halfValue#6,groups#11], true, 10000, StorageLevel(true, true, false, true, 1), Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11], None
//
//== Physical Plan ==
//TungstenExchange RoundRobinPartitioning(3), None
//+- InMemoryColumnarTableScan [value#3,halfValue#6,groups#11], InMemoryRelation [value#3,halfValue#6,groups#11], true, 10000, StorageLevel(true, true, false, true, 1), Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11], None
```
Το hash partitioning  χρησιμοποιείται από το spark όταν ο προγραμματιστής καλέσει τη μέθοδο `repartition(C)`, όπου το C είναι μια συγκεκριμένη κολώνα (σημείωση: το C μπορεί να είναι και συγκεκριμένη έκφραση πάνω σε κολώνα ή ακόμα και `udf`). Εδώ το Spark, παίρνει το value από το C και το περνάει μέσα από μια hash function (συγκεκριμένα χρησιμοποιεί murmur hashing). Με βάση το αποτέλεσμα, αναθέτει partition στο row. Το πλήθος των partitions ορίζεται από μια παράμετρο του Spark (`spark.sql.shuffle.partitions`). Εναλλακτικά, μπορεί να χρησιμοποιηθεί η μέθοδος `repartition(N, C)` όπου δίνεται πλήθος partitions αλλά και κολώνα. Πχ:
```scala
df3.repartition(3, col("groups")).explain(true)
//== Parsed Logical Plan ==
//'RepartitionByExpression ['groups], Some(3)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double, groups: int
//RepartitionByExpression [groups#11], Some(3)
//+- Filter if (isnull(value#3)) null else UDF(value#3)
//   +- Project [value#3,halfValue#6,(value#3 % 10) AS groups#11]
//      +- Project [value#3,(cast(value#3 as double) / cast(2 as double)) AS halfValue#6]
//         +- LocalRelation [value#3], [[0,1],[0,2],[0,3],[0,4],[0,5],[0,6],[0,7],[0,8],[0,9],[0,a],[0,b],[0,c],[0,d],[0,e],[0,f],[0,10],[0,11],[0,12],[0,13],[0,14],[0,15],[0,16],[0,17],[0,18],[0,19],[0,1a],[0,1b],[0,1c],[0,1d],[0,1e],[0,1f],[0,20],[0,21],[0,22],[0,23],[0,24],[0,25],[0,26],[0,27],[0,28],[0,29],[0,2a],[0,2b],[0,2c],[0,2d],[0,2e],[0,2f],[0,30],[0,31],[0,32]]
//
//== Optimized Logical Plan ==
//RepartitionByExpression [groups#11], Some(3)
//+- InMemoryRelation [value#3,halfValue#6,groups#11], true, 10000, StorageLevel(true, true, false, true, 1), Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11], None
//
//== Physical Plan ==
//TungstenExchange hashpartitioning(groups#11,3), None
//+- InMemoryColumnarTableScan [value#3,halfValue#6,groups#11], InMemoryRelation [value#3,halfValue#6,groups#11], true, 10000, StorageLevel(true, true, false, true, 1), Project [value#3,(cast(value#3 as double) / 2.0) AS halfValue#6,(value#3 % 10) AS groups#11], None
```
Το range partitioning χρησιμοποιείται από το spark όταν ο προγραμματιστής καλέσει τη μέθοδο `repartitionByRange(C)`, όπου το C είναι μια συγκεκριμένη κολώνα (όπως και από πάνω). Εδώ το Spark κάνει ένα μικρό sampling στο dataframe, και τραβάει το sample στον driver. Χρησιμοποιώντας το sample προσπαθεί να ορίσει τα ranges με τα οποία θα κάνει το partitioning σε ολόκληρο το DataFrame. Το πλήθος των partitions ορίζεται όπως και προηγουμένως είτε από την παράμετρο `spark.sql.shuffle.partitions` του Spark, είτε χρησιμοποιώντας τη μέθοδο  `repartitionByRange(Ν, C)`. Πχ: (ΜΟΝΟ σε έκδοση Spark 2.3.0 και πάνω)
```scala
df3.repartitionByRange(3, col("groups")).explain(true)
//== Parsed Logical Plan ==
//'RepartitionByExpression ['groups ASC NULLS FIRST], 3
//+- Filter if (isnull(value#4)) null else UDF(value#4)
//   +- Project [value#4, halfValue#6, (value#4 % 10) AS groups#9]
//      +- Project [value#4, (cast(value#4 as double) / cast(2 as double)) AS halfValue#6]
//         +- Project [value#2 AS value#4]
//            +- SerializeFromObject [input[0, int, false] AS value#2]
//               +- ExternalRDD [obj#1]
//
//== Analyzed Logical Plan ==
//value: int, halfValue: double, groups: int
//RepartitionByExpression [groups#9 ASC NULLS FIRST], 3
//+- Filter if (isnull(value#4)) null else UDF(value#4)
//   +- Project [value#4, halfValue#6, (value#4 % 10) AS groups#9]
//      +- Project [value#4, (cast(value#4 as double) / cast(2 as double)) AS halfValue#6]
//         +- Project [value#2 AS value#4]
//            +- SerializeFromObject [input[0, int, false] AS value#2]
//               +- ExternalRDD [obj#1]
//
//== Optimized Logical Plan ==
//RepartitionByExpression [groups#9 ASC NULLS FIRST], 3
//+- Project [value#2, (cast(value#2 as double) / 2.0) AS halfValue#6, (value#2 % 10) AS groups#9]
//   +- Filter UDF(value#2)
//      +- SerializeFromObject [input[0, int, false] AS value#2]
//         +- ExternalRDD [obj#1]
//
//== Physical Plan ==
//Exchange rangepartitioning(groups#9 ASC NULLS FIRST, 3)
//+- *(1) Project [value#2, (cast(value#2 as double) / 2.0) AS halfValue#6, (value#2 % 10) AS groups#9]
//   +- *(1) Filter UDF(value#2)
//      +- *(1) SerializeFromObject [input[0, int, false] AS value#2]
//         +- Scan[obj#1]
```

## DataFrames and RDDs
Ένα DataFrame μπορεί να μετατραπεί σε RDD για την επεξεργασία των δεδομένων του με τις μεθόδους ενός RDD:
```scala
val rdd = df3.rdd
//rdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[59]
```
Η μετατροπή ενός DataFrame σε RDD είναι μια ακριβή διαδικασία γιατί πρέπει να μετατραπεί όλη η δομή από column based (DataFrames) σε row based (RDDs). Τα δεδομένα ενός DataFrame κωδικοποιούνται από το Spark με τέτοιο τρόπο, ώστε να είναι εύκολη η επεξεργασία μεμονομένων columns. Επίσης το serialization που πραγματοποιεί το Spark στα DataFrames, είναι optimized για column-based access στα δεδομένα. Η μετατροπή του DataFrame σε RDD δημιουργεί ένα με τύπο δεδομένων Row (`RDD[Row]`). Το Row είναι μια γενική δομή που μπορεί να περιέχει οποιοδήποτε πλήθος/τύπο απο πεδία (κολώνες). Η μετατροπή λοιπόν των δεδομένων ενός DataFrame από την εσωτερική μορφή σε Row μορφή, έχει μεγάλο κόστος αφού απαιτεί το deserialization όλων των πεδίων και δημιουργία νέας δομής για κάθε γραμμή.

Στο DataFrame, μπορούμε να εκτελέσουμε `map` συναρτήσεις, οι οποίες όμως μετατρέπουν εσωτερικά το DataFrame σε RDD και στη συνέχεια επιστρέφουν το RDD αφού εκτελέσουν σε αυτό τη `map` συνάρτηση.
Παράδειγμα εκτέλεσης map συνάρτησης:
```scala
import org.apache.spark.sql.Row
df3.map((r: Row) => {
  r.getAs[Int]("value") + 1
})
//res30: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[60]
```
Το DataFrame `df3` περιέχει 3 κολώνες: `value`, `halfValue` και `groups`. Κάθε αντικείμενο `r: Row` περιέχει τιμές και για τα 3 πεδία. Μπορούμε να πάρουμε την τιμή του κάθε πεδίου για τη συγκεκριμένη γραμμή χρησιμοποιώντας τη μέθοδο getAs και το όνομα του πεδίου. Το παραπάνω παράδειγμα παίρνει ως όρισμα ένα RDD με Rows και επιστρέφει ένα RDD με ένα μόνο πεδίο (το value προσαυξημένο κατά 1).

Παρόλο που η μετατροπή ενός DataFrame σε RDD πρέπει να αποφεύγεται για λόγους επιδόσεων, μπορεί να βοηθήσει σε περίπτωση που θέλουμε να επιτύχουμε ένα συγκεκριμένο partitioning στα DataFrames το οποίο δεν υποστηρίζεται natively από το Spark. Η μετατροπή ενός RDD σε DataFrame διατηρεί το partitioning του RDD στο DataFrame.

## Working with JSON data
Μπορούμε να δημιουργήσουμε ένα DataFrame διαβάζοντας από JSON αρχείο:
```scala
val dfJson = sqlContext.read.json("t.txt")
dfJson.printSchema
```

Τα δεδομένα είναι διαθέσιμα σε named columns, χρησιμοποιώντας τα ονόματα των πεδίων από το JSON αρχείο. Υποστηρίζονται nested columns χρησιμοποιώντας το σύμβολο `.` πχ: `user.followers_count`

By default, το Spark προσθέτει μια επιπλέον κολώνα στο DataFrame με όνομα `_corrupt_record`. Η κολώνα αυτή παίρνει τιμή null στις γραμμές που είναι valid JSON records. Στις γραμμές που έχουμε invalid JSON records, η κολώνα αυτή παίρνει την τιμή όλης της γραμμής.

Μπορούμε να βρούμε το πλήθος των invalid records στο JSON αρχείο:
```scala
dfJson.filter(col("_corrupt_record").isNotNull).count
```

Ή και να κρατήσουμε μόνο τα valid records:
```scala
val dfJ1 = dfJson.filter(col("_corrupt_record").isNull).drop("_corrupt_record")
```

Επιλέγουμε ορισμένα πεδία για να συνεχίσουμε την επεξεργασία:
```scala
val dfJ2 = dfJ1.select(col("text"),
      col("lang"),
      col("user.followers_count").as("user_followers"),
      col("user.name").as("user_name")
```

Εύρεση των χρηστών με το μεγαλύτερο μέσο όρο χαρακτήρων ανά μήνυμα.

Αρχικά δημιουργούμε μια κολώνα που περιέχει το πλήθος των χαρακτήρων στην κολώνα `text` χρησιμοποιώντας τη συνάρτηση `length`:
```scala
val dfJ3 = dfJ2.withColumn("textLength", length(col("text")))
```

Στη συνέχεια εκτελούμε το query:
```scala
dfJ3.groupBy("user_name").agg(avg("textLength").as("AvgTextSize")).orderBy(col("AvgTextSize").desc).show
```

Εύρεση του μέγιστου πλήθους των followers σε κάποιο χρήστη:
```scala
dfJ2.select(max("user_followers")).show
```

Εύρεση του χρήση με τους περισσότερους followers. Εδώ δεν μπορούμε να χρησιμοποιήσουμε τη συνάρτηση max γιατί δε μας δίνει πρόσβαση σε ολόκληρο το row ώστε να μπορέσουμε να πάρουμε και την τιμή του user_name. Έτσι στο παρακάτω παράδειγμα, κάνουμε sort όλο το DataFrame και επιλέγουμε την πρώτη γραμμή:
```scala
dfJ2.orderBy(col("user_followers").desc).select(first("user_name"), first("user_followers")).show
```

Επειδή αυτή η λύση έχει αρκετά μεγάλο κόστος σε αποδοτικότητα, ένας άλλος τρόπος είναι να το κάνουμε σε 2 βήματα, με μικρότερο κόστος:
```scala
val m = dfJ2.select(max("user_followers")).first().getLong(0)
dfJ2.select("user_followers", "user_name").filter(col("user_followers") === m).show
```

Ένας άλλος αποδοτικός τρόπος, είναι να χρησιμοποιήσουμε τη συνάρτηση struct η οποία ομαδοποιεί 2 ή περισσότερες κολώνες σε μία:
```scala
dfJ2.withColumn("st", struct(col("user_followers"), col("user_name"))).
      select(max("st").as("st")).
      select(col("st.user_followers").as("user_followers"), col("st.user_name").as("user_name")).
      show
```

Το struct μπορεί επίσης να βοηθήσει στην περίπτωση που θέλουμε να βρούμε το χρήστη με το μέγιστο πλήθος followers ανά γλώσσα:
```scala
dfJ2.withColumn("st", struct(col("user_followers"), col("user_name"))).
      groupBy(col("lang")).agg(max(col("st")).as("st")).
      select(col("lang"), col("st.user_followers").as("user_followers"), col("st.user_name").as("user_name")).
      orderBy(col("user_followers").desc).show(false)
```



## Resources
Spark SQL paper: https://dblp.org/rec/conf/sigmod/ArmbrustXLHLBMK15

Spark SQL Documentation (1.6.1): https://spark.apache.org/docs/1.6.1/sql-programming-guide.html

Spark SQL Documentation (latest): https://spark.apache.org/docs/latest/sql-programming-guide.html

Book: Spark The Definitive Guide (O'REILLY)
