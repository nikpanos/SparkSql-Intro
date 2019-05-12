# Introduction to Spark SQL

Ο παρακάτω οδηγός αφορά την έκδοση 1.6.1 του spark. 

## DataFrame Creation
Η βασική δομή επεξεργασίας δεδομένων στο Sparql SQL API είναι ένα DataFrame.

Μπορούμε να δημιουργήσουμε ένα DataFrame είτε διαβάζοντας απευθείας από κάποιο αρχείο, είτε να μετατρέψουμε ένα RDD σε DataFrame:

Αρχικά δημιουργούμε ένα RDD που περιέχει νούμερα από το 1 μέχρι το 50:
```scala
val rdd: RDD[Int] = sc.parallelize(1 to 50)
```

Στη συνέχεια, μετατρέπουμε το RDD σε DataFrame χρησιμοποιώντας τη μέθοδο toDF και περνώντας ως παράμετρο το όνομα της κολώνας από την οποία θα αποτελείται:
```scala
val df = rdd.toDF("value")
```

Με τη μέθοδο show μπορούμε να εμφανίσουμε τα περιεχόμενα του DataFrame:
```scala
df.show()       //By default εμφανίζει τα πρώτα 20 rows. Σημείωση: Στη scala μπορούμε να καλούμε μεθόδους και χωρίς το "()" πχ. df.show
df.show(50)		  //Εδώ εμφανίζει τα 50 πρώτα rows
df.printSchema	//Τυπώνει το σχήμα των δεδομένων μέσα στο DataFrame
```

Ένας άλλος τρόπος για να δημιουργήσουμε DataFrames, είναι μέσω των Datasets. Ένα Dataset μοιάζει με ένα RDD αφού μπορεί να εκτελέσει περίπου τα ίδια operations (map, reduce, κλπ). Επίσης στα Datasets (όπως και στα RDD) πρέπει να δηλώνουμε τον τύπο δεδομένων από τον οποίο αποτελούνται.

Αρχικά δημιουργούμε ένα DataSet το οποίο έχει πάντα έχει ένα συγκεκριμένο τύπο δεδομένων. Εδώ έχει τον τύπο Int:
```scala
val ds: Dataset[Int] = (1 to 50).toDS()		//By default, το Dataset θα δημιουργήσει μια κολώνα που θα της δώσει το όνομα "value"
ds.show
```

Ένα Dataset μπορούμε να το μετατρέψουμε σε DataFrame ξανά με τη μέθοδο toDF:
```scala
val df = ds.toDF()
df.show
```

Τα DataFrames είναι μια semi-structured δομή όπου τα δεδομένα οργανώνονται σε named columns. Ένα DataFrame μπορεί να έχει οποιοδήποτε πλήθος/τύπο στηλών.

Τα DataFrames είναι column-based data structures σε αντίθεση με τα rdd που είναι row-based. Έτσι, σχεδόν operations στα DataFrames γίνονται πάνω σε συγκεκριμένες στήλες και όχι σε ολόκληρο το row.

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
```
Προσθήκη νέας κολώνας με όνομα "halfValue" η οποία θα πάρει σε κάθε γραμμή τη μισή τιμή της κολώνας "value":
```scala
val df1 = df.withColumn("halfValue", col("value") / 2)		//Υποστηρίζονται όλα τα αιρθμητικά operations (+ - * / %)
df1.show
```
Επιλογή μερικών από τις κολώνες:
```scala
df1.select(col("value")).show
```
Εμφάνιση της μέγιστης τιμής για μια κολώνα:
```scala
df1.select(max(col("value"))).show			//Υποστηρίζονται max, min, avg, stddev, sum, variance, first, last, και άλλα
```
Ομαδοποίηση των δεδομένων με βάση μια κολώνα και εξαγωγή στατιστικών ανά ομάδα:
```scala
val df2 = df1.withColumn("groups", col("value") % 10)
df2.groupBy("groups").agg(max(col("halfValue"))).show
```
Διαγραφή κολώνας από DataFrame:
```scala
df2.drop(col("halfValue")).show
```
Σημείωση: Τα DataFrames είναι immutable. Δε διαγράφεται η τιμή από το df2. Δημιουργείται ένα νέο DataFrame χωρίς την κολώνα "halfValue"

Υπάρχουν πολλά ακόμα operations που μπορούν να γίνουν πάνω σε dataframes με τη μορφή συναρτήσεων.

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
```

## User Defined Functions (udf)
Υπάρχουν περιπτώσεις όπου οι διαθέσιμες συναρτήσεις δεν παρέχουν τη λειτουργικότητα που θέλουμε.

Το Spark παρέχει τη δυνατότητα να γράφουμε δικές μας συναρτήσεις που θα εκτελεστούν πάνω σε κολώνες των δεδομένων.

Η παρακάτω udf συνάρτηση επιστρέφει true όταν το τρίτο bit είναι 1 και false όταν είναι 0:
```scala
val bitFilter = udf((value: Int) => ((value >> 2) & 1) == 1)
```
Οι udf συναρτήσεις μπορούν να χρησιμοποιηθούν σαν μια οποιαδήποτε συνάρτηση:
```scala
val df3 = df2.filter(bitFilter(col("value")))
df3.show
```
Οι udf συναρτήσεις μπορούν να παίρνουν ως όρισμα μία ή περισσότερες κολώνες και μία ή περισσότερες constant τιμές.

Μπορούν να χρησιμοποιηθούν οπουδήποτε ως αντικατάσταση μιας κολώνας πχ: στα aggregation operations, στα filter operations, στα withColumn operations και άλλα.

## Lazy Evaluation
Όπως και στα RDDs, τα operations στα DataFrames γίνονται lazy evaluated. Αυτό σημαίνει ότι δεν εκτελούνται αμέσως, αλλά όταν θέλουμε να πάρουμε το αποτέλεσμα.

Τα operations που αναγκάζουν την εκτέλεση των υπολογισμών είναι τα παρακάτω:

Συγκέντρωση όλων των αποτελεσμάτων σε centralized δομή στον driver:
```scala
df.collect()
```
Αποθήκευση των δεδομένων στο σκληρό δίσκο:
```scala
df.write.text("test.txt")	//Εδώ αποθηκεύονται τα δεδομένα σε απλή Text μορφή
```
Καταμέτρηση του πλήθους των γραμμών:
```scala
df.count()
```
Εμφάνιση των αποτελεσμάτων στην οθόνη:
```scala
df.show()
```

## Query Engine
Το Spark SQL API διαθέτει ένα query engine, το οποίο κατασκευάζει logical και physical query plans για την εκτέλεση των υπολογισμών.

Οι μέθοδοι withColumn, filter, drop, groupBy, aggregate δεν εκτελούν κάποια επεξεργασία στα δεδομένα, αλλά ενημερώνουν το logical query plan.

Όταν εκτελέσουμε μέθοδο που αναγκάζει τον υπολογισμό του αποτελέσματος (collect, write, count) τότε το logical plan γίνεται optimized και έπειτα μετατρέπεται σε physical plan.

Μπορούμε να δούμε τα πλάνα που κατασκευάζονται χρησιμοποιώντας την εντολή explain:
```scala
df2.explain(true)
```
Εδώ παρατηρούμε ότι το spark κάνει optimize το logical plan ώστε να εκτελέσει σε ένα βήμα όλους τους υπολογισμούς που γίνονται στο df2.
```scala
df3.explain(true)
```
Εδώ παρατηρούμε ότι το Spark δεν μπορεί να κάνει optimize το udf και να το ενσωματώσει σε ένα βήμα εκτέλεσης γιατί δεν έχει έλεγχο στον κώδικά του.

Τα UDFs παρόλο που διευκολύνουν την επεξεργασία των δεδομένων, θα πρέπει να αποφεύγονται όσο είναι δυνατό γιατί επηρεάζουν το performance της ανάλυσης.

## Caching
Κάθε φορά που εκτελούμε κάποια από τις μεθόδους που αναγκάζουν τον υπολογισμό του αποτελέσματος (collect, write, count) το Spark εκτελεί όλο το ερώτημα από το πρώτο του βήμα. Παράδειγμα:
```scala
val df5 = df3.filter(col("value") < 20)
df5.explain(true)
val df6 = df3.filter(col("value") > 20)
df6.explain(true)
```
Εδώ πραγματοποιεί την επεξεργασία του df3 πρώτα για το df5 και μετά ξανά υπολογίζει από την αρχή το df3 για να το df6.

Μπορούμε να κρατήσουμε στη μνήμη τον υπολογισμό του df3, για να ξεκινάει από εκεί ο υπολογισμός:
```scala
val df4 = df3.cache()
val df5 = df4.filter(col("value") < 20)
df5.explain(true)
val df6 = df4.filter(col("value") > 20)
df6.explain(true)
```
Προσοχή: το caching πρέπει να χρησιμοποιείται με προσοχή γιατί επηρεάζει τα optimizations που γίνονται στο query planning από το Spark.
Για παράδειγμα ο κώδικας που είχαμε πριν παράγει ένα optimized physical plan:
```scala
val ds: Dataset[Int] = (1 to 50).toDS()
val df = ds.toDF()
val df1 = df.withColumn("halfValue", col("value") / 2)
df1.explain(true)
```
Αντίθετα ο παρακάτω κώδικας δεν παράγει το ίδιο optimization επειδή το caching δεν επιτρέπει τον υπολογισμό του αποτελέσματος σε ένα βήμα:
```scala
val ds: Dataset[Int] = (1 to 50).toDS()
val df = ds.toDF().cache()
val df1 = df.withColumn("halfValue", col("value") / 2)
df1.explain(true)
```

## Partitioning in DataFrames
Το Spark SQL API υποστηρίζει τα εξής 3 partitioning schemes: round robin partitioning, hash partitioning, range partitioning (το τελευταίο υποστηρίζεται από την έκδοση 2.3.0 και μετά).

Το round robin χρησιμοποιείται από το spark όταν ο προγραμματιστής καλέσει τη μέθοδο repartition(N) όπου το N είναι το πλήθος των partitions. Εδώ δεν ορίζεται πουθενά συγκεκριμένη κολώνα για το partitioning, και έτσι το spark μοιράζει equally τα records στα nodes με round robin. Πχ:
```scala
df3.repartition(3).explain(true)
```
Το hash partitioning  χρησιμοποιείται από το spark όταν ο προγραμματιστής καλέσει τη μέθοδο repartition(C), όπου το C είναι μια συγκεκριμένη κολώνα (σημείωση: το C μπορεί να είναι και συγκεκριμένη έκφραση πάνω σε κολώνα ή ακόμα και udf). Εδώ το spark, παίρνει το value από το C και το περνάει μέσα από μια hash function (συγκεκριμένα χρησιμοποιεί murmur hashing). Με βάση το αποτέλεσμα, αναθέτει partition στο row. Το πλήθος των partitions ορίζεται από μια παράμετρο του spark (spark.sql.shuffle.partitions). Εναλλακτικά, μπορεί να χρησιμοποιηθεί η μέθοδος  repartition(N, C) όπου δίνεται πλήθος partitions αλλά και κολώνα. Πχ:
```scala
df3.repartition(3, col("groups")).explain(true)
```
Το range partitioning χρησιμοποιείται από το spark όταν ο προγραμματιστής καλέσει τη μέθοδο repartitionByRange(C), όπου το C είναι μια συγκεκριμένη κολώνα (όπως και από πάνω). Εδώ το spark κάνει ένα μικρό sampling στο dataframe, και τραβάει το sample στον driver. Χρησιμοποιώντας το sample προσπαθεί να ορίσει τα ranges με τα οποία θα κάνει το partitioning σε ολόκληρο το dataframe. Το πλήθος των partitions ορίζεται όπως και προηγουμένως είτε από την παράμετρο spark.sql.shuffle.partitions του spark, είτε χρησιμοποιώντας τη μέθοδο  repartitionByRange(Ν, C). Πχ: (ΜΟΝΟ σε έκδοση Spark 2.3.0 και πάνω)
```scala
df3.repartitionByRange(3, col("groups")).explain(true)
```

## DataFrames & RDDs
Ένα DataFrame μπορεί να μετατραπεί σε RDD για την επεξεργασία των δεδομένων του με τις μεθόδους ενός RDD:
```scala
val rdd = df3.rdd
```
Η μετατροπή ενός DataFrame σε RDD είναι μια ακριβή διαδικασία γιατί πρέπει να μετατραπεί όλη η δομή από column based (DataFrames) σε row based (RDDs). Τα δεδομένα ενός DataFrame κωδικοποιούνται από το Spark με τέτοιο τρόπο, ώστε να είναι εύκολη η επεξεργασία μεμονομένων columns. Επίσης το serialization που πραγματοποιεί το Spark στα DataFrames, είναι optimized για column-based access στα δεδομένα. Η μετατροπή του DataFrame σε RDD δημιουργεί ένα με τύπο δεδομένων Row (RDD[Row]). Το Row είναι μια γενική δομή που μπορεί να περιέχει οποιοδήποτε πλήθος/τύπο απο πεδία (κολώνες). Η μετατροπή λοιπόν των δεδομένων ενός DataFrame από την εσωτερική μορφή σε Row μορφή, έχει μεγάλο κόστος αφού απαιτεί το deserialization όλων των πεδίων και δημιουργία νέας δομής για κάθε γραμμή.

Στο DataFrame, μπορούμε να εκτελέσουμε map συναρτήσεις, οι οποίες όμως μετατρέπουν εσωτερικά το DataFrame σε RDD και στη συνέχεια επιστρέφουν το RDD αφού εκτελέσουν σε αυτό τη map συνάρτηση.
Παράδειγμα εκτέλεσης map συνάρτησης:
```scala
df3.map((r: Row) => {
  r.getAs[Int]("value") + 1
})
```
Το DataFrame df3 περιέχει 3 κολώνες: value, halfValue και groups. Κάθε αντικείμενο "r: Row" περιέχει τιμές και για τα 3 πεδία. Μπορούμε να πάρουμε την τιμή του κάθε πεδίου για τη συγκεκριμένη γραμμή χρησιμοποιώντας τη μέθοδο getAs και το όνομα του πεδίου. Το παραπάνω παράδειγμα παίρνει ως όρισμα ένα RDD με Rows και επιστρέφει ένα RDD με ένα μόνο πεδίο (το value προσαυξημένο κατά 1).

Παρόλο που η μετατροπή ενός DataFrame σε RDD πρέπει να αποφεύγεται για λόγους επιδόσεων, μπορεί να βοηθήσει σε περίπτωση που θέλουμε να επιτύχουμε ένα συγκεκριμένο partitioning στα DataFrames το οποίο δεν υποστηρίζεται natively από το Spark. Η μετατροπή ενός RDD σε DataFrame διατηρεί το partitioning του RDD στο DataFrame.

## Resources
Spark SQL paper: https://dblp.org/rec/conf/sigmod/ArmbrustXLHLBMK15

Spark SQL Documentation (1.6.1): https://spark.apache.org/docs/1.6.1/sql-programming-guide.html

Spark SQL Documentation (latest): https://spark.apache.org/docs/latest/sql-programming-guide.html

Book: Spark The Definitive Guide (O'REILLY)
