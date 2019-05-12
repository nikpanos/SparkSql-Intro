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
```
val df = rdd.toDF("value")
```
