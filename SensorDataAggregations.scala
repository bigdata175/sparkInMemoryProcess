
/////   Source Code

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._

val inputSensorDataPath="hdfs://input/sensorData/directory/"
val data = sc.wholeTextFiles(inputSensorDataPath)
var fileCount = sc.longAccumulator
var totalRecords = sc.longAccumulator
var processedRecords = sc.longAccumulator
var failedRecords = sc.longAccumulator
var dataRDD = sc.emptyRDD[Row]
val customSchema = StructType(Array(StructField("sensorId", StringType, true),StructField("humidity", StringType, true)))
val MaxAvgMinSchema = StructType(Array(StructField("sensorId", StringType, true),StructField("MIN", DoubleType, true),StructField("AVG", DoubleType, true),StructField("MAX", DoubleType, true)))
var finalProcessedDF = spark.createDataFrame(dataRDD, MaxAvgMinSchema)
var finalFailedDF = spark.createDataFrame(dataRDD, MaxAvgMinSchema)
val files = data.map {case (filename, content) => filename}


def calMeasure(file: String) = {
println (file);
val sensorDF = spark.read.format("csv").option("delimiter",",").option("header", "true").schema(customSchema).load(file)
fileCount.add(1);
totalRecords.add(sensorDF.count);
val processedRec=sensorDF.filter("humidity != 'NaN'")
val processedDF=processedRec.groupBy(sensorDF.col("sensorId")).agg(min("humidity"), avg("humidity"), max("humidity")).orderBy(avg("humidity").desc)
processedRecords.add(processedRec.count)
val failedDF=sensorDF.filter("humidity = 'NaN'").groupBy(sensorDF.col("sensorId")).agg(min("humidity"), avg("humidity"), max("humidity")).orderBy(avg("humidity").desc)
failedRecords.add(failedDF.count)
finalProcessedDF=finalProcessedDF.union(processedDF)
finalFailedDF=finalFailedDF.union(failedDF)
}

files.collect.foreach( filename => {calMeasure(filename)});
val highAvgHumidityDF=(finalProcessedDF.groupBy(finalProcessedDF.col("sensorId")).agg(min("MIN"), avg("AVG"), max("MAX")).orderBy(avg("AVG").desc)).union(finalFailedDF.groupBy(finalFailedDF.col("sensorId")).agg(min("MIN"), avg("AVG"), max("MAX")).orderBy(avg("AVG").desc))

println(s" <--- Sensor Statistics --->");

println(s"[Info] Num of processed files: ${fileCount.value}");
println(s"[Info] Total Num of measurements: ${totalRecords.value}");
println(s"[Info] Num of processed measurements: ${processedRecords.value}");
finalProcessedDF.groupBy(finalProcessedDF.col("sensorId")).agg(min("MIN"), avg("AVG"), max("MAX")).orderBy(avg("AVG").desc).show()
println(s"[Info] Num of failed measurements: ${failedRecords.value}");
finalFailedDF.groupBy(finalFailedDF.col("sensorId")).agg(min("MIN"), avg("AVG"), max("MAX")).orderBy(avg("AVG").desc).show()
println(s"[Info] min/avg/max of processed measurements:");
println(s"[Info] min/avg/max of failed measurements:");
println(s"[Info] sensors by highest avg humidity:");
highAvgHumidityDF.show()

