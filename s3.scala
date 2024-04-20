/* cmd: spark-shell
chcp 65001 && spark-shell -i D:\Geekbrains_ETL\s3.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"

import scala.io.Source
val lines = Source.fromFile("/Users/Alex/Desktop/Geekbrains/Geekbrains_ETL/Seminars/s3.txt").getLines.toList
println("test "+lines(0))
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis()
val mysqlcon = "jdbc:mysql://localhost:3306/spark?user=root&password="

//if(1==1){
var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("D:/Geekbrains_ETL/s3.xlsx")
	
	df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl3a")
        .mode("overwrite").save()

val web_3= """
SELECT ei, FROM_UNIXTIME(StatusTime) StatusTime, 
	IFNULL ((LEAD(StatusTime) OVER(PARTITION BY ei ORDER BY StatusTime) - StatusTime) / 3600, 0) Длительность,
	case when status IS NULL then @prev1 
	ELSE @prev1:=status 
	END status,
	case when `group` IS NULL then @prev2 
	ELSE @prev2:=`group` 
	END `group`
FROM 
(SELECT ei, StatusTime,  status, 
	if (row_number() OVER(PARTITION BY ei ORDER BY StatusTime) = 1 AND Назначение IS NULL,"",`group`) `group` 
FROM 
(SELECT DISTINCT a.objectid ei, a.restime StatusTime, status, `group`, Назначение, 
(SELECT @prev1:=''),
(SELECT @prev2:='') FROM 
	(SELECT DISTINCT a.objectId, a.restime FROM spark.tasketl3a AS a
	WHERE fieldname IN ('status','gname2')) a
LEFT JOIN 
	(SELECT DISTINCT objectId, restime, fieldvalue STATUS FROM spark.tasketl3a
 	WHERE fieldname IN ('status')) a1
ON a.objectid = a1.objectId AND a.restime = a1.restime

LEFT JOIN 
	(SELECT DISTINCT objectId, restime, fieldvalue `group`, 1 Назначение FROM spark.tasketl3a
 	WHERE fieldname IN ('gname2')) a2
ON a.objectid = a2.objectId AND a.restime = a2.restime) b1) b2
ORDER BY 1,2
"""

spark.read.format("jdbc").option("url",mysqlcon)
.option("driver","com.mysql.cj.jdbc.Driver")
.option("query", web_3).load()
.write.format("jdbc").option("url",mysqlcon)
.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl3b")
.mode("overwrite").save()


val web_3_1="""
WITH tb AS (SELECT ei, CONCAT(Statustime, ' ', status, ' ', `group`) con
FROM tasketl3b)
SELECT ei, GROUP_CONCAT(con SEPARATOR '\r\n')con FROM tb
GROUP BY 1
"""

spark.read.format("jdbc").option("url",mysqlcon)
.option("driver","com.mysql.cj.jdbc.Driver")
.option("query", web_3).load()
.write.format("jdbc").option("url",mysqlcon)
.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl3_hw")
.mode("overwrite").save()

val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)