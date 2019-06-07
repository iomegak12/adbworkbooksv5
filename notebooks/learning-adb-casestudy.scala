// Databricks notebook source
val secret = dbutils.secrets.get(scope = "sql-credentials", key = "sqlpassword")

print(secret)

// COMMAND ----------

// DBTITLE 1,Accessing the Complete Table (SQL Configuration)
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url"            -> "iomegasqlserverv2.database.windows.net",
  "databaseName"   -> "iomegasqldatabasev2",
  "dbTable"        -> "dbo.Customers",
  "user"           -> "iomegaadmin",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection = spark.read.sqlDB(config)

collection.printSchema
collection.createOrReplaceTempView("customers")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM customers

// COMMAND ----------

// DBTITLE 1,Execute Query to Populate Data Frame
val query = "SELECT * FROM Locations"
val config1 = Config(Map(
	  "url"          -> "iomegasqlserverv2.database.windows.net",
	  "databaseName" -> "iomegasqldatabasev2",
	  "user"         -> "iomegaadmin",
	  "password"     -> secret,
	  "queryCustom"  -> query
	))

val collection1 = spark.read.sqlDB(config1)

collection1.createOrReplaceTempView("locations")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM locations

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC WHERE L.State in ( "AP", "TN" )

// COMMAND ----------

// DBTITLE 1,DLS Configuration to Volume Mounting
val dlsSecret = dbutils.secrets.get(scope = "training-scope", key = "adbclientaccesskey")
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "c923f5fe-b583-4268-bb90-6c6530e9c972",
  "fs.azure.account.oauth2.client.secret" -> dlsSecret,
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/381a10df-8e85-43db-86e1-8893b075b027/oauth2/token")

dbutils.fs.mount(
  source = "abfss://data@iomegastoragev2.dfs.core.windows.net/",
  mountPoint = "/mnt/dlsdata",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /mnt/dlsdata/salesfiles/

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val fileNames = "/mnt/dlsdata/salesfiles/*.csv"
val schema = StructType(
Array(
StructField("SaleId",IntegerType,true),
StructField("SaleDate",IntegerType,true),
StructField("CustomerId",DoubleType,true),
StructField("EmployeeId",DoubleType,true),
StructField("StoreId",DoubleType,true),
StructField("ProductId",DoubleType,true),
StructField("NoOfUnits",DoubleType,true),
StructField("SaleAmount",DoubleType,true),
StructField("SalesReasonId",DoubleType,true),
StructField("ProductCost",DoubleType,true)
)
)

val data = spark.read.option("inferSchema",false).option("header","true").option("sep",",").schema(schema).csv(fileNames)

data.printSchema
data.createOrReplaceTempView("factsales")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TEMP VIEW ProcessedResults
// MAGIC AS
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country,
// MAGIC   S.SaleAmount, S.NoOfUnits
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC INNER JOIN factsales S on S.CustomerId = C.CustomerId
// MAGIC WHERE L.State in ( "AP", "TN" )

// COMMAND ----------

val results = spark.sql("SELECT * FROM ProcessedResults")

results.printSchema
results.write.mode("append").parquet("/mnt/dlsdata/parque/sales")

// COMMAND ----------

val processedSales = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("/mnt/dlsdata/parque/sales")
processedSales.printSchema
processedSales.show(100, false)

// COMMAND ----------

// DBTITLE 1,DW and Polybase Configuration to Bulk Updates
val blobStorage = "iomegastoragev4.blob.core.windows.net"
val blobContainer = "tempdata"
val blobAccessKey =  "I8Y6WRWBZQfAD4LvfHoyRVi0AuiVznI9P+Ks2oaPX2a+DpiAainGEJDz8d+fSz/pscLD4+SrWMw1SLHUxZyZCQ=="
val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

// COMMAND ----------

val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

val dwDatabase = "iomegadatawarehouse"
val dwServer = "iomegasqlserverv2.database.windows.net"
val dwUser = "iomegaadmin"
val dwPass = secret
val dwJdbcPort =  "1433"
val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")

// COMMAND ----------

results.write
    .format("com.databricks.spark.sqldw")
    .option("url", sqlDwUrlSmall) 
    .option("dbtable", "ProcessedResults")
    .option("forward_spark_azure_storage_credentials","True")
    .option("tempdir", tempDir)
    .mode("overwrite")
    .save()