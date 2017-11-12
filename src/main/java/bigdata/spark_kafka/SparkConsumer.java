package bigdata.spark_kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkConsumer {

	public static void main(String[] args) throws StreamingQueryException {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop-common-2.2.0-bin-master");

		StructType schema = new StructType().add("productName",
				"string").add("time", DataTypes.TimestampType);

		SparkSession sparkSession = SparkSession.builder().appName("sparkConsumer").config("spark.master", "local")
				.getOrCreate();

		Dataset<EcommerceLog> ds = sparkSession.readStream().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "ecommerce_topic").load()
				.selectExpr("CAST(value AS STRING) as message")
				.select(functions.from_json(functions.col("message"), schema).as("json")).select("json.*")
				.as(Encoders.bean(EcommerceLog.class));

		Dataset<Row> windowedCounts = ds
				.groupBy(functions.window(ds.col("time"), "2 minute"), ds.col("productName")).count();

		ds.printSchema();

		StreamingQuery query = windowedCounts.writeStream().outputMode("complete").format("console").start();
		query.awaitTermination();

	}

}
