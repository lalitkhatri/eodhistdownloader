package util.downloader.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataSourceConfiguration {

	
	@Bean(name = "sparkSession")
	public SparkSession getSparkSession() {
		SparkSession spark = SparkSession.builder().appName("EODHistDataLoader")
				.config("spark.master", "local[*]")
				.config("spark.sql.sources.partitionOverwriteMode","dynamic")
				.config("spark.memory.storageFraction","0.3")
				.config("spark.memory.fraction","0.8")
				.getOrCreate();
		return spark;
	}

	
}