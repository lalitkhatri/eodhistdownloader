package util.downloader.controller;

import static org.apache.spark.sql.functions.lit;
import static util.downloader.util.Constants.API_TOKEN;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import util.downloader.util.UtilityMethods;


@SuppressWarnings("rawtypes")
@RestController
@RequestMapping("/exchange")
public class ExchangeController {
	
	@Value("${dataPath}")
	private String dataPath;
	
	@Autowired
	private SparkSession spark;
	
	public static Dataset<Row> exchange;
	
	
	@PostConstruct
	public void init() throws IOException {
		if(Files.list(Paths.get(dataPath+"/exchange")).count() != 0 ) {
			exchange = spark.read().parquet(dataPath+"/exchange").cache();
			System.out.println(" ######## Exchange count ######################  "+exchange.count());
	//		exchange.createOrReplaceTempView("EXCHANGE");
		}
	}
	
	@GetMapping("/list")
	public List getExchangeList() throws Exception {
		return UtilityMethods.convertToMap(exchange);
	}
	
	@GetMapping("/track")
	public List getTrackedExchange() throws Exception {
		Dataset<Row> exchangeList = exchange.filter("track = 'Y'");
		return UtilityMethods.convertToMap(exchangeList);
	}
		
	@GetMapping("/load")
	public List loadExchangeList() throws Exception {
		URL url = new URL("https://eodhistoricaldata.com/api/exchanges-list/?api_token="+API_TOKEN+"&fmt=json");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
		}
		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		String output;
		while ((output = br.readLine()) != null) {
			System.out.println(output);
			List<String> jsonData = Arrays.asList(output);
			Dataset<String> stringdataset = spark.createDataset(jsonData, Encoders.STRING());
			exchange = spark.read().json(stringdataset);
			exchange = exchange.coalesce(1).withColumnRenamed("Code", "EXCHANGE").withColumn("track",lit("N")).orderBy("EXCHANGE");
			exchange.write().mode(SaveMode.Overwrite).parquet(dataPath+"/exchange");
		}
		conn.disconnect();
		return UtilityMethods.convertToMap(exchange);
	}
	
	@GetMapping("/track/{exchange}")
	public List setTrackExchange(@PathVariable("exchange") String _exchange) throws Exception {
		Dataset<Row> updated = exchange.filter("exchange = '"+_exchange+"'").withColumn("track",lit("Y"));
		Dataset<Row> other = exchange.filter("exchange <> '"+_exchange+"'");
		exchange = updated.union(other).coalesce(1).orderBy("EXCHANGE");
		exchange.write().mode(SaveMode.Overwrite).parquet(dataPath+"/exchange");
		return UtilityMethods.convertToMap(exchange);
	}
	
	@GetMapping("/untrack/{exchange}")
	public List setUntrackExchange(@PathVariable("exchange") String _exchange) throws Exception {
		Dataset<Row> updated = exchange.filter("exchange = '"+_exchange+"'").withColumn("track",lit("N"));
		Dataset<Row> other = exchange.filter("exchange <> '"+_exchange+"'");
		exchange = updated.union(other).coalesce(1).orderBy("EXCHANGE");
		exchange.write().mode(SaveMode.Overwrite).parquet(dataPath+"/exchange");
		return UtilityMethods.convertToMap(exchange);
	}
	
	@GetMapping("/delete/{exchange}")
	public List setDeleteExchange(@PathVariable("exchange") String _exchange) throws Exception {
		exchange = exchange.filter("exchange <> '"+_exchange+"'").coalesce(1).orderBy("EXCHANGE");
		exchange.write().mode(SaveMode.Overwrite).parquet(dataPath+"/exchange");
		return UtilityMethods.convertToMap(exchange);
	}
	
	@PreDestroy
    public void destroy() {
		spark.clearActiveSession();
        spark.close();
    }
	
}
