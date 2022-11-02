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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;

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
@RequestMapping("/ticker")
public class TickerController {
	
	@Value("${dataPath}")
	private String dataPath;
	
	@Autowired
	private SparkSession spark;
	
	private Dataset<Row> ticker;
	
	@PostConstruct
	@GetMapping("/refresh")
	public void init() throws IOException {
		if(Files.list(Paths.get(dataPath+"/ticker")).count() != 0 ) {
			ticker = spark.read().parquet(dataPath+"/ticker").cache();
			System.out.println(" ############# Ticker Count #############  "+ticker.count());
	//		ticker.createOrReplaceTempView("TICKER");
		}

	}
	
	
	@GetMapping("/count")
	public List getTickerCount() throws Exception {
		return UtilityMethods.convertToMap(ticker.groupBy("EXCHANGE").count().orderBy("EXCHANGE"));
	}
	
	@GetMapping("/list/{exchange}")
	public List getTickerList(@PathVariable("exchange") String exchange) throws Exception {
		Dataset<Row> tickerList = ticker.filter("exchange = '"+exchange+"'");
		tickerList.count();
		return UtilityMethods.convertToMap(tickerList);
	}
	
	@GetMapping("/bulk")
	public String bulkLoadTicker() throws Exception {
		List<Row> exchangeList = ExchangeController.exchange.filter("track = 'Y'").collectAsList();
		for (Row row : exchangeList) {
			int i = row.fieldIndex("EXCHANGE");
			System.out.println("############## Downloading ticker for - "+row.getString(i));
			loadTickerList(row.getString(i));
		}					
		return "Loaded ticker for all tracked exchanges";
	}
	
	@GetMapping("/load/{exchange}")
	public List loadTickerList(@PathVariable("exchange") String exchange) throws Exception {
		List<Object[]> data = new ArrayList<>();
		URL url = new URL("https://eodhistoricaldata.com/api/exchange-symbol-list/"+exchange+"/?api_token="+API_TOKEN+"&fmt=json");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
		}
		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		String output;
		while ((output = br.readLine()) != null) {
			List<String> jsonData = Arrays.asList(output);
			Dataset<String> stringdataset = spark.createDataset(jsonData, Encoders.STRING());
			Dataset<Row> tickerList = spark.read().json(stringdataset);
			ticker = tickerList.coalesce(1).withColumnRenamed("Code", "SYMBOL").withColumn("track",lit("N")).orderBy("EXCHANGE","SYMBOL");
			ticker.write().partitionBy("EXCHANGE").mode(SaveMode.Overwrite).parquet(dataPath+"/ticker");
		}
		init();
		System.out.println("################# Ticker List size after "+exchange+" ############## "+ ticker.count());
		conn.disconnect();
		return null;
	}
	
	@GetMapping("/track")
	public List getTrackedExchange() throws Exception {
		Dataset<Row> tickerList = ticker.filter("track = 'Y'");
		tickerList.count();
		return UtilityMethods.convertToMap(tickerList);
	}
	
	@GetMapping("/track/{exchange}/{symbol}")
	public List setTrackTicker(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol) throws Exception {
		Dataset<Row> updated = ticker.filter("exchange = '"+exchange+"'").filter("symbol = '"+symbol+"'").withColumn("track",lit("Y"));
		Dataset<Row> other = ticker.filter("exchange = '"+exchange+"'").filter("symbol <> '"+symbol+"'");
		Dataset<Row> otherExchange = ticker.filter("exchange <> '"+exchange+"'");
		ticker = updated.union(other).union(otherExchange).coalesce(1).orderBy("EXCHANGE","SYMBOL");
		ticker.write().partitionBy("EXCHANGE").mode(SaveMode.Overwrite).parquet(dataPath+"/ticker");
		return UtilityMethods.convertToMap(ticker);
	}
	
	
	@GetMapping("/untrack/{exchange}/{symbol}")
	public List setUntrackExchange(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol) throws Exception {
		Dataset<Row> updated = ticker.filter("exchange = '"+exchange+"'").filter("symbol = '"+symbol+"'").withColumn("track",lit("Y"));
		Dataset<Row> other = ticker.filter("exchange = '"+exchange+"'").filter("symbol <> '"+symbol+"'");
		Dataset<Row> otherExchange = ticker.filter("exchange <> '"+exchange+"'");
		ticker = updated.union(other).union(otherExchange).coalesce(1).orderBy("EXCHANGE","SYMBOL");
		ticker.write().partitionBy("EXCHANGE").mode(SaveMode.Overwrite).parquet(dataPath+"/ticker");
		return UtilityMethods.convertToMap(ticker);
	}
	
	@GetMapping("/delete/{exchange}/{symbol}")
	public List setDeleteExchange(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol) throws Exception {
		Dataset<Row> other = ticker.filter("exchange = '"+exchange+"'").filter("symbol <> '"+symbol+"'");
		Dataset<Row> otherExchange = ticker.filter("exchange <> '"+exchange+"'");
		ticker = other.union(otherExchange).coalesce(1).orderBy("EXCHANGE","SYMBOL");
		ticker.write().partitionBy("EXCHANGE").mode(SaveMode.Overwrite).parquet(dataPath+"/ticker");
		return UtilityMethods.convertToMap(ticker);
	}
		
}
