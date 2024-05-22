package util.downloader.controller;

import static util.downloader.util.Constants.API_TOKEN;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import util.downloader.util.UtilityMethods;

@SuppressWarnings({"rawtypes"})
@RestController
@RequestMapping("/eoddata")
public class EODDataController {
	
	@Value("${dataPath}")
	private String dataPath;
	
	@Value("${exportPath}")
	private String exportPath;
	
	@Autowired
	private SparkSession spark;
	
	private Dataset<Row> eqdata;
	
	@PostConstruct
	@GetMapping("/refresh")
	public void init() throws IOException {
		if(Files.list(Paths.get(dataPath+"/eqdata")).count() != 0 ) {
			eqdata = spark.read().parquet(dataPath+"/eqdata").cache();
//			System.out.println(" ############# Ticker Count #############  "+eqdata.count());
		}

	}
	
	@GetMapping("/load/{exchange}")
	public String loadData(@PathVariable("exchange") String exchange, 
			@RequestParam(required = false, defaultValue = "2001-01-01") String from, 
			@RequestParam(required = false, defaultValue = "2032-05-01") String to,
			@RequestParam(required = false, defaultValue = "d") String freq) throws Exception  {
		List<Row> tickerList = TickerController.ticker.filter("exchange = '"+exchange+"'").collectAsList();
		for (Row row : tickerList) {
			int i = row.fieldIndex("EXCHANGE");
			int j = row.fieldIndex("SYMBOL");
			System.out.println("############## Downloading ticker for - "+row.getString(i));
			loadData(row.getString(i),row.getString(j),"2001-01-01","2032-01-01","d");
		}	
		return "Data Loaded for "+exchange  ;
	}
	
	@GetMapping("/load/{exchange}/{symbol}")
	public String loadData(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol,
			@RequestParam(required = false, defaultValue = "2001-01-01") String from, 
			@RequestParam(required = false, defaultValue = "2032-01-01") String to ,
			@RequestParam(required = false, defaultValue = "d") String freq) throws Exception  {
		
		return "Started Data Load for "+symbol+"."+exchange;
	}
	
	@GetMapping("/count")
	public List getDataCount() throws Exception {
		return UtilityMethods.convertToMap(eqdata.groupBy("EXCHANGE").count().orderBy("EXCHANGE"));
	}
	
	@GetMapping("/bulk")
	public String bulkLoadData(@RequestParam(required = false, defaultValue = "2001-01-01") String date) throws Exception  {
		
		List<Row> exchangeList = ExchangeController.exchange.select(new Column("EXCHANGE")).distinct().collectAsList();
		for (Row row : exchangeList) {
			int i = row.fieldIndex("EXCHANGE");
			System.out.println("############## Downloading eod bulk file for - "+row.getString(i));
			bulkLoadPerExchange(row.getString(i),date);
		}	
		return "Data Loaded for date "+date  ;
		
	}
	
	private void bulkLoadPerExchange(String exchange, String date) throws Exception {
		List<Object[]> newData = new ArrayList<>();
		
		URL url = new URL("https://eodhistoricaldata.com/api/eod-bulk-last-day/"+exchange.toUpperCase()+"?api_token="+API_TOKEN+"&date="+date+"&fmt=json");
//		System.out.println(url.toString());
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

		String output = br.readLine();
		
		conn.disconnect();
		System.out.println("Data Loaded for "+exchange + " - " + date +" - " + newData.size());
	}
	
	
	@GetMapping("/export/{exchange}")
	public String exportData(@PathVariable("exchange") String exchange, 
			@RequestParam(required = false, defaultValue = "2001-01-01") String from, 
			@RequestParam(required = false, defaultValue = "2032-05-01") String to,
			@RequestParam(required = false, defaultValue = "d") String freq) throws Exception  {
		List<Row> tickerList = TickerController.ticker.filter("exchange = '"+exchange+"'").collectAsList();
		for (Row row : tickerList) {
			int i = row.fieldIndex("EXCHANGE");
			int j = row.fieldIndex("SYMBOL");
			System.out.println("############## Downloading ticker for - "+row.getString(i));
			exportData(row.getString(i),row.getString(j),"2001-01-01","2032-01-01",freq);
		}	
		return "Data Exported for "+exchange  ;
	}
	
	@GetMapping("/export/{exchange}/{symbol}")
	public String exportData(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol,
			@RequestParam(required = false, defaultValue = "2001-01-01") String from, 
			@RequestParam(required = false, defaultValue = "2032-01-01") String to ,
			@RequestParam(required = false, defaultValue = "d") String freq) throws Exception  {
		eqdata.filter("exchange = '"+exchange+"'").filter("symbol = '"+symbol+"'").write().csv(exportPath+"/"+exchange+"/"+symbol);
		return "Data Exported for "+symbol+"."+exchange;
	}
	
//	private class EODDataLoader implements Runnable{
//		private final String exchange;
//		private final String symbol;
//		private final String freq;
//		private final String country;
//		private String exch ;
//		private String from;
//		private String to;
//		
//		public EODDataLoader(String exchange, String symbol, String freq, String country, String from, String to) {
//			this.exchange= exchange;
//			this.symbol = symbol;
//			this.freq = freq;
//			this.country = country;
//			this.from = from;
//			this.to = to;
//		}
//		
//		private int loadData() throws Exception  {
//			List<Object[]> data = new ArrayList<>();
//			
//			URL url = new URL("https://eodhistoricaldata.com/api/eod/"+symbol+"."+exch+"?api_token="+API_TOKEN+"&period="+freq+"&fmt=json&from="+from+"&to="+to);
//	//		System.out.println(url.toString());
//			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//			conn.setRequestMethod("GET");
//			conn.setRequestProperty("Accept", "application/json");
//			if (conn.getResponseCode() != 200) {
//				throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
//			}
//	
//			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
//	
//			String output;
//			while ((output = br.readLine()) != null) {
//				
//			}
//			conn.disconnect();
//			return data.size();
//		}
//		
//		@Override
//		public void run() {
//			try{
//				if(country!=null && country.equals("USA")) {
//					exch="US";
//				}
//				else {
//					exch = exchange;
//				}
//				getSplitData();
//				loadData();
//			}
//			catch (Exception e) {
//				System.out.println("Exception while processing - "+symbol+"."+exchange + " - "+ e.getMessage());
//			}
//		}
//		
//		private void getSplitData() throws Exception {
//			List<Object[]> data = new ArrayList<>();
//			URL url = new URL("https://eodhistoricaldata.com/api/splits/"+symbol+"."+exch+"?api_token="+API_TOKEN+"&fmt=json&from="+from);
//	//		System.out.println(url.toString());
//			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//			conn.setRequestMethod("GET");
//			conn.setRequestProperty("Accept", "application/json");
//			if (conn.getResponseCode() != 200) {
//				throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
//			}
//	
//			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
//	
//			String output = br.readLine();
//			
//			conn.disconnect();
//		}
//	}
//	
}
