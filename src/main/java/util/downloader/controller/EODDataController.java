package util.downloader.controller;

import static util.downloader.util.Constants.API_TOKEN;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

import util.downloader.dao.PhoenixDAO;
import util.downloader.mapper.TickerMapper;
import util.downloader.model.EODData;
import util.downloader.model.SplitData;
import util.downloader.model.Ticker;

@SuppressWarnings("unchecked")
@RestController
@RequestMapping("/eoddata")
public class EODDataController {
	
	@Autowired
	private PhoenixDAO dao;
	
	@Autowired
	private TickerMapper tickerMapper;
	
	private final ExecutorService executor = Executors.newFixedThreadPool(10);
	
	private static final String countEODData = "select exchange, count(distinct symbol) as sym_cnt, count(1) as data_cnt from GLOBALDATA.EQDATA group by exchange";
		
	private static final String loadEODData = "upsert into GLOBALDATA.EQDATA (EXCHANGE, SYMBOL,TRADEDATE,FREQ,OPENPX,CLOSEPX,HIGH,LOW,PREVCLOSE,TOTTRDQTY,ADJCLOSEPX) "
			+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)  ";
	private static final String tickerListSQL = "select * from GLOBALDATA.TICKER where EXCHANGE=? and TYPE in ('Common Stock','INDEX', 'Currency' )";
	
	private static final String loadSplitsData = "upsert into GLOBALDATA.SPLITS (EXCHANGE, SYMBOL,TRADEDATE,RATIO) VALUES (?,?,?,?) ";
			
	
	@GetMapping("/load/{exchange}")
	public String loadData(@PathVariable("exchange") String exchange) throws Exception  {
		List<Ticker> ticker = dao.executeQuery(tickerListSQL, tickerMapper, exchange.toUpperCase());
		for (Ticker a : ticker) {
			executor.execute(new EODDataLoader(exchange.toUpperCase(), a.getCode(), "d", a.getCountry()));
		}
		return "Started Data Load for "+exchange;
		
	}
	
	@GetMapping("/load/{exchange}/{symbol}")
	public String loadData(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol) throws Exception  {
		executor.execute(new EODDataLoader(exchange.toUpperCase(), symbol.toUpperCase(), "d", null));
		return "Started Data Load for "+symbol+"."+exchange;
	}
	
	@GetMapping("/count")
	public List getTickerCount() throws Exception {
		return dao.executeQuery(countEODData);
	}
	
	
private class EODDataLoader implements Runnable{
	private final String exchange;
	private final String symbol;
	private final String freq;
	private final String country;
	private String exch ;
	
	public EODDataLoader(String exchange, String symbol, String freq, String country) {
		this.exchange= exchange;
		this.symbol = symbol;
		this.freq = freq;
		this.country = country;
	}
	
	private int loadData() throws Exception  {
		List<Object[]> data = new ArrayList<>();
		
		URL url = new URL("https://eodhistoricaldata.com/api/eod/"+symbol+"."+exch+"?api_token="+API_TOKEN+"&period="+freq+"&fmt=json&from=2012-01-01&to=2022-05-03");
//		System.out.println(url.toString());
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

		String output;
		while ((output = br.readLine()) != null) {
			Gson gson = new Gson();
			EODData recordArray[] = gson.fromJson(output, EODData[].class);
			Float prevclose = null;
			for (int i = 0; i < recordArray.length; i++) {
				EODData record = recordArray[i];
				Object[] row = new Object[11];
				row[0] = exchange.toUpperCase();
				row[1] = symbol.toUpperCase();
				row[2] = record.getDate();
				row[3] = freq.toUpperCase();
				row[4] = record.getOpen();
				row[5] = record.getClose();
				row[6] = record.getHigh();
				row[7] = record.getLow();
				row[8] = prevclose;
				row[9] = record.getVolume();
				row[10] = record.getAdjusted_close();
				prevclose = record.getClose();
				data.add(row);
			}
//			System.out.println("Loading data for "+symbol+"."+exchange+" - "+data.size()+" - "+ Instant.now());
			if(data.size()>0) {
				dao.executeBatch(loadEODData, data);
			}
		}
		conn.disconnect();
		return data.size();
	}
	
	@Override
	public void run() {
		try{
			if(country!=null && country.equals("USA")) {
				exch="US";
			}
			else {
				exch = exchange;
			}
			getSplitData();
			loadData();
		}
		catch (Exception e) {
			System.out.println("Exception while processing - "+symbol+"."+exchange + " - "+ e.getMessage());
		}
	}
	
	private void getSplitData() throws Exception {
		List<Object[]> data = new ArrayList<>();
		URL url = new URL("https://eodhistoricaldata.com/api/splits/"+symbol+"."+exch+"?api_token="+API_TOKEN+"&fmt=json");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ conn.getResponseCode());
		}

		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

		String output = br.readLine();
		Gson gson = new Gson();
		SplitData recordArray[] =  gson.fromJson(output, SplitData[].class);
		for (int i = 0; i < recordArray.length; i++) {
			SplitData record = recordArray[i];
			Object[] row = new Object[4];
			row[0] = exchange.toUpperCase();
			row[1] = symbol.toUpperCase();
			row[2] = record.getDate();
			row[3] = record.getSplit();
			data.add(row);
		}
		if(data.size()>0) {
			dao.executeBatch(loadSplitsData, data);
		}
		conn.disconnect();
	}
}
	
}
