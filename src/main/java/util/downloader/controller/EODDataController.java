package util.downloader.controller;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

import util.downloader.dao.PhoenixDAO;
import util.downloader.mapper.TickerMapper;
import util.downloader.model.EODData;
import util.downloader.model.Ticker;

import static util.downloader.util.Constants.API_TOKEN;

@SuppressWarnings("unchecked")
@RestController
@RequestMapping("/eoddata")
public class EODDataController {
	
	@Autowired
	private PhoenixDAO dao;
	
	@Autowired
	private TickerMapper tickerMapper;
	
	
	private static final String loadEODData = "upsert into GLOBALDATA.EQDATA (EXCHANGE, SYMBOL,TRADEDATE,FREQ,OPENPX,CLOSEPX,HIGH,LOW,PREVCLOSE,TOTTRDQTY) "
			+ "VALUES (?,?,?,?,?,?,?,?,?,?)  ";
	private static final String tickerListSQL = "select * from GLOBALDATA.TICKER where EXCHANGE=? and TYPE in ('Common Stock','INDEX', 'Currency' )";
	
	@GetMapping("/load/{exchange}")
	public String loadData(@PathVariable("exchange") String exchange) throws Exception  {
		int count = 0;
		List<Ticker> ticker = dao.executeQuery(tickerListSQL, tickerMapper, exchange.toUpperCase());
		for (Ticker a : ticker) {
			count += loadData(exchange,a.getCode(),"d",a.getCountry());
//			System.out.println(exchange.toUpperCase()+"-"+a.getCode()+"-"+a.getCountry());
			Thread.sleep(100); //To throttle request to api - max allowed 1000 per min
		}
		return "Total Data Loaded for "+exchange+" - " + count;
		
	}
	
	@GetMapping("/load/{exchange}/{symbol}")
	public String loadData(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol) throws Exception  {
		int count = loadData(exchange,symbol,"d",null);
		return "Total Data Loaded for "+symbol+"."+exchange+" - " + count;
	}
	
	public int loadData(String exchange, String symbol, String freq, String country) throws Exception  {
		List<Object[]> data = new ArrayList<>();
		String exch = exchange;
		if(country!=null && country.equals("USA")) {
			exch="US";
		}
		URL url = new URL("https://eodhistoricaldata.com/api/eod/"+symbol+"."+exch+"?api_token="+API_TOKEN+"&period="+freq+"&fmt=json&from=2012-01-01&to=2022-04-28");
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
				Object[] row = new Object[10];
				row[0] = exchange;
				row[1] = symbol;
				row[2] = record.getDate();
				row[3] = freq.toUpperCase();
				row[4] = record.getOpen();
				row[5] = record.getAdjusted_close();
				row[6] = record.getHigh();
				row[7] = record.getLow();
				row[8] = prevclose;
				row[9] = record.getVolume();
				prevclose = record.getAdjusted_close();
				data.add(row);
			}
//			System.out.println("Loading data for "+symbol+"."+exchange+" - "+data.size());
			dao.executeBatch(loadEODData, data);
		}
		conn.disconnect();
		return data.size();
	}
	
	
	
	
}
