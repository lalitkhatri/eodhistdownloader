package util.downloader.controller;

import static util.downloader.util.Constants.API_TOKEN;
import static util.downloader.util.SQL.deleteTickerInfoSQL;
import static util.downloader.util.SQL.loadTickerSQL;
import static util.downloader.util.SQL.tickerCountSQL;
import static util.downloader.util.SQL.tickerInfoSQL;
import static util.downloader.util.SQL.tickerListSQL;
import static util.downloader.util.SQL.trackExchangeListSQL;
import static util.downloader.util.SQL.trackUntrackTickerSQL;
import static util.downloader.util.SQL.trackedTickerListSQL;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

import util.downloader.dao.PhoenixDAO;
import util.downloader.model.Ticker;


@SuppressWarnings("rawtypes")
@RestController
@RequestMapping("/ticker")
public class TickerController {
	
	@Autowired
	private PhoenixDAO dao;
	
	
	@GetMapping("/count")
	public List getTickerCount() throws Exception {
		return dao.executeQuery(tickerCountSQL);
	}
	
	@GetMapping("/list/{exchange}")
	public List getTickerList(@PathVariable("exchange") String exchange) throws Exception {
		return dao.executeQuery(tickerListSQL,exchange);
	}
	
	@GetMapping("/bulk")
	public String bulkLoadTicker() throws Exception {
		List<Map<String, Object>> exchangeList = dao.executeQuery(trackExchangeListSQL);
		for (Map<String, Object> map : exchangeList) {
			loadTickerList(map.get("EXCHANGE").toString());
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
			Gson gson = new Gson();
			Ticker recordArray[] = gson.fromJson(output, Ticker[].class);
			for (int i = 0; i < recordArray.length; i++) {
				Ticker record = recordArray[i];
				Object[] row = new Object[7];
				row[0] = record.getCode();
				row[1] = record.getName();
				row[2] = record.getExchange();
				row[3] = record.getCountry();
				row[4] = record.getCurrency();
				row[5] = record.getType();
				row[6] = "N";
				
				data.add(row);
			}
			dao.executeBatch(loadTickerSQL, data);
		}
		conn.disconnect();
		return dao.executeQuery(tickerListSQL,exchange);
	}
	
	@GetMapping("/track/{exchange}/{symbol}")
	public List setTrackTicker(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol) throws Exception {
		dao.execute(trackUntrackTickerSQL, exchange,symbol,"Y");
		return dao.executeQuery(tickerInfoSQL,exchange,symbol);
	}
	
	@GetMapping("/track")
	public List getTrackedExchange() throws Exception {
		return dao.executeQuery(trackedTickerListSQL);
	}
	
	@GetMapping("/untrack/{exchange}/{symbol}")
	public List setUntrackExchange(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol) throws Exception {
		dao.execute(trackUntrackTickerSQL, exchange,"N");
		return dao.executeQuery(tickerInfoSQL,exchange,symbol);
	}
	
	@GetMapping("/delete/{exchange}/{symbol}")
	public String setDeleteExchange(@PathVariable("exchange") String exchange,@PathVariable("symbol") String symbol) throws Exception {
		dao.execute(deleteTickerInfoSQL, exchange,symbol);
		return "Deleted Exchange - "+ symbol + "."+exchange;
	}
		
}
