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
import org.springframework.web.servlet.view.RedirectView;

import com.google.gson.Gson;

import util.downloader.dao.PhoenixDAO;
import util.downloader.model.Exchange;
import static util.downloader.util.Constants.*;


@SuppressWarnings("rawtypes")
@RestController
@RequestMapping("/exchange")
public class ExchangeController {
	
	@Autowired
	private PhoenixDAO dao;
	
	private static final String exchangeListSQL = "select * from GLOBALDATA.EXCHANGE";
	public static final String trackExchangeListSQL = "select * from GLOBALDATA.EXCHANGE where TRACK='Y' ";
	private static final String exchangeInfoSQL = "select * from GLOBALDATA.EXCHANGE where EXCHANGE=?";
	private static final String deleteExchangeInfoSQL = "delete from GLOBALDATA.EXCHANGE where EXCHANGE=?";
	private static final String loadExchangeSQL = "upsert into GLOBALDATA.EXCHANGE (EXCHANGE,NAME,MIC,COUNTRY,CURRENCY,TRACK) values (?,?,?,?,?,?) ";
	private static final String trackUntrackExchangeSQL = "upsert into GLOBALDATA.EXCHANGE (EXCHANGE,TRACK) values (?,?) ";
	
	@GetMapping("/list")
	public List getExchangeList() throws Exception {
		return dao.executeQuery(exchangeListSQL);
	}
	
	@GetMapping("/track")
	public List getTrackedExchange() throws Exception {
		return dao.executeQuery(trackExchangeListSQL);
	}
		
	@GetMapping("/load")
	public RedirectView loadExchangeList() throws Exception {
		List<Object[]> data = new ArrayList<>();
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
			Gson gson = new Gson();
			Exchange recordArray[] = gson.fromJson(output, Exchange[].class);
			for (int i = 0; i < recordArray.length; i++) {
				Exchange record = recordArray[i];
				Object[] row = new Object[6];
				row[0] = record.getCode();
				row[1] = record.getName();
				row[2] = record.getOperatingMIC();
				row[3] = record.getCountry();
				row[4] = record.getCurrency();
				row[5] = "N";
				
				data.add(row);
			}
			dao.executeBatch(loadExchangeSQL, data);
		}
		conn.disconnect();
		return new RedirectView("/exchange/list");
	}
	
	@GetMapping("/track/{exchange}")
	public List setTrackExchange(@PathVariable("exchange") String exchange) throws Exception {
		dao.execute(trackUntrackExchangeSQL, exchange,"Y");
		return dao.executeQuery(exchangeInfoSQL,exchange);
	}
	
	@GetMapping("/untrack/{exchange}")
	public List setUntrackExchange(@PathVariable("exchange") String exchange) throws Exception {
		dao.execute(trackUntrackExchangeSQL, exchange,"N");
		return dao.executeQuery(exchangeInfoSQL,exchange);
	}
	
	@GetMapping("/delete/{exchange}")
	public String setDeleteExchange(@PathVariable("exchange") String exchange) throws Exception {
		dao.execute(deleteExchangeInfoSQL, exchange);
		return "Deleted Exchange - "+ exchange;
	}
	
}
