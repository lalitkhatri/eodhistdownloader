package util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.zip.GZIPOutputStream;

import org.joda.time.Instant;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import util.downloader.model.EODData;

public class EodHistDataDownloader implements RequestHandler<ScheduledEvent, String> {

	@Override
	public String handleRequest(ScheduledEvent event, Context context) {
		System.out.println(Instant.now());
		LambdaLogger logger = context.getLogger();
		String exchange = "US";
		String ticker  = "MCD";
		String apiKey = "demo";
		String fromDt = "2001-01-01";
		String toDt = "2050-01-01";
		String path = "/tmp/eodData/"+exchange;
		try {
			String jsonResult = getTimeseries(ticker,exchange,apiKey,fromDt,toDt);
			
			String processedTimeseries = processTimeseries(jsonResult);
			
			File dir = new File(path);
			if (!dir.exists()){
				dir.mkdirs();
			}
			GZIPOutputStream os = new GZIPOutputStream(new FileOutputStream(new File(path, ticker+".json.gz")));
			os.write(processedTimeseries.getBytes());
			os.close();
			logger.log(processedTimeseries.substring(0,500));
			
		} catch (Exception e) {
			logger.log("ERROR while processing request : "+e.getMessage());
			e.printStackTrace();
		}
		System.out.println(Instant.now());
		return null;
	}
 
	private String processTimeseries(String jsonResult) {
		Gson gson = new Gson();
		Type listType = new TypeToken<List<EODData>>() {}.getType();
		List<EODData> eodDataList = gson.fromJson(jsonResult, listType);
		EODData prevEodData = null;
		
		Queue<EODData> eodData20q = new LinkedList<EODData>();
		Float sumSMA20 = 0.0f;
		
		Queue<EODData> eodData50q = new LinkedList<EODData>();
		Float sumSMA50 = 0.0f;
		
		for (EODData eodData : eodDataList) {
			if(prevEodData != null) {
				eodData.setPrevClose(prevEodData.getClose());
			}
			
			if(eodData20q.size()>20) {
				EODData first = eodData20q.remove();
				sumSMA20 = sumSMA20 - first.getClose() + eodData.getClose();
				eodData.setSma20(sumSMA20/20);
			}
			else if (eodData20q.size() == 20) {
				sumSMA20 += eodData.getClose();
				eodData.setSma20(sumSMA20/20);
			}
			else {
				sumSMA20 += eodData.getClose();
			}
			eodData20q.add(eodData);
			
			if(eodData50q.size()>50) {
				EODData first = eodData50q.remove();
				sumSMA50 = sumSMA50 - first.getClose() + eodData.getClose();
				eodData.setSma50(sumSMA50/50);
			}
			else if (eodData50q.size() == 50) {
				sumSMA50 += eodData.getClose();
				eodData.setSma50(sumSMA50/50);
			}
			else {
				sumSMA50 += eodData.getClose();
			}
			eodData50q.add(eodData);
			
			prevEodData = eodData;
		}
		
		return gson.toJson(eodDataList, listType);
	}
	
	private String getTimeseries(String ticker,String exchange, String apiKey, String fromDt, String toDt) throws IOException {
		URL url = new URL("https://eodhd.com/api/eod/"+ticker+"."+exchange+"?period=d&api_token="+apiKey+"&fmt=json&from="+fromDt+"&to="+toDt);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		}
		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		String jsonResult = br.readLine();
		conn.disconnect();
		return jsonResult;
	}
	
//	public static void main(String[] args) {
//		EodHistDataDownloader downloader = new EodHistDataDownloader();
//		downloader.handleRequest(null, null);
//	}
}
