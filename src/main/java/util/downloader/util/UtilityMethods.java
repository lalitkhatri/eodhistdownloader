package util.downloader.util;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class UtilityMethods {
	
	public static String getNextBusinessDate(String from, String to) {
		LocalDate fromDt = LocalDate.parse(from).plusDays(1l);
		while(DayOfWeek.SATURDAY.equals(fromDt.getDayOfWeek())
				|| DayOfWeek.SUNDAY.equals(fromDt.getDayOfWeek())) {
			fromDt = fromDt.plusDays(1l);
		}
		LocalDate today = LocalDate.now();
		LocalDate toDt = LocalDate.parse(to);
		if( today.isBefore(fromDt) || toDt.isBefore(fromDt))
			return null;
		else
			return fromDt.format(DateTimeFormatter.ISO_LOCAL_DATE);
	}
	
	public static List<Map<String,Object>> convertToMap(Dataset<Row> data){
		List<Map<String,Object>> finalData = new LinkedList<>();
		String[] cols = data.columns();
		data.collectAsList().forEach(row -> {
			Map<String,Object> record = new LinkedHashMap<String, Object>();
			for (String col : cols) {
				record.put(col,row.get(row.fieldIndex(col)));				
			}
			finalData.add(record);
		});
		
		return finalData;
	}

}
