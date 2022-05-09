package util.downloader.util;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

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

}
