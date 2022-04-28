package util.downloader.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.stereotype.Component;

import util.downloader.model.Ticker;

@Component
public class TickerMapper extends AbstractMapper<Ticker>{

	public Ticker mapRow(ResultSet rs) throws SQLException {
		setMetadata(rs);
		Ticker record = new Ticker();
		record.setExchange(safeGetString(rs,"EXCHANGE"));
	    record.setCode(safeGetString(rs,"SYMBOL"));
	    record.setCountry(safeGetString(rs,"COUNTRY"));
	    record.setCurrency(safeGetString(rs,"CURRENCY"));
	    record.setName(safeGetString(rs,"NAME"));
	    record.setType(safeGetString(rs,"TYPE"));
	    record.setTrack(safeGetString(rs,"TRACK"));
	    
	    return record;
	}

	
}