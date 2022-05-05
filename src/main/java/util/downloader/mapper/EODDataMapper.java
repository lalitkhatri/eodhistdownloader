package util.downloader.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.springframework.stereotype.Component;

import util.downloader.model.EODData;

@Component
public class EODDataMapper extends AbstractMapper<EODData> {

	public EODData mapRow(ResultSet rs) throws SQLException {
		setMetadata(rs);
		EODData record = new EODData();
		record.setExchange(safeGetString(rs, "EXCHANGE"));
		record.setCode(safeGetString(rs, "SYMBOL"));
		record.setDate(safeGetString(rs, "TRADEDATE"));
		record.setFreq(safeGetString(rs, "FREQ"));
		record.setOpen(safeGetFloat(rs, "OPENPX"));
		record.setClose(safeGetFloat(rs, "CLOSEPX"));
		record.setHigh(safeGetFloat(rs, "HIGH"));
		record.setLow(safeGetFloat(rs, "LOW"));
		if(keys!=null) {
	    	  StringBuffer keyString = new StringBuffer();
	    	  for (int i=0; i < keys.length;i++) {
	    		  if(keyString.length()>0) {
	    			  keyString.append("_");
	    		  }
	    		  keyString.append(safeGetString(rs, keys[i]));
	    	  }
	    	  dataMap.put(keyString.toString(),record);
	      } else if (this.dataMap != null) {
	    	  dataMap.put(record.getCode(), record);
	      }
		return record;
	}
	
	public EODDataMapper() {
		super();
	}
	
	public EODDataMapper(Map<String,EODData> dataMap) {
		super(dataMap);
	}
	
	public EODDataMapper(Map<String,EODData> dataMap,String... keys) {
		super(dataMap,keys);
	}

}