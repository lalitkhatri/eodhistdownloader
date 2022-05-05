package util.downloader.mapper;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public abstract class AbstractMapper<T> implements IMapper<T>{

	protected void setMetadata(ResultSet rs) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int colCount = meta.getColumnCount();
		for(int i=1;i<=colCount;i++) {
			String key = meta.getColumnName(i);
			Integer type = meta.getColumnType(i);
			metadata.put(key, type);
		}
	}
	
	protected String safeGetString(ResultSet rs, String colName) throws SQLException {
		String val = null;
		if(metadata.containsKey(colName)) {
			val = rs.getString(colName);
		}
		return val;
	}
	
	protected Float safeGetFloat(ResultSet rs, String colName) throws SQLException {
		Float val = null;
		if(metadata.containsKey(colName)) {
			val = rs.getFloat(colName);
		}
		return val;
	}
	
	protected Long safeGetLong(ResultSet rs, String colName) throws SQLException {
		Long val = null;
		if(metadata.containsKey(colName)) {
			val = rs.getLong(colName);
		}
		return val;
	}
	
	protected Boolean safeGetBoolean(ResultSet rs, String colName) throws SQLException {
		Boolean val = false;
		if(metadata.containsKey(colName)) {
			val = rs.getBoolean(colName);
		}
		return val;
	}
	
	public AbstractMapper() {
		super();
	}
	
	public AbstractMapper(Map<String,T> dataMap) {
		super();
		this.dataMap = dataMap;
	}
	
	public AbstractMapper(Map<String,T> dataMap,String... keys) {
		super();
		this.dataMap = dataMap;
		this.keys = keys;
	}
	
	protected Map<String,T> dataMap = null;
	protected String[] keys = null;
	protected Map<String,Integer> metadata = new HashMap<>();
}
