package util.downloader.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import util.downloader.mapper.IMapper;

@SuppressWarnings({"unchecked","rawtypes"})
@Component
public class PhoenixDAO {
	
	private Connection getConnection() throws SQLException{
		return DriverManager.getConnection("jdbc:phoenix:localhost");
	}
	
	public void execute(String sql,Object ... a ) throws SQLException{
		try(Connection con = getConnection();
				PreparedStatement ps = con.prepareStatement(sql);)
		{
			setParameters(ps, a);
			ps.setQueryTimeout(180);
			ps.execute();
			con.commit();
		}
	}
	
	public List<Map<String,Object>> executeQuery(String sql, Object ... a) throws SQLException {
		List<Map<String,Object>> result = new ArrayList<>();
		try(Connection con = getConnection();
				PreparedStatement ps = con.prepareStatement(sql);)
		{
			setParameters(ps, a);
			ps.setQueryTimeout(180);
			ResultSet rs = ps.executeQuery();
			ResultSetMetaData meta = rs.getMetaData();
			int colCount = meta.getColumnCount();
			while(rs.next()) {
				Map<String,Object> row = new HashMap<>();
				for(int i=1; i<=colCount;i++) {
					row.put(meta.getColumnName(i), rs.getObject(i));
				}
				result.add(row);
			}
			rs.close();
		}
		return result;
	}
	
	public List executeQuery(String sql,IMapper mapper, Object ... a) throws SQLException {
		List result = new ArrayList<>();
		try(Connection con = getConnection();
				PreparedStatement ps = con.prepareStatement(sql);)
		{
			setParameters(ps, a);
			ps.setQueryTimeout(180);
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				result.add(mapper.mapRow(rs));
			}
			rs.close();
		}
		return result;
	}

	public int executeUpdate(String sql,Object ... a) throws SQLException{
		int count=0;
		try(Connection con = getConnection();
				PreparedStatement ps = con.prepareStatement(sql);)
		{
			setParameters(ps, a);
			ps.setQueryTimeout(180);
			count = ps.executeUpdate();
			con.commit();
		}
		return count;
	}
	
	public int[] executeBatch(String sql, Object ... a) throws SQLException {
		
		int[] rowcounts = new int[1];
		try(Connection con = getConnection();
				PreparedStatement ps = con.prepareStatement(sql);){
			
			if(a.length == 1 && a[0] instanceof List && ((List)a[0]).get(0) instanceof Object[]) {
				List<Object []> paramsList = (List<Object []>) a[0];
				for (Object[] paramsArray : paramsList) {
					setParameters(ps, paramsArray);
					ps.addBatch();
				}
				ps.setQueryTimeout(180);
				
				rowcounts = ps.executeBatch();
				con.commit();
			}
			
		}
		return rowcounts;
	}
	
	private void setParameters(PreparedStatement ps,Object ... a) throws SQLException {
		for(int i=1;i<=a.length;i++) {
			if(a[i-1] == null) {
				ps.setString(i, null);
			} else if(a[i-1] instanceof String) {
				ps.setString(i, a[i-1].toString());
			}else if(a[i-1] instanceof Integer) {
				ps.setInt(i,(Integer) a[i-1] );
			}else if(a[i-1] instanceof Long) {
				ps.setLong(i,(Long) a[i-1] );
			}else if(a[i-1] instanceof Float) {
				ps.setFloat(i,(Float) a[i-1] );
			}else if(a[i-1] instanceof Double) {
				ps.setDouble(i,(Double) a[i-1] );
			}else if(a[i-1] instanceof Date) {
				ps.setDate(i,(Date) a[i-1] );
			}else {
				throw new SQLException("Invalid Parameter Datatype ");
			}
		}
	}
}
