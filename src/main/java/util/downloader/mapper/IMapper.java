package util.downloader.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface IMapper<T> {
	public T mapRow(ResultSet rs) throws SQLException;
}
