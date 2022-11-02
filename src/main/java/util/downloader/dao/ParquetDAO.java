package util.downloader.dao;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@SuppressWarnings({"rawtypes"})
@Component
public class ParquetDAO {
	
	@Value("${dataPath}")
	private String dataPath;
	
	@Autowired
	private SparkSession spark;
	
	
	private Dataset<Row> eqdata;

	 @PostConstruct
	 public void init() {
		 System.out.println(dataPath);
		 spark = SparkSession.builder().appName("Data Migration")
					.config("spark.master", "local[*]")
					.config("spark.sql.sources.partitionOverwriteMode","dynamic")
					.config("spark.memory.storageFraction","0.3")
					.config("spark.memory.fraction","0.8")
					.getOrCreate();
		 
//		 eqdata = spark.read().parquet(dataPath+"/eqdata").cache();
//		 
//		 eqdata.createOrReplaceTempView("EQDATA");
	 }
	
	public List executeQuery(String sql, Object ... a) {
		sql = setParameters(sql,a);
		Dataset<Row> data = spark.sql(sql);
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

	private String setParameters(String sql,Object ... a) {
		for(int i=1;i<=a.length;i++) {
			System.out.println("SQL Paramerters - "+a[i-1]);
			sql = sql.replaceFirst("\\?","'"+a[i-1].toString()+"'");
		}
		System.out.println(" --------------------- \n"+ sql + " \n -------------------------------------");
		return sql;
	}
}