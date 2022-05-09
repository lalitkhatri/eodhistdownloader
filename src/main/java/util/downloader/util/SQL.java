package util.downloader.util;

public class SQL {

	/* Ticker api sql */
	public static final String tickerCountSQL = "select exchange,count(1) from GLOBALDATA.TICKER where TYPE in ('Common Stock','INDEX', 'Currency' ) group by exchange order by exchange";
	public static final String tickerListSQL = "select * from GLOBALDATA.TICKER where EXCHANGE=? and TYPE in ('Common Stock','INDEX', 'Currency' )";
	public static final String tickerInfoSQL = "select * from GLOBALDATA.TICKER where EXCHANGE=? and SYMBOL=?";
	public static final String deleteTickerInfoSQL = "delete from GLOBALDATA.TICKER where EXCHANGE=? and SYMBOL=?";
	public static final String trackedTickerListSQL = "select * from GLOBALDATA.TICKER where TRACK='Y'";
	public static final String loadTickerSQL = "upsert into GLOBALDATA.TICKER (SYMBOL,NAME,EXCHANGE,COUNTRY,CURRENCY,TYPE,TRACK) values (?,?,?,?,?,?,?) ";
	public static final String trackUntrackTickerSQL = "upsert into GLOBALDATA.TICKER (EXCHANGE,SYMBOL,TRACK) values (?,?,?) ";

	
	/* Exchange API sql */
	public static final String exchangeListSQL = "select * from GLOBALDATA.EXCHANGE";
	public static final String trackExchangeListSQL = "select * from GLOBALDATA.EXCHANGE where TRACK='Y' ";
	public static final String exchangeInfoSQL = "select * from GLOBALDATA.EXCHANGE where EXCHANGE=?";
	public static final String deleteExchangeInfoSQL = "delete from GLOBALDATA.EXCHANGE where EXCHANGE=?";
	public static final String loadExchangeSQL = "upsert into GLOBALDATA.EXCHANGE (EXCHANGE,NAME,MIC,COUNTRY,CURRENCY,TRACK) values (?,?,?,?,?,?) ";
	public static final String trackUntrackExchangeSQL = "upsert into GLOBALDATA.EXCHANGE (EXCHANGE,TRACK) values (?,?) ";
	
	
	/* EOD Data API sql */
	public static final String eodDataForPrevDate = "select * from GLOBALDATA.EQDATA where EXCHANGE=? and FREQ = 'D' "
			+ "and TRADEDATE = (select max(TRADEDATE) from GLOBALDATA.EQDATA where EXCHANGE=? and TRADEDATE < ? and FREQ = 'D')";
	
	public static final String countEODData = "select exchange, count(distinct symbol) as sym_cnt, count(1) as data_cnt "
			+ "from GLOBALDATA.EQDATA group by exchange";
		
	public static final String loadEODData = "upsert into GLOBALDATA.EQDATA (EXCHANGE, SYMBOL,TRADEDATE,FREQ,OPENPX,CLOSEPX,HIGH,LOW,PREVCLOSE,TOTTRDQTY,ADJCLOSEPX) "
			+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)  ";
	
	public static final String loadSplitsData = "upsert into GLOBALDATA.SPLITS (EXCHANGE, SYMBOL,TRADEDATE,RATIO) VALUES (?,?,?,?) ";
			
	
}
