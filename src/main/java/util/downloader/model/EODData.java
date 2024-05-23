package util.downloader.model;

public class EODData {
	private String exchange;
	private String code;
	private String date;
	private String freq;
	private Float open;
	private Float high;
	private Float low;
	private Float close;
	private Float prevClose;
	private Float adjusted_close;
	private Float volume;
	
	private Float sma20;
	private Float sma50;
	private Float sma100;
	private Float sma200;
	
	private Float ema20;
	private Float ema50;
	private Float ema100;
	private Float ema200;
	
	private Float stochK;
	private Float stochD;
	
	public String getExchange() {
		return exchange;
	}
	
	public void setExchange(String exchange) {
		this.exchange = exchange;
	}
	
	public String getCode() {
		return code;
	}
	
	public void setCode(String code) {
		this.code = code;
	}
	
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getFreq() {
		return freq;
	}
	
	public void setFreq(String freq) {
		this.freq = freq;
	}
	
	public Float getOpen() {
		return open;
	}

	public void setOpen(Float open) {
		this.open = open;
	}

	public Float getHigh() {
		return high;
	}

	public void setHigh(Float high) {
		this.high = high;
	}

	public Float getLow() {
		return low;
	}

	public void setLow(Float low) {
		this.low = low;
	}

	public Float getClose() {
		return close;
	}

	public void setClose(Float close) {
		this.close = close;
	}

	public Float getPrevClose() {
		return this.prevClose;
	}
	
	public void setPrevClose(Float prevClose) {
		this.prevClose = prevClose;
	}
	
	public Float getAdjusted_close() {
		return adjusted_close;
	}

	public void setAdjusted_close(Float adjusted_close) {
		this.adjusted_close = adjusted_close;
	}

	public Float getVolume() {
		return volume;
	}

	public void setVolume(Float volume) {
		this.volume = volume;
	}

	public Float getSma20() {
		return sma20;
	}

	public void setSma20(Float sma20) {
		this.sma20 = sma20;
	}

	public Float getSma50() {
		return sma50;
	}

	public void setSma50(Float sma50) {
		this.sma50 = sma50;
	}

	public Float getSma100() {
		return sma100;
	}

	public void setSma100(Float sma100) {
		this.sma100 = sma100;
	}

	public Float getSma200() {
		return sma200;
	}

	public void setSma200(Float sma200) {
		this.sma200 = sma200;
	}

	public Float getEma20() {
		return ema20;
	}

	public void setEma20(Float ema20) {
		this.ema20 = ema20;
	}

	public Float getEma50() {
		return ema50;
	}

	public void setEma50(Float ema50) {
		this.ema50 = ema50;
	}

	public Float getEma100() {
		return ema100;
	}

	public void setEma100(Float ema100) {
		this.ema100 = ema100;
	}

	public Float getEma200() {
		return ema200;
	}

	public void setEma200(Float ema200) {
		this.ema200 = ema200;
	}

	public Float getStochK() {
		return stochK;
	}

	public void setStochK(Float stochK) {
		this.stochK = stochK;
	}

	public Float getStochD() {
		return stochD;
	}

	public void setStochD(Float stochD) {
		this.stochD = stochD;
	}	
	
}