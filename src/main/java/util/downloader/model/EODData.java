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
	private Float adjusted_close;
	private Float volume;
	
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
}
