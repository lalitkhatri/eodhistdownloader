package util.downloader.model;

public class EODData {
	private String date;
	private Float open;
	private Float high;
	private Float low;
	private Float close;
	private Float adjusted_close;
	private Long volume;
	
	public EODData(String date, Float open, Float high, Float low, Float close, Float adjusted_close, Long volume) {
		this.date = date;
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.adjusted_close = adjusted_close;
		this.volume = volume;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
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

	public Long getVolume() {
		return volume;
	}

	public void setVolume(Long volume) {
		this.volume = volume;
	}	
}
