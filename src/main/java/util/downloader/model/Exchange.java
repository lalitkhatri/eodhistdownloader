package util.downloader.model;

public class Exchange {

	private String Name;
	private String Code;
	private String OperatingMIC;
	private String Country;
	private String Currency;
	private String track;
	
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public String getCode() {
		return Code;
	}
	public void setCode(String code) {
		Code = code;
	}
	public String getOperatingMIC() {
		return OperatingMIC;
	}
	public void setOperatingMIC(String operatingMIC) {
		OperatingMIC = operatingMIC;
	}
	public String getCountry() {
		return Country;
	}
	public void setCountry(String country) {
		Country = country;
	}
	public String getCurrency() {
		return Currency;
	}
	public void setCurrency(String currency) {
		Currency = currency;
	}
	public String getTrack() {
		return track;
	}
	public void setTrack(String track) {
		this.track = track;
	}
		
}
