package smartBatch.app.model;

public class TimeGapModel {
	String rowid;
	String version;
	String screen;
	int timegap;
	int duration;
	
	public TimeGapModel(String rowid, int timegap, int duration, String version, String screen) {
		this.rowid = rowid;
		this.timegap = timegap;
		this.version = version;
		this.duration = duration;
		this.screen = screen;
	}
	public String getRowid() {
		return rowid;
	}
	public void setRowid(String rowid) {
		this.rowid = rowid;
	}
	public int getTimegap() {
		return timegap;
	}
	public void setTimegap(int timegap) {
		this.timegap = timegap;
	}
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public int getDuration() {
		return duration;
	}
	public void setDuration(int duration) {
		this.duration = duration;
	}
	public String getScreen() {
		return screen;
	}
	public void setScreen(String screen) {
		this.screen = screen;
	}
}
