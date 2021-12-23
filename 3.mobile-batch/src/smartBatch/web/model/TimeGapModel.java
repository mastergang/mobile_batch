package smartBatch.web.model;

public class TimeGapModel {
	String rowid;
	int timegap;
	
	public TimeGapModel(String rowid, int timegap) {
		this.rowid = rowid;
		this.timegap = timegap;
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
}
