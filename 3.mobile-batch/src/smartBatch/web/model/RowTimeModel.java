package smartBatch.web.model;

public class RowTimeModel {
	String panelid;
	String rowid;
	int reqtime;
	
	public void setPanelid(String panelid) {
		this.panelid = panelid;
	}
	public String getPanelid() {
		return panelid;
	}
	public String getRowid() {
		return rowid;
	}
	public void setRowid(String rowid) {
		this.rowid = rowid;
	}
	public int getReqtime() {
		return reqtime;
	}
	public void setReqtime(int reqtime) {
		this.reqtime = reqtime;
	}
}
