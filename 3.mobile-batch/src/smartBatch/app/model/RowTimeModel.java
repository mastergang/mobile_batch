package smartBatch.app.model;

public class RowTimeModel {
	String panelid;
	String rowid;
	String package_name;
	String screen;
	String version;
	int reqtime;
	
	public String getPackage_name() {
		return package_name;
	}
	public void setPackage_name(String package_name) {
		this.package_name = package_name;
	}
	public String getScreen() {
		return screen;
	}
	public String getVersion() {
		return version;
	}
	
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
	public void setPackage(String package_name) {
		this.package_name = package_name;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public int getReqtime() {
		return reqtime;
	}
	public void setScreen(String screen) {
		this.screen = screen;
	}
	public void setReqtime(int reqtime) {
		this.reqtime = reqtime;
	}
}
