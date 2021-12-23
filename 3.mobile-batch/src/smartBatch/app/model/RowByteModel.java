package smartBatch.app.model;

public class RowByteModel {
	String rowid;
	String panel_id;
	String item_value;
	String wifistatus;
	String track_version;
	long rxbyte;
	long txbyte;
	
	public String getRowid() {
		return rowid;
	}
	public void setRowid(String rowid) {
		this.rowid = rowid;
	}
	
	public String getPanel_id() {
		return panel_id;
	}
	public void setPanel_id(String panel_id) {
		this.panel_id = panel_id;
	}
	
	public String getItem_value() {
		return item_value;
	}
	public void setItem_value(String item_value) {
		this.item_value = item_value;
	}
	
	public String getWifistatus() {
		return wifistatus;
	}
	public void setWifistatus(String wifistatus) {
		this.wifistatus = wifistatus;
	}
	
	public String getTrack_version() {
		return track_version;
	}
	public void setTrack_version(String track_version) {
		this.track_version = track_version;
	}
	
	public long getRxbyte() {
		return rxbyte;
	}
	public void setRxbyte(long rxbyte) {
		this.rxbyte = rxbyte;
	}
	
	public long getTxbyte() {
		return txbyte;
	}
	public void setTxbyte(long txbyte) {
		this.txbyte = txbyte;
	}
}
