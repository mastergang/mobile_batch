package smartBatch.app.model;

public class ByteGapModel {
	String rowid;
	String version;
	long rxbytegap;
	long txbytegap;
	
	public ByteGapModel(String rowid, long rxbytegap, long txbytegap, String version) {
		this.rowid = rowid;
		this.rxbytegap= rxbytegap;
		this.txbytegap = txbytegap;
		this.version = version;
		
	}
	public String getRowid() {
		return rowid;
	}
	public void setRowid(String rowid) {
		this.rowid = rowid;
	}
	
	public long getRxbytegap() {
		return rxbytegap;
	}
	public void setRxbytegap(long rxbytegap) {
		this.rxbytegap = rxbytegap;
	}
	
	public long getTxbytegap() {
		return txbytegap;
	}
	public void setTxbytegap(long txbytegap) {
		this.txbytegap = txbytegap;
	}
	
	
	public String getVersion() {
		return version;
	}
	public void setVersion(String version) {
		this.version = version;
	}
}
