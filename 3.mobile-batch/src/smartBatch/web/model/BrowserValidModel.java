package smartBatch.web.model;

public class BrowserValidModel {
	String rowid;
	boolean valid;
	
	public BrowserValidModel(String rowid, boolean valid) {
		this.rowid = rowid;
		this.valid = valid;
	}
	public String getRowid() {
		return rowid;
	}
	public void setRowid(String rowid) {
		this.rowid = rowid;
	}
	public boolean getValid() {
		return valid;
	}
	public void setValid(boolean valid) {
		this.valid = valid;
	}
}
