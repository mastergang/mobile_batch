package smartBatch.app.model;

public class AppModel {
	String title;
	String package_name;
	String app_name;
	String app_type;
	String provider;
	String site;
	int smartid;
	int installs;
	boolean primary;

	public AppModel() {
	}
	public AppModel(String package_name, String app_name, int smartid, boolean primary) {
		this.package_name = package_name;
		this.app_name = app_name;
		this.smartid = smartid;
		this.primary = primary;
	}
	public AppModel(String package_name, int smartid, String app_type, String provider, String site, String title, int installs) {
		this.package_name = package_name;
		this.smartid = smartid;
		this.app_type = app_type;
		this.site = site;
		this.provider = provider;
		this.title = title;
		this.installs = installs;
	}
	public String getAppname() {
		return app_name;
	}
	public void setAppname(String app_name) {
		this.app_name = app_name;
	}
	public String getPackagename() {
		return package_name;
	}
	public void setPackagename(String package_name) {
		this.package_name = package_name;
	}
	public boolean getPrimary() {
		return primary;
	}
	public void setPrimary(boolean primary) {
		this.primary = primary;
	}
	public int getSmartid() {
		return smartid;
	}
	public void setSmartid(int smartid) {
		this.smartid = smartid;
	}
	public String getApp_type() {
		return app_type;
	}
	public void setApp_type(String app_type) {
		this.app_type = app_type;
	}
	public String getProvider() {
		return provider;
	}
	public void setProvider(String provider) {
		this.provider = provider;
	}
	public String getSite() {
		return site;
	}
	public void setSite(String site) {
		this.site = site;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public int getInstalls() {
		return installs;
	}
	public void setInstalls(int installs) {
		this.installs = installs;
	}
}
