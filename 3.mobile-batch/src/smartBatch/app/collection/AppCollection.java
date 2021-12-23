package smartBatch.app.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import smartBatch.app.model.AppModel;
import smartBatch.app.set.AppSet;
import smartBatch.web.model.QueryModel;
import smartBatch.web.model.TimeGapModel;
import smartBatch.web.set.QuerySet;

import log.WriteMsgLog;
import DB.DBConnection;

public class AppCollection {
	DBConnection dbcon;
	String filtername;
	WriteMsgLog log = null;
	
	AppSet Apps = new AppSet();
	static String dirlog = "D:\\workspace/smartBatch/log/";
	
	
	public AppCollection(DBConnection dbcon, String filtername){
		this.dbcon = dbcon;
		this.filtername = filtername;
	}
	
	/**
	 * 
	 * @return: 1은 정상처리, 99는 panel의 미처리, 98은 req_date의 미처리
	 */
	public Collection access() {
		Apps.setDbcon(dbcon);
		int maxid = Apps.maxIDSet();
		Collection AddingApps = new ArrayList<AppModel>();
		
		if(maxid == 0){
			System.out.println("Error occurred.");
			System.exit(0);
		}
		
		Collection NewApp = Apps.newAppSet(filtername);
		if(NewApp != null) {
			Iterator it = NewApp.iterator();
			String prevPackage = "";
			int smartid = maxid;
			try {
				while(it.hasNext()) {
					AppModel App=(AppModel)it.next();

					String packagename = App.getPackagename();
					String Appname = App.getAppname();
					packagename=packagename.replaceAll("\'","\'\'");
					if(Appname != null){
						Appname=Appname.replaceAll("\'","\'\'");
					}
					//package_name 중복검사.
					
					if(!prevPackage.equals(packagename)){
						AppModel tg = new AppModel(packagename, Appname, ++smartid, true);
						AddingApps.add(tg);
					} else {
						AppModel tg = new AppModel(packagename, Appname, smartid, false);
						AddingApps.add(tg);
					}
					prevPackage = packagename;
				}
				//System.exit(0);
			} catch(Exception e){
				e.printStackTrace();
				log.writeLog(dirlog+"smartBatch.log","failed.");
			}
		}
		return AddingApps;
	}
}
