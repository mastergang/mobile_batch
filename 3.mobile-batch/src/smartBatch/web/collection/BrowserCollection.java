package smartBatch.web.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import log.WriteMsgLog;
import smartBatch.web.model.BrowserValidModel;
import smartBatch.web.model.RowTimeModel;
import smartBatch.web.model.TimeGapModel;
import smartBatch.web.set.BrowserSet;
import smartBatch.web.set.EnvBrowserSet;
import smartBatch.web.set.TimeSet;
import DB.DBConnection;

public class BrowserCollection {
	DBConnection dbcon;
	String filtername;

	BrowserSet timeSetting = new BrowserSet();
	Collection valids = new ArrayList<BrowserValidModel>();
	
	public BrowserCollection(DBConnection dbcon, String filtername){
		this.dbcon = dbcon;
		this.filtername = filtername;
	}
	
	public Collection access() {
		timeSetting.setDbcon(dbcon);
		Collection times=timeSetting.timeSet(filtername);
		Collection valids = new ArrayList<BrowserValidModel>();
		Collection empty = null;
		Collection gaps = new ArrayList<BrowserValidModel>();
		
		if(times != null) {
			Iterator it = times.iterator();
			try {
				while(it.hasNext()) {
					RowTimeModel rTime=(RowTimeModel)it.next();
					
					String panel = rTime.getPanelid();
					String rowid = rTime.getRowid();
					int reqtime = rTime.getReqtime();
					
					Boolean valid = timeSetting.validTest(panel, reqtime);
					//System.out.println("panel_id:"+panel+" req_time:"+reqtime+"valid: "+valid);
					BrowserValidModel tg = new BrowserValidModel(rowid, valid);
					gaps.add(tg);
				}
			} catch(Exception e){
				e.printStackTrace();
				return empty;
			}
		}
		return gaps;
	}
}
