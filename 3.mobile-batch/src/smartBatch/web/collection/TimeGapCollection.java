package smartBatch.web.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import log.WriteMsgLog;
import smartBatch.web.model.RowTimeModel;
import smartBatch.web.model.TimeGapModel;
import smartBatch.web.set.TimeSet;
import DB.DBConnection;

public class TimeGapCollection {
	DBConnection dbcon;
	String filtername;

	TimeSet timesetting = new TimeSet();
	Collection panels = new ArrayList<String>();
	Collection times = new ArrayList<RowTimeModel>();
	
	//Timegap 기본값(마지막값)
	static final int defaulttimegap = 30;
	
	public TimeGapCollection(DBConnection dbcon, String filtername){
		this.dbcon = dbcon;
		this.filtername = filtername;
	}
	
	/**
	 * 
	 * @return: timegap object를 return
	 */
	public Collection access() {
		timesetting.setDbcon(dbcon);
		Collection times=timesetting.timeSet(filtername);
		Collection empty = null;
		Collection gaps = new ArrayList<TimeGapModel>();
		if(times != null) {
			Iterator it = times.iterator();
			try {
				String prevPanel="";
				String prevrowid = "";
				int prevreqtime = 0;
				
				while(it.hasNext()) {
					RowTimeModel rTime=(RowTimeModel)it.next();
					
					String currPanel = rTime.getPanelid();
					String currRowid = rTime.getRowid();
					int currReqtime = rTime.getReqtime();
					
					//System.out.println(currPanel + currRowid + currReqtime);
					
					if(prevrowid != ""){
						//System.out.println(prevPanel+" : "+currPanel);
						if(currPanel.equals(prevPanel)){
							int timegap = currReqtime-prevreqtime;
							if(timegap < 0){
								timegap = -1;
							}
							//System.out.println("panel_id ="+prevPanel+" for rowid: "+prevrowid+" timegap is :"+timegap);
							TimeGapModel tg = new TimeGapModel(prevrowid, timegap);
							gaps.add(tg);
						}
						else {
							//System.out.println("panel_id ="+prevPanel+" for rowid: "+prevrowid+" timegap is :"+defaulttimegap);
							TimeGapModel tg = new TimeGapModel(prevrowid, defaulttimegap);
							gaps.add(tg);
						}
					}
					prevPanel=currPanel;
					prevrowid=currRowid;
					prevreqtime=currReqtime;
				}
				
				if(prevrowid!=""){
					//System.out.println("panel_id ="+prevPanel+" for rowid: "+prevrowid+" timegap is :"+defaulttimegap);
					TimeGapModel lasttg = new TimeGapModel(prevrowid, defaulttimegap);
					gaps.add(lasttg);
				}
			} catch(Exception e){
				e.printStackTrace();
				return empty;
			}
		}
		return gaps;
	}
}
