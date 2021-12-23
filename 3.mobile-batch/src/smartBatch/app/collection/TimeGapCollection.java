package smartBatch.app.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import log.WriteMsgLog;
import smartBatch.app.model.RowTimeModel;
import smartBatch.app.model.TimeGapModel;
import smartBatch.app.set.TimeSet;
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
				String prevPackage = "";
				String prevVersion = "";
				String prevScreen = "";
				String prevFlag = "";
				int prevreqtime = 0;
				boolean beingOff = false;
				
				while(it.hasNext()) {
					RowTimeModel rTime=(RowTimeModel)it.next();
					
					String currPanel = rTime.getPanelid();
					String currRowid = rTime.getRowid();
					String currPackage = rTime.getPackage_name();
					String currScreen = ((beingOff == true && !currPackage.equals("on")) || prevPackage.equals("off"))? "1" : rTime.getScreen();
					String currVersion = rTime.getVersion();
					
					int currReqtime = rTime.getReqtime();
					//System.out.println(currPanel + currRowid + currReqtime);	
					if(prevrowid != ""){
						//System.out.println(prevPanel+" : "+currPanel);
						if(currPanel.equals(prevPanel)){
							int timegap = currReqtime-prevreqtime;
							int duration = 0;
							if(beingOff == false) {
								duration = timegap;
							}
							//System.out.println("panel_id ="+prevPanel+" for rowid: "+prevrowid+" package: "+prevPackage+" timegap is :"+timegap+" duration is :"+duration+" beingOff: "+beingOff+" Screen: "+prevScreen);
							TimeGapModel tg = new TimeGapModel(prevrowid, timegap, duration, prevVersion, prevScreen);
							gaps.add(tg);
							
							if(prevPackage.equals("off") && currScreen.equals("1") && 
							   !currPackage.equals("off") && !currPackage.equals("on") &&
							   Integer.parseInt(currVersion) >= 5) {
								beingOff = true;
							} else if ((currPackage.equals("on") || currScreen.equals("0")) && 
										!currPackage.equals("off") &&
									    Integer.parseInt(currVersion) >= 5){
								beingOff = false;
							}
						}
						else {
							int lastDuration = 0;
							if(beingOff==false){
								lastDuration = defaulttimegap;
							}
							//System.out.println("panel_id ="+prevPanel+" for rowid: "+prevrowid+" package: "+prevPackage+" timegap is :"+defaulttimegap+" duration is :"+lastDuration+" beingOff: "+beingOff+" Screen: "+prevScreen);
							TimeGapModel tg = new TimeGapModel(prevrowid, defaulttimegap, lastDuration, prevVersion, prevScreen);
							gaps.add(tg);
							beingOff = false;
						}
					}
					
					prevPanel=currPanel;
					prevrowid=currRowid;
					prevreqtime=currReqtime;
					prevPackage=currPackage;
					prevVersion=currVersion;
					prevScreen=currScreen;
				}
				
				if(prevrowid!=""){
					int lastDuration = 0;
					if(beingOff==false){
						lastDuration = defaulttimegap;
					}
					//System.out.println("panel_id ="+prevPanel+" for rowid: "+prevrowid+" package: "+prevPackage+" timegap is :"+defaulttimegap+" duration is :"+lastDuration+" beingOff: "+beingOff+" Screen: "+prevScreen);
					TimeGapModel tg = new TimeGapModel(prevrowid, defaulttimegap, lastDuration, prevVersion, prevScreen);
					gaps.add(tg);
					beingOff = false;
				}
			} catch(Exception e){
				e.printStackTrace();
				return empty;
			}
		}
		return gaps;
	}
}
