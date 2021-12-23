package smartBatch.web.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import smartBatch.web.model.iTrackModel;
import smartBatch.web.set.Extreme1Set;
import DB.DBConnection;

public class Extreme1Collection {
	DBConnection dbcon;
	String filtername;

	Extreme1Set extremeSetting = new Extreme1Set();
	Collection rowids = new ArrayList<String>();
	
	public Extreme1Collection(DBConnection dbcon, String filtername){
		this.dbcon = dbcon;
		this.filtername = filtername;
	}
	
	/**
	 * 
	 * @return: timegap object를 return
	 */
	public Collection access() {
		extremeSetting.setDbcon(dbcon);
		Collection extremes=extremeSetting.extreme1Set(filtername);
		Collection empty = null;
		Collection samplingnum = null;
		Collection removals = new ArrayList<String>();
		if(extremes != null) {
			Iterator it = extremes.iterator();
			try {
				String prevPanel="";
				String prevdomain = "";
				Iterator rowno = null;
				
				while(it.hasNext()) {
					iTrackModel iTrack=(iTrackModel)it.next();
					
					String currPanel = iTrack.getPanelid();
					String currRowid = iTrack.getRowid();
					String domain = iTrack.getDomain();
					String ext_domain = iTrack.getExt_domain();
					int siteid = iTrack.getSiteid();
					int p30 = iTrack.getP30();
					int pvadjust = iTrack.getPv_adjust();
					
					//System.out.print("panel_id ="+currPanel+" for rowid: "+currRowid+" domain is :"+domain);
					
					if(!prevdomain.equals(ext_domain)||!currPanel.equals(prevPanel)){
					//System.out.println(prevPanel+" : "+currPanel);
						samplingnum = extremeSetting.delete1Set(p30, pvadjust);
						rowno = samplingnum.iterator();
						try{
							int removal = Integer.parseInt((String)rowno.next());
							if(removal > 0){
								removals.add(currRowid);
							}
							//System.out.print(" delete? "+removal);
						} catch(Exception e){
							e.printStackTrace();
						}
					}
					else {
						try {
							int removal = Integer.parseInt((String)rowno.next());
							if(removal > 0){
								removals.add(currRowid);
							}
							//System.out.print(" delete? "+removal);
						} catch(Exception e){
							e.printStackTrace();
						}
					}
					//System.out.println("");
					prevPanel=currPanel;
					prevdomain=ext_domain;
				}
			} catch(Exception e){
				e.printStackTrace();
				return empty;
			}
		}
		return removals;
	}
}
