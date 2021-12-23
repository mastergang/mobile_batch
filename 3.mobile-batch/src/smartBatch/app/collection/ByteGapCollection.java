package smartBatch.app.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import log.WriteMsgLog;
import smartBatch.app.model.RowByteModel;
import smartBatch.app.model.ByteGapModel;
import smartBatch.app.set.ByteSet;
import DB.DBConnection;

public class ByteGapCollection {
	DBConnection dbcon;
	String filtername;

	ByteSet bytesetting = new ByteSet();
	Collection panels = new ArrayList<String>();
	Collection bytes = new ArrayList<RowByteModel>();
	
	static final int defaulbytegap = 0;
	
	public ByteGapCollection(DBConnection dbcon, String filtername){
		this.dbcon = dbcon;
		this.filtername = filtername;
	}
	
	public Collection access() {
		bytesetting.setDbcon(dbcon);
		Collection bytes=bytesetting.byteSet(filtername);
		Collection empty = null;
		
		Collection gaps = new ArrayList<ByteGapModel>();
		if(bytes != null) {
			Iterator it = bytes.iterator();
			try {
				String prevRowid = "";
				String prevPanel_id="";
				String prevItem_value = "";
				String prevWifistatus = "";
				String prevTrack_version = "";
				long prevRxbyte = 0;
				long prevTxbyte = 0;
				
				while(it.hasNext()) {					
					RowByteModel rByte=(RowByteModel)it.next();
					
					String currRowid = rByte.getRowid();
					String currPanel_id = rByte.getPanel_id();
					String currItem_value = rByte.getItem_value();
					String currWifistatus = rByte.getWifistatus();
					String currTrack_version = rByte.getTrack_version();
					long currRxbyte = rByte.getRxbyte();
					long currTxbyte = rByte.getTxbyte();
					
					//System.out.println(currPanel_id + currRowid + currRxbyte);	
					if(prevRowid != ""){
						//System.out.println(prevPanel_id +" : "+currPanel_id);
						if(currPanel_id.equals(prevPanel_id)){
							if(currItem_value.equals(prevItem_value) && currRxbyte>=prevRxbyte && currTxbyte>=prevTxbyte){
								long rxbytegap = currRxbyte-prevRxbyte;
								long txbytegap = currTxbyte-prevTxbyte;
								
								//System.out.println("panel_id ="+prevPanel_id+" for rowid: "+prevRowid+" package: "+prevItem_value+" rxbyte is :"+rxbytegap+" txbyte is :"+txbytegap);							
								ByteGapModel tg = new ByteGapModel(prevRowid, rxbytegap, txbytegap, prevTrack_version);
								gaps.add(tg);
							} else if(currItem_value.equals(prevItem_value) && currRxbyte<=prevRxbyte && currTxbyte<=prevTxbyte){
								long rxbytegap = currRxbyte;
								long txbytegap = currTxbyte;
								
								//System.out.println("panel_id ="+prevPanel_id+" for rowid: "+prevRowid+" package: "+prevItem_value+" rxbyte is :"+rxbytegap+" txbyte is :"+txbytegap);							
								ByteGapModel tg = new ByteGapModel(prevRowid, rxbytegap, txbytegap, prevTrack_version);
								gaps.add(tg);
							} else if(currItem_value.equals(prevItem_value) && currRxbyte<=prevRxbyte && currTxbyte>=prevTxbyte){
								long rxbytegap = currRxbyte;
								long txbytegap = currTxbyte-prevTxbyte;
								
								//System.out.println("panel_id ="+prevPanel_id+" for rowid: "+prevRowid+" package: "+prevItem_value+" rxbyte is :"+rxbytegap+" txbyte is :"+txbytegap);							
								ByteGapModel tg = new ByteGapModel(prevRowid, rxbytegap, txbytegap, prevTrack_version);
								gaps.add(tg);
							} else if(currItem_value.equals(prevItem_value) && currRxbyte>=prevRxbyte && currTxbyte<=prevTxbyte){
								long rxbytegap = currRxbyte-prevRxbyte;
								long txbytegap = currTxbyte;
								
								//System.out.println("panel_id ="+prevPanel_id+" for rowid: "+prevRowid+" package: "+prevItem_value+" rxbyte is :"+rxbytegap+" txbyte is :"+txbytegap);							
								ByteGapModel tg = new ByteGapModel(prevRowid, rxbytegap, txbytegap, prevTrack_version);
								gaps.add(tg);
							} else {
								//System.out.println("panel_id ="+prevPanel_id+" for rowid: "+prevRowid+" package: "+prevItem_value+" rxbyte is :"+defaulbytegap+" txbyte is :"+defaulbytegap);							
								ByteGapModel tg = new ByteGapModel(prevRowid, defaulbytegap, defaulbytegap, prevTrack_version);
								gaps.add(tg);
							}
						} else {
							//System.out.println("panel_id ="+prevPanel_id+" for rowid: "+prevRowid+" package: "+prevItem_value+" rxbyte is :"+defaulbytegap+" txbyte is :"+defaulbytegap);							
							ByteGapModel tg = new ByteGapModel(prevRowid, defaulbytegap, defaulbytegap, prevTrack_version);
							gaps.add(tg);
						}
					}
					prevRowid = currRowid; 
					prevPanel_id = currPanel_id;  
					prevItem_value = currItem_value; 
					prevWifistatus = currWifistatus; 
					prevTrack_version = currTrack_version; 
					prevRxbyte = currRxbyte; 
					prevTxbyte = currTxbyte;
				}
				
				if(prevRowid!=""){
					//System.out.println("panel_id ="+prevPanel_id+" for rowid: "+prevRowid+" package: "+prevItem_value+" rxbyte is :"+defaulbytegap+" txbyte is :"+defaulbytegap);							
					ByteGapModel tg = new ByteGapModel(prevRowid, defaulbytegap, defaulbytegap, prevTrack_version);
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
