package smartBatch.web;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;

import log.WriteMsgLog;
import smartBatch.web.collection.Extreme1Collection;
import smartBatch.web.collection.QueryCollection;
import smartBatch.web.collection.TimeGapCollection;
import smartBatch.web.model.QueryModel;
import smartBatch.web.model.TimeGapModel;
import DB.DBConnection;

public class DailyWebBatch {
	DBConnection dbcon;
	WriteMsgLog log = new WriteMsgLog();
	String filtername;
	String code = "D";
	String dirlog;
	
	public DailyWebBatch(DBConnection dbcon, String filtername, String dirlog){
		this.dbcon = dbcon;
		this.filtername = filtername;
		this.dirlog = dirlog;
	}
	
	public static long findTimeDif(Calendar startPt){
		long timeDif=0;
		Calendar later = Calendar.getInstance();
		
		timeDif = (later.getTimeInMillis()-startPt.getTimeInMillis())/1000;
		return timeDif;
	}

//	public void insertDailyPanelSignal(){
//		Calendar eachPt;
//		try {
//			eachPt = dbcon.executePanelSignal(code,filtername);
//			log.writeLog(dirlog,filtername+"'s Daily Panel Signal took: "+findTimeDif(eachPt)+"s");
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
	
	public void updateBanURL(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeBanURL();
			log.writeLog(dirlog,filtername+"'s Ban_URL Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void updateServer(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeServerUpdate(filtername);
			log.writeLog(dirlog,filtername+"'s Serverdate Update took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void insertDailyNewSite(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeIndefinedInsert(filtername);
			log.writeLog(dirlog,filtername+"'s Daily New Site took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
/*	public void updateCheckDate() {
		Calendar eachPt;
		try {
			eachPt = dbcon.executeCheckDate(filtername);
			log.writeLog(dirlog,filtername+"'s Checking ServerDate took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}*/
	
	public void updateResultCD(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeResultUpdate(filtername);
			log.writeLog(dirlog,filtername+"'s Daily ResultCD update took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void updateSiteID(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeSiteIdUpdate(filtername);
			log.writeLog(dirlog,filtername+"'s Daily SiteID Update took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void chromeSbrowserResultCD(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeChromeSbrowserReslutUpdate(filtername);
			log.writeLog(dirlog,filtername+"'s Daily SiteID Update took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDaytimeVisit(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeDaytimeVisit(filtername);
			log.writeLog(dirlog,filtername+"'s Daytime Visit Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertRawDaytime(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeRawDaytime(filtername);
			log.writeLog(dirlog,filtername+"'s Daytime Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
		
	
	public void InsertDaytimeFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeDaytimeFact(filtername);
			log.writeLog(dirlog,filtername+"'s Daytime Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDailyFact(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeFact(code, filtername);
			log.writeLog(dirlog,filtername+"'s Daily Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDaySiteSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeDaySiteSum(filtername);
			log.writeLog(dirlog,filtername+"'s Daily Site Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDayBounceRate(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeDayBounceRate(filtername);
			log.writeLog(dirlog,filtername+"'s Daily Site Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertTarItrack(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeTarItrack(filtername);
			log.writeLog(dirlog,filtername+"'s Daily TAR insert Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void updateQueryDecode(){
		Calendar eachPt = Calendar.getInstance();
		System.out.print("Decode Update has started...");
		QueryCollection querycollection = new QueryCollection(dbcon, filtername);
		int cnt = 0;
		Collection decodes = new ArrayList<QueryModel>();
		decodes = querycollection.access();
		
		if(decodes != null){
			Iterator decode = decodes.iterator();
			try {
				String prev_rowid = "";
				while(decode.hasNext()) {
					QueryModel query=(QueryModel)decode.next();
					String query_decode = query.getQuery();
					
					//System.out.println(query_decode+ " : ");
					//예외케이스 처리
					if(query_decode.contains("'")){
						query_decode=query_decode.replace("'","''");
						query.setQuery(query_decode);
					}
					if(query_decode.length()>1000){
						query_decode=query_decode.substring(0, 900);
						query.setQuery(query_decode);
					}
					String rowid = query.getRowid();
					
					if(!prev_rowid.equals(rowid) && rowid != null 
							&& !query_decode.trim().equals("")){
						//System.out.println(query_decode+ " : "+rowid);
						dbcon.executeDecodeUpdate(query);
						cnt++;
						prev_rowid=rowid;
					}
				}
				//System.exit(0);
			System.out.println("Decode Update: "+filtername+" is done; decoded rows are "+cnt);
			log.writeLog(dirlog,filtername+"'s Daily Query Decode took: "+findTimeDif(eachPt)+"s ; decode rows are "+cnt);
			} catch(Exception e){
				e.printStackTrace();
			} finally {
				try {
			    	dbcon.partclose();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	public void InsertDayEnterService(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeDayEnterService(filtername);
			log.writeLog(dirlog,filtername+"'s Daily Entertainment Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDayTotal(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeDayTotal(filtername);
			log.writeLog(dirlog,filtername+"'s Daily Total Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDailyPanel(){
		Calendar eachPt;
		System.out.print("Inserting Daily Panel Seg has started...");
		
		try {
			eachPt = dbcon.executeDayPanel(filtername);
			log.writeLog(dirlog,filtername+"'s Daily Panel Seg Insertion took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}		
		
	
	public void InsertBrowserVirtual(){
		Calendar eachPt;
		System.out.print("Inserting Browser Virtual has started...");
		
		try {
			eachPt = dbcon.insertBrowserVitual(filtername);
			log.writeLog(dirlog,filtername+"'s Browser Virtual Insertion took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}
	
	public void updateDuration(){
		//TimeGapBatch 시작
		Calendar eachPt = Calendar.getInstance();
		System.out.print("Duration Update has started...");
		TimeGapCollection timegapcollection = new TimeGapCollection(dbcon, filtername);
		Collection timegaps = new ArrayList<TimeGapModel>();
		timegaps = timegapcollection.access();
		
		if(timegaps != null){
			Iterator gap = timegaps.iterator();
			try {
				int i = 1;
				dbcon.setAutoCommit(false);
				while(gap.hasNext()) {
					TimeGapModel TimeG=(TimeGapModel)gap.next();
					if(TimeG.getRowid()!=""){
						String rowid = TimeG.getRowid();
						int duration = TimeG.getTimegap();
						int timegap = TimeG.getTimegap();
						//duration의 기준. 600초가 넘어가면 60초로 들어간다.
						if(duration > 600){
							duration = 60;
						}
						//System.out.print("Rowid: "+rowid);
						//System.out.println(" || duration: "+duration);
						dbcon.executeDurationUpdate(rowid, timegap, duration);
						i++;
						if(i%30000==0){
							dbcon.executeQuery("commit");
						}
					}
				}
				dbcon.executeQuery("commit");
				dbcon.setAutoCommit(true);
				System.out.println("Duration Update: "+filtername+" is done; rows are "+i);
				log.writeLog(dirlog,filtername+"'s Daily Duration Update took: "+findTimeDif(eachPt)+"s ; updated rows are "+i);
				//System.out.println("Duration.");
			} catch(Exception e){
				e.printStackTrace();
			} finally {
				try {
			    	dbcon.partclose();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	public void updateBrowserValid(){
		//Browser Valid Testing
		Calendar eachPt = Calendar.getInstance();
		System.out.print("Browser Valid Update has started...");
		try {
			dbcon.executeQueryExecute("truncate table tb_smart_temp_browser_test");
			//Insert Env Data
			String query = 					
					"insert into tb_smart_temp_browser_test "+
					"select ACCESS_DAY, PANEL_ID, REGISTER_DATE, TIME_GAP, "+
					"       to_number(to_char(REGISTER_DATE,'SSSSS')) START_TIME, "+
					"       to_number(to_char(REGISTER_DATE,'SSSSS'))+nvl(TIME_GAP,60) END_TIME "+
					"from   tb_smart_env_itrack "+
					//"from   TEMP_JSPARK_SMART_ENV_ITRACK "+
					"where  access_day = '"+filtername+"' "+
					"and    item_value = 'net.daum.android.daum' ";
			dbcon.executeQueryExecute(query);
			
			//Remove Env Related Browser
			String query1 = 
					"update tb_smart_browser_itrack "+
					"set    result_cd = 'A' "+
					"where  rowid in ( "+
					"    select rid "+
					"    from ( "+
					"        select panel_id, rid, max(valid) valid_test "+
					"        from ( "+
					"            select a.panel_id, rid, "+
					"                   case when REQ_TIME between START_TIME-30 and END_TIME+30 then '1' "+
					"                   else '0' end as valid, "+
					"                   START_TIME "+
					"            from   ( "+
					"                select panel_id, rowid rid, to_number(to_char(REQ_DATE,'SSSSS')) req_time "+
					"                from   tb_smart_browser_itrack "+
					"                where  panel_flag = 'D' "+
					"                and    result_cd = 'S' "+
					"                and    access_day = '"+filtername+"' "+
					"            ) a, tb_smart_temp_browser_test b "+
					"            where  a.panel_id = b.panel_id(+) "+
					"        ) "+
					"        group by panel_id, rid "+
					"    ) "+
					"    where valid_test = '1' "+
					") ";
			eachPt = dbcon.executeQueryTime(query1);
			System.out.println("DONE.");
			log.writeLog(dirlog,filtername+"'s Daily Browser Valid took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void updateExtreme(int term){
		//TimeGapBatch 시작
		Calendar eachPt = Calendar.getInstance();
		System.out.print("Extreme"+term+" Value Update has started...");
		
		try {
			dbcon.executeExtremeInsert(filtername,term);
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		
		Extreme1Collection exCollection = new Extreme1Collection(dbcon, filtername);
		int cnt = 0;
		Collection removals = new ArrayList<String>();
		removals = exCollection.access();
		
		if(removals != null){
			Iterator remove = removals.iterator();
			try {
				while(remove.hasNext()) {
					String rowid=(String)remove.next();
					if(rowid !=""){
						//System.out.println("Rowid: "+rowid);
						dbcon.executeExtremeUpdate(rowid, term);
						cnt++;
					}
				}
				System.out.println("Extreme"+term+" Update: "+filtername+" is done; rows are "+cnt);
				//log.writeLog(dirlog,filtername+"'s Daily Duration Update took: "+findTimeDif(eachPt)+"s ; updated rows are "+cnt);
				//System.out.println("Duration.");
			} catch(Exception e){
				e.printStackTrace();
			} finally {
				try {
			    	dbcon.partclose();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
}
