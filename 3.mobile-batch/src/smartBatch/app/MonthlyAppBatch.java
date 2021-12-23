package smartBatch.app;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;

import smartBatch.app.collection.AppCollection;
import smartBatch.app.model.AppModel;
import smartBatch.web.collection.QueryCollection;
import smartBatch.web.collection.TimeGapCollection;
import smartBatch.web.model.QueryModel;
import smartBatch.web.model.TimeGapModel;
import log.WriteMsgLog;
import DB.DBConnection;

public class MonthlyAppBatch {
	DBConnection dbcon;
	WriteMsgLog log = new WriteMsgLog();
	String filtername;
	String code = "M";
	String dirlog;
	String lastday;
	
	public MonthlyAppBatch(DBConnection dbcon, String filtername, String dirlog){
		this.dbcon = dbcon;
		this.filtername = filtername;
		this.dirlog = dirlog;
		this.lastday = this.GetMonthLastDay(filtername);
	}
	
	public String GetMonthLastDay(String filename){
		String lastday ="";

		try {
			lastday = dbcon.executeGetMonthLastDay(filename);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return lastday;
	}
	
	public static long findTimeDif(Calendar startPt){
		long timeDif=0;
		Calendar later = Calendar.getInstance();
		
		timeDif = (later.getTimeInMillis()-startPt.getTimeInMillis())/1000;
		return timeDif;
	}
	
	public void InsertVirtual(String table_name){
		Calendar eachPt;
		System.out.print("Inserting "+table_name+" Virtual has started...");
		try {
			eachPt = dbcon.insertVitual(filtername, table_name);
			log.writeLog(dirlog,filtername+"'s "+table_name+"virtual Insertion took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppWgt(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeAppFact(code, filtername);
			log.writeLog(dirlog,filtername+"'s Monthly App Weight took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthSessionWgt(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeSessionWgt(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Session Weight took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDeviceFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeDeviceFact(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Device Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertSetupFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeSetupFact(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Setup Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppSumError(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppSumError(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Application Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void InsertMonthDaytimeAppSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthDaytimeAppSum(filtername, lastday);
			log.writeLog(dirlog,filtername+"'s Monthly App Daytime Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthDailyAppSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthDailyAppSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Daily App Sum  Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertMonthNextApp(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthNextApp(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Next App Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthPreApp(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthPreApp(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Pre App Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppLoyalty(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppLoyalty(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly App Loyalty Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppSiteSwitch(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppSwitch(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly App Switch Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertMonthAppDurationTimeSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppDurationTimeSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly App Duration Time Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppLVL1Fact(){
		Calendar eachPt;
		System.out.print("The batch - Monthly AppLvl1 Fact is processing...");
		try {
			String query = 
					"insert into tb_smart_month_applvl1_fact "+
					"SELECT   /*+index(a,pk_smart_day_app_fact) use_hash(b,a)*/" +
					"	      substr(access_day,1,6) monthcode, PANEL_ID, APP_CATEGORY_CD1,   "+
					"         sum(DURATION) DURATION, count(distinct access_day) DAILY_FREQ_CNT, max(proc_date) PROC_DATE, sum(app_cnt) app_cnt, null keyuser_cd "+
					"FROM     tb_smart_day_app_fact a, tb_smart_app_info b  "+
					"WHERE    a.access_day like '"+filtername+"'||'%'  "+
					"and      a.smart_id = b.smart_id  "+
					"and      a.package_name != 'kclick_equal_app' "+
					"and      b.ef_time < to_date(fn_month_lastday('"+filtername+"'),'yyyymmdd')+1  "+
					"and      b.exp_time > to_date(fn_month_lastday('"+filtername+"'),'yyyymmdd')+1  "+
					"and      APP_CATEGORY_CD1 is not null "+
					"group by substr(access_day,1,6), PANEL_ID, APP_CATEGORY_CD1 ";
			eachPt = dbcon.executeQueryTime(query);
			
		//	String query2 = "truncate table tb_temp_month_applvl1_keyuser ";
			
		//	eachPt = dbcon.executeQueryTime(query2);
			
//			String query3 = "insert into tb_temp_month_applvl1_keyuser "+
//							"SELECT   '"+filtername+"' monthcode, a.app_category_cd1, panel_id, "+
//							"         case when dt_sum > total_nfactor/2 and fq_sum > total_nfactor/2 then 'L' "+
//							"              when dt_sum < total_nfactor/2 and fq_sum < total_nfactor/2 then 'H' "+
//							"              else 'M' "+
//							"         end code, "+
//							"         sysdate "+
//							"FROM "+
//							"(        SELECT   /*+use_hash(b,a)*/ a.panel_id, a.duration dt, a.daily_freq_cnt fq, mo_n_factor, app_category_cd1, "+
//							"                  sum(mo_n_factor) over (partition by app_category_cd1 order by a.duration desc, a.daily_freq_cnt desc, a.panel_id) dt_sum, "+
//							"                  sum(mo_n_factor) over (partition by app_category_cd1 order by a.daily_freq_cnt desc, a.duration desc, a.panel_id) fq_sum "+
//							"         FROM     tb_smart_month_applvl1_fact a, tb_smarT_month_panel_seg b "+
//							"         WHERE    a.monthcode = '"+filtername+"' "+
//							"         and      a.monthcode = b.monthcode "+
//							"         and      a.panel_id = b.panel_id "+
//							") A, "+
//							"( "+
//							"         select   /*+use_hash(b,a)*/ app_category_cd1, sum(mo_n_factor) total_nfactor "+ 
//							"         from     ( "+
//							"                  select   app_category_cd1, panel_id "+ 
//							"                  from     tb_smart_month_applvl1_fact "+
//							"                  where    monthcode = '"+filtername+"' "+
//							"                  group by app_category_cd1, panel_id "+
//							"                  ) a, "+
//							"                  ( "+
//							"                  select   panel_id, mo_n_factor "+
//							"                  from     tb_smarT_month_panel_seg "+
//							"                  where    monthcode = '"+filtername+"' "+
//							"                  ) b "+
//							"         where    a.panel_id = b.panel_id "+
//							"         group by app_category_cd1 "+
//							") B "+
//							"WHERE    A.app_category_cd1 = B.app_category_cd1 ";
//			
//			eachPt = dbcon.executeQueryTime(query3);
//			
//		    String query4 = "merge into tb_smart_month_applvl1_fact a "+
//							"using tb_temp_month_applvl1_keyuser b "+
//							"on (A.app_category_cd1  = b.app_category_cd1 "+
//							"    and  a.monthcode = b.monthcode "+
//							"    and    a.panel_id = b.panel_id "+
//							"    and a.monthcode = '"+filtername+"') "+
//							"when matched then "+
//							"update set a.keyuser_cd = b.code ";
//			
//			eachPt = dbcon.executeQueryTime(query4);
			
			log.writeLog(dirlog,filtername+"'s Monthly AppLvl1 Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("DONE.");
	}
	
	public void InsertMonthAppFact(){
		Calendar eachPt;
		System.out.print("The batch - Monthly App Fact is processing...");
		try {
			String query = 
				"INSERT   INTO tb_smart_month_app_fact "+
				"         (monthcode, smart_id, package_name, panel_id, duration, daily_freq_cnt, proc_date, app_cnt) "+
				"SELECT   /*+index(a,pk_smart_day_app_fact)*/ "+
				"         substr(access_day,1,6) monthcode, smart_id, package_name, panel_id, sum(duration) duration, "+
				"         count(distinct access_day), sysdate, sum(app_cnt) app_cnt "+
				"FROM     tb_smart_day_app_fact a "+
				"WHERE    access_day >= '"+filtername+"01' "+
				"AND      access_day <= fn_month_lastday('"+filtername+"') "+
				"GROUP BY substr(access_day,1,6), smart_id, package_name, panel_id ";
			eachPt = dbcon.executeQueryTime(query);	
			

			log.writeLog(dirlog,filtername+"'s Monthly App Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("DONE.");
	}
	
	public void InsertMonthSetupFact(){
		Calendar eachPt;
		System.out.print("The batch - Monthly Setup Fact is processing...");
		try {
			String query = 
					"insert into tb_smart_month_setup_fact "+
					"select /*+index(a,pk_smart_day_setup_fact)*/substr(access_day,1,6) monthcode, smart_id, package_name, panel_id, sysdate "+
					"from   tb_smart_day_setup_fact a "+
					"where  access_day between '"+filtername+"'||'01' and fn_month_lastday('"+filtername+"') "+
					"group by substr(access_day,1,6), smart_id, package_name, panel_id ";
			eachPt = dbcon.executeQueryTime(query);
			
			log.writeLog(dirlog,filtername+"'s Monthly Setup Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("DONE.");
	}
	
	public void InsertMonthAppLVL2Fact(){
		Calendar eachPt;
		System.out.print("The batch - Monthly AppLvl2 Fact is processing...");
		try {
			String query = 
					"insert into tb_smart_month_applvl2_fact "+
					"SELECT   /*+index(a,pk_smart_day_app_fact) use_hash(b,a)*/" +
					"         substr(access_day,1,6) monthcode, PANEL_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2,    "+
					"         sum(DURATION) DURATION, count(distinct access_day) DAILY_FREQ_CNT, max(proc_date) PROC_DATE, sum(app_cnt) app_cnt, null keyuser_cd "+
					"FROM     tb_smart_day_app_fact a, tb_smart_app_info b  "+
					"WHERE    a.access_day like '"+filtername+"'||'%'  "+
					"and      a.smart_id = b.smart_id  "+
					"and      a.package_name != 'kclick_equal_app' "+
					"and      b.ef_time < to_date(fn_month_lastday('"+filtername+"'),'yyyymmdd')+1  "+
					"and      b.exp_time > to_date(fn_month_lastday('"+filtername+"'),'yyyymmdd')+1  "+
					"and      APP_CATEGORY_CD2 is not null "+
					"group by substr(access_day,1,6), PANEL_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2 ";
			eachPt = dbcon.executeQueryTime(query);
			
//			String query2 = "truncate table tb_temp_month_applvl2_keyuser ";
//			
//			eachPt = dbcon.executeQueryTime(query2);
//			
//			String query3 = "insert into tb_temp_month_applvl2_keyuser "+
//							"SELECT   '"+filtername+"' monthcode, a.app_category_cd1, a.app_category_cd2, panel_id, "+
//							"         case when dt_sum > total_nfactor/2 and fq_sum > total_nfactor/2 then 'L' "+
//							"              when dt_sum < total_nfactor/2 and fq_sum < total_nfactor/2 then 'H' "+
//							"              else 'M' "+
//							"         end code, "+
//							"         sysdate "+
//							"FROM "+
//							"(        SELECT   /*+use_hash(b,a)*/ a.panel_id, a.duration dt, a.daily_freq_cnt fq, mo_n_factor, app_category_cd1, app_category_cd2, "+
//							"                  sum(mo_n_factor) over (partition by app_category_cd2 order by a.duration desc, a.daily_freq_cnt desc, a.panel_id) dt_sum, "+
//							"                  sum(mo_n_factor) over (partition by app_category_cd2 order by a.daily_freq_cnt desc, a.duration desc, a.panel_id) fq_sum "+
//							"         FROM     tb_smart_month_applvl2_fact a, tb_smarT_month_panel_seg b "+
//							"         WHERE    a.monthcode = '"+filtername+"' "+
//							"         and      a.monthcode = b.monthcode "+
//							"         and      a.panel_id = b.panel_id "+
//							") A, "+
//							"( "+
//							"         select   /*+use_hash(b,a)*/ app_category_cd1, app_category_cd2, sum(mo_n_factor) total_nfactor "+
//							"         from     ( "+
//							"                  select   app_category_cd1, app_category_cd2, panel_id "+
//							"                  from     tb_smart_month_applvl2_fact "+
//							"                  where    monthcode = '"+filtername+"' "+
//							"                  group by app_category_cd1, app_category_cd2, panel_id "+
//							"                  ) a, "+
//							"                  ( "+
//							"                  select   panel_id, mo_n_factor "+
//							"                  from     tb_smarT_month_panel_seg "+
//							"                  where    monthcode = '"+filtername+"' "+
//							"                  ) b "+
//							"         where    a.panel_id = b.panel_id "+
//							"         group by app_category_cd1, app_category_cd2 "+
//							") B "+
//							"WHERE    A.app_category_cd1 = B.app_category_cd1 "+
//							"and      A.app_category_cd2 = B.app_category_cd2 ";
//			
//			eachPt = dbcon.executeQueryTime(query3);
//			
//		    String query4 = "merge into tb_smart_month_applvl2_fact a "+
//				    		"using tb_temp_month_applvl2_keyuser b "+
//				    		"on (A.app_category_cd1  = b.app_category_cd1 "+
//				    		"    and A.app_category_cd2  = b.app_category_cd2 "+
//				    		"    and a.monthcode = b.monthcode "+
//				    		"    and a.panel_id = b.panel_id "+
//				    		"    and a.monthcode = '"+filtername+"') "+
//				    		"when matched then "+
//				    		"update set a.keyuser_cd = b.code ";    
//
//			
//			eachPt = dbcon.executeQueryTime(query4);
						
			
			log.writeLog(dirlog,filtername+"'s Monthly AppLvl2 Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("DONE.");
	}

	public void InsertMonthWifiFact(){
		Calendar eachPt;
		System.out.print("The batch - Monthly Wifi Fact is processing...");
		try {
			//20140122 RXBYTE_GAP, TXBYTE_GAP 異붽� 紐⑹쟻�쑝濡� 二쇱꽍 泥섎━
//			String query = 
//					"INSERT   INTO tb_smart_month_wifi_fact "+
//					"         ( monthcode, smart_id, package_name, panel_id, WIFISTATUS, duration, proc_date) "+    
//					"SELECT   substr(access_day,1,6) monthcode, smart_id, package_name, panel_id, WIFISTATUS, sum(duration) duration, "+  
//					"         sysdate "+ 
//					"FROM     tb_smart_day_wifi_fact a "+
//					"WHERE    a.access_day like '"+filtername+"'||'%'  "+
//					"GROUP BY substr(access_day,1,6), smart_id, package_name, panel_id, WIFISTATUS ";
			
			//20140122 RXBYTE_GAP, TXBYTE_GAP 異붽�			
			String query = 
					"INSERT   INTO tb_smart_month_wifi_fact "+
					"         ( monthcode, smart_id, package_name, panel_id, WIFISTATUS, duration, proc_date, RXBYTE_GAP, TXBYTE_GAP) "+    
					"SELECT   substr(access_day,1,6) monthcode, smart_id, package_name, panel_id, WIFISTATUS, sum(duration) duration, "+  
					"         sysdate proc_date, sum(RXBYTE_GAP) RXBYTE_GAP, sum(TXBYTE_GAP) TXBYTE_GAP "+ 
					"FROM     tb_smart_day_wifi_fact a "+
					"WHERE    a.access_day like '"+filtername+"'||'%'  "+
					"GROUP BY substr(access_day,1,6), smart_id, package_name, panel_id, WIFISTATUS ";
			eachPt = dbcon.executeQueryTime(query);
			
			log.writeLog(dirlog,filtername+"'s Monthly Wifi Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("DONE.");
	}	
}
