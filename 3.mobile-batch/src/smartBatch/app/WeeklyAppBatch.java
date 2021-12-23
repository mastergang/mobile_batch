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

public class WeeklyAppBatch {
	DBConnection dbcon;
	WriteMsgLog log = new WriteMsgLog();
	String filtername;
	String code = "M";
	String dirlog;
	String weekcode;
	
	public WeeklyAppBatch(DBConnection dbcon, String filtername, String dirlog){
		this.dbcon = dbcon;
		this.filtername = filtername;
		this.dirlog = dirlog;
		this.weekcode = this.GetWeekCode(filtername);
	}
	
	public String GetWeekCode(String filename){
		String weekcode ="";

		try {
			weekcode = dbcon.executeGetWeekcode(filename);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return weekcode;
	}
	public static long findTimeDif(Calendar startPt){
		long timeDif=0;
		Calendar later = Calendar.getInstance();
		
		timeDif = (later.getTimeInMillis()-startPt.getTimeInMillis())/1000;
		return timeDif;
	}
	
	public void InsertWeekAppFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppFact(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSetupFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekSetupFact(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Setup Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertWeekAppSeg(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppSeg(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Seg took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
		
	
	public void InsertWeekAppSum(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppSum(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Sum took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekAppSession(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppSession(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Session took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekAppSumError(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppSumError(weekcode);
			log.writeLog(dirlog,filtername+"'s Weekly App Session took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekDaytimeAppSum(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekDaytimeAppSum(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Level1 took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	

	public void InsertWeekDailyAppSum(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekDailyAppSum(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Level1 took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void InsertWeekNextApp(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekNextApp(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Level1 took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekPreApp(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekPreApp(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Level1 took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertWeekAppLvl1(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppLvl1(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Level1 took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekAppLvl2(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppLvl2(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Level2 took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekAppLv1Seg(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppLv1Seg(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Level1 Seg took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekAppLv2Seg(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppLv2Seg(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Level2 Seg took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
	public void InsertWeekAppSiteSum(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekAppSiteSum(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly App Site Sum took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekAppLoyalty(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekAppLoyalty(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Domain Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekAppSwitch(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekAppSwitch(filtername);
			log.writeLog(dirlog,filtername+"'s Week App Switch Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekAppDurationTimeSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekAppDurationTimeSum(filtername);
			log.writeLog(dirlog,filtername+"'s Week App Switch Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekEntertain(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekEntertain(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Entertainment took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekTotal(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekTotal(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Total took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekTotalSession(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekTotalSession(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Total Session took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//리포트 개편에 따른 모바일 웹앱 세션 정보 입력
	public void InsertWeekWebAppSession(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekWebAppSession(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly WebAppSession took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekNotice(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWeekNotice(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Notice took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDeviceFact(){
		Calendar eachPt;
		System.out.print("The batch - Week Device Fact is processing...");
		try {
			//First Query
			String query = 					
				"insert into   tb_smart_week_device_wgt "+
				"select fn_weekcode("+filtername+") weekcode, "+
				"       PANEL_ID, "+
				"       PANEL_DEVICE_CODE, "+
				"       TELECOM_CODE, "+
				"       MODEL, "+
				"       SDK_VERSION, "+
				"       IN_MEMORY, "+
				"       EX_MEMORY, "+
				"       RAM_SIZE "+
				"from   tb_smart_device_itrack "+
				"where  (panel_id, server_date) in ( "+
				"    select panel_id, max(SERVER_DATE) max_server "+
				"    from   tb_smart_device_itrack "+
				"    where  access_day between fn_week_startday(fn_weekcode('"+filtername+"')) and fn_week_lastday(fn_weekcode('"+filtername+"')) "+
				"    and    to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= ( "+
				"        select min(to_number(track_version)) "+
				"        from   TB_SMART_TRACK_VER "+
				"        where  exp_time > sysdate "+
				"    ) "+
				"    group by panel_id "+
				") ";
			eachPt = dbcon.executeQueryTime(query);
			
			//Adjustment
			String query1 = 
					"update tb_smart_week_device_wgt a "+
					"set TELECOM_CODE=(select decode(M_PHONE_COM,1,'SK',2,'KT',3,'LG') telecom_code from tb_panel where panelid = a.panel_id) "+
					"where TELECOM_CODE='NONE' "+
					"and weekcode = fn_weekcode('"+filtername+"') ";
			eachPt = dbcon.executeQueryTime(query1);
			
			String query2 = 
					"update tb_smart_week_device_wgt "+
					"set telecom_code = upper(substr(trim(replace(TELECOM_CODE,'竊�','')),1,2)) "+
					"where weekcode = fn_weekcode('"+filtername+"') ";
			eachPt = dbcon.executeQueryTime(query2);
			
			dbcon.insertVitual(filtername,"TB_SMART_WEEK_DEVICE_WGT");
			log.writeLog(dirlog,filtername+"'s Weekly Device Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekAppLVL1Fact(){
		Calendar eachPt;
		System.out.print("The batch - Weekly AppLvl1 Fact is processing...");
		try {
			String query = 
//					"insert into tb_smart_week_applvl1_fact "+
//					"select /*+index(a,pk_smart_day_app_fact)*/ "+
//					"       fn_weekcode(access_day) weekcode, panel_id, APP_CATEGORY_CD1, "+
//					"       sum(DURATION) DURATION, count(distinct access_day) DAILY_FREQ_CNT, max(proc_date) PROC_DATE, sum(app_cnt) app_cnt "+
//					"from 	tb_smart_day_app_fact a, tb_smart_app_info b  "+
//					"WHERE   access_day between to_char(to_date('"+filtername+"','yyyymmdd')-6,'yyyymmdd') and '"+filtername+"' "+
//					"and 	a.smart_id = b.smart_id "+
//					"and 	a.proc_date < b.EXP_TIME "+
//					"and 	a.proc_date > b.EF_TIME "+
//					"and    APP_CATEGORY_CD1 is not null "+
//					"group by fn_weekcode(access_day), PANEL_ID, APP_CATEGORY_CD1 ";
					//주간 카테고리 앱 선정기준 변경 proc_date에서 해당주간 마지막 일자로 앱선정 bt kwshin. 20150210 
					"insert into tb_smart_week_applvl1_fact "+
					"select /*+index(a,pk_smart_day_app_fact)*/ "+
					"       fn_weekcode(access_day) weekcode, panel_id, APP_CATEGORY_CD1, "+
					"       sum(DURATION) DURATION, count(distinct access_day) DAILY_FREQ_CNT, max(proc_date) PROC_DATE, sum(app_cnt) app_cnt, null "+
					"from 	tb_smart_day_app_fact a, tb_smart_app_info b  "+
					"WHERE   access_day between to_char(to_date('"+filtername+"','yyyymmdd')-6,'yyyymmdd') and '"+filtername+"' "+
					"and 	a.smart_id = b.smart_id "+
					"and    a.package_name != 'kclick_equal_app' "+
					"and    exp_time > to_date('"+filtername+"','yyyymmdd')+1 "+
					"and    ef_time  <  to_date('"+filtername+"','yyyymmdd')+1 "+
					"and    APP_CATEGORY_CD1 is not null "+
					"group by fn_weekcode(access_day), PANEL_ID, APP_CATEGORY_CD1 ";
			
			eachPt = dbcon.executeQueryTime(query);
			
			String query2 = "truncate table tb_temp_week_applvl1_keyuser";
			
			eachPt = dbcon.executeQueryTime(query2);
			
			String query3 = "insert into tb_temp_week_applvl1_keyuser "+
					"SELECT   fn_weekcode('"+filtername+"') weekcode, a.app_category_cd1, panel_id, "+
					"         case when dt_sum > total_nfactor/2 and fq_sum > total_nfactor/2 then 'L' "+
					"              when dt_sum < total_nfactor/2 and fq_sum < total_nfactor/2 then 'H' "+
					"              else 'M' "+
					"         end code, "+
					"         sysdate "+
					"FROM "+
					"(        SELECT   /*+use_hash(b,a)*/ a.panel_id, a.duration dt, a.daily_freq_cnt fq, mo_n_factor, app_category_cd1, "+
					"                  sum(mo_n_factor) over (partition by app_category_cd1 order by a.duration desc, a.daily_freq_cnt desc, a.panel_id) dt_sum, "+
					"                  sum(mo_n_factor) over (partition by app_category_cd1 order by a.daily_freq_cnt desc, a.duration desc, a.panel_id) fq_sum "+
					"         FROM     tb_smart_week_applvl1_fact a, tb_smarT_panel_seg b "+
					"         WHERE    a.weekcode = fn_weekcode('"+filtername+"') "+
					"         and      a.weekcode = b.weekcode "+
					"         and      a.panel_id = b.panel_id "+
					") A, "+
					"( "+
					"         select   /*+use_hash(b,a)*/ app_category_cd1, sum(mo_n_factor) total_nfactor "+
					"         from     ( "+
					"                  select   app_category_cd1, panel_id "+
					"                  from     tb_smart_week_applvl1_fact "+
					"                  where    weekcode = fn_weekcode('"+filtername+"') "+
					"                  group by app_category_cd1, panel_id "+
					"                  ) a, "+
					"                  ( "+
					"                  select   panel_id, mo_n_factor "+
					"                  from     tb_smarT_panel_seg "+
					"                  where    weekcode = fn_weekcode('"+filtername+"') "+
					"                  ) b "+
					"         where    a.panel_id = b.panel_id "+
					"         group by app_category_cd1 "+
					") B "+
					"WHERE    A.app_category_cd1 = B.app_category_cd1 ";
			
			eachPt = dbcon.executeQueryTime(query3);			
			
			String query4 = "merge into tb_smart_week_applvl1_fact a "+
							"using tb_temp_week_applvl1_keyuser b "+
							"on (A.app_category_cd1  = b.app_category_cd1 "+
							"    and  a.weekcode = b.weekcode "+
							"    and    a.panel_id = b.panel_id "+
							"    and a.weekcode = fn_weekcode('"+filtername+"')) "+
							"when matched then "+
							"update set a.keyuser_cd = b.code ";
			eachPt = dbcon.executeQueryTime(query4);	

			log.writeLog(dirlog,filtername+"'s Weekly AppLvl1 Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("DONE.");
	}
	
	public void InsertWeekAppLVL2Fact(){
		Calendar eachPt;
		System.out.print("The batch - Weekly AppLvl2 Fact is processing...");
		try {
			String query = 
//					"insert into tb_smart_week_applvl2_fact "+
//					"select  /*+index(a,pk_smart_day_app_fact)*/ "+
//					"		fn_weekcode(access_day) weekcode, panel_id, APP_CATEGORY_CD1, APP_CATEGORY_CD2,  "+
//					"		sum(DURATION) DURATION, count(distinct access_day) DAILY_FREQ_CNT, max(proc_date) PROC_DATE, sum(app_cnt) app_cnt "+
//					"from 	tb_smart_day_app_fact a, tb_smart_app_info b "+
//					"WHERE   access_day between to_char(to_date('"+filtername+"','yyyymmdd')-6,'yyyymmdd') and '"+filtername+"' "+
//					"and 	a.smart_id = b.smart_id "+
//					"and 	a.proc_date < b.EXP_TIME "+
//					"and 	a.proc_date > b.EF_TIME "+
//					"and    APP_CATEGORY_CD2 is not null "+
//					"group by fn_weekcode(access_day), PANEL_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2 ";
					////주간 카테고리 앱 선정기준 변경 proc_date에서 해당주간 마지막 일자로 앱선정 bt kwshin. 20150210
					"insert into tb_smart_week_applvl2_fact "+
					"select  /*+index(a,pk_smart_day_app_fact)*/ "+
					"		fn_weekcode(access_day) weekcode, panel_id, APP_CATEGORY_CD1, APP_CATEGORY_CD2,  "+
					"		sum(DURATION) DURATION, count(distinct access_day) DAILY_FREQ_CNT, max(proc_date) PROC_DATE, sum(app_cnt) app_cnt, null "+
					"from 	tb_smart_day_app_fact a, tb_smart_app_info b "+
					"WHERE   access_day between to_char(to_date('"+filtername+"','yyyymmdd')-6,'yyyymmdd') and '"+filtername+"' "+
					"and 	a.smart_id = b.smart_id "+
					"and    a.package_name != 'kclick_equal_app' "+
					"and    exp_time > to_date('"+filtername+"','yyyymmdd')+1 "+
					"and    ef_time  <  to_date('"+filtername+"','yyyymmdd')+1 "+
					"and    APP_CATEGORY_CD2 is not null "+
					"group by fn_weekcode(access_day), PANEL_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2 ";					
			eachPt = dbcon.executeQueryTime(query);
			
			String query2 = "truncate table tb_temp_week_applvl2_keyuser";
			
			eachPt = dbcon.executeQueryTime(query2);			
			
			String query3 = "insert into tb_temp_week_applvl2_keyuser "+
							"SELECT   fn_weekcode('"+filtername+"') weekcode, a.app_category_cd1, a.app_category_cd2, panel_id, "+
							"         case when dt_sum > total_nfactor/2 and fq_sum > total_nfactor/2 then 'L' "+
							"              when dt_sum < total_nfactor/2 and fq_sum < total_nfactor/2 then 'H' "+
							"              else 'M' "+
							"         end code, "+
							"         sysdate "+
							"FROM "+
							"(        SELECT   /*+use_hash(b,a)*/ a.panel_id, a.duration dt, a.daily_freq_cnt fq, mo_n_factor, app_category_cd1, app_category_cd2, "+
							"                  sum(mo_n_factor) over (partition by app_category_cd2 order by a.duration desc, a.daily_freq_cnt desc, a.panel_id) dt_sum, "+
							"                  sum(mo_n_factor) over (partition by app_category_cd2 order by a.daily_freq_cnt desc, a.duration desc, a.panel_id) fq_sum "+
							"         FROM     tb_smart_week_applvl2_fact a, tb_smarT_panel_seg b "+
							"         WHERE    a.weekcode = fn_weekcode('"+filtername+"') "+
							"         and      a.weekcode = b.weekcode "+
							"         and      a.panel_id = b.panel_id "+
							") A, "+
							"( "+
							"         select   /*+use_hash(b,a)*/ app_category_cd1, app_category_cd2, sum(mo_n_factor) total_nfactor "+
							"         from     ( "+
							"                  select   app_category_cd1, app_category_cd2, panel_id "+
							"                  from     tb_smart_week_applvl2_fact "+
							"                  where    weekcode = fn_weekcode('"+filtername+"') "+
							"                  group by app_category_cd1, app_category_cd2, panel_id "+
							"                  ) a, "+
							"                  ( "+
							"                  select   panel_id, mo_n_factor "+
							"                  from     tb_smarT_panel_seg "+
							"                  where    weekcode = fn_weekcode('"+filtername+"') "+
							"                  ) b "+
							"         where    a.panel_id = b.panel_id "+
							"         group by app_category_cd1, app_category_cd2 "+
							") B "+
							"WHERE    A.app_category_cd1 = B.app_category_cd1 "+
							"and      A.app_category_cd2 = B.app_category_cd2 ";
			
			eachPt = dbcon.executeQueryTime(query3);
			
			String query4 = "merge into tb_smart_week_applvl2_fact a "+
							"using tb_temp_week_applvl2_keyuser b "+
							"on (A.app_category_cd1  = b.app_category_cd1 "+
							"    and A.app_category_cd2  = b.app_category_cd2 "+
							"    and a.weekcode = b.weekcode "+
							"    and a.panel_id = b.panel_id "+
							"    and a.weekcode = fn_weekcode('"+filtername+"')) "+
							"when matched then "+
							"update set a.keyuser_cd = b.code ";
			
			eachPt = dbcon.executeQueryTime(query4);

			log.writeLog(dirlog,filtername+"'s Weekly AppLvl2 Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("DONE.");
	}
	
	public void InsertWeekWifiFact(){
		Calendar eachPt;
		System.out.print("The batch - Weekly Wifi Fact is processing...");
		try {
			////20140122 RXBYTE_GAP, TXBYTE_GAP 추가 목적으로 주석처리 MSKIM
//			String query =	
//					"INSERT   INTO tb_smart_week_wifi_fact "+
//					"         (weekcode, smart_id, package_name, panel_id, WIFISTATUS, duration, proc_date) "+
//					"SELECT   fn_weekcode(access_day) weekcode, smart_id, package_name, panel_id, WIFISTATUS, sum(duration) duration, "+  
//					"         sysdate "+ 
//					"FROM     tb_smart_day_wifi_fact a "+
//					"WHERE   access_day between to_char(to_date('"+filtername+"','yyyymmdd')-6,'yyyymmdd') and '"+filtername+"' "+
//					"GROUP BY fn_weekcode(access_day), smart_id, package_name, panel_id, WIFISTATUS ";
			
			////20140122 RXBYTE_GAP, TXBYTE_GAP 추가 MSKIM
			String query =	
					"INSERT   INTO tb_smart_week_wifi_fact "+
					"         (weekcode, smart_id, package_name, panel_id, WIFISTATUS, duration, proc_date, RXBYTE_GAP, TXBYTE_GAP) "+
					"SELECT   fn_weekcode(access_day) weekcode, smart_id, package_name, panel_id, WIFISTATUS, sum(duration) duration, "+  
					"         sysdate proc_date, sum(RXBYTE_GAP) RXBYTE_GAP, sum(TXBYTE_GAP) TXBYTE_GAP "+ 
					"FROM     tb_smart_day_wifi_fact a "+
					"WHERE   access_day between to_char(to_date('"+filtername+"','yyyymmdd')-6,'yyyymmdd') and '"+filtername+"' "+
					"GROUP BY fn_weekcode(access_day), smart_id, package_name, panel_id, WIFISTATUS ";
			
			eachPt = dbcon.executeQueryTime(query);
			
			log.writeLog(dirlog,filtername+"'s Weekly Wifi Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("DONE.");
	}	
}
