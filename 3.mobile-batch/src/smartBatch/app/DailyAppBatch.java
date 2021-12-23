package smartBatch.app;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;

import smartBatch.app.collection.AppCollection;
import smartBatch.app.collection.AppParser;
import smartBatch.app.model.AppModel;
import smartBatch.app.model.RowTimeModel;
import smartBatch.web.collection.QueryCollection;
import smartBatch.app.collection.TimeGapCollection;
import smartBatch.app.collection.ByteGapCollection;
import smartBatch.web.model.QueryModel;
import smartBatch.app.model.TimeGapModel;
import smartBatch.app.model.ByteGapModel;
import log.WriteMsgLog;
import DB.DBConnection;

public class DailyAppBatch {
	DBConnection dbcon;
	WriteMsgLog log = new WriteMsgLog();
	String filtername;
	String dirlog;
	String code = "D";
	
	public DailyAppBatch(DBConnection dbcon, String filtername, String dirlog){
		this.dbcon = dbcon;
		this.filtername = filtername;
		this.dirlog = dirlog;
	}
	
	public void InsertDailySetupFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeDailySetupFact(filtername);
			log.writeLog(dirlog,filtername+"'s Daily Setup Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public void InsertDeviceFact(){	//20140430 異붽� MSKIM
		Calendar eachPt;
		try {
			eachPt = dbcon.executeDailyDeviceFact(filtername);
			log.writeLog(dirlog,filtername+"'s Daily Device Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static long findTimeDif(Calendar startPt){
		long timeDif=0;
		Calendar later = Calendar.getInstance();
		
		timeDif = (later.getTimeInMillis()-startPt.getTimeInMillis())/1000;
		return timeDif;
	}
	
	public void InsertAppDaytimeFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeAppDaytimeFact(filtername);
			log.writeLog(dirlog,filtername+"'s Daytime App Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void DeleteEnvError(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeEnvDelete(filtername);
			log.writeLog(dirlog,filtername+"'s Daytime App Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void updateAppExtreme(){
		Calendar eachPt = null;
		System.out.print("The batch - Day App Extreme is processing...");
		try {
			//Truncate
			String query = "truncate table tb_smart_day_app_new_wgt ";
			dbcon.executeQueryExecute(query);
			query = "truncate table tb_smart_day_extreme_app_fact ";
			dbcon.executeQueryExecute(query);
			query = "delete tb_smart_daytime_app_new_wgt where access_day = '"+filtername+"' ";
			dbcon.executeQueryExecute(query);
			
			//Insert day wgt
			query = "insert into tb_smart_day_app_new_wgt "+
					"select /*+ index(a,PK_SMART_DAYTIME_APP_WGT) */ access_day, smart_id, package_name, panel_id, sum(duration), sysdate, null new_duration, '' flag "+
					"from   tb_smart_daytime_app_wgt a "+
					"where  access_day = '"+filtername+"' "+
					"AND    PACKAGE_NAME is not null "+
					"GROUP BY access_day, smart_id, package_name, panel_id ";
			dbcon.executeQueryExecute(query);
			
			//2nd Rank Calculation
			query = "insert into tb_smart_day_extreme_app_fact "+
					"select access_day, smart_id, "+
					"       panel_id, d30, d29, d28, d1, dsum28, dsum_all,  "+
					"       duration_new_fvalue, '', '', panel_cnt, "+
					"       panel_new_fvalue, duration_new_adjusted_value "+
					"from "+
					"( "+
					"     select access_day, smart_id, "+
					"            d30, d29, d28, d27, d1, d0, dsum28, dsum27, dsum2930, dsum30, dsum_all, top_rank2, "+
					"            round((dsum28+2*d28-30*d1)/(top_rank-3),2) da, "+
					"            decode((dsum28+2*d28-30*d1)/(top_rank-3),0,0 ,decode((dsum28+2*d28-top_rank*d1),0,0, decode(sign(d30-101),-1,0, "+
					"            round((d30-d29)/((dsum28+2*d28-top_rank*d1)/(top_rank-3)),2)))) d_fvalue, "+
					"            panel_cnt, "+
					"            panel_id, "+
					"            sd_duration, "+
					"            avg_duration, "+
					"            decode(sign(panel_cnt-30),-1,kclick.fn_fvalue3(top_rank2),kclick.fn_fvalue2(top_rank2)) panel_new_fvalue, "+
					"            round(((((d30-1*d0)/decode((d29-d0),0,0.0001,(d29-d0)))-1)/1)/((((dsum28-(top_rank2-2)*d0)/decode((d29-d0),0,0.0001,(d29-d0)))+1+1)/(top_rank-1)),2) duration_new_fvalue, "+
					"            round(d29+decode(sign(panel_cnt-30),-1,kclick.fn_fvalue3(top_rank2),kclick.fn_fvalue2(top_rank2))*(((dsum29-(top_rank2-1)*d0)+1*(d29-d0))/(top_rank2-1))) duration_new_adjusted_value "+
					"     from "+
					"     ( "+
					"             select access_day, SMART_ID, "+
					"                    min(case when DT_RANK = 2 then panel_id end ) panel_id, "+
					"                    sum(case when DT_RANK >= 2 and DT_RANK <= top_rank-1 then DURATION end )  dsum30, "+
					"                    sum(case when DT_RANK >= 3 and DT_RANK <= top_rank-1 then DURATION end )  dsum29, "+
					"                    sum(case when DT_RANK >= 4 and DT_RANK <= top_rank-1 then DURATION end )  dsum28, "+
					"                    sum(case when DT_RANK >= 5 and DT_RANK <= top_rank-1 then DURATION end )  dsum27, "+
					"                    sum(case when DT_RANK <= 3 AND DT_RANK <= 2 then DURATION end )  dsum2930, "+
					"                    sum(case when DT_RANK = 2 then DURATION end )    d30, "+
					"                    sum(case when DT_RANK = 3 then DURATION end )    d29, "+
					"                    sum(case when DT_RANK = 4 then DURATION end )    d28, "+
					"                    sum(case when DT_RANK = 5 then DURATION end )    d27, "+
					"                    sum(case when DT_RANK = top_rank-1 then DURATION end ) d1, "+
					"                    sum(case when DT_RANK = top_rank then DURATION end ) d0, "+
					"                    min(panel_cnt-1) panel_cnt, "+
					"                    min(decode(sign(top_rank-32),-1, top_rank, 32))  top_rank, "+
					"                    min(decode(sign(top_rank-32),-1, top_rank-2, 30)) top_rank2, "+
					"                    sum(duration)  dsum_all, "+
					"                    stddev(DURATION) sd_duration, "+
					"                    avg(DURATION) avg_duration "+
					"             from ( "+
					"                    SELECT ACCESS_DAY, SMART_ID, PANEL_ID, DURATION, DT_RANK, PANEL_CNT, "+
					"                           DECODE(SIGN(PANEL_CNT-30),-1,7, TOP_RANK) TOP_RANK "+
					"                    FROM ( "+
					"                         SELECT ACCESS_DAY, SMART_ID, PANEL_ID, DURATION,  "+
					"                                RANK() OVER ( PARTITION BY ACCESS_DAY, SMART_ID ORDER BY DURATION   DESC, ROWNUM ) DT_RANK, "+
					"                                SUM(1) OVER ( PARTITION BY ACCESS_DAY, SMART_ID)  PANEL_CNT, "+
					"                                ROUND(SUM(1) OVER ( PARTITION BY ACCESS_DAY, SMART_ID)/4)  TOP_RANK "+
					"                         FROM  "+
                    "							( "+
                    "       					   SELECT   access_day, smart_id, package_name, panel_id, sum(duration) duration "+
                    "        					   FROM     tb_smart_daytime_app_wgt  a  "+
                    "       					   WHERE ACCESS_DAY = '"+filtername+"' "+
                    " 						       GROUP BY access_day, smart_id, package_name, panel_id "+                    
                    "                           ) "+
					"                         ) "+
					"             ) "+
					"             group by access_day, SMART_ID "+
					"             having min(panel_cnt) >= 5 "+
					"     ) "+
					") "+
					"where duration_new_fvalue > panel_new_fvalue "+
					"and   duration_NEW_ADJUSTED_VALUE < d30 "+
					"and   d30>2*d29 "+
					"and   d30-d29>2*(d29-d28) "+
					"and   d30 >30 ";
			dbcon.executeQueryExecute(query);
			
			//Update 2nd Rank
			query = "update tb_smart_day_app_new_wgt a  "+
					"set (NEW_DURATION, FLAG) = (   "+
					"    select DURATION_ADJUST_VALUE, 'Y' FLAG "+
					"    from   tb_smart_day_extreme_app_fact "+
					"    where  a.access_day = access_day "+
					"    and    a.smart_id = smart_id "+
					"    and    a.panel_id = panel_id "+
					") "+
					"where access_day = '"+filtername+"' ";
			dbcon.executeQueryExecute(query);
			
			query = 
					"update  tb_smart_day_app_new_wgt "+
					"set     NEW_DURATION = DURATION, FLAG = 'N' "+
					"where   NEW_DURATION is null "+
					"and     access_day = '"+filtername+"' ";
			dbcon.executeQueryExecute(query);
			
			//1st Rank Calculation
			query = "insert into tb_smart_day_extreme_app_fact "+
					"select access_day, smart_id, panel_id, d30, d29, d28, d1, dsum28, dsum_all,  "+
					"       duration_new_fvalue, '', '', panel_cnt, "+
					"       panel_new_fvalue, duration_new_adjusted_value "+
					"from "+
					"( "+
					"     select access_day, smart_id, "+
					"            d30, d29, d28, d27, d1, d0, dsum28, dsum29, dsum27, dsum2930, dsum30, dsum_all, top_rank2, "+
					"            round((dsum28+2*d28-30*d1)/(top_rank-3),2) pa, "+
					"            decode((dsum28+2*d28-30*d1)/(top_rank-3),0,0 ,decode((dsum28+2*d28-top_rank*d1),0,0, decode(sign(d30-101),-1,0, "+
					"            round((d30-d29)/((dsum28+2*d28-top_rank*d1)/(top_rank-3)),2)))) duration_fvalue, "+
					"            panel_cnt, "+
					"            panel_id, "+
					"            sd_duration, "+
					"            avg_duration, "+
					"            decode(sign(panel_cnt-31),-1,kclick.fn_fvalue3(top_rank2),kclick.fn_fvalue2(top_rank2)) panel_new_fvalue, "+
					"            round(((((d30-1*d0)/decode((d29-d0),0,0.0001,(d29-d0)))-1)/1)/((((dsum28-(top_rank2-2)*d0)/decode((d29-d0),0,0.0001,(d29-d0)))+1+1)/(top_rank-1)),2) duration_new_fvalue, "+
					"            round(d29+decode(sign(panel_cnt-31),-1,kclick.fn_fvalue3(top_rank2),kclick.fn_fvalue2(top_rank2))*(((dsum29-(top_rank2-1)*d0)+1*(d29-d0))/(top_rank2-1))) duration_new_adjusted_value "+
					"     from "+
					"     ( "+
					"             select access_day, smart_id, "+
					"                    min(case when dt_rank = 1 then panel_id end ) panel_id, "+
					"                    sum(case when dt_rank >= 1 and dt_rank <= top_rank2 then new_duration end )  dsum30, "+
					"                    sum(case when dt_rank >= 2 and dt_rank <= top_rank2 then new_duration end )  dsum29, "+
					"                    sum(case when dt_rank >= 3 and dt_rank <= top_rank2 then new_duration end )  dsum28, "+
					"                    sum(case when dt_rank >= 4 and dt_rank <= top_rank2 then new_duration end )  dsum27, "+
					"                    sum(case when dt_rank <= 2 then new_duration end )  dsum2930, "+
					"                    sum(case when dt_rank = 1 then new_duration end )    d30, "+
					"                    sum(case when dt_rank = 2 then new_duration end )    d29, "+
					"                    sum(case when dt_rank = 3 then new_duration end )    d28, "+
					"                    sum(case when dt_rank = 4 then new_duration end )    d27, "+
					"                    sum(case when dt_rank = top_rank2 then new_duration end ) d1, "+
					"                    sum(case when dt_rank = top_rank2+1 then new_duration end ) d0, "+
					"                    min(panel_cnt) panel_cnt, "+
					"                    min(decode(sign(top_rank2-30),-1, top_rank2+1, 31))  top_rank, "+
					"                    min(decode(sign(top_rank2-30),-1, top_rank2, 30)) top_rank2, "+
					"                    sum(new_duration)    dsum_all, "+
					"                    stddev(new_duration) sd_duration, "+
					"                    avg(new_duration) avg_duration "+
					"             from        "+
					"             ( "+
					"                    select access_day, smart_id, panel_id, new_duration, dt_rank, panel_cnt, "+
					"                           decode(sign(panel_cnt-31),-1,5,top_rank2) top_rank2 "+
					"                    from  "+
					"                        ( "+
					"                         select access_day, smart_id, panel_id, new_duration,  "+
					"                                rank() over ( partition by access_day, smart_id order by new_duration desc, rownum ) dt_rank, "+
					"                                sum(1) over ( partition by access_day, smart_id)  panel_cnt, "+
					"                                round(sum(1) over (partition by access_day, smart_id)/4)  top_rank2 "+
					"                         from  tb_smart_day_app_new_wgt "+
					"                         where access_day = '"+filtername+"' "+
					"                    ) "+
					"             ) "+
					"             group by access_day, smart_id "+
					"             having min(panel_cnt) >= 6 "+
					"     ) "+
					") "+
					"where duration_new_fvalue > panel_new_fvalue "+
					"and   duration_NEW_ADJUSTED_VALUE < d30 "+
					"and   d30>2*d29 "+
					"and   d30-d29>2*(d29-d28) "+
					"and   d30 >30 ";
			dbcon.executeQueryExecute(query);
			
			//Panel# less then 5 more than 2 Calculation
			query = "insert into tb_smart_day_extreme_app_fact "+
					"( access_day, smart_id, panel_id, d30, duration_adjust_value) "+
					"select access_day, smart_id, panel_id, d30, duration_adjust_value  "+
					"from  ( "+
					"    select  access_day, smart_id, panel_id, "+
					"            duration, round(exp(duration_adjust_value)+0.5) duration_adjust_value, sum(case when duration > round(exp(duration_adjust_value)+0.5) then 1  end) over ( partition by access_day, smart_id) M_count,  "+
					"            duration-round(exp(duration_adjust_value)+0.5), d30, d29, d28, panel_cnt "+
					"    from  ( "+
					"        select  access_day, smart_id, panel_id,  "+
					"                round(exp(duration)) duration, d30, d29, d28, panel_cnt, "+
					"                case when m_value1 >1.5 then 1.5*decode(s,0,1.0001,s)+t0 else null end duration_adjust_value "+
					"        from   ( "+
					"            select  /*+ use_hash(a,b) */ "+ 
					"                    a.access_day access_day, a.smart_id, a.panel_id, S, t0, t1, d30, d29, d28, panel_cnt, "+
					"                    round((duration-t0)/decode(s,0,0.0001,s), 2) M_value1, duration, "+
					"                    round((duration-t1)/decode(s,0,0.0001,s), 2) M_value2 "+
					"            from    (  "+
					"                select  a.access_day access_day, a.smart_id, panel_id, "+
					"                        1.483*decode(mod(panel_cnt,2), 1, duration_med1, (duration_med1+duration_med2)/2) S,  "+
					"                        duration_med t0, t1, d30, d29, d28, panel_cnt "+
					"                from    ( "+
					"                    select  access_day, smart_id, "+
					"                            min(panel_cnt1) panel_cnt, "+
					"                            min(case when duration_rank = round(panel_cnt1/2) then duration1 end) duration_med1, "+
					"                            min(case when duration_rank = round(panel_cnt1/2)+1 then duration1 end) duration_med2, "+
					"                            min(duration_med) duration_med, "+
					"                            min(t1) t1, "+
					"                            min(d30) d30, "+
					"                            min(d29) d29, "+
					"                            min(d28) d28 "+
					"                    from    ( "+
					"                        select  a.access_day access_day, "+
					"                                a.smart_id,  "+
					"                                panel_id,             "+
					"                                abs(duration-t1) duration1, "+
					"                                abs(duration-duration_med) duration2, "+
					"                                duration_med, t1, d30, d29, d28, "+
					"                                rank() over ( partition by a.access_day, a.smart_id order by abs(duration-t1) desc, rownum ) duration_rank, "+
					"                                sum(1) over ( partition by a.access_day, a.smart_id)  panel_cnt1 "+
					"                        from   ( "+
					"                            select access_day, smart_id, panel_id, ln(duration) duration        "+
					"                            from  kclick.tb_smart_day_app_new_wgt "+
					"                            where access_day = '"+filtername+"' "+
					"                        ) a, "+
					"                        ( "+
					"                            select  access_day, smart_id, "+
					"                                    decode(mod(panel_cnt,2), 1, duration_med1, (duration_med1+duration_med2)/2) duration_med, t1, d30, d29, d28 "+
					"                            from    ( "+
					"                                select  access_day,  "+
					"                                        smart_id, "+
					"                                        min(panel_cnt) panel_cnt, "+
					"                                        min(case when duration_rank = round(panel_cnt/2) then duration end) duration_med1, "+
					"                                        min(case when duration_rank = round(panel_cnt/2)+1 then duration end) duration_med2, "+
					"                                        avg(case when duration_rank >= panel_cnt*0.25 and duration_rank <= panel_cnt*0.75 then duration end )  t1, "+
					"                                        min(case when duration_rank = 1 then round(exp(duration)) end) d30, "+
					"                                        min(case when duration_rank = 2 then round(exp(duration)) end) d29, "+
					"                                        min(case when duration_rank = 3 then round(exp(duration)) end) d28 "+
					"                                from        "+
					"                                ( "+
					"                                    select  access_day, smart_id, panel_id, ln(duration) duration,  "+
					"                                            rank() over ( partition by access_day, smart_id order by duration   desc, rownum ) duration_rank, "+
					"                                            sum(1) over ( partition by access_day, smart_id)  panel_cnt "+
					"                                    from  kclick.tb_smart_day_app_new_wgt "+
					"                                    where access_day = '"+filtername+"' "+
					"                                ) "+
					"                                where panel_cnt<=5 "+
					"                                and   panel_cnt>=2 "+
					"                                group by access_day, smart_id "+
					"                            ) "+
					"                        ) b "+
					"                        where a.access_day=b.access_day "+
					"                        and   a.smart_id=b.smart_id "+
					"                        and   (d30>20 or d29>20) "+
					"                    ) "+
					"                    group by access_day, smart_id "+
					"                ) a, "+
					"                ( "+
					"                    select access_day, smart_id, panel_id, ln(duration) duration "+
					"                    from  kclick.tb_smart_day_app_new_wgt "+
					"                    where access_day = '"+filtername+"' "+
					"                ) b "+
					"                where a.access_day=b.access_day "+
					"                and   a.smart_id=b.smart_id "+
					"            ) a, "+
					"            ( "+
					"                select access_day, smart_id, panel_id, ln(duration) duration "+
					"                from  tb_smart_day_app_new_wgt "+
					"                where access_day = '"+filtername+"' "+
					"            ) b "+
					"            where a.access_day=b.access_day "+
					"            and   a.smart_id=b.smart_id "+
					"            and   a.panel_id=b.panel_id "+
					"        ) "+
					"    ) A "+
					") "+
					"where m_count <=2  "+
					"and   d30-d29 > 2*(d29-d28) "+
					"and   (d30 > 2*d29 or d29>2*d28) "+
					"and   0 < duration - duration_adjust_value ";
			dbcon.executeQueryExecute(query);
			
			//Update 1st&Rest
			query = "update tb_smart_day_app_new_wgt a  "+
					"set (NEW_DURATION, FLAG) = (   "+
					"    select DURATION_ADJUST_VALUE, 'Y' FLAG "+
					"    from   tb_smart_day_extreme_app_fact "+
					"    where  a.access_day = access_day "+
					"    and    a.smart_id = smart_id "+
					"    and    a.panel_id = panel_id "+
					") "+
					"where access_day = '"+filtername+"' ";
			dbcon.executeQueryExecute(query);
			
			query = "update  tb_smart_day_app_new_wgt "+
					"set     NEW_DURATION = DURATION, FLAG = 'N' "+
					"where   NEW_DURATION is null "+
					"and     access_day = '"+filtername+"' ";
			dbcon.executeQueryExecute(query);
			
			//divide duration by time
			query = "insert into tb_smart_daytime_app_new_wgt "+
					"select /*+ use_hash(a,b) index(a,pk_smart_daytime_app_wgt)*/ "+
					"       a.ACCESS_DAY, TIME_CD, a.SMART_ID, a.PACKAGE_NAME, a.PANEL_ID, a.DURATION,  "+
					"       case when flag = 'Y' and a.duration > 1 then round(NEW_DURATION*(a.DURATION/b.DURATION)) "+
					"       else a.duration end as new_duration, "+
					"       a.PROC_DATE, FLAG, app_cnt "+					
					"from   tb_smart_daytime_app_wgt a, tb_smart_day_app_new_wgt b "+
					"where  a.access_day = '"+filtername+"' "+
					"and    a.panel_id = b.panel_id "+
					"and    a.smart_id = b.smart_id ";
			dbcon.executeQueryExecute(query);
			
			//delete previous daytime wgt, pure down new daytime wgt
			query = "delete tb_smart_daytime_app_wgt where access_day = '"+filtername+"' ";
			dbcon.executeQueryExecute(query);
			
			query = "insert into tb_smart_daytime_app_wgt "+
					"select /*+ index(a,PK_SMART_DAYTIME_APP_NEW_WGT) */ ACCESS_DAY, TIME_CD, SMART_ID, PACKAGE_NAME, PANEL_ID, NEW_DURATION, PROC_DATE, app_cnt "+
					"from  tb_smart_daytime_app_new_wgt a "+
					"where access_day = '"+filtername+"' ";
			//dbcon.executeQueryExecute(query);
			eachPt = dbcon.executeQueryTime(query);
			
			
//20140122 DA.InsertDailyWifiFact();濡� �씠�룞 MSKIM
			/*wifistatus */		
//			query = "insert into TB_SMART_DAY_WIFI_NEW_WGT "+
//					"select a.ACCESS_DAY, WIFISTATUS, a.SMART_ID, a.PACKAGE_NAME, a.PANEL_ID, a.DURATION,  "+
//					"       case when flag = 'Y' and a.duration > 1 then round(NEW_DURATION*(a.DURATION/b.DURATION)) "+
//					"       else a.duration end as new_duration,  "+
//					"       a.PROC_DATE, "+
//					"       FLAG "+
//					"from   tb_smart_day_wifi_wgt a, tb_smart_day_app_new_wgt b "+
//					"where  a.access_day = '"+filtername+"' "+
//					"and    a.panel_id = b.panel_id "+
//					"and    a.smart_id = b.smart_id ";
//			dbcon.executeQueryExecute(query);			
			
			
			//delete previous daytime wgt, pure down new daytime wgt
//			query = "delete tb_smart_day_wifi_wgt where access_day = '"+filtername+"' ";
//			dbcon.executeQueryExecute(query);			
//			
//			
//			query = "insert into tb_smart_day_wifi_wgt "+
//					"select ACCESS_DAY, SMART_ID, PACKAGE_NAME, PANEL_ID, WIFISTATUS, NEW_DURATION, PROC_DATE "+
//					"from  TB_SMART_DAY_WIFI_NEW_WGT "+
//					"where access_day = '"+filtername+"' ";
//			
//			eachPt = dbcon.executeQueryTime(query);			
			
			System.out.println("DONE.");
			log.writeLog(dirlog,filtername+"'s Daily App Extreme took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertAppDaytimeETCFact(String factor){
		Calendar eachPt;
		try {
			String section = "";
			eachPt = dbcon.executeAppDaytimeFactETC(filtername, factor);
			log.writeLog(dirlog,filtername+"'s Daytime App "+section+"Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	

	
	public void InsertDivideDuration(){	//20131010 異붽� - mskim
		Calendar eachPt;
		try {
			eachPt = dbcon.executeAppDayDivideDuration(filtername);
			log.writeLog(dirlog,filtername+"'s Daily Divide Duration took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void InsertPushPanelProccess(){ //20140502 異붽� - kwshin
		Calendar eachPt;
		try {
			eachPt = dbcon.executePushPanelProccess(filtername);
			log.writeLog(dirlog,filtername+"'s PushPanel Proccess took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void InsertEqualAppDaytime(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeEqualAppDaytime(filtername);
			log.writeLog(dirlog,filtername+"'s Daily Equal AppDaytime took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}
	
	public void InsertDailyAppFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeAppFact(code, filtername);
			log.writeLog(dirlog,filtername+"'s Daily App Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDailyAppTOTFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeAppTOTFact(filtername);
			log.writeLog(dirlog,filtername+"'s Daily App Total Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertAppTotalFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeTotDaytimeFact(filtername);
			log.writeLog(dirlog,filtername+"'s App Total Fact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	
	public void InsertDayAppNavi(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeDayAppNavi(filtername);
			log.writeLog(dirlog,filtername+"'s DayAppNaivFact Insertion took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void InsertDayAppSum(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeDayAppSum(filtername);
			log.writeLog(dirlog,filtername+"'s DayAppSum Insertion took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void UpdateDeletePanel(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeDeletePanelUpdate(filtername);
			log.writeLog(dirlog,filtername+"'s Daily UpdateDeletePanel took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InserDsMobileData(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeDsMobileFact(filtername);
			log.writeLog(dirlog,filtername+"'s Daily InsertDsMobileFact took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertNscreenVodDailyFact(){
		Calendar eachPt = null;
		System.out.print("The batch - Day N-Screen Vod is processing...");
		String query = "";
		try {
			eachPt = Calendar.getInstance();
			
			query = "insert into tbns_vod_day_app_fact@kc3screen "+
			         "select  access_day, start_time, a.panel_id, types, package_name, smart_id, app_name, "+ 
			         "        sum(duration) duration, sysdate SERVER_DATE, " +
			         "        '' program, '' use_time, null register_date " +
			         "from " +
			         "( " +
			         "    select  /*+parallel(a,4) use_hash(b,a)*/ "+ 
			         "            access_day, to_char(register_date,'hh24:mm') start_time, panel_id, "+ 
			         "            types, package_name, smart_id, app_name, duration " +
			         "    from    tb_smart_env_itrack a, tbns_vod_app_list@kc3screen b "+ 
			         "    where   access_day = '"+filtername+"' " +
			         "    and     a.item_value = package_name " +
			         "    and     exp_time > sysdate " +
			         "    and     duration > 300 " +
			         "    and     SUBJECT in ('APP', 'CHANNEL', 'MEDIA') " +
			         "    and     types = 1 " +
			         ")a, " +
			         "( " +
			         "    select panel_id "+ 
			         "    from tbns_month_panel_seg@kc3screen "+ 
			         "    where monthcode in (select max(monthcode) from tbns_month_panel_seg@kc3screen) "+
			         "	  and isvalid = 1 "+
			         ")b "+ 
			         "where a.panel_id = b.panel_id " +
			         "group by access_day, start_time, a.panel_id, types, PACKAGE_NAME, SMART_ID, APP_NAME ";
			dbcon.executeQueryExecute(query);
			
			query = "insert into tbns_vod_day_app_fact@kc3screen "+
			         "select  /*use_hash(c,b,a)*/ "+
			         "        access_day, '1' start_time, a.panel_id, types, package_name, smart_id, app_name, "+ 
			         "        sum(duration) duration, sysdate SERVER_DATE, " +
			         "        '' program, '' use_time, null REGISTER_DATE " +
			         "from " +
			         "( " +
			         "    select  /*+parallel(a,4) use_hash(b,a)*/ "+ 
			         "            access_day, to_char(register_date,'hh24:mm') start_time, panel_id, "+ 
			         "            types, package_name, smart_id, app_name, duration "+ 
			         "    from    tb_smart_env_itrack a, tbns_vod_app_list@kc3screen b "+ 
			         "    where   access_day = '"+filtername+"' "+ 
			         "    and     a.item_value = package_name "+ 
			         "    and     exp_time > sysdate "+ 
			         "    and     duration > 300 "+ 
			         "    and     SUBJECT in ('APP', 'CHANNEL', 'MEDIA') " +
			         "    and     types = 2 "+ 
			         ")a, "+ 
			         "( "+ 
			         "    select panel_id "+ 
			         "    from tbns_month_panel_seg@kc3screen "+ 
			         "    where monthcode in (select max(monthcode) from tbns_month_panel_seg@kc3screen) "+
			         "	  and isvalid = 1 "+ 
			         ")b, "+ 
			         "( "+ 
			         "    select distinct panel_id "+ 
			         "    from tbns_vod_day_app_fact@kc3screen "+ 
			         "    where access_Day = '"+filtername+"' "+ 
			         "    and types = 1 "+ 
			         ")c "+ 
			         "where   a.panel_id = b.panel_id "+ 
			         "and     a.panel_id = c.panel_id "+ 
			         "group by access_day, a.panel_id, types, PACKAGE_NAME, SMART_ID, APP_NAME";
			dbcon.executeQueryExecute(query);

			System.out.println("DONE.");
			log.writeLog(dirlog,filtername+"'s Daily N-Screen Vod took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	
	public void InsertPushVirtual(String table_name){
		Calendar eachPt;
		System.out.print("Inserting "+table_name+" Virtual has started...");
		try {
			eachPt = dbcon.executePushVirtual(filtername);
			log.writeLog(dirlog,filtername+"'s "+table_name+"virtual Insertion took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void insertTopTask(){
		Calendar eachPt;
		System.out.print("Inserting Top Task Info has started...");
		try {
			eachPt = dbcon.executeTopTask(filtername);
			log.writeLog(dirlog,filtername+"'s Top Task Info Insertion took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	

	
	public void insertNewDevices(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeNewDevice();
			log.writeLog(dirlog,filtername+"'s New Device Info Insertion took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteEnvFail(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeFailrawDelete(filtername);
			log.writeLog(dirlog,filtername+"'s New Device Info Insertion took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	


	public void updateEnvDuration(){
		//TimeGapBatch 占쎌뮇�삂
		Calendar eachPt = Calendar.getInstance();
		System.out.print("Env Duration Update has started...");
		
		
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
						int duration = TimeG.getDuration();
						int timegap = TimeG.getTimegap();
						int version = Integer.parseInt(TimeG.getVersion());
						
						//duration�쓽 湲곗�. 180珥덇� �꽆�뼱媛�硫� 30珥덈줈 �뱾�뼱媛꾨떎. 5踰꾩졏 �씠�긽�뿉�꽌�뒗 6�떆媛� �씠�긽 �씠�슜�떆, 60珥덈줈 媛꾩＜
						if(duration > 180 && version <= 4){
							//duration = 30;
							if(duration > 21600) duration = 60;
						} else if(version > 4) {
							if(duration > 21600) duration = 60;
						}
						//System.out.print("Rowid: "+rowid);
						//System.out.println(" || duration: "+duration);
						dbcon.executeEnvDurationUpdate(rowid, timegap, duration);
						i++;
						if(i%30000==0){
							dbcon.executeQuery("commit"); //30000踰덉뿉 �븳踰� 而ㅻ컠
						}
					}
				}
				//留덉�留� 而ㅻ컠
				dbcon.executeQuery("commit");
				dbcon.setAutoCommit(true);
				//60珥덈쭏�떎 task瑜� �뾽�뜲�씠�듃 �떆耳쒖��떎. flag = 0, 裕ㅼ쭅 �빋留�
				dbcon.executeUpdate_flag6_duration(filtername); //濡ㅻ━�뙘�쑝濡� �씤�븳 flag = 6�뿉 ���븳 Duration Update by kwshin - 20150204
    			dbcon.executeTaskDurationUpdate(filtername); //�쓬�븙 �뼱�뵆 以묐났 �떆媛꾩쓣 duration = 0 �쑝濡� �뾽�뜲�씠�듃 by kwshin - 20140303
				System.out.println("Duration Update: "+filtername+" is done; rows are "+i);
				log.writeLog(dirlog,filtername+"'s Daily Duration Update took: "+findTimeDif(eachPt)+"s ; updated rows are "+i);
				//System.out.println("Duration.");
			} catch(Exception e){
				e.printStackTrace();
				log.writeLog(dirlog,filtername+"'s Daily Duration ERROR");
				System.exit(0);
			} finally {
				try {
			    	dbcon.partclose();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	public void updateEnvByte(){
		Calendar eachPt = Calendar.getInstance();
		System.out.print("Env Rxbyte Update has started...");

		ByteGapCollection bytegapcollection = new ByteGapCollection(dbcon, filtername);
		Collection bytegaps = new ArrayList<ByteGapModel>();
		bytegaps  = bytegapcollection.access();
		
		if(bytegaps  != null){
			Iterator gap = bytegaps .iterator();
			try {
				int i = 1;
				dbcon.setAutoCommit(false);
				while(gap.hasNext()) {
					ByteGapModel ByteG=(ByteGapModel)gap.next();
					if(ByteG.getRowid()!=""){
						String rowid = ByteG.getRowid();
						long rxbytegap = ByteG.getRxbytegap();
						long txbytegap = ByteG.getTxbytegap();
						String version = ByteG.getVersion();
						
						//System.out.println("Rowid: "+rowid+" || RxByteGap: "+rxbytegap+" || TxByteGap: "+txbytegap);
						dbcon.executeEnvRxbyteUpdate(rowid, rxbytegap, txbytegap);
						i++;
						if(i%30000==0){
							dbcon.executeQuery("commit"); //30000踰덉뿉 �븳踰� 而ㅻ컠
						}
					}
				}
				//留덉�留� 而ㅻ컠
				dbcon.executeQuery("commit");
				dbcon.setAutoCommit(true);
				//System.out.println("ByteGap Update: "+filtername+" is done; rows are "+i);
				log.writeLog(dirlog,filtername+"'s Daily ByteGap Update took: "+findTimeDif(eachPt)+"s ; updated rows are "+i);
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
	
	public void insertNewApps(){
		Calendar eachPt = Calendar.getInstance();
		System.out.print("App Insertion has started...");
		AppCollection appcollection = new AppCollection(dbcon, filtername);
		int cnt = 0;
		Collection newapps = new ArrayList<AppModel>();
		newapps = appcollection.access();
		
		if(newapps != null){
			Iterator newapp = newapps.iterator();
			try {
				while(newapp.hasNext()) {
					AppModel appmodel=(AppModel)newapp.next();
					
					int smartid = appmodel.getSmartid();
					String packagename = appmodel.getPackagename();
					String appname = appmodel.getAppname();
					boolean primary = appmodel.getPrimary();
					
					if(primary == true){
						//System.out.println(smartid+"||"+packagename+"||"+appname+"||"+primary);
						dbcon.executeAppInsert(appmodel);
						dbcon.executeAppnameInsert(appmodel);
						cnt++;
					} else {
						//System.out.println(smartid+"||"+packagename+"||"+appname+"||"+primary);
						dbcon.executeAppnameInsert(appmodel);
					}
				}
			// : �젣�쇅泥섎━
			dbcon.executeAppDelete();
			System.out.println("New App Insertion "+filtername+" is done; newly added apps are "+cnt);
			log.writeLog(dirlog,filtername+"'s new App Insertion took: "+findTimeDif(eachPt)+"s ; newly added apps are "+cnt);
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
	
	public void findAppProvider(){
		Calendar eachPt = Calendar.getInstance();
		System.out.print("App Search has started...");
		//�떆�옉�븯湲� �쟾�뿉 package_name蹂꾨줈 �뾽�뜲�씠�듃 �떎�떆.
		AppParser appcollection = new AppParser(dbcon, filtername);
		int cnt = 0;
		Collection newapps = new ArrayList<AppModel>();
		newapps = appcollection.access();
		
		if(newapps != null){
			Iterator newapp = newapps.iterator();
			try {
				while(newapp.hasNext()) {
					AppModel appmodel=(AppModel)newapp.next();
					
					int smartid = appmodel.getSmartid();
					String packagename = appmodel.getPackagename();
					String site = appmodel.getSite();
					String provider = appmodel.getProvider();
					String app_type = appmodel.getApp_type();
					String title = appmodel.getTitle();
					int installs = appmodel.getInstalls();
					//System.out.println("Insert:"+title+"||"+smartid+"||"+packagename+"||"+site+"||"+provider+"||"+app_type);
					dbcon.executeProviderInsert(appmodel);
					cnt ++;
				}
			} catch(Exception e){
				e.printStackTrace();
			} finally {
				System.out.println("DONE.");
				log.writeLog(dirlog,filtername+"'s new App Search took: "+findTimeDif(eachPt)+"s ; newly searched apps are "+cnt);
				try {
			    	dbcon.partclose();
				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
		}
	}
	
	public void InsertDailyAppByte(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeAppByte(filtername);
			log.writeLog(dirlog,filtername+"'s Daily App Byte took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertDailyWifiFact(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executeWifi(filtername);
			log.writeLog(dirlog,filtername+"'s Daily App Byte took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
