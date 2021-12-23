package smartBatch.web;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;

import smartBatch.web.collection.QueryCollection;
import smartBatch.web.collection.TimeGapCollection;
import smartBatch.web.model.QueryModel;
import smartBatch.web.model.TimeGapModel;
import log.WriteMsgLog;
import DB.DBConnection;

public class WeeklyWebBatch {
	DBConnection dbcon;
	WriteMsgLog log = new WriteMsgLog();
	String filtername;
	String code = "M";
	String dirlog;
	String weekcode;
	
	public WeeklyWebBatch(DBConnection dbcon, String filtername, String dirlog){
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
	
	public static boolean isSunday(String accessday){
		Date date = null;
		boolean result = false;
		try{
			date = new SimpleDateFormat("yyyyMMdd").parse(accessday);
		} catch (ParseException e) {
			System.out.println("Not date format.");
		}
		if(date.toString().substring(0,3).equals("Sun")){
			result = true;
		}
		return result;
	}
	
	
	public static long findTimeDif(Calendar startPt){
		long timeDif=0;
		Calendar later = Calendar.getInstance();
		
		timeDif = (later.getTimeInMillis()-startPt.getTimeInMillis())/1000;
		return timeDif;
	}

	public void InsertWeeklyPanel(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekPanel(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Panel Seg Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeeklyFact(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeFact("w", filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSiteSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSiteSum(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void UpdateWeekkBounceRate(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekBounceRate(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Bounce Rate Update took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void InsertWeekPersonSeg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekPersonSeg(filtername, weekcode);
			log.writeLog(dirlog,filtername+"'s Weekly Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSiteSumError(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSiteSumError(weekcode);
			log.writeLog(dirlog,filtername+"'s Weekly Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	

	
	public void InsertWeeklyDomainFact(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekDomainFact(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Domain Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeeklyDomainSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekDomainSum(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Domain Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeeklyLoyalty(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeeklyLoyalty(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Domain Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSiteSwitch(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSiteSwitch(filtername);
			log.writeLog(dirlog,filtername+"'s Week Site Switch Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekDurationTimeSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekDurationTimeSum(filtername);
			log.writeLog(dirlog,filtername+"'s Week Site Duration Time Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertSectionPath(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeSectionPath("w", filtername);
			log.writeLog(dirlog,filtername+"'s Section Path Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSectionTemp(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSectionTemp(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Section Temp Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSectionURL(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSectionURL(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Section URL Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSectionFact(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSectionFact(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Section Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekCsectionFact(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekCsectionFact(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Csection Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekKeywordFact(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekKeywordFact(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Keyword Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekQueryFact(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekQueryFact(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Query Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSiteSeg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSiteSeg(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Seg Site Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	};
	
	
	public void InsertWeeklyBoardCheck(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeeklyBoardCheck();
			log.writeLog(dirlog,filtername+"'s Weekly Seg Site Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	};
	
	public void InsertWeekDailySiteSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekDailySiteSum(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Level1 Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeeklevel1Sum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekLevel1(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Level1 Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeeklevel2Sum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekLevel2(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Level2 Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
	public void InsertWeeklevel1Seg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekLevel1Seg(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Level2 Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	
	public void InsertWeeklevel2Seg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekLevel2Seg(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Level2 Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void InsertWeekKeywordSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekKeyword(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Keyword Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSession(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSession(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Session Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSection(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSection(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Section Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekCsection(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekCsection(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Csection Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekSectionSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekSectionSum(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Section Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertWeekCsectionSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeWeekCsectionSum(filtername);
			log.writeLog(dirlog,filtername+"'s Weekly Csection Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void tempDelete(){
		try {
//			//WEB
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_FACT           WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_DOMAIN_FACT    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_1LEVEL_SUM     WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_2LEVEL_SUM     WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_DOMAIN_SUM     WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_PERSON_SEG     WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SEG_SITE       WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SESSION        WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SITE_SUM       WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_DAY_LOYALTY_SUM     WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SITE_SWITCH    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_TOTAL_WEEK_SESSION        WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			
//			//WEB SECTION
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_TEMP_SECTION   WHERE ACCESS_DAY BETWEEN TO_CHAR(TO_DATE('"+filtername+"','YYYYMMDD')-6,'YYYYMMDD') AND '"+filtername+"'");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SECTION_FACT   WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_CSECTION_FACT  WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_KEYWORD_FACT   WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_KEYWORD_SUM    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_QUERY_FACT     WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_CSECTION       WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_CSECTION_SUM   WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SECTION        WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SECTION_SUM    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SECTION_URL    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			
			//APP
			dbcon.executeQueryExecute("DELETE tb_smart_week_app_sum        WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_APP_SESSION    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_APPLVL1_SUM    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_APPLVL2_SUM    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE tb_smart_week_app_sum_site   WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE tb_smart_day_app_loyalty_sum WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE tb_smart_week_app_switch     WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			
//			//ENTER
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SERVICE_FACT   WHERE ACCESS_DAY BETWEEN TO_CHAR(TO_DATE('"+filtername+"','YYYYMMDD')-6,'YYYYMMDD') AND '"+filtername+"'");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SERVICE_SITE   WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SERVICE_SUM    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
//			
			//TOTAL
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SITEAPP_FACT   WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_SITEAPP_SUM    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_PCMOBILE_SUM   WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_TOTAPP_FACT    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_TOTAPP_SUM     WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_TOTSITEAPP_FACT WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_TOTSITEAPP_SUM WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_TOTSITE_FACT   WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("DELETE TB_SMART_WEEK_TOTSITE_SUM    WHERE WEEKCODE = FN_WEEKCODE('"+filtername+"')");
			dbcon.executeQueryExecute("delete TB_SMART_WEEK_TOTAL_FACT where access_day between to_char(to_date('"+filtername+"','yyyymmdd')-6,'yyyymmdd') and '"+filtername+"'");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
