package smartBatch.web;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;

import smartBatch.web.collection.QueryCollection;
import smartBatch.web.collection.TimeGapCollection;
import smartBatch.web.model.QueryModel;
import smartBatch.web.model.TimeGapModel;
import log.WriteMsgLog;
import DB.DBConnection;

public class MonthlyWebBatch {
	DBConnection dbcon;
	WriteMsgLog log = new WriteMsgLog();
	String filtername;
	String code = "M";
	String dirlog;
	String lastday;
	
	public MonthlyWebBatch(DBConnection dbcon, String filtername, String dirlog){
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

	public void InsertMonthlyPanel(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthPanel(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Panel Seg Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void insertMonthlyPanelSignal(){
		Calendar eachPt;
		try {
			eachPt = dbcon.executePanelSignal(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Panel Signal took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthlyFact(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeFact(code, filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthSiteSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthSiteSum(filtername, lastday);
			log.writeLog(dirlog,filtername+"'s Monthly Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void UpdateMonthkBounceRate(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthBounceRate(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Bounce Rate Update took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	public void InsertMonthPersonSeg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthPersonSeg(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Person Segment Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthSiteSumError(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthSiteSumError(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Summary Error Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthSegSiteSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthSegSite(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Seg Site Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthDailySiteSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthDailySiteSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Daily Site Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertMonth1lvlSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonth1lvlSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly level1 Site Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonth2lvlSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonth2lvlSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly level2 Site Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthLv1Seg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthLv1Seg(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly level1 Site Seg Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthLv2Seg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthLv2Seg(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly level2 Site Seg Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
		
	public void InsertMonthKeywordSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthKeywordSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Keyword Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthSession(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthSession(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Session Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthSection(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthSection(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Section Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthCSection(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthCSection(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly CSection Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthSectionSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthSectionSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Section Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertMonthAppSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Application Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppSeg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppSeg(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly App Seg Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthSectionURL(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthSectionURL(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly SectionURL Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthlyDomainFact(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthDomainFact(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Domain Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthlyDomainSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthDomainSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Domain Summary Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthLoyalty(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthLoyalty(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Loyalty Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthSiteSwitch(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthSiteSwitch(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Site Switch Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertMonthDurationTimeSum(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthSiteDurationTimeSum(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Site Duration Time Sum Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppSession(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppSession(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Application Session Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppLVL1(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppLVL1(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Application Level1 Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppLVL2(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppLVL2(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Application Level2 Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppLv1Seg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppLv1Seg(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Application Level1 Seg Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthAppLv2Seg(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppLv2Seg(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Application Level2 Seg Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	public void InsertMonthAppSumSite(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthAppSumSite(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Application SumSite Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthEnterService(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthEnterService(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Entertainment Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthTotal(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthTotal(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Total Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthWebAppSession(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthWebAppSession(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly WebAppSession Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}	
	
	
	public void InsertMonthTotalSession(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthTotalSession(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Total Session Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertMonthNotice(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthNotice(filtername);
			log.writeLog(dirlog,filtername+"'s Monthly Notice Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void InsertMonthlyBoardCheck(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeMonthlyBoardCheck();
			log.writeLog(dirlog,filtername+"'s Monthly board check Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertSectionPath(){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeSectionPath(code, filtername);
			log.writeLog(dirlog,filtername+"'s Section Path Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertSectionTemp(String mode){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeSectionTemp(mode, filtername);
			log.writeLog(dirlog,filtername+"'s Section Temp Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertSectionSiteFact(String mode, int factor){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeSectionSiteFact(mode,factor,filtername);
			log.writeLog(dirlog,filtername+"'s Section LVL1 Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertKeywordFact(String mode){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeKeywordFact(mode, filtername);
			log.writeLog(dirlog,filtername+"'s Keyword Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void InsertQueryFact(String mode){
		Calendar eachPt = Calendar.getInstance();
		try {
			eachPt = dbcon.executeQueryFact(mode, filtername);
			log.writeLog(dirlog,filtername+"'s Query Fact Insert took: "+findTimeDif(eachPt)+"s");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void tempDelete(){
		try {
//			//WEB FACT
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_FACT           WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_DOMAIN_FACT    WHERE MONTHCODE = '"+filtername+"' ");
//			//WEB SECTION FACT
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_TEMP_SECTION   WHERE ACCESS_DAY BETWEEN '"+filtername+"'||'01' AND fn_month_lastday('"+filtername+"') ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SECTION_FACT   WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_CSECTION_FACT  WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_KEYWORD_FACT   WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_QUERY_FACT     WHERE MONTHCODE = '"+filtername+"' ");
//			
//			//WEB SUM
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_1LEVEL_SUM     WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_2LEVEL_SUM     WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_DOMAIN_SUM     WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_PERSON_SEG     WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SEG_SITE       WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SESSION        WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SITE_SUM       WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SITE_SWITCH    WHERE MONTHCODE = '"+filtername+"' ");
//			
//			//WEB SECTION SUM
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_CSECTION       WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_CSECTION_SUM   WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SECTION        WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SECTION_SUM    WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SECTION_URL    WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_KEYWORD_SUM    WHERE MONTHCODE = '"+filtername+"' ");
//			
//			//APP WGT
//			dbcon.executeQueryExecute("DELETE tb_smart_month_app_wgt        WHERE MONTHCODE = '"+filtername+"' ");
//			dbcon.executeQueryExecute("DELETE tb_smart_month_setup_wgt      WHERE MONTHCODE = '"+filtername+"' ");
			//APP FACT
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_APP_FACT       WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SETUP_FACT     WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE tb_smart_month_applvl1_fact   WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE tb_smart_month_applvl2_fact   WHERE MONTHCODE = '"+filtername+"' ");
			//APP SUM
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_APP_SUM        WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_APP_SESSION    WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_APPLVL1_SUM    WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_APPLVL2_SUM    WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE tb_smart_month_app_sum_site   WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE tb_smart_month_app_switch     WHERE MONTHCODE = '"+filtername+"' ");
			
			//TOTAL
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SITEAPP_FACT   WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SITEAPP_SUM    WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_PCMOBILE_SUM   WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_TOTAPP_FACT    WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_TOTAPP_SUM     WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_TOTSITEAPP_FACT WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_TOTSITEAPP_SUM WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_TOTSITE_FACT   WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_TOTSITE_SUM    WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("delete TB_SMART_MONTH_TOTAL_FACT where ACCESS_DAY BETWEEN '"+filtername+"'||'01' AND fn_month_lastday('"+filtername+"') ");
			
			//ENTER
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SERVICE_FACT   WHERE ACCESS_DAY BETWEEN '"+filtername+"'||'01' AND fn_month_lastday('"+filtername+"') ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SERVICE_SITE   WHERE MONTHCODE = '"+filtername+"' ");
			dbcon.executeQueryExecute("DELETE TB_SMART_MONTH_SERVICE_SUM    WHERE MONTHCODE = '"+filtername+"' ");
			} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
