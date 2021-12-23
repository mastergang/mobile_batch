package smartBatch;

import java.sql.SQLException;
import java.util.Calendar;

import log.MailSender;
import log.SmsSender;
import log.WriteMsgLog;
import smartBatch.app.DailyAppBatch;
import smartBatch.app.MonthlyAppBatch;
import smartBatch.app.WeeklyAppBatch;
import smartBatch.web.DailyWebBatch;
import smartBatch.web.MonthlyWebBatch;
import smartBatch.web.WeeklyWebBatch;
 
public class LdrBatch {	
	//일간 D, 주간 W, 월간 M
	static String code = "";
	static String filtername = "";
	//일간 배치 : 가중치일 때는 "W"로, 마지막 FACT일 때는 "V"로.
	//주월간 배치 : 웹일때는 MW 앱일때는 MA
	static String mode = "";
	static String dirlog = "";
	
	//web일때는 "W"로, app일 때는 "A"로.
	//static String gubun ="";
	
	/* main */
	public static void main(String[] args) throws SQLException, InterruptedException {
		mode = args[0].trim();
		code = args[1].trim();
		filtername = args[2].trim();
		//gubun = args[2].trim();
		long timeDif = 0;
		
		String os = System.getProperty("os.name").toLowerCase();
		if(os.contains("windows")){
			dirlog = "D:\\Mobile_batch/koreanclick-mobile-batch/logs";
			//dirlog = "C:\\Users/Dell/workspace/smartBatch/log/batch_";
		} else {
			dirlog = "/home/users/samjin/strackload/logs/batch_";
		}
		
		WriteMsgLog log = new WriteMsgLog();
		Calendar startPt = Calendar.getInstance();
		SmsSender sms = new SmsSender("01033105501","01088896295");

	
		String[] openReceivers  = {"product@koreanclick.com","cs@koreanclick.com","tech@koreanclick.com","bjchoi@koreanclick.com"};
		//도너 경고메일
		String[] donerReceivers = {"product@koreanclick.com","panel@koreanclick.com","tech@koreanclick.com","bjchoi@koreanclick.com"};
		MailSender mail = new MailSender(openReceivers,donerReceivers);
		DB.DBConnection dbcon = null;
		Calendar eachPt;

		
		if (!(mode.equalsIgnoreCase("v")||mode.equalsIgnoreCase("w")||mode.equalsIgnoreCase("ma")||mode.equalsIgnoreCase("mw")||mode.equalsIgnoreCase("r"))){
			System.out.println("False mode type.");
			System.exit(0);
		}
		if (code.length() != 1 && filtername.length() < 6 && filtername.length() > 8){
			System.out.println("False input type. Example input: 'W D 20110304'");
			System.exit(0);
		} else if (!code.equalsIgnoreCase("m") && !code.equalsIgnoreCase("d") && !code.equalsIgnoreCase("w")) {
    		System.out.println("False period input type. Example input: 'W D 20110304'");
    		System.exit(0);
	    } else {
	    	if(code.equalsIgnoreCase("m") && filtername.length() != 6){
	    		System.out.println("False monthcode input type. Example input: 'V M 201103'");
	    		System.exit(0);
	    	} else if (code.equalsIgnoreCase("d") && filtername.length() != 8){
	    		System.out.println("False accessday input type. Example input: 'W D 20110304'");
	    		System.exit(0);
	    	} else if (code.equalsIgnoreCase("w") && !WeeklyWebBatch.isSunday(filtername)){
	    		System.out.println("False Week accessday input type. The input date must be Sunday.");
	    		System.exit(0);
	    	}
	    }
	 	
		if(code.equalsIgnoreCase("w")){
			if(mode.equalsIgnoreCase("mw")) {
				System.out.println("System runs weekly WEB batch process and the enddate is "+filtername);
				dirlog = dirlog+filtername.substring(0, 6)+".log";
				log.writeLog(dirlog,"-- "+filtername+" Weekly Batch Process : "+startPt.getTime()+" --");
			} else if(mode.equalsIgnoreCase("ma")) {
				System.out.println("System runs weekly APP batch process and the enddate is "+filtername);
				dirlog = dirlog+filtername.substring(0, 6)+".log";
				log.writeLog(dirlog,"-- "+filtername+" Weekly Batch Process : "+startPt.getTime()+" --");
			} else {
				System.out.println("Wrong input type");
				System.exit(0);
			}
		} else if(code.equalsIgnoreCase("m")){
			if(mode.equalsIgnoreCase("mw")) {
				System.out.println("System runs monthly WEB batch process and the enddate is "+filtername);
				dirlog = dirlog+filtername.substring(0, 6)+".log";
				log.writeLog(dirlog,"-- "+filtername+" Weekly Batch Process : "+startPt.getTime()+" --");
			} else if(mode.equalsIgnoreCase("ma")) {
				System.out.println("System runs monthly APP batch process and the enddate is "+filtername);
				dirlog = dirlog+filtername.substring(0, 6)+".log";
				log.writeLog(dirlog,"-- "+filtername+" Weekly Batch Process : "+startPt.getTime()+" --");
			}else {
				System.out.println("Wrong input type");
				System.exit(0);
			}
		} else {
			if (mode.equalsIgnoreCase("w")){
				System.out.println("System runs daily batch process and the access_day is "+filtername);
				dirlog = dirlog+filtername.substring(0, 6)+"_weight.log";
				log.writeLog(dirlog,"-- "+filtername+" Daily Batch Process : "+startPt.getTime()+" --");
			} else if(mode.equalsIgnoreCase("v")) {
				System.out.println("System runs daily batch process and the access_day is "+filtername);
				dirlog = dirlog+filtername.substring(0, 6)+".log";
				log.writeLog(dirlog,"-- "+filtername+" Daily Batch Process : "+startPt.getTime()+" --");
			} else if(mode.equalsIgnoreCase("r")) {
				System.out.println("System runs daily batch process and the access_day is "+filtername);
				dirlog = dirlog+filtername.substring(0, 6)+".log";
				log.writeLog(dirlog,"-- "+filtername+" Daily Batch Process : "+startPt.getTime()+" --");
			}
			else {
				System.out.println("Wrong input type");
				System.exit(0);
			}
		}
		
		try {
			dbcon = new DB.DBConnection();
			if(dbcon.openConnection()) {
				//daily batch process
				if(code.equalsIgnoreCase("d")){
					//Web
					DailyWebBatch DB = new DailyWebBatch(dbcon, filtername, dirlog);
					DailyAppBatch DA = new DailyAppBatch(dbcon, filtername, dirlog);

					if(mode.equalsIgnoreCase("w")){ //가중치 - 즉, 버츄얼 패널이 들어가기 전에 FACT가 들어가기 전에는 동일하므로 진행한다.
					  //DB.insertDailyPanelSignal(); 
						DB.insertDailyNewSite();
						//DB.updateBanURL(); //20200511 ban_url통합으로 인해 사용X by sjhyun 
						//DB.updateSiteID();
						//DB.chromeSbrowserResultCD();
						//DB.updateDuration();
						//DB.updateResultCD(); 
						//DB.updateQueryDecode();		
					} else if(mode.equalsIgnoreCase("v")) { //버츄얼 패널이 들어갈 경우에만 FACT를 생성한다. 가중치는 RAW DATA만 가지고 있는다.
						//web traffic remove --이동 20140401
						//DB.updateBrowserValid();
						//DB.updateExtreme(1); 
						
						//DB.InsertBrowserVirtual();
						//DB.updateExtreme(2);
						//DB.updateExtreme(3);
						//DB.InsertDaytimeVisit();
						//DB.InsertRawDaytime();
						//DB.InsertDaytimeFact();
						//DB.InsertDailyFact();
						//DB.InsertDaySiteSum();		
						//DB.InsertDayBounceRate();
					//	DB.InsertTarItrack(); //20160526 jykim : TAR 프로젝트 data insert
					} else if(mode.equalsIgnoreCase("r")) {
						
						DB.updateExtreme(2);
						DB.updateExtreme(3);
						DB.InsertDaytimeVisit();
						DB.InsertRawDaytime();
						DB.InsertDaytimeFact();
						DB.InsertDailyFact();
						DB.InsertDaySiteSum();		
						DB.InsertDayBounceRate();
					}
					
					//App
					if(mode.equalsIgnoreCase("w")){
						DA.insertNewApps();
						DA.insertNewDevices();
						DA.deleteEnvFail(); //20180917 트랙버전 듀레이션 계산 무자인식으로 인한 에러로 관련 로우 삭제 
						DA.updateEnvDuration();
						DA.InsertAppDaytimeFact();
						//DA.InsertDailyWifiFact(); //20140122 DA.updateEnvByte() 뒤로 이동 MSKIM
						if(Integer.parseInt(filtername) >= 20130101){
							DA.updateAppExtreme(); //2013년 1월 1일부터 앱극단값 적용
						}
						DA.InsertDivideDuration();	//20131010 추가 MSKIM//
						DA.InsertPushPanelProccess(); //20140502 추가 KWSHIN//
						DA.InsertEqualAppDaytime(); //20150205 추가 KWSHIN//
						DA.InsertDailyAppFact();
						//DA.InsertAppDaytimeETCFact("TOT"); //20150916 주석
						DA.InsertDailySetupFact();
						DA.InsertDeviceFact();		//20140430 추가 MSKIM
						
						//Server Update
						DB.updateServer();
						
						//Byte Update
						DA.updateEnvByte();
					//	DA.InsertDailyWifiFact(); //20140122 rxbyte_gap, txbyte_gap 추가
					//	DA.InsertDailyAppByte();

					} else if(mode.equalsIgnoreCase("v")){
						DB.InsertDailyPanel();
						DA.InsertVirtual("tb_smart_daytime_app_wgt");
						DA.InsertVirtual("tb_smart_day_app_wgt");
						//DA.InsertVirtual("tb_smart_day_wifi_wgt");
						//DA.InsertVirtual("tb_smart_daytime_apptot_wgt"); //20150916 주석
						DA.InsertVirtual("tb_smart_day_device_wgt");	//20140430 추가 MSKIM
						DA.InsertPushVirtual("tb_smart_day_push_panel_fact"); //20140603 푸쉬패널 퍀트 생성 로직 추가  by kwshin
						DA.InsertDailyAppTOTFact();
						if(dbcon.donerTest(filtername)){ //도너의 퍼센티지가 90%미만일때 경고메일 발송
							//mail.sendByNetAll(filtername);
						}
						DA.InsertVirtual("tb_smart_day_setup_wgt");
						DA.InsertDayAppNavi();
						DA.InsertDayAppSum();
						DA.UpdateDeletePanel(); //장기 미활동자 패널 데이터 분리 보관 또는 삭제 처리
						DA.InserDsMobileData(); //DS 사전검수 모바일 데이터 팩트  by kwshin 18.10.19
						//DA.InsertNscreenVodDailyFact();	//20141007 추가 mskim / 20150108 스마트미디어 측정 종료 saraim
						//Etc
						//DA.findAppProvider(); 
					}
				}
				//weekly batch process
				else if(code.equalsIgnoreCase("w")){
					WeeklyWebBatch WB = new WeeklyWebBatch(dbcon, filtername, dirlog);
					WeeklyAppBatch WA = new WeeklyAppBatch(dbcon, filtername, dirlog);
					//WB.tempDelete();

					if(mode.equalsIgnoreCase("ma")) {
						WB.InsertWeeklyPanel();			
						
						//fact-app
						WA.InsertWeekAppFact();
						WA.InsertWeekSetupFact();
						WA.InsertWeekAppLVL1Fact();
						WA.InsertWeekAppLVL2Fact();
						//WA.InsertWeekWifiFact();	//20140122 rxbyte_gap, txbyte_gap가 추가 되어 수정
												
						WA.InsertWeekAppSum(); //20140603 푸쉬노출 비실행 이용자 지표 데이터 산출 로직 추가  by kwshin
						WA.InsertWeekAppSeg();
						WA.InsertWeekAppSession();
						WB.InsertWeekPersonSeg();
						WA.InsertWeekAppSumError();
						WA.InsertWeekDaytimeAppSum();
						WA.InsertWeekDailyAppSum();
						WA.InsertWeekNextApp();
						WA.InsertWeekPreApp();
						WA.InsertWeekAppLvl1();
						WA.InsertWeekAppLvl2();
						WA.InsertWeekAppLv1Seg();
						WA.InsertWeekAppLv2Seg();
						WA.InsertWeekAppSiteSum();
						WA.InsertWeekAppLoyalty();
						WA.InsertWeekAppSwitch();
						WA.InsertWeekAppDurationTimeSum();
						WA.InsertDeviceFact();
						
						WA.InsertWeekTotal();
						//WA.InsertWeekWebAppSession();
						WA.InsertWeekTotalSession();
						
						//WB.InsertWeeklyBoardCheck(); 						
						//mail.sendAll(filtername,"week");
					}
					else if(mode.equalsIgnoreCase("mw")){
						
						/* 2020.11.02 mobile android web shutdown
						//section
						WB.InsertSectionPath();
						WB.InsertWeekSectionTemp();
						WB.InsertWeekSectionFact();
						WB.InsertWeekCsectionFact();
						WB.InsertWeekKeywordFact();
						WB.InsertWeekQueryFact();
						//fact-web
						WB.InsertWeeklyFact();
						WB.InsertWeeklyDomainFact();
						
						//summary
						WB.InsertWeekSiteSum();
						WB.UpdateWeekkBounceRate();						
						WB.InsertWeekSiteSumError();
						WB.InsertWeekDailySiteSum();
						WB.InsertWeeklevel1Sum();
						WB.InsertWeeklevel2Sum();
						WB.InsertWeeklevel1Seg();
						WB.InsertWeeklevel2Seg();
						WB.InsertWeekSession();
						WB.InsertWeekSection();
						WB.InsertWeekCsection();
						WB.InsertWeekSectionURL();
						WB.InsertWeekSectionSum();
						WB.InsertWeekCsectionSum();
						WB.InsertWeekKeywordSum();
						WB.InsertWeeklyDomainSum();
						WB.InsertWeeklyLoyalty();
						WB.InsertWeekSiteSwitch();
						WB.InsertWeekDurationTimeSum();
						*/
						//WA.InsertWeekEntertain(); //20191125 service사용X by sjhyun
						
						//WB.InsertWeekSiteSeg();
						//WA.InsertWeekNotice();
						//mail.sendAll(filtername,"week");
					}
				}
				//monthly batch process	
				else {
					MonthlyWebBatch MB = new MonthlyWebBatch(dbcon, filtername, dirlog);
					MonthlyAppBatch MA = new MonthlyAppBatch(dbcon, filtername, dirlog);

					/*
					if(mode.equalsIgnoreCase("w")){
						MB.InsertSectionPath();
						MB.InsertSectionTemp(mode);
						MA.InsertMonthSessionWgt();
						MA.InsertDeviceFact();
						
						MA.InsertMonthAppWgt();
						MA.InsertSetupFact();
					} */
					
					if(mode.equalsIgnoreCase("ma")){
						
						//w
						MA.InsertMonthAppWgt();
						MA.InsertSetupFact();
						
//						MB.insertMonthlyPanelSignal();
						MB.InsertMonthlyPanel();
						 
						//app
						MA.InsertMonthAppFact();
						MA.InsertMonthSetupFact();
						MA.InsertMonthAppLVL1Fact();
						MA.InsertMonthAppLVL2Fact();
						//MA.InsertMonthWifiFact();		//20140122 rxbyte_gap, txbyte_gap가 추가 되어 수정
						MA.InsertVirtual("tb_smart_month_device_wgt");
						
						
						//app_sum
						MB.InsertMonthAppSum(); //20140603 푸쉬노출 비실행 이용자 지표 데이터 산출 로직 추가  by kwshin
						MB.InsertMonthAppSeg(); 
						MB.InsertMonthAppSession();
						MB.InsertMonthPersonSeg();
						MA.InsertMonthAppSumError();
						MA.InsertMonthDaytimeAppSum();
						MA.InsertMonthDailyAppSum();
						MA.InsertMonthNextApp();
						MA.InsertMonthPreApp();
						MB.InsertMonthAppLVL1();
						MB.InsertMonthAppLVL2();
						MB.InsertMonthAppLv1Seg();
						MB.InsertMonthAppLv2Seg();
						MB.InsertMonthAppSumSite();
						MA.InsertMonthAppLoyalty();
                 		MA.InsertMonthAppSiteSwitch();
                 		MA.InsertMonthAppDurationTimeSum();
                 		
						/*Summary Test 추가부분.
						서머리 테이블의 추가 방법은:
							1. MonthlyWebBatch에 method 추가.
							2. DBConnection.java에 쿼리 추가.
							3. MB.method이름(); 추가.*/
                 		
                 		/* 2020.11.02 mobile android web shutdown
						//w
						MB.InsertSectionPath();
						MB.InsertSectionTemp("w");
						MA.InsertMonthSessionWgt();
						MA.InsertDeviceFact();
				     
						//section
						MB.InsertSectionTemp("v");
						MB.InsertSectionSiteFact("v",1);
						MB.InsertSectionSiteFact("v",2);
						MB.InsertKeywordFact("v");
						MB.InsertQueryFact("v");
						//web
						MB.InsertMonthlyFact();
			        	MB.InsertMonthlyDomainFact();
						
						//web_sum
						MB.InsertMonthSegSiteSum();
						MB.InsertMonthSiteSum();
						MB.UpdateMonthkBounceRate();						
						MB.InsertMonthSiteSumError();
						MB.InsertMonthDailySiteSum();
						MB.InsertMonth1lvlSum();
						MB.InsertMonth2lvlSum();
						MB.InsertMonthLv1Seg();
						MB.InsertMonthLv2Seg();
						MB.InsertMonthSession();
						MB.InsertMonthlyDomainSum();
						MB.InsertMonthLoyalty();
						MB.InsertMonthSiteSwitch();
						MB.InsertMonthDurationTimeSum();
						//web_section_sum
						MB.InsertMonthKeywordSum();
						MB.InsertMonthSection();
						MB.InsertMonthCSection();
						MB.InsertMonthSectionURL();
						MB.InsertMonthSectionSum();
						*/
			
						//MB.InsertMonthEnterService();//20191125 service사용X by sjhyun
						MB.InsertMonthTotal();
						//MB.InsertMonthWebAppSession(); 
						MB.InsertMonthTotalSession(); 
						
						//MB.InsertMonthNotice();
						
//						mail.sendAll(filtername,"month");
					}
				}
			}
			dbcon.close();
			//System.exit(0);
		} catch(Exception ex) {
	        ex.printStackTrace();
	        dbcon.close();
		} finally {
			System.out.println("DONE");
			Calendar endPt = Calendar.getInstance();
			timeDif = (endPt.getTimeInMillis()-startPt.getTimeInMillis())/1000;
			System.out.println("Batch Process has been ended. Took "+timeDif+"s --");
			log.writeLog(dirlog,"-- "+endPt.getTime()+": Batch Process has been ended. Took "+timeDif+"s --");
			log.writeLog(dirlog,"");
		}
		//sending sms to me; checking parameters and execution date.
		long timeMin = Math.round(timeDif/60);
		SmsSender.Send("Mobile_Manual_Batch_"+mode+"_"+code+"_"+filtername+"_is_DONE._Took_"+timeMin+"_mins");
	}
}
