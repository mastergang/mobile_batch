package log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Calendar;

public class SmsSender {
	static String receiver;
	static String receiver1;
	
	public SmsSender(String receiver, String receiver1){
		this.receiver = receiver;
		this.receiver1 = receiver1;
	}
	
//	public static String getServiceDate(){
//		Calendar curr = Calendar.getInstance();
//		Calendar dday = Calendar.getInstance();
//		dday.set(2013, 5, 14);
//		long timing = (dday.getTimeInMillis() - curr.getTimeInMillis())/1000/60/60/24;
//		String daysleft = String.valueOf(timing);
//		
//		return daysleft;
//	}
	
	public static void Send(String content) {
		URL sms = null;
		URL sms1 = null;
		try {
//			if(receiver.equals("01033425341")) {
//				content+="_D-"+getServiceDate();
//			}
			System.out.println("The final message has been sent to "+receiver+" with the contents of "+content);
			sms = new URL("http://219.240.39.110:1080/sms/sendSMS.php?from=0221220180&msg="+content+"&to="+receiver);
			BufferedReader in = new BufferedReader(new InputStreamReader(sms.openStream()));
			
			sms1 = new URL("http://219.240.39.110:1080/sms/sendSMS.php?from=0221220180&msg="+content+"&to="+receiver1);
			BufferedReader in1 = new BufferedReader(new InputStreamReader(sms1.openStream()));

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("The final message has been sent to "+receiver1+" with the contents of "+content);
		}
	}
}
