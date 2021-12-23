package log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;

public class MailSender {
	static String[] openReceiver;
	static String[] donerAlert;
	
	public MailSender(String[] openReceiver,String[] donerAlert){
		this.openReceiver = openReceiver;
		this.donerAlert = donerAlert;
	}
	
	public static void sendAll(String accessday, String mode) {
		String[] receivers = openReceiver;
		for(int i = 0; i < receivers.length; i++) {
			SendOpenByNet(accessday, receivers[i], mode);
		}
	}
	
	public static void sendByNetAll(String accessday) {
		String[] receivers = donerAlert;
		for(int i = 0; i < receivers.length; i++) {
			SendByNet(accessday, receivers[i]);
		}
	}
	
	public static void Send(String title, String content, String receiver) {
		// Recipient's email ID needs to be mentioned.
	      String to = receiver;

	      // Sender's email ID needs to be mentioned
	      String from = "tech@koreanclick.com";

	      // Get system properties
	      Properties properties = System.getProperties();
	      properties.setProperty("mail.user", "");
	      properties.setProperty("mail.password", "");
	      properties.setProperty("mail.smtp.host", "mail.koreanclick.com");

	      // Get the default Session object.
	      Session session = Session.getDefaultInstance(properties);

	      try{
	         // Create a default MimeMessage object.
	         MimeMessage message = new MimeMessage(session);

	         // Set From: header field of the header.
	         message.setFrom(new InternetAddress(from));

	         // Set To: header field of the header.
	         message.addRecipient(Message.RecipientType.TO,
	                                  new InternetAddress(to));

	         message.setSubject(title);
	         message.setText(content);

	         // Send message
	         Transport.send(message);
	         System.out.println("Sent message successfully....");
	      }catch (MessagingException mex) {
	         mex.printStackTrace();
	      }
	}
	
	public static void SendOpenByNet(String accessday, String receiver, String mode) {
		URL sms = null;
		try {
			sms = new URL("http://219.240.39.116:1080/mail/smart_open_mail.php?day="+accessday+"&to="+receiver+"&from=tech@koreanclick.com"+"&mode="+mode);
			BufferedReader in = new BufferedReader(new InputStreamReader(sms.openStream()));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("The open mail has been sent to "+receiver+" at the day of "+accessday);
		}
	}
	
	public static void SendByNet(String accessday, String receiver) {
		URL sms = null;
		try {
			sms = new URL("http://219.240.39.116:1080/mail/smart_doner_alert_mail.php?day="+accessday+"&to="+receiver+"&from=tech@koreanclick.com");
			BufferedReader in = new BufferedReader(new InputStreamReader(sms.openStream()));
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("The doner alert has been sent to "+receiver+" at the day of "+accessday);
		}
	}
}
