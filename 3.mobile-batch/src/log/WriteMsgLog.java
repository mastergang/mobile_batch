package log;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class WriteMsgLog implements Serializable{
        
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String logURL = ".\\logs\\access.log";

	public WriteMsgLog() {
	}

	public synchronized void  writeLog( String msg ) {
		File logfile = new File(logURL);
		RandomAccessFile ranfile = null;
	
		try{
		
			ranfile = new RandomAccessFile(logfile,"rw");
	
			SimpleDateFormat dformat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
				
			ranfile.seek(ranfile.length());

			ranfile.writeBytes( dformat.format( cal.getTime() ) + "   " + msg +"\n");
				
		}catch(FileNotFoundException fe){
			System.out.println("log file not found");
		}catch(Exception ie){
			System.out.println("IO ERROR");	
		}finally{  
			try{
				if(ranfile != null) ranfile.close();
			 }catch(IOException ie){
				
				System.out.println("IO ERROR");
			 }	
		}	
		
	}
	public synchronized void writeLog(String url, String msg){
			                  
		File logfile = new File(url);
		RandomAccessFile ranfile = null;

		try{		
			ranfile = new RandomAccessFile(logfile,"rw");
			       		
			ranfile.seek(ranfile.length());		
			ranfile.writeBytes(msg+"\n");
	
		}catch(FileNotFoundException fe){

			
			System.out.println("log file not found- : "+url);
		}catch(IOException ie){
			
			System.out.println("IO ERROR");	
		}finally{  
			try{
				if(ranfile != null) ranfile.close();
			 }catch(IOException ie){
				
				System.out.println("IO ERROR");
			 }	
		}		
    }	
	public synchronized void writeLog(String url, String eclass, String ip, String msg) throws IOException
	{
		if(url != null && !url.equals("")){
					
			File logfile = new File(url);
			RandomAccessFile ranfile = null;
		
			try{
		
				ranfile = new RandomAccessFile(logfile,"rw");
	
				SimpleDateFormat dformat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Calendar cal = Calendar.getInstance();
				
				ranfile.seek(ranfile.length());

				ranfile.writeBytes(dformat.format(cal.getTime())+"   " +eclass+"    "+ip+"    "+msg+"\n");
				
			}catch(FileNotFoundException fe){
				System.out.println("log file not found-==: "+url);
			}catch(IOException ie){
				System.out.println("IO ERROR");	
			}finally{  
				ranfile.close(); 
			}
		}		
	}	
}
