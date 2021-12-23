package smartBatch.web.model;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.omg.CosNaming.NamingContextExtPackage.URLStringHelper;

public class QueryModel {
	String query;
	String rowid;
	
	public QueryModel() {
	}
	public QueryModel(String query, String rowid) {
		this.query = query;
		this.rowid = rowid;
	}
	public String getRowid() {
		return rowid;
	}
	public void setRowid(String rowid) {
		this.rowid = rowid;
	}
	public String getQuery() {
		return query;
	}
	public void setQuery(String query) {
		this.query = query;
	}
	
    /**
   * 메소드 : E2K
    * 역  할 : 한글 인코딩 (ISO8859 -> KSC5601)
    * 인자값 : String strEn (ISO8859 코드의 문자열)
    * 반환값 : String (KSC5601로 인코딩된 문자열)
    */
	public String E2K(String strEn) {
	 if ( strEn == null ) {
	     return "";
	 } // end if
	 try {
	     return new String( strEn.getBytes( "8859_1" ), "KSC5601" );
	 } catch ( Exception e ){
	     return strEn;
	 } // end try catch
	} // end E2K
	
	
    
	public QueryModel Decode(QueryModel val)
	{
		QueryModel encode = new QueryModel("","");
		if ( val == null ) {
        	return encode;
        }
        try {
        	String utf = new String(URLDecoder.decode(val.getQuery(), "utf-8"));
        	String euc = new String(URLDecoder.decode(val.getQuery(), "euc-kr"));
        	
        	String enutf = URLEncoder.encode(utf,"utf-8");
        	String eneuc = URLEncoder.encode(euc,"euc-kr");
        	
        	String backutf = new String(URLDecoder.decode(enutf, "utf-8"));
        	String backeuc = new String(URLDecoder.decode(eneuc, "euc-kr"));
        	
//        	System.out.println("UTF8="+utf);
//        	System.out.println("EUC="+euc);
//        	System.out.println();
//        	System.out.println("UTF8="+enutf);
//        	System.out.println("EUC="+eneuc);
//        	System.out.println();
//        	System.out.println("UTF8="+backutf);
//        	System.out.println("EUC="+backeuc);
//        	System.out.println();
//        	System.out.println("UTF8="+utf.equals(backutf));
//        	System.out.println("EUC="+euc.equals(backeuc));
//        	System.out.println(val.getQuery());
        	
        	boolean UTFIS = utf.equals(backutf);
        	boolean EUCIS = euc.equals(backeuc);
        	
        	if(UTFIS == true) {
        		if(utf.indexOf("�") == -1  && E2K(utf).indexOf("�") == -1) {
        		//System.out.println("final:"+utf);
        			return new QueryModel(utf, val.getRowid());
        		} else if(EUCIS == false){
        			return new QueryModel(utf, val.getRowid());
        		} else {
        			return new QueryModel(euc, val.getRowid());
        		}
        	}
        	else if(EUCIS == true){
        		//System.out.println("final:"+euc);
        		return new QueryModel(euc, val.getRowid());
        	} else {
        		return new QueryModel(utf, val.getRowid());
        	}
        	
        } catch ( Exception e ){
            return encode;
        }
	}
}
