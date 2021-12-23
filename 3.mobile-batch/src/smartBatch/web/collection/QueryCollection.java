package smartBatch.web.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import smartBatch.web.model.QueryModel;
import smartBatch.web.set.QuerySet;

import log.WriteMsgLog;
import DB.DBConnection;

public class QueryCollection {
	DBConnection dbcon;
	String filtername;
	
	QuerySet queries = new QuerySet();
	Collection decodes = new ArrayList<QueryModel>();
	
	
	public QueryCollection(DBConnection dbcon, String filtername){
		this.dbcon = dbcon;
		this.filtername = filtername;
	}
	
	/**
	 * 
	 * @return: 1은 정상처리, 99는 panel의 미처리, 98은 req_date의 미처리
	 */
	public Collection access() {
		queries.setDbcon(dbcon);
		Collection encodes = queries.querySet(filtername);
		Collection decodes = new ArrayList<QueryModel>();
		QueryModel decoded = new QueryModel();
		
		if(encodes != null) {
			Iterator it = encodes.iterator();
			try {
				while(it.hasNext()) {
					QueryModel decode=(QueryModel)it.next();

					String query = decode.getQuery();
					String rowid = decode.getRowid();
					
					decoded = decoded.Decode(decode);
					if(decoded.getQuery()!=null && decoded.getRowid()!=null){
						decodes.add(decoded);
					}
				}
				//System.exit(0);
			} catch(Exception e){
				e.printStackTrace();
			}
		}
		return decodes;
	}
}
