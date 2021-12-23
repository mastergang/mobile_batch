package smartBatch.web.set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import smartBatch.web.model.RowTimeModel;
import DB.DBConnection;

public class BrowserSet {
	
	DBConnection dbcon;
	
	public void setDbcon(DBConnection dbcon){
		this.dbcon=dbcon;
	}
	
	public Collection timeSet(String accessday){
		return timesSetup(accessday);
	}
	
	//변경본, 패널까지 다 불러온다.
	private Collection timesSetup(String accessday){
		ResultSet rs = null;
		Collection col = new ArrayList<RowTimeModel>();
		String sql = 
				"select to_char(REQ_DATE, 'SSSSS') as reqtime, rowid, panel_id " +
				"from temp_hwlee_itrack_brow " +
				"where access_day = '"+accessday+"' " +
				"and panel_flag in ('D','N') "+
				//"and panel_id = 'choongsa' "+
				" order by panel_id, req_date, req_million, req_site_id ";
		System.out.println(sql);
		//System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			while (rs.next()) {
				RowTimeModel rt = new RowTimeModel();
				rt.setReqtime(rs.getInt("reqtime"));
				rt.setRowid(rs.getString("rowid"));
				rt.setPanelid(rs.getString("panel_id"));
				col.add(rt);
		  	}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) rs.close();
		    	dbcon.partclose();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return col;
	}
	
	//valid test
	public Boolean validTest(String panel_id, int timeSet){
		ResultSet rs = null;
		Boolean Result = false;
		Collection col = new ArrayList<RowTimeModel>();
		String sql = 
				"select nvl(max(valid),0) valid "+
				"from ( "+
				"    select START_TIME, END_TIME, "+
				"           case when "+timeSet+" between START_TIME-30 and END_TIME+30 then 1 "+
				"           else 0 end valid "+
				"    from   temp_hwlee_browser_itrack "+
				"    where  panel_id = '"+panel_id+"' " +
				"	 and    access_day = '20130101' "+
				") ";
		System.out.println(sql);
//		System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			if(rs.next()) {
				if(rs.getString("valid").equals("1")) Result = true;
				else Result = false;
		  	}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) rs.close();
		    	dbcon.partclose();
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		return Result;
	}
}
