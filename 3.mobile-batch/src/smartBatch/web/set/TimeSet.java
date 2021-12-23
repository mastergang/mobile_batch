package smartBatch.web.set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import smartBatch.web.model.RowTimeModel;
import DB.DBConnection;

public class TimeSet {
	
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
		String sql = "select to_char(REQ_DATE, 'SSSSS') as reqtime, rowid, panel_id " +
				"from tb_smart_browser_itrack " +
				"where access_day = '"+accessday+"' "+
				"and ((browser_kind in ('1','2','3'))" +
				"or (browser_kind ='9' and result_cd = 'S')) "+	
				"order by panel_id, req_date, req_million, req_site_id ";
//		System.out.println(sql);
//		System.exit(0);
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
}
