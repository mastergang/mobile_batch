package smartBatch.web.set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import smartBatch.web.model.EnvModel;
import smartBatch.web.model.RowTimeModel;
import DB.DBConnection;

public class EnvBrowserSet {
	
	DBConnection dbcon;
	
	public void setDbcon(DBConnection dbcon){
		this.dbcon=dbcon;
	}
	
	public Collection envBrowserSet(String accessday){
		return envSetup(accessday);
	}
	
	//변경본, 패널까지 다 불러온다.
	private Collection envSetup(String accessday){
		ResultSet rs = null;
		Collection col = new ArrayList<RowTimeModel>();
		String sql = 
				"select panel_id, to_number(to_char(REGISTER_DATE, 'SSSSS')) start_time, to_number(to_char(REGISTER_DATE, 'SSSSS'))+time_gap end_time " +
				"from  tb_smart_env_itrack " +
				"where access_day = '"+accessday+"' "+
				"order by panel_id, register_date ";
//		System.out.println(sql);
//		System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			while (rs.next()) {
				EnvModel rt = new EnvModel();
				rt.setStarttime(rs.getInt("start_time"));
				rt.setEndtime(rs.getInt("end_time"));
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
