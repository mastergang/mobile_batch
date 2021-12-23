package smartBatch.app.set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import smartBatch.app.model.RowTimeModel;
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
		String sql = 
				"select  "+
				"        to_char(REGISTER_DATE, 'SSSSS') as reqtime, rid, panel_id, "+
				"        item_value, nvl(screen,1) screen, "+
				"        substr(track_version,1,2)track_version "+
				"from ( "+
				//음악 앱이 아니고 flag가 0 또는 1인 앱
				"    select /*+parallel(a,4)*/ a.*, rowid rid "+
				"    from   tb_smart_env_itrack a "+
				"    where  access_day = '"+accessday+"' "+
				"    and    flag in ('0','1','4','5','6') "+
				"    and    item_value not in ( "+
				"        select package_name "+
				"        from   tb_smart_app_media_list "+
				"        where  exp_time > sysdate "+
				"        and    ef_time < sysdate "+
				"    ) "+
				"    and    to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6"+
				" "+
				"    union all "+
				" "+
				//음악 앱이나 flag가 1인 앱
				"    select /*+parallel(a,4)*/ a.*, rowid rid "+
				"    from   tb_smart_env_itrack a "+
				"    where  access_day = '"+accessday+"' "+
				"    and    flag in ('1','6') "+
				"    and    item_value in ( "+
				"        select package_name "+
				"        from   tb_smart_app_media_list "+
				"        where  exp_time > sysdate "+
				"        and    ef_time < sysdate "+
				"    ) " +
				"    and    to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
//				" "+
//				"    union all "+
//				" "+
//				//4버젼 이하의 하위 기종
//				"    select a.*, rowid rid "+
//				"    from   tb_smart_env_itrack a "+
//				"    where  access_day = '"+accessday+"' " +
//				"	 and    track_version < '5'"+
				") " +
				"order by PANEL_ID, REGISTER_DATE, SUBJECT desc, SERVER_DATE ";
//		System.out.println(sql);
//		System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			while (rs.next()) {
				RowTimeModel rt = new RowTimeModel();
				rt.setReqtime(rs.getInt("reqtime"));
				rt.setRowid(rs.getString("rid"));
				rt.setPanelid(rs.getString("panel_id"));
				rt.setPackage(rs.getString("item_value"));
				rt.setScreen(rs.getString("screen"));
				rt.setVersion(rs.getString("track_version"));
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
