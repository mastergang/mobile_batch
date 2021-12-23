package smartBatch.app.set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import smartBatch.app.model.AppModel;
import smartBatch.web.model.QueryModel;

import DB.DBConnection;

public class AppSet {
	
	DBConnection dbcon;
	
	public void setDbcon(DBConnection dbcon){
		this.dbcon=dbcon;
	}
	
	public Collection newAppSet(String accessday){
		return newappSetup(accessday);
	}
	
	public int maxIDSet(){
		return maxIDSetup();
	}
	
	private Collection newappSetup(String accessday){
		ResultSet rs = null;
		Collection col = new ArrayList<AppModel>();
		String sql = 
				"select   ITEM_VALUE PACKAGE_NAME, "+
				"         ITEM_NAME APP_NAME "+
				"from     tb_smart_env_itrack "+
//				"where    subject = 'APP' "+
				"where    subject in ('APP', 'MEDIA', 'CHANNEL') "+
				"and      access_day between to_char(to_date('"+accessday+"', 'yyyymmdd')-3, 'yyyymmdd') and '"+accessday+"' "+
				"and      to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 5 "+
				"and      ITEM_VALUE in ( "+
				"    select   ITEM_VALUE PACKAGE_NAME "+
				"    from     tb_smart_env_itrack "+
//				"    where    subject = 'APP' "+
				"    where    subject in ('APP', 'MEDIA', 'CHANNEL') "+
				"    and      access_day between to_char(to_date('"+accessday+"', 'yyyymmdd')-3, 'yyyymmdd') and '"+accessday+"' "+
				"    and      to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 5 "+
				"    group by ITEM_VALUE "+
				"    minus "+
				"    select   package_name "+
				"    from     tb_smart_app_info "+
				"    group by package_name "+
				") "+
				"group by ITEM_VALUE, ITEM_NAME "+
				"order by ITEM_VALUE, ITEM_NAME ";
				;
		
		try {
			rs = dbcon.executeQuery(sql);
			while (rs.next()) {		
				String packagename = rs.getString("PACKAGE_NAME");
				String APPNAME = rs.getString("APP_NAME");
				boolean primary = true;
				
				if(packagename != null){
					AppModel rt = new AppModel();
					rt.setPackagename(packagename);
					rt.setAppname(APPNAME);
					rt.setPrimary(primary);
					col.add(rt);
				}
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
	
	private int maxIDSetup(){
		ResultSet rs = null;
		int maxid = 0;
		String sql = 
				"select max(Smart_ID) maxid "+
				"from tb_smart_app_info "
				;
				        
//		System.out.println(sql);
//		System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			rs.next();
			maxid = rs.getInt("maxid");
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
		return maxid;
	}
}
