package smartBatch.app.set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import smartBatch.app.model.AppModel;
import smartBatch.web.model.QueryModel;

import DB.DBConnection;

public class PackageSet {
	
	DBConnection dbcon;
	
	public void setDbcon(DBConnection dbcon){
		this.dbcon=dbcon;
	}
	
	public Collection newPackageSet(String accessday){
		return newappSetup(accessday);
	}
	
	private Collection newappSetup(String accessday){
		ResultSet rs = null;
		Collection col = new ArrayList<AppModel>();
		String sql = 
				 "select package_name, smart_id " +
			     "from tb_smart_app_info " +
			     "where EF_TIME > to_date('"+accessday+"','YYYY/MM/DD')-5 " +
			     "and   EXP_TIME > sysdate "+
			     "and   smart_id not in ( " +
			     "select smart_id from tb_smart_provider_info )"
			     ;
			     
				        
		//System.out.println(sql);
		//System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			while (rs.next()) {		
				String packagename = rs.getString("PACKAGE_NAME");
				int smartid = rs.getInt("SMART_ID");
				boolean primary = true;
				
				if(packagename != null){
					AppModel rt = new AppModel();
					rt.setPackagename(packagename);
					rt.setSmartid(smartid);
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
}
