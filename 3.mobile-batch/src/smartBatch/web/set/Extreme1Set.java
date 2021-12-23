package smartBatch.web.set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import smartBatch.web.model.iTrackModel;
import DB.DBConnection;

public class Extreme1Set {
	
	DBConnection dbcon;
	
	public void setDbcon(DBConnection dbcon){
		this.dbcon=dbcon;
	}
	
	public Collection extreme1Set(String accessday){
		return extreme1Setup(accessday);
	}
	
	public Collection delete1Set (int p30, int pv_adjust){
		String p30s = Integer.toString(p30);
		String pv_adjusts = Integer.toString(pv_adjust);
		
		ResultSet rs = null;
		Collection col = new ArrayList<iTrackModel>();
		String sql = 
				"select nvl(delete_rowno, -1) nol "+
				"from "+
				"( "+
				"       select decode( sign('"+p30s+"' / '"+pv_adjusts+"' - 2), 1, "+
				"              decode(trunc(no/trunc(('"+p30s+"'/'"+pv_adjusts+"'))), no/trunc(('"+p30s+"'/'"+pv_adjusts+"')),  "+
				"                    decode( sign(trunc('"+p30s+"'/'"+pv_adjusts+"')*'"+pv_adjusts+"'-no), 1, null, 0, null, "+
				"                        decode(sign(trunc('"+p30s+"'/'"+pv_adjusts+"')*'"+pv_adjusts+"' - no), 1, no+1, 0, no+1, no)), "+
				"                    decode(sign(trunc('"+p30s+"'/'"+pv_adjusts+"')*'"+pv_adjusts+"'-no), 1, no+1, 0, no+1, no )), "+
				"                "+
				"                 decode(trunc(no/trunc(('"+p30s+"'/('"+p30s+"'-'"+pv_adjusts+"')))),no/trunc(('"+p30s+"'/('"+p30s+"'-'"+pv_adjusts+"'))), "+
				"                    decode(sign(trunc('"+p30s+"'/('"+p30s+"'-'"+pv_adjusts+"'))*('"+p30s+"'-'"+pv_adjusts+"')-no), -1, null, no), "+
				"                    decode(sign(trunc('"+p30s+"'/('"+p30s+"'-'"+pv_adjusts+"'))*'"+pv_adjusts+"'-no), -1,  "+
				"                       decode(sign(trunc('"+p30s+"'/('"+p30s+"'-'"+pv_adjusts+"'))*('"+p30s+"'-'"+pv_adjusts+"')-no), null, no)), null) "+
				"               ) delete_rowno "+
				"        from copy_t "+
				"        where no <= '"+p30s+"' "+
				") ";
//		System.out.println(sql);
//		System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			while (rs.next()) {
				String rownum = rs.getString("nol");
				col.add(rownum);
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
	
	//itrack에서 변경되어야할 PV로 불러온다.
	private Collection extreme1Setup(String accessday){
		ResultSet rs = null;
		Collection col = new ArrayList<iTrackModel>();
		String sql = 
					"select /*+use_hash(B,A)*/ "+
					"      A.rowid rid, B.panel_id, B.domain_url, B.ext_domain_url, B.site_id, B.p30, B.pv_adjust_value "+
					"from    "+
					"( "+
					"   select   rowid,panel_id,req_site_id site_id,req_domain domain_url  "+
					"   from     tb_smart_browser_itrack a "+
					"   where	 access_day = '"+accessday+"' "+
					"   and      result_cd in ('S')  "+
					"   and      panel_flag in ('D','V') "+
					") A, "+
					"( "+
					"      select /*+use_hash(b,a)*/a.site_id, a.domain_url, a.ext_domain_url, b.panel_id, b.p30, b.pv_adjust_value "+
					"      from "+
					"      (  "+
					"                 SELECT   site_id, domain_url, ext_domain_url  "+
					"                 FROM     vi_ext_domain_info  "+
					"      ) a, "+
					"      ( "+
					"                 SELECT site_id, domain_url, panel_id, p30, pv_adjust_value "+
					"                 FROM   tb_smart_day_extreme_new_fact b "+
					"                 WHERE  access_day = '"+accessday+"' "+
					"      ) b "+
					"      where  a.site_id        = b.site_id "+
					"      and    a.ext_domain_url = b.domain_url "+
					") B "+
					"where  A.site_id    = B.site_id "+
					"and    A.domain_url = B.domain_url "+
					"and    A.panel_id   = B.panel_id  "+
					"order by B.panel_id, B.domain_url, B.site_id ";
//		System.out.println(sql);
//		System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			while (rs.next()) {
				iTrackModel rt = new iTrackModel();
				rt.setRowid(rs.getString("rid"));
				rt.setPanelid(rs.getString("panel_id"));
				rt.setDomain(rs.getString("domain_url"));
				rt.setExt_domain(rs.getString("ext_domain_url"));
				rt.setSiteid(rs.getInt("site_id"));
				rt.setP30(rs.getInt("p30"));
				rt.setPv_adjust(rs.getInt("pv_adjust_value"));
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
