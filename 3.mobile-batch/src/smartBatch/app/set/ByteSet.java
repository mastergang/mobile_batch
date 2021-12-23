package smartBatch.app.set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import smartBatch.app.model.RowByteModel;
import DB.DBConnection;

public class ByteSet {
	
	DBConnection dbcon;
	
	public void setDbcon(DBConnection dbcon){
		this.dbcon=dbcon;
	}
	
	public Collection byteSet(String accessday){
		return bytesSetup(accessday);
	}
	
	private Collection bytesSetup(String accessday){
		ResultSet rs = null;
		Collection col = new ArrayList<RowByteModel>();
		String sql = 	"select rowid, panel_id, item_value, wifistatus, track_version, rxbyte, txbyte " +
						"from   tb_smart_env_itrack " +
						"where  access_day = '"+accessday+"' " +
						"and    to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 " +
						"and    rxbyte is not null " +
						"and    rxbyte <> -1 " +
						"and    item_value is not null " +
						"order by panel_id, item_value, register_date ";
		//System.out.println(sql);
//		System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			while (rs.next()) {
				RowByteModel rt = new RowByteModel();
				rt.setRowid(rs.getString("rowid"));
				rt.setPanel_id(rs.getString("panel_id"));
				rt.setItem_value(rs.getString("item_value"));
				rt.setWifistatus(rs.getString("wifistatus"));
				rt.setTrack_version(rs.getString("track_version"));
				rt.setRxbyte(rs.getLong("rxbyte"));
				rt.setTxbyte(rs.getLong("txbyte"));
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
