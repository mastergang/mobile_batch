package smartBatch.web.set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import smartBatch.web.model.QueryModel;

import DB.DBConnection;

public class QuerySet {
	
	DBConnection dbcon;
	
	public void setDbcon(DBConnection dbcon){
		this.dbcon=dbcon;
	}
	
	public Collection querySet(String accessday){
		return queriesSetup(accessday);
	}
	
	private Collection queriesSetup(String accessday){
		ResultSet rs = null;
		
		int date = Integer.parseInt(accessday.substring(0, 6));
		if(date%100 == 12){
			date+=89;
		} else {
			date+=1;
		}
		String indate = Integer.toString(date)+"05";
		
		Collection col = new ArrayList<QueryModel>();
		String sql = 
				"SELECT  /*+ use_hash(b,a) */ a.rowid, " +
						"substr(parameter, decode(instr(parameter, b.query), 1, instr(parameter, b.query)+lengthb(b.query), instr(parameter, '&'||b.query)+lengthb('&'||b.query)), "+
						"decode(instr(substr(parameter, decode(instr(parameter, b.query), 1, instr(parameter, b.query)+lengthb(b.query), instr(parameter, '&'||b.query)+lengthb('&'||b.query))), '&'),0, lengthb(parameter), "+
						"instr(substr(parameter, decode(instr(parameter, b.query), 1, instr(parameter, b.query)+lengthb(b.query), instr(parameter, '&'||b.query)+lengthb('&'||b.query))), '&')-1) ) decode_string "+
				"from ( "+
				"select a.rowid, SITE_ID, DOMAIN_URL, PAGE, org_page, " + 
				"       case when SITE_ID in (43339,5033) and instr(PAGE,'#') > 0 " + 
				"                then replace(substr(PAGE,instr(PAGE,'#')+1)||PARAMETER,'#','&') " + 
				"            when site_id in (43339,5033) " +
				"                then replace(parameter,'#','&') " +
				"            when site_id = 178 and domain_url like 'http://%dic.naver.com' " + 
				"                 and instr(page,'/',1,2) > 0 " + 
				"                 and instr(page,'/',1,3) = 0 " + 
				"                 and flag = 'Y' " + 
				"                 and page like '%!%%' escape '!' " + 
				"             then case when page like '%q=%' then '' else 'q=' end " + 
				"                 ||substr(page,instr(page,'/',1,1)+1,instr(page,'/',1,2)-instr(page,'/',1,1)-1)||'&'||parameter " + 
				"            when site_id = 178 and domain_url like 'http://%dic.naver.com' " + 
				"                 and instr(page,'/',1,4) > 0 " + 
				"                 and instr(page,'/',1,5) = 0 " + 
				"                 and flag = 'Y' " + 
				"                 and page like '%!%%' escape '!' " + 
				"             then case when page like '%q=%' then '' else 'q=' end " + 
				"                 ||substr(page,instr(page,'/',1,3)+1,instr(page,'/',1,4)-instr(page,'/',1,3)-1)||'&'||parameter " + 
				"             when site_id = 178 and domain_url like 'http://%dic.naver.com' " + 
				"                 and instr(page,'/',1,3) > 0 " + 
				"                 and instr(page,'/',1,4) = 0 " + 
				"                 and flag = 'Y' " + 
				"                 and page like '%!%%' escape '!' " + 
				"             then case when page like '%q=%' then '' else 'q=' end " + 
				"                 ||substr(page,instr(page,'/',1,2)+1,instr(page,'/',1,3)-instr(page,'/',1,2)-1)||'&'||parameter " + 
				"             when site_id = 178 and domain_url like 'http://%dic.naver.com' " + 
				"             then replace(page,'/','&')||'&'||replace(parameter,'/','&') " + 
				"             when site_id = 178 and domain_url in ('https://search.naver.com','https://m.search.naver.com') " +
				"             then replace(replace(parameter,'#imgId','&imgId'),'#api','&api') " +
				"        else parameter end parameter, flag " + 
				"from " + 
				"( " + 
				"   select /*+parallel(a,8)*/access_day, req_site_id site_id, req_domain domain_url, " + 
				"           case when req_site_id = 178 and req_domain like 'http://%dic.naver.com' and req_page not like '%jsessionid=%' " + 
				"                     and req_page like '%!%%' escape '!' and req_page not like '%=!%%' escape '!' " + 
				"                then decode(substr(req_page,length(req_page),1),'/',req_page,req_page||'/') " + 
				"          else req_page end page, req_page org_page, req_parameter parameter, " + 
				"           case when req_site_id = 178 and req_domain like 'http://%dic.naver.com' " + 
				"           and req_page||req_parameter not like '%=!%%' escape '!' then 'Y' else 'N' end flag " + 
				"   from tb_smart_browser_itrack a " + 
				"   where access_day = '"+accessday+"' " + 
				"   and req_query_decode is null " +
				") a " +
					  ") a, (" +
						  "select   site_id, domain_url, query "+ 
						  "from ( select  /*+ordered use_hash(b,a)*/ site_id, decode(instr(path_url, '/', 1, 3), 0, " +
						   				 "path_url, substr(path_url, 1, instr(path_url, '/', 1, 3)-1)) domain_url, query " +
						   		 "from (select site_id, section_id, path_url, ef_time, exp_time, query from tb_section_info) "+
						   		 "where section_id in (select code from tb_codebook b where meta_code='SECTION' and (code_etc='2' or code=2)) "+
						   		 "and exp_time > to_date('"+indate+"','yyyymmdd') and ef_time < to_date('"+indate+"','yyyymmdd') and query is not null) "+
						   		 "group by site_id, domain_url, query "+
					  ") b "+
				"where   a.domain_url = b.domain_url and     (parameter like '%&'||b.query||'%' or parameter like b.query||'%') and     a.site_id  = b.site_id" +
				" order by a.rowid"
				;
				        
//		System.out.println(sql);
//		System.exit(0);
		try {
			rs = dbcon.executeQuery(sql);
			while (rs.next()) {		
				String rowid = rs.getString("rowid");
				String query = rs.getString("decode_string");
				
				if(query != null){
					QueryModel rt = new QueryModel();
					rt.setRowid(rowid);
					rt.setQuery(query);
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
