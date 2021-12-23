package DB;

/**************************************************************************
 *  프로그램명	: DbConnection.java
 *  설명			: DB에 Connection을 하며 쿼리를 실행하고, 실행 결과에 따른
 *				  결과값을 가져오는 자바빈이다.
 *************************************************************************/
 
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;
import java.io.UnsupportedEncodingException;
import log.MailSender;
import log.SmsSender;
import log.WriteMsgLog;
import smartBatch.app.model.AppModel;
import smartBatch.web.model.QueryModel;


public class DBConnection
{
	private DBConnectionManager connMgr;

	Connection connection = null;
	PreparedStatement pstmt = null;
	ResultSet result = null;
	/***************************************************************************
	 * 생성자 메소드
	 **************************************************************************/
	public DBConnection()
	{
	}

	/**************************************************************************
	 *  메소드명	: openConnection 
	 *  인자		: 없음
	 *  리턴형	: boolean 
	 *  설명		: 데이터베이스로 커넥션을 열기 위한 메소드
	 *			  DBpool을 이용하여 커넥션을 얻는다.
	 *************************************************************************/
	public boolean openConnection()
	{
		connMgr = DBConnectionManager.getInstance();
		try
		{
			connection = connMgr.getConnection("kcdb");
			if (connection == null)
			{
				System.err.println("Can't get connection");
				return false;
			}
		}
		catch (Exception e)
		{
			System.err.println ("Exception: " + e);
			return false;
		}
		return true;
	}

	/**************************************************************************
	 *		메소드명	: executeQuery
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: java.sql.ResultSet
	 *		설명			: 데이터베이스로 질의를 하기 위한 메소드 (SELECT)
	 *************************************************************************/
	public ResultSet executeQuery(String query,Vector vData) throws SQLException 
	{
				
		this.pstmt = connection.prepareStatement(query);
			
		if(vData != null) {
			for( int i=0; i<vData.size() ; i++){
				this.pstmt.setString(i+1, (String)vData.elementAt(i));
			}
		}
		this.result = this.pstmt.executeQuery();
		return result;
	}
	
	/**************************************************************************
	 *		메소드명	: executeQuery
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: java.sql.ResultSet
	 *		설명			: 데이터베이스로 질의를 하기 위한 메소드 (SELECT)
	 *************************************************************************/
	public ResultSet executeQuery(String query) throws SQLException 
	{		
		this.pstmt = connection.prepareStatement(query);
		this.result = this.pstmt.executeQuery();
		return result;
	}
	
	/**************************************************************************
	 *		메소드명		: executeQueryTime
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: Calendar
	 *		설명			: 데이터베이스로 실행하기 위한 메소드
	 *************************************************************************/
	public Calendar executeQueryTime(String query) throws SQLException
	{		
		Calendar eachPt = Calendar.getInstance();
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeQueryExecute
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: Void
	 *		설명			: 데이터베이스로 실행하기 위한 메소드
	 *************************************************************************/
	public void executeQueryExecute(String query) throws SQLException
	{
		System.out.println(query);
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
	}
	
	/**************************************************************************
	 *		메소드명		: executeUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 데이터베이스로 수정을 위한 메소드
	 *					: (UPDATE,DELETE,INSERT)
	 *************************************************************************/
	
	public void executeUpdate(String query, Vector vData) throws SQLException 
	{
		this.pstmt = connection.prepareStatement(query);
		for( int i=0; i<vData.size() ; i++){
			this.pstmt.setString(i+1, (String)vData.elementAt(i));
		}
		this.pstmt.executeUpdate();
		
//		if(this.pstmt!=null) this.pstmt.close();
	}
	/**************************************************************************
	 *		메소드명		: executeDecodeUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 특정 rowid의 query_decode를 업데이트 (Update)
	 *************************************************************************/
	
	public void executeDecodeUpdate(QueryModel model) throws SQLException 
	{
		String query = "update TB_SMART_BROWSER_ITRACK set req_query_decode = '"+model.getQuery()+"' where rowid='"+model.getRowid()+"'";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		//System.out.println(query);
		//System.exit(0);
		if(this.pstmt!=null) this.pstmt.close();
	}
	
	/**************************************************************************
	 *		메소드명		: executeExtremeUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 극단값 처리가 될 특정 rowid에 업데이트문 (UPDATE)
	 *************************************************************************/
		
	public void executeExtremeInsert(String accessday, int term) throws SQLException 
	{
		String query1 = 
				"Delete tb_smart_day_domain_fact ";
		this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
		
		String queryE = 
				"Delete tb_smart_day_extreme_new_fact ";
		this.pstmt = connection.prepareStatement(queryE);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
		
		String query2="";
		
		if(term == 1){
			query2 = 
				"INSERT INTO tb_smart_day_domain_fact "+
				"(access_day, site_id, domain_url, panel_id, pv_cnt, duration, PROC_DATE) "+
				"SELECT   /*+ use_hash(a,b)*/ "+
				"      	'"+accessday+"' access_day, a.req_site_id, b.ext_domain_url domain_url, panel_id,  "+
				"      	count(*) pv_cnt, "+
				"      	sum(duration) duration, sysdate "+
				"FROM     tb_smart_browser_itrack a, vi_ext_domain_info b "+
				"WHERE    access_day = '"+accessday+"'  "+
				"AND      result_cd = 'S' "+
				"AND      duration > 0 "+
				"AND      a.req_site_id = b.site_id "+
				"AND      a.req_domain = b.domain_url "+
				"GROUP BY a.req_site_id, panel_id, b.ext_domain_url ";
		} else if (term == 2 || term == 3){
			query2 =
				"INSERT INTO tb_smart_day_domain_fact "+
				"(access_day, site_id, panel_id, domain_url, pv_cnt, duration, proc_date) "+
				"SELECT /*+ use_hash(b,a)*/ "+
				"'"+accessday+"' access_day, site_id, a.panel_id, a.domain_url, "+
				"round(sum(pv_cnt*kc_p_factor)) pv_cnt, "+
				"round(sum(duration*kc_p_factor),5) duration, sysdate "+
				"FROM   ( "+
				"    SELECT /*+ use_hash(a,b)*/ "+
				"           a.req_site_id site_id, panel_id, b.ext_domain_url domain_url, "+
				"           count(*) pv_cnt, sum(duration) duration "+
				"    FROM     tb_smart_browser_itrack a, vi_ext_domain_info b "+
				"    WHERE    access_day = '"+accessday+"' "+
				"    AND      result_cd = 'S' "+
				"    and      panel_flag in ('D','V') "+
				"    AND      a.req_site_id = b.site_id "+
				"    AND      a.req_domain = b.domain_url "+
				"    GROUP BY a.req_site_id, panel_id, b.ext_domain_url "+
				") a, "+
				"( "+
//				"    SELECT   panel_id, kc_n_factor, FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR kc_p_factor "+
//				"    FROM     tb_day_panel_seg "+
//				"    WHERE    access_day = '"+accessday+"' "+
				"    SELECT   panel_id, kc_n_factor, FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR kc_p_factor "+
				"    FROM     tb_day_total_panel_seg "+
				"    WHERE    access_day = '"+accessday+"' "+
				") b "+
				"WHERE a.panel_id = b.panel_id "+
				"GROUP BY site_id, a.panel_id, domain_url ";
		}
		this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
		
		String query3 = "";
		if (term == 1 || term ==2) {
			query3 = 
					"INSERT INTO tb_smart_day_extreme_new_fact "+
					"           (access_day, site_id, domain_url, panel_id, p30, p29, p28, p1, psum28, psum_all, "+
					"            pv_fvalue, pv_adjust_value_f, pv_adjust_value_c, panel_cnt, "+
					"            panel_fvalue, pv_adjust_value) "+
					"select access_day, site_id, domain_url, panel_id, p30, p29, p28, p1, psum28, psum_all,  "+
					"       pv_new_fvalue, '', '', panel_cnt, "+
					"       panel_new_fvalue, pv_new_adjusted_value "+
					"from "+
					"( "+
					"     select access_day, site_id, domain_url, "+
					"            p30, p29, p28, p27, p1, p0, psum28, psum27, psum2930, psum30, psum_all,  top_rank2, "+
					"            round((psum28+2*p28-30*p1)/(top_rank-3),2) pa, "+
					"            decode((psum28+2*p28-30*p1)/(top_rank-3),0,0 ,decode((psum28+2*p28-top_rank*p1),0,0, decode(sign(p30-101),-1,0, "+
					"            round((p30-p29)/((psum28+2*p28-top_rank*p1)/(top_rank-3)),2)))) pv_fvalue, "+
					"            panel_cnt, "+
					"            panel_id, "+
					"            sd_pv, "+
					"            avg_pv, "+
					"            decode(sign(panel_cnt-30),-1,kclick.fn_fvalue3(top_rank2),kclick.fn_fvalue2(top_rank2)) panel_new_fvalue, "+
					"            round(((((p30-1*p0)/decode((p29-p0),0,0.0001,(p29-p0)))-1)/1)/((((psum28-(top_rank2-2)*p0)/decode((p29-p0),0,0.0001,(p29-p0)))+1+1)/(top_rank-1)),2) pv_new_fvalue, "+
					"            round(p29+decode(sign(panel_cnt-30),-1,kclick.fn_fvalue3(top_rank2),kclick.fn_fvalue2(top_rank2))*(((psum29-(top_rank2-1)*p0)+1*(p29-p0))/(top_rank2-1))) pv_new_adjusted_value "+
					"     from "+
					"     ( "+
					"             select access_day, site_id, "+
					"                    domain_url,  "+
					"                    min(case when pv_rank = 2 then panel_id end ) panel_id, "+
					"                    sum(case when pv_rank >= 2 and pv_rank <= top_rank-1 then pv_cnt end )  psum30, "+
					"                    sum(case when pv_rank >= 3 and pv_rank <= top_rank-1 then pv_cnt end )  psum29, "+
					"                    sum(case when pv_rank >= 4 and pv_rank <= top_rank-1 then pv_cnt end )  psum28, "+
					"                    sum(case when pv_rank >= 5 and pv_rank <= top_rank-1 then pv_cnt end )  psum27, "+
					"                    sum(case when pv_rank <= 3 AND pv_rank <= 2 then pv_cnt end )  psum2930, "+
					"                    sum(case when pv_rank = 2 then pv_cnt end )    p30, "+
					"                    sum(case when pv_rank = 3 then pv_cnt end )    p29, "+
					"                    sum(case when pv_rank = 4 then pv_cnt end )    p28, "+
					"                    sum(case when pv_rank = 5 then pv_cnt end )    p27, "+
					"                    sum(case when pv_rank = top_rank-1 then pv_cnt end ) p1, "+
					"                    sum(case when pv_rank = top_rank then pv_cnt end ) p0, "+
					"                    min(panel_cnt-1) panel_cnt, "+
					"                    min(decode(sign(top_rank-32),-1, top_rank, 32))  top_rank, "+
					"                    min(decode(sign(top_rank-32),-1, top_rank-2, 30)) top_rank2, "+
					"                    sum(pv_cnt)    psum_all, "+
					"                    sum(duration)  dsum_all, "+
					"                    stddev(pv_cnt) sd_pv, "+
					"                    avg(pv_cnt) avg_pv "+
					"             from        "+
					"             ( "+
					"                    select access_day, site_id, panel_id, domain_url, pv_cnt, duration, pv_rank, dt_rank, panel_cnt, "+
					"                           decode(sign(panel_cnt-30),-1,7, top_rank) top_rank "+
					"                    from  "+
					"                        ( "+
					"                             select access_day, site_id, panel_id, domain_url, pv_cnt, duration,  "+
					"                                    rank() over ( partition by access_day, domain_url order by pv_cnt   desc, rownum ) pv_rank, "+
					"                                    rank() over ( partition by access_day, domain_url order by duration   desc, rownum ) dt_rank, "+
					"                                    sum(1) over ( partition by access_day, domain_url)  panel_cnt, "+
					"                                    round(sum(1) over ( partition by access_day, domain_url)/4)  top_rank "+
					"                             from  kclick.tb_smart_day_domain_fact "+
					"                             where access_day = '"+accessday+"' "+
					"                             and   site_id not in ( "+
					"                                                    select site_id  "+
					"                                                    from  tb_site_info  "+
					"                                                    where category_code2 in ( 'ZP','ZY')  "+
					"                                                    and   exp_time >= sysdate "+
					"                                                    and   ef_time  <= sysdate "+
					"                                                   ) "+
					"                        ) "+
					"             ) "+
					"             group by access_day, site_id, domain_url "+
					"             having min(panel_cnt) >= 5 "+
					"     ) "+
					") "+
					"where pv_new_fvalue > panel_new_fvalue "+
					"and   PV_NEW_ADJUSTED_VALUE < p30 "+
					"and   p30>2*p29 "+
					"and   p30-p29>2*(p29-p28) "+
					"and   p30 >30 ";
		} else if(term == 3){
			query3 = 
					"INSERT INTO tb_smart_day_extreme_new_fact "+
					"(access_day, site_id, domain_url, panel_id, p30, p29, p28, p1, psum28, psum_all, "+
					"pv_fvalue, pv_adjust_value_f, pv_adjust_value_c, panel_cnt, "+
					"panel_fvalue, pv_adjust_value) "+
					"select  /*+use_hash(b,a)*/ "+
					"	a.access_day, "+
					"	site_id, "+
					"	domain_url, "+
					"	panel_id, "+
					"	pv_cnt p30, '', '', '', '', '', '', '', "+
					"	panel_cnt, "+
					"	sum(1) over (partition by a.access_day, domain_url) panel_cnt, '', "+
					"	adjust_value "+
					"from    "+
					"( "+
					"	select  /*+use_hash(b,a)*/ "+
					"		a.access_day, site_id, domain_url, a.panel_id,  kc_p_factor, pv_cnt, "+
					"		pv_rank1, "+
					"		percent_rank() over (partition by a.access_day order by pv_cnt desc) Pv_rank2, "+
					"		panel_cnt, "+
					"		p_rank "+
					"	from    "+
					"	( "+
					"		select  access_day, site_id, domain_url, panel_id, pv_cnt, "+
					"				sum(1) over (partition by access_day, site_id, domain_url) panel_cnt,  "+
					"				percent_rank() over (partition by access_day, site_id, domain_url order by pv_cnt desc) Pv_rank1 "+
					"		from    tb_smart_day_domain_fact "+
					"		where   access_day = '"+accessday+"'  "+
					"	) a, "+
					"	( "+
//					"		select  access_day, panel_id, FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR kc_p_factor, "+
//					"				percent_rank() over (partition by access_day order by FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR desc) P_rank "+
//					"		from    tb_day_panel_seg "+
//					"		where   access_day = '"+accessday+"' "+
					"		select  access_day, panel_id, FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR kc_p_factor, "+
					"				percent_rank() over (partition by access_day order by FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR desc) P_rank "+
					"		from    tb_day_total_panel_seg "+
					"		where   access_day = '"+accessday+"' "+
					"	) b "+
					"	where  a.access_day = b.access_day "+
					"	and    a.panel_id = b.panel_id "+
					"	and    pv_rank1 <= 0.05 "+
					"	and    p_rank <= 0.1  "+
					") a, "+
					"( "+
					"	select  access_day, "+
					"		min(pv_cnt) adjust_value, "+
					"		max(pv_rank2) "+
					"	from    "+
					"	( "+
					"		select  /*+use_hash(b,a)*/ "+
					"				a.access_day, "+
					"				pv_cnt, "+
					"				percent_rank() over (partition by a.access_day order by pv_cnt desc) Pv_rank2 "+
					"		from "+
					"		( "+
					"			select  access_day, site_id, domain_url, panel_id, pv_cnt, "+
					"					sum(1) over (partition by access_day, site_id, domain_url) panel_cnt, "+
					"					percent_rank() over (partition by access_day, site_id, domain_url order by pv_cnt desc) Pv_rank1 "+
					"			from    tb_smart_day_domain_fact "+
					"			where   access_day = '"+accessday+"'  "+
					"		) a, "+
					"		( "+
//					"			select  access_day, panel_id, FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR kc_p_factor, "+
//					"					percent_rank() over (partition by access_day order by FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR desc) P_rank "+
//					"			from    tb_day_panel_seg "+
//					"			where   access_day = '"+accessday+"' "+
					"			select  access_day, panel_id, FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR kc_p_factor, "+
					"					percent_rank() over (partition by access_day order by FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR desc) P_rank "+
					"			from    tb_day_total_panel_seg "+
					"			where   access_day = '"+accessday+"' "+
					"		) b "+
					"		where  a.access_day = b.access_day "+
					"		and    a.panel_id = b.panel_id "+
					"		and    pv_rank1 <= 0.05 "+
					"		and    p_rank <= 0.1  "+
					"	)  "+
					"	where  pv_rank2 <= 0.01 "+
					"	group by access_day      "+
					") b "+
					"where  a.pv_rank2 <= 0.01 ";
		}
		this.pstmt = connection.prepareStatement(query3);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
		
		String query4 = 
				"update tb_smart_day_domain_fact a  "+
				"set new_pv_cnt = (  select pv_adjust_value "+
				"                    from   tb_smart_day_extreme_new_fact "+
				"                    where  a.access_day = access_day "+
				"                    and    a.site_id = site_id "+
				"                    and    a.domain_url = domain_url "+
				"                    and    a.panel_id = panel_id "+
				"                 ) "+
				"where access_day = '"+accessday+"' ";
				
		this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
		
		String query5 = 
				"update tb_smart_day_domain_fact "+
				"set new_pv_cnt = pv_cnt "+
				"where new_pv_cnt is null "+
				"and   access_day = '"+accessday+"' ";
				
		this.pstmt = connection.prepareStatement(query5);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
		
		if(term == 1 || term == 2){
			String query6 = 
			"INSERT INTO tb_smart_day_extreme_new_fact "+
			"           (access_day, site_id, domain_url, panel_id, p30, p29, p28, p1, psum28, psum_all, "+
			"            pv_fvalue, pv_adjust_value_f, pv_adjust_value_c, panel_cnt, "+
			"            panel_fvalue, pv_adjust_value) "+
			"select access_day, site_id, domain_url, panel_id, p30, p29, p28, p1, psum28, psum_all,  "+
			"       pv_new_fvalue, '', '', panel_cnt, "+
			"       panel_new_fvalue, pv_new_adjusted_value "+
			"from "+
			"( "+
			"     select access_day, site_id, domain_url, "+
			"            p30, p29, p28, p27, p1, p0, psum28, psum29, psum27, psum2930, psum30, psum_all, top_rank2, "+
			"            round((psum28+2*p28-30*p1)/(top_rank-3),2) pa, "+
			"            decode((psum28+2*p28-30*p1)/(top_rank-3),0,0 ,decode((psum28+2*p28-top_rank*p1),0,0, decode(sign(p30-101),-1,0, "+
			"            round((p30-p29)/((psum28+2*p28-top_rank*p1)/(top_rank-3)),2)))) pv_fvalue, "+
			"            panel_cnt, "+
			"            panel_id, "+
			"            sd_pv, "+
			"            avg_pv, "+
			"            decode(sign(panel_cnt-31),-1,kclick.fn_fvalue3(top_rank2),kclick.fn_fvalue2(top_rank2)) panel_new_fvalue, "+
			"            round(((((p30-1*p0)/decode((p29-p0),0,0.0001,(p29-p0)))-1)/1)/((((psum28-(top_rank2-2)*p0)/decode((p29-p0),0,0.0001,(p29-p0)))+1+1)/(top_rank-1)),2) pv_new_fvalue, "+
			"            round(p29+decode(sign(panel_cnt-31),-1,kclick.fn_fvalue3(top_rank2),kclick.fn_fvalue2(top_rank2))*(((psum29-(top_rank2-1)*p0)+1*(p29-p0))/(top_rank2-1))) pv_new_adjusted_value "+
			"     from "+
			"     ( "+
			"             select access_day, site_id, "+
			"                    domain_url,  "+
			"                    min(case when pv_rank = 1 then panel_id end ) panel_id, "+
			"                    sum(case when pv_rank >= 1 and pv_rank <= top_rank2 then new_pv_cnt end )  psum30, "+
			"                    sum(case when pv_rank >= 2 and pv_rank <= top_rank2 then new_pv_cnt end )  psum29, "+
			"                    sum(case when pv_rank >= 3 and pv_rank <= top_rank2 then new_pv_cnt end )  psum28, "+
			"                    sum(case when pv_rank >= 4 and pv_rank <= top_rank2 then new_pv_cnt end )  psum27, "+
			"                    sum(case when pv_rank <= 2 then new_pv_cnt end )  psum2930, "+
			"                    sum(case when pv_rank = 1 then new_pv_cnt end )    p30, "+
			"                    sum(case when pv_rank = 2 then new_pv_cnt end )    p29, "+
			"                    sum(case when pv_rank = 3 then new_pv_cnt end )    p28, "+
			"                    sum(case when pv_rank = 4 then new_pv_cnt end )    p27, "+
			"                    sum(case when pv_rank = top_rank2 then new_pv_cnt end ) p1, "+
			"                    sum(case when pv_rank = top_rank2+1 then new_pv_cnt end ) p0, "+
			"                    min(panel_cnt) panel_cnt, "+
			"                    min(decode(sign(top_rank2-30),-1, top_rank2+1, 31))  top_rank, "+
			"                    min(decode(sign(top_rank2-30),-1, top_rank2, 30)) top_rank2, "+
			"                    sum(new_pv_cnt)    psum_all, "+
			"                    sum(duration)  dsum_all, "+
			"                    stddev(new_pv_cnt) sd_pv, "+
			"                    avg(new_pv_cnt) avg_pv "+
			"             from        "+
			"             ( "+
			"                    select access_day, site_id, panel_id, domain_url, new_pv_cnt, duration, pv_rank, dt_rank, panel_cnt, "+
			"                           decode(sign(panel_cnt-31),-1,5,top_rank2) top_rank2 "+
			"                    from  "+
			"                        ( "+
			"                         select access_day, site_id, panel_id, domain_url, new_pv_cnt, duration,  "+
			"                                rank() over ( partition by access_day, domain_url order by new_pv_cnt   desc, rownum ) pv_rank, "+
			"                                rank() over ( partition by access_day, domain_url order by duration   desc, rownum ) dt_rank, "+
			"                                sum(1) over ( partition by access_day, domain_url)  panel_cnt, "+
			"                                round(sum(1) over (partition by access_day, domain_url)/4)  top_rank2 "+
			"                         from  kclick.tb_smart_day_domain_fact "+
			"                         where access_day = '"+accessday+"' "+
			"                         and   site_id not in ( "+
			"                                                select site_id  "+
			"                                                from  tb_site_info  "+
			"                                                where category_code2 in ( 'ZP','ZY')  "+
			"                                                and   exp_time >= sysdate "+
			"                                                and   ef_time  <= sysdate "+
			"                                              ) "+
			"                        ) "+
			"             ) "+
			"             group by access_day, site_id, domain_url "+
			"             having min(panel_cnt) >= 6 "+
			"     ) "+
			") "+
			"where pv_new_fvalue > panel_new_fvalue "+
			"and   PV_NEW_ADJUSTED_VALUE < p30 "+
			"and   p30>2*p29 "+
			"and   p30-p29>2*(p29-p28) "+
			"and   p30 >30 ";
			
			this.pstmt = connection.prepareStatement(query6);
			this.pstmt.executeUpdate();
			
			if(this.pstmt!=null) this.pstmt.close();
		}
		
		String query7 = "";
		if(term==1){
			query7 = 
					"insert into tb_smart_day_extreme_new_fact "+
					"( access_day, site_id, domain_url, panel_id, p30, pv_adjust_value) "+
					"select access_day, site_id, domain_url, panel_id, p30, pv_adjust_value from  "+
					"( "+
					"select access_day, site_id, domain_url, panel_id, "+
					"       pv_cnt, round(exp(pv_adjust_value)+0.5) pv_adjust_value, sum(case when pv_cnt > round(exp(pv_adjust_value)+0.5) then 1  end) over ( partition by access_day, domain_url) M_count,  "+
					"       pv_cnt-round(exp(pv_adjust_value)+0.5) diff, p30, p29, p28, panel_cnt "+
					"from  ( "+
					"        select access_day, site_id, domain_url, panel_id,  "+
					"               round(exp(pv_cnt)) pv_cnt, p30, p29, p28, panel_cnt, "+
					"               case when m_value1 >1.5 then 1.5*decode(s,0,1.0001,s)+t0 else null end pv_adjust_value "+
					"        from   ( "+
					"                        select  /*+ use_hash(a,b) */ "+
					"                                a.access_day access_day, a.site_id, a.domain_url domain_url, a.panel_id, S, t0, t1, p30, p29, p28, panel_cnt, "+
					"                                round((pv_cnt-t0)/decode(s,0,0.0001,s), 2) M_value1, pv_cnt, "+
					"                                round((pv_cnt-t1)/decode(s,0,0.0001,s), 2) M_value2 "+
					"                        from "+
					"                            (    "+
					"                            select a.access_day access_day, a.site_id, a.domain_url domain_url, panel_id, "+
					"                                   1.483*decode(mod(panel_cnt,2), 1, pv_med1, (pv_med1+pv_med2)/2) S,  "+
					"                                   pv_med t0, t1, p30, p29, p28, panel_cnt "+
					"                            from      "+
					"                                  ( "+
					"                                     select access_day, domain_url, site_id, "+
					"                                            min(panel_cnt1) panel_cnt, "+
					"                                            min(case when pv_rank = round(panel_cnt1/2) then pv_cnt1 end) pv_med1, "+
					"                                            min(case when pv_rank = round(panel_cnt1/2)+1 then pv_cnt1 end) pv_med2, "+
					"                                            min(pv_med) pv_med, "+
					"                                            min(t1) t1, "+
					"                                            min(p30) p30, "+
					"                                            min(p29) p29, "+
					"                                            min(p28) p28 "+
					"                                     from      "+
					"                                           ( "+
					"                                             select a.access_day access_day, "+
					"                                                    a.site_id, "+
					"                                                    a.domain_url domain_url,  "+
					"                                                    panel_id,             "+
					"                                                    abs(pv_cnt-t1) pv_cnt1, "+
					"                                                    abs(pv_cnt-pv_med) pv_cnt2, "+
					"                                                    pv_med, t1, p30, p29, p28, "+
					"                                                    rank() over ( partition by a.access_day, a.domain_url order by abs(pv_cnt-t1) desc, rownum ) pv_rank, "+
					"                                                    sum(1) over ( partition by a.access_day, a.domain_url)  panel_cnt1 "+
					"                                             from        "+
					"                                                     ( "+
					"                                                         select access_day, site_id, domain_url, panel_id, ln(pv_cnt) pv_cnt, duration        "+
					"                                                         from  kclick.tb_smart_day_domain_fact "+
					"                                                         where access_day = '"+accessday+"' "+
					"                                                         and   site_id not in ( "+
					"                                                                                select site_id  "+
					"                                                                                from  tb_site_info  "+
					"                                                                                where category_code2 in ( 'ZP','ZY')  "+
					"                                                                                and   exp_time >= sysdate "+
					"                                                                                and   ef_time  <= sysdate "+
					"                                                                              ) "+
					"                                                     ) a, "+
					"                                                     ( "+
					"                                                     select access_day, domain_url, site_id, "+
					"                                                            decode(mod(panel_cnt,2), 1, pv_med1, (pv_med1+pv_med2)/2) pv_med, t1, p30, p29, p28 "+
					"                                                     from      "+
					"                                                         ( "+
					"                                                             select access_day,  "+
					"                                                                    site_id, "+
					"                                                                    domain_url,  "+
					"                                                                    min(panel_cnt) panel_cnt, "+
					"                                                                    min(case when pv_rank = round(panel_cnt/2) then pv_cnt end) pv_med1, "+
					"                                                                    min(case when pv_rank = round(panel_cnt/2)+1 then pv_cnt end) pv_med2, "+
					"                                                                    avg(case when pv_rank >= panel_cnt*0.25 and pv_rank <= panel_cnt*0.75 then pv_cnt end )  t1, "+
					"                                                                    min(case when pv_rank = 1 then round(exp(pv_cnt)) end) p30, "+
					"                                                                    min(case when pv_rank = 2 then round(exp(pv_cnt)) end) p29, "+
					"                                                                    min(case when pv_rank = 3 then round(exp(pv_cnt)) end) p28 "+
					"                                                              from        "+
					"                                                             ( "+
					"                                                                 select access_day, site_id, domain_url, panel_id, ln(pv_cnt) pv_cnt, duration,  "+
					"                                                                        rank() over ( partition by access_day, domain_url order by pv_cnt   desc, rownum ) pv_rank, "+
					"                                                                        sum(1) over ( partition by access_day, domain_url)  panel_cnt "+
					"                                                                 from  kclick.tb_smart_day_domain_fact "+
					"                                                                 where access_day = '"+accessday+"' "+
					"                                                                 and   site_id not in ( "+
					"                                                                                        select site_id  "+
					"                                                                                        from  tb_site_info  "+
					"                                                                                        where category_code2 in ( 'ZP','ZY')  "+
					"                                                                                        and   exp_time >= sysdate "+
					"                                                                                        and   ef_time  <= sysdate "+
					"                                                                                      ) "+
					"                                                             ) "+
					"                                                             where panel_cnt<=5 "+
					"                                                             and   panel_cnt>=2 "+
					"                                                             group by access_day, site_id, domain_url "+
					"                                                         )   "+
					"                                                     ) b "+
					"                                                where a.access_day=b.access_day "+
					"                                                and   a.site_id=b.site_id "+
					"                                                and   a.domain_url=b.domain_url "+
					"                                                and   (p30>20 or p29>20)     "+
					"                                    ) "+
					"                                    group by access_day, domain_url, site_id "+
					"                                 ) a, "+
					"                                 ( "+
					"                                 select access_day, site_id, domain_url, panel_id, ln(pv_cnt) pv_cnt, duration        "+
					"                                 from  kclick.tb_smart_day_domain_fact "+
					"                                 where access_day = '"+accessday+"' "+
					"                                 and   site_id not in ( select site_id from kclick.vi_site_info where category_code2 in ( 'ZP','ZY') ) "+
					"                                 ) b "+
					"                             where a.access_day=b.access_day "+
					"                             and   a.site_id=b.site_id "+
					"                             and   a.domain_url=b.domain_url "+
					"                          ) a, "+
					"                          ( "+
					"                          select access_day, site_id, domain_url, panel_id, ln(pv_cnt) pv_cnt "+
					"                          from  kclick.tb_smart_day_domain_fact "+
					"                          where access_day = '"+accessday+"' "+
					"                          and   site_id not in ( "+
					"                                                    select site_id  "+
					"                                                    from  tb_site_info  "+
					"                                                    where category_code2 in ( 'ZP','ZY')  "+
					"                                                    and   exp_time >= sysdate "+
					"                                                    and   ef_time  <= sysdate "+
					"                                                ) "+
					"                          ) b "+
					"                    where a.access_day=b.access_day "+
					"                    and   a.site_id=b.site_id "+
					"                    and   a.domain_url=b.domain_url "+
					"                    and   a.panel_id=b.panel_id "+
					"                ) "+
					"        ) A "+
					") "+
					"where m_count <=2  "+
					"and   p30-p29 > 2*(p29-p28) "+
					"and   (p30 > 2*p29 or p29>2*p28) "+
					"and   0< pv_cnt - pv_adjust_value ";
		} else if (term == 2){
			query7 = 
					"UPDATE tb_smart_day_extreme_new_fact c " +
					"set p30 = (" +
//					"	SELECT ceil(a.p30/(FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*b.KC_N_FACTOR)) "+
//					"	FROM   tb_smart_day_extreme_new_fact a, tb_day_panel_seg b "+
//					"	WHERE  a.panel_id = b.panel_id "+
//					"	AND    a.access_day = b.access_day "+
//					"	AND    a.access_day = '"+accessday+"' "+
//					"	AND    c.access_day = a.access_day "+
//					"	AND    c.panel_id = a.panel_id "+
//					"	AND    c.domain_url = a.domain_url "+
					"	SELECT ceil(a.p30/(FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*b.KC_N_FACTOR)) "+
					"	FROM   tb_smart_day_extreme_new_fact a, tb_day_total_panel_seg b "+
					"	WHERE  a.panel_id = b.panel_id "+
					"	AND    a.access_day = b.access_day "+
					"	AND    a.access_day = '"+accessday+"' "+
					"	AND    c.access_day = a.access_day "+
					"	AND    c.panel_id = a.panel_id "+
					"	AND    c.domain_url = a.domain_url "+
					") "+
					"where c.access_day = '"+accessday+"' ";
		}
		if(term == 1 || term == 2) {
			this.pstmt = connection.prepareStatement(query7);
			this.pstmt.executeUpdate();
			
			if(this.pstmt!=null) this.pstmt.close();
		}
		
		if(term == 2){
			String query8 = 
					"UPDATE tb_smart_day_extreme_new_fact c " +
					"set pv_adjust_value = ( " +
//					"	SELECT ceil(a.pv_adjust_value/(FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*b.KC_N_FACTOR)) "+
//					"   FROM   tb_smart_day_extreme_new_fact a, tb_day_panel_seg b "+
//					"   WHERE  a.panel_id = b.panel_id "+
//					"   AND    a.access_day = b.access_day "+
//					"   AND    a.access_day = '"+accessday+"' "+
//					"   AND    c.access_day = a.access_day "+
//					"   AND    c.panel_id = a.panel_id "+
//					"   AND    c.domain_url = a.domain_url "+
					"	SELECT ceil(a.pv_adjust_value/(FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*b.KC_N_FACTOR)) "+
					"   FROM   tb_smart_day_extreme_new_fact a, tb_day_total_panel_seg b "+
					"   WHERE  a.panel_id = b.panel_id "+
					"   AND    a.access_day = b.access_day "+
					"   AND    a.access_day = '"+accessday+"' "+
					"   AND    c.access_day = a.access_day "+
					"   AND    c.panel_id = a.panel_id "+
					"   AND    c.domain_url = a.domain_url "+
					") "+
					"WHERE c.access_day = '"+accessday+"' ";
			this.pstmt = connection.prepareStatement(query8);
			this.pstmt.executeUpdate();
			
			if(this.pstmt!=null) this.pstmt.close();
		}
		
		if(term == 2 || term == 3){
			String query9 = 
					"DELETE  "+
					"FROM   tb_smart_day_extreme_new_fact "+
					"WHERE  p30 = pv_adjust_value "+
					"AND    access_day = '"+accessday+"' ";
			this.pstmt = connection.prepareStatement(query9);
			this.pstmt.executeUpdate();
			
			if(this.pstmt!=null) this.pstmt.close();
		}
	}
	
	
	
	/**************************************************************************
	 *		메소드명		: executeExtremeUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 극단값 처리가 될 특정 rowid에 업데이트문 (UPDATE)
	 *************************************************************************/
	
	public void executeExtremeUpdate(String rowid, int term) throws SQLException 
	{
		String type = "";
		if(term == 1){
			type = "E";
		} else if(term == 2){
			type = "X";
		} else if(term == 3){
			type = "G";
		}
		String query = "update tb_smart_browser_itrack set result_cd='"+type+"' where rowid='"+rowid+"'";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
	}
	
	/**************************************************************************
	 *		메소드명		: executeServerUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 서버데이트 업데이트문 (UPDATE)
	 *************************************************************************/	
	public Calendar executeServerUpdate(String accessday) throws SQLException 
	{
		Calendar beginPt = Calendar.getInstance();
		System.out.print("Serverdate Update is beginning...");
		String query = "update   tb_smart_panel "+
						"set     PANEL_STATUS_CD=1 "+
						"where   panel_id in ( "+
						"    select  distinct panel_id "+
						"    from    tb_smart_day_app_wgt "+
						"    where   access_day = '"+accessday+"' "+
						") "+
						"and ISAGREE='Y' ";
		//System.out.println(query);
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();	
		
		String queryEnv =  "update /*+bypass_ujvc*/ "+
							"(  "+
							"    select /*+ordered*/ b.STATUS_ENV ENV, a.server_date sdate "+
							"    from "+
							"    ( "+
							"        select /*+parallel(a,8)*/panel_id, to_date(max(access_day),'yyyymmdd') server_date  "+
							"        from   tb_smart_day_app_wgt a "+
							"        where  access_day >= to_char(sysdate-6,'yyyymmdd') "+
							"        group by panel_id "+
							"    ) a, "+
							"    ( "+
							"        select * "+
							"        from tb_smart_panel "+
							"    ) b "+
							"    where a.panel_id=b.panel_id "+
							") "+
							"set ENV=sdate ";
		//System.out.println(queryEnv);
		this.pstmt = connection.prepareStatement(queryEnv);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();	
		
		String queryDevice =    "update /*+bypass_ujvc*/ "+
								"(  "+
								"    select /*+ordered*/ b.status_device device, a.server_date sdate "+
								"    from "+
								"    ( "+
								"        select /*+parallel(a,8)*/panel_id,  "+
								"               case when max(server_date) > sysdate then sysdate "+
								"               else max(server_date) end as server_date "+
								"        from   tb_smart_device_itrack a "+
								"        where  access_day >= sysdate-6 "+
								"        group by panel_id "+
								"    ) a, "+
								"    ( "+
								"        select * "+
								"        from tb_smart_panel "+
								"    ) b "+
								"    where a.panel_id=b.panel_id "+
								") "+
								"set device=sdate ";
		//System.out.println(queryDevice);
		this.pstmt = connection.prepareStatement(queryDevice);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();

		String queryApp ="update /*+bypass_ujvc*/ "+
						"(  "+
						"    select /*+ordered*/ b.status_app app, a.server_date sdate "+
						"    from "+
						"    ( "+
						"        select /*+parallel(a,8)*/panel_id, to_date(max(access_day),'yyyymmdd') server_date  "+
						"        from   tb_smart_day_setup_wgt a "+
						"        where  access_day >= to_char(sysdate-6,'yyyymmdd') "+
						"        group by panel_id "+
						"    ) a, "+
						"    ( "+
						"        select * "+
						"        from tb_smart_panel "+
						"    ) b "+
						"    where a.panel_id=b.panel_id "+
						") "+
						"set app=sdate ";
		//System.out.println(queryApp);
		this.pstmt = connection.prepareStatement(queryApp);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String queryVersion =
				 "update /*+bypass_ujvc*/ ( "+
				 "    SELECT /*+ORDERED*/ B.TRACK_VERSION VERSION, A.VERSION UPDATE_VERSION "+
				 "    from "+
				 "    ( "+
				 "        select panel_id, max(track_version) VERSION "+
				 "        from "+
				 "        ( "+
				 "            select  /*+parallel(a,8)*/panel_id, track_version, "+
				 "                    rank() over(partition by panel_id order by access_day desc) rn "+
				 "            from    tb_smart_device_itrack a "+
				 "            where   access_day >= to_char(sysdate-30,'yyyymmdd') "+
				 "        ) "+
				 "        where rn =1 "+
				 "        group by panel_id "+
				 "    ) a, "+
				 "    ( "+
				 "        select * "+
				 "        from tb_smart_panel "+
				 "    ) b "+
				 "    where a.panel_id=b.panel_id "+
				 ") "+
				 "SET VERSION=UPDATE_VERSION ";
		System.out.println(queryVersion);
		this.pstmt = connection.prepareStatement(queryVersion);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return beginPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeDurationUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Timegap과 Duration을 특정 rowid에 업데이트문 (UPDATE)
	 *************************************************************************/
		
	public void executeDurationUpdate(String rowid, int timegap, int duration) throws SQLException 
	{
		String query = "update tb_smart_browser_itrack set time_gap= ? , duration= ? where rowid= ? ";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.setInt(1, timegap);
		this.pstmt.setInt(2, duration);
		this.pstmt.setString(3, rowid);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
//		System.exit(0);
		if(this.pstmt!=null) this.pstmt.close();
	}
	
	public void executeBrowserEnvTest(String rowid) throws SQLException 
	{
		String query = "update temp_hwlee_itrack_brow set result_cd='A' where rowid='"+rowid+"' and result_cd = 'S' ";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
//		System.exit(0);
		if(this.pstmt!=null) this.pstmt.close();
	}
	
	
	
	/**************************************************************************
	 *		메소드명		: executeEnvDurationUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Timegap과 Duration을 특정 rowid에 업데이트문 (UPDATE)
	 *************************************************************************/
	
	public void executeEnvDurationUpdate(String rowid, int timegap, int duration) throws SQLException 
	{
		String query = "update 	tb_smart_env_itrack " +
					   //"update 	TEMP_JSPARK_SMART_ENV_ITRACK " +
					   "set 	time_gap= ? , duration=? " +
					   "where 	rowid= ? ";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.setInt(1,timegap);
		this.pstmt.setInt(2,duration);
		this.pstmt.setString(3, rowid);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
//		System.exit(0);
		if(this.pstmt!=null) this.pstmt.close();
	}

	/**************************************************************************
	 *		메소드명		: executeEnvRxbyteUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Rxbyte_gap과Txbyte_gap을 특정 Rowid에 업데이트문 (UPDATE)
	 *************************************************************************/
	
	public void executeEnvRxbyteUpdate(String rowid, long rxbytegap, long txbytegap) throws SQLException 
	{
		String query = "update tb_smart_env_itrack set rxbyte_gap=? , txbyte_gap=? where rowid=? ";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.setLong(1,rxbytegap);
		this.pstmt.setLong(2,txbytegap);
		this.pstmt.setString(3, rowid);
		this.pstmt.executeUpdate();
		
		//System.out.println(query);
		//System.exit(0);
		if(this.pstmt!=null) this.pstmt.close();
	}
	
	/**************************************************************************
	 *		메소드명		: executeEnvDurationUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 嚥▲끇�봺占쎈솚 flag 6 占쎈쑓占쎌뵠占쎄숲 占쎈씜占쎈쑓占쎌뵠占쎈뱜
	 *************************************************************************/	
	public void executeUpdate_flag6_duration(String accessday) throws SQLException 
	{
		String query =  "update /*+bypass_ujvc*/ "+ 
						"( "+ 
						"    select a.duration  org_duration, b.duration copy_duration "+ 
						"    from "+ 
						"    ( "+ 
						"        select ACCESS_DAY, PANEL_ID, REGISTER_DATE, max(duration)duration "+ 
						"        from tb_smart_env_itrack a "+ 
						"        where access_day = '"+accessday+"' "+ 
						"        and flag = '6' "+ 
						"        group by ACCESS_DAY, PANEL_ID, REGISTER_DATE "+ 
						"    ) a  ,tb_smart_env_itrack b "+ 
						"    where b.access_day = '"+accessday+"' "+ 
						"    and a.panel_id = b.panel_id "+ 
						"    and  a.REGISTER_DATE = b.REGISTER_DATE "+ 
						"    and  b.flag = '6' "+ 
						") "+ 
						"set copy_duration = org_duration ";

		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		query = "update tb_smart_env_itrack set duration = 0 " +
				"where access_day = '"+accessday+"' " +
				"and flag = '6' " +
				"and item_value in " +
				"                    ( " +
				"                    'com.lge.smartshare.provider','com.lge.smartshare.homecloud','com.lge.smartshare','com.lge.settings.easy', " +
				"                    'com.android.providers.media','com.android.providers.userdictionary','com.android.vending','com.google.android.gms', " +
				"                    'com.android.providers.partnerbookmarks','com.google.android.tts','com.sec.android.provider.logsprovider','com.lge.sizechangable.weather.platform', " +
				"                    'com.android.mms.service','com.sec.android.provider.badge','com.android.providers.calendar','com.lge.eula','com.airplug.abc.agent', " +
				"                    'com.skt.skaf.Z00000TAPI','com.lge.cloudhub','com.lge.mrg.boardcontent','com.android.defcontainer','com.google.android.googlequicksearchbox', " +
				"                    'com.sktelecom.smartcard.SmartcardService' " +
				"                    ) " ; 
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();		
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeEnvDurationUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Timegap과 Duration을 특정 rowid에 업데이트문 (UPDATE)
	 *************************************************************************/
	
	public void executeTaskDurationUpdate(String accessday) throws SQLException 
	{
		String query =  "update tb_smart_env_itrack "+
						"set    time_gap = 60, duration = 60 "+
						"where  access_day = '"+accessday+"' "+
						"and    item_value in ( "+
						"    select package_name "+
						"    from   tb_smart_app_media_list "+
						"    where  exp_time > sysdate "+
						"    and    ef_time < sysdate "+
						") "+
						"and     flag in ('3') "+
						"and     duration is null "+
						"and     time_gap is null "+
						"and     to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 ";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();

		
		String query2 =	"update tb_smart_env_itrack set duration = 0 "+
				"where rowid in "+
				"( "+
				"    select b.rid "+
				"    from "+
				"    ( "+
				"        select panel_id,item_value, register_date, register_date + duration/(24*60*60) end_date,duration "+
				"        from tb_smart_env_itrack "+
				"        where access_day = '"+accessday+"' "+
				"        and    flag = 1 "+
				"        and    item_value in ( "+
				"            select package_name "+
				"            from   tb_smart_app_media_list "+
				"            where  exp_time > sysdate "+
				"        )  "+
				"        and    to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
				"    )a, "+
				"    ( "+
				"        select rowid rid, panel_id,item_value, register_date, duration "+
				"        from tb_smart_env_itrack "+
				"        where access_day = '"+accessday+"' "+
				"        and    flag = 3 "+
				"        and    item_value in ( "+
				"            select package_name "+
				"            from   tb_smart_app_media_list "+
				"            where  exp_time > sysdate "+
				"            and    ef_time < sysdate "+
				"        )  "+
				"        and    to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
				"    )b "+
				"    where  a.panel_id = b.panel_id "+
				"    and a.item_value = b.item_value "+
				"    and a.register_date <= b.register_date "+
				"    and a.end_date > b.register_date "+
				") ";

		this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
//		System.out.println(query);
//		System.exit(0);
		if(this.pstmt!=null) this.pstmt.close();
	}
	
	/**************************************************************************
	 *		메소드명		: executeAppInsert
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 app info를 대표로 삽입
	 *************************************************************************/
	
	public void executeAppInsert(AppModel appmodel) throws SQLException 
	{
		String query = "insert into tb_smart_app_info values " +
				"(0, "+appmodel.getSmartid()+", '"+appmodel.getPackagename()+"', " +
				"'"+appmodel.getAppname()+"', '', '', sysdate, to_date('9999/01/01','YYYY/MM/DD'), null, null, null)";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
  
		if(this.pstmt!=null) this.pstmt.close();
	}
	
	/**************************************************************************
	 *		메소드명		: executeAppDelete
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 app info에서 package_name이 :이 들어간 것들을 제외처리
	 *************************************************************************/
	
	public void executeAppDelete() throws SQLException 
	{
		String query = "update tb_smart_app_info "+
					"set EXP_TIME = sysdate "+
					"where package_name like '%:%' "+
					"and EXP_TIME > sysdate";
		
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
	
		if(this.pstmt!=null) this.pstmt.close();
	}

	/**************************************************************************
	 *		메소드명		: executeProviderInsert
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 공급자 정보를 삽입
	 *************************************************************************/
	
	public void executeProviderInsert(AppModel appmodel) throws SQLException, UnsupportedEncodingException 
	{
		String proname = appmodel.getProvider();
		proname = proname.replace("'", "''");
		int smartid = appmodel.getSmartid();
		String title = appmodel.getTitle();
		title = title.replace("'", "''");
		String package_name = appmodel.getPackagename();
		int siteid = 0;
		
		//package_name占쎈퓠占쎄퐣 占쎈뱟占쎌젟 占쎈솭占쎄쉘占쏙옙 沃섎챶�봺 占쎈쾻嚥∽옙
		String sql = 
				 "select PRO_ID, SITE_ID, PACKAGE_NAME, EXCEPTION " +
			     "from  tb_smart_provider_pattern ";   
		try {
			ResultSet rs = null;
			this.pstmt = connection.prepareStatement(sql);
			rs = this.pstmt.executeQuery(sql);
			while (rs.next()) {		
				int pproid = rs.getInt("PRO_ID");
				int psiteid = rs.getInt("SITE_ID");
				String ppsiteid = "";
				String ppackagename = rs.getString("PACKAGE_NAME");
				String pexception = rs.getString("EXCEPTION");
				if(psiteid==0){
					ppsiteid = "null";
				} else {
					ppsiteid = Integer.toString(psiteid);
				}
				
				if(package_name.startsWith(ppackagename)){
					if(pexception==null || (pexception!=null && !package_name.contains(pexception))){
						String updatepro = 
								  "update tb_smart_app_info "+
								  "set pro_id = "+pproid+", site_id = "+ppsiteid+" "+
								  "where smart_id = "+smartid;
						this.pstmt = connection.prepareStatement(updatepro);
						
						
						//2013占쎈�� 12占쎌뜞 占쎄텣占쎌젫
						//this.pstmt.executeUpdate();
						if(this.pstmt!=null) this.pstmt.close();
					}
				}
		  	}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//占쎈늄嚥≪뮆而�占쎌뵠占쎈쐭 筌뤿굞肉됵옙苑� 占쎌젫占쎌뇚占쎈┷占쎈뮉 野껉퍓諭� 占쎈땾占쎌젟
		ArrayList<String> thewords = new ArrayList<String>();
		thewords.add("Ltd");
		thewords.add("ltd");
		thewords.add("LTD");
		thewords.add("AG");
		thewords.add("SA");
		thewords.add("LLC");
		thewords.add("GmbH");
		thewords.add("SpA");
		thewords.add("PLC");
		thewords.add("Pty");
		thewords.add("Oy");
		for(int j=0; j<thewords.size();j++){
			String theword = thewords.get(j);
			if(proname.contains(theword)){
				int index1 = (proname.indexOf(theword)-1>0) ? proname.indexOf(theword)-1:0;
				int index2 = (proname.indexOf(theword)-2>0) ? proname.indexOf(theword)-2:0;
				String check1 = proname.substring(index1,proname.indexOf(theword));
				String check2 = proname.substring(index2,proname.indexOf(theword));
				if(check2.equals(", ")){
				  proname = proname.replace(", "+theword+".", "");
				  proname = proname.replace(", "+theword, "");
				} else if(check1.equals(",")){
				  proname = proname.replace(","+theword+".", "");
				  proname = proname.replace(","+theword, "");
				} else if(check1.equals(" ")){
				  proname = proname.replace(" "+theword+".", "");
				  proname = proname.replace(" "+theword, "");
				} else if(check1.equals(".")){
				  proname = proname.replace(theword+".", "");
				  proname = proname.replace(theword, "");
				}
			}
		}
		
		//占쎌굙占쎌뇚�놂옙占쎌뵠占쎈뮞 占쎌젫椰꾬옙
		if(proname.equals("NEW CONTENT")){
			proname = "";
		}
		//占쎈늄嚥≪뮆而�占쎌뵠占쎈쐭揶쏉옙 鈺곕똻�삺占쎈릭占쎈뮉 野껋럩�뒭, 占쎈씜占쎈쑓占쎌뵠占쎈뱜
		if(!(proname.equals("")||proname==null)){
			
			ResultSet rs = null;
			int proid=0;
			String procheck = "select pro_id "+
							  "from ( "+
							  "    select PRO_ID, rank() over(order by pro_name) rnk "+
							  "    from tb_smart_provider_name_info "+
							  "    where lower(pro_name) like lower('"+proname+"') "+
							  ") where rnk = 1 ";
			try {
				this.pstmt = connection.prepareStatement(procheck);
				rs = this.pstmt.executeQuery(procheck);
				
				while (rs.next()) {	
					if(rs.getInt("pro_id") != 0){
						proid = rs.getInt("pro_id");
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if(this.pstmt!=null) this.pstmt.close();
			}
			
			if(proid != 0){
				String updatepro = 
						  "update tb_smart_app_info "+
						  "set pro_id = "+proid+
						  "where smart_id = "+smartid;
				this.pstmt = connection.prepareStatement(updatepro);
				
				
				//2013占쎈�� 12占쎌뜞 占쎄텣占쎌젫
				//this.pstmt.executeUpdate();
				if(this.pstmt!=null) this.pstmt.close();
				
				//proname="";
			}
		}
		
		//占쎌끏占쎌읈 占쎈짗占쎌뵬占쎈립 占쎈즲筌롫뗄�뵥占쎌뵠 鈺곕똻�삺占쎈릭占쎈뮉 野껋럩�뒭 site_id 占쎈씜占쎈쑓占쎌뵠占쎈뱜
		String sitename = appmodel.getSite().toLowerCase();
		if(!(sitename.equals("")||sitename==null)){
			//占쎈츟占쎈퓠 /揶쏉옙 �겫�늿堉깍옙�뿳占쎈뮉 野껋럩�뒭 占쎌젫椰꾬옙
			if(sitename.substring(sitename.length()-1).equals("/")){
				sitename=sitename.substring(0,sitename.length()-1);
			}
			
			//http://占쎌젫椰꾬옙
			if(sitename.contains("http://")){
				sitename = sitename.substring(sitename.indexOf("http://")+7);
			} else if(sitename.contains("https://")){
				sitename = sitename.substring(sitename.indexOf("https://")+8);
			}
			
			//domain_info占쎈퓠占쎄퐣 �뜮袁㏉꺍
			ResultSet rs = null;
			String sitecheck ="select site_id "+
							  "from vi_site_info "+
							  "where url_link = '"+sitename+"' ";
			try {
				this.pstmt = connection.prepareStatement(sitecheck);
				rs = this.pstmt.executeQuery(sitecheck);
				
				while (rs.next()) {	
					if(rs.getInt("site_id") != 0){
						siteid = rs.getInt("site_id");
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if(this.pstmt!=null) this.pstmt.close();
			}
			
			//占쎌뿳占쎌몵筌롳옙 獄쏅뗀以� 占쎈쾻嚥≪빜鍮먧빳占쏙옙�뼄. 127.0.0.1占쏙옙 占쎌젫占쎌뇚
			if(siteid != 0 && siteid != 8930){
				//nhnCorp占쎌뵬 野껋럩�뒭 naver.com�⑨옙 占쎈퉸占쎈뼣 �⑤벀�닋占쎌쁽嚥∽옙 占쎈쾻嚥∽옙
				if(siteid == 14445){
					String updatesite = 
							  "update tb_smart_app_info "+
							  "set site_id = 178, pro_id = 3126 "+
							  "where smart_id = "+smartid;
					this.pstmt = connection.prepareStatement(updatesite);
					

					//2013占쎈�� 12占쎌뜞 占쎄텣占쎌젫
					//this.pstmt.executeUpdate();
					if(this.pstmt!=null) this.pstmt.close();
				} else {
					String updatesite = 
							  "update tb_smart_app_info "+
							  "set site_id = "+siteid+
							  "where smart_id = "+smartid;
					this.pstmt = connection.prepareStatement(updatesite);
					
					
					//2013占쎈�� 12占쎌뜞 占쎄텣占쎌젫
					//this.pstmt.executeUpdate();
					if(this.pstmt!=null) this.pstmt.close();
				}
			}
			
			//占쎄돌�솒紐꾬옙占쎈굶占쏙옙 domain_url占쎈뼊筌랃옙 占쎄텚疫뀀��뼄.
			if(sitename.contains("/")){
				sitename = sitename.substring(0,sitename.indexOf("/"));
			}
		}
		
		String query = "MERGE INTO tb_smart_provider_info "+
					    "USING DUAL"+
					       "ON (smart_id = "+appmodel.getSmartid()+")"+
					    "WHEN MATCHED THEN"+
					        "UPDATE SET  PRO_NAME = '"+new String(proname.getBytes("euc-kr"),"ksc5601")+"',"+
					                    "DOMAIN_URL = '"+sitename+"', "+
					                    "PATH_URL = '"+appmodel.getSite()+"', "+
					                    "CATEGORY = '"+new String(appmodel.getApp_type().getBytes("euc-kr"),"ksc5601")+"',"+
					                    "INSTALLS = "+appmodel.getInstalls()+" "+
					    "WHEN NOT MATCHED THEN"+
					        "insert (SMART_ID,PRO_NAME,DOMAIN_URL,PATH_URL,CATEGORY,INSTALLS) values "+
							"("+appmodel.getSmartid()+", '"+new String(proname.getBytes("euc-kr"),"ksc5601")+"', '"+sitename+"',"+
					        "'"+appmodel.getSite()+"', '"+new String(appmodel.getApp_type().getBytes("euc-kr"),"ksc5601")+"', "+appmodel.getInstalls()+" )";
		this.pstmt = connection.prepareStatement(query);
		try {
			this.pstmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if(this.pstmt!=null) this.pstmt.close();
		//System.out.println(query);
		//System.exit(0);

		//占쏙옙占쎌뵠占쏙옙 筌ｌ꼶�봺
		if(!(title.equals("")||title==null)){

			//System.out.println(title);
			String updatepro = 
					  "insert into tb_smart_app_name_info values ("+smartid+",'"+new String(title.getBytes("euc-kr"),"ksc5601")+"',sysdate,to_date('9999/01/01','yyyy/mm/dd'))";
			
			//System.out.println(updatepro);
			//System.exit(0);
			
			this.pstmt = connection.prepareStatement(updatepro);
			this.pstmt.executeUpdate();
			if(this.pstmt!=null) this.pstmt.close();
		}
	}
	
	/**************************************************************************
	 *		메소드명		: executeAppnameInsert
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 app name들을 삽입
	 *************************************************************************/
	
	public void executeAppnameInsert(AppModel appmodel) throws SQLException 
	{
		String query = "insert into tb_smart_app_name_info values " +
				"("+appmodel.getSmartid()+", '"+appmodel.getAppname()+"', sysdate, to_date('9999/01/01','YYYY/MM/DD'))";
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
	}
	
	/**************************************************************************
	 *		메소드명		: executeBanURL
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 BAN_URL를 KC에서부터 업데이트
	 *************************************************************************/
	
	public Calendar executeBanURL() throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "insert into tb_smart_ban_url_info (path_url, type_cd, description, EF_TIME, EXP_TIME, DURATION_FLAG) "+
					   "select PATH_URL, 'D' type_cd, 'SYSTEM 일괄등록' description, sysdate, to_date('9999/01/01'), null duration_flag "+
					   "from ( "+
					   "    select PATH_URL "+
					   "    from tb_ban_url_info "+
					   "    where exp_time > sysdate "+
					   "    minus "+
					   "    select PATH_URL "+
					   "    from tb_smart_ban_url_info "+
					   "    where exp_time > sysdate "+
					   ") ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeTopTask
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 Top Task를 Env에 삽입
	 *************************************************************************/
	
	public Calendar executeTopTask(String accessday) throws SQLException
	{
		Calendar eachPt = Calendar.getInstance();
		String query =  "insert into tb_smart_env_itrack "+
						"select ACCESS_DAY, PANEL_ID, PANEL_DEVICE_CODE, 'APP', 'TASK', TOP_PACKAGE_NAME, SERVER_DATE, '3', SERVER_DATE,  WIFISTATUS, null, null, '1', '1', '', '', '', '', '' "+
						"from ( "+
						"    select /*+parallel(a,8)*/ "+
						"           ACCESS_DAY, PANEL_ID, PANEL_DEVICE_CODE, TOP_PACKAGE_NAME, WIFISTATUS, SERVER_DATE "+
						"    from   TB_SMART_TASK_ITRACK a "+
						"    where  to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
						"    and    SCREEN = 1 "+
						"    and    APP_STATUS = 100 "+
						"    and    access_day = '"+accessday+"' "+
						"    and    panel_id in ( "+
						"        select distinct panel_id "+
						"        from   tb_smart_env_itrack "+
						"        where  access_day = '"+accessday+"' " +
						"		 and    track_version in ('3','4') "+
						"    ) "+
						"    group by ACCESS_DAY, PANEL_ID, PANEL_DEVICE_CODE, TOP_PACKAGE_NAME, WIFISTATUS, SERVER_DATE "+
						")";
		
//		System.out.println(query);
//		System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeNewDevice
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 Top Task를 Env에 삽입
	 *************************************************************************/
	
	public Calendar executeNewDevice() throws SQLException
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("Inserting New Device Info has began...");
		String query =  
				"insert into tb_smart_device_info "+
				"select model, '' series, '' producer, sysdate, to_date('99990101','yyyymmdd'), '' model_name, '' model_type "+
				"from ( "+
				"    select model "+
				"    from   tb_smart_device_itrack "+
				"    where  access_day >= to_char(sysdate-6,'yyyymmdd') "+
				"    group by model "+
				"    minus "+
				"    select MODEL "+
				"    from   tb_smart_device_info "+
				"    where  exp_time > sysdate "+
				"    and    ef_time < sysdate "+
				") ";
//		System.out.println(query);
//		System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	
	public Calendar executeFailrawDelete(String accessday) throws SQLException  
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "delete 	tb_smart_env_itrack "+
					   "where 	access_day = '"+accessday+"' "+
					   "and regexp_like(TRACK_VERSION,'^[a-zA-Z]')";
		System.out.print("The batch - Device Weight is processing...");
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	//20140430 추가 MSKIM
	/**************************************************************************
	 *		메소드명		: executeDailyDeviceFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형			: void
	 *		설명			: Device Daily Fact에 insert
	 *************************************************************************/
	public Calendar executeDailyDeviceFact(String access_day) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		if(countTable(access_day, "d", "tb_smart_day_device_wgt")){
			String query = 
					"insert into   tb_smart_day_device_wgt "+
					"select ACCESS_DAY, "+
					"       PANEL_ID, "+
					"       PANEL_DEVICE_CODE, "+
					"       TELECOM_CODE, "+
					"       MODEL, "+
					"       SDK_VERSION, "+
					"       IN_MEMORY, "+
					"       EX_MEMORY, "+
					"       RAM_SIZE, "+
					"       SERVER_DATE, "+
					"       track_version, "+
					"       DEVICE_MO_RXBYTE, "+
					"       DEVICE_MO_TXBYTE, "+
					"       DEVICE_TOTAL_RXBYTE, "+
					"       DEVICE_TOTAL_TXBYTE "+
					"from   tb_smart_device_itrack "+
					"where  (panel_id, server_date) in ( "+
					"    select panel_id, max(SERVER_DATE) max_server "+
					"    from   tb_smart_device_itrack "+
					"    where  access_day = '"+access_day+"' "+
					"    and    to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
					"    group by panel_id "+
					") ";
			
			System.out.print("The batch - Device Weight is processing...");
			this.pstmt = connection.prepareStatement(query);
			this.pstmt.executeUpdate();
			if(this.pstmt!=null) this.pstmt.close();
			
			String query1 = 
					"update tb_smart_day_device_wgt a "+
					"set TELECOM_CODE=(select decode(M_PHONE_COM,1,'SK',2,'KT',3,'LG') telecom_code from tb_panel where panelid = a.panel_id) "+
					"where TELECOM_CODE='NONE' "+
					"and access_day = '"+access_day+"' ";
			
			this.pstmt = connection.prepareStatement(query1);
			this.pstmt.executeUpdate();
			if(this.pstmt!=null) this.pstmt.close();
			
			String query2 = 
					"update tb_smart_day_device_wgt "+
					"set telecom_code = upper(substr(trim(replace(TELECOM_CODE,'塋딉옙','')),1,2)) "+
					"where access_day = '"+access_day+"' ";
			
			this.pstmt = connection.prepareStatement(query2);
			this.pstmt.executeUpdate();
			//System.out.println(query);
			System.out.println("DONE.");
			if(this.pstmt!=null) this.pstmt.close();
		} else {
			System.out.print("Device WGT already exists.");
		}
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeDeviceFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Device Monthly Fact에 insert
	 *************************************************************************/
	
	public Calendar executeDeviceFact(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		if(countTable(monthcode, "m", "tb_smart_month_device_wgt")){
			String query = 
					"insert into   tb_smart_month_device_wgt "+
					"select substr(ACCESS_DAY, 1, 6) monthcode, "+
					"       PANEL_ID, "+
					"       PANEL_DEVICE_CODE, "+
					"       TELECOM_CODE, "+
					"       MODEL, "+
					"       SDK_VERSION, "+
					"       IN_MEMORY, "+
					"       EX_MEMORY, "+
					"       RAM_SIZE "+
					"from   tb_smart_device_itrack "+
					"where  (panel_id, server_date) in ( "+
					"    select panel_id, max(SERVER_DATE) max_server "+
					"    from   tb_smart_device_itrack "+
					"    where  access_day between '"+monthcode+"01' and fn_month_lastday('"+monthcode+"') "+
					"    and    to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
//					"        select min(to_number(TRACK_VERSION)) "+
//					"        from   TB_SMART_TRACK_VER "+
//					"        where  exp_time > sysdate "+
//					"    ) "+
					"    group by panel_id "+
					") ";
			
			System.out.print("The batch - Device Weight is processing...");
			this.pstmt = connection.prepareStatement(query);
			this.pstmt.executeUpdate();
			if(this.pstmt!=null) this.pstmt.close();
			
			String query1 = 
					"update tb_smart_month_device_wgt a "+
					"set TELECOM_CODE=(select decode(M_PHONE_COM,1,'SK',2,'KT',3,'LG') telecom_code from tb_panel where panelid = a.panel_id) "+
					"where TELECOM_CODE='NONE' "+
					"and monthcode = '"+monthcode+"' ";
			
			this.pstmt = connection.prepareStatement(query1);
			this.pstmt.executeUpdate();
			if(this.pstmt!=null) this.pstmt.close();
			
			String query2 = 
					"update tb_smart_month_device_wgt "+
					"set telecom_code = upper(substr(trim(replace(TELECOM_CODE,'塋딉옙','')),1,2)) "+
					"where monthcode = '"+monthcode+"' ";
			
			this.pstmt = connection.prepareStatement(query2);
			this.pstmt.executeUpdate();
			//System.out.println(query);
			System.out.println("DONE.");
			if(this.pstmt!=null) this.pstmt.close();
		} else {
			System.out.print("Device WGT already exists.");
		}
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeSetupFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Setup Monthly Fact에 insert
	 *************************************************************************/
	
	public Calendar executeSetupFact(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Setup Weight is processing...");
//		String tablename = "";
//		tablename = findSetupTablename(monthcode);
//		String indate = nextMonth(monthcode);
//		
//		if(countTable(monthcode, "m", "tb_smart_month_setup_wgt")){
//			String query = 
//					"insert into tb_smart_month_setup_wgt "+
//					"select /*+parallel(a,8)*/substr(access_day,1,6) monthcode, b.smart_id, a.package_name, panel_id, sysdate "+
//					"from "+tablename+" a, tb_smart_app_info b "+
//					"where access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
//					"and a.package_name = b.package_name "+
//					"and b.exp_time > to_date('"+indate+"','yyyy/mm/dd') "+
//					"group by substr(access_day,1,6), b.smart_id, a.package_name, panel_id ";
//			
//			
//			this.pstmt = connection.prepareStatement(query);
//			this.pstmt.executeUpdate();
//			if(this.pstmt!=null) this.pstmt.close();
//			
//			String queryUpdate = 
//					"insert into tb_smart_month_setup_wgt "+
//					"select MONTHCODE, SMART_ID, PACKAGE_NAME, PANEL_ID, sysdate "+
//					"from ( "+
//					"    select MONTHCODE, SMART_ID, PACKAGE_NAME, PANEL_ID "+
//					"    from tb_smart_month_app_wgt "+
//					"    where monthcode = '"+monthcode+"' "+
//					"    minus "+
//					"    select MONTHCODE, SMART_ID, PACKAGE_NAME, PANEL_ID "+
//					"    from tb_smart_month_setup_wgt "+
//					"    where monthcode = '"+monthcode+"' "+
//					") ";
//			
//			System.out.print("The batch - Setup Weight is processing...");
//			this.pstmt = connection.prepareStatement(queryUpdate);
//			this.pstmt.executeUpdate();
//			if(this.pstmt!=null) this.pstmt.close();
//		} else {
//			System.out.print("Setup WGT already exists.");
//		}
		
		String query = 
				"insert into tb_smart_month_setup_wgt "+
				"select substr(access_day,1,6) monthcode, smart_id, package_name, panel_id, sysdate "+
				"from   tb_smart_day_setup_wgt "+
				"where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"group by substr(access_day,1,6), smart_id, package_name, panel_id ";
//		System.out.println(query);
//		System.exit(0);
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.print("DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekSectionFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSectionFact에 insert
	 *************************************************************************/
	
	public Calendar executeWeekSectionFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Section Fact is processing...");
		String query = 
				"INSERT INTO tb_smart_week_section_fact "+
				"(weekcode, site_id, panel_id, section_id, visit_cnt, pv_cnt, duration, daily_freq_cnt, proc_date) "+
				"SELECT   fn_weekcode(access_day) weekcode, site_id, panel_id, psection_id, sum(visit_cnt) visit_cnt, "+
				"         sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate proc_date "+
				"FROM "+
				"(         "+
				"    SELECT  panel_id,  psection_id, site_id, "+
				"            case when round((server_date - lag(server_date, 1) over (partition by site_id, visit_psection_id, "+
				"            panel_id order by server_date))*60* 60*24) > 60*30 "+
				"            or lag(server_date, 1) over (partition by site_id, visit_psection_id, panel_id "+
				"            order by server_date) is NULL then 1  end visit_cnt, "+
				"            pv_cnt, "+
				"            duration, "+
				"            access_day "+
				"    FROM "+
				"    (       "+
				"        SELECT /*+ parallel(a,8) */ "+
				"               panel_id, psection_id, site_id, server_date, "+
				"               visit_psection_id, pv_cnt, duration, access_day "+
				"        FROM   tb_smart_week_temp_section "+
				"        WHERE  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"        AND    access_day <= '"+accessday+"' "+
				"    ) "+
				") "+
				"where   panel_id in ( " +
				"	select panel_id " +
				"	from   tb_smart_panel_seg " +
				"	where  weekcode = fn_weekcode('"+accessday+"') " +
				") "+
				"GROUP  BY fn_weekcode(access_day), site_id, panel_id, psection_id "+
				"having sum(pv_cnt) > 0 ";
		//System.out.println(query);
		//System.exit(0);
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekQueryFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSectionFact에 insert
	 *************************************************************************/
	
	public Calendar executeWeekQueryFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Query Fact is processing...");
		String query = 
				"insert into TB_SMART_WEEK_QUERY_FACT "+
				"select   /*+index(a,PK_SMART_WEEK_KEYWORD_FACT)*/weekcode, SITE_ID, SECTION_ID, PANEL_ID , sum(QUERY_CNT) QUERY_CNT "+
				"from     TB_SMART_WEEK_KEYWORD_FACT a "+
				"WHERE    weekcode = fn_weekcode('"+accessday+"') " +
				"and      panel_id in ( " +
				"	select panel_id " +
				"	from   tb_smart_panel_seg " +
				"	where  weekcode = fn_weekcode('"+accessday+"') " +
				") "+
				"group by weekcode, SITE_ID, SECTION_ID, PANEL_ID " +
				"having sum(QUERY_CNT) > 0 ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekDailySiteSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 주간 웹 일평균 insert
	 *************************************************************************/
	
	public Calendar executeWeekDailySiteSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Week Day Site Sum is processing...");
		String query = 
				"INSERT   INTO tb_smart_daily_week_sum "+
				"(        WEEKCODE, SITE_ID, SITE_NAME, "+
				"         CATEGORY_NAME, category_code1, category_code2, category_code3, "+
				"         DAILY_UU_CNT_ADJ, REACH_RATE_ADJ, "+
				"         DAILY_VISIT_CNT_ADJ, DAILY_PV_CNT_ADJ, DAILY_AVG_DURATION, "+
				"         DAILY_UU_CNT_ADJ_WEEKDAYS, DAILY_PV_CNT_ADJ_WEEKDAYS, "+
				"         DAILY_VISIT_CNT_ADJ_WEEKDAYS, DAILY_AVG_DURATION_WEEKDAYS, "+
				"         DAILY_UU_CNT_ADJ_WEEKEND, DAILY_PV_CNT_ADJ_WEEKEND, "+
				"         DAILY_VISIT_CNT_ADJ_WEEKEND, DAILY_AVG_DURATION_WEEKEND, "+
				"         uu_overall_rank, visit_overall_rank, pv_overall_rank, avg_duration_overall_rank, "+
				"         proc_date "+
				") "+
				"select  fn_weekcode('"+accessday+"') weekcode, site_id, site_name, "+
				"nvl(FN_CATEGORY_REF2_NAME2((select CATEGORY_CODE2 from tb_site_info b where exp_time > sysdate and a.site_id = b.site_id)),max_category_name) category_name, "+
				"        nvl((select CATEGORY_CODE1 from tb_site_info b where exp_time > sysdate and a.site_id = b.site_id),MAX_CATEGORY_CODE1) CATEGORY_CODE1, "+
				"        nvl((select CATEGORY_CODE2 from tb_site_info b where exp_time > sysdate and a.site_id = b.site_id),MAX_CATEGORY_CODE2) CATEGORY_CODE2, "+
				"        nvl((select CATEGORY_CODE3 from tb_site_info b where exp_time > sysdate and a.site_id = b.site_id),MAX_CATEGORY_CODE2) CATEGORY_CODE3, "+
				"        UV, RR, VS, PV, DT, "+
				"        UV_weekdays, PV_weekdays, VS_weekdays, DT_weekdays, "+
				"        UV_weekend, PV_weekend, VS_weekend, DT_weekend, "+
				"        rank() over (order by UV desc) uu_overall_rank, "+
				"        rank() over (order by VS desc) visit_overall_rank, "+
				"        rank() over (order by PV desc) pv_overall_rank, "+
				"        rank() over (order by DT desc) avg_duration_overall_rank, "+
				"        sysdate proc_date "+
				"FROM "+
				"(       select  site_id, max(site_name) site_name, "+
				"                round(sum(UV)/7) UV, "+
				"                round(sum(RR)/7, 2) RR, "+
				"                round(sum(VS)/7) VS, "+
				"                round(sum(PV)/7) PV, "+
				"                round(sum(DT)/7, 2) DT, "+
				"                max(category_name) max_category_name, "+
				"                max(CATEGORY_CODE1) MAX_CATEGORY_CODE1, "+
				"                max(CATEGORY_CODE2) MAX_CATEGORY_CODE2, "+
				"                max(CATEGORY_CODE3) MAX_CATEGORY_CODE3, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"                         AND  access_day <= to_char(to_date('"+accessday+"','YYYYMMDD')-2,'YYYYMMDD') "+
				"                         then UV end)/5) UV_weekdays, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"                         AND  access_day <= to_char(to_date('"+accessday+"','YYYYMMDD')-2,'YYYYMMDD') "+
				"                         then VS end)/5) VS_weekdays, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+ 
				"                         AND  access_day <= to_char(to_date('"+accessday+"','YYYYMMDD')-2,'YYYYMMDD') "+
				"                         then PV end)/5) PV_weekdays, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"                         AND  access_day <= to_char(to_date('"+accessday+"','YYYYMMDD')-2,'YYYYMMDD') "+
				"                         then DT end)/5, 2) DT_weekdays, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-1,'YYYYMMDD') "+
				"                         AND  access_day <= '"+accessday+"' "+
				"                         then UV end)/2) UV_weekend, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-1,'YYYYMMDD') "+
				"                         AND  access_day <= '"+accessday+"' "+
				"                         then VS end)/2) VS_weekend, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-1,'YYYYMMDD') "+
				"                         AND  access_day <= '"+accessday+"' "+
				"                         then PV end)/2) PV_weekend, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-1,'YYYYMMDD') "+
				"                         AND  access_day <= '"+accessday+"' "+
				"                         then DT end)/2, 2) DT_weekend "+
				"        from   (select /*+ ordered(a) */ "+
				"                       a.access_day, site_id, site_name, "+
				"                       uu_cnt_adj*fn_day_modifier(a.access_day) UV, "+
				"                       reach_rate_adj RR, "+
				"                       visit_cnt_adj*fn_day_modifier(a.access_day) VS, "+
				"                       pv_cnt_adj*fn_day_modifier(a.access_day) PV, "+
				"                       avg_duration/60 DT, category_name, "+
				"                       CATEGORY_CODE1, CATEGORY_CODE2, CATEGORY_CODE3 "+
				"                from   tb_smart_day_site_sum a "+
				"                where  a.access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"                AND    a.access_day <= '"+accessday+"' "+
				"                and    a.category_code1!= 'Z' "+
				"               ) "+
				"        group by site_id "+
				")a ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekLevel1
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekLevel1 insert
	 *************************************************************************/
	
	public Calendar executeWeekLevel1(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Level1 Sum is processing...");
		String query = 
				"insert into tb_smart_week_1level_sum  "+
				"       ( weekcode, category_code, best_site_id, emerging_site_id, "+
				"     uu_cnt, pv_cnt, reach_rate, "+
				"     uu_cnt_adj, pv_cnt_adj, reach_rate_adj, proc_date, tot_duration_adj, "+
				"     duration, daily_freq_cnt, site_cnt, cate_site_cnt, "+
				"     uu_market_rate, pv_market_rate) "+
				"select 	v_category.weekcode, v_category.category_code1, best_site_id, NULL, "+
				"        v_category.uu_cnt, v_category.pv_cnt, "+
				"        round( v_category.uu_cnt/FN_SMART_WEEK_COUNT(v_category.weekcode)*100, 2)  reach_rate, "+
				"        v_category.uu_cnt_adj, v_category.pv_cnt_adj, "+
				"        round(uu_cnt_adj/FN_SMART_WEEK_NFACTOR(v_category.weekcode)*100,5) reach_rate_adj, "+
				"        sysdate, v_category.tot_duration_adj, "+
				"        duration, daily_freq_cnt, site_cnt, cate_site_cnt, "+
				"        round(uu_market/uu_cnt_adj*100,2) uu_market_rate, "+
				"        round(pv_market/pv_cnt_adj*100,2) pv_market_rate "+
				"from 	 "+
				"(	 "+
				"    select 	weekcode, category_code1, count(*) uu_cnt, sum(pv_cnt) pv_cnt,  "+
				"            sum(mo_n_factor) uu_cnt_adj, sum(pv_cnt_adj) pv_cnt_adj, "+
				"            round(decode(sum(mo_p_factor), 0,1, sum(duration*mo_p_factor)/sum(mo_n_factor)) ,2) duration, "+
				"            round(avg(site_cnt),2) site_cnt,  "+
				"            round(decode(sum(mo_n_factor), 0,1, sum(fq*mo_n_factor)/sum(mo_n_factor)) ,2)  daily_freq_cnt, "+
				"            round(sum(duration*mo_p_factor),5) tot_duration_adj "+
				"    from  "+
				"    (	 "+
				"        select  /*+index(a,PK_SMART_DAY_FACT)*/  "+
				"                b.weekcode, "+
				"                a.panel_id,  "+
				"                a.category_code1, "+
				"                sum(a.pv_cnt) pv_cnt, "+
				"                min(b.mo_n_factor) mo_n_factor, "+
				"                min(b.mo_p_factor) mo_p_factor, "+
				"                sum(a.pv_cnt * b.mo_p_factor) pv_cnt_adj, "+
				"                sum(a.duration) duration, "+
				"                count(distinct a.site_id) site_cnt, "+
				"                count(distinct a.access_day) fq  "+
				"        from  tb_smart_day_fact a, tb_smart_panel_seg b "+
				"        where access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"        AND   access_day <= '"+accessday+"' "+
				"        and   a.panel_id = b.panel_id "+
				"        and   b.weekcode = fn_weekcode('"+accessday+"') "+
				"        and   a.category_code1 <> 'Z' "+
				"        group by b.weekcode, a.category_code1, a.panel_id "+
				"    ) "+
				"    group by weekcode, category_code1 "+
				") v_category, "+
				"( 	 "+
				"    select weekcode, category_code1, min(site_id) best_site_id "+
				"    from   tb_smart_week_site_sum "+
				"    where  UU_1LEVEL_RANK = 1 "+
				"    and    weekcode = fn_weekcode('"+accessday+"') "+
				"    group by weekcode, category_code1 "+
				") v_best_site, "+
				"( 	 "+
				"    select 	weekcode, category_code1, count(distinct site_id) cate_site_cnt "+
				"    from 	tb_smart_week_site_sum  "+
				"    where 	weekcode = fn_weekcode('"+accessday+"') "+
				"    group by weekcode, category_code1  "+
				") v_cate_site, "+
				"( 	 "+
				"    select  a.weekcode, category_code1, sum(mo_n_factor) uu_market, sum(pv_cnt*mo_p_factor) pv_market "+
				"    from     "+
				"    (  "+
				"        select        /*+ use_hash(b,a)*/ "+
				"                      a.weekcode, a.category_code1, a.panel_id, sum(a.pv_cnt) pv_cnt "+
				"        from          tb_smart_week_fact a "+
				"        where         a.weekcode = fn_weekcode('"+accessday+"') "+
				"        and           a.site_id    in (	 "+
				"            select site_id "+
				"            from tb_smart_week_site_sum  "+
				"            where weekcode = a.weekcode "+
				"            and   UU_1LEVEL_RANK <= 3 "+
				"        ) "+
				"        group by      a.weekcode, a.category_code1, panel_id "+
				"    ) a, "+
				"    (  "+
				"        select        weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"        from          tb_smart_panel_seg "+
				"        where         weekcode = fn_weekcode('"+accessday+"') "+
				"    ) b "+
				"    where   a.panel_id = b.panel_id "+
				"    and     a.weekcode = b.weekcode "+
				"    group by a.weekcode, category_code1 "+
				") v_market "+
				"where 	v_category.category_code1 = v_best_site.category_code1 "+
				"and 	v_category.category_code1 = v_cate_site.category_code1 "+
				"and   	v_category.category_code1 = v_market.category_code1 "+
				"and   	v_category.weekcode = v_best_site.weekcode "+
				"and   	v_category.weekcode = v_cate_site.weekcode "+
				"and   	v_category.weekcode = v_market.weekcode ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekLevel2
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekLevel2 insert
	 *************************************************************************/
	
	public Calendar executeWeekLevel2(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Level2 Sum is processing...");
		String query = 
				"insert into tb_smart_week_2level_sum  "+
				"       ( weekcode, category_code, best_site_id, emerging_site_id, "+
				"     uu_cnt, pv_cnt, reach_rate, "+
				"     uu_cnt_adj, pv_cnt_adj, reach_rate_adj, proc_date, tot_duration_adj, "+
				"     duration, daily_freq_cnt, site_cnt, cate_site_cnt, "+
				"     uu_market_rate, pv_market_rate) "+
				"select 	v_category.weekcode, v_category.category_code2, best_site_id, NULL, "+
				"        v_category.uu_cnt, v_category.pv_cnt, "+
				"        round( v_category.uu_cnt/FN_SMART_WEEK_COUNT(v_category.weekcode)*100, 2)  reach_rate, "+
				"        v_category.uu_cnt_adj, v_category.pv_cnt_adj, "+
				"        round(uu_cnt_adj/FN_SMART_WEEK_NFACTOR(v_category.weekcode)*100,5) reach_rate_adj, "+
				"        sysdate, v_category.tot_duration_adj, "+
				"        duration, daily_freq_cnt, site_cnt, cate_site_cnt, "+
				"        round(uu_market/uu_cnt_adj*100,2) uu_market_rate, "+
				"        round(pv_market/pv_cnt_adj*100,2) pv_market_rate "+
				"from 	 "+
				"(	 "+
				"    select 	weekcode, category_code2, count(*) uu_cnt, sum(pv_cnt) pv_cnt,  "+
				"            sum(mo_n_factor) uu_cnt_adj, sum(pv_cnt_adj) pv_cnt_adj, "+
				"            round(decode(sum(mo_p_factor), 0,1, sum(duration*mo_p_factor)/sum(mo_n_factor)) ,2) duration, "+
				"            round(avg(site_cnt),2) site_cnt,  "+
				"            round(decode(sum(mo_n_factor), 0,1, sum(fq*mo_n_factor)/sum(mo_n_factor)) ,2)  daily_freq_cnt, "+
				"            round(sum(duration*mo_p_factor),5) tot_duration_adj "+
				"    from  "+
				"    (	 "+
				"        select  /*+index(a,PK_SMART_DAY_FACT)*/  "+
				"                b.weekcode, "+
				"                a.panel_id,  "+
				"                a.category_code2, "+
				"                sum(a.pv_cnt) pv_cnt, "+
				"                min(b.mo_n_factor) mo_n_factor, "+
				"                min(b.mo_p_factor) mo_p_factor, "+
				"                sum(a.pv_cnt * b.mo_p_factor) pv_cnt_adj, "+
				"                sum(a.duration) duration, "+
				"                count(distinct a.site_id) site_cnt, "+
				"                count(distinct a.access_day) fq  "+
				"        from  tb_smart_day_fact a, tb_smart_panel_seg b "+
				"        where access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"        AND   access_day <= '"+accessday+"' "+
				"        and   a.panel_id = b.panel_id "+
				"        and   b.weekcode = fn_weekcode('"+accessday+"') "+
				"        and   a.category_code2 < 'Z' "+
				"        group by b.weekcode, a.category_code2, a.panel_id "+
				"    ) "+
				"    group by weekcode, category_code2 "+
				") v_category, "+
				"( 	 "+
				"    select /*+index(a,PK_SMART_WEEK_SITE_SUM)*/weekcode, category_code2, min(site_id) best_site_id "+
				"    from   tb_smart_week_site_sum a "+
				"    where  UU_2LEVEL_RANK = 1 "+
				"    and    weekcode = fn_weekcode('"+accessday+"') "+
				"    group by weekcode, category_code2 "+
				") v_best_site, "+
				"( 	 "+
				"    select 	/*+index(a,PK_SMART_WEEK_SITE_SUM)*/weekcode, category_code2, count(distinct site_id) cate_site_cnt "+
				"    from 	tb_smart_week_site_sum a "+
				"    where 	weekcode = fn_weekcode('"+accessday+"') "+
				"    group by weekcode, category_code2  "+
				") v_cate_site, "+
				"( 	 "+
				"    select  a.weekcode, category_code2, sum(mo_n_factor) uu_market, sum(pv_cnt*mo_p_factor) pv_market "+
				"    from     "+
				"    (  "+
				"        select        /*+ use_hash(b,a) index(a,PK_SMART_WEEK_FACT)*/ "+
				"                      a.weekcode, a.category_code2, a.panel_id, sum(a.pv_cnt) pv_cnt "+
				"        from          tb_smart_week_fact a "+
				"        where         a.weekcode = fn_weekcode('"+accessday+"') "+
				"        and           a.site_id    in (	 "+
				"            select site_id "+
				"            from tb_smart_week_site_sum  "+
				"            where weekcode = a.weekcode "+
				"            and   UU_2LEVEL_RANK <= 3 "+
				"        ) "+
				"        group by      a.weekcode, a.category_code2, panel_id "+
				"    ) a, "+
				"    (  "+
				"        select        weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"        from          tb_smart_panel_seg "+
				"        where         weekcode = fn_weekcode('"+accessday+"') "+
				"    ) b "+
				"    where   a.panel_id = b.panel_id "+
				"    and     a.weekcode = b.weekcode "+
				"    group by a.weekcode, category_code2 "+
				") v_market "+
				"where 	v_category.category_code2 = v_best_site.category_code2 "+
				"and 	v_category.category_code2 = v_cate_site.category_code2 "+
				"and   	v_category.category_code2 = v_market.category_code2 "+
				"and   	v_category.weekcode = v_best_site.weekcode "+
				"and   	v_category.weekcode = v_cate_site.weekcode "+
				"and   	v_category.weekcode = v_market.weekcode ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekLevel1Seg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekLevel1Seg insert
	 *************************************************************************/
	
	public Calendar executeWeekLevel1Seg(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Level2 Sum is processing...");
		String query = 
				"insert into kclick.tb_smart_week_seg_cate1 "+
				 "( "+
				 "    weekcode, segment_id, category_code1, "+
				 "    age_cls,  sex_cls,  income_cls, job_cls,    education_cls, "+
				 "    ismarried_cls, region_cd,  uu_cnt, uu_est_cnt, pv_cnt, pv_est_cnt, "+
				 "    avg_duration, duration_est, avg_daily_freq_cnt, "+
				 "    avg_pv_est_cnt, reach_rate, visit_cnt, visit_est_cnt, "+
				 "    proc_date "+
				 ") "+
				 "select weekcode, a.segment_id, d.category_code1, "+
				 "    min(b.age_cls)        age_cls, "+
				 "    min(b.sex_cls)        sex_cls, "+
				 "    min(b.income_cls)     income_cls, "+
				 "    min(b.job_cls)        job_cls, "+
				 "    min(b.education_cls)  education_cls, "+
				 "    min(b.ismarried_cls ) ismarried_cls, "+
				 "    min(b.region_cd )     region_cd, "+
				 "    count(distinct a.panel_id) uu_cnt, "+
				 "    round(sum( b.mo_n_factor),5) uu_est_cnt, "+
				 "    sum(pv_cnt) pv_cnt, "+
				 "    round(sum(pv_cnt*b.mo_p_factor),5) pv_est_cnt, "+
				 "    round(decode(sum(mo_n_factor),0,1, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				 "    round(sum(duration*b.mo_p_factor),5) duration_est, "+
				 "    round(decode(sum(mo_n_factor),0,1, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				 "    round(decode(sum(mo_n_factor),0,1, sum(pv_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				 "    round(sum(b.mo_n_factor)/ max(sum_kc_nfactor)*100, 5) reach_rate, "+
				 "    sum(visit_cnt) visit_cnt, "+
				 "    round(sum(visit_cnt*b.mo_p_factor),5) visit_est_cnt, "+
				 "    sysdate "+
				 "from "+
				 "( "+
				 "    select fn_weekcode('"+accessday+"') weekcode, segment_id, site_id, panel_id, "+
				 "       pv_cnt, visit_cnt, daily_freq_cnt, duration "+
				 "    from   tb_smart_week_fact "+
				 "    where  weekcode = fn_weekcode('"+accessday+"') "+
				 ") a, "+
				 "( "+
				 "    select panel_id, kc_seg_id, mo_n_factor, mo_p_factor, "+
				 "          age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				 "    from   tb_smart_panel_seg "+
				 "    where  weekcode = fn_weekcode('"+accessday+"') "+
				 ") b, "+
				 "( "+
				 "    select sum(mo_n_factor) sum_kc_nfactor "+
				 "    from tb_smart_panel_seg "+
				 "    where weekcode = fn_weekcode('"+accessday+"') "+
				 ") c, "+
				 "( "+
				 "    select site_id, category_code1 "+
				 "    from tb_smart_week_site_sum "+
				 "    where weekcode = fn_weekcode('"+accessday+"') "+
				 ") d "+
				 "where a.panel_id   = b.panel_id "+
				 "and   a.site_id    = d.site_id "+
				 "group by a.segment_id, d.category_code1 ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}	
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekLevel2Seg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekLevel2Seg insert
	 *************************************************************************/
	
	public Calendar executeWeekLevel2Seg(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Level2 Sum is processing...");
		String query = 
				"insert into kclick.tb_smart_week_SEG_CATE "+
				"( "+
				"    weekcode, segment_id, category_code2, "+
				"    age_cls,  sex_cls,  income_cls, job_cls,    education_cls, "+
				"    ismarried_cls, region_cd,  uu_cnt, uu_est_cnt, pv_cnt, pv_est_cnt, "+
				"    avg_duration, duration_est, avg_daily_freq_cnt, "+
				"    avg_pv_est_cnt, reach_rate, visit_cnt, visit_est_cnt, "+
				"    proc_date "+
				") "+
				"select weekcode, a.segment_id, d.category_code2, "+
				"    min(b.age_cls)        age_cls, "+
				"    min(b.sex_cls)        sex_cls, "+
				"    min(b.income_cls)     income_cls, "+ 
				"    min(b.job_cls)        job_cls, "+
				"    min(b.education_cls)  education_cls, "+
				"    min(b.ismarried_cls ) ismarried_cls, "+
				"    min(b.region_cd )     region_cd, "+
				"    count(distinct a.panel_id) uu_cnt, "+
				"    round(sum( b.mo_n_factor),5) uu_est_cnt, "+
				"    sum(pv_cnt) pv_cnt, "+
				"    round(sum(pv_cnt*b.mo_p_factor),5) pv_est_cnt, "+
				"    round(decode(sum(mo_n_factor),0,1, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"    round(sum(duration*b.mo_p_factor),5) duration_est, "+
				"    round(decode(sum(mo_n_factor),0,1, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"    round(decode(sum(mo_n_factor),0,1, sum(pv_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				"    round(sum(b.mo_n_factor)/ max(sum_kc_nfactor)*100, 5) reach_rate, "+
				"    sum(visit_cnt) visit_cnt, "+
				"    round(sum(visit_cnt*b.mo_p_factor),5) visit_est_cnt, "+
				"    sysdate "+
				"from "+ 
				"( "+
				"    select fn_weekcode('"+accessday+"') weekcode, segment_id, site_id, panel_id, "+
				"       pv_cnt, visit_cnt, daily_freq_cnt, duration "+
				"    from   tb_smart_week_fact "+
				"    where  weekcode = fn_weekcode('"+accessday+"') "+
				") a, "+
				"( "+
				"    select panel_id, kc_seg_id, mo_n_factor, mo_p_factor, "+
				"          age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"    from   tb_smart_panel_seg "+
				"    where  weekcode = fn_weekcode('"+accessday+"') "+
				") b, "+
				"( "+
				"    select sum(mo_n_factor) sum_kc_nfactor "+
				"    from tb_smart_panel_seg "+
				"    where weekcode = fn_weekcode('"+accessday+"') "+
				") c, "+
				"( "+
				"    select site_id, category_code2 "+ 
				"    from tb_smart_week_site_sum "+
				"    where weekcode = fn_weekcode('"+accessday+"') "+
				") d "+
				"where a.panel_id   = b.panel_id "+
				"and   a.site_id    = d.site_id "+
				"group by a.segment_id, d.category_code2 ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}	
		
	/**************************************************************************
	 *		메소드명		: executeWeekKeyword
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekLevel2 insert
	 *************************************************************************/
	
	public Calendar executeWeekKeyword(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Keyword Sum is processing...");
		String query = 
				"INSERT INTO tb_smart_week_keyword_sum "+
				"SELECT weekcode, keyword, uv, pv, qv, "+
				"    rank() over (partition by weekcode order by uv desc) u_rank, "+
				"    rank() over (partition by weekcode order by pv desc) p_rank, "+
				"    rank() over (partition by weekcode order by qv desc) q_rank "+
				"FROM  "+
				"( "+
				"    SELECT /*+ordered */ "+
				"        a.weekcode,  "+
				"        keyword, "+
				"        round(sum(mo_n_factor), 5) uv, "+
				"        round(sum(mo_p_factor*pv_cnt), 5) pv, "+
				"        round(sum(mo_p_factor*query_cnt), 5) qv "+
				"    FROM  "+
				"    ( "+
				"        SELECT /*+index(a,PK_SMART_WEEK_KEYWORD_FACT)*/weekcode, keyword, panel_id, sum(pv_cnt) pv_cnt, sum(query_cnt) query_cnt "+
				"        FROM   tb_smart_week_keyword_fact a"+
				"        WHERE  weekcode = fn_weekcode('"+accessday+"') "+
				"        GROUP BY weekcode, keyword, panel_id "+
				"    ) a, "+
				"    ( "+
				"        SELECT weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"        FROM   tb_smart_panel_seg "+
				"        WHERE  weekcode = fn_weekcode('"+accessday+"') "+
				"    ) b "+
				"    WHERE a.panel_id = b.panel_id "+
				"    AND a.weekcode = b.weekcode "+
				"    GROUP BY keyword, a.weekcode "+
				") ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekSession
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekLevel2 insert
	 *************************************************************************/
	
	public Calendar executeWeekSession(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Session is processing...");
		String query = 
				"INSERT   INTO tb_smart_week_session (  "+
				"        weekcode, panel_id, pv_cnt, duration , "+
				"        daily_freq_cnt, mo_n_factor, mo_p_factor, age_cls, sex_cls, "+
				"        education_cls, income_cls, job_cls, ismarried_cls, site_cnt, proc_date  "+
				") "+
				"SELECT  v_session.weekcode, v_session.panel_id, pv_cnt, duration,                  "+
				"        daily_freq_cnt, mo_n_factor, mo_p_factor, age_cls, sex_cls,  "+
				"        education_cls, income_cls, job_cls, ismarried_cls, nvl(site_cnt,0), sysdate "+
				"FROM     ( select /*+index(a,pk_smart_day_fact)*/ fn_weekcode(access_day) weekcode, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, "+
				"                  count(distinct access_day) daily_freq_cnt "+
				"           from   tb_smart_day_fact a "+
				"           WHERE  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"           AND    access_day <= '"+accessday+"' "+
				"           group by fn_weekcode(access_day), panel_id "+
				"         ) v_session, "+
				"         ( select weekcode, panel_id, mo_n_factor, age_cls, sex_cls, education_cls,  "+
				"                  income_cls, job_cls, ismarried_cls, mo_p_factor "+
				"             from tb_smart_panel_seg "+
				"            where weekcode = fn_weekcode('"+accessday+"') "+
				"         ) v_panel_seg, "+
				"         ( select /*+index(a,PK_SMART_WEEK_FACT)*/weekcode, panel_id, count(distinct site_id) site_cnt "+
				"             from tb_smart_week_fact a "+
				"            where weekcode = fn_weekcode('"+accessday+"') "+
				"              and category_code2 < 'Z' "+
				"            group by weekcode, panel_id "+
				"         ) v_fact "+
				"WHERE v_session.panel_id = v_panel_seg.panel_id "+
				"AND v_session.panel_id = v_fact.panel_id(+) "+
				"AND v_session.weekcode = v_panel_seg.weekcode "+
				"AND v_session.weekcode = v_fact.weekcode(+) ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekSection
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekLevel2 insert
	 *************************************************************************/
	
	public Calendar executeWeekSection(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Section is processing...");
		String query = 
				"INSERT  INTO TB_SMART_WEEK_SECTION "+
				"(         "+
				"    weekcode, site_id, section_id, site_name, url_link, "+
				"    reach_rate, uu_cnt, pv_cnt, avg_duration, "+
				"    daily_freq_cnt, uu_cnt_adj, pv_cnt_adj, reach_rate_adj,  "+
				"    uu_overall_rank,  "+
				"    pv_overall_rank,  "+
				"    avg_duration_overall_rank, "+
				"    freq_overall_rank, "+
				"    tot_duration_overall_rank, "+
				"    visit_cnt, visit_cnt_adj, tot_duration_adj, proc_date "+
				") "+
				"SELECT  /*+ordered use_hash(b,a)*/ "+
				"        A.weekcode, A.site_id, A.section_id, B.site_name, B.url_link, "+
				"        A.reach_rate, A.uu_cnt, A.pv_cnt, A.avg_duration, "+
				"        A.daily_freq_cnt, A.uu_cnt_adj, A.pv_cnt_adj, A.reach_rate_adj,  "+
				"        rank() over (partition by A.section_id order by uu_cnt_adj desc) uu_overall_rank, "+
				"        rank() over (partition by A.section_id order by pv_cnt_adj desc) pv_overall_rank, "+
				"        rank() over (partition by A.section_id order by avg_duration desc) avg_duration_overall_rank, "+
				"        rank() over (partition by A.section_id order by daily_freq_cnt desc) daily_freq_overall_rank, "+
				"        rank() over (partition by A.section_id order by tot_duration_adj desc) tot_duration_overall_rank, "+
				"        visit_cnt, visit_cnt_adj, tot_duration_adj, sysdate proc_date "+
				"FROM      "+
				"(         "+
				"    SELECT  /*+use_hash(b,a)*/ "+
				"            A.weekcode,  "+
				"            A.site_id,  "+
				"            A.section_id, "+
				"            count(*)/fn_smart_week_count(A.weekcode)*100 reach_rate,  "+
				"            count(*) uu_cnt, "+
				"            sum(pv_cnt) pv_cnt, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"            sum(B.mo_n_factor) uu_cnt_adj,  "+
				"            sum(A.pv_cnt*B.mo_p_factor) pv_cnt_adj, "+
				"            sum(B.mo_n_factor)/fn_smart_week_nfactor(A.weekcode)*100 reach_rate_adj, "+
				"            sum(visit_cnt) visit_cnt, "+
				"            sum(visit_cnt*mo_p_factor) visit_cnt_adj, "+
				"            round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj "+
				"    FROM "+
				"    (         "+
				"            SELECT   /*+index(a,PK_SMART_WEEK_SECTION_FACT)*/weekcode, site_id, section_id, panel_id, pv_cnt, duration, daily_freq_cnt, visit_cnt "+
				"            FROM     tb_smart_week_section_fact a "+
				"            where    weekcode = fn_weekcode('"+accessday+"') "+
				"    ) A, "+
				"    (         "+
				"            SELECT   weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"            FROM     tb_smart_panel_seg "+
				"            where    weekcode = fn_weekcode('"+accessday+"') "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    AND      A.weekcode = B.weekcode "+
				"    GROUP BY A.weekcode, A.site_id, A.section_id "+
				") A, "+
				"(         "+
				"    SELECT   /*+index(a,PK_SITE_INFO)*/site_id, site_name, url_link "+
				"    FROM     tb_site_info a "+
				"    WHERE    exp_time >= sysdate "+
				") B "+
				"WHERE    A.site_id = B.site_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekSection
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekLevel2 insert
	 *************************************************************************/
	
	public Calendar executeWeekCsection(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Csection is processing...");
		String query = 
				"INSERT  INTO TB_SMART_WEEK_CSECTION "+
				"(         "+
				"    weekcode, site_id, section_id, site_name, url_link, "+
				"    reach_rate, uu_cnt, pv_cnt, avg_duration, "+
				"    daily_freq_cnt, uu_cnt_adj, pv_cnt_adj, reach_rate_adj,  "+
				"    uu_overall_rank,  "+
				"    pv_overall_rank,  "+
				"    avg_duration_overall_rank, "+
				"    freq_overall_rank, "+
				"    tot_duration_overall_rank, "+
				"    visit_cnt, visit_cnt_adj, tot_duration_adj, proc_date "+
				") "+
				"SELECT  /*+ordered use_hash(b,a)*/ "+
				"        A.weekcode, A.site_id, A.section_id, B.site_name, B.url_link, "+
				"        A.reach_rate, A.uu_cnt, A.pv_cnt, A.avg_duration, "+
				"        A.daily_freq_cnt, A.uu_cnt_adj, A.pv_cnt_adj, A.reach_rate_adj,  "+
				"        rank() over (partition by A.section_id order by uu_cnt_adj desc) uu_overall_rank, "+
				"        rank() over (partition by A.section_id order by pv_cnt_adj desc) pv_overall_rank, "+
				"        rank() over (partition by A.section_id order by avg_duration desc) avg_duration_overall_rank, "+
				"        rank() over (partition by A.section_id order by daily_freq_cnt desc) daily_freq_overall_rank, "+
				"        rank() over (partition by A.section_id order by tot_duration_adj desc) tot_duration_overall_rank, "+
				"        visit_cnt, visit_cnt_adj, tot_duration_adj, sysdate proc_date "+
				"FROM      "+
				"(         "+
				"    SELECT  /*+use_hash(b,a)*/ "+
				"            A.weekcode,  "+
				"            A.site_id,  "+
				"            A.section_id, "+
				"            count(*)/fn_smart_week_count(A.weekcode)*100 reach_rate,  "+
				"            count(*) uu_cnt, "+
				"            sum(pv_cnt) pv_cnt, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"            sum(B.mo_n_factor) uu_cnt_adj,  "+
				"            sum(A.pv_cnt*B.mo_p_factor) pv_cnt_adj, "+
				"            sum(B.mo_n_factor)/fn_smart_week_nfactor(A.weekcode)*100 reach_rate_adj, "+
				"            sum(visit_cnt) visit_cnt, "+
				"            sum(visit_cnt*mo_p_factor) visit_cnt_adj, "+
				"            round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj "+
				"    FROM "+
				"    (         "+
				"            SELECT   /*+index(a,PK_SMART_WEEK_SECTION_PFACT)*/weekcode, site_id, section_id, panel_id, pv_cnt, duration, daily_freq_cnt, visit_cnt "+
				"            FROM     tb_smart_week_csection_fact a "+
				"            where    WEEKCODE = fn_weekcode('"+accessday+"') "+
				"    ) A, "+
				"    (         "+
				"            SELECT   weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"            FROM     tb_smart_panel_seg "+
				"            where    WEEKCODE = fn_weekcode('"+accessday+"') "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    AND      A.weekcode = B.weekcode "+
				"    GROUP BY A.weekcode, A.site_id, A.section_id "+
				") A, "+
				"(         "+
				"    SELECT   /*+index(a,PK_SITE_INFO)*/site_id, site_name, url_link "+
				"    FROM     tb_site_info a "+
				"    WHERE    exp_time >= sysdate "+
				") B "+
				"WHERE    A.site_id = B.site_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekSectionSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSectionSum insert
	 *************************************************************************/
	
	public Calendar executeWeekSectionSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Section Sum is processing...");
		String query = 
				"insert into TB_SMART_WEEK_SECTION_SUM "+
				"SELECT      A.weekcode, A.SECTION_ID, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"            rank() over (partition by A.weekcode order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"            round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"            round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"            round(sum(B.mo_n_factor)/FN_SMART_WEEK_NFACTOR(A.weekcode)*100,5) reach_rate_adj, "+
				"            round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"            round(sum(A.visit_cnt*B.mo_p_factor),5) visit_cnt_adj, "+
				"            sysdate "+
				"FROM "+
				"(        SELECT   /*+index(a,PK_SMART_WEEK_SECTION_FACT)*/weekCODE, PANEL_ID, SECTION_ID, sum(VISIT_CNT) VISIT_CNT,  "+
				"                  SUM(PV_CNT) PV_CNT, SUM(DURATION) DURATION, SUM(DAILY_FREQ_CNT) DAILY_FREQ_CNT "+
				"         FROM     tb_smart_week_SECTION_fact a "+
				"         WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				"         group by weekCODE, PANEL_ID, SECTION_ID "+
				") A, "+
				"(        SELECT   weekCODE, panel_id, mo_n_factor, mo_p_factor "+
				"         FROM     tb_smart_panel_seg "+
				"         WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				") B "+
				"WHERE    A.panel_id = B.panel_id "+
				"and      A.weekCODE = B.weekCODE "+
				"GROUP BY A.weekcode, A.SECTION_ID ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekCsectionSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekCsectionSum insert
	 *************************************************************************/
	
	public Calendar executeWeekCsectionSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Csection Sum is processing...");
		String query = 
				"insert into TB_SMART_WEEK_CSECTION_SUM "+
				"SELECT      A.weekcode, A.SECTION_ID, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"            rank() over (partition by A.weekcode order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"            round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"            round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"            round(sum(B.mo_n_factor)/FN_SMART_WEEK_NFACTOR(A.weekcode)*100,5) reach_rate_adj, "+
				"            round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"            round(sum(A.visit_cnt*B.mo_p_factor),5) visit_cnt_adj, "+
				"            sysdate "+
				"FROM "+
				"(        SELECT   /*+index(a,PK_SMART_WEEK_SECTION_PFACT)*/WEEKCODE, PANEL_ID, SECTION_ID, sum(VISIT_CNT) VISIT_CNT,  "+
				"                  SUM(PV_CNT) PV_CNT, SUM(DURATION) DURATION, SUM(DAILY_FREQ_CNT) DAILY_FREQ_CNT "+
				"         FROM     tb_smart_week_csection_fact a "+
				"         WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				"         group by weekCODE, PANEL_ID, SECTION_ID "+
				") A, "+
				"(        SELECT   weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"         FROM     tb_smart_panel_seg "+
				"         WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				") B "+
				"WHERE    A.panel_id = B.panel_id "+
				"AND      A.weekcode = B.weekcode "+
				"GROUP BY A.weekcode, A.SECTION_ID ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}

	
	/**************************************************************************
	 *		메소드명		: executeWeekAppSeg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 앱 인구통계 교차 관련 insert
	 *************************************************************************/
	
	public Calendar executeWeekAppSeg(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly APP Seg is processing...");
		String query = 
				"insert  into TB_SMART_WEEK_SEG_APP "+
				"select b.weekcode, b.kc_seg_id, b.smart_id, b.package_name, APP_CATEGORY_CD1, APP_CATEGORY_CD2, age_cls, sex_cls, income_cls, job_cls, "+
				"    education_cls, ismarried_cls, region_cd, nvl(uu_cnt,0) uu_cnt, nvl(uu_est_cnt,0) uu_est_cnt, nvl(avg_duration,0) avg_duration, "+
				"    nvl(duration_est,0) duration_est, nvl(avg_daily_freq_cnt,0) avg_daily_freq_cnt, nvl(app_cnt,0) app_cnt, "+
				"    nvl(app_est_cnt,0) app_est_cnt, nvl(avg_pv_est_cnt,0) avg_pv_est_cnt, nvl(reach_rate,0) reach_rate, sysdate, iu_cnt, iu_est_cnt "+
				"from "+
				"( "+
				"    select  fn_weekcode('"+accessday+"') weekcode, "+
				"            kc_seg_id, "+
				"            v_fact.smart_id, "+
				"            count(distinct v_fact.panel_id) uu_cnt, "+
				"            round(sum(v_panel_seg.mo_n_factor),5) uu_est_cnt, "+
				"            round(decode(sum(mo_n_factor),0,0, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"            round(sum(duration*v_panel_seg.mo_p_factor),5) duration_est, "+
				"            round(decode(sum(mo_n_factor),0,0, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"            sum(app_cnt) app_cnt, "+
				"            round(sum(app_cnt*v_panel_seg.mo_p_factor),5) app_est_cnt, "+
				"            round(decode(sum(mo_n_factor),0,0, sum(app_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+ 
				"            round(sum(v_panel_seg.mo_n_factor)/ fn_smart_week_nfactor(v_fact.weekcode)*100, 5) reach_rate, "+
				"            sysdate "+
				"    from "+  
				"    ( "+
				"            select  /*+use_hash(b,a) index(a,PK_SMART_WEEK_APP_FACT)*/ WEEKcode, smart_id, package_name, panel_id, duration, daily_freq_cnt, app_cnt "+
				"            from    tb_smart_week_app_fact  "+
				"            where   weekcode = fn_weekcode('"+accessday+"') "+
				"    ) v_fact, "+
				"    ( "+
				"            select  panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"            from    tb_smart_panel_seg "+
				"            where   weekcode = fn_weekcode('"+accessday+"') "+
				"    ) v_panel_seg "+
				"    where v_fact.panel_id   = v_panel_seg.panel_id "+
				"    group by v_fact.weekcode, v_panel_seg.kc_seg_id, v_fact.smart_id, package_name "+
				")a, "+               
				"( "+
				"    select  fn_weekcode('"+accessday+"') weekcode, "+
				"            kc_seg_id, "+
				"            v_setup.smart_id, "+
				"            v_setup.package_name, "+
				"            min(APP_CATEGORY_CD1) APP_CATEGORY_CD1, "+
				"            min(APP_CATEGORY_CD2) APP_CATEGORY_CD2, "+
				"            min(v_panel_seg.age_cls)        age_cls, "+
				"            min(v_panel_seg.sex_cls)        sex_cls, "+
				"            min(v_panel_seg.income_cls)     income_cls, "+  
				"            min(v_panel_seg.job_cls)        job_cls, "+
				"            min(v_panel_seg.education_cls)  education_cls, "+
				"            min(v_panel_seg.ismarried_cls ) ismarried_cls, "+
				"            min(v_panel_seg.region_cd )     region_cd, "+
				"            count(*)                        iu_cnt, "+
				"            round(sum(v_panel_seg.mo_n_factor)*fn_week_modifier(v_setup.weekcode),5) iu_est_cnt "+
				"    from  "+
				"    ( "+
				"            select  /*+use_hash(b,a) index(a,PK_SMART_WEEK_SETUP_FACT)*/ b.WEEKcode, a.smart_id, b.package_name, panel_id, "+
				"                     APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				"            from    tb_smart_week_setup_fact a, tb_smart_week_app_sum b "+
				"            where   b.weekcode = fn_weekcode('"+accessday+"') "+
				"            and     a.weekcode = fn_weekcode('"+accessday+"') "+
				"            and     a.weekcode = b.weekcode "+
				"            AND     a.smart_id = b.smart_id "+
				"    ) v_setup, "+
				"    ( "+
				"            select  panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"            from    tb_smart_panel_seg "+
				"            where   weekcode = fn_weekcode('"+accessday+"') "+
				"    ) v_panel_seg "+
				"    where v_setup.panel_id   = v_panel_seg.panel_id "+
				"    group by weekcode, v_panel_seg.kc_seg_id, v_setup.smart_id, v_setup.package_name "+
				")b "+
				"where a.weekcode(+) = b.weekcode "+
				"and a.smart_id(+) = b.smart_id "+
				"and a.kc_seg_id(+) = b.kc_seg_id";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekAppSum insert
	 *************************************************************************/
	
	public Calendar executeWeekAppSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly App Sum is processing...");
		
		String queryT = "truncate table tb_temp_smart_app_info";
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		System.out.println("truncate tb_temp_smart_app_info DONE.");
		
		
		queryT = "insert into tb_temp_smart_app_info "+
				"SELECT   PRO_ID,SMART_ID,PACKAGE_NAME,APP_NAME,APP_CATEGORY_CD1,APP_CATEGORY_CD2,EF_TIME,EXP_TIME,SITE_ID,ENG_APP_NAME,P_SMART_ID, 'ALL' TYPE "+
				"FROM     tb_smart_app_info b "+
				"WHERE    exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+ 
				"AND      ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
				"and      p_smart_id is null ";
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		
		queryT = "insert into tb_temp_smart_app_info "+
				"SELECT   PRO_ID,SMART_ID,PACKAGE_NAME,APP_NAME,APP_CATEGORY_CD1,APP_CATEGORY_CD2,EF_TIME,EXP_TIME,SITE_ID,ENG_APP_NAME,P_SMART_ID, 'EQUAL' TYPE "+
				"FROM     tb_smart_app_info b "+
				"WHERE    exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+ 
				"AND      ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
				"and      p_smart_id is not null ";
		
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		System.out.println("Insertion tb_temp_smart_app_info DONE.");
		
		String query = 
				"insert into tb_smart_week_app_sum "+
				"select /*+ordered*/a.weekcode, a.smart_id, a.package_name, a.app_name, a.pro_id, a.site_id, "+
				"       app_category_cd1, app_category_cd2, reach_rate, a.uu_cnt, uu_overall_rank, uu_1level_rank, uu_2level_rank, "+
				"       avg_duration, avg_duration_overall_rank, avg_duration_1level_rank, avg_duration_2level_rank, daily_freq_cnt, "+
				"       a.uu_cnt_adj, reach_rate_adj, freq_overall_rank, tot_duration_overall_rank, tot_duration_adj, "+ 
				"       b.uu_cnt install_uu_cnt, b.uu_cnt_adj install_uu_cnt_adj, "+
				"       round(a.uu_cnt/b.uu_cnt*100,2) install_rate, round(a.uu_cnt_adj/b.uu_cnt_adj*100,2) install_rate_adj, sysdate, null, null, app_cnt_adj, P_UU_CNT_ADJ, P_TOT_DURATION_ADJ "+
				"from "+
				"( "+
				"    SELECT   /*+ordered*/ "+
				"             A.WEEKCODE,  SMART_ID, PACKAGE_NAME, APP_NAME, PRO_ID, SITE_ID, "+
				"             min(app_category_cd1) app_category_cd1, min(app_category_cd2) app_category_cd2, "+
				"             round(count(A.panel_id)/FN_SMART_WEEK_COUNT(A.WEEKCODE)*100,2) reach_rate, "+
				"             count(A.panel_id) uu_cnt, "+
				"             rank() over (partition by A.WEEKCODE, type order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"             rank() over (partition by A.WEEKCODE, type, min(app_category_cd1) order by sum(B.mo_n_factor) desc) uu_1level_rank, "+
				"             rank() over (partition by A.WEEKCODE, type, min(app_category_cd2) order by sum(B.mo_n_factor) desc) uu_2level_rank, "+
				"             round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"             rank() over (partition by A.WEEKCODE, type order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"             rank() over (partition by A.WEEKCODE, type,  min(app_category_cd1) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_1level_rank, "+
				"             rank() over (partition by A.WEEKCODE, type,  min(app_category_cd2) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_2level_rank, "+
				"             round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"             round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"             round(sum(B.mo_n_factor)/FN_SMART_WEEK_NFACTOR(A.weekcode)*100,5) reach_rate_adj, "+
				"             rank() over (partition by A.WEEKCODE, type order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"             rank() over (partition by A.WEEKCODE, type order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
				"             round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"             round(sum(app_cnt*mo_p_factor),5) app_cnt_adj, "+
				"             null P_UU_CNT_ADJ, "+
				"             null P_TOT_DURATION_ADJ "+
				"    FROM "+
				"    ( "+
				"        SELECT   /*+index(a,pk_smart_week_app_fact)*/ WEEKCODE, a.SMART_ID, a.PACKAGE_NAME, PANEL_ID, DURATION,  "+
				"                 DAILY_FREQ_CNT, PRO_ID, APP_NAME,  "+
				"                 APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, app_cnt, 'All' type  "+
				"        FROM     tb_smart_week_app_fact a, tb_temp_smart_app_info b "+
				"        WHERE    weekcode = fn_weekcode('"+accessday+"') "+
				"        AND      a.smart_id = b.smart_id "+
				"        and      type = 'ALL'  "+				
                "        union all "+ 
 				"        SELECT   /*+index(a,pk_smart_week_app_fact)*/ WEEKCODE, a.SMART_ID, a.PACKAGE_NAME, PANEL_ID, DURATION, "+ 
 				"                 DAILY_FREQ_CNT, PRO_ID, APP_NAME, "+
 				"                 APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, app_cnt, 'EQUAL' type "+
 				"        FROM     tb_smart_week_app_fact a, tb_temp_smart_app_info b "+
 				"        WHERE    weekcode = fn_weekcode('"+accessday+"') "+
 				"        AND      a.smart_id = b.smart_id "+
                "        and      type = 'EQUAL' "+	
				"    ) A, "+
				"    ( "+
				"        SELECT   WEEKCODE, panel_id, mo_n_factor, mo_p_factor "+
				"        FROM     tb_smart_panel_seg "+
				"        WHERE    weekcode = fn_weekcode('"+accessday+"') "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    and      A.WEEKCODE = B.WEEKCODE "+
				"    GROUP BY A.WEEKCODE, SMART_ID, PACKAGE_NAME, APP_NAME, PRO_ID, SITE_ID , type"+
				") a, "+
				"( "+
				"    select  /*+ordered*/ "+
				"            a.weekcode, smart_id, "+
				"            count(A.panel_id) uu_cnt, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj "+
				"    from "+
				"    ( "+
				"        select /*+ index(a,pk_smart_week_setup_fact) */weekcode, panel_id, a.smart_id "+
				"        from tb_smart_week_setup_FACT a, tb_temp_smart_app_info b "+
				"        where weekcode = fn_weekcode('"+accessday+"') "+
				"        and   a.smart_id = b.smart_id "+
				"        group by weekcode, panel_id, a.smart_id "+
				"    ) a, "+
				"    ( "+
				"        select weekCODE, PANEL_ID, mo_n_factor "+
				"        from tb_smart_panel_seg "+
				"        where weekcode = fn_weekcode('"+accessday+"') "+
				"    ) b "+
				"    where a.weekcode=b.weekcode "+
				"    and   a.panel_id=b.panel_id "+
				"    group by a.weekcode, smart_id "+
				") b "+
				"where a.weekcode = b.weekcode "+
				"and   a.smart_id = b.smart_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String query1 = 
				"update (select /*+index(tb_smart_week_app_sum,pk_smart_week_app_sum)*/ "+
				"            weekcode, smart_id , uu_cnt_adj, tot_duration_adj ,p_tot_duration_adj, p_uu_cnt_adj "+
				"        from  tb_smart_week_app_sum where WEEKCODE = fn_weekcode('"+accessday+"')) a "+
				"set (p_uu_cnt_adj, p_tot_duration_adj) = "+
				"( "+
				"    select   uv - a.uu_cnt_adj, tts - a.tot_duration_adj "+
				"    from "+
				"    ( "+
				"        select /*+use_hash(b,a)*/  "+
				"            a.weekcode, smart_id, package_name, round(sum(mo_n_factor),5) uv, round(sum(mo_p_factor*duration),5) tts "+
				"        from "+
				"        ( "+
				"            select weekcode, panel_id, smart_id, package_name, sum(duration) duration "+
				"            from "+
				"            ( "+
				"                select /*+index(a,pk_smart_week_app_fact)*/ weekcode, panel_id, smart_id, package_name, duration "+
				"                from tb_smart_week_app_fact a "+
				"                where  WEEKCODE = fn_weekcode('"+accessday+"') "+
				"                union all "+
				"                select fn_weekcode(access_day) weekcode, panel_id, smart_id, package_name, duration "+
				"                from tb_smart_day_push_panel_fact "+
				"                where  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"                AND    access_day <= '"+accessday+"' "+
				"            ) "+
				"            group by weekcode, panel_id, smart_id, package_name "+
				"        )a, "+
				"        ( "+
				"            select weekcode, panel_id, mo_n_factor, mo_p_factor"+
				"            from tb_smart_panel_seg "+
				"            where  WEEKCODE = fn_weekcode('"+accessday+"') "+
				"        )b "+
				"        where a.weekcode = b.weekcode "+
				"        and a.panel_id = b.panel_id "+
				"        group by a.weekcode, smart_id, package_name "+
				"    )b "+
				"    where a.weekcode = b.weekcode "+
				"    and a.smart_id = b.smart_id "+
				") ";				
		
		this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppSession
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekAppSession insert
	 *************************************************************************/
	
	public Calendar executeWeekAppSession(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly App Session is processing...");
		String query = 
				"insert into TB_SMART_WEEK_APP_SESSION "+
				"select /*+use_hash(b,c)*/ "+
				"       a.weekcode, a.PANEL_ID, APP_CNT, SETUP_CNT,  "+
				"       DAILY_FREQ_CNT,  "+
				"       tot_duration DURATION, media_duration, laun_duration, "+
				"       MO_N_FACTOR, MO_P_FACTOR,  "+
				"       AGE_CLS, SEX_CLS, NVL(INCOME_CLS,0), NVL(JOB_CLS,0), NVL(EDUCATION_CLS,0), NVL(ISMARRIED_CLS,0), "+
				"       REGION_CD, NVL(LIFESTYLE_CLS,0), KC_SEG_ID, RI_SEG_ID, sysdate        "+
				"from ( "+
				"    select fn_weekcode(access_day) weekcode, panel_id,  "+
				"           count(distinct smart_id) app_cnt,  "+
				"           count(distinct access_day) DAILY_FREQ_CNT, "+
				"           sum(case when flag = 'TOTAL' then duration else 0 end) tot_duration, "+
				"           sum(case when flag = 'MEDIA' then duration else 0 end) media_duration, "+
				"           sum(case when flag = 'LAUN'  then duration else 0 end) laun_duration "+
				"    from  "+
				"    (   "+
				"        select /*+index(a,pk_smart_day_app_fact)*/ access_day, smart_id, panel_id, duration, 'TOTAL' flag "+
				"        from   tb_smart_day_app_fact a "+
				"        where  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"        AND    access_day <= '"+accessday+"' "+
				"        AND    package_name != 'kclick_equal_app' "+
				"         "+
				"        union all "+
				"         "+
				"        select /*+index(a,pk_smart_day_app_fact) use_hash(b,a)*/ access_day, a.smart_id, panel_id, duration, 'MEDIA' flag "+
				"        from   tb_smart_day_app_fact a, TB_SMART_APP_MEDIA_LIST b "+
				"        where  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"        AND    access_day <= '"+accessday+"' "+
				"        and    a.smart_id = b.smart_id "+
	            "        and    b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
	            "        and    b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+ 					
				"         "+
				"        union all "+
				"         "+
				"        select /*+index(a,pk_smart_day_app_fact) use_hash(b,a)*/ access_day, a.smart_id, panel_id, duration, 'LAUN' flag "+
				"        from   tb_smart_day_app_fact a, TB_SMART_APP_LAUN_LIST b "+
				"        where  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"        AND    access_day <= '"+accessday+"' "+
				"        and    a.smart_id = b.smart_id "+
	            "        and    b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
	            "        and    b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+ 					
				"     ) "+
				"     group  by fn_weekcode(access_day), panel_id "+
				") a, "+
				"( "+
				"    select weekcode, panel_id, count(distinct smart_id) SETUP_CNT  "+
				"    from  "+
				"    (         "+
				"        select /*+ index(a,pk_smart_week_setup_fact) */ weekcode, smart_id, panel_id "+
				"        from   tb_smart_week_setup_fact a "+
				"        where  WEEKCODE = fn_weekcode('"+accessday+"') "+
				"        AND    package_name != 'kclick_equal_app' "+
				"    ) "+
				"    group  by weekcode, panel_id "+
				") b, "+
				"( "+
				"    select * "+
				"    from tb_smart_panel_seg "+
				"    where WEEKCODE = fn_weekcode('"+accessday+"') "+
				") c "+
				"where a.weekcode = c.weekcode "+
				"and a.weekcode = b.weekcode "+
				"and a.panel_id = c.panel_id "+
				"and a.panel_id = b.panel_id ";
//		System.out.println(query);
//		System.exit(0);
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	public Calendar executeWeekAppSumError(String weekcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Site Summary Error is processing...");
		
		String queryT = "TRUNCATE TABLE tb_temp_error ";
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		String query = 
				"INSERT INTO tb_temp_error (code,site_id,rr_error) "+
				"SELECT weekcode, smart_id, "+
				"       round(fn_week_modifier(weekcode)*1.96*sqrt((1/power(sum(netizen_cnt),2))*sum(UV_VAR)),4) Reach_E "+
				"FROM "+
				"    ( "+
				"    SELECT '"+weekcode+"' weekcode, smart_id, "+
				"           loc_cd, region_cd, sex_cls, age_cls,  "+
				"           netizen_cnt, "+
				"           netizen_cnt*rr UV_S, "+
				"           power(netizen_cnt,2)*((netizen_cnt-panel_cnt)/netizen_cnt)*(rr*(1-rr)/(decode(panel_cnt,1,1.000000000000001,panel_cnt)-1)) UV_VAR "+
				"    FROM ( "+
				"          SELECT a.smart_id, a.loc_cd, a.region_cd, a.sex_cls, a.age_cls, panel_cnt, netizen_cnt, decode(cnt/panel_cnt,null,0,cnt/panel_cnt) rr "+
				"          FROM   "+
				"            (  "+
				"                SELECT /*+use_hash(a,b) index(a,PK_SMART_WEEK_PERSON_SEG)*/ "+
				"                       smart_id, loc_cd, region_cd, sex_cls, age_cls, max(panel_cnt) panel_cnt, max(netizen_cnt) netizen_cnt  "+
				"                FROM   tb_smart_week_person_seg a, tb_smart_week_app_sum b  "+
				"                WHERE  a.weekcode = '"+weekcode+"' "+
				"                AND    a.weekcode = b.weekcode "+       
				"                and    b.smart_id in (select smart_id "+
				"                                      from (select smart_id, rank()over(order by UU_CNT_ADJ desc) rnk "+
				"                                            from tb_smart_week_app_sum b "+
				"                                            where weekcode = '"+weekcode+"' "+
				"                                            and package_name <> 'kclick_equal_app') "+
				"                                      where rnk <=200 ) "+
				"                GROUP BY smart_id, loc_cd, region_cd, sex_cls, age_cls "+
				"            ) a, "+  
				"            ( "+
				"                SELECT /*+use_hash(b,a) index(b,PK_SMART_WEEK_PERSON_SEG)*/ "+
				"                       a.weekcode, smart_id, b.loc_cd, b.region_cd, b.sex_cls, b.age_cls, count(*) cnt "+
				"                FROM   tb_smart_week_app_fact a, tb_smart_week_person_seg b "+
				"                WHERE  a.weekcode = '"+weekcode+"' "+
				"                AND    a.weekcode = b.weekcode "+
				"                AND    a.panel_id = b.panel_id "+
				"                GROUP BY a.weekcode, smart_id, b.loc_cd, b.region_cd, b.sex_cls, b.age_cls "+
				"            ) b "+
				"         WHERE a.age_cls=b.age_cls(+) "+
				"         AND   a.sex_cls=b.sex_cls(+) "+ 
				"         AND   a.loc_cd=b.loc_cd(+) "+
				"         AND   a.region_cd=b.region_cd(+) "+
				"         AND   a.smart_id = b.smart_id(+) "+
				"        ) "+
				"      ) "+
				"GROUP BY weekcode, smart_id ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String queryU = "UPDATE tb_smart_week_app_sum a "+
						"SET    rr_error = (select rr_error from tb_temp_error b where a.smart_id = b.site_id and a.weekcode = b.code) "+
						"WHERE  weekcode = '"+weekcode+"' "+
						"AND    smart_id in ( "+
						"                        select smart_id from tb_smart_week_app_sum "+
						"                        where weekcode = '"+weekcode+"' "+
						"                        and   smart_id in (select smart_id "+
						"                                           from (select smart_id, rank()over(order by UU_CNT_ADJ desc) rnk "+
						"                                                 from tb_smart_week_app_sum b "+
						"                                                 where weekcode = '"+weekcode+"' "+
						"                                                 and package_name <> 'kclick_equal_app') "+
						"                                           where rnk <=200 ) "+
						"                    )" ;
        this.pstmt = connection.prepareStatement(queryU);
		this.pstmt.executeUpdate();
		
		this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		String query2 = 
				"INSERT INTO tb_temp_error (code,site_id,tts_error) "+
				"SELECT  weekcode, smart_id, "+
				"        round(1.96*sqrt(sum(D_var))/60,2) TTS_E "+
				"FROM ( "+
				"    SELECT b.weekcode weekcode, "+
				"           smart_id, "+
				"           sex_cls, age_cls, loc_cd, region_cd, "+
				"           max(netizen_cnt) netizen_cnt, "+
				"           sum(kc_n_factor) kc_n_factor_S, "+
				"           sum(duration*kc_p_factor) duration_s, "+
				"           ((sum(kc_n_factor)*(sum(kc_n_factor)-count(a.panel_id)))/count(a.panel_id)*((sum(power(duration,2))-power(sum(duration),2)/count(a.panel_id))/(decode(count(a.panel_id),1,1.000000001,count(a.panel_id))-1))) d_var "+
				"    FROM    ( "+
				"                SELECT smart_id, panel_id, duration "+
				"                FROM   tb_smart_week_app_fact "+
				"                WHERE  weekcode = '"+weekcode+"' "+
				"                AND    smart_id in ( "+
				"                    SELECT smart_id "+
				"                    FROM   tb_smart_week_app_sum "+
				"                    WHERE  weekcode = '"+weekcode+"' "+
				"                    and   smart_id in (select smart_id "+
				"                                       from (select smart_id, rank()over(order by UU_CNT_ADJ desc) rnk "+
				"                                             from tb_smart_week_app_sum b "+
				"                                             where weekcode = '"+weekcode+"' "+
				"                                             and package_name <> 'kclick_equal_app') "+
				"                                       where rnk <=200 ) "+
				"                ) "+
				"            ) a, "+ 
				"            ( "+
				"                SELECT b.weekcode, a.panel_id, "+
				"                       a.SEX_CLS, a.AGE_CLS, a.LOC_CD, a.REGION_CD, P_person kc_p_factor,person kc_n_factor, netizen_cnt, panel_cnt "+
				"                FROM   tb_smart_week_person_seg a, tb_smart_panel_seg b "+
				"                WHERE  a.weekcode = b.weekcode "+
				"                AND    b.weekcode = '"+weekcode+"' "+
				"                AND    a.panel_id = b.panel_id "+
				"            ) b "+
				"    WHERE    b.panel_id=a.panel_id "+
				"    GROUP BY b.weekcode, a.smart_id, sex_cls, age_cls, loc_cd, region_cd "+
				"    ) "+
				"GROUP BY weekcode, smart_id" ;
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		String queryU2 ="UPDATE tb_smart_week_app_sum a "+
						"SET    tts_error = (SELECT tts_error from tb_temp_error b WHERE a.smart_id = b.site_id AND a.weekcode = b.code) "+
						"WHERE  weekcode = '"+weekcode+"' "+
						"AND    smart_id in ( "+
						"                        select smart_id from tb_smart_week_app_sum "+
						"                        where weekcode = '"+weekcode+"' "+
						"                        and   smart_id in (select smart_id "+
						"                                           from (select smart_id, rank()over(order by UU_CNT_ADJ desc) rnk "+
						"                                                 from tb_smart_week_app_sum b "+
						"                                                 where weekcode = '"+weekcode+"' "+
						"                                                 and package_name <> 'kclick_equal_app') "+
						"                                           where rnk <=200 ) "+
						"                    )";

		this.pstmt = connection.prepareStatement(queryU2);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}	
	
	/**************************************************************************
	 *		메소드명		: executeWeekDaytimeAppSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 주간 앱 시간대 summary insert
	 *************************************************************************/
	
	public Calendar executeWeekDaytimeAppSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Week Daytime App is processing...");
		String query =
				"insert into tb_smart_week_daytime_app_sum "+
				"select fn_Weekcode(access_day) weekcode, time_cd, smart_id, package_name, app_name, pro_id, site_id, "+
				"    APP_CATEGORY_CD1, APP_CATEGORY_CD2, "+
				"    round(sum(uu_cnt_adj)/7) uu_cnt_adj, "+
				"    round(sum(tot_duration_adj)/7) tot_duration_adj, "+
				"    round(sum(avg_duration)/7,2) avg_duration, "+
				"    round(sum(app_cnt_adj)/7) app_cnt_adj, "+
				"    sysdate , "+
				"    round(sum(reach_rate_adj)/7,2) reach_rate_adj "+
				"from "+
				"( "+
				"    select /*+no_merge(a)*/  a.access_day, time_cd, smart_id, package_name, APP_NAME, PRO_ID, SITE_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2, "+
				"        round(sum(mo_n_factor)) uu_cnt_adj, "+
				"        round(sum(mo_p_factor*duration)) tot_duration_adj, "+
				"        round(sum(mo_p_factor*duration)/sum(mo_n_factor),2) avg_duration, "+
				"        round(sum(mo_p_factor*app_cnt)) app_cnt_adj, "+
				"        round(sum(mo_n_factor)/FN_SMART_day_NFACTOR(A.access_day)*100,2) reach_rate_adj "+
				"    from "+
				"    ( "+
				"        select /*+ index(a,pk_smart_daytime_app_fact)*/ access_Day, decode(time_cd,'24','23',time_cd) time_cd, "+
				"            a.smart_id, a.package_name, panel_id, APP_NAME, APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, pro_id, sum(duration) duration, sum(app_cnt) app_cnt "+
				"        from tb_smart_daytime_app_fact a, tb_smart_week_app_sum b "+
				"        WHERE    access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"        AND      access_day <= '"+accessday+"' "+
				"        and      a.smart_id = b.smart_id "+
				"        and      b.weekcode = fn_weekcode('"+accessday+"') "+
				"        group by access_day, decode(time_cd,'24','23',time_cd), a.smart_id, a.package_name, panel_id, APP_NAME, APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, pro_id "+
				"    )a, "+
				"    ( "+
				"        select access_day, panel_id, mo_n_factor, mo_p_factor "+
				"        from tb_smart_day_panel_seg "+
				"        WHERE    access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"        AND      access_day <= '"+accessday+"' "+
				"    )b "+
				"    where a.access_day = b.access_day "+
				"    and a.panel_id = b.panel_id "+
				"    group by a.access_day, time_cd, smart_id, package_name, APP_NAME, PRO_ID, SITE_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				") "+
				"group by fn_Weekcode(access_day), time_cd, smart_id, package_name, app_name, pro_id, site_id, APP_CATEGORY_CD1, APP_CATEGORY_CD2 ";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekDailyAppSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 주간 앱 일간 summary insert
	 *************************************************************************/
	
	public Calendar executeWeekDailyAppSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Week Daily App is processing...");
		String query =
				"insert into TB_SMART_DAILY_WEEK_APP_SUM "+
				"select  fn_weekcode('"+accessday+"') weekcode, smart_id, package_name, "+
				"        MAX_CATEGORY_CODE1, "+
				"        MAX_CATEGORY_CODE2, "+
				"        UV, RR, APP_CNT, DT, "+
				"        UV_weekdays, APP_CNT_weekdays, DT_weekdays, "+
				"        UV_weekend, APP_CNT_weekend, DT_weekend, "+
				"        rank() over (order by UV desc) uu_overall_rank, "+
				"        rank() over (order by APP_CNT desc) APP_CNT_overall_rank, "+
				"        rank() over (order by DT desc) avg_duration_overall_rank, "+
				"        sysdate proc_date "+
				"FROM "+
				"(       select  smart_id, package_name, "+
				"                round(sum(UV)/7) UV, "+
				"                round(sum(RR)/7, 2) RR, "+
				"                round(sum(APP_CNT)/7) APP_CNT, "+
				"                round(sum(DT)/7, 2) DT, "+
				"                max(APP_CATEGORY_CD1) MAX_CATEGORY_CODE1, "+
				"                max(APP_CATEGORY_CD2) MAX_CATEGORY_CODE2, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"                         AND  access_day <= to_char(to_date('"+accessday+"','YYYYMMDD')-2,'YYYYMMDD') "+
				"                         then UV end)/5) UV_weekdays, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"                         AND  access_day <= to_char(to_date('"+accessday+"','YYYYMMDD')-2,'YYYYMMDD') "+
				"                         then APP_CNT end)/5) APP_CNT_weekdays, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+ 
				"                         AND  access_day <= to_char(to_date('"+accessday+"','YYYYMMDD')-2,'YYYYMMDD') "+
				"                         then DT end)/5, 2) DT_weekdays, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-1,'YYYYMMDD') "+
				"                         AND  access_day <= '"+accessday+"' "+
				"                         then UV end)/2) UV_weekend, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-1,'YYYYMMDD') "+
				"                         AND  access_day <= '"+accessday+"' "+
				"                         then APP_CNT end)/2) APP_CNT_weekend, "+
				"                round(sum(case when access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-1,'YYYYMMDD') "+
				"                         AND  access_day <= '"+accessday+"' "+
				"                         then DT end)/2, 2) DT_weekend "+
				"        from   (select /*+ ordered(a) */ "+
				"                       a.access_day, smart_id, package_name, "+
				"                       uu_cnt_adj*fn_day_modifier(a.access_day) UV, "+
				"                       reach_rate_adj RR, "+
				"                       avg_duration/60 DT, "+
				"                       APP_CATEGORY_CD1, APP_CATEGORY_CD2, "+
				"                       APP_CNT_ADJ*fn_day_modifier(a.access_day) APP_CNT "+
				"                from   tb_smart_day_app_sum a "+
				"                where  a.access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"                AND    a.access_day <= '"+accessday+"' "+
				"               ) "+
				"        group by smart_id, package_name "+
				")a ";
		
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekNextApp
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 주간 앱 일간 summary insert
	 *************************************************************************/
	
	public Calendar executeWeekNextApp(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Week Next App is processing...");
		String query =
				"insert into TB_SMART_WEEK_NEXT_APP "+
				"select * "+
				"from "+
				"( "+
				"    select a.weekcode, smart_id,  package_name, fn_smart_app_name(smart_id) app_name, "+
				"        next_smart_id, next_package_name, fn_smart_app_name(next_smart_id) next_app_name,  APP_CATEGORY_CD1, APP_CATEGORY_CD2, "+
				"        round(sum(mo_n_factor),5) uu_cnt_adj, "+
				"        round(sum(mo_n_factor)/uu_cnt_adj*100,2) reach_rate_adj, "+
				"        round(sum(mo_p_factor*move_cnt),5) move_cnt_adj, "+
				"        rank()over(partition by a.weekcode, smart_id, APP_CATEGORY_CD1 order by sum(mo_n_factor) desc) CATEGORY1_RANK, "+
				"        rank()over(partition by a.weekcode, smart_id, APP_CATEGORY_CD2 order by sum(mo_n_factor) desc) CATEGORY2_RANK, "+
				"        sysdate proc_date "+
				"     from "+
				"     ( "+
				"        select fn_weekcode(access_day) weekcode, panel_id, a.smart_id, package_name, next_smart_id, next_package_name, APP_CATEGORY_CD2, APP_CATEGORY_CD1, "+
				"        sum(move_cnt) move_cnt, max(uu_cnt_adj) uu_cnt_adj "+
				"        from tb_smart_day_app_navi_Fact a, "+
				"        ( "+
				"            select smart_id, APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				"            from tb_temp_smart_app_info "+
				"        )b, "+
				"        ( "+
				"            select smart_id , uu_cnt_adj "+
				"            from tb_smart_week_app_sum "+
				"            where weekcode = fn_weekcode('"+accessday+"') "+
				"            and reach_rate >= 0.1 "+
				"        ) c "+
				"        where  a.access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"        AND    a.access_day <= '"+accessday+"' "+
				"        and a.smart_id = c.smart_id "+
				"        and a.next_smart_id = b.smart_id "+
				"        group by fn_weekcode(access_day), panel_id, a.smart_id, package_name, next_smart_id, next_package_name, APP_CATEGORY_CD2, APP_CATEGORY_CD1 "+
				"     )a, "+
				"     ( "+
				"        select weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"        from tb_smart_panel_seg "+
				"        where weekcode = fn_weekcode('"+accessday+"') "+
				"     )b "+
				"     where a.weekcode = b.weekcode "+
				"     and a.panel_id = b.panel_id "+
				"     group by a.weekcode,  smart_id, package_name, next_smart_id, next_package_name,APP_CATEGORY_CD1, APP_CATEGORY_CD2, uu_cnt_adj "+
				") "+
				"where CATEGORY2_RANK <= 100";
		
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekPreApp
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 주간 앱 일간 summary insert
	 *************************************************************************/
	
	public Calendar executeWeekPreApp(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Week Pre App is processing...");
		String query =
				"insert into TB_SMART_WEEK_PRE_APP "+
				"select * "+
				"from "+
				"( "+
				"    select /*+leading(b,a)*/a.weekcode, smart_id, package_name, fn_smart_app_name(smart_id) app_name, "+
				"        pre_smart_id, pre_package_name, fn_smart_app_name(pre_smart_id) pre_app_name, APP_CATEGORY_CD1 PRE_CATEGORY_CD1, APP_CATEGORY_CD2 PRE_CATEGORY_CD2, "+
				"        round(sum(mo_n_factor),5) uu_cnt_adj, "+
				"        round(sum(mo_n_factor)/uu_cnt_adj*100,2) reach_rate_adj, "+
				"        round(sum(mo_p_factor*move_cnt),5) move_cnt_adj, "+
				"        rank()over(partition by a.weekcode, smart_id, APP_CATEGORY_CD1 order by sum(mo_n_factor) desc) CATEGORY1_RANK, "+
				"        rank()over(partition by a.weekcode, smart_id, APP_CATEGORY_CD2 order by sum(mo_n_factor) desc) CATEGORY2_RANK, "+
				"        sysdate pro_date "+
				"     from "+
				"     ( "+
				"         select /*+qb_name(main) leading(b@sub) use_hash(a@main)*/fn_weekcode(access_day) weekcode, panel_id, next_smart_id smart_id, next_package_name package_name, a.smart_id pre_smart_id, package_name pre_package_name, APP_CATEGORY_CD2, APP_CATEGORY_CD1, "+
				"            sum(move_cnt) move_cnt, max(uu_cnt_adj) uu_cnt_adj "+
				"         from tb_smart_day_app_navi_Fact a, "+
				"        ( "+
				"            select smart_id pre_smart_id, APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				"            from tb_temp_smart_app_info "+
				"        )b, "+
				"        ( "+
				"            select smart_id , uu_cnt_adj "+
				"            from tb_smart_week_app_sum  "+
				"            where weekcode = fn_weekcode('"+accessday+"')  "+
				"            and reach_rate >= 0.1 "+
				"        ) c "+
				"       where  a.access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"       AND    a.access_day <= '"+accessday+"' "+
				"        and a.next_smart_id = c.smart_id "+
				"        and a.smart_id = b.pre_smart_id "+
				"        group by fn_weekcode(access_day), panel_id, a.smart_id, package_name, next_smart_id, next_package_name, APP_CATEGORY_CD2, APP_CATEGORY_CD1 "+
				"     )a, "+
				"     ( "+
				"        select weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"        from tb_smart_panel_seg "+
				"        where weekcode in fn_weekcode('"+accessday+"') "+
				"     )b "+
				"     where a.weekcode = b.weekcode "+
				"     and a.panel_id = b.panel_id "+
				"     group by a.weekcode,  smart_id, package_name, pre_smart_id, pre_package_name,APP_CATEGORY_CD1, APP_CATEGORY_CD2, uu_cnt_adj "+
				" ) "+
				" where CATEGORY2_RANK <= 100 ";
		
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppLvl1
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekAppLvl1 insert
	 *************************************************************************/
	
	public Calendar executeWeekAppLvl1(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly App Level1 is processing...");
		String query =
				"insert into TB_SMART_WEEK_APPLVL1_SUM "+
				"select /*+ordered*/a.weekcode, a.app_category_cd1, reach_rate, a.uu_cnt, uu_overall_rank, avg_duration, avg_duration_overall_rank, "+
			    "   daily_freq_cnt, a.uu_cnt_adj, reach_rate_adj, freq_overall_rank, tot_duration_overall_rank, tot_duration_adj, "+
			    "   round(a.uu_cnt/b.uu_cnt*100,2) install_rate, round(a.uu_cnt_adj/b.uu_cnt_adj*100,2) install_rate_adj, sysdate, "+
			    "   b.uu_cnt install_uu_cnt, b.uu_cnt_adj install_uu_cnt_adj, app_cnt_adj, keyuser_uv "+
				"from "+
				"( "+
				"    SELECT   /*+use_hash(b,a)*/ "+
				"             A.weekcode,  app_category_cd1, "+
				"             round(count(A.panel_id)/FN_SMART_WEEK_COUNT(A.weekcode)*100,2) reach_rate, "+
				"             count(A.panel_id) uu_cnt, "+
				"             rank() over (partition by a.weekcode order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"             round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"             rank() over (partition by a.weekcode order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"             round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"             round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"             round(sum(B.mo_n_factor)/FN_SMART_WEEK_NFACTOR(A.weekcode)*100,5) reach_rate_adj, "+
				"             rank() over (partition by a.weekcode order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"             rank() over (partition by a.weekcode order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
				"             round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"             round(sum(app_cnt*mo_p_factor),5) app_cnt_adj, "+		
				"             round(sum(decode(keyuser_cd, 'H', mo_p_factor, 0)),5)keyuser_uv "+
				"    FROM "+
				"    ( "+
				"		 SELECT   weekcode, PANEL_ID, APP_CATEGORY_CD1, DURATION, DAILY_FREQ_CNT, app_cnt, keyuser_cd  "+
				"		 FROM     tb_smart_week_applvl1_fact "+
				"		 WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				"    ) A, "+
				"    ( " +
				"        SELECT   weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"        FROM     tb_smart_panel_seg "+
				"        WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    AND      A.weekcode = B.weekcode "+
				"    GROUP BY A.weekcode, APP_CATEGORY_CD1 "+
				") a, "+
				"( "+
				"    select  /*+ordered*/ "+
				"            a.weekcode, APP_CATEGORY_CD1, "+
				"            count(A.panel_id) uu_cnt, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj "+
				"    from "+
				"    ( "+
				"        select /*+ index(a,pk_smart_week_setup_fact) */weekcode, panel_id, APP_CATEGORY_CD1 "+
				"        from tb_smart_week_setup_FACT a, tb_smart_app_info b "+
				"        where WEEKCODE = fn_weekcode('"+accessday+"') "+
				"        and a.smart_id = b.smart_id "+
				"        and b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
				"        and b.ef_time  <  to_date('"+accessday+"','yyyymmdd')+1 "+
				"        group by weekcode, panel_id, APP_CATEGORY_CD1 "+
				"    ) a, "+
				"    ( "+
				"        select weekCODE, PANEL_ID, mo_n_factor "+
				"        from tb_smart_panel_seg "+
				"        where WEEKCODE = fn_weekcode('"+accessday+"') "+
				"    ) b "+
				"    where a.weekcode=b.weekcode "+
				"    and a.panel_id=b.panel_id "+
				"    group by a.weekcode, APP_CATEGORY_CD1 "+
				") b "+
				"where a.weekcode = b.weekcode "+
				"and   a.APP_CATEGORY_CD1 = b.APP_CATEGORY_CD1(+) ";
				
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppLvl2
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekAppLvl2 insert
	 *************************************************************************/
	
	public Calendar executeWeekAppLvl2(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly App Level2 is processing...");
		String query = 
				"insert into TB_SMART_WEEK_APPLVL2_SUM "+
				"select a.weekcode, a.app_category_cd1, a.app_category_cd2, reach_rate, a.uu_cnt, uu_overall_rank, uu_1level_rank, "+
			    "       avg_duration, avg_duration_overall_rank, avg_duration_1level_rank, daily_freq_cnt, a.uu_cnt_adj, "+
			    "       reach_rate_adj, freq_overall_rank, tot_duration_overall_rank, tot_duration_adj, "+
			    "       round(a.uu_cnt/b.uu_cnt*100,2) install_rate, round(a.uu_cnt_adj/b.uu_cnt_adj*100,2) install_rate_adj, "+
			    "       sysdate, b.uu_cnt install_uu_cnt, b.uu_cnt_adj install_uu_cnt_adj, app_cnt_adj, keyuser_uv "+
				"from "+
				"( "+
				"    SELECT   /*+use_hash(b,a)*/ "+
				"             A.weekcode,  app_category_cd1, APP_CATEGORY_CD2, "+
				"             round(count(A.panel_id)/FN_SMART_WEEK_COUNT(A.weekcode)*100,2) reach_rate, "+
				"             count(A.panel_id) uu_cnt, "+
				"             rank() over (partition by a.weekcode order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"             rank() over (partition by a.weekcode, min(app_category_cd1) order by sum(B.mo_n_factor) desc) uu_1level_rank, "+
				"             round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"             rank() over (partition by a.weekcode order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"             rank() over (partition by a.weekcode,min(app_category_cd1) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_1level_rank, "+
				"             round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"             round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"             round(sum(B.mo_n_factor)/FN_SMART_WEEK_NFACTOR(A.weekcode)*100,5) reach_rate_adj, "+
				"             rank() over (partition by a.weekcode order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"             rank() over (partition by a.weekcode order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
				"             round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"             round(sum(app_cnt*mo_p_factor),5) app_cnt_adj, "+
				"             round(sum(decode(keyuser_cd, 'H', mo_p_factor, 0)),5) keyuser_uv "+
				"    FROM "+
				"    ( "+
				"		SELECT   weekcode, PANEL_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2, DURATION, DAILY_FREQ_CNT, app_cnt, keyuser_cd  "+
				"		FROM     tb_smart_week_applvl2_fact "+
				"		WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				"    ) A, "+
				"    ( "+
				"       SELECT   panel_id, WEEKCODE, mo_n_factor, mo_p_factor "+
				"       FROM     tb_smart_panel_seg "+
				"       WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    and      A.WEEKCODE = B.WEEKCODE "+
				"    GROUP BY A.weekcode, APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				") a, "+
				"( "+
				"    select  /*+ordered*/ "+
				"            a.weekcode, APP_CATEGORY_CD2, "+
				"            count(A.panel_id) uu_cnt, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj "+
				"    from "+
				"    ( "+
				"        select /*+ index(a,pk_smart_week_setup_fact) */weekcode, panel_id, APP_CATEGORY_CD2 "+
				"        from tb_smart_week_setup_FACT a, tb_smart_app_info b "+
				"        where WEEKCODE = fn_weekcode('"+accessday+"') "+
				"        and a.smart_id = b.smart_id "+
				"        and b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
				"        and b.ef_time  <  to_date('"+accessday+"','yyyymmdd')+1 "+
				"        group by weekcode, panel_id, APP_CATEGORY_CD2 "+
				"    ) a, "+
				"    ( "+
				"        select WEEKCODE, PANEL_ID, mo_n_factor "+
				"        from  tb_smart_panel_seg "+
				"        where WEEKCODE = fn_weekcode('"+accessday+"') "+
				"    ) b "+
				"    where a.weekcode=b.weekcode "+
				"    and a.panel_id=b.panel_id "+
				"    group by a.weekcode, APP_CATEGORY_CD2 "+
				") b "+
				"where a.weekcode = b.weekcode "+
				"and   a.APP_CATEGORY_CD2 = b.APP_CATEGORY_CD2(+) ";
				
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppLv1Seg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekAppLvl2 insert
	 *************************************************************************/
	
	public Calendar executeWeekAppLv1Seg(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly App Level1 SEG is processing...");
		
		
		String query = 
				"insert  into TB_SMART_WEEK_SEG_APPlv1 "+
				"select  fn_weekcode('"+accessday+"') weekcode, "+
				"        kc_seg_id, "+
				"        v_fact.APP_CATEGORY_CD1, "+
				"        min(v_panel_seg.age_cls)        age_cls, "+
				"        min(v_panel_seg.sex_cls)        sex_cls, "+
				"        min(v_panel_seg.income_cls)     income_cls, "+  
				"        min(v_panel_seg.job_cls)        job_cls, "+
				"        min(v_panel_seg.education_cls)  education_cls, "+
				"        min(v_panel_seg.ismarried_cls ) ismarried_cls, "+ 
				"        min(v_panel_seg.region_cd )     region_cd,  "+
				"        count(distinct v_fact.panel_id) uu_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor),5) uu_est_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"        round(sum(duration*v_panel_seg.mo_p_factor),5) duration_est, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"        sum(app_cnt) app_cnt, "+
				"        round(sum(app_cnt*v_panel_seg.mo_p_factor),5) app_est_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(app_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor)/ max(sum_kc_nfactor)*100, 5) reach_rate, "+
				"        sysdate "+
				"from  "+
				"( "+
				"        select  /*+use_hash(b,a) index(a,pk_smart_week_applvl1_fact)*/ weekcode, a.APP_CATEGORY_CD1, panel_id, duration, daily_freq_cnt, app_cnt "+
				"        from    tb_smart_week_applvl1_fact a "+
				"        where   weekcode = fn_weekcode('"+accessday+"') "+
				") v_fact, "+
				"( "+
				"        select  panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"        from    tb_smart_panel_seg "+
				"        where   weekcode = fn_weekcode('"+accessday+"') "+
				") v_panel_seg, "+
				"( "+
				"        select  sum(mo_n_factor) sum_kc_nfactor "+
				"        from    tb_smart_panel_seg "+
				"        where   weekcode = fn_weekcode('"+accessday+"') "+
				") v_total_panel_seg "+
				"where v_fact.panel_id   = v_panel_seg.panel_id "+
				"group by v_panel_seg.kc_seg_id, v_fact.APP_CATEGORY_CD1 ";
				
		System.out.print(query);
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppLv2Seg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekAppLv2Seg insert
	 *************************************************************************/
	
	public Calendar executeWeekAppLv2Seg(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly App Level2 is processing...");
		String query = 
				"insert  into TB_SMART_WEEK_SEG_APPLV2 "+
				"select  fn_weekcode('"+accessday+"') weekcode, "+
				"        kc_seg_id, "+
				"        v_fact.APP_CATEGORY_CD2, "+
				"        min(v_panel_seg.age_cls)        age_cls, "+
				"        min(v_panel_seg.sex_cls)        sex_cls, "+
				"        min(v_panel_seg.income_cls)     income_cls, "+  
				"        min(v_panel_seg.job_cls)        job_cls, "+
				"        min(v_panel_seg.education_cls)  education_cls, "+
				"        min(v_panel_seg.ismarried_cls ) ismarried_cls, "+
				"        min(v_panel_seg.region_cd )     region_cd,  "+
				"        count(distinct v_fact.panel_id) uu_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor),5) uu_est_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"        round(sum(duration*v_panel_seg.mo_p_factor),5) duration_est, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"        sum(app_cnt) app_cnt, "+
				"        round(sum(app_cnt*v_panel_seg.mo_p_factor),5) app_est_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(app_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor)/ max(sum_kc_nfactor)*100, 5) reach_rate, "+
				"        sysdate "+
				"from  "+
				"( "+
				"        select  /*+use_hash(b,a) index(a,pk_smart_week_applvl1_fact)*/ weekcode, a.APP_CATEGORY_CD2, panel_id, duration, daily_freq_cnt, app_cnt "+
				"        from    tb_smart_week_applvl2_fact a "+
				"        where   weekcode = fn_weekcode('"+accessday+"') "+
				") v_fact, "+
				"( "+
				"        select  panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"        from    tb_smart_panel_seg "+
				"        where   weekcode = fn_weekcode('"+accessday+"') "+
				") v_panel_seg, "+
				"( "+
				"        select  sum(mo_n_factor) sum_kc_nfactor "+
				"        from    tb_smart_panel_seg "+
				"        where   weekcode = fn_weekcode('"+accessday+"') "+
				") v_total_panel_seg "+
				"where v_fact.panel_id   = v_panel_seg.panel_id "+
				"group by v_panel_seg.kc_seg_id, v_fact.APP_CATEGORY_CD2 ";
				
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}	
	/**************************************************************************
	 *		메소드명		: executeWeekAppSiteSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekAppSiteSum insert
	 *************************************************************************/
	
	public Calendar executeWeekAppSiteSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly App Site Sum is processing...");
		String query = 
				"insert into tb_smart_week_app_sum_site "+
				"SELECT   /*+use_hash(b,a)*/ "+
				"         A.weekcode,  SITE_ID, "+
				"         round(count(A.panel_id)/FN_SMART_WEEK_COUNT(A.weekcode)*100,2) reach_rate, "+
				"         count(A.panel_id) uu_cnt, "+
				"         rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"         round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"         rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"         round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"         round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"         round(sum(B.mo_n_factor)/FN_SMART_WEEK_NFACTOR(A.weekcode)*100,5) reach_rate_adj, "+
				"         rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"         rank() over (order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank, "+
				"         round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"         sysdate, "+
				"         round(sum(app_cnt*mo_p_factor),5) app_cnt_adj "+
				"FROM "+
				"( "+
				"    SELECT   /*+ index(a,pk_smart_day_app_fact) */ fn_weekcode(access_day) weekcode, site_id, panel_id, "+
				"             count(distinct access_day) daily_freq_cnt, sum(duration) duration, sum(app_cnt) app_cnt "+
				"    FROM     tb_smart_day_app_fact a, tb_smart_app_info b "+
				"    WHERE    access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"    AND      access_day <= '"+accessday+"' "+
				"    AND      a.smart_id = b.smart_id "+
				"	 and b.package_name != 'kclick_equal_app' "+
				"    and b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
				"    and b.ef_time  <  to_date('"+accessday+"','yyyymmdd')+1 "+
				"    AND      b.site_id is not null "+
				"         group by fn_weekcode(access_day), site_id, panel_id "+
				") A, "+
				"( "+
				"    SELECT   weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"    FROM     tb_smart_panel_seg "+
				"    WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				") B "+
				"WHERE    A.panel_id = B.panel_id "+
				"AND      A.weekcode = B.weekcode "+
				"GROUP BY A.weekcode, SITE_ID ";
				
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekEntertain
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekEntertain insert
	 *************************************************************************/
	
	public Calendar executeWeekEntertain(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Entertainment is processing...");
		//쿼리 교체 (code_num 4,5 추가 20150105주간 적용)
		String queryFact = 
				"insert into TB_SMART_WEEK_SERVICE_FACT "+
				"select access_day, site_id, code_num, panel_id,  "+
				"       sum(pv_cnt) pv_cnt,  "+
				"       sum(duration) duration,  "+
				"       tab_flag, sysdate "+
				"from ( "+
				"    select  /*+ index(a,pk_smart_day_fact) */ access_day, site_id,  "+
				"            case when category_code1='E' then '1' "+
				"                 when category_code2='DJ' then '2' "+
				"                 when category_code2='DB' then '3' "+
				"                 when category_code1='C' then '4' "+
				"                 when category_code1='Q' then '5' "+
				"            else 'Bad' end code_num, "+
				"            panel_id, pv_cnt, duration, 'W' tab_flag "+
				"    from    tb_smart_day_fact a "+
				"    where    access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				"    and     (category_code1 in ('E','C','Q') or category_code2 in ('DJ','DB') " +
				"	 ) " +
				//"	 and     (category_code1='E' or category_code2 in ('DJ','DB')) "+
				"    union all "+
				"    select /*+ index(a,pk_smart_day_app_fact) use_hash(b,a)*/ access_day, site_id, "+
				"           case when b.app_category_cd1='C' then '1' "+
				"                when b.app_category_cd2='176' then '2' "+ //카테고리 변경으로 인한 113->176번 변경
				"                when b.app_category_cd2='114' then '3' "+
				"                when b.app_category_cd1='B' then '4' "+
				"                when b.app_category_cd1='L' then '5' "+
				"           else 'Bad' end code_num, "+
				"           panel_id, 0 pv_cnt, duration, 'A' tab_flag "+
				"    from   tb_smart_day_app_fact a, tb_smart_app_info b "+
				"    where  a.smart_id=b.smart_id "+
				"    and    a.package_name != 'kclick_equal_app' "+
				"    and     access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				//"    and    b.site_id is not null "+
				"    and    b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
				"    and    b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
				"    and    (b.app_category_cd1 in ('C','B','L') or b.app_category_cd2 in ('176','114')) "+
				"    union all "+
				"    select access_day, SITE_ID, "+
				"           case when section_id=8   then '1' "+
				"                when section_id=26  then '2' "+
				"                when section_id=402  then '3' "+
				"                when psection_id=5  then '4' "+
				"                when psection_id=6  then '5' "+
				"           else 'Bad' end code_num, "+
				"           panel_id, pv_cnt, duration, 'S' tab_flag "+
				"    from   tb_smart_week_temp_section "+
				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				"    and    (section_id in (8,26,402) or psection_id = 5 or (psection_id=6 and site_id != 262)) "+
//				"    union all "+
//				"    select access_day, SITE_ID, "+
//				"           '3' code_num, "+
//				"           panel_id, pv_cnt, duration, 'S' tab_flag "+
//				"    from   tb_smart_week_temp_section "+
//				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
//				"    and    section_id = 402 "+
				") "+
				"group by access_day, site_id, code_num, panel_id, tab_flag ";
				
		this.pstmt = connection.prepareStatement(queryFact);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query = 
				"insert into TB_SMART_WEEK_SERVICE_SUM "+
				"select  /*+leading(b) use_hash(b,a)*/ a.weekcode, code_num, "+
				"        round(sum(B.monf),5) uu_cnt_adj, "+
				"        round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"        round(decode(sum(B.monf),0,1, sum(A.duration * B.mo_p_factor)/sum(B.monf)),2) avg_duration, "+
				"        round(decode(sum(B.mo_p_factor),0,1, sum(daily_freq_cnt*B.monf)/sum(B.monf)),2) daily_freq_cnt, "+
				"        round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"        round(sum(B.monf)/fn_smart_week_nfactor(a.weekcode)*100,5) reach_rate_adj, "+
				"        decode(round(sum(B.monf),5), 0, 0, round(round(sum(A.pv_cnt*B.mo_p_factor),5)/round(sum(B.monf),5), 2)) avg_pv, "+
				"        sysdate proc_date "+
				"from "+
				"( "+
				"    select   /*+ordered*/ fn_weekcode(access_day) weekcode, code_num, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt "+
				"    from     TB_SMART_WEEK_SERVICE_FACT "+
				"    WHERE    access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"    AND      access_day <= '"+accessday+"' "+
				"    group by fn_weekcode(access_day), code_num, panel_id "+
				") a, "+
				"( "+
				"    select weekcode, panel_id, mo_n_factor monf, mo_p_factor "+
				"    from   tb_smart_panel_seg "+
				"    WHERE  WEEKCODE = fn_weekcode('"+accessday+"') "+
				") b "+
				"where a.weekcode=b.weekcode "+
				"and   a.panel_id=b.panel_id "+
				"group by a.weekcode, code_num ";
		//System.out.println(query);
		//System.exit(0);
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query1 = 
				"insert into tb_smart_week_service_site "+
				"select  a.weekcode, code_num, site_id, "+
				"        round(sum(B.monf),5) uu_cnt_adj, "+
				"        round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"        round(decode(sum(B.monf),0,1, sum(A.duration * B.mo_p_factor)/sum(B.monf)),2) avg_duration, "+
				"        round(decode(sum(B.mo_p_factor),0,1, sum(daily_freq_cnt*B.monf)/sum(B.monf)),2) daily_freq_cnt, "+
				"        round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"        round(sum(B.monf)/fn_smart_week_nfactor(a.weekcode)*100,5) reach_rate_adj, "+
				"        decode(round(sum(B.monf),5), 0, 0, round(round(sum(A.pv_cnt*B.mo_p_factor),5)/round(sum(B.monf),5), 2)) avg_pv, "+
				"        sysdate proc_date "+
				"from "+
				"( "+
				"    select   fn_weekcode(access_day) weekcode, code_num, site_id, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt "+
				"    from     TB_SMART_WEEK_SERVICE_FACT "+
				"    WHERE    access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"    AND      access_day <= '"+accessday+"' " +
				"	 AND      site_id > 0 "+
				"    group by fn_weekcode(access_day), code_num, site_id, panel_id "+
				") a, "+
				"( "+
				"    select weekcode, panel_id, mo_n_factor monf, mo_p_factor "+
				"    from   tb_smart_panel_seg "+
				"    WHERE  WEEKCODE = fn_weekcode('"+accessday+"') "+
				") b "+
				"where a.weekcode=b.weekcode "+
				"and   a.panel_id=b.panel_id "+
				"group by a.weekcode, code_num, site_id ";
				
		this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		
		/* 리포트 개편으로 인한 PC+Mobile service 쿼리 생성 - kwshin 20170601 */
		String query2 = 
				"insert into TB_WEEK_TOTAL_SERVICE_FACT "+
				"select weekcode, access_day, site_id, code_num, panel_id, max(mo_n_factor) uu_cnt_adj, sum(pv_cnt) pv_cnt_adj, sum(duration) tot_duration_adj, sysdate, "+
				"   sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls "+
				"from "+
				"( "+
				"    select a.weekcode, access_day, a.panel_id, site_id, code_num, mo_n_factor, pv_cnt*mo_p_factor pv_cnt, duration*mo_p_factor duration, "+
				"        sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls "+
				"    from "+
				"    ( "+
				"         select /*+index(a,PK_SMART_WEEK_SERVICE_FACT) */ fn_weekcode(access_day) weekcode, access_day, panel_id, site_id, code_num, pv_cnt, duration "+
				"         from tb_smart_week_service_fact a "+
				"         WHERE   access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"         AND     access_day <= '"+accessday+"' "+
				"         and code_num in (1,2,3,5) "+
				"    )a, "+
				"    ( "+
				"        select weekcode, panel_id, mo_n_factor, mo_p_factor, sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls "+
				"        from tB_smart_panel_seg "+
				"        where weekcode = fn_weekcode('"+accessday+"') "+
				"    )b "+
				"    where a.weekcode = b.weekcode "+
				"    and a.panel_id = b.panel_id "+
				"    union all "+
				"    select a.weekcode, access_day, a.panel_id, site_id, code_num, kc_n_factor, pv_cnt*kc_p_factor pv_cnt, duration*kc_p_factor duration, "+
				"        sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls"+
				"    from "+
				"    ( "+
				"        select fn_weekcode(access_day) weekcode, access_day, panel_id, site_id, code_num, sum(pv_cnt) pv_cnt, sum(duration) duration "+
				"        from "+
				"        ( "+
				"            SELECT  /*+parallel(c 8)*/  "+
				"                    access_day,  case when section_id in ('22','23') then '5' "+
				"                                      when psection_id ='26' then '2' "+
				"                                      when psection_id = '8' then '1' "+
				"                                      when section_id = '402' then '3' end code_num, panel_id, site_id, to_number(pv_cnt) pv_cnt, duration "+
				"            FROM    tb_week_temp_section c "+
				"            WHERE   access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"            AND     access_day <= '"+accessday+"' "+
				"            AND     ( (section_id in ('22','23') "+
				"            ANd     site_id not in (261,290,324,340,384,386,1182,436)) "+
				"            OR (psection_id in ('26')) "+
				"            OR (psection_id in ('8') "+
				"            AND     site_id not in (261,290,340,384,405,436,1154,1182,1183,3062,7703,14116)) "+
				"            OR (section_id in ('402') "+
				"            AND     site_id not in (436))) "+
				"            UNION ALL "+
				"            SELECT  access_day, case when category_code2 = 'EC' then '1' "+
				"                                     when category_code2 = 'DJ' then '2' "+
				"                                     when category_code2 = 'DB' then '3' "+
				"                                     when category_code2 in ('QA','QB') then '5' end code_num, panel_id, site_id, pv_cnt, duration "+
				"            FROM    tb_day_fact "+
				"            WHERE   access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"            AND     access_day <= '"+accessday+"' "+
				"            AND     category_code2 in ('DB','EC','DJ','QA','QB') "+
				"        ) "+
				"        group by access_day, code_num, panel_id, site_id "+
				"    )a, "+
				"    ( "+
				"        select weekcode, panel_id, kc_n_factor, kc_p_factor, sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls "+
				"        from tB_panel_seg "+
				"        where weekcode = fn_weekcode('"+accessday+"') "+
				"    )b "+
				"    where a.weekcode = b.weekcode "+
				"    and a.panel_id = b.panel_id "+
				") "+
				"group by weekcode, access_day, panel_id, site_id, code_num, sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls";
		
		this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();		
		
		String query3 = 
				"insert into TB_WEEK_TOTAL_SERVICE_SUM "+
				"select weekcode, code_num, "+
				"    sum(uu_cnt_adj) uu_cnt_adj, "+
				"    sum(pv_cnt_adj) pv_cnt_daj, "+
				"    round(sum(pv_cnt_adj)/sum(uu_cnt_adj),2) avg_pv, "+
				"    sum(tot_duration_adj) tot_duration_adj, "+
				"    round(sum(tot_duration_adj)/sum(uu_cnt_adj),2) avg_duration, "+
				"    round(sum(daily_freq_cnt*uu_cnt_adj)/sum(uu_cnt_adj),2) daily_freq_cnt, "+
				"    round(sum(uu_cnt_adj)/FN_PCANDROID_WEEK_NFACTOR(weekcode)*100,5) reach_rate_adj, "+
				"    sysdate "+
				"from "+
				"( "+
				"    select fn_weekcode(access_day) weekcode, panel_id, code_num, max(uu_cnt_adj) uu_cnt_adj, sum(pv_cnt_adj) pv_cnt_adj, sum(tot_duration_adj) tot_duration_adj, "+
				"        count(distinct access_day) daily_freq_cnt "+
				"    from TB_WEEK_TOTAL_SERVICE_FACT "+
				"    WHERE   access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"    AND     access_day <= '"+accessday+"' "+
				"    group by fn_weekcode(access_day), panel_id, code_num "+
				") "+
				"group by weekcode, code_num ";
		this.pstmt = connection.prepareStatement(query3);
		this.pstmt.executeUpdate();	
		
		String query4 =
				"insert into TB_WEEK_TOTAL_SERVICE_SITE "+
				"select weekcode, code_num, site_id, "+
				"    sum(uu_cnt_adj) uu_cnt_adj, "+
				"    sum(pv_cnt_adj) pv_cnt_daj, "+
				"    round(sum(pv_cnt_adj)/sum(uu_cnt_adj),2) avg_pv, "+
				"    sum(tot_duration_adj) tot_duration_adj, "+
				"    round(sum(tot_duration_adj)/sum(uu_cnt_adj),2) avg_duration, "+
				"    round(sum(daily_freq_cnt*uu_cnt_adj)/sum(uu_cnt_adj),2) daily_freq_cnt, "+
				"    round(sum(uu_cnt_adj)/FN_PCANDROID_WEEK_NFACTOR(weekcode)*100,5) reach_rate_adj, "+
				"    sysdate "+
				"from "+
				"( "+
				"    select fn_weekcode(access_day) weekcode, panel_id, code_num, site_id, max(uu_cnt_adj) uu_cnt_adj, sum(pv_cnt_adj) pv_cnt_adj, sum(tot_duration_adj) tot_duration_adj, "+
				"        count(distinct access_day) daily_freq_cnt "+
				"    from TB_WEEK_TOTAL_SERVICE_FACT "+
				"    WHERE   access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"    AND     access_day <= '"+accessday+"' "+
				"    group by fn_weekcode(access_day), panel_id, code_num, site_id "+
				") "+
				"group by weekcode, code_num, site_id ";
		
		this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();	
		
		
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekTotalSession
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekTotalSession insert
	 ******************************************************************************/
	
	public Calendar executeWeekTotalSession(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Total Session is processing...");
		String query = 
//				"INSERT   INTO TB_TOTAL_WEEK_SESSION (  "+
//				"        WEEKCODE, PANEL_ID, PV_CNT, DURATION , "+
//				"        DAILY_FREQ_CNT, KC_N_FACTOR, KC_P_FACTOR, AGE_CLS, SEX_CLS, "+
//				"        EDUCATION_CLS, INCOME_CLS, JOB_CLS, ISMARRIED_CLS, SITE_CNT, PROC_DATE  "+
//				") "+
//				"SELECT  A.WEEKCODE, A.PANEL_ID, PV_CNT, DURATION,                  "+
//				"        DAILY_FREQ_CNT, KC_N_FACTOR, KC_P_FACTOR, AGE_CLS, SEX_CLS,  "+
//				"        EDUCATION_CLS, INCOME_CLS, JOB_CLS, ISMARRIED_CLS, NVL(SITE_CNT,0), SYSDATE "+
//				"FROM     (  "+
//				"    SELECT  FN_WEEKCODE(ACCESS_DAY) WEEKCODE, PANEL_ID, SUM(PV_CNT) PV_CNT, SUM(DURATION) DURATION, "+
//				"            COUNT(DISTINCT ACCESS_DAY) DAILY_FREQ_CNT "+
//				"    FROM    TB_SMART_WEEK_TOTAL_FACT "+
//				"    WHERE   ACCESS_DAY >= TO_CHAR(TO_DATE('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
//				"    AND     ACCESS_DAY <= '"+accessday+"' "+
//				"    AND     TAB_CD IN ('PW','MW') "+
//				"    GROUP BY FN_WEEKCODE(ACCESS_DAY), PANEL_ID "+
//				") A, "+
//				"( "+
//				"    SELECT  WEEKCODE, PANEL_ID, KC_N_FACTOR, AGE_CLS, SEX_CLS, EDUCATION_CLS,  "+
//				"            INCOME_CLS, JOB_CLS, ISMARRIED_CLS, KC_P_FACTOR "+
//				"    FROM    tb_total_panel_seg "+
//				"    WHERE   WEEKCODE = FN_WEEKCODE('"+accessday+"') "+
//				") B, "+
//				"( "+
//				"    SELECT  PANEL_ID, COUNT(DISTINCT SITE_ID) SITE_CNT "+
//				"    FROM    TB_SMART_WEEK_TOTSITE_FACT "+
//				"    WHERE   WEEKCODE = FN_WEEKCODE('"+accessday+"') "+
//				"    GROUP BY PANEL_ID "+
//				") C "+
//				"WHERE A.PANEL_ID = B.PANEL_ID "+
//				"AND   A.PANEL_ID = C.PANEL_ID(+) "+
//				"AND   A.WEEKCODE = B.WEEKCODE ";
				
				"insert into tb_total_week_session "+ 
				"select /*+ordered*/fn_weekcode(access_day) weekcode, panel_id, "+ 
				"       age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls, "+ 
				"       sum(pv_cnt_adj) pv_cnt_adj, sum(tot_duration_adj) tot_duration_adj, sum(laun_duration_adj) laun_duration_adj, sum(media_duration_adj) media_duration_adj, max(kc_n_factor) kc_n_factor, "+ 
				"       count(distinct access_day) daily_freq_cnt, sysdate proc_date "+ 
				"from "+ 
				"( "+ 
				"    select /*+leading(a) use_hash(b,a)*/access_day, a.panel_id, "+ 
				"           pv_cnt, duration, pv_cnt*kc_p_factor pv_cnt_adj, duration*kc_p_factor tot_duration_adj, 0 laun_duration_adj, 0 media_duration_adj, "+ 
				"           kc_n_factor, kc_p_factor, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls "+ 
				"    from "+ 
				"    ( "+ 
				"        select /*+index(a,pk_day_session)*/ access_day, panel_id, pv_cnt, duration "+ 
				"        from   tb_day_session a "+ 
				"        WHERE  access_day between TO_CHAR(TO_DATE('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+
				"    ) a, "+ 
				"    ( "+ 
				"        select panel_id, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls, kc_n_factor, kc_p_factor "+ 
				"        from tb_panel_seg "+ 
				"        where weekcode = FN_WEEKCODE('"+accessday+"') "+ 
				"    ) b "+ 
				"    where a.panel_id=b.panel_id "+ 
				"    union all "+ 
				"    select /*+use_hash(b,a)*/access_day, a.panel_id, "+ 
				"           pv_cnt, duration, pv_cnt*mo_p_factor pv_cnt_adj, duration*mo_p_factor tot_duration_adj, laun_duration*mo_p_factor laun_duration_adj, "+ 
				"           media_duration*mo_p_factor media_duration_adj, mo_n_factor, mo_p_factor, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls "+ 
				"    from "+ 
				"    ( "+ 
				"        select access_day, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, sum(laun_duration) laun_duration, sum(media_duration) media_duration "+ 
				"        from "+ 
				"        ( "+ 
//				"            select /*+index(a,pk_smart_day_fact)*/ access_day, panel_id, pv_cnt, duration, 0 laun_duration, 0 media_duration "+ 
//				"            from   tb_smart_day_fact a "+ 
//				"            WHERE  access_day between TO_CHAR(TO_DATE('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+ 
//				"            union all "+ 
		        "           select /*+index(a,pk_smart_day_app_fact)*/access_day, panel_id, 0 pv_cnt, duration, "+
		        "                   0 laun_duration, 0 media_duration "+
		        "            from   tb_smart_day_app_fact a "+
		        "            WHERE  access_day between TO_CHAR(TO_DATE('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+
		        "            and    package_name != 'kclick_equal_app' "+
		        "            union all "+
		        "            select /*+index(a,pk_smart_day_app_fact)*/access_day, panel_id, 0 pv_cnt, 0 duration, "+
		        "                   duration laun_duration, 0 media_duration "+
		        "            from   tb_smart_day_app_fact a, tb_smart_app_laun_list b "+
		        "            WHERE  access_day between TO_CHAR(TO_DATE('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+ 
		        "            and   a.smart_id = b.smart_id "+
	            "            and    b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
	            "            and    b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+ 	
		        "            union all "+
		        "            select /*+index(a,pk_smart_day_app_fact)*/access_day, panel_id, 0 pv_cnt, 0 duration, "+
		        "                   0 laun_duration, duration media_duration "+
		        "            from   tb_smart_day_app_fact a, tb_smart_app_media_list b "+
		        "            WHERE  access_day between TO_CHAR(TO_DATE('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+ 
		        "            and   a.smart_id = b.smart_id "+
	            "            and    b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
	            "            and    b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+ 	            
				"        ) "+ 
				"        group by access_day, panel_id "+ 
				"    ) a, "+ 
				"    ( "+ 
				"        select panel_id, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls, mo_n_factor, mo_p_factor "+ 
				"        from tb_smart_panel_seg "+ 
				"        where weekcode = FN_WEEKCODE('"+accessday+"') "+ 
				"    ) b "+ 
				"    where a.panel_id=b.panel_id "+ 
				") "+ 
				"group by fn_weekcode(access_day), panel_id, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls ";
				
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	/**************************************************************************
	 *		메소드명		: executeWeekWebAppSession
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 web app Session insert
	 *************************************************************************/
	
	
	public Calendar executeWeekWebAppSession(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly WebApp Session is processing...");
		String query = 
				"insert into tb_smart_week_webapp_session "+
				"select weekcode, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, sum(laun_duration) laun_duration, sum(media_duration) media_duration,"+
				"       sum(web_daily_freq_cnt) web_daily_freq_cnt, sum(app_daily_freq_cnt) app_daily_freq_cnt, max(mo_n_factor) mo_n_factor, max(mo_p_factor) mo_p_factor, "+
				"       age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls "+
				"from "+
				"( "+
				"    select weekcode, panel_id, pv_cnt, duration, 0 laun_duration, 0 media_duration, daily_freq_cnt web_daily_freq_cnt, 0 app_daily_freq_cnt, mo_n_factor, mo_p_factor, "+
				"           age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls "+
				"    from tb_smart_week_session "+
				"    where weekcode = fn_weekcode('"+accessday+"') "+
				"    union all "+
				"    select weekcode, panel_id, 0 pv_cnt, duration, laun_duration, media_duration, 0, daily_freq_cnt, mo_n_factor, mo_p_factor, "+
				"           age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls "+
				"    from tb_smart_week_app_session "+
				"    where weekcode = fn_weekcode('"+accessday+"') "+
				") "+
				"group by weekcode, panel_id, age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls ";
				
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekTotal
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekTotal insert
	 *************************************************************************/
	
	public Calendar executeWeekTotal(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		executeQueryExecute("TRUNCATE TABLE TB_TEMP_DAY_WAPP_FACT");
		String queryTemp =  "insert into tb_temp_day_wapp_fact  "+
							"        (   access_day,    site_id,    app_id,    panel_id,    pv_cnt,    duration  ) "+
							"select  /*+use_hash(b,a)*/ a.access_day,a.site_id,a.app_id,a.panel_id, 0 pv_cnt,  "+
							"        case when ( a.duration - b.duration ) < 0 then 0 else ( a.duration - b.duration ) end duration  "+
							"from "+
							"( "+
							"    select  access_day, site_id, app_id, panel_id, duration  "+
							"    from    tb_day_app_fact  "+
							"    where   access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
							"    AND     access_day <= '"+accessday+"' "+
							") a, "+
							"( "+
							"    select  access_day, site_id, app_id, panel_id, duration  "+
							"    from    tb_day_wapp_fact  "+
							"    where   access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
							"    and     access_day <= '"+accessday+"' "+
							") b "+
							"where a.access_day = b.access_day "+
							"and a.site_id = b.site_id "+
							"and a.app_id = b.app_id "+
							"and a.panel_id = b.panel_id ";
		executeQueryExecute(queryTemp);
		
		executeQueryExecute("TRUNCATE TABLE tb_temp_week_day_siteapp_fact");
		String queryTemp2 =  
				"insert into tb_temp_week_day_siteapp_fact "+
				"select access_day, site_id, panel_id, tab_cd, sum(pv_cnt) pv_cnt, sum(duration) duration, sysdate "+
				"from "+
				"( "+
				"    select  access_day, site_id, panel_id, 'PW' tab_cd, pv_cnt, duration "+
				"    from    tb_day_fact  "+
				"    where   access_day between to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+
				"    AND     category_code1 <> 'Z' "+
				"    union all "+
				"    select  /*+use_hash(b,a)*/ access_day, b.site_id, panel_id, 'PA' tab_cd, 0 pv_cnt, duration "+
				"    from    tb_day_app_fact a, tb_app_info b "+
				"    where   access_day between to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+
				"    and     a.app_id = b.app_id "+
				"    and     b.ef_time < to_date('"+accessday+"','yyyy/mm/dd')+1 "+
				"    and     b.exp_time > to_date('"+accessday+"','yyyy/mm/dd')+1 "+
				"    and     (a.access_day,a.site_id,a.app_id,a.panel_id ) not in ( "+
				"        select access_day,site_id,app_id,panel_id  "+
				"        from TB_TEMP_DAY_WAPP_FACT  "+
				"        where access_day between to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+
				"        group by access_day,site_id,app_id,panel_id  "+
				"    ) "+
				"    AND    a.app_type_cd != 'Z' "+
				"    AND    a.app_id not in (2656,2657,1764,2653,3114,3192) "+
				"    union all "+
				"    select  /*+use_hash(b,a)*/access_day, a.site_id, panel_id, 'PWA' tab_cd, pv_cnt, 0 duration "+
				"    from    tb_day_vapp_fact  a, tb_app_info b "+
				"    where   access_day between to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+
				"    and    a.site_id = b.site_id "+
				"    AND    a.app_id  = b.app_id "+
				"    and    b.ef_time < to_date('"+accessday+"','yyyy/mm/dd')+1 "+
				"    and    b.exp_time > to_date('"+accessday+"','yyyy/mm/dd')+1 "+
				"    AND    b.app_type_cd < 'Z' "+
				"    AND    RESULT_CD='Y' "+
				"    AND    a.app_id not in (2656,2657,1764,2653,3114,3192) "+
				"    union all "+
				"    select  access_day, site_id, panel_id, 'PWA' tab_cd, pv_cnt, duration "+
				"    from    TB_TEMP_DAY_WAPP_FACT  "+
				"    where   access_day between to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+
				"    union all "+
				"    SELECT access_day, to_number(code_etc) site_id, panel_id, 'PWA' tab_cd, 0 pv_cnt, duration "+
				"    FROM   tb_day_mess_fact a, (select code, case when  fn_weekcode('"+accessday+"') <  201325 then code_etc else code_modify end code_etc from tb_codebook b where meta_code = 'MESSENGER') b "+
				"    WHERE  access_day between to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and '"+accessday+"' "+
				"    AND    a.messenger = code "+
				") "+
				"group by access_day, site_id, panel_id, tab_cd ";
		executeQueryExecute(queryTemp2);
		
		System.out.print("The batch - Weekly Total Sum is processing...");
		String queryFact = 
				"insert into TB_SMART_WEEK_TOTAL_FACT "+
				"select access_day, site_id, panel_id, tab_cd, sum(pv_cnt), sum(duration), sysdate "+
				"from ( "+
				"    select access_day, site_id, panel_id, tab_cd, pv_cnt, duration "+
				"    from   tb_temp_week_day_siteapp_fact "+
				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"'  "+
//				"    union all "+
//				"    select access_day, site_id, panel_id, 'MW' tab_cd, pv_cnt, duration "+
//				"    from   tb_smart_day_fact "+
//				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
//				"    and     category_code1 <> 'Z' " +
//				"	 and    panel_id in ( " +
//				"	 	select panel_id " +
//				"		from   tb_smart_panel_seg " +
//				"		where  weekcode = fn_weekcode('"+accessday+"') " +
//				"	 ) "+
				"    union all "+
				"    select /*+use_hash(b,a)*/ "+
				"           access_day, b.site_id, panel_id, 'MA' tab_cd, 0 pv_cnt, duration "+
				"    from   tb_smart_day_app_fact a, tb_smart_app_info b "+
				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				"    and    a.smart_id = b.smart_id "+
				"    and    b.site_id > 0 "+
				"    and    b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
				"    and    b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
				"    and    b.package_name != 'kclick_equal_app' "+
				"	 and    panel_id in ( " +
				"	 	select panel_id " +
				"		from   tb_smart_panel_seg " +
				"		where  weekcode = fn_weekcode('"+accessday+"') " +
				"	 ) "+
				") "+
				"group by access_day, site_id, panel_id, tab_cd ";
				
		this.pstmt = connection.prepareStatement(queryFact);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query = 
//				"insert into TB_SMART_WEEK_TOTSITEAPP_FACT "+
//				"select /*+parallel(a,8) index(a,idx_day_totsiteapp_fact)*/fn_weekcode(access_day) weekcode, site_id, panel_id, "+
//				"       sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate proc_date "+
//				"from   TB_SMART_WEEK_TOTAL_FACT a "+
//				"WHERE  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
//				"AND    access_day <= '"+accessday+"' "+
//				"group by fn_weekcode(access_day), site_id, panel_id ";
				
				"insert into TB_SMART_WEEK_TOTSITEAPP_FACT "+
				"select fn_weekcode(access_day) weekcode, site_id, panel_id,  "+
				"       sum(pv_cnt) pv_cnt, sum(duration) duration, "+
				"       count(distinct access_day) daily_freq_cnt, sysdate, "+
				"       max(kc_n_factor) kc_n_factor,  "+
				"       sum(adj_pv_cnt) adj_pv_cnt, sum(adj_duration) adj_duration "+
				"from ( "+
				"    select access_day, "+
				"           site_id, a.panel_id, pv_cnt, duration, kc_n_factor, kc_p_factor, "+
				"           pv_cnt*kc_p_factor adj_pv_cnt, duration*kc_p_factor adj_duration "+
				"    from   TB_SMART_WEEK_TOTAL_FACT a, tb_panel_seg b "+
				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				"    and    weekcode = fn_weekcode('"+accessday+"') "+
				"    and    a.panel_id = b.panel_id "+
				"    and    tab_cd like 'P%' "+
				" "+
				"    union all "+
				" "+
				"    select access_day, "+
				"           site_id, a.panel_id, pv_cnt, duration, mo_n_factor, mo_p_factor, "+
				"           pv_cnt*mo_p_factor adj_pv_cnt, duration*mo_p_factor adj_duration "+
				"    from   TB_SMART_WEEK_TOTAL_FACT a, tb_smart_panel_seg b "+
				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				"    and    weekcode = fn_weekcode('"+accessday+"') "+
				"    and    a.panel_id = b.panel_id "+
				"	 and 	tab_cd = 'MA' "+
				") "+
				"group by fn_weekcode(access_day), site_id, panel_id";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query1 = 
//				"insert into tb_smart_week_totsiteapp_sum "+
//				"select  /*+use_hash(b,a)*/ "+
//				"        a.weekcode weekcode, "+
//				"        site_id, "+
//				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
//				"        round(sum(pv_cnt * kc_p_factor),5) PV_CNT_ADJ, "+
//				"        round(decode(sum(kc_n_factor),0,0,round(sum(pv_cnt * kc_p_factor),5)/sum(kc_n_factor)),2) AVG_PV, "+
//				"        round(decode(sum(B.kc_n_factor),0,1, sum(A.duration * B.kc_p_factor)/sum(B.kc_n_factor)),2) AVG_DURATION, "+
//				"        round(sum(duration * kc_p_factor),5) tot_duration_adj, "+
//				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
//				"        round(sum(B.kc_n_factor)/fn_week_nfactor(a.weekcode)*100,5) reach_rate_adj, "+
//				"        rank() over (partition by a.weekcode order by sum(B.kc_n_factor) desc) uu_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by sum(A.pv_cnt*B.kc_p_factor) desc) pv_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by decode(sum(B.kc_p_factor),0,1, sum(A.duration*B.kc_p_factor)/sum(B.kc_n_factor)) desc) avg_duration_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by sum(A.duration*B.kc_p_factor) desc) tot_duration_overall_rank,     "+
//				"        rank() over (partition by a.weekcode order by decode(sum(B.kc_n_factor),0,1, sum(daily_freq_cnt*B.kc_n_factor)/sum(B.kc_n_factor)) desc) freq_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by decode(sum(kc_n_factor),0,0,sum(pv_cnt * kc_p_factor)/sum(kc_n_factor)) desc) avg_pv_overall_rank, "+
//				"        sysdate "+
//				"from  "+
//				"( "+
//				"    select /*+parallel(a,8) index(a,idx_day_totsiteapp_fact)*/weekcode, site_id, panel_id, pv_cnt, duration, daily_freq_cnt "+
//				"    from tb_smart_week_totsiteapp_fact a "+
//				"    where weekcode = fn_weekcode('"+accessday+"') "+
//				") a, "+
//				"( "+
//				"    select weekcode, panel_id, kc_n_factor, kc_p_factor "+
//				"    from   tb_panel_seg "+
//				"    where  weekcode = fn_weekcode('"+accessday+"') "+
//				") b "+
//				"where a.weekcode = b.weekcode "+
//				"and   a.panel_id  = b.panel_id "+
//				"group by a.weekcode, site_id ";
				
				"insert into tb_smart_week_totsiteapp_sum "+
				"select  /*+use_hash(b,a) index(a,SMART_WEEK_TOTSITEAPP_FACT) */ "+
				"        a.weekcode weekcode, "+
				"        site_id, "+
				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
				"        round(sum(adj_pv_cnt),5) PV_CNT_ADJ, "+
				"        round(decode(sum(kc_n_factor),0,0,round(sum(adj_pv_cnt),5)/sum(kc_n_factor)),2) AVG_PV, "+
				"        round(decode(sum(kc_n_factor),0,1, sum(adj_duration)/sum(kc_n_factor)),2) AVG_DURATION, "+
				"        round(sum(adj_duration),5) tot_duration_adj, "+
				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
				"        round(sum(kc_n_factor)/FN_PCANDROID_WEEK_NFACTOR(a.weekcode)*100,5) reach_rate_adj, "+
				"        rank() over (partition by a.weekcode order by sum(kc_n_factor) desc) uu_overall_rank, "+
				"        rank() over (partition by a.weekcode order by sum(adj_pv_cnt) desc) pv_overall_rank, "+
				"        rank() over (partition by a.weekcode order by decode(sum(adj_duration),0,1, sum(adj_duration)/sum(kc_n_factor)) desc) avg_duration_overall_rank, "+
				"        rank() over (partition by a.weekcode order by sum(adj_duration) desc) tot_duration_overall_rank,     "+
				"        rank() over (partition by a.weekcode order by decode(sum(kc_n_factor),0,1,sum(daily_freq_cnt*kc_n_factor)/sum(kc_n_factor)) desc) freq_overall_rank, "+
				"        rank() over (partition by a.weekcode order by decode(sum(kc_n_factor),0,0,sum(adj_pv_cnt)/sum(kc_n_factor)) desc) avg_pv_overall_rank, "+
				"        sysdate "+
				"from  tb_smart_week_totsiteapp_fact a "+
				"where weekcode = fn_weekcode('"+accessday+"') "+
				"group by weekcode, site_id ";
		
		this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
//		String query2 = 
//				"insert into tb_smart_week_siteapp_fact "+
//				"select /*+index(a,PK_WEEK_TOTAL_FACT)*/ fn_weekcode(access_day) weekcode, site_id, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duartion, count(distinct access_day) DAILY_FREQ_CNT, sysdate "+
//				"from   TB_SMART_WEEK_TOTAL_FACT a "+
//				"WHERE  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
//				"AND    access_day <= '"+accessday+"' "+
//				"and   tab_cd in ('MW','MA') "+
//				"group by fn_weekcode(access_day), site_id, panel_id ";
//				
//		this.pstmt = connection.prepareStatement(query2);
//		this.pstmt.executeUpdate();
//		if(this.pstmt!=null) this.pstmt.close();
//		
//		String query3 = 
//				"insert into tb_smart_week_siteapp_sum "+
//				"select  /*+use_hash(b,a)*/ "+
//				"        a.weekcode weekcode, "+
//				"        site_id, "+
//				"        round(sum(mo_n_factor),5) UU_CNT_ADJ, "+
//				"        round(sum(pv_cnt * mo_p_factor),5) PV_CNT_ADJ, "+
//				"        round(decode(sum(B.mo_n_factor),0,1, sum(A.duration * B.mo_p_factor)/sum(B.mo_n_factor)),2) AVG_DURATION, "+
//				"        round(sum(duration * mo_p_factor),5) tot_duration_adj, "+
//				"        round(sum(daily_freq_cnt * mo_n_factor)/sum(mo_n_factor),2) daily_freq_cnt, "+
//				"        round(sum(B.mo_n_factor)/fn_smart_week_nfactor(a.weekcode)*100,5) reach_rate_adj, "+
//				"        rank() over (partition by a.weekcode order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by decode(sum(B.mo_p_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
//				"        rank() over (partition by a.weekcode order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
//				"        sysdate proc_date "+
//				"from  "+
//				"( "+
//				"    select /*+index(a,PK_SMART_WEEK_SITEAPP_FACT)*/ weekcode, site_id, panel_id, pv_cnt, duration, daily_freq_cnt "+
//				"    from tb_smart_week_siteapp_fact a "+
//				"    where weekcode = fn_weekcode('"+accessday+"') "+
//				") a, "+
//				"( "+
//				"    select weekcode, panel_id, mo_n_factor, mo_p_factor "+
//				"    from   tb_smart_panel_seg "+
//				"    where weekcode = fn_weekcode('"+accessday+"') "+
//				") b "+
//				"where a.weekcode = b.weekcode "+
//				"and   a.panel_id  = b.panel_id "+
//				"group by a.weekcode, site_id ";
//				
//		this.pstmt = connection.prepareStatement(query3);
//		this.pstmt.executeUpdate();
//		if(this.pstmt!=null) this.pstmt.close();
		
		String query4 = 
//				"insert into tb_smart_week_pcmobile_sum "+
//				"select fn_weekcode(access_day) weekcode, panel_id, site_id, tab_cd, "+
//				"       sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate proc_date "+
//				"from   TB_SMART_WEEK_TOTAL_FACT "+
//				"WHERE  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
//				"AND    access_day <= '"+accessday+"' "+
//				"group by fn_weekcode(access_day), panel_id, site_id, tab_cd ";
				
				"insert into tb_smart_week_pcmobile_sum "+
				"select fn_weekcode(access_day) weekcode, panel_id, site_id, tab_cd, "+
				"       sum(pv_cnt) pv_cnt, sum(duration) duration, "+
				"       count(distinct access_day) daily_freq_cnt, sysdate, "+
				"       max(kc_n_factor) kc_n_factor,  "+
				"       sum(adj_pv_cnt) adj_pv_cnt, sum(adj_duration) adj_duration "+
				"from ( "+
				"    select access_day, tab_cd, "+
				"           site_id, a.panel_id, pv_cnt, duration, kc_n_factor, kc_p_factor, "+
				"           pv_cnt*kc_p_factor adj_pv_cnt, duration*kc_p_factor adj_duration "+
				"    from   TB_SMART_WEEK_TOTAL_FACT a, tb_panel_seg b "+
				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				"    and    weekcode = fn_weekcode('"+accessday+"') "+
				"    and    a.panel_id = b.panel_id "+
				"    and    tab_cd like 'P%' "+
				" "+
				"    union all "+
				" "+
				"    select access_day, tab_cd, "+
				"           site_id, a.panel_id, pv_cnt, duration, mo_n_factor, mo_p_factor, "+
				"           pv_cnt*mo_p_factor adj_pv_cnt, duration*mo_p_factor adj_duration "+
				"    from   TB_SMART_WEEK_TOTAL_FACT a, tb_smart_panel_seg b "+
				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				"    and    weekcode = fn_weekcode('"+accessday+"') "+
				"    and    a.panel_id = b.panel_id "+
				"	 and 	tab_cd = 'MA' "+
				") "+
				"group by fn_weekcode(access_day), site_id, panel_id, tab_cd ";
		
		this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();

//		String query5 = 
////				"insert into tb_smart_week_totsite_fact "+
////				"select /*+index(a,idx_day_totsiteapp_fact)*/fn_weekcode(access_day) weekcode, site_id, panel_id, "+
////				"       sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate "+
////				"from   TB_SMART_WEEK_TOTAL_FACT "+
////				"WHERE  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
////				"AND    access_day <= '"+accessday+"' "+
////				"and   tab_cd in ('PW','MW') "+
////				"group by fn_weekcode(access_day), site_id, panel_id ";
//				"insert into tb_smart_week_totsite_fact "+
//				"select fn_weekcode(access_day) weekcode, site_id, panel_id, "+
//				"       sum(pv_cnt) pv_cnt, sum(duration) duration, "+
//				"       count(distinct access_day) daily_freq_cnt, sysdate, "+
//				"       max(kc_n_factor) kc_n_factor,  "+
//				"       sum(adj_pv_cnt) adj_pv_cnt, sum(adj_duration) adj_duration "+
//				"from ( "+
//				"    select access_day, "+
//				"           site_id, a.panel_id, pv_cnt, duration, kc_n_factor, kc_p_factor, "+
//				"           pv_cnt*kc_p_factor adj_pv_cnt, duration*kc_p_factor adj_duration "+
//				"    from   TB_SMART_WEEK_TOTAL_FACT a, tb_panel_seg b "+
//				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
//				"    and    weekcode = fn_weekcode('"+accessday+"') "+
//				"    and    a.panel_id = b.panel_id "+
//				"    and    tab_cd = 'PW' "+
//				" "+
//				"    union all "+
//				" "+
//				"    select access_day, "+
//				"           site_id, a.panel_id, pv_cnt, duration, mo_n_factor, mo_p_factor, "+
//				"           pv_cnt*mo_p_factor adj_pv_cnt, duration*mo_p_factor adj_duration "+
//				"    from   TB_SMART_WEEK_TOTAL_FACT a, tb_smart_panel_seg b "+
//				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
//				"    and    weekcode = fn_weekcode('"+accessday+"') "+
//				"    and    a.panel_id = b.panel_id "+
//				"    and    tab_cd = 'MW' "+
//				") "+
//				"group by fn_weekcode(access_day), site_id, panel_id ";
//		
//		this.pstmt = connection.prepareStatement(query5);
//		this.pstmt.executeUpdate();
//		if(this.pstmt!=null) this.pstmt.close();
		
//		String query6 = 
////				"insert into tb_smart_week_totsite_sum "+
////				"select  /*+use_hash(b,a)*/ "+
////				"        a.weekcode weekcode, "+
////				"        site_id, "+
////				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
////				"        round(sum(pv_cnt * kc_p_factor),5) PV_CNT_ADJ, "+
////				"        round(decode(sum(kc_n_factor),0,0,round(sum(pv_cnt * kc_p_factor),5)/sum(kc_n_factor)),2) AVG_PV, "+
////				"        round(decode(sum(B.kc_n_factor),0,1, sum(A.duration * B.kc_p_factor)/sum(B.kc_n_factor)),2) AVG_DURATION, "+
////				"        round(sum(duration * kc_p_factor),5) tot_duration_adj, "+
////				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
////				"        round(sum(B.kc_n_factor)/fn_week_nfactor(a.weekcode)*100,5) reach_rate_adj, "+
////				"        rank() over (partition by a.weekcode order by sum(B.kc_n_factor) desc) uu_overall_rank, "+
////				"        rank() over (partition by a.weekcode order by sum(A.pv_cnt*B.kc_p_factor) desc) pv_overall_rank, "+
////				"        rank() over (partition by a.weekcode order by decode(sum(B.kc_p_factor),0,1, sum(A.duration*B.kc_p_factor)/sum(B.kc_n_factor)) desc) avg_duration_overall_rank, "+
////				"        rank() over (partition by a.weekcode order by sum(A.duration*B.kc_p_factor) desc) tot_duration_overall_rank,     "+
////				"        rank() over (partition by a.weekcode order by decode(sum(B.kc_n_factor),0,1, sum(daily_freq_cnt*B.kc_n_factor)/sum(B.kc_n_factor)) desc) freq_overall_rank, "+
////				"        rank() over (partition by a.weekcode order by decode(sum(kc_n_factor),0,0,sum(pv_cnt * kc_p_factor)/sum(kc_n_factor)) desc) avg_pv_overall_rank, "+
////				"        sysdate proc_date "+
////				"from  "+
////				"( "+
////				"    select /*+parallel(a,8)*/ weekcode,site_id,panel_id,pv_cnt,duration,daily_freq_cnt "+
////				"    from tb_smart_week_totsite_fact a "+
////				"    where weekcode = fn_weekcode('"+accessday+"') "+
////				") a, "+
////				"( "+
////				"    select weekcode, panel_id, kc_n_factor, kc_p_factor "+
////				"    from   tb_panel_seg "+
////				"    where weekcode = fn_weekcode('"+accessday+"') "+
////				") b "+
////				"where a.weekcode = b.weekcode "+
////				"and   a.panel_id  = b.panel_id "+
////				"group by a.weekcode, site_id ";
//				"insert into tb_smart_week_totsite_sum "+
//				"select  /*+use_hash(b,a) index(a,PK_WEEK_TOTSITE_FACT) */ "+
//				"        a.weekcode weekcode, "+
//				"        site_id, "+
//				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
//				"        round(sum(adj_pv_cnt),5) PV_CNT_ADJ, "+
//				"        round(decode(sum(kc_n_factor),0,0,round(sum(adj_pv_cnt),5)/sum(kc_n_factor)),2) AVG_PV, "+
//				"        round(decode(sum(kc_n_factor),0,1, sum(adj_duration)/sum(kc_n_factor)),2) AVG_DURATION, "+
//				"        round(sum(adj_duration),5) tot_duration_adj, "+
//				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
//				"        round(sum(kc_n_factor)/FN_PCANDROID_WEEK_NFACTOR(a.weekcode)*100,5) reach_rate_adj, "+
//				"        rank() over (partition by a.weekcode order by sum(kc_n_factor) desc) uu_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by sum(adj_pv_cnt) desc) pv_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by decode(sum(adj_duration),0,1, sum(adj_duration)/sum(kc_n_factor)) desc) avg_duration_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by sum(adj_duration) desc) tot_duration_overall_rank,     "+
//				"        rank() over (partition by a.weekcode order by decode(sum(kc_n_factor),0,1, sum(daily_freq_cnt*kc_n_factor)/sum(kc_n_factor)) desc) freq_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by decode(sum(kc_n_factor),0,0,sum(adj_pv_cnt)/sum(kc_n_factor)) desc) avg_pv_overall_rank, "+
//				"        sysdate proc_date "+
//				"from    tb_smart_week_totsite_fact a "+
//				"where   weekcode = fn_weekcode('"+accessday+"') "+
//				"group by weekcode, site_id ";
//		
//		this.pstmt = connection.prepareStatement(query6);
//		this.pstmt.executeUpdate();
//		if(this.pstmt!=null) this.pstmt.close();
		
		String query7 = 
//				"insert into tb_smart_week_totapp_fact "+
//				"select /*+index(a,idx_day_totsiteapp_fact)*/fn_weekcode(access_day) weekcode, site_id, panel_id, "+
//				"       sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate "+
//				"from   TB_SMART_WEEK_TOTAL_FACT "+
//				"WHERE  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
//				"AND    access_day <= '"+accessday+"' "+
//				"and   tab_cd in ('PA','MA') "+
//				"group by fn_weekcode(access_day), site_id, panel_id ";
				"insert into tb_smart_week_totapp_fact "+
				"select fn_weekcode(access_day) weekcode, site_id, panel_id, "+
				"       sum(pv_cnt) pv_cnt, sum(duration) duration, "+
				"       count(distinct access_day) daily_freq_cnt, sysdate, "+
				"       max(kc_n_factor) kc_n_factor,  "+
				"       sum(adj_pv_cnt) adj_pv_cnt, sum(adj_duration) adj_duration "+
				"from ( "+
				"    select access_day, "+
				"           site_id, a.panel_id, pv_cnt, duration, kc_n_factor, kc_p_factor, "+
				"           pv_cnt*kc_p_factor adj_pv_cnt, duration*kc_p_factor adj_duration "+
				"    from   TB_SMART_WEEK_TOTAL_FACT a, tb_panel_seg b "+
				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				"    and    weekcode = fn_weekcode('"+accessday+"') "+
				"    and    a.panel_id = b.panel_id "+
				"    and    tab_cd = 'PA' "+
				" "+
				"    union all "+
				" "+
				"    select access_day, "+
				"           site_id, a.panel_id, pv_cnt, duration, mo_n_factor, mo_p_factor, "+
				"           pv_cnt*mo_p_factor adj_pv_cnt, duration*mo_p_factor adj_duration "+
				"    from   TB_SMART_WEEK_TOTAL_FACT a, tb_smart_panel_seg b "+
				"    where  access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				"    and    weekcode = fn_weekcode('"+accessday+"') "+
				"    and    a.panel_id = b.panel_id "+
				"    and    tab_cd = 'MA' "+
				") "+
				"group by fn_weekcode(access_day), site_id, panel_id ";
		
		this.pstmt = connection.prepareStatement(query7);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query8 = 
//				"insert into tb_smart_week_totapp_sum "+
//				"select  /*+use_hash(b,a)*/ "+
//				"        a.weekcode weekcode, "+
//				"        site_id, "+
//				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
//				"        round(decode(sum(B.kc_n_factor),0,1, sum(A.duration * B.kc_p_factor)/sum(B.kc_n_factor)),2) AVG_DURATION, "+
//				"        round(sum(duration * kc_p_factor),5) tot_duration_adj, "+
//				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
//				"        round(sum(B.kc_n_factor)/fn_week_nfactor(a.weekcode)*100,5) reach_rate_adj, "+
//				"        rank() over (partition by a.weekcode order by sum(B.kc_n_factor) desc) uu_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by decode(sum(B.kc_p_factor),0,1, sum(A.duration*B.kc_p_factor)/sum(B.kc_n_factor)) desc) avg_duration_overall_rank, "+
//				"        rank() over (partition by a.weekcode order by sum(A.duration*B.kc_p_factor) desc) tot_duration_overall_rank,     "+
//				"        rank() over (partition by a.weekcode order by decode(sum(B.kc_n_factor),0,1, sum(daily_freq_cnt*B.kc_n_factor)/sum(B.kc_n_factor)) desc) freq_overall_rank, "+
//				"        sysdate proc_date "+
//				"from  "+
//				"( "+
//				"    select /*+parallel(a,8)*/ weekcode,site_id,panel_id,pv_cnt,duration,daily_freq_cnt "+
//				"    from tb_smart_week_totapp_fact a "+
//				"    where weekcode = fn_weekcode('"+accessday+"') "+
//				") a, "+
//				"( "+
//				"    select weekcode, panel_id, kc_n_factor, kc_p_factor "+
//				"    from   tb_panel_seg "+
//				"    where weekcode = fn_weekcode('"+accessday+"') "+
//				") b "+
//				"where a.weekcode = b.weekcode "+
//				"and   a.panel_id  = b.panel_id "+
//				"group by a.weekcode, site_id ";
				"insert into tb_smart_week_totapp_sum "+
				"select  /*+use_hash(b,a) index(a,PK_WEEK_TOTAPP_FACT) */ "+
				"        a.weekcode weekcode, "+
				"        site_id, "+
				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
				"        round(decode(sum(kc_n_factor),0,1, sum(adj_duration)/sum(kc_n_factor)),2) AVG_DURATION, "+
				"        round(sum(adj_duration),5) tot_duration_adj, "+
				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
				"        round(sum(kc_n_factor)/FN_PCANDROID_WEEK_NFACTOR(a.weekcode)*100,5) reach_rate_adj, "+
				"        rank() over (partition by a.weekcode order by sum(kc_n_factor) desc) uu_overall_rank, "+
				"        rank() over (partition by a.weekcode order by decode(sum(adj_duration),0,1, sum(adj_duration)/sum(kc_n_factor)) desc) avg_duration_overall_rank, "+
				"        rank() over (partition by a.weekcode order by sum(adj_duration) desc) tot_duration_overall_rank,     "+
				"        rank() over (partition by a.weekcode order by decode(sum(kc_n_factor),0,1, sum(daily_freq_cnt*kc_n_factor)/sum(kc_n_factor)) desc) freq_overall_rank, "+
				"        sysdate proc_date "+
				"from    tb_smart_week_totapp_fact a "+
				"where weekcode = fn_weekcode('"+accessday+"') "+
				"group by a.weekcode, site_id ";
		
		this.pstmt = connection.prepareStatement(query8);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekNotice
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekNotice insert
	 *************************************************************************/
	
	public Calendar executeWeekNotice(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Weekly Notice is processing...");
		
		int maxid = 0;
		String weekcode = "";
		String sql = 
				 "select max(id) ID, fn_weekcode('"+accessday+"') weekcode " +
			     "from   tb_notice@kcred2 ";   
		try {
			ResultSet rs = null;
			this.pstmt = connection.prepareStatement(sql);
			rs = this.pstmt.executeQuery(sql);	
			rs.next();
			maxid = rs.getInt("ID")+1;
			weekcode = rs.getString("WEEKCODE");
		}  catch (SQLException e) {
			e.printStackTrace();
		}
		
		String query = 
				"insert into tb_notice@kcred2 "+
				"select "+maxid+" id, "+
				"       to_char(sysdate,'yyyy-mm-dd') from_date, "+
				"       '31' title, "+
				"       '모바일 주간 : '||dates title2, "+
				"       '<p><img alt=\\\"Image\\\" src=\\\"http://www.koreanclick.com/images/popup/blt.gif\\\">&nbsp;<strong>주간 데이터 처리<br></strong></p> "+
				"        <p>지난주 ('||dates||') 모바일 자료가 업데이트 되었습니다. 지금 클라이언트 센터에 로그인 하시면&nbsp;업데이트 된 데이터를 보실 수 있습니다.</p>' contents, "+
				"        'N' headline "+
				"from   ( "+
				"    select YEAR||'년 '||MONTH||'월 '||substr(START_DATE,7,2)||'일 ~ '|| "+
				"           YEAR||'년 '||MONTH||'월 '||substr(END_DATE,7,2)||'일' dates "+
				"    from   tb_week_cat "+
				"    where  weekcode = '"+weekcode+"' "+
				") ";
		
		//System.out.println(query);
		//System.exit(0);
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthlyBoardCheck
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthlyBoardCheck insert
	 *************************************************************************/
	
	public Calendar executeMonthlyBoardCheck( ) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Monthly Board check insert is processing...");
		String query = 
				"INSERT INTO TB_SMART_MONTHLY_REPORT_CHECK "+
					"SELECT ( "+
					"    SELECT MAX(MONTHCODE) "+
					"    FROM   TB_MONTH_CAT "+
					"), TEAM, NAME, AREA, 'N' FLAG, "+
					"( "+
					"    select max(ID) "+
					"    from   tb_admin_member "+
					"    where  NAME = a.Name "+
					") LOGIN_ID "+
					"FROM TB_SMART_REPORT_CHECK a";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeeklyBoardCheck
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthlyBoardCheck insert
	 *************************************************************************/
	
	public Calendar executeWeeklyBoardCheck( ) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Board check insert is processing...");
		String query = 
				"INSERT INTO TB_SMART_WEEKLY_REPORT_CHECK "+
					"SELECT ( "+
					"    SELECT MAX(WEEKCODE) "+
					"    FROM   TB_WEEK_CAT "+
					"), TEAM, NAME, AREA, 'N' FLAG, "+
					"( "+
					"    select max(ID) "+
					"    from   tb_admin_member "+
					"    where  NAME = a.Name "+
					") LOGIN_ID "+
					"FROM TB_SMART_REPORT_CHECK a";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	

	
	
	/**************************************************************************
	 *		메소드명		: executeMonthNotice
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekNotice insert
	 *************************************************************************/
	
	public Calendar executeMonthNotice(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Notice is processing...");
		
		int maxid = 0;
		String sql = 
				 "select max(id) ID " +
			     "from   tb_notice@kcred2 ";   
		try {
			ResultSet rs = null;
			this.pstmt = connection.prepareStatement(sql);
			rs = this.pstmt.executeQuery(sql);	
			rs.next();
			maxid = rs.getInt("ID")+1;
		}  catch (SQLException e) {
			e.printStackTrace();
		}
		
		String query = 
				"insert into tb_notice@kcred2 "+
				"select "+maxid+" id, "+
				"       to_char(sysdate,'yyyy-mm-dd') from_date, "+
				"       '31' title, "+
				"       '모바일 월간 : '||dates title2, "+
				"       '<p><img alt=\\\"Image\\\" src=\\\"http://www.koreanclick.com/images/popup/blt.gif\\\">&nbsp;<strong>월간 데이터 처리<br></strong></p> "+
				"        <p>지난월 ('||dates||') 모바일 자료가 업데이트 되었습니다. 지금 클라이언트 센터에 로그인 하시면&nbsp;업데이트 된 데이터를 보실 수 있습니다.</p>' contents, "+
				"        'N' headline "+
				"from   ( "+
				"    select substr('"+monthcode+"',1,4)||'년 '||substr('"+monthcode+"',5,2)||'월' dates"+
				"    from   dual "+
				") ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSetupFact insert
	 *************************************************************************/
	
	public Calendar executeWeekAppFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly App Fact is processing...");
		String query = 
				"INSERT   INTO tb_smart_week_app_fact "+
				"         (weekcode, smart_id, package_name, panel_id, duration, daily_freq_cnt, proc_date, app_cnt) "+
				"SELECT   /*+index(a,pk_smart_day_app_fact)*/" +
				"         fn_weekcode(access_day) weekcode, smart_id, package_name, panel_id, sum(duration) duration, "+
				"         count(distinct access_day), sysdate, sum(app_cnt) app_cnt "+
				"FROM     tb_smart_day_app_fact a "+
				"WHERE    access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"AND      access_day <= '"+accessday+"' "+
				"GROUP BY fn_weekcode(access_day), smart_id, package_name, panel_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSetupFact insert
	 *************************************************************************/
	
	public Calendar executeWeekSiteSeg(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Site Seg is processing...");
		String query = 
				"insert  into TB_SMART_WEEK_SEG_SITE "+
				"( weekcode, segment_id, site_id, category_code1, category_code2, category_code3, "+
				"age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls, region_cd,  "+
				"uu_cnt, uu_est_cnt, pv_cnt, pv_est_cnt, avg_duration, duration_est, avg_daily_freq_cnt, avg_pv_est_cnt, reach_rate, visit_cnt, visit_est_cnt, proc_date ) "+
				"select  /*+use_hash(v_panel_seg, v_fact)*/ v_fact.weekcode,  "+
				"        v_fact.segment_id,  "+
				"        v_fact.site_id,  "+
				"        min(category_code1), "+
				"        min(category_code2),  "+
				"        min(category_code3), "+
				"        min(v_panel_seg.age_cls)        age_cls,  "+
				"        min(v_panel_seg.sex_cls)        sex_cls,  "+
				"        min(v_panel_seg.income_cls)     income_cls,  "+
				"        min(v_panel_seg.job_cls)        job_cls,  "+
				"        min(v_panel_seg.education_cls)  education_cls,  "+
				"        min(v_panel_seg.ismarried_cls ) ismarried_cls,  "+
				"        min(v_panel_seg.region_cd )     region_cd,  "+
				"        count(distinct v_fact.panel_id) uu_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor),5) uu_est_cnt, "+
				"        sum(pv_cnt) pv_cnt,  "+
				"        round(sum(pv_cnt*v_panel_seg.mo_p_factor),5) pv_est_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"        round(sum(duration*v_panel_seg.mo_p_factor),5) duration_est, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(pv_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor)/ fn_smart_week_nfactor(v_fact.weekcode) *100, 5) reach_rate, "+
				"        sum(visit_cnt) visit_cnt, "+
				"        round(sum(visit_cnt*v_panel_seg.mo_p_factor),5) visit_est_cnt, "+
				"        sysdate "+
				"from  "+
				"( "+
				"        select  /*+index(a,pk_smart_week_fact)*/ WEEKCODE, segment_id, site_id, panel_id, pv_cnt, visit_cnt, daily_freq_cnt, duration, "+
				"                CATEGORY_CODE1, CATEGORY_CODE2, CATEGORY_CODE3 "+
				"        from    tb_smart_week_fact a "+
				"        where   WEEKCODE = fn_weekcode('"+accessday+"') "+
				") v_fact, "+
				"( "+
				"        select  weekcode, panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"        from    tb_smart_panel_seg "+
				"        where   WEEKCODE = fn_weekcode('"+accessday+"') "+
				") v_panel_seg "+
				"where v_fact.panel_id = v_panel_seg.panel_id "+
				"and   v_fact.weekcode = v_panel_seg.weekcode   "+
				"group by v_fact.weekcode, v_fact.segment_id, v_fact.site_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekSetupFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSetupFact insert
	 *************************************************************************/
	
	public Calendar executeWeekSetupFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Setup Fact is processing...");
		String query = 
				"insert into tb_smart_week_setup_fact "+
				"select   /*+index(a,pk_smart_day_setup_fact)*/ fn_weekcode(ACCESS_DAY), SMART_ID, PACKAGE_NAME, PANEL_ID, sysdate "+
				"from     tb_smart_day_setup_fact a "+
				"WHERE    access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"AND      access_day <= '"+accessday+"' "+
				"group by fn_weekcode(ACCESS_DAY), SMART_ID, PACKAGE_NAME, PANEL_ID ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekCsectionFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekCsectionFact insert
	 *************************************************************************/
	
	public Calendar executeWeekCsectionFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Csection Fact is processing...");
		String query = 
				"insert into tb_smart_week_csection_fact "+
				"(weekcode, site_id, panel_id, section_id, visit_cnt, pv_cnt, duration, daily_freq_cnt, proc_date) "+
				"SELECT   fn_weekcode(access_day) weekcode, site_id, panel_id, section_id, sum(visit_cnt) visit_cnt, "+
				"         sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate proc_date "+
				"FROM "+
				"(        "+
				"    SELECT  panel_id,  section_id, site_id, "+
				"            case when round((server_date - lag(server_date, 1) over (partition by site_id, visit_section_id, "+
				"            panel_id order by server_date))*60* 60*24) > 60*30 "+
				"            or lag(server_date, 1) over (partition by site_id, visit_section_id, panel_id "+
				"            order by server_date) is NULL then 1  end visit_cnt, "+
				"            pv_cnt, "+
				"            duration, "+
				"            access_day "+
				"    FROM "+
				"    (       "+
				"        SELECT /*+ parallel(a,8) */ "+
				"               panel_id, section_id, site_id, server_date, "+
				"               visit_section_id, pv_cnt, duration, access_day "+
				"        FROM   tb_smart_week_temp_section "+
				"        WHERE  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"        AND    access_day <= '"+accessday+"' "+
				"        and    section_id in (19,20,21,22,23,24,25,41,42,43,44,51,52,53,54,55,61,62,301,302,303,304,305,306,307,308,309,310,311,312,313,401,402,403,404) "+
				"    ) "+
				") "+
				"where   panel_id in ( " +
				"	select panel_id " +
				"	from   tb_smart_panel_seg " +
				"	where  weekcode = fn_weekcode('"+accessday+"') " +
				") "+
				"GROUP  BY fn_weekcode(access_day), site_id, panel_id, section_id "+
				"having nvl(sum(pv_cnt),0) > 0 ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekKeywordFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekKeywordFact insert
	 *************************************************************************/
	
	public Calendar executeWeekKeywordFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();

		System.out.print("The batch - Weekly Keyword Fact is processing...");
		String query = 
                "INSERT INTO TB_SMART_WEEK_KEYWORD_FACT " + 
                "SELECT weekcode, site_id, section_id, lower(query_decode), panel_id, count(*), sum(query_cnt) " + 
                "FROM " + 
                "( " + 
                "   select fn_weekcode('"+accessday+"') weekcode, site_id, section_id, query_decode, panel_id, rid, case when sum(query_cnt) > 0 then 1 else 0 end query_cnt " +
                "       from "+
                "   ( "+
                        "    select rid,  site_id, section_id, query_decode, panel_id, " + 
                        "           case when query_decode1||query_decode2 in ('MM','MT','TM','TT','TF','FT') then 1 else 0 end query_cnt, path_rank " + 
                        "    from " + 
                        "    ( " + 
                        "        select rid , a.site_id, b.section_id, query_decode, panel_id, domain_url, page, parameter, " + 
                        "               case when add_page1 is null then 'M' " + 
                        "                    when instr(parameter, add_page1) = 1 then 'T' " + 
                        "               else case when instr(parameter, add_page1) <> 0 and instr(parameter, '&'||add_page1) > 1 then 'T' " + 
                        "                    else case when (add_page2 is not null and instr(parameter,add_page2) <> 0 and " + 
                        "                                       (instr(parameter,add_page2) > 0 or instr(parameter,'&'||add_page2) > 0)) or " + 
                        "                                   (add_page3 is not null and (instr(parameter,add_page3) > 0 or instr(parameter,'&'||add_page3) > 0)) or " + 
                        "                                   (add_page4 is not null and (instr(parameter,add_page4) > 0 or instr(parameter,'&'||add_page4) > 0)) or " + 
                        "                                   (add_page5 is not null and (instr(parameter,add_page5) > 0 or instr(parameter,'&'||add_page5) > 0)) then 'T' " + 
                        "                         else 'F' " + 
                        "                         end " + 
                        "                    end " + 
                        "               end query_decode1, " + 
                        "               case when instr(parameter, nvl(del_page1,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page1,'pgheo')) > 0 then 'F' " + 
                        "                    when instr(parameter, nvl(del_page1,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page1,'pgheo')) > 0 then 'F' " + 
                        "                    when (instr(parameter, nvl(del_page1,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page1,'pgheo')) = 0) or " + 
                        "                         (instr(parameter ,nvl(del_page1,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page1,'pgheo')) = 0) " + 
                        "                    then case when instr(parameter, nvl(del_page2,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page2,'pgheo')) > 0 then 'F' " + 
                        "                              when instr(parameter, nvl(del_page2,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page2,'pgheo')) > 0 then 'F' " + 
                        "                              when (instr(parameter, nvl(del_page2,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page2,'pgheo')) = 0) or " + 
                        "                                   (instr(parameter, nvl(del_page2,'pgheo')) > 0 and instr(parameter, '&'||nvl(del_page2,'pgheo')) = 0) " + 
                        "                              then case when instr(parameter, nvl(del_page3,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page3,'pgheo')) > 0 then 'F' " + 
                        "                                        when instr(parameter, nvl(del_page3,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page3,'pgheo')) > 0 then 'F' " + 
                        "                                        when (instr(parameter, nvl(del_page3,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page3,'pgheo')) = 0) or " + 
                        "                                             (instr(parameter, nvl(del_page3,'pgheo')) > 0 and instr(parameter, '&'||nvl(del_page3,'pgheo')) = 0) " + 
                        "                                        then case when instr(parameter, nvl(del_page4,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page4,'pgheo')) > 0 then 'F' " + 
                        "                                                  when instr(parameter, nvl(del_page4,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page4,'pgheo')) > 0 then 'F' " + 
                        "                                                  when (instr(parameter, nvl(del_page4,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page4,'pgheo')) = 0) or " + 
                        "                                                       (instr(parameter, nvl(del_page4,'pgheo')) > 0 and instr(parameter, '&'||nvl(del_page4,'pgheo')) = 0) " + 
                        "                                                  then case when instr(parameter, nvl(del_page5,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page5,'pgheo')) > 0 then 'F' " + 
                        "                                                            when instr(parameter, nvl(del_page5,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page5,'pgheo')) > 0 then 'F' " + 
                        "                                                            when (instr(parameter, nvl(del_page5,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page5,'pgheo')) = 0) or " + 
                        "                                                                 (instr(parameter, nvl(del_page5,'pgheo')) > 0 and instr(parameter, '&'||nvl(del_page5,'pgheo')) = 0) then 'T' " + 
                        "                                                       end " + 
                        "                                             end " + 
                        "                                   end " + 
                        "                         end " + 
                        "               else 'T' " + 
                        "               end query_decode2, " + 
                        "               rank() over (partition by a.domain_url||decode(page, null, null, '/'||page)||decode(parameter, null, null, '?'||parameter) order by lengthb(b.path_url) desc ) path_rank " + 
                        "        from " + 
                        "        ( " + 
                        "            select /*+parallel(a,8)*/ " + 
                        "                   rowid rid, access_day, site_id, section_id, query_decode, panel_id, domain_url, page, parameter org_parameter, " + 
                        "                   case when site_id in (43339,5033) and instr(page,'#') > 0 " + 
                        "                            then replace(substr(page,instr(page,'#')+1)||parameter,'#','&') " + 
                        "                        when site_id in (43339,5033) " + 
                        "                            then replace(parameter,'#','&') " + 
                       "                        when site_id = 178 and domain_url like 'http://%dic.naver.com' " + 
                        "                            and instr(decode(substr(page,length(page),1),'/',substr(page,1,length(page)-1),page),'/',1,3) > 0 " + 
                        "                            then substr(page,instr(page,'/',1,2))||'&'||parameter " + 
                        "                        when site_id = 1173 and domain_url = 'https://m.search.daum.net' and page ='search' "+
                        "                            then replace(parameter,'#','&') "+
                        "                   else parameter end parameter " + 
                        "            from   tb_smart_week_temp_section a " +
                        "            WHERE  access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
                        "            AND    access_day <= '"+accessday+"' "+
                        "            and    query_decode is not null " + 
                        "            and    query_decode <> '=' " + 
                        "            and    pv_cnt > 0 " + 
                        "        ) a, " + 
                        "        ( " + 
                        "            SELECT /*+use_hash(b,a)*/ " + 
                        "                   b.site_id, b.section_id, b.path_url, b.add_page1, b.add_page2, b.add_page3, b.add_page4, b.add_page5,  b.del_page1, b.del_page2, b.del_page3, b.del_page4, b.del_page5 " + 
                        "            FROM   tb_section_info b " + 
                        "            WHERE  b.exp_time > sysdate " + 
                        "            AND    section_id in (select code from tb_codebook a where meta_code='SECTION' and (code_etc='2' or code=2)) " + 
                        "            AND    b.query is not null " + 
                        "        ) b " + 
                        "        where a.site_id = b.site_id " + 
                        "        and   a.section_id = b.section_id " + 
                        "        and   a.domain_url||decode(page||org_parameter, null, null, '/'||page)||decode(org_parameter, null, null, '?'||org_parameter) like b.path_url||'%' " + 
                        "    ) " + 
                        ") " + 
                        "WHERE path_rank = 1 " + 
                        "group by site_id, section_id, query_decode, panel_id, rid "+
                  ") "+   
                  "GROUP BY weekcode, site_id, section_id, lower(query_decode), panel_id ";
		
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeDailySetupFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Setup Daily Fact에 insert
	 *************************************************************************/
	
	public Calendar executeDailySetupFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String tablename = "";
		String monthcode = accessday.substring(0,6);
		tablename = findSetupTablename(monthcode);

		System.out.print("The batch - Setup Weight is processing...");
		String query = 
				"insert into tb_smart_day_setup_wgt "+
				"select /*+parallel(a,8)*/ access_day, b.smart_id, a.package_name, panel_id, sysdate "+
				"from  "+tablename+" a, tb_smart_app_info b "+
				"where access_day = '"+accessday+"' "+
				"and   a.package_name = b.package_name "+
				"and   b.exp_time > sysdate "+
				"group by access_day, b.smart_id, a.package_name, panel_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		 query = 
				"insert into tb_smart_day_setup_wgt "+ 
				"select /*+parallel(a,8)*/ access_day, b.p_smart_id smart_id, 'kclick_equal_app' package_name, panel_id, sysdate "+ 
				"from  tb_smart_day_setup_wgt a, tb_smart_app_info b "+ 
				"where access_day = '"+accessday+"' "+ 
				"and   b.p_smart_id is not null "+ 
				"and   b.exp_time > sysdate "+ 
				"and   a.smart_id = b.smart_id "+ 
				"group by access_day, b.p_smart_id, panel_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String queryUpdate = 
				"insert into tb_smart_day_setup_wgt "+
				"select access_day, SMART_ID, PACKAGE_NAME, PANEL_ID, sysdate "+
				"from ( "+
				"    select access_day, SMART_ID, PACKAGE_NAME, PANEL_ID "+
				"    from  tb_smart_day_app_wgt "+
				"    where access_day = '"+accessday+"' " +
				"	 group by access_day, SMART_ID, PACKAGE_NAME, PANEL_ID "+
				"    minus "+
				"    select access_day, SMART_ID, PACKAGE_NAME, PANEL_ID "+
				"    from tb_smart_day_setup_wgt "+
				"    where access_day = '"+accessday+"' "+
				") ";
		
		//System.out.println(queryUpdate);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(queryUpdate);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeIndefinedInsert
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: itrack에 indefinite site 삽입문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeIndefinedInsert(String accessday) throws SQLException 
	{
		Calendar beginPt = Calendar.getInstance();
		System.out.print("Undefined Sites are being inserted...");
		
		String queryD = "delete TB_SMART_INDEFINITE_SITE " +
						"where  CREATE_DATE <= to_date('"+accessday+"','yyyymmdd')-6 ";
		this.pstmt = connection.prepareStatement(queryD);
		this.pstmt.executeUpdate();
		
		String queryB = "delete TB_SMART_INDEFINITE_SITE a " +
						"where EXISTS (SELECT 'X' FROM tb_ban_url b  " +
						"WHERE a.domain_url like b.domain_url " +
						"and b.type_cd = 'D' " +
						"and b.exp_time > sysdate) ";
		this.pstmt = connection.prepareStatement(queryB);
		this.pstmt.executeUpdate();
		
        String query = "insert into TB_SMART_indefinite_site "+
		                "select /*+parallel(a,8)*/ req_domain, count(distinct panel_id) uv_cnt,  "+
		                "       count(*) pv_cnt, sysdate, '' browser_kind "+
		                "from tb_smart_browser_itrack a "+
		                "where access_day between to_char(to_date('"+accessday+"','yyyymmdd')-180,'yyyymmdd') and '"+accessday+"' "+
		                "and req_domain in ( "+
		                "    select REQ_DOMAIN DOMAIN_URL "+
		                "    from tb_smart_browser_itrack "+
		                "    where access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
		                "    and panel_id not like 'tset%' "+
		                "    and browser_kind != '9' "+
		                "    group by REQ_DOMAIN "+
		                "    minus "+
		                "    select domain_url "+
		                "    from tb_domain_info "+
		                "    where exp_time > sysdate and ef_time < sysdate "+
		                "    minus "+
		                "    select domain_url "+
		                "    from TB_SMART_indefinite_site "+
		                "    minus "+
		                "    select DOMAIN_URL "+
		                "    from TB_BAN_URL "+
		                "    where type_cd = 'D' "+
		                "    and exp_time > sysdate and ef_time < sysdate "+
		                ") "+
		                "group by req_domain " +
		                "having length(req_domain) <= 50 ";
				this.pstmt = connection.prepareStatement(query);
				this.pstmt.executeUpdate();
				
				String query1 = "insert into TB_SMART_indefinite_site "+
				                "select /*+parallel(a,8)*/ req_domain, count(distinct panel_id) uv_cnt,  "+
				                "       count(*) pv_cnt, sysdate, '9' browser_kind "+
				                "from tb_smart_browser_itrack a "+
				                "where access_day between to_char(to_date('"+accessday+"','yyyymmdd')-180,'yyyymmdd') and '"+accessday+"' "+
				                "and req_domain in ( "+
				                "    select REQ_DOMAIN DOMAIN_URL "+
				                "    from tb_smart_browser_itrack "+
				                "    where access_day between to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') and '"+accessday+"' "+
				                "    and panel_id not like 'tset%' "+
				                "    and     BROWSER_KIND = '9' "+
				                "    and     RESULT_CD = 'V'"+
				                "    group by REQ_DOMAIN "+
				                "    minus "+
				                "    select domain_url "+
				                "    from tb_domain_info "+
				                "    where exp_time > sysdate and ef_time < sysdate "+
				                "    minus "+
				                "    select domain_url "+
				                "    from TB_SMART_indefinite_site "+
				                "    minus "+
				                "    select DOMAIN_URL "+
				                "    from TB_BAN_URL "+
				                "    where type_cd = 'D' "+
				                "    and exp_time > sysdate and ef_time < sysdate "+
				                ") "+
				                "group by req_domain " +
				                "having length(req_domain) <= 50 ";
				this.pstmt = connection.prepareStatement(query1);
				this.pstmt.executeUpdate();
				
				String queryDelete = 
				  "DELETE FROM tb_smart_indefinite_site a "+
				  "WHERE exists (SELECT 1 FROM tb_domain_info b WHERE exp_time>=sysdate and a.domain_url = b.domain_url)";
				this.pstmt = connection.prepareStatement(queryDelete);
				this.pstmt.executeUpdate();
				
				String queryDeleteSum = "truncate table tb_smart_indefinite_site_sum ";
				this.pstmt = connection.prepareStatement(queryDeleteSum);
				this.pstmt.executeUpdate();
				
				String queryInsertSum = 
				  "insert into tb_smart_indefinite_site_sum "+
				  "select domain_url, uv_cnt, pv_cnt, part_domain part_domain_url, "+
				  "       case when maxid = minid then ','||to_char(maxid)||',' "+
				  "       when maxid is null then ',,' "+
				  "       else ','||maxid||','||minid||',' end as site_ids, create_date, '' browser_kind "+
				  "from ( "+
				  "    select a.DOMAIN_URL, a.UV_CNT, a.PV_CNT, max(site_id) maxid, min(site_id) minid, part_domain, create_date "+
				  "    from ( "+
				  "        select case when FIRSTONE in ('com','net','biz','org','gov','edu','info','to', 'ro', 'ru', 'cc', 'tv', 'de', 'ca', 'nl', 'au') then "+
				   "               '.'||secondone||'.'||firstone "+
				  "               when FIRSTONE in ('uk','kr','jp') and length(secondone)=2 then '.'||thirdone||'.'||secondone||'.'||firstone "+
				  "               else '.'||secondone||'.'||firstone end as part_domain, main_url domain_url, UV_CNT, PV_CNT, create_date "+
				  "        from ( "+
				  "            select  case when instr(leftone,'.') > 0 then reverse(substr(reverse(leftone),1,instr(reverse(leftone),'.')-1)) "+
				  "                    else leftone end as thirdone, secondone, firstone, main_url, UV_CNT, PV_CNT, create_date "+
				  "            from ( "+
				  "                select case when instr(leftone,'.') > 0 then reverse(substr(reverse(leftone),1,instr(reverse(leftone),'.')-1)) "+
				  "                       else leftone end as secondone, firstone, main_url, UV_CNT, PV_CNT,  "+
				  "                       case when instr(leftone,'.') > 0 then reverse(substr(reverse(leftone),instr(reverse(leftone),'.')+1)) "+
				  "                       else '' end as leftone, create_date "+
				  "                from ( "+
				  "                    select reverse(substr(reverse(domain_url),1,instr(reverse(domain_url),'.')-1)) firstone,  "+
				  "                           reverse(substr(reverse(domain_url),instr(reverse(domain_url),'.')+1)) leftone, "+
				  "                           main_url, UV_CNT, PV_CNT, create_date "+
				  "                    from ( "+
				  "                        select case when DOMAIN_URL like '%:%' then substr(domain_url,1,instr(domain_url,':')-1)  "+
				  "                               else domain_url end domain_url, main_url, UV_CNT, PV_CNT, create_date "+
				  "                        from ( "+
				  "                            select case when substr(domain_url,length(domain_url),1) = '.' then substr(domain_url,1, length(domain_url)-1) "+
				  "                                   else domain_url end domain_url, main_url, UV_CNT, PV_CNT, create_date "+
				  "                            from ( "+
				  "                                select case when substr(DOMAIN_URL,1,7) = 'http://' then substr(DOMAIN_URL,8) "+
				  "                                       when substr(DOMAIN_URL,1,8) = 'https://' then substr(DOMAIN_URL,9) "+
				  "                                       else '' end as domain_url, domain_url main_url, UV_CNT, PV_CNT, create_date "+
				  "                                from tb_smart_indefinite_site "+
				  "                                where UV_CNT >= 3 "+
				"                                and     nvl(browser_kind,0) != '9' "+
				  "                            ) "+
				  "                        ) "+
				  "                    ) "+
				  "                ) "+
				  "            ) "+
				  "        ) "+
				  "    ) a, ( "+
				  "        select   DOMAIN_URL, SITE_ID, " +
				  "                                           case when PART_DOMAIN_URL like '..%' then replace(PART_DOMAIN_URL,'..','.') " +
				  "                                           else PART_DOMAIN_URL end as PART_DOMAIN_URL " +
				  "                    from      tb_domain_info "+
				  "        WHERE    exp_time >= sysdate "+
				  "        AND      status = 'O' "+
				  "    ) "+
				  "    where part_domain = PART_DOMAIN_URL(+) "+
				  "    group by a.DOMAIN_URL, a.UV_CNT, a.PV_CNT, create_date "+
				  ") ";
				this.pstmt = connection.prepareStatement(queryInsertSum);
				this.pstmt.executeUpdate();
				
				String queryInsertSum1 = 
				  "insert into tb_smart_indefinite_site_sum "+
				  "select domain_url, uv_cnt, pv_cnt, part_domain part_domain_url, "+
				  "       case when maxid = minid then ','||to_char(maxid)||',' "+
				  "       when maxid is null then ',,' "+
				  "       else ','||maxid||','||minid||',' end as site_ids, create_date, '9' browser_kind "+
				  "from ( "+
				  "    select a.DOMAIN_URL, a.UV_CNT, a.PV_CNT, max(site_id) maxid, min(site_id) minid, part_domain, create_date "+
				  "    from ( "+
				  "        select case when FIRSTONE in ('com','net','biz','org','gov','edu','info','to', 'ro', 'ru', 'cc', 'tv', 'de', 'ca', 'nl', 'au') then "+
				  "               '.'||secondone||'.'||firstone "+
				  "               when FIRSTONE in ('uk','kr','jp') and length(secondone)=2 then '.'||thirdone||'.'||secondone||'.'||firstone "+
				  "               else '.'||secondone||'.'||firstone end as part_domain, main_url domain_url, UV_CNT, PV_CNT, create_date "+
				  "        from ( "+
				  "            select  case when instr(leftone,'.') > 0 then reverse(substr(reverse(leftone),1,instr(reverse(leftone),'.')-1)) "+
				  "                    else leftone end as thirdone, secondone, firstone, main_url, UV_CNT, PV_CNT, create_date "+
				  "            from ( "+
				  "                select case when instr(leftone,'.') > 0 then reverse(substr(reverse(leftone),1,instr(reverse(leftone),'.')-1)) "+
				  "                       else leftone end as secondone, firstone, main_url, UV_CNT, PV_CNT,  "+
				  "                       case when instr(leftone,'.') > 0 then reverse(substr(reverse(leftone),instr(reverse(leftone),'.')+1)) "+
				  "                       else '' end as leftone, create_date "+
				  "                from ( "+
				  "                    select reverse(substr(reverse(domain_url),1,instr(reverse(domain_url),'.')-1)) firstone,  "+
				  "                           reverse(substr(reverse(domain_url),instr(reverse(domain_url),'.')+1)) leftone, "+
				  "                           main_url, UV_CNT, PV_CNT, create_date "+
				  "                    from ( "+
				  "                        select case when DOMAIN_URL like '%:%' then substr(domain_url,1,instr(domain_url,':')-1)  "+
				  "                               else domain_url end domain_url, main_url, UV_CNT, PV_CNT, create_date "+
				  "                        from ( "+
				  "                            select case when substr(domain_url,length(domain_url),1) = '.' then substr(domain_url,1, length(domain_url)-1) "+
				  "                                   else domain_url end domain_url, main_url, UV_CNT, PV_CNT, create_date "+
				  "                            from ( "+
				  "                                select case when substr(DOMAIN_URL,1,7) = 'http://' then substr(DOMAIN_URL,8) "+
				  "                                       when substr(DOMAIN_URL,1,8) = 'https://' then substr(DOMAIN_URL,9) "+
				  "                                       else '' end as domain_url, domain_url main_url, UV_CNT, PV_CNT, create_date "+
				  "                                from tb_smart_indefinite_site "+
				  "                                where UV_CNT >= 3 "+
				"                                and     browser_kind = '9' "+
				  "                            ) "+
				  "                        ) "+
				  "                    ) "+
				  "                ) "+
				  "            ) "+
				  "        ) "+
				  "    ) a, ( "+
				  "        select   DOMAIN_URL, SITE_ID, " +
			  "                                           case when PART_DOMAIN_URL like '..%' then replace(PART_DOMAIN_URL,'..','.') " +
			  "                                           else PART_DOMAIN_URL end as PART_DOMAIN_URL " +
				  "                    from      tb_domain_info "+
				  "        WHERE    exp_time >= sysdate "+
				  "        AND      status = 'O' "+
				  "    ) "+
				  "    where part_domain = PART_DOMAIN_URL(+) "+
				  "    group by a.DOMAIN_URL, a.UV_CNT, a.PV_CNT, create_date "+
				  ") ";
				this.pstmt = connection.prepareStatement(queryInsertSum1);
				this.pstmt.executeUpdate();
		
		System.out.println("UPDATE DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return beginPt;
	}
	
	//다음 달의 3일을 계산해준다.
	public String nextMonth(String period){
		int date = 0;
		if(period.length()>6){
			date = Integer.parseInt(period.substring(0, 6));
		} else {
			date = Integer.parseInt(period);
		}
		
		if(date%100 == 12){
			date+=89;
		} else {
			date+=1;
		}
		
		return Integer.toString(date)+"03";
	}
	
	/**************************************************************************
	 *		메소드명		: executeSiteIdUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: itrack에 Site_id 업데이트문 (UPDATE)
	 *************************************************************************/
	
	public Calendar executeSiteIdUpdate(String accessday) throws SQLException 
	{
		Calendar beginPt = Calendar.getInstance();
		System.out.print("SiteID is being updated...");
		String query = "truncate table tb_smart_temp_browser_itrack";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
		query = "truncate table tb_smart_temp_ban_rid";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
		
    	String queryD ="update tb_smart_browser_itrack a " +
					   "set req_site_id = (" +
					   "	select nvl(site_id,0) " +
					   "	from tb_domain_info " +
					   "	where domain_url = a.req_domain "+
					   " 	and exp_time > sysdate " +
					   " 	and ef_time < sysdate " +
					   " 	and NOT EXISTS (SELECT * FROM tb_ban_url b " +
					   "					WHERE a.req_domain like b.domain_url " + 
					   "					and b.type_cd = 'D' " + 
					   "					and b.exp_time > sysdate) " +
					   ") " +
					   "where access_day = '"+accessday+"' ";

		
		this.pstmt = connection.prepareStatement(queryD);
		this.pstmt.executeUpdate();
		
		query = "insert into tb_smart_temp_browser_itrack "+
				"select rowid rid, req_site_id, req_domain, req_page, req_parameter "+
				"from   tb_smart_browser_itrack "+
				"where  access_day = '"+accessday+"' "+
				"and    req_site_id <> 0 "+
				"and    ((result_cd = 'V') or (browser_kind<>9)) ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
		
		String queryP = "insert into tb_smart_temp_ban_rid "+
				"select rid " +
				"from  " +
				"(  " +
				"        select rid, req_site_id, req_domain||decode(req_page||req_parameter,null,null,'/'||req_page)||decode(req_parameter,null,null,'?'||req_parameter) path_url " +  
				"        from   tb_smart_temp_browser_itrack " +
				")a, " +
				"(  " +
				"        select site_id, domain_url||decode(page||parameter,null,null,'/'||page)||decode(parameter,null,null,'?'||parameter) ban_url  " +
				"        from tb_ban_url " +
				"        where exp_time > sysdate " +
				"        and f_condition = '1' " +
				"        and f_pattern = 'P' " +
				"        and site_id <> 0 " +
				"        and type_cd in ('P', 'Q') " +
				")b " +
				"where a.req_site_id = b.site_id " +
				"and   a.path_url like b.ban_url ";
		
		
//		System.out.println(queryP);
//		System.exit(0);
		this.pstmt = connection.prepareStatement(queryP);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String queryQ = "insert into tb_smart_temp_ban_rid "+
				"select rid " +
				"from  " +
				"(  " +
				"        select rid, req_site_id, req_domain||decode(req_page||req_parameter,null,null,'/'||req_page)||decode(req_parameter,null,null,'?'||req_parameter) path_url " +  
				"        from   tb_smart_temp_browser_itrack " +
				")a, " +
				"(  " +
				"        select site_id, domain_url||decode(page||parameter,null,null,'/'||page)||decode(parameter,null,null,'?'||parameter) ban_url  " +
				"        from tb_ban_url " +
				"        where exp_time > sysdate " +
				"        and f_condition = '1' " +
				"        and f_pattern = 'E' " +
				"        and site_id <> 0 " +
				"        and type_cd in ('P', 'Q') " +
				")b " +
				"where a.req_site_id = b.site_id " +
				"and   a.path_url = b.ban_url ";
		
//	System.out.println(queryP);
//	System.exit(0);
	this.pstmt = connection.prepareStatement(queryQ);
	this.pstmt.executeUpdate();
	
	if(this.pstmt!=null) this.pstmt.close();		
	
	String UpdateQuery ="update tb_smart_browser_itrack a "+
						"set a.req_site_id = 0 "+
						"where rowid in (select rid from tb_smart_temp_ban_rid) ";

	
	this.pstmt = connection.prepareStatement(UpdateQuery);
	this.pstmt.executeUpdate();
	
	
	if(this.pstmt!=null) this.pstmt.close();		
		
		String query2 = "update tb_smart_browser_itrack a " +
				   "set req_site_id = 0 where req_site_id is null " +
				   "and access_day = '"+accessday+"' ";
		
		this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
//		System.out.println(query2);
		System.out.println("UPDATE DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return beginPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeChromeReslutUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: browser_kind result_cd 선정 (UPDATE)
	 *************************************************************************/	
	
	public Calendar executeChromeSbrowserReslutUpdate(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		
    	String InsertChPanel =
				"insert into TB_SMART_CHROME_PANEL "+
				"select access_day, panel_id "+
				"from "+
				"( "+
				"	select access_day, panel_id, browser_kind "+
				"   from tb_smart_browser_itrack "+
				"   where access_day = '"+accessday+"' "+
				"   and ((browser_kind = '2')  "+
				"   or (browser_kind ='9' and package_name = 'com.android.chrome' )) "+
				"   group by access_day, panel_id, browser_kind "+
				") "+
				"group by access_day, panel_id " +
				"having count(*) =2 ";
		
		this.pstmt = connection.prepareStatement(InsertChPanel);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();		
				
		String ResultS = 				
				" update tb_smart_browser_itrack "+
				" set result_cd = 'S' "+
				" where access_day = '"+accessday+"' " +
				" and browser_kind = '9' "+
				" and pre_package_name = 'com.android.chrome' "+
				" and pre_package_name=package_name "+
				" and accept_name like '%text/html,application/xhtml%' " +
				" and req_site_id in (select site_id from tb_site_info where exp_time > sysdate and category_code2 <>'ZP') "+
				" and panel_id not in (select panel_id from TB_SMART_CHROME_PANEL where access_day = '"+accessday+"') ";
		
		System.out.print("Browser_kind ='9' chrome Result_CD S is being updated...");

		this.pstmt = connection.prepareStatement(ResultS);
		this.pstmt.executeUpdate();
    	if(this.pstmt!=null) this.pstmt.close();
    	System.out.println("UPDATE DONE.");

    	String InsertSBrowserPanel =
				"insert into TB_SMART_SBROWSER_PANEL "+
				"select access_day, panel_id "+
				"from "+
				"( "+
				"	select access_day, panel_id, browser_kind "+
				"   from tb_smart_browser_itrack "+
				"   where access_day = '"+accessday+"' "+
				"   and ((browser_kind = '3')  "+
				"   or (browser_kind ='9' and package_name = 'com.sec.android.app.sbrowser' )) "+
				"   group by access_day, panel_id, browser_kind "+
				") "+
				"group by access_day, panel_id " +
				"having count(*) =2 ";
		
		this.pstmt = connection.prepareStatement(InsertSBrowserPanel);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();	
    	
		ResultS = 				
				" update tb_smart_browser_itrack "+
				" set result_cd = 'S' "+
				" where access_day = '"+accessday+"' " +
				//" and track_version = 20 "+
				" and browser_kind = '9' "+
				" and pre_package_name = 'com.sec.android.app.sbrowser' "+
				" and pre_package_name=package_name "+
				" and accept_name like '%text/html,application/xhtml%' " +
				" and req_site_id in (select site_id from tb_site_info where exp_time > sysdate and category_code2 <>'ZP') "+
				" and panel_id not in (select panel_id from TB_SMART_SBROWSER_PANEL where access_day = '"+accessday+"') ";
    	
    	System.out.print("Browser_kind ='9' sbrowser network is being updated...");
		
    	this.pstmt = connection.prepareStatement(ResultS);
		this.pstmt.executeUpdate();
    	if(this.pstmt!=null) this.pstmt.close();
    	System.out.println("UPDATE DONE.");		
    	
    	String banner_url ="update tb_smart_browser_itrack a "+
    			"set 	result_cd = 'D' "+
    			"where access_day = '"+ accessday +"' "+
    			"        and a.rowid in (  "+
    			"        select /*+ USE_CONCAT */ a.rowid "+
    			"        from tb_smart_browser_itrack a, tb_ban_url b "+
    			"        where (( type_cd = 'D' and req_site_id = b.site_id and req_domain like domain_url) or  "+
    			"  		        ( type_cd = 'P' and req_site_id = b.site_id and req_domain like domain_url and req_page like page ) or "+
    			"			    ( type_cd = 'Q' and req_site_id = b.site_id and req_domain like domain_url and req_page like page and req_parameter like parameter )) "+
    			"        and   b.exp_time > sysdate  "+
    			"        and   access_day =  '"+ accessday +"' "+
    			"        and   b.description = 'network' "+
    			"        and   browser_kind = '9' "+
    			"        and   result_cd = 'S' "+
    			"    ) " ;
		
		this.pstmt = connection.prepareStatement(banner_url);
		this.pstmt.executeUpdate();
    	if(this.pstmt!=null) this.pstmt.close();
    	System.out.println("UPDATE DONE.");
    	
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeResultUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: itrack에 result_cd(유효페이지) 업데이트 (UPDATE): 사용안함
	 *************************************************************************/
	
	public Calendar executeCheckDate(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = 
				//" update tb_smart_browser_itrack "+
				" update temp_hwlee_browser_itrack "+
				" set result_cd = 'X' "+
				" where access_day = '"+accessday+"' " +
				" and access_day <> to_char(server_date, 'yyyymmdd')";
		
		System.out.print("Result_CD(Checking Date) is being updated...");
//		System.out.println(query);
//		System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();

		System.out.println("UPDATE DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeResultUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: itrack에 result_cd(유효페이지) 업데이트 (UPDATE)
	 *************************************************************************/
	
	public Calendar executeResultUpdate(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("Result_CD is being updated...");
		String query = 
				" update tb_smart_browser_itrack a "+
				" set result_cd = 'S' "+
				" where duration > 0 "+
				" and result_cd is null "+
				" and req_site_id != 0 "+
				" and to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
				" and access_day = '" + accessday +"' " +
				" and browser_kind in ('1','2','3') ";
		

		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String queryA =
				"update tb_smart_browser_itrack set result_cd = null "+
				"where access_day = '" + accessday +"' "+
				"and duration = 0 "+
				"and result_cd = 'S' "+
				"and browser_kind = '9' ";
		
		this.pstmt = connection.prepareStatement(queryA);
		this.pstmt.executeUpdate();		
		
		String queryB =
				"update tb_smart_browser_itrack set result_cd = null "+
				"where rowid in "+
				"( "+
				"    select rid "+
				"    from "+
				"    ( "+
				"        select rid, access_day, panel_id, req_site_id, req_sec, "+
				"            lead(req_site_id,1) over(partition by access_day, panel_id order by req_million, req_date, req_site_id) fore_site_id, "+
				"         lead(duration,1) over(partition by access_day, panel_id order by req_million, req_date, req_site_id) fore_duration, duration, "+
				"         lead(req_sec,1) over(partition by access_day, panel_id order by req_million, req_date, req_site_id) fore_req_sec "+
				"        from "+
				"        ( "+
				"            select rowid rid, access_day, panel_id, req_million,  duration, req_date, req_site_id, to_number(to_CHAR(req_date,'SSSSS')) req_sec "+
				"            from tb_smart_browser_itrack "+
				"            where access_day = '"+accessday+"' "+
				"            and result_cd = 'S' "+
				"            and browser_kind ='9' "+
				"            order by access_day, panel_id, req_million, req_date "+
				"        ) "+
				"    ) "+
				"    where duration = 1 "+
				"    and fore_req_sec - req_sec =1 "+
				"    and req_site_id = fore_site_id "+
				"    and fore_duration <> 0 "+
				") ";

		this.pstmt = connection.prepareStatement(queryB);
		this.pstmt.executeUpdate();		
		
		
		String queryC = 
				"update tb_smart_browser_itrack a "+
				"set 	result_cd = 'D'  "+
				"where access_day = '"+ accessday +"' "+
				"and a.rowid in ( "+
				"   select /*+ USE_CONCAT */ a.rowid "+
				"   from tb_smart_browser_itrack a, tb_ban_url b "+
				"   where   (( type_cd = 'D' and req_site_id = b.site_id and req_domain like domain_url) or  "+
				"   		    ( type_cd = 'P' and req_site_id = b.site_id and req_domain like domain_url and req_page like page ) or "+
				"			( type_cd = 'Q' and req_site_id = b.site_id and req_domain like domain_url and req_page like page and req_parameter like parameter )) "+
				"   and   b.exp_time > sysdate "+
				"   and   access_day =  '"+ accessday +"' "+
				"   and   result_cd = 'S' "+
				"   and   b.site_id <> 0 "+
				")  ";
		this.pstmt = connection.prepareStatement(queryC);
		this.pstmt.executeUpdate();
		
		//�뿰�빀�돱�뒪 dummy page �젣嫄� -- 20150521 --二쇱꽍
		String query1 = "update tb_smart_browser_itrack set result_cd = 'D' "+
				   "where req_site_id = 404 "+
				   "and req_domain = 'http://m.yna.co.kr' "+
				   "and req_page = 'kr/' "+
				   "and req_parameter like 'cid=AKR%' " +
				   "and   access_day =  '"+ accessday +"' "+
				   "and duration = 1 "+
				   "and result_cd = 'S' ";
	
		this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		
		
		String query2 = "update tb_smart_browser_itrack set result_cd = 'D' "+
				   "where req_site_id = 44435 "+
				   "and req_domain = 'http://m.heraldbiz.com' "+
				   "and req_page = 'view.php' "+
				   "and req_parameter like 'ud=%&pos=naver' " +
				   "and   access_day =  '"+ accessday +"' "+
				   "and duration = 1 "+
				   "and result_cd = 'S' ";
	
		this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();

		String query3 = "update tb_smart_browser_itrack set result_cd = 'D' "+
				   "where req_site_id = 1573 "+
				   "and req_domain = 'http://m.mt.co.kr' "+
				   "and req_page = 'renew/' "+
				   "and req_parameter like 'no=%' " +
				   "and   access_day =  '"+ accessday +"' "+
				   "and duration = 1 "+
				   "and result_cd = 'S' ";
	
		this.pstmt = connection.prepareStatement(query3);
		this.pstmt.executeUpdate();
		
		String query4 = "update tb_smart_browser_itrack set result_cd = 'D' "+
				   "where req_site_id =1181 "+
				   "and req_domain = 'http://m.seoul.co.kr' "+
				   "and req_page = 'redirection.php' "+
				   "and req_parameter like 'id=%' " +
				   "and   access_day =  '"+ accessday +"' "+
				   "and duration = 1 "+
				   "and result_cd = 'S' ";
	
		this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();
		
		
		
		executeQueryExecute("TRUNCATE TABLE TB_TEMP_NAVI_ITRACK");
		String Tempquery =  "insert into TB_TEMP_NAVI_ITRACK"+
				"			 select  rid, access_day, req_date, panel_id, REQ_SITE_ID, req_domain, req_page, req_parameter,"+
				"                    lead(REQ_SITE_ID,1) over(partition by panel_id order by req_date) next_site_id,"+
				"                    lead(req_domain,1) over(partition by panel_id order by req_date) next_domain,"+
				"                    lead(req_page,1) over(partition by panel_id order by req_date) next_page,"+
				"                    lead(req_parameter,1) over(partition by panel_id order by req_date) next_parameter"+
				"            from "+
				"            ("+
				"                select  /*+parallel(a,8)*/"+
				"                        rowid rid, a.*"+
				"                from    tb_smart_browser_itrack a"+
				"                where   access_day = '"+ accessday +"' "+
				"                AND     result_cd = 'S'"+
				"                and     duration >0"+
				"            )    ";
		executeQueryExecute(Tempquery);
		
		
		String query5 = "update /*+ BYPASS_UJVC */"+
				"("+
				"    select b.*"+
				"    from"+
				"    ("+
				"        select *"+
				"        from TB_TEMP_NAVI_ITRACK"+
				"        where access_day = '"+ accessday +"' "+
				"        and REQ_SITE_ID = 261"+
				"        and next_Site_id = 261"+
				"        and req_domain = 'http://news.chosun.com' and req_page like '%html%'"+
				"        and next_domain = 'http://m.chosun.com' and next_parameter like '%contid=%'"+
				"    )a, tb_smart_browser_itrack b"+
				"    where a.rid = b.rowid "+
				")"+
				"set result_cd = 'D'";
	
		this.pstmt = connection.prepareStatement(query5);
		this.pstmt.executeUpdate();
		
		String query6 = "update /*+ BYPASS_UJVC */"+
				"("+
				"    select b.*"+
				"    from"+
				"    ("+
				"        select *"+
				"        from TB_TEMP_NAVI_ITRACK"+
				"        where access_day = '"+ accessday +"' "+
				"        and REQ_SITE_ID = 261"+
				"        and next_Site_id = 261"+
				"        and req_domain = 'http://www.chosun.com' and req_page is null and req_parameter is null"+
				"        and next_domain = 'http://m.chosun.com' and next_page is null and next_parameter is null"+
				"    )a, tb_smart_browser_itrack b"+
				"    where a.rid = b.rowid "+
				")"+
				"set result_cd = 'D'";
	
		this.pstmt = connection.prepareStatement(query6);
		this.pstmt.executeUpdate();
		
		
		//http://sports.chosun.com/news/ntype2.htm%, http://m.sportschosun.com/news.htm%
		String query7 = "update /*+ BYPASS_UJVC */"+
				"("+
				"    select b.*"+
				"    from"+
				"    ("+
				"        select *"+
				"        from TB_TEMP_NAVI_ITRACK"+
				"        where access_day = '"+ accessday +"' "+
				"        and REQ_SITE_ID = 261"+
				"        and next_Site_id = 261"+
				"        and req_domain = 'http://sports.chosun.com' and req_page like 'news/ntype2.htm%'"+
				"        and next_domain = 'http://m.sportschosun.com' and next_page like 'news.htm%'"+
				"    )a, tb_smart_browser_itrack b"+
				"    where a.rid = b.rowid "+
				")"+
				"set result_cd = 'D'";
	
		this.pstmt = connection.prepareStatement(query7);
		this.pstmt.executeUpdate();
		
		//http://sports.chosun.com/news/ntype2.htm%, http://m.sportschosun.com/news.htm%
		String query8 = "update /*+ BYPASS_UJVC */ "+
				"( "+
				"    select b.* "+
				"    from "+
				"    ( "+
				"        select * "+
				"        from  "+
				"        ( "+
				"            select  rid, access_day, req_date, panel_id, REQ_SITE_ID, req_domain, req_page, req_parameter, "+
				"                    lead(rid,1) over(partition by panel_id order by req_date) next_rid, "+
				"                    lead(REQ_SITE_ID,1) over(partition by panel_id order by req_date) next_site_id, "+
				"                    lead(req_domain,1) over(partition by panel_id order by req_date) next_domain, "+
				"                    lead(req_page,1) over(partition by panel_id order by req_date) next_page, "+
				"                    lead(req_parameter,1) over(partition by panel_id order by req_date) next_parameter, "+
				"                    lead(REQ_SITE_ID,2) over(partition by panel_id order by req_date) next2_site_id, "+
				"                    lead(req_domain,2) over(partition by panel_id order by req_date) next2_domain, "+
				"                    lead(req_page,2) over(partition by panel_id order by req_date) next2_page, "+
				"                    lead(req_parameter,2) over(partition by panel_id order by req_date) next2_parameter "+
				"            from  "+
				"            ( "+
				"                select  /*+parallel(a,8)*/ "+ 
				"                        rowid rid, a.* "+
				"                from    tb_smart_browser_itrack a "+
				"                where   access_day ='"+ accessday +"' "+
				"                AND     result_cd = 'S' "+
				"                AND     panel_flag in ('D','V') "+ 
				"                and     duration >0 "+
				"                and     BROWSER_KIND != 9 "+
				"            )  "+
				"        ) "+
				"        where access_day ='"+ accessday +"' "+
				"        and REQ_SITE_ID = 261 "+
				"        and next_Site_id = 261 "+
				"        and next2_Site_id = 261 "+
				"        and req_domain = 'http://sports.chosun.com' and req_page like 'news/ntype2.htm%' "+
				"        and next_domain = 'http://m.sportschosun.com' and next_page like 'list_p.htm%' "+
				"        and next2_domain = 'http://m.sportschosun.com' and next2_page like 'news.htm%' "+
				"        and next_parameter = next2_parameter "+
				"    )a, tb_smart_browser_itrack b "+
				"    where (a.rid = b.rowid or a.next_rid = b.rowid) "+
				") "+
				"set result_cd = 'D'";

	
		this.pstmt = connection.prepareStatement(query8);
		this.pstmt.executeUpdate();
		
		System.out.println("UPDATE DONE.");
		if(this.pstmt!=null) this.pstmt.close();

		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executePanelSignal
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: tb_smart_day_panel_signal에 Panel Signal(유효페이지) 삽입 (INSERT)
	 *************************************************************************/
	
	public Calendar executePanelSignal(String monthcode) throws SQLException 
	{
		Calendar beginPt = Calendar.getInstance();
		String query = "";
		System.out.print("The batch - Monthly Signal is processing...");

//		if(code.equalsIgnoreCase("d") && countTable(period, code, "tb_smart_day_panel_signal")){
//			String table_name = findAppTablename(period);
//			query = 
//					"insert into tb_smart_day_panel_signal "+
//					"select access_day, panel_id, count(*) sig_no, 'Y' isvalid, 'W' flag "+
//					"from tb_smart_browser_itrack "+
//					"where access_day ='"+period+"' "+
//					" group by access_day, panel_id "+
//					"union all "+
//					"select access_day, panel_id, count(*) sig_no, 'Y' isvalid, 'T' flag "+
//					"from "+table_name+" "+
//					"where access_day = '"+period+"' "+
//					" group by access_day, panel_id "
////					+"union all "+
////					"select access_day, panel_id, count(*) sig_no, 'Y' isvalid, 'A' flag "+
////					"from tb_smart_app_itrack "+
////					"where access_day ="+period+
////					" group by access_day, panel_id "
////					+"union all "+
////					"select access_day, panel_id, count(*) sig_no, 'Y' isvalid, 'D' flag "+
////					"from tb_smart_device_itrack "+
////					"where access_day ="+period+
////					" group by access_day, panel_id "
//					;
//			
//			System.out.print("Daily Signal is being inserting...");
//		}
//		else if(code.equalsIgnoreCase("m") && countTable(period, code, "TB_SMART_MONTH_PANEL_SIGNAL")){
//			query = 
//					"INSERT   INTO TB_SMART_MONTH_PANEL_SIGNAL "+
//					"         (monthcode, panel_id, sig_no, isvalid, flag) "+
//					"SELECT   '"+period+"', "+
//					"         B.panel_id, "+
//					"         nvl(sig_no,0) sig_no, "+
//					"         case when sig_no > 0 then 'Y' "+
//					"         else 'N' end as isvalid," +
//					"		  flag "+
//					"FROM "+
//					"(        SELECT   panel_id, flag, "+
//					"                  sum(nvl(sig_no,0)) sig_no "+
//					"         FROM     tb_smart_day_panel_signal "+
//					"         WHERE    access_day like '"+period+"%' "+
//					"         GROUP BY panel_id, flag "+
//					") A,              "+
//					"(        SELECT   panel_id "+
//					"         FROM     tb_smart_panel "+
//					"		  WHERE    PANEL_STATUS_CD=1 "+
//					"		  and      ISAGREE='Y'"+
//					") B       "+
//					"WHERE    A.panel_id(+) = B.panel_id ";
//			
//			System.out.print("Monthly Signal is being inserting...");
//		}
//		else {
//			System.out.print("Signal already exists.");
//		}
//		System.out.println(query);
//		System.exit(0);
		
		String queryT = "TRUNCATE TABLE tb_temp_smart_signal";
		this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		query = "INSERT INTO tb_temp_smart_signal  "+
				"select panel_id "+
				"from ( "+
				"    select panel_id, count(distinct access_day) daily_freq "+
				"    from   tb_smart_day_app_wgt "+
				"    where  access_day like '"+monthcode+"%' "+
				"    and    panel_id not in (SELECT panel_id FROM tb_exception_panel WHERE exp_time > sysdate GROUP BY panel_id) "+
				"    and    panel_id in (select panel_id from tb_smart_panel where isagree='Y' and reg_date < to_date('"+monthcode+"','YYYYMM') and PANEL_STATUS_CD=1 and to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6) "+
				"    group by panel_id "+
				") "+
				"where daily_freq >= 20 ";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERT DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return beginPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeDaytimeVisit
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Daytime_Visit 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeDaytimeVisit(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daytime Visit is processing...");
		
		if(countTable(accessday, "d", "tb_smart_daytime_visit")){
			String query = "INSERT INTO tb_smart_daytime_visit (access_day, time_cd, panel_id, site_id, visit_cnt, proc_date) "+
			"SELECT access_day, time_cd, panel_id, req_site_id site_id, sum(visit_cnt) visit_cnt, sysdate proc_date "+
			"FROM  "+
			"(         "+
			"    SELECT  "+
			"			 access_day, panel_id, req_site_id, to_char(REQ_DATE,'HH24') time_cd,  "+
			"            case when round((REQ_DATE - lag(REQ_DATE, 1) "+
			"            over (partition by req_site_id, panel_id order by REQ_DATE))*60* 60*24) > 60*30 "+
			"            or lag(REQ_DATE, 1) over (partition by req_site_id, panel_id order by REQ_DATE) is NULL then 1  end visit_cnt "+
			"    FROM    tb_smart_browser_itrack a "+
			"    WHERE   access_day = '"+accessday+"' "+
			"    AND     result_cd = 'S' "+
			"    AND     panel_flag in ('D','V') "+
			"    AND     (req_site_id != 15537 or req_site_id > 0) "+
			") "+
			"GROUP BY access_day, panel_id, req_site_id, time_cd "+
			"HAVING sum(visit_cnt) > 0";
			
	        this.pstmt = connection.prepareStatement(query);
			this.pstmt.executeUpdate();
			
			//System.out.println(query);
			System.out.println("INSERTION DONE.");
			if(this.pstmt!=null) this.pstmt.close();
		} else {
			System.out.print("Daytime Visit already exists.");
			System.exit(0);
		}
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeDaytimeVisit
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Daytime_Visit 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeRawDaytime(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daytime Visit is processing...");
		
		if(countTable(accessday, "d", "tb_smart_raw_daytime")){
			String query = "insert into tb_smart_raw_daytime "+
					"select access_day, to_char(req_Date,'hh24') time_cd, req_site_id site_id, req_domain domain_url, "+
					"    req_page page, panel_id, count(*) pv_cnt, sum(duration) duration, sysdate proc_date "+
					"from tb_smart_browser_itrack a "+
					"where access_day = '"+accessday+"' "+
					"and panel_flag in ('D','V') "+
					"and result_cd = 'S' "+
					"and duration > 0 "+
					"group by access_day, to_char(req_Date,'hh24'), req_site_id, req_domain, req_page, panel_id ";
			
	        this.pstmt = connection.prepareStatement(query);
			this.pstmt.executeUpdate();
			
			//System.out.println(query);
			System.out.println("INSERTION DONE.");
			if(this.pstmt!=null) this.pstmt.close();
		} else {
			System.out.print("tb_smart_raw_daytime");
			System.exit(0);
		}
		return eachPt;
	}
	/**************************************************************************
	 *		메소드명		: executeDaytimeFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Daytime_Fact 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeDaytimeFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		if(countTable(accessday, "d", "tb_smart_daytime_fact")){
			String query = "insert into tb_smart_daytime_fact "+
			"SELECT   access_day, C.time_cd, C.site_id, C.panel_id,  "+
			"         B.category_code1, B.category_code2, B.category_code3, "+
			"         C.pv_cnt, visit_cnt, C.duration, sysdate "+
			"FROM      "+
			"(         "+
			"    SELECT   * "+
			"    FROM     tb_site_info "+
			"    WHERE    ef_time  <= to_date('"+accessday+"','yyyymmdd')+1 "+
			"    AND      exp_time >= to_date('"+accessday+"','yyyymmdd')+1 "+
			") B, "+
			"(   SELECT   A.access_day, A.time_cd, A.site_id, A.panel_id, A.pv_cnt, A.duration, nvl(B.visit_cnt,0) visit_cnt "+
			"    FROM "+
			"	(      "+
			"        select access_day, to_char(REQ_DATE,'HH24') time_cd, REQ_SITE_ID site_id, PANEL_ID, count(*) pv_cnt, sum(duration) duration "+
			"        from tb_smart_browser_itrack "+
			"        where result_cd = 'S' "+
			"        and access_day = '"+ accessday +"' "+
			"        and REQ_SITE_ID > 0 "+
			"        and panel_flag in ('D','V') "+
			"        group by access_day, to_char(REQ_DATE,'HH24'), REQ_SITE_ID, PANEL_ID "+
			"    ) a, "+
			"    ( "+
			"        SELECT   access_day, time_cd, site_id, panel_id, visit_cnt "+
			"		FROM     tb_smart_daytime_visit "+
			"		WHERE    access_day = '"+ accessday +"' "+
			"    ) b "+
			"    WHERE    A.time_cd = B.time_cd(+) "+
			"    AND      A.site_id = B.site_id(+) "+
			"    AND      A.panel_id = B.panel_id(+) "+
			"    AND      A.access_day = B.access_day(+) "+
			") C "+
			"WHERE    C.site_id  = B.site_id ";
			
			System.out.print("The batch - Daytime Fact is processing...");
	        this.pstmt = connection.prepareStatement(query);
			this.pstmt.executeUpdate();
			
//					System.out.println(query);
			System.out.println("INSERTION DONE.");
			if(this.pstmt!=null) this.pstmt.close();
		} else {
			System.out.print("Daytime Fact already exists.");
			System.exit(0);
		}
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeAppDaytimeFactETC
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: AppDaytimeFactETC 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeAppDaytimeFactETC(String accessday, String type) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "";
		if(type.equals("1")&&(!countTable(accessday, "D", "TB_SMART_DAYTIME_APPLVL1_WGT"))){
			System.out.print("DAYTIME_APPLVL1_FACT already exists");
			System.exit(0);
		} else if(type.equals("2")&&(!countTable(accessday, "D", "TB_SMART_DAYTIME_APPLVL2_WGT"))){
			System.out.print("DAYTIME_APPLVL2_FACT already exists");
			System.exit(0);
		} else if(type.equals("TOT")&&(!countTable(accessday, "D", "TB_SMART_DAYTIME_APPTOT_WGT"))){
			System.out.print("DAYTIME_APPTOT_FACT already exists");
			System.exit(0);
		} 
		
		if(!type.equals("TOT")) {
			query = 
					"INSERT   INTO TB_SMART_DAYTIME_APPLVL1_WGT "+
					"         (access_day, time_cd, panel_id, APP_CATEGORY_CD1, duration, proc_date)  "+
					"SELECT   /*+leading(b)*/ access_day, time_cd, panel_id, max(APP_CATEGORY_CD1), sum(duration), sysdate "+
					"FROM     tb_smart_daytime_app_wgt a, tb_smart_app_info b "+
					"WHERE    access_day = '"+accessday+"' "+
					"AND      a.smart_id = b.smart_id "+
					"and      b.ef_time  < to_date('"+accessday+"','yyyymmdd') "+
					"and      b.exp_time > to_date('"+accessday+"','yyyymmdd') "+
					"and      APP_CATEGORY_CD1 is not null "+
					"GROUP BY access_day, time_cd, panel_id, APP_CATEGORY_CD1 ";
			
			if(type.equals("2")){
				query = query.replaceAll("CD1", "CD2");
				query = query.replaceAll("APPLVL1", "APPLVL2");
			}
		} else {
			query = 
					"INSERT   INTO TB_SMART_DAYTIME_APPTOT_WGT "+
					"         (access_day, time_cd, panel_id, duration, proc_date)   "+
					"SELECT   /*+ index(a,PK_SMART_DAYTIME_APP_WGT) */ access_day, time_cd, panel_id, sum(duration), sysdate "+
					"FROM     tb_smart_daytime_app_wgt a "+
					"WHERE    access_day = '"+accessday+"' "+
					"GROUP BY access_day, time_cd, panel_id ";
		}
		
		//System.out.println(query);
		
		System.out.print("The batch - Daytime App Fact Level "+type+" is processing...");
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeTotDaytimeFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: TotDaytimeFact 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeTotDaytimeFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String table_name = findAppTablename(accessday);
		String query = "";
		
		if(countTable(accessday, "d", "tb_smart_daytime_tot_wgt")){
			query = 
					"insert into tb_smart_daytime_tot_wgt "+
					"select access_day, to_char(server_date,'HH24') time_cd, panel_id, count(server_date)*180 duration, sysdate "+
					"from ( "+
				    "select /*+ parallel(a, 8) */access_day, panel_id, server_date "+
				    "from "+table_name+" a, ( "+
				    "        select a.package_name package_name, b.package_name media "+
				    "        from tb_smart_app_info a, tb_smart_app_media_list b "+
				    "        where a.EXP_TIME(+) > to_date('"+accessday+"','yyyymmdd') "+
				    "        and b.EXP_TIME(+) > to_date('"+accessday+"','yyyymmdd') "+
				    "        and a.package_name = b.package_name(+) "+
				    "        ) b "+
				    "where a.package_name = b.package_name "+
				    "and ((screen = '1' and media is null) or "+
				    "     (media is not null)) "+
				    "and to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
				    "and RESULT_CD='Y' "+
				    "and access_day ='"+accessday+"' "+
				    "group by access_day, panel_id, server_date "+
					") "+
					"group by access_day, to_char(server_date,'HH24'), panel_id ";
		} else {
			System.out.print("DAYTIME_TOT_FACT already exists");
			System.exit(0);
		}
//		System.out.println(query);
//		System.exit(0);
		
		System.out.print("The batch - Daytime Total Fact is processing...");
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		return eachPt;
	}
	
	
	
	/**************************************************************************
	 *		메소드명		: executeFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Day_Fact 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeFact(String code, String period) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "";
		
		if(code.equalsIgnoreCase("d") && countTable(period, code, "TB_SMART_DAY_FACT")){
			query = "INSERT   INTO tb_smart_day_fact "+
					" (        access_day, site_id, panel_id,  "+
					"         category_code1, category_code2, category_code3, "+
					"         pv_cnt, visit_cnt, duration, proc_date "+
					" ) "+
					" SELECT   access_day, site_id, panel_id,  "+
					"         min(category_code1) category_code1, min(category_code2) category_code2, min(category_code3) category_code3, "+
					"         sum(pv_cnt) pv_cnt, sum(nvl(visit_cnt,0)) visit_cnt, sum(duration) duration, sysdate "+
					" FROM     tb_smart_daytime_fact "+
					" WHERE    access_day = '"+ period +"' "+
					" GROUP BY access_day, site_id, panel_id ";
			
			System.out.print("The batch - Day Fact is processing...");
		}
		else if (code.equalsIgnoreCase("w")){
			query = "insert into tb_smart_week_fact "+
					"SELECT   /*+use_hash(b,a) index(a,pk_smart_day_fact)*/  "+
					"         fn_weekcode('"+period+"'), A.site_id, A.panel_id, KC_SEG_ID segment_id, "+
					"         min(B.age_cls) age_cls, min(B.sex_cls) sex_cls, nvl(min(B.income_cls),0) income_cls, "+
					"         nvl(min(B.job_cls),0) job_cls, nvl(min(B.education_cls),0) education_cls, nvl(min(B.ismarried_cls),0) ismarried_cls, "+
					"         min(C.category_code1) category_code1, min(C.category_code2) category_code2, min(C.category_code3) category_code3, "+
					"         sum(A.pv_cnt) pv_cnt, sum(nvl(A.visit_cnt,0)) visit_cnt, sum(A.duration) duration, "+
					"         count(distinct A.access_day) daily_freq_cnt, sysdate "+
					"FROM     tb_smart_day_fact A, tb_smart_panel_seg B, tb_site_info C "+
					"WHERE    access_day >= to_char(to_date('"+period+"','yyyymmdd')-6,'yyyymmdd') " +
					"AND      access_day <= '"+period+"' "+
					"and      a.site_id = c.sitE_id "+
					"and      c.exp_time > to_date('"+period+"','yyyymmdd') + 1 "+
					"AND      c.ef_time < to_date('"+period+"','yyyymmdd') + 1 "+					
					"AND      A.panel_id = B.panel_id "+
					"AND      B.weekcode = fn_weekcode('"+period+"') "+
					"GROUP BY fn_weekcode(access_day), A.site_id, A.panel_id, KC_SEG_ID ";
			
			System.out.print("The batch - Week Fact is processing...");
		}
		else if (code.equalsIgnoreCase("m") && countTable(period, code, "TB_SMART_MONTH_FACT")){
			query = "insert into tb_smart_month_fact "+
					"SELECT   /*+use_hash(a,c) use_hash(b,a)  index(a,pk_smart_day_fact)*/ "+
					"         '"+period+"', A.site_id, A.panel_id, KC_SEG_ID segment_id, "+
					"         min(B.age_cls) age_cls, min(B.sex_cls) sex_cls, nvl(min(B.income_cls),0) income_cls, "+
					"         nvl(min(B.job_cls),0) job_cls, nvl(min(B.education_cls),0) education_cls, nvl(min(B.ismarried_cls),0) ismarried_cls, "+
					"         min(C.category_code1) category_code1, min(C.category_code2) category_code2, min(C.category_code3) category_code3, "+
					"         sum(A.pv_cnt) pv_cnt, sum(nvl(A.visit_cnt,0)) visit_cnt, sum(A.duration) duration, "+
					"         count(distinct A.access_day) daily_freq_cnt, sysdate "+
					"FROM     tb_smart_day_fact A, tb_smart_month_panel_seg B, tb_site_info C "+
					"WHERE    A.access_day like '"+period+"'||'%' "+
					"AND      A.panel_id = B.panel_id "+
					"and      a.site_id = c.sitE_id "+
					"and      c.exp_time > to_date(fn_month_lastday('"+period+"'),'yyyymmdd') + 1 "+
					"AND      c.ef_time < to_date(fn_month_lastday('"+period+"'),'yyyymmdd') + 1 "+
					"AND      B.monthcode = '"+period+"' "+
					"GROUP BY A.site_id, A.panel_id, KC_SEG_ID ";
			//System.out.println(query);
			System.out.print("The batch - Month Fact is processing...");
		} else {
			System.out.print("Daily/Month Fact already exists");
			System.exit(0);
		}
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();

		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthSegSite
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 브라우져 정보에서 site별 seg정보 Summary
	 *************************************************************************/

	public Calendar executeMonthSegSite(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Seg Site is processing...");
		String query = 
					"insert  into tb_smart_month_seg_site "+
					"( monthcode, segment_id, site_id, category_code1, category_code2, category_code3, "+
					"age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls, region_cd,  "+
					"uu_cnt, uu_est_cnt, pv_cnt, pv_est_cnt, avg_duration, duration_est, avg_daily_freq_cnt, avg_pv_est_cnt, reach_rate, visit_cnt, visit_est_cnt, proc_date ) "+
					"select  monthcode,  "+
					"        v_fact.segment_id,  "+
					"        v_fact.site_id,  "+
					"        min(category_code1) category_code1, "+
					"        min(category_code2) category_code2,  "+
					"        min(category_code3) category_code3, "+
					"        min(v_panel_seg.age_cls)        age_cls,  "+
					"        min(v_panel_seg.sex_cls)        sex_cls,  "+
					"        min(v_panel_seg.income_cls)     income_cls,  "+
					"        min(v_panel_seg.job_cls)        job_cls,  "+
					"        min(v_panel_seg.education_cls)  education_cls,  "+
					"        min(v_panel_seg.ismarried_cls ) ismarried_cls,  "+
					"        min(v_panel_seg.region_cd )     region_cd,  "+
					"        count(distinct v_fact.panel_id) uu_cnt, "+
					"        round(sum(v_panel_seg.mo_n_factor),5) uu_est_cnt, "+
					"        sum(pv_cnt) pv_cnt,  "+
					"        round(sum(pv_cnt*v_panel_seg.mo_p_factor),5) pv_est_cnt, "+
					"        round(decode(sum(mo_n_factor),0,0, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
					"        round(sum(duration*v_panel_seg.mo_p_factor),5) duration_est, "+
					"        round(decode(sum(mo_n_factor),0,0, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
					"        round(decode(sum(mo_n_factor),0,0, sum(pv_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
					"        round(sum(v_panel_seg.mo_n_factor)/ max(sum_kc_nfactor)*100, 5) reach_rate, "+
					"        sum(visit_cnt) visit_cnt, "+
					"        round(sum(visit_cnt*v_panel_seg.mo_p_factor),5) visit_est_cnt, "+
					"        sysdate "+
					"from  "+
					"( "+
					"        select  '"+monthcode+"' monthcode, segment_id, site_id, panel_id, pv_cnt, visit_cnt, daily_freq_cnt, duration, "+
					" 		 		 CATEGORY_CODE1, CATEGORY_CODE2, CATEGORY_CODE3 "+	
					"        from    tb_smart_month_fact "+
					"        where   monthcode = '"+monthcode+"' "+
					") v_fact, "+
					"( "+
					"        select  panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
					"        from    tb_smart_month_panel_seg "+
					"        where   monthcode = '"+monthcode+"' "+
					") v_panel_seg, "+
					"( "+
					"        select  sum(mo_n_factor) sum_kc_nfactor  "+
					"        from    tb_smart_month_panel_seg "+
					"        where   monthcode = '"+monthcode+"' "+
					") v_total_panel_seg "+
					"where v_fact.panel_id   = v_panel_seg.panel_id "+
					"group by v_fact.segment_id, v_fact.site_id ";
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthSegSite
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 브라우져 정보에서 site별 seg정보 Summary
	 *************************************************************************/

	public Calendar executeSessionWgt(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Session Wgt is processing...");
		String query = 
				"insert into tb_smart_month_session_wgt "+
				"select /*+use_hash(b,a) parallel(a,8)*/  "+
				"       substr(access_day,1,6) monthcode, panel_id,  "+
				"       decode(sex,'m','10','f','20') sex_cls, "+
				"       fn_kc_agecls2(FN_PANEL_AGE(SOCIALID)) age_cls, "+
				"       (SELECT code_etc FROM tb_codebook WHERE meta_code = 'ADDRESS' and substr(region_cd,1,2)||'00' = code) region_cd, "+
				"       count(distinct req_site_id) site_cnt, "+
				"       count(distinct access_day) DAILY_FREQ_CNT, "+
				"       count(*) pv_cnt, "+
				"       sum(duration) duration, "+
				"       sysdate PROC_DATE "+
				"from   tb_smart_browser_itrack a, tb_panel b "+
				"where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"and    result_cd = 'S' "+
				"and    panel_flag in ('D','N') "+
				"and    req_site_id <> 0 "+
				"and    a.panel_id = b.panelid "+
				"group by substr(access_day,1,6), panel_id, decode(sex,'m','10','f','20'), SOCIALID, region_cd ";
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeDaySiteSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 브라우져 정보에서 site별 Daily Summary
	 *************************************************************************/
	
	public Calendar executeDaySiteSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - DayTime Site Sum is processing...");
		String query = 
						"insert into tb_smart_daytime_site_sum "+
						"SELECT   A.access_day,  a.time_cd, A.site_id, "+
						"         min(category_code1), min(category_code2), min(category_code3), "+
						"         count(A.panel_id)/FN_SMART_DAY_COUNT('"+accessday+"')*100 reach_rate, "+
						"         count(A.panel_id) uu_cnt, "+
						"         rank() over (partition by a.time_cd order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
						"         rank() over (partition by a.time_cd, min(category_code2) order by sum(B.mo_n_factor) desc) uu_2level_rank, "+
						"         rank() over (partition by a.time_cd, min(category_code3) order by sum(B.mo_n_factor) desc) uu_3level_rank, "+
						"         sum(A.pv_cnt) pv_cnt, "+
						"         rank() over (partition by a.time_cd order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_overall_rank, "+
						"         rank() over (partition by a.time_cd, min(category_code2) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_2level_rank, "+
						"         rank() over (partition by a.time_cd, min(category_code3) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_3level_rank, "+
						"         round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
						"         rank() over (partition by a.time_cd order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
						"         rank() over (partition by a.time_cd, min(category_code2) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_2level_rank, "+
						"         rank() over (partition by a.time_cd, min(category_code3) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_3level_rank, "+
						"         round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
						"         round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
						"         sum(B.mo_n_factor)/fn_smart_day_nfactor('"+accessday+"')*100  reach_rate_adj, "+
						"         sum(A.visit_cnt) visit_cnt, "+
						"         round(sum(A.visit_cnt*B.mo_p_factor),5) visit_cnt_adj, "+
						"         rank() over (partition by a.time_cd order by sum(A.visit_cnt*B.mo_p_factor) desc) visit_overall_rank, "+
						"         rank() over (partition by a.time_cd order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank, "+
						"         round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
						"         fn_site_name(site_id), "+
						"         FN_CATEGORY_REF2_NAME2(min(category_code2)), "+
						"         sysdate "+
						"FROM "+
						"(        SELECT   /*+index(a,pk_smart_daytime_fact)*/ * "+
						"         FROM     tb_smart_daytime_fact a "+
						"         WHERE    access_day = '"+accessday+"' "+
						") A, "+
						"(        SELECT   access_day, panel_id, mo_n_factor, mo_p_factor "+
						"         FROM     tb_smart_day_panel_seg "+
						"         WHERE    access_day = '"+accessday+"' "+
						") B "+
						"WHERE    A.panel_id = B.panel_id "+
						"GROUP BY A.access_day, a.time_cd, A.site_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
		System.out.print("The batch - Day Site Sum is processing...");
		query = 
				"insert into tb_smart_day_site_sum "+
				"SELECT   A.access_day,  A.site_id, "+
				"         min(category_code1), min(category_code2), min(category_code3), "+
				"         count(A.panel_id)/FN_SMART_DAY_COUNT('"+accessday+"')*100 reach_rate, "+
				"         count(A.panel_id) uu_cnt, "+
				"         rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"         rank() over (partition by min(category_code2) order by sum(B.mo_n_factor) desc) uu_2level_rank, "+
				"         rank() over (partition by min(category_code3) order by sum(B.mo_n_factor) desc) uu_3level_rank, "+
				"         sum(A.pv_cnt) pv_cnt, "+
				"         rank() over (order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_overall_rank, "+
				"         rank() over (partition by min(category_code2) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_2level_rank, "+
				"         rank() over (partition by min(category_code3) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_3level_rank, "+
				"         round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"         rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"         rank() over (partition by min(category_code2) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_2level_rank, "+
				"         rank() over (partition by min(category_code3) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_3level_rank, "+
				"         round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"         round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"         sum(B.mo_n_factor)/fn_smart_day_nfactor('"+accessday+"')*100 reach_rate_adj, "+
				"         sum(A.visit_cnt) visit_cnt, "+
				"         round(sum(A.visit_cnt*B.mo_p_factor),5) visit_cnt_adj, "+
				"         rank() over (order by sum(A.visit_cnt*B.mo_p_factor) desc) visit_overall_rank, "+
				"         rank() over (order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
				"         round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"         fn_site_name(site_id), "+
				"         FN_CATEGORY_REF2_NAME2(min(category_code2)), "+
				"         sysdate, "+
				"         null, null "+
				"FROM "+
				"(        SELECT   * "+
				"         FROM     tb_smart_day_fact "+
				"         WHERE    access_day = '"+accessday+"' "+
				") A, "+
				"(        SELECT   access_day, panel_id, mo_n_factor, mo_p_factor "+ 
				"         FROM     tb_smart_day_panel_seg "+ 
				"         WHERE    access_day = '"+accessday+"' "+ 
				") B "+ 
				"WHERE    A.panel_id = B.panel_id "+
				"GROUP BY A.access_day, A.site_id ";
		
//		System.out.println(query);
//		System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeDayBounceRate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 웹 반송률
	 *************************************************************************/
	
	public Calendar executeDayBounceRate(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Bounce Rate is processing...");
		String query = "truncate table tb_temp_day_bounce_fact";
						
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
		query = "insert into tb_temp_day_bounce_fact "+
				"select a_seq,  b_seq, req_date, a.panel_id, a.site_id, a.rank, hour, minute, second, "+
				"       case when a.rank = 1 and b_seq is null then -1 "+
				"            when a.rank = 1 and (a.hour >=1 or (a.minute >= 30 and a.second >= 1)) then -11 "+
				"            when a.rank <> 1 and (a.hour >=1 or (a.minute >= 30 and a.second >= 1)) then b_seq "+
				"       else -2 end flag "+
				"from "+
				"( "+
				"    select "+
				"            a.seq a_seq, "+
				"            lead(a.seq,1) over (partition by site_id,panel_id order by req_date)  b_seq, "+
				"            req_date, "+
				"            a.panel_id, a.site_id, a.rank, "+
				"            abs(TRUNC(MOD((a.req_date-a.req_date2),1)*24)) hour, "+
				"            abs(TRUNC(MOD((a.req_date - a.req_date2)*24,1)*60)) minute, "+
				"            abs(TRUNC(MOD((a.req_date-a.req_date2)*24*60,1)*60)) second "+
				"    from "+
				"    ( "+
				"        select  "+
				"               rownum seq, PANEL_ID,SITE_ID, req_date , req_date2,RANK "+
				"        from "+
				"        ( "+
				"           select   /*+parallel(a,8)*/ "+
				"                    panel_id, req_site_id site_id, req_date, "+
				"                    lead(a.req_date,1) over (partition by req_site_id,panel_id order by req_date) req_date2, "+
				"                    row_number() over (partition by panel_id, req_site_id order by req_date) rank "+
				"           from     tb_smart_browser_itrack a "+
				"           where    a.access_day  = '"+accessday+"' "+
				"           and panel_flag in ('D','V') "+
				"           and result_cd = 'S' "+
				"           and duration > 0 "+
				"           order by panel_id, site_id, req_date, rank "+
				"        ) "+
				"    ) a "+
				") a";
					
		
//		System.out.println(query);
//		System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		query = "insert into TB_SMART_DAY_BOUNCERATE_FACT "+
		        "select '"+accessday+"' access_day, A_SEQ, B_SEQ, SERVER_DATE, PANEL_ID,SITE_ID,RANK,HOUR,MINUTE,FLAG,SECOND "+
		        "from "+
		        "( "+
		        "    select A_SEQ, B_SEQ, SERVER_DATE, PANEL_ID,SITE_ID,RANK,HOUR,MINUTE,FLAG,SECOND /*1번본 페이지와 첫페이지가 bounce 인것.*/ "+
		        "    from tb_temp_day_bounce_fact  "+
		        "    where flag in (-1,-11) "+
		        "    union all "+
		        "    select A_SEQ, B_SEQ, SERVER_DATE, PANEL_ID,SITE_ID,RANK,HOUR,MINUTE,FLAG,SECOND /*30분후에 접속후 닫음*/ "+
		        "    from tb_temp_day_bounce_fact "+
		        "    where a_seq in ( "+
		        "                        select flag "+
		        "                        from   tb_temp_day_bounce_fact "+
		        "                        where  hour >=1 or (minute >= 30 and second >= 1) "+
		        "                    ) "+
		        "    and   b_seq is null "+
		        "    union all "+
		        "    select A_SEQ, B_SEQ, SERVER_DATE, PANEL_ID,SITE_ID,RANK,HOUR,MINUTE,FLAG,SECOND /*30분후에 접속후 30분후에 후속로그가 온것*/ "+
		        "    from tb_temp_day_bounce_fact "+
		        "    where a_seq in ( "+
		        "                        select flag "+
		        "                        from   tb_temp_day_bounce_fact "+
		        "                        where  hour >=1 or (minute >= 30 and second >= 1) "+
		        "                   ) "+
		        "    and   (hour >=1 or (minute >= 30 and second >= 1)) "+
		        "    union all "+
		        "    select A_SEQ, B_SEQ, SERVER_DATE, PANEL_ID,SITE_ID,RANK,HOUR,MINUTE,FLAG,SECOND /*마지막 로그가 2번째이고 첫로그와 30분간격인것*/ "+
		        "    from tb_temp_day_bounce_fact "+
		        "    where a_seq in ( "+
		        "                        select a_seq+1 "+
		        "                        from   tb_temp_day_bounce_fact "+
		        "                        where  flag=-11 "+
		        "                    ) "+
		        "    and   b_seq is null "+
		        "    union all "+
		        "    select A_SEQ, B_SEQ, SERVER_DATE, PANEL_ID,SITE_ID,RANK,HOUR,MINUTE,FLAG,SECOND /*2번째로그가 와 3번째로그사이가 30분간격인것*/ "+
		        "    from tb_temp_day_bounce_fact "+
		        "    where a_seq in ( "+
		        "                        select a_seq+1 "+
		        "                        from   tb_temp_day_bounce_fact "+
		        "                        where  flag=-11 "+
		        "                    ) "+
		        "    and   (hour >=1 or (minute >= 30 and second >= 1)) "+
		        ") "+
		        "group by A_SEQ, B_SEQ, SERVER_DATE, PANEL_ID,SITE_ID,RANK,HOUR,MINUTE,FLAG,SECOND ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("DONE.");
		return eachPt;
	}
		
	
	
	
	/**************************************************************************
	 *		메소드명		: executeDaySiteSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 브라우져 정보에서 site별 Daily Summary
	 *************************************************************************/
	
	public Calendar executeTarItrack(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch -TAR insert is processing...");
		String query = "insert into tb_smart_tar_itrack "+
						"select * "+
						"from tb_smart_browser_itrack "+
						"where access_day = '"+accessday+"' "+
						"and REQ_DOMAIN in ('http://smr.tvpot.daum.net','http://advod.smartmediarep.com') "+
						"and panel_flag in ('D','N') "+
						"union all "+
						"select * "+
						"from tb_smart_browser_itrack "+
						"where access_day = '"+accessday+"' "+
						"and REQ_DOMAIN like 'http://%.midas-i.com'  "+
						"and req_page like 'ad3%' "+
						"and panel_flag in ('D','N') "+
						"union all "+
						"select * "+
						"from tb_smart_browser_itrack "+
						"where access_day = '"+accessday+"' "+
						"and REQ_DOMAIN = 'http://damovie.dn.naver.com' "+
						"and req_page like 'ad3%' "+
						"and panel_flag in ('D','N') "+
						"union all "+
						"select * "+
						"from tb_smart_browser_itrack "+
						"where access_day = '"+accessday+"' "+
						"and REQ_DOMAIN = 'http://videofarm.daum.net' "+
						"and req_page = 'controller/api/open/v1_0/MobileVideo.action' "+
						"and req_parameter like '%play_loc=daum_player_ad%' "+
						"and panel_flag in ('D','N')";

		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("DONE.");
		return eachPt;
	}
	
	
	

	/**************************************************************************
	 *		메소드명		: executeWeekSiteSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSiteSum 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekSiteSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Week Summary is processing...");
		String query = 
				"insert into tb_smart_week_site_sum "+
				"SELECT   /*+use_hash(b,a)*/ A.weekcode,  A.site_id, "+
				"         min(category_code1), min(category_code2), min(category_code3), "+
				"         round(count(A.panel_id)/FN_SMART_WEEK_COUNT(A.weekcode)*100,2) reach_rate, "+
				"         count(A.panel_id) uu_cnt, "+
				"         rank() over (partition by a.weekcode order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"         rank() over (partition by a.weekcode, min(category_code1) order by sum(B.mo_n_factor) desc) uu_1level_rank, "+
				"         rank() over (partition by a.weekcode, min(category_code2) order by sum(B.mo_n_factor) desc) uu_2level_rank, "+
				"         sum(A.pv_cnt) pv_cnt, "+
				"         rank() over (partition by a.weekcode order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_overall_rank, "+
				"         rank() over (partition by a.weekcode, min(category_code1) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_1level_rank, "+
				"         rank() over (partition by a.weekcode, min(category_code2) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_2level_rank, "+
				"         round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"         rank() over (partition by a.weekcode order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"         rank() over (partition by a.weekcode, min(category_code1) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_1level_rank, "+
				"         rank() over (partition by a.weekcode, min(category_code2) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_2level_rank, "+
				"         round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"         round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"         round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"         round(sum(B.mo_n_factor)/FN_SMART_WEEK_NFACTOR(A.weekcode)*100,5) reach_rate_adj, "+
				"         sum(A.visit_cnt) visit_cnt, "+
				"         round(sum(A.visit_cnt*B.mo_p_factor),5) visit_cnt_adj, "+
				"         sysdate, "+
				"         rank() over (partition by a.weekcode order by sum(A.visit_cnt*B.mo_p_factor) desc) visit_overall_rank, "+
				"         rank() over (partition by a.weekcode order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"         rank() over (partition by a.weekcode order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
				"         round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"         fn_site_name(site_id), "+
				"         null,null,null," +
				"		  FN_CATEGORY_REF2_NAME2(min(category_code2)), null "+
				"FROM "+
				"(        SELECT   * "+
				"         FROM     tb_smart_week_fact "+
				"         WHERE    weekcode = fn_weekcode('"+accessday+"') "+
				"		  AND      CATEGORY_CODE1 != 'Z'"+
				") A, "+
				"(        SELECT   weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"         FROM     tb_smart_panel_seg "+
				"         WHERE    weekcode = fn_weekcode('"+accessday+"') "+
				") B "+
				"WHERE    A.panel_id = B.panel_id " +
				"and      A.weekcode = B.weekcode "+
				"GROUP BY A.weekcode, A.site_id";
		//System.out.println(query);
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		query = "TRUNCATE TABLE tb_smart_temp_day_acc_fact";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
		query = "INSERT INTO tb_smart_temp_day_acc_fact "+
				"SELECT   c.access_day, a.site_id, sum(mo_n_factor) acc_uv "+
				"from "+
				"( "+
				"    select b.access_day,a.site_id,panel_id "+
				"    FROM "+
				"    ( "+
				"        SELECT min(access_day) access_day, site_id, PANEL_ID "+
				"        FROM   tb_smart_day_fact "+
				"        WHERE  access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"        AND    access_day <= '"+accessday+"' "+
				"        GROUP BY site_id, PANEL_ID "+
				"    ) a, "+
				"    ( "+
				"        SELECT access_day, site_id "+
				"        FROM   tb_smart_day_site_sum "+
				"        WHERE  access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"        AND    access_day <= '"+accessday+"' "+
				"    ) b "+
				"    where  a.access_day <= b.access_day "+
				"    AND    a.site_id = b.site_id "+
				"    group by b.access_day,a.site_id,panel_id "+
				") a, "+
				"( "+
				"    SELECT access_day, panel_id, mo_n_factor "+
				"    FROM   tb_smart_day_panel_seg "+
				"    WHERE  access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"    AND    access_day <= '"+accessday+"' "+
				") c "+
				"WHERE  a.access_day = c.access_day "+
				"AND    a.panel_id = c.panel_id "+
				"GROUP BY c.access_day, a.site_id";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
		query = "UPDATE tb_smart_day_site_sum a set week_acc_uv =(SELECT acc_uv "+
				"                                           FROM   tb_smart_temp_day_acc_fact "+
				"                                           WHERE  access_day = a.access_day "+
				"                                           AND    site_id = a.site_id) "+
				"WHERE  access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"AND    access_day <= '"+accessday+"' ";

        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
		query = "UPDATE tb_smart_day_site_sum@kcred3 a set week_acc_uv =(SELECT acc_uv "+
				"                                           FROM   tb_smart_temp_day_acc_fact "+
				"                                           WHERE  access_day = a.access_day "+
				"                                           AND    site_id = a.site_id) "+
				"WHERE  access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"AND    access_day <= '"+accessday+"' ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekPersonSeg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekPersonSeg 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekBounceRate(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Week Person Segment is processing...");
		String query = "";
		query = "truncate table tb_temp_week_bounce_sum";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		query =	"insert into tb_temp_week_bounce_sum "+
				"select /*+use_hash(b,a)*/ "+
				"         fn_weekcode('"+accessday+"') weekcode, b.site_id, "+
				"         case when round((a.visit_cnt/b.visit_cnt)*100,2) > 100 then 100 "+
				"         else round((a.visit_cnt/b.visit_cnt)*100,2) "+
				"         end bouncerate "+
				"from "+
				"( "+
				"    select site_id, count(*) visit_cnt "+
				"    from "+
				"    ( "+
				"        select   site_id, panel_id "+
				"        from     TB_SMART_DAY_BOUNCERATE_FACT "+
				"        where    access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and access_day <= '"+accessday+"' "+
				"    ) a, "+
				"    ( "+
				"        select panel_id "+
				"        from   tb_smart_panel_seg "+
				"        where  weekcode = fn_weekcode('"+accessday+"') "+
				"    )b "+
				"    where a.panel_id=b.panel_id "+
				"    group by site_id "+
				") a, "+
				"( "+
				"    select site_id, sum(visit_cnt) visit_cnt "+
				"    from "+
				"    ( "+
				"        select req_site_id site_id, "+
				"                case when round((REQ_DATE - lag(REQ_DATE, 1) "+
				"                over (partition by req_site_id, panel_id order by REQ_DATE))*60* 60*24) > 60*30 "+
				"                or lag(REQ_DATE, 1) over (partition by req_site_id, panel_id order by REQ_DATE) is NULL then 1  end visit_cnt  "+
				"        from "+
				"        ( "+
				"           select req_site_id, a.panel_id, to_date(REQ_DATE,'yyyy/mm/dd hh24:mi:ss') REQ_DATE "+
				"           from "+
				"           ( "+
				"               select /*+parallel(a,8)*/ req_site_id, panel_id, REQ_DATE "+
				"               from   tb_smart_browser_itrack a "+
				"               where  access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') and access_day <= '"+accessday+"' "+
				"               AND     result_cd = 'S' "+
				"               AND     duration > 0 "+
				"               AND     panel_flag in ('D','V') "+
				"               AND     (req_site_id != 15537 or req_site_id > 0) "+
				"           ) a, "+
				"           ( "+
				"               select panel_id "+
				"               from tb_smart_panel_seg "+
				"               where weekcode = fn_weekcode('"+accessday+"') "+
				"           ) b "+
				"           where a.panel_id = b.panel_id "+
				"        ) "+
				"     ) "+
				"    group by site_id "+
				") b "+
				"where a.site_id = b.site_id "+
				"group by b.site_id,a.visit_cnt, b.visit_cnt ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		query = "update tb_smart_week_site_sum a "+
				"set bouncerate = ( "+
				"            select bouncerate  "+
				"            from tb_temp_week_bounce_sum b "+
				"            where a.weekcode = b.weekcode "+
				"            and   a.site_id = b.site_id "+
				"          ) "+
				"where weekcode = fn_weekcode('"+accessday+"') ";

        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
		System.out.println("Update DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekPersonSeg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekPersonSeg 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekPersonSeg(String filtername, String weekcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Week Person Segment is processing...");
		String query = "";
		query = 
				"insert into TB_SMART_WEEK_PERSON_SEG "+ 
				"select weekCODE, PANEL_ID, A.LOC_CD, NETIZEN_CNT, PANEL_CNT, PERSON, A.SEX_CLS, A.AGE_CLS, P_PERSON, A.REGION_CD "+ 
				"from ( "+ 
				"    select location LOC_CD, AGE_CLS, SEX_CLS, REGION_CD, NETIZEN_CNT "+ 
				"    from tb_nielsen_netizen "+ 
				"    where EXP_TIME > to_date('"+filtername+"','yyyymmdd') "+ 
				"    and   ef_time < to_Date('"+filtername+"','yyyymmdd') "+ 
				"    and   mobile_cd = '10' "+ 
				") a, "+ 
				"( "+ 
				"    select weekCODE, a.PANEL_ID, LOC_CD, b.SEX_CLS, b.AGE_CLS, b.REGION_CD, "+ 
				"           ( "+ 
				"                select count(d.panel_id) "+ 
				"                from   tb_temp_week_person_seg c, tb_smart_panel_seg d "+ 
				"                where access_day = '"+filtername+"' "+ 
				"                and   weekcode = fn_weekcode('"+filtername+"') "+ 
				"                and   c.panel_id = d.panel_id "+ 
				"                and   a.loc_cd = c.loc_cd "+ 
				"                and   b.sex_cls = d.sex_cls "+ 
				"                and   b.age_cls = d.age_cls "+ 
				"                and   b.region_cd = d.region_cd "+ 
				"           ) panel_cnt, MO_N_FACTOR PERSON, MO_P_FACTOR P_PERSON "+ 
				"    from  tb_temp_week_person_seg a, tb_smart_panel_seg b "+ 
				"    where access_day = '"+filtername+"' "+ 
				"    and   weekcode =  '"+weekcode+"' "+ 
				"    and   a.panel_id = b.panel_id "+ 
				") b "+ 
				"where a.loc_cd = b.loc_cd "+ 
				"and   a.age_cls = b.age_cls "+ 
				"and   a.sex_cls = b.sex_cls "+ 
				"and   a.region_cd = b.region_cd ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	public Calendar executeWeekSiteSumError(String weekcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Site Summary Error is processing...");
		
		String queryT = "TRUNCATE TABLE tb_temp_error ";
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		String query = 
				"INSERT INTO tb_temp_error (code,site_id,rr_error) "+ 
				"SELECT weekcode, site_id, "+ 
				"       round(fn_week_modifier(weekcode)*1.96*sqrt((1/power(sum(netizen_cnt),2))*sum(UV_VAR)),4) Reach_E "+ 
				"FROM "+ 
				"    ( "+ 
				"    SELECT '"+weekcode+"' weekcode, site_id, "+ 
				"           loc_cd, region_cd, sex_cls, age_cls, "+ 
				"           netizen_cnt, "+ 
				"           netizen_cnt*rr UV_S, "+ 
				"           power(netizen_cnt,2)*((netizen_cnt-panel_cnt)/netizen_cnt)*(rr*(1-rr)/(decode(panel_cnt,1,1.000000000000001,panel_cnt)-1)) UV_VAR "+ 
				"    FROM ( "+ 
				"          SELECT a.site_id, a.loc_cd, a.region_cd, a.sex_cls, a.age_cls, panel_cnt, netizen_cnt, decode(cnt/panel_cnt,null,0,cnt/panel_cnt) rr "+ 
				"          FROM "+ 
				"            ( "+ 
				"                SELECT /*+use_hash(a,b) index(a,PK_SMART_WEEK_PERSON_SEG)*/ "+ 
				"                       site_id, loc_cd, region_cd, sex_cls, age_cls, max(panel_cnt) panel_cnt, max(netizen_cnt) netizen_cnt "+ 
				"                FROM   tb_smart_week_person_seg a, tb_smart_week_site_sum b "+ 
				"                WHERE  a.weekcode = '"+weekcode+"' "+ 
				"                AND    a.weekcode = b.weekcode "+ 
				"                AND    b.uu_overall_rank <= 100 "+ 
				"                GROUP BY site_id, loc_cd, region_cd, sex_cls, age_cls "+ 
				"            ) a, "+ 
				"            ( "+ 
				"                SELECT /*+use_hash(b,a) index(b,PK_SMART_WEEK_PERSON_SEG)*/ "+ 
				"                       a.weekcode, site_id, b.loc_cd, b.region_cd, b.sex_cls, b.age_cls, count(*) cnt "+ 
				"                FROM   tb_smart_week_fact a, tb_smart_week_person_seg b "+ 
				"                WHERE  a.weekcode = '"+weekcode+"' "+ 
				"                AND    a.weekcode = b.weekcode "+ 
				"                AND    a.panel_id = b.panel_id "+ 
				"                GROUP BY a.weekcode, site_id, b.loc_cd, b.region_cd, b.sex_cls, b.age_cls "+ 
				"            ) b "+ 
				"         WHERE a.age_cls=b.age_cls(+) "+ 
				"         AND   a.sex_cls=b.sex_cls(+) "+ 
				"         AND   a.loc_cd=b.loc_cd(+) "+ 
				"         AND   a.region_cd=b.region_cd(+) "+ 
				"         AND   a.site_id = b.site_id(+) "+ 
				"        ) "+ 
				"      ) "+ 
				"GROUP BY weekcode, site_id ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String queryU = "UPDATE tb_smart_week_site_sum a "+
						"SET    rr_error = (select rr_error from tb_temp_error b where a.site_id = b.site_id and a.weekcode = b.code) "+
						"WHERE  weekcode = '"+weekcode+"' "+
						"AND    site_id in (select site_id from tb_smart_week_site_sum where weekcode = '"+weekcode+"'  and uu_overall_rank <= 100) ";
        this.pstmt = connection.prepareStatement(queryU);
		this.pstmt.executeUpdate();
		
		this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		String query2 = 
				"INSERT INTO tb_temp_error (code,site_id,tts_error,pv_error) "+
				"SELECT  weekcode, site_id, "+
				"        round(1.96*sqrt(sum(D_var))/60,2) TTS_E, "+
				"        round(1.96*sqrt(sum(P_var)),2) PV_E "+
				"FROM ( "+
				"    SELECT b.weekcode weekcode, "+
				"           site_id, "+
				"           sex_cls, age_cls, loc_cd, region_cd, "+
				"           max(netizen_cnt) netizen_cnt, "+
				"           sum(kc_n_factor) kc_n_factor_S, "+
				"           sum(duration*kc_p_factor) duration_s, sum(pv_cnt*kc_p_factor) pv_cnt_s, "+
				"           ((sum(kc_n_factor)*(sum(kc_n_factor)-count(a.panel_id)))/count(a.panel_id)*((sum(power(duration,2))-power(sum(duration),2)/count(a.panel_id))/(decode(count(a.panel_id),1,1.000000001,count(a.panel_id))-1))) d_var, "+
				"           ((sum(kc_n_factor)*(sum(kc_n_factor)-count(a.panel_id)))/count(a.panel_id)*((sum(power(pv_cnt,2))-power(sum(pv_cnt),2)/count(a.panel_id))/(decode(count(a.panel_id),1,1.000000001,count(a.panel_id))-1))) p_var "+
				"    FROM    ( "+
				"                SELECT site_id, panel_id, duration, pv_cnt "+
				"                FROM   tb_smart_week_fact "+
				"                WHERE  weekcode = '"+weekcode+"' "+
				"                AND    site_id in ( "+
				"                    SELECT site_id "+
				"                    FROM   tb_smart_week_site_sum "+
				"                    WHERE  weekcode = '"+weekcode+"' "+
				"                    AND    uu_overall_rank <= 100 "+
				"                    AND    category_code1 <> 'Z' "+
				"                ) "+
				"            ) a, "+
				"            ( "+
				"                SELECT b.weekcode, a.panel_id, "+
				"                       a.SEX_CLS, a.AGE_CLS, a.LOC_CD, a.REGION_CD, P_person kc_p_factor,person kc_n_factor, netizen_cnt, panel_cnt "+
				"                FROM   tb_smart_week_person_seg a, tb_smart_panel_seg b "+
				"                WHERE  a.weekcode = b.weekcode "+
				"                AND    b.weekcode = '"+weekcode+"' "+
				"                AND    a.panel_id = b.panel_id "+
				"            ) b "+
				"    WHERE    b.panel_id=a.panel_id "+
				"    GROUP BY b.weekcode, a.site_id, sex_cls, age_cls, loc_cd, region_cd "+
				"    ) "+
				"GROUP BY weekcode, site_id ";
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		String queryU2 ="UPDATE tb_smart_week_site_sum a "+
						"SET    tts_error = (SELECT tts_error from tb_temp_error b WHERE a.site_id = b.site_id AND a.weekcode = b.code), "+
						"       pv_error = (select pv_error from tb_temp_error b WHERE a.site_id = b.site_id AND a.weekcode = b.code) "+
						"WHERE  weekcode = '"+weekcode+"' "+
						"AND    site_id in (SELECT site_id from tb_smart_week_site_sum WHERE weekcode = '"+weekcode+"'  AND uu_overall_rank <= 100) ";
		this.pstmt = connection.prepareStatement(queryU2);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}	
	
	/**************************************************************************
	 *		메소드명		: executeMonthSiteSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthSiteSum 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthSiteSum(String monthcode, String lastday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Summary is processing...");
		String query = "";
		query = 
				"insert into tb_smart_month_site_sum "+
				"SELECT   A.monthcode,  A.site_id, "+
				"         min(category_code1), min(category_code2), min(category_code3), "+
				"         round(count(A.panel_id)/FN_SMART_MONTH_COUNT('"+monthcode+"')*100,2) reach_rate, "+
				"         count(A.panel_id) uu_cnt, "+
				"         rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"         rank() over (partition by min(category_code1) order by sum(B.mo_n_factor) desc) uu_1level_rank, "+
				"         rank() over (partition by min(category_code2) order by sum(B.mo_n_factor) desc) uu_2level_rank, "+
				"         sum(A.pv_cnt) pv_cnt, "+
				"         rank() over (order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_overall_rank, "+
				"         rank() over (partition by min(category_code1) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_1level_rank, "+
				"         rank() over (partition by min(category_code2) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_2level_rank, "+
				"         round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"         rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"         rank() over (partition by min(category_code1) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_1level_rank, "+
				"         rank() over (partition by min(category_code2) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_2level_rank, "+
				"         round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"         round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"         round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"         round(sum(B.mo_n_factor)/FN_SMART_MONTH_NFACTOR('"+monthcode+"')*100,5) reach_rate_adj, "+
				"         sum(A.visit_cnt) visit_cnt, "+
				"         round(sum(A.visit_cnt*B.mo_p_factor),5) visit_cnt_adj, "+
				"         sysdate, "+
				"         rank() over (order by sum(A.visit_cnt*B.mo_p_factor) desc) visit_overall_rank, "+
				"         rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"         rank() over (order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
				"         round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"         fn_site_name(site_id), "+
				"         FN_CATEGORY_REF2_NAME2(min(category_code2)),null,null,null, null "+
				"FROM "+
				"(        SELECT   * "+
				"         FROM     tb_smart_month_fact "+
				"         WHERE    monthcode = '"+monthcode+"' "+
				"		  AND      CATEGORY_CODE1 != 'Z'"+
				") A, "+
				"(        SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"         FROM     tb_smart_month_panel_seg "+
				"         WHERE    monthcode = '"+monthcode+"' "+
				") B "+
				"WHERE    A.panel_id = B.panel_id "+
				"GROUP BY A.monthcode, A.site_id ";
//		System.out.println(query);
//		System.exit(0);
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		
		query = "TRUNCATE TABLE tb_smart_temp_day_acc_fact";

        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		query = "INSERT INTO tb_smart_temp_day_acc_fact "+
				"SELECT   c.access_day, a.site_id, sum(mo_n_factor) acc_uv "+
				"from "+
				"( "+
				"    select b.access_day,a.site_id,panel_id "+
				"    FROM "+ 
				"    ( "+
				"        SELECT min(access_day) access_day, site_id, PANEL_ID "+
				"        FROM   tb_smart_day_fact "+
				"        WHERE  access_day >= '"+monthcode+"'||'01' "+
				"        AND    access_day <= '"+monthcode+"' "+
				"        GROUP BY site_id, PANEL_ID "+
				"    ) a, "+
				"    ( "+
				"        SELECT access_day, site_id "+
				"        FROM   tb_smart_day_site_sum "+
				"        WHERE  access_day >= '"+monthcode+"'||'01' "+
				"        AND    access_day <= fn_month_lastday('"+monthcode+"') "+
				"    ) b "+
				"    where  a.access_day <= b.access_day "+
				"    AND    a.site_id = b.site_id "+
				"    group by b.access_day,a.site_id,panel_id "+
				") a, "+
				"( "+
				"    SELECT access_day, panel_id, mo_n_factor "+
				"    FROM   tb_smart_day_panel_seg "+
				"    WHERE  access_day >= '"+monthcode+"'||'01' "+
				"    AND    access_day <= fn_month_lastday('"+monthcode+"') "+
				") c "+
				"WHERE  a.access_day = c.access_day "+
				"AND    a.panel_id = c.panel_id "+
				"GROUP BY c.access_day, a.site_id";

        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		query = "UPDATE tb_smart_day_site_sum a set month_acc_uv = (SELECT acc_uv "+
				"                                             FROM   tb_smart_temp_day_acc_fact "+
				"                                             WHERE  access_day = a.access_day "+
				"                                             AND    site_id = a.site_id) "+
				"WHERE  access_day >= '"+monthcode+"'||'01' "+
				"AND    access_day <= '"+lastday+"'";


        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		query = "UPDATE tb_smart_day_site_sum@kcred3 a set month_acc_uv = (SELECT acc_uv "+
				"			                                            FROM   tb_smart_temp_day_acc_fact "+
				"			                                            WHERE  access_day = a.access_day "+
				"			                                            AND    site_id = a.site_id) "+
				"WHERE  access_day >= '"+monthcode+"'||'01' "+
				"AND    access_day <= '"+lastday+"'";

        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthBounceRate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Monthly Bounce Rate Update 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthBounceRate(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month BounceRate is processing...");
		String query = "";
		query = 
				"truncate table tb_temp_month_bounce_sum ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		query = 
				"insert into tb_temp_month_bounce_sum "+
				"select /*+use_hash(b,a)*/ "+
				"         '"+monthcode+"' monthcode, b.site_id, "+
				"         case when round((a.visit_cnt/b.visit_cnt)*100,2) > 100 then 100  "+
				"         else round((a.visit_cnt/b.visit_cnt)*100,2)  "+
				"         end bouncerate "+
				"from "+ 
				"( "+
				"    select /*+use_hash(b,a)*/site_id, count(*) visit_cnt "+
				"    from "+
				"    ( "+
				"        select   access_day, site_id, panel_id "+
				"        from     tb_smart_day_bouncerate_fact "+
				"        where    access_day >= '"+monthcode+"'||'01' and access_day <= fn_month_lastday('"+monthcode+"') "+
				"    ) a, "+
				"    ( "+
				"        select panel_id "+
				"        from   tb_smart_month_panel_seg "+
				"        where  monthcode = '"+monthcode+"' "+
				"    ) b "+
				"    where a.panel_id = b.panel_id "+
				"    group by site_id "+
				") a, "+
				"( "+
				"    select site_id, sum(visit_cnt) visit_cnt "+
				"    from "+ 
				"    ( "+
				"                select req_site_id site_id, "+
				"                case when round((REQ_DATE - lag(REQ_DATE, 1) "+
				"                over (partition by req_site_id, panel_id order by REQ_DATE))*60* 60*24) > 60*30 "+
				"                or lag(REQ_DATE, 1) over (partition by req_site_id, panel_id order by REQ_DATE) is NULL then 1  end visit_cnt  "+
				"        from "+
				"        ( "+
				"            select req_site_id, a.panel_id, to_date(REQ_DATE,'yyyy/mm/dd hh24:mi:ss') REQ_DATE "+
				"            from "+
				"            ( "+
				"                select /*+parallel(a,8)*/ req_site_id, panel_id, REQ_DATE "+
				"                from   tb_smart_browser_itrack a "+
				"                where  access_day >= '"+monthcode+"'||'01' and access_day <= fn_month_lastday('"+monthcode+"') "+
				"                AND     result_cd = 'S' "+
				"                AND     duration > 0 "+
				"                AND     panel_flag in ('D','V') "+
				"                AND     (req_site_id != 15537 or req_site_id > 0) "+
				"            ) a, "+
				"            ( "+
				"                select panel_id "+
				"                from tb_smarT_month_panel_seg "+
				"                where monthcode = '"+monthcode+"' "+
				"            ) b "+
				"            where a.panel_id = b.panel_id "+
				"        ) "+
				"    ) "+
				"    group by site_id "+
				") b "+
				"where a.site_id = b.site_id "+
				"group by b.site_id,a.visit_cnt, b.visit_cnt";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
		query = 
				"update tb_smart_month_site_sum a "+
				"set bouncerate = ( "+
				"                    select bouncerate "+
				"                    from tb_temp_month_bounce_sum b "+
				"                    where a.monthcode = b.monthcode "+
				"                    and   a.site_id = b.site_id "+
				"                  ) "+
				"where monthcode = '"+monthcode+"' ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();		
		
//		System.out.println(query);
		System.out.println("Update DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthPersonSeg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Monthly Person Seg 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthPersonSeg(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Person Segment is processing...");
		String query = "";
		query = 
				"insert into TB_SMART_MONTH_PERSON_SEG "+
				"select MONTHCODE, PANEL_ID, A.LOC_CD, NETIZEN_CNT, PANEL_CNT, PERSON, A.SEX_CLS, A.AGE_CLS, P_PERSON, A.REGION_CD "+
				"from ( "+
				"    select location LOC_CD, AGE_CLS, SEX_CLS, REGION_CD, NETIZEN_CNT "+
				"    from tb_nielsen_netizen "+
				"    where EXP_TIME > sysdate "+
				"    and   mobile_cd = '10' "+
				") a, "+
				"( "+
				"    select MONTHCODE, a.PANEL_ID, LOC_CD, b.SEX_CLS, b.AGE_CLS, b.REGION_CD, "+
				"           ( "+
				"                select count(d.panel_id) "+
				"                from   tb_temp_day_person_seg c, tb_smart_month_panel_seg d "+
				"                where access_day = fn_month_lastday('"+monthcode+"') "+
				"                and   monthcode = substr(c.access_day,1,6) "+
				"                and   c.panel_id = d.panel_id "+
				"                and   a.loc_cd = c.loc_cd "+
				"                and   b.sex_cls = d.sex_cls "+
				"                and   b.age_cls = d.age_cls "+
				"                and   b.region_cd = d.region_cd "+
				"           ) panel_cnt, MO_N_FACTOR PERSON, MO_P_FACTOR P_PERSON "+
				"    from  tb_temp_day_person_seg a, tb_smart_month_panel_seg b "+
				"    where access_day = fn_month_lastday('"+monthcode+"') "+
				"    and   monthcode = substr(a.access_day,1,6) "+
				"    and   a.panel_id = b.panel_id "+
				") b "+
				"where a.loc_cd = b.loc_cd "+
				"and   a.age_cls = b.age_cls "+
				"and   a.sex_cls = b.sex_cls "+
				"and   a.region_cd = b.region_cd ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthSiteSumError
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Monthly Person Seg 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthSiteSumError(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Site Summary Error is processing...");
		
		String queryT = "TRUNCATE TABLE tb_temp_error ";
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		String query = 
				"INSERT INTO tb_temp_error (code,site_id,rr_error)  "+
				"SELECT monthcode, site_id,   "+
				"       round(fn_month_modifier(monthcode)*1.96*sqrt((1/power(sum(netizen_cnt),2))*sum(UV_VAR)),4) Reach_E  "+
				"FROM  "+
				"    (  "+
				"    SELECT '"+monthcode+"' monthcode, site_id,   "+
				"           loc_cd, region_cd, sex_cls, age_cls,   "+
				"           netizen_cnt,  "+
				"           netizen_cnt*rr UV_S,  "+
				"           power(netizen_cnt,2)*((netizen_cnt-panel_cnt)/netizen_cnt)*(rr*(1-rr)/(decode(panel_cnt,1,1.000000000000001,panel_cnt)-1)) UV_VAR  "+
				"    FROM (  "+
				"          SELECT a.site_id, a.loc_cd, a.region_cd, a.sex_cls, a.age_cls, panel_cnt, netizen_cnt, decode(cnt/panel_cnt,null,0,cnt/panel_cnt) rr  "+
				"          FROM   "+
				"            (  "+
				"                SELECT /*+use_hash(a,b)*/ "+
				"                       site_id, loc_cd, region_cd, sex_cls, age_cls, max(panel_cnt) panel_cnt, max(netizen_cnt) netizen_cnt  "+
				"                FROM   tb_smart_month_person_seg a, tb_smart_month_site_sum b  "+
				"                WHERE  a.monthcode = b.monthcode "+
				"                AND    b.monthcode = '"+monthcode+"' "+
				"                AND    b.uu_overall_rank <= 100  "+
				"                GROUP BY site_id, loc_cd, region_cd, sex_cls, age_cls  "+
				"            ) a,  "+
				"            (  "+
				"                SELECT /*+use_hash(b,a)*/ "+
				"                       a.monthcode, site_id, b.loc_cd, b.region_cd, b.sex_cls, b.age_cls, count(*) cnt  "+
				"                FROM   tb_smart_month_fact a, tb_smart_month_person_seg b  "+
				"                WHERE  a.monthcode = '"+monthcode+"' "+
				"                AND    a.monthcode = b.monthcode "+
				"                AND    a.panel_id = b.panel_id  "+
				"                GROUP BY a.monthcode, site_id, b.loc_cd, b.region_cd, b.sex_cls, b.age_cls  "+
				"            ) b  "+
				"         WHERE a.age_cls=b.age_cls(+)  "+
				"         AND   a.sex_cls=b.sex_cls(+)  "+
				"         AND   a.loc_cd=b.loc_cd(+)  "+
				"         AND   a.region_cd=b.region_cd(+)  "+
				"         AND   a.site_id = b.site_id(+)  "+
				"        )  "+
				"      )  "+
				"GROUP BY monthcode, site_id";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String queryU = "UPDATE tb_smart_month_site_sum a "+
						"SET    rr_error = (select rr_error from tb_temp_error b where a.site_id = b.site_id and a.monthcode = b.code) "+
						"WHERE  monthcode = '"+monthcode+"' "+
						"AND    site_id in (select site_id from tb_smart_month_site_sum where monthcode = '"+monthcode+"' and uu_overall_rank <= 100) ";
        this.pstmt = connection.prepareStatement(queryU);
		this.pstmt.executeUpdate();
		
		this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		String query2 = 
				"INSERT INTO tb_temp_error (code,site_id,tts_error,pv_error)  "+
				"SELECT  monthcode, site_id, "+
				"        round(1.96*sqrt(sum(D_var))/60,2) TTS_E,  "+
				"        round(1.96*sqrt(sum(P_var)),2) PV_E  "+
				"FROM ( "+
				"    SELECT b.monthcode monthcode,  "+
				"           site_id,  "+
				"           sex_cls, age_cls, loc_cd, region_cd,  "+
				"           max(netizen_cnt) netizen_cnt,  "+
				"           sum(kc_n_factor) kc_n_factor_S, "+
				"           sum(duration*kc_p_factor) duration_s, sum(pv_cnt*kc_p_factor) pv_cnt_s, "+
				"           ((sum(kc_n_factor)*(sum(kc_n_factor)-count(a.panel_id)))/count(a.panel_id)*((sum(power(duration,2))-power(sum(duration),2)/count(a.panel_id))/(decode(count(a.panel_id),1,1.000000001,count(a.panel_id))-1))) d_var, "+
				"           ((sum(kc_n_factor)*(sum(kc_n_factor)-count(a.panel_id)))/count(a.panel_id)*((sum(power(pv_cnt,2))-power(sum(pv_cnt),2)/count(a.panel_id))/(decode(count(a.panel_id),1,1.000000001,count(a.panel_id))-1))) p_var "+
				"    FROM    ( "+
				"                SELECT site_id, panel_id, duration, pv_cnt  "+
				"                FROM   tb_smart_month_fact  "+
				"                WHERE  monthcode = '"+monthcode+"'   "+
				"                AND    site_id in ( "+
				"                    SELECT site_id  "+
				"                    FROM   tb_smart_month_site_sum  "+
				"                    WHERE  monthcode = '"+monthcode+"'   "+
				"                    AND    uu_overall_rank <= 100  "+
				"                    AND    category_code1 <> 'Z' "+
				"                ) "+
				"            ) a, "+
				"            ( "+
				"                SELECT b.monthcode, a.panel_id, "+
				"                       a.SEX_CLS, a.AGE_CLS, a.LOC_CD, a.REGION_CD, P_person kc_p_factor,person kc_n_factor, netizen_cnt, panel_cnt      "+
				"                FROM   tb_smart_month_person_seg a, tb_smart_month_panel_seg b "+
				"                WHERE  a.monthcode = b.monthcode "+
				"                AND    b.monthcode = '"+monthcode+"' "+
				"                AND    a.panel_id = b.panel_id "+
				"            ) b "+
				"    WHERE    b.panel_id=a.panel_id "+
				"    GROUP BY b.monthcode, a.site_id, sex_cls, age_cls, loc_cd, region_cd "+
				"    ) "+
				"GROUP BY monthcode, site_id ";
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		String queryU2 = "UPDATE tb_smart_month_site_sum a "+
						"SET    tts_error = (SELECT tts_error from tb_temp_error b WHERE a.site_id = b.site_id AND a.monthcode = b.code), "+
						"       pv_error = (select pv_error from tb_temp_error b WHERE a.site_id = b.site_id AND a.monthcode = b.code) "+
						"WHERE  monthcode = '"+monthcode+"' "+
						"AND    site_id in (SELECT site_id from tb_smart_month_site_sum WHERE monthcode = '"+monthcode+"' AND uu_overall_rank <= 100)";
		this.pstmt = connection.prepareStatement(queryU2);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthDaytimeAppSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 월간 앱 시간대 Summary
	 *************************************************************************/
	
	public Calendar executeMonthDaytimeAppSum(String monthcode, String lastday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Daytime APP is processing...");
		String query = 
				"insert into tb_smart_month_daytime_app_sum "+
				"select '"+monthcode+"' monthcode, time_cd, smart_id, package_name, app_name, pro_id, site_id, "+
				"    APP_CATEGORY_CD1, APP_CATEGORY_CD2, "+
				"    round(sum(uu_cnt_adj)/substr('"+lastday+"',7,2)) uu_cnt_adj, "+
				"    round(sum(tot_duration_adj)/substr('"+lastday+"',7,2)) tot_duration_adj, "+
				"    round(sum(avg_duration)/substr('"+lastday+"',7,2),2) avg_duration, "+
				"    round(sum(app_cnt_adj)/substr('"+lastday+"',7,2)) app_cnt_adj, "+
				"    sysdate , "+
				"    round(sum(reach_rate_adj)/substr('"+lastday+"',7,2),2) reach_rate_adj "+
				"from "+
				"( "+
				"    select /*+use_hash(b,a)*/ a.access_day, time_cd, smart_id, package_name, APP_NAME, PRO_ID, SITE_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2, "+
				"        round(sum(mo_n_factor)) uu_cnt_adj, "+
				"        round(sum(mo_p_factor*duration)) tot_duration_adj, "+
				"        round(sum(mo_p_factor*duration)/sum(mo_n_factor),2) avg_duration, "+
				"        round(sum(mo_p_factor*app_cnt)) app_cnt_adj, "+
				"        round(sum(mo_n_factor)/FN_SMART_day_NFACTOR(A.access_day)*100,2) reach_rate_adj "+
				"    from "+
				"    ( "+
				"        select /*+ leading(b,a) use_hash(b,a) index(a,pk_smart_daytime_app_fact) index(b,PK_SMART_MONTH_APP_SUM)*/ access_Day, decode(time_cd,'24','23',time_cd) time_cd, "+
				"            a.smart_id, a.package_name, panel_id, APP_NAME, APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, pro_id, sum(duration) duration, sum(app_cnt) app_cnt "+
				"        from tb_smart_daytime_app_fact a, tb_smart_month_app_sum b "+
				"        WHERE    access_day between '"+monthcode+"'||'01' and '"+lastday+"' "+
				"        and      b.monthcode = '"+monthcode+"' "+
				"        and      a.smart_id = b.smart_id "+
				"        group by access_day, decode(time_cd,'24','23',time_cd), a.smart_id, a.package_name, panel_id, APP_NAME, APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, pro_id "+
				"    )a, "+
				"    ( "+
				"        select /*+index(a,PK_SMART_DAY_PANEL_SEG)*/ access_day, panel_id, mo_n_factor, mo_p_factor "+
				"        from tb_smart_day_panel_seg a "+
				"        WHERE    access_day between '"+monthcode+"'||'01' and '"+lastday+"' "+
				"    )b "+
				"    where a.access_day = b.access_day "+
				"    and a.panel_id = b.panel_id "+
				"    group by a.access_day, time_cd, smart_id, package_name, APP_NAME, PRO_ID, SITE_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				") "+
				"group by time_cd, smart_id, package_name, app_name, pro_id, site_id, APP_CATEGORY_CD1, APP_CATEGORY_CD2";
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthDailyAppSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 월간 앱 일간 Summary
	 *************************************************************************/
	
	public Calendar executeMonthDailyAppSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Daytime APP is processing...");
		String query = 
				"insert into TB_SMART_DAILY_MONTH_APP_SUM "+
				"select  '"+monthcode+"' monthcode, smart_id, package_name, "+
				"        MAX_CATEGORY_CODE1, "+
				"        MAX_CATEGORY_CODE2, "+
				"        UV, RR, APP_CNT, DT, "+
				"        UV_weekdays, APP_CNT_weekdays, DT_weekdays, "+
				"        UV_weekend, APP_CNT_weekend, DT_weekend, "+
				"        rank() over (order by UV desc) uu_overall_rank, "+
				"        rank() over (order by APP_CNT desc) APP_CNT_overall_rank, "+
				"        rank() over (order by DT desc) avg_duration_overall_rank, "+ 
				"        sysdate proc_date "+
				"FROM "+
				"(       select  smart_id, package_name, "+
				"                round(sum(UV)/max(week_cnt)) UV, "+
				"                round(sum(RR)/max(week_cnt), 2) RR, "+
				"                round(sum(APP_CNT)/max(week_cnt)) APP_CNT, "+
				"                round(sum(DT)/max(week_cnt), 2) DT, "+
				"                max(APP_CATEGORY_CD1) MAX_CATEGORY_CODE1, "+
				"                max(APP_CATEGORY_CD2) MAX_CATEGORY_CODE2, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') >= 2 "+
				"                         AND  to_char(to_date(access_day, 'YYYYMMDD'), 'D') <= 6 "+
				"                         then UV end)/max(weekday_cnt)) UV_weekdays, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') >= 2 "+
				"                         AND  to_char(to_date(access_day, 'YYYYMMDD'), 'D') <= 6 "+
				"                         then APP_CNT end)/max(weekday_cnt)) APP_CNT_weekdays, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') >= 2 "+
				"                         AND  to_char(to_date(access_day, 'YYYYMMDD'), 'D') <= 6  "+
				"                         then DT end)/max(weekday_cnt), 2) DT_weekdays, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 1 "+
				"                         or  to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 7 "+
				"                         then UV end)/max(weekend_cnt)) UV_weekend, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 1 "+
				"                         or  to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 7 "+
				"                         then APP_CNT end)/max(weekend_cnt)) APP_CNT_weekend, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 1 "+
				"                         or  to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 7 "+
				"                         then DT end)/max(weekend_cnt),2) DT_weekend "+
				"        from   (select /*+ ordered(a) */ "+
				"                       a.access_day, smart_id, package_name, "+
				"                       uu_cnt_adj*fn_day_modifier(a.access_day) UV, "+
				"                       reach_rate_adj RR, "+
				"                       avg_duration/60 DT, "+
				"                       APP_CATEGORY_CD1, APP_CATEGORY_CD2, "+
				"                       APP_CNT_ADJ*fn_day_modifier(a.access_day) APP_CNT, "+
				"                       week_cnt, weekend_cnt, weekday_cnt "+
				"                from   tb_smart_day_app_sum a, "+
				"                       ( "+
				"                        select count(distinct access_day) week_cnt, count(day_cnt) weekday_cnt, "+
				"                               count(distinct access_day)-count(day_cnt) weekend_cnt "+
				"                        from   ( "+
				"                                select access_day, case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') >= 2 and "+
				"                                       to_char(to_date(access_day, 'YYYYMMDD'), 'D') <= 6 then 1 end day_cnt "+
				"                                from   tb_smart_day_panel_seg "+
				"                                where  access_day >= '"+monthcode+"'||'01' "+
				"                                AND    access_day <= fn_month_lastday('"+monthcode+"') "+
				"                                group by access_day "+
				"                               ) "+
				"                       ) c "+
				"                where  access_day >= '"+monthcode+"'||'01'  "+
				"                AND    access_day <= fn_month_lastday('"+monthcode+"')  "+
				"               ) "+
				"        group by smart_id, package_name "+
				")a ";
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthNextApp
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 월간 앱 일간 Summary
	 *************************************************************************/
	
	public Calendar executeMonthNextApp(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Next APP is processing...");
		String query = 
				"insert into TB_SMART_MONTH_next_APP "+
				"select * "+
				"from "+
				"( "+
				"    select a.monthcode, smart_id, package_name,  fn_smart_app_name(smart_id) app_name, "+
				"        next_smart_id, next_package_name, fn_smart_app_name(next_smart_id) next_app_name, APP_CATEGORY_CD1, APP_CATEGORY_CD2, "+
				"        round(sum(mo_n_factor),5) uu_cnt_adj, "+
				"        round(sum(mo_n_factor)/uu_cnt_adj*100,2) reach_rate_adj, "+
				"        round(sum(mo_p_factor*move_cnt),5) move_cnt_adj, "+
				"        rank()over(partition by a.monthcode, smart_id, APP_CATEGORY_CD1 order by sum(mo_n_factor) desc) CATEGORY1_RANK, "+
				"        rank()over(partition by a.monthcode, smart_id, APP_CATEGORY_CD2 order by sum(mo_n_factor) desc) CATEGORY2_RANK, "+
				"        sysdate proc_date "+
				"     from "+
				"     ( "+
				"         select /*+use_hash(c)*/substr(access_day,1,6) monthcode, panel_id, a.smart_id, package_name, next_smart_id, next_package_name, APP_CATEGORY_CD2, APP_CATEGORY_CD1, "+
				"            sum(move_cnt) move_cnt, max(uu_cnt_adj) uu_cnt_adj "+
				"         from tb_smart_day_app_navi_Fact a, "+
				"        ( "+
				"            select smart_id, APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				"            from tb_temp_smart_app_info "+
				"        )b, "+
				"        ( "+
				"            select smart_id , uu_cnt_adj "+
				"            from tb_smart_month_app_sum  "+
				"            where monthcode = '"+monthcode+"' "+
				"            and reach_rate >= 0.1 "+
				"        ) c "+
				"        where  access_day >= '"+monthcode+"'||'01' "+
				"        AND    access_day <= fn_month_lastday('"+monthcode+"') "+
				"        and a.smart_id = c.smart_id "+
				"        and a.next_smart_id = b.smart_id "+
				"        group by substr(access_day,1,6), panel_id, a.smart_id, package_name, next_smart_id, next_package_name, APP_CATEGORY_CD2, APP_CATEGORY_CD1 "+
				"     )a, "+ 
				"     ( "+
				"        select monthcode, panel_id, mo_n_factor, mo_p_factor "+
				"        from tb_smart_month_panel_seg "+
				"        where monthcode = '"+monthcode+"' "+
				"     )b "+
				"     where a.monthcode = b.monthcode "+
				"     and a.panel_id = b.panel_id "+
				"     group by a.monthcode,  smart_id, package_name, next_smart_id, next_package_name,APP_CATEGORY_CD1, APP_CATEGORY_CD2, uu_cnt_adj "+
				" ) "+
				" where CATEGORY2_RANK <= 100 ";
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthPretApp
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 월간 앱 일간 Summary
	 *************************************************************************/
	
	public Calendar executeMonthPreApp(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Pre APP is processing...");
		String query = 
				"insert into TB_SMART_MONTH_PRE_APP "+
				"select * "+
				"from "+
				"( "+
				"    select a.monthcode, smart_id, package_name, fn_smart_app_name(smart_id) app_name,  "+
				"        pre_smart_id, pre_package_name, fn_smart_app_name(pre_smart_id) pre_app_name,  APP_CATEGORY_CD1, APP_CATEGORY_CD2, "+
				"        round(sum(mo_n_factor),5) uu_cnt_adj, "+
				"        round(sum(mo_n_factor)/uu_cnt_adj*100,2) reach_rate_adj, "+
				"        round(sum(mo_p_factor*move_cnt),5) move_cnt_adj, "+
				"        rank()over(partition by a.monthcode, smart_id, APP_CATEGORY_CD1 order by sum(mo_n_factor) desc) CATEGORY1_RANK, "+
				"        rank()over(partition by a.monthcode, smart_id, APP_CATEGORY_CD2 order by sum(mo_n_factor) desc) CATEGORY2_RANK, "+
				"        sysdate proc_date "+
				"     from "+
				"     ( "+
				"         select substr(access_day,1,6) monthcode, panel_id, next_smart_id smart_id, next_package_name package_name, a.smart_id pre_smart_id, package_name pre_package_name, APP_CATEGORY_CD2, APP_CATEGORY_CD1, "+
				"            sum(move_cnt) move_cnt, max(uu_cnt_adj) uu_cnt_adj "+
				"         from tb_smart_day_app_navi_Fact a, "+
				"        ( "+
				"        select smart_id pre_smart_id, APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				"        from tb_temp_smart_app_info "+
				"        )b, "+
				"        ( "+
				"            select smart_id , uu_cnt_adj "+
				"            from tb_smart_month_app_sum  "+
				"            where monthcode = '"+monthcode+"' "+
				"            and reach_rate >= 0.1 "+
				"        ) c "+
				"        where  access_day >= '"+monthcode+"'||'01' "+
				"        AND    access_day <= fn_month_lastday('"+monthcode+"') "+
				"        and a.next_smart_id in c.smart_id "+
				"        and a.smart_id = b.pre_smart_id "+
				"        group by substr(access_day,1,6), panel_id, a.smart_id, package_name, next_smart_id, next_package_name, APP_CATEGORY_CD2, APP_CATEGORY_CD1 "+
				"     )a, "+
				"     ( "+
				"        select monthcode, panel_id, mo_n_factor, mo_p_factor "+
				"        from tb_smart_month_panel_seg "+
				"        where monthcode = '"+monthcode+"' "+
				"     )b "+
				"     where a.monthcode = b.monthcode "+
				"     and a.panel_id = b.panel_id "+
				"     group by a.monthcode,  smart_id, package_name, pre_smart_id, pre_package_name,APP_CATEGORY_CD1, APP_CATEGORY_CD2, uu_cnt_adj "+
				" ) "+
				" where CATEGORY2_RANK <= 100 ";
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}	
	
	/**************************************************************************
	 *		메소드명		: executeMonthDailySiteSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 월간 웹 일평균 Summary
	 *************************************************************************/
	
	public Calendar executeMonthDailySiteSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Site daily is processing...");
		String query = 
				"INSERT   INTO tb_smart_daily_month_sum "+
				"(        MONTHCODE, SITE_ID, SITE_NAME, "+
				"         CATEGORY_NAME, category_code1, category_code2, category_code3, "+
				"         DAILY_UU_CNT_ADJ, REACH_RATE_ADJ, "+
				"         DAILY_VISIT_CNT_ADJ, DAILY_PV_CNT_ADJ, DAILY_AVG_DURATION, "+
				"         DAILY_UU_CNT_ADJ_WEEKDAYS, DAILY_PV_CNT_ADJ_WEEKDAYS, "+
				"         DAILY_VISIT_CNT_ADJ_WEEKDAYS, DAILY_AVG_DURATION_WEEKDAYS, "+
				"         DAILY_UU_CNT_ADJ_WEEKEND, DAILY_PV_CNT_ADJ_WEEKEND, "+
				"         DAILY_VISIT_CNT_ADJ_WEEKEND, DAILY_AVG_DURATION_WEEKEND, "+
				"         uu_overall_rank, visit_overall_rank, pv_overall_rank, avg_duration_overall_rank, "+
				"         proc_date "+
				") "+
				"select  '"+monthcode+"' monthcode, site_id, site_name, "+
				"nvl(FN_CATEGORY_REF2_NAME2((select CATEGORY_CODE2 from tb_site_info b where exp_time > sysdate and a.site_id = b.site_id)),max_category_name) category_name, "+
				"        nvl((select CATEGORY_CODE1 from tb_site_info b where exp_time > sysdate and a.site_id = b.site_id),MAX_CATEGORY_CODE1) CATEGORY_CODE1, "+
				"        nvl((select CATEGORY_CODE2 from tb_site_info b where exp_time > sysdate and a.site_id = b.site_id),MAX_CATEGORY_CODE2) CATEGORY_CODE2, "+
				"        nvl((select CATEGORY_CODE3 from tb_site_info b where exp_time > sysdate and a.site_id = b.site_id),MAX_CATEGORY_CODE2) CATEGORY_CODE3, "+
				"        UV, RR, VS, PV, DT, "+
				"        UV_weekdays, PV_weekdays, VS_weekdays, DT_weekdays, "+
				"        UV_weekend, PV_weekend, VS_weekend, DT_weekend, "+
				"        rank() over (order by UV desc) uu_overall_rank, "+
				"        rank() over (order by VS desc) visit_overall_rank, "+
				"        rank() over (order by PV desc) pv_overall_rank, "+
				"        rank() over (order by DT desc) avg_duration_overall_rank, "+
				"        sysdate proc_date "+
				"FROM "+
				"(       select  site_id, max(site_name) site_name, "+
				"                round(sum(UV)/max(week_cnt)) UV, "+
				"                round(sum(RR)/max(week_cnt), 2) RR, "+
				"                round(sum(VS)/max(week_cnt)) VS, "+
				"                round(sum(PV)/max(week_cnt)) PV, "+
				"                round(sum(DT)/max(week_cnt), 2) DT, "+
				"                max(category_name) max_category_name, "+
				"                max(CATEGORY_CODE1) MAX_CATEGORY_CODE1, "+
				"                max(CATEGORY_CODE2) MAX_CATEGORY_CODE2, "+
				"                max(CATEGORY_CODE3) MAX_CATEGORY_CODE3, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') >= 2 "+
				"                         AND  to_char(to_date(access_day, 'YYYYMMDD'), 'D') <= 6 "+
				"                         then UV end)/max(weekday_cnt)) UV_weekdays, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') >= 2 "+
				"                         AND  to_char(to_date(access_day, 'YYYYMMDD'), 'D') <= 6 "+
				"                         then VS end)/max(weekday_cnt)) VS_weekdays, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') >= 2 "+
				"                         AND  to_char(to_date(access_day, 'YYYYMMDD'), 'D') <= 6 "+
				"                         then PV end)/max(weekday_cnt)) PV_weekdays, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') >= 2 "+
				"                         AND  to_char(to_date(access_day, 'YYYYMMDD'), 'D') <= 6 "+ 
				"                         then DT end)/max(weekday_cnt), 2) DT_weekdays, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 1 "+
				"                         or  to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 7 "+
				"                         then UV end)/max(weekend_cnt)) UV_weekend, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 1 "+
				"                         or  to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 7 "+
				"                         then VS end)/max(weekend_cnt)) VS_weekend, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 1 "+
				"                         or  to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 7 "+
				"                         then PV end)/max(weekend_cnt)) PV_weekend, "+
				"                round(sum(case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 1 "+
				"                         or  to_char(to_date(access_day, 'YYYYMMDD'), 'D') = 7 "+
				"                         then DT end)/max(weekend_cnt), 2) DT_weekend "+
				"        from   (select /*+ ordered(a) */ "+
				"                       a.access_day, site_id, site_name, "+
				"                       uu_cnt_adj*fn_day_modifier(a.access_day) UV, "+
				"                       reach_rate_adj RR, "+
				"                       visit_cnt_adj*fn_day_modifier(a.access_day) VS, "+
				"                       pv_cnt_adj*fn_day_modifier(a.access_day) PV, "+
				"                       avg_duration/60 DT, category_name, "+
				"                       CATEGORY_CODE1, CATEGORY_CODE2, CATEGORY_CODE3, "+
				"                       week_cnt, weekday_cnt, weekend_cnt "+
				"                from   tb_smart_day_site_sum a, "+
				"                       ( "+
				"                        select count(distinct access_day) week_cnt, count(day_cnt) weekday_cnt, "+
				"                               count(distinct access_day)-count(day_cnt) weekend_cnt "+
				"                        from   ( "+
				"                                select access_day, case when to_char(to_date(access_day, 'YYYYMMDD'), 'D') >= 2 and  "+
				"                                       to_char(to_date(access_day, 'YYYYMMDD'), 'D') <= 6 then 1 end day_cnt "+
				"                                from   tb_smart_day_panel_seg "+
				"                                where  access_day >= '"+monthcode+"'||'01' "+
				"                                AND    access_day <= fn_month_lastday('"+monthcode+"') "+
				"                                group by access_day "+
				"                               ) "+
				"                       ) c "+
				"                where  a.access_day >= '"+monthcode+"'||'01' "+
				"                AND    a.access_day <= fn_month_lastday('"+monthcode+"') "+
				"                and    a.category_code1!= 'Z' "+
				"               ) "+
				"        group by site_id "+
				")a ";
	
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonth1lvlSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 브라우져 정보에서 카테고리 레벨1별 Summary
	 *************************************************************************/
	
	public Calendar executeMonth1lvlSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Site Level1 is processing...");
		String query = 
						"insert into tb_smart_month_1level_sum  "+
						"       ( monthcode, category_code, best_site_id, emerging_site_id, "+
						"     uu_cnt, pv_cnt, reach_rate, "+
						"     uu_cnt_adj, pv_cnt_adj, reach_rate_adj, proc_date, tot_duration_adj, "+
						"     duration, daily_freq_cnt, site_cnt, cate_site_cnt, "+
						"     uu_market_rate, pv_market_rate) "+
						"select 	'"+monthcode+"', v_category.category_code1, best_site_id, NULL, "+
						"    v_category.uu_cnt, v_category.pv_cnt, "+
						"    round( v_category.uu_cnt/FN_SMART_MONTH_COUNT('"+monthcode+"')*100, 2)  reach_rate, "+
						"    v_category.uu_cnt_adj, v_category.pv_cnt_adj, "+
						"    round(uu_cnt_adj/FN_SMART_MONTH_NFACTOR('"+monthcode+"')*100,5) reach_rate_adj, "+
						"    sysdate, v_category.tot_duration_adj, "+
						"    duration, daily_freq_cnt, site_cnt, cate_site_cnt, "+
						"    round(uu_market/uu_cnt_adj*100,2) uu_market_rate, "+
						"    round(pv_market/pv_cnt_adj*100,2) pv_market_rate "+
						"from 	 "+
						"(	 "+
						"    select 	category_code1, count(*) uu_cnt, sum(pv_cnt) pv_cnt,  "+
						"        sum(mo_n_factor) uu_cnt_adj, sum(pv_cnt_adj) pv_cnt_adj, "+
						"        round(decode(sum(mo_p_factor), 0,1, sum(duration*mo_p_factor)/sum(mo_n_factor)) ,2) duration, "+
						"        round(avg(site_cnt),2) site_cnt,  "+
						"        round(decode(sum(mo_n_factor), 0,1, sum(fq*mo_n_factor)/sum(mo_n_factor)) ,2)  daily_freq_cnt, "+
						"        round(sum(duration*mo_p_factor),5) tot_duration_adj "+
						"    from  "+
						"    (	 "+
						"        select /*+use_hash(b,a) index(a,PK_SMART_DAY_FACT)*/ "+
						"            a.panel_id,  "+
						"            c.category_code1, "+
						"            sum(a.pv_cnt) pv_cnt, "+
						"            min(b.mo_n_factor) mo_n_factor, "+
						"            min(b.mo_p_factor) mo_p_factor, "+
						"            sum(a.pv_cnt * b.mo_p_factor) pv_cnt_adj, "+
						"            sum(a.duration) duration, "+
						"            count(distinct a.site_id) site_cnt, "+
						"            count(distinct a.access_day) fq  "+
						"        from tb_smart_day_fact a, tb_smart_month_panel_seg b, tb_smart_month_site_sum c "+
						"        where a.access_day >= '"+monthcode+"'||'01' "+
						"        and   a.access_day <= fn_month_lastday('"+monthcode+"') "+
						"        and   a.site_id  = c.site_id "+
						"        and   a.panel_id = b.panel_id "+
						"        and   b.monthcode = '"+monthcode+"' "+
						"        and   c.monthcode = '"+monthcode+"' "+
						"        and   c.category_code1 <> 'Z' "+
						"        group by c.category_code1, a.panel_id "+
						"    ) "+
						"    group by category_code1 "+
						") v_category, "+
						"( 	 "+
						"    select /*+use_hash(b,a)*/ category_code1, min(site_id) best_site_id "+
						"    from tb_smart_month_site_sum A "+
						"    where monthcode = '"+monthcode+"' "+
						"    and   uu_overall_rank = ( select min(uu_overall_rank) "+
						"    from   tb_smart_month_site_sum b "+
						"    where  A.category_code1= category_code1 "+
						"    and    A.monthcode     = monthcode "+
						"    ) "+
						"    group by category_code1  "+
						") v_best_site, "+
						"( 	 "+
						"    select 	category_code1, count(distinct site_id) cate_site_cnt "+
						"    from 	tb_smart_month_site_sum  "+
						"    where 	monthcode = '"+monthcode+"' "+
						"    group by category_code1  "+
						") v_cate_site, "+
						"( 	 "+
						"    select  category_code1, sum(mo_n_factor) uu_market, sum(pv_cnt*mo_p_factor) pv_market "+
						"    from     "+
						"    (  "+
						"        select        /*+ use_hash(b,a)*/ "+
						"        a.category_code1, a.panel_id, sum(a.pv_cnt) pv_cnt "+
						"        from          tb_smart_month_fact a "+
						"        where         a.monthcode      = '"+monthcode+"' "+
						"        and           a.site_id    in (	select site_id "+
						"                    from "+
						"                    ( "+
						"                    select site_id, "+
						"                    rank() over (partition by CATEGORY_CODE1 order by uu_cnt_adj desc ) rnk "+
						"                    from tb_smart_month_site_sum  "+
						"                    where monthcode = '"+monthcode+"' "+
						"                    )  "+
						"                    where rnk <=3 "+
						"        ) "+
						"        group by      a.category_code1, panel_id "+
						"    ) a, "+
						"    (  "+
						"        select        panel_id, mo_n_factor, mo_p_factor "+
						"        from          tb_smart_month_panel_seg "+
						"        where         monthcode = '"+monthcode+"' "+
						"    ) b "+
						"    where   a.panel_id      = b.panel_id "+
						"    group by category_code1 "+
						") v_market "+
						"where 	v_category.category_code1 = v_best_site.category_code1 "+
						"and 	v_category.category_code1 = v_cate_site.category_code1 "+
						"and   	v_category.category_code1 = v_market.category_code1";
	
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonth2lvlSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 브라우져 정보에서 카테고리 레벨2별 Summary
	 *************************************************************************/

	public Calendar executeMonth2lvlSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Site Level2 is processing...");
		String query = 
						"insert into tb_smart_month_2level_sum "+
						"       ( monthcode, category_code, best_site_id, emerging_site_id, "+
						"     uu_cnt, pv_cnt, reach_rate, "+
						"     uu_cnt_adj, pv_cnt_adj, reach_rate_adj, proc_date, tot_duration_adj,  "+
						"     duration, daily_freq_cnt, site_cnt, cate_site_cnt, uu_market_rate, pv_market_rate) "+
						"select 	'"+monthcode+"',  "+
						"    a.category_code2, best_site_id, NULL, "+
						"    a.uu_cnt, a.pv_cnt, "+
						"    round( a.uu_cnt/fn_smart_month_count('"+monthcode+"')*100, 2)  reach_rate, "+
						"    a.uu_cnt_adj, a.pv_cnt_adj, "+
						"    round(uu_cnt_adj/fn_smart_month_nfactor('"+monthcode+"')*100,5) reach_rate_adj, "+
						"    sysdate, a.tot_duration_adj, "+
						"    duration, daily_freq_cnt, site_cnt, cate_site_cnt, "+
						"    round(uu_market/uu_cnt_adj*100,2) uu_market_rate, "+
						"    round(pv_market/pv_cnt_adj*100,2) pv_market_rate "+
						"from 	 "+
						"(	 "+
						"    select 	category_code2, count(*) uu_cnt, sum(pv_cnt) pv_cnt,  "+
						"        sum(mo_n_factor) uu_cnt_adj, sum(pv_cnt_adj) pv_cnt_adj, "+
						"        round(decode(sum(mo_p_factor), 0,1, sum(duration*mo_p_factor)/sum(mo_n_factor)) ,2) duration, "+
						"        round(avg(site_cnt),2) site_cnt,  "+
						"        round(decode(sum(mo_n_factor),0,1, sum(fq*mo_n_factor)/sum(mo_n_factor)) ,2)  daily_freq_cnt, "+
						"        round(sum(duration*mo_p_factor),5) tot_duration_adj "+
						"    from  "+
						"    (	 "+
						"        select /*+fact(a)*/ "+
						"            a.panel_id,  "+
						"            c.category_code2, "+
						"            sum(a.pv_cnt) pv_cnt, "+
						"            min(b.mo_n_factor) mo_n_factor, "+
						"            min(b.mo_p_factor) mo_p_factor, "+
						"            sum(a.pv_cnt * b.mo_p_factor) pv_cnt_adj, "+
						"            sum(a.duration) duration, "+
						"            count(distinct a.site_id) site_cnt, "+
						"            count(distinct a.access_day) fq  "+
						"        from tb_smart_day_fact a, tb_smart_month_panel_seg b, tb_smart_month_site_sum c "+
						"        where a.access_day >= '"+monthcode+"'||'01' "+
						"        and   a.access_day <= fn_month_lastday('"+monthcode+"') "+
						"        and   a.site_id  = c.site_id "+
						"        and   a.panel_id = b.panel_id "+
						"        and   b.monthcode = '"+monthcode+"' "+
						"        and   c.monthcode = '"+monthcode+"' "+
						"        and   c.category_code2 < 'Z' "+
						"        group by c.category_code2, a.panel_id "+
						"    ) "+
						"    group by category_code2 "+
						") a, "+
						"( 	 "+
						"    select a.category_code2,best_site_id,cate_site_cnt,uu_market,pv_market  "+
						"    from  "+
						"    ( "+
						"    select category_code2, min(site_id) best_site_id "+
						"        from tb_smart_month_site_sum "+
						"        where monthcode = '"+monthcode+"' "+
						"        and uu_2level_rank = 1 "+
						"        group by category_code2  "+
						"    ) a, "+
						"    ( 	 "+
						"        select category_code2, count(distinct site_id) cate_site_cnt "+
						"        from   tb_smart_month_site_sum "+
						"        where monthcode = '"+monthcode+"' "+
						"        group by category_code2  "+
						"    ) b, "+
						"    ( 	 "+
						"        select 	/*+use_hash(b,a)*/ category_code2,	sum(mo_n_factor) uu_market, sum(pv_cnt*mo_p_factor) pv_market "+
						"        from  	 "+
						"        (  "+
						"            select	/*+use_hash(b,a)*/ "+
						"            b.category_code2, a.panel_id, sum(a.pv_cnt) pv_cnt "+
						"            from 		tb_smart_month_fact a, tb_smart_month_site_sum b "+
						"            where		a.monthcode 	= '"+monthcode+"' "+
						"            and		a.monthcode	= b.monthcode "+
						"            and		b.uu_2level_rank <= 3 "+
						"            and		a.site_id	= b.site_id "+
						"            group by	b.category_code2, panel_id "+
						"        ) a, "+
						"        (  "+
						"            select	panel_id, mo_n_factor, mo_p_factor "+
						"            from 		tb_smart_month_panel_seg "+
						"            where 	monthcode = '"+monthcode+"' "+
						"        ) b "+
						"        where	a.panel_id 	= b.panel_id "+
						"        group by category_code2 "+
						"    ) c "+
						"    where 	a.category_code2 = b.category_code2 "+
						"    and 	a.category_code2 = c.category_code2 "+
						") b "+
						"where 	a.category_code2 = b.category_code2 ";
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthlv1Seg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 브라우져 정보에서 카테고리 레벨1별 Seg
	 *************************************************************************/

	public Calendar executeMonthLv1Seg(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Site Level1 Seg is processing...");
		String query = 
				" insert into kclick.tb_smart_month_seg_cate1 "+
				" ( monthcode, segment_id, category_code1, "+
				"   age_cls,  sex_cls,  income_cls, job_cls,    education_cls, "+
				"   ismarried_cls, region_cd,  uu_cnt, uu_est_cnt, pv_cnt, pv_est_cnt, "+
				"   avg_duration, duration_est, avg_daily_freq_cnt, "+
				"   avg_pv_est_cnt, reach_rate, visit_cnt, visit_est_cnt, "+
				"   proc_date) "+
				"select monthcode, a.segment_id, d.category_code1, "+
				"    min(b.age_cls)        age_cls, "+
				"    min(b.sex_cls)        sex_cls, "+
				"    min(b.income_cls)     income_cls, "+ 
				"    min(b.job_cls)        job_cls, "+
				"    min(b.education_cls)  education_cls, "+
				"    min(b.ismarried_cls ) ismarried_cls, "+
				"    min(b.region_cd )     region_cd, "+
				"    count(distinct a.panel_id) uu_cnt, "+
				"    round(sum( b.mo_n_factor),5) uu_est_cnt, "+
				"    sum(pv_cnt) pv_cnt, "+
				"    round(sum(pv_cnt*b.mo_p_factor),5) pv_est_cnt, "+
				"    round(decode(sum(mo_n_factor),0,1, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"    round(sum(duration*b.mo_p_factor),5) duration_est, "+
				"    round(decode(sum(mo_n_factor),0,1, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"    round(decode(sum(mo_n_factor),0,1, sum(pv_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				"    round(sum(b.mo_n_factor)/ max(sum_kc_nfactor)*100, 5) reach_rate, "+
				"    sum(visit_cnt) visit_cnt, "+
				"    round(sum(visit_cnt*b.mo_p_factor),5) visit_est_cnt, "+
				"    sysdate "+
				"from "+
				"( "+
				"    select '"+monthcode+"' monthcode, segment_id, site_id, panel_id, "+
				"        pv_cnt, visit_cnt, daily_freq_cnt, duration "+
				"    from   tb_smart_month_fact "+
				"    where  monthcode = '"+monthcode+"' "+
				") a, "+
				"( "+
				"    select panel_id, kc_seg_id, mo_n_factor, mo_p_factor, "+
				"        age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"    from   tb_smart_month_panel_seg "+
				"    where  monthcode = '"+monthcode+"' "+
				") b, "+
				"( "+
				"    select sum(mo_n_factor) sum_kc_nfactor "+
				"    from tb_smart_month_panel_seg "+
				"    where monthcode = '"+monthcode+"' "+
				") c, "+
				"( "+
				"    select site_id, category_code1 "+
				"    from tb_smart_month_site_sum "+
				"    where monthcode = '"+monthcode+"' "+
				") d "+
				"where a.panel_id   = b.panel_id "+
				"and   a.site_id    = d.site_id "+
				"group by a.segment_id, d.category_code1 ";
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonth1lv2Seg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 브라우져 정보에서 카테고리 레벨2별 Seg
	 *************************************************************************/

	public Calendar executeMonthLv2Seg(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Site Level2 Seg is processing...");
		String query = 
				"insert into kclick.tb_smart_month_seg_cate "+
				"( monthcode, segment_id, CATEGORY_CODE2, "+
				"  age_cls,  sex_cls,  income_cls, job_cls,    education_cls, "+
				"  ismarried_cls, region_cd,  uu_cnt, uu_est_cnt, pv_cnt, pv_est_cnt, "+
				"  avg_duration, duration_est, avg_daily_freq_cnt, "+
				"  avg_pv_est_cnt, reach_rate, visit_cnt, visit_est_cnt, "+
				"  proc_date) "+
				"select monthcode, a.segment_id, d.CATEGORY_CODE2, "+
				"  min(b.age_cls)        age_cls, "+
				"  min(b.sex_cls)        sex_cls, "+
				"  min(b.income_cls)     income_cls, "+ 
				"  min(b.job_cls)        job_cls, "+
				"  min(b.education_cls)  education_cls, "+
				"  min(b.ismarried_cls ) ismarried_cls, "+
				"  min(b.region_cd )     region_cd, "+
				"  count(distinct a.panel_id) uu_cnt, "+
				"  round(sum( b.mo_n_factor),5) uu_est_cnt, "+
				"  sum(pv_cnt) pv_cnt, "+
				"  round(sum(pv_cnt*b.mo_p_factor),5) pv_est_cnt, "+
				"  round(decode(sum(mo_n_factor),0,1, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"  round(sum(duration*b.mo_p_factor),5) duration_est, "+
				"  round(decode(sum(mo_n_factor),0,1, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"  round(decode(sum(mo_n_factor),0,1, sum(pv_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				"  round(sum(b.mo_n_factor)/ max(sum_kc_nfactor)*100, 5) reach_rate, "+
				"  sum(visit_cnt) visit_cnt, "+
				"  round(sum(visit_cnt*b.mo_p_factor),5) visit_est_cnt, "+
				"  sysdate "+
				"  from "+
				" ( "+
				"    select '"+monthcode+"' monthcode, segment_id, site_id, panel_id, "+
				"       pv_cnt, visit_cnt, daily_freq_cnt, duration "+
				"    from   tb_smart_month_fact "+
				"    where  monthcode = '"+monthcode+"' "+
				" ) a, "+
				" ( "+
				"   select panel_id, kc_seg_id, mo_n_factor, mo_p_factor, "+
				"      age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"   from   tb_smart_month_panel_seg "+
				"   where  monthcode = '"+monthcode+"' "+
				" ) b, "+
				" ( "+
				"   select sum(mo_n_factor) sum_kc_nfactor "+
				"   from tb_smart_month_panel_seg "+
				"   where monthcode = '"+monthcode+"' "+
				" ) c, "+
				" (select site_id, CATEGORY_CODE2 from tb_smart_month_site_sum where monthcode = '"+monthcode+"') d "+
				" where a.panel_id   = b.panel_id "+
				"and   a.site_id    = d.site_id "+
				"group by a.segment_id, d.CATEGORY_CODE2 ";
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}	
	
	/**************************************************************************
	 *		메소드명		: executeMonthKeywordSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 브라우져 정보에서 키워드별 Monthly Summary
	 *************************************************************************/

	public Calendar executeMonthKeywordSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Keyword Sum is processing...");
		String query = 
						"INSERT INTO tb_smart_month_keyword_sum "+
						"SELECT monthcode, keyword, uv, pv, qv, "+
						"    rank() over (partition by monthcode order by uv desc) u_rank, "+
						"    rank() over (partition by monthcode order by pv desc) p_rank, "+
						"    rank() over (partition by monthcode order by qv desc) q_rank "+
						"FROM  "+
						"( "+
						"    SELECT /*+ordered */ "+
						"        a.monthcode,  "+
						"        keyword, "+
						"        round(sum(mo_n_factor), 5) uv, "+
						"        round(sum(mo_p_factor*pv_cnt), 5) pv, "+
						"        round(sum(mo_p_factor*query_cnt), 5) qv "+
						"    FROM  "+
						"    ( "+
						"        SELECT monthcode, keyword, panel_id, sum(pv_cnt) pv_cnt, sum(query_cnt) query_cnt "+
						"        FROM   tb_smart_month_keyword_fact "+
						"        WHERE  monthcode = '"+monthcode+"' "+
						"        GROUP BY monthcode, keyword, panel_id "+
						"    ) a, "+
						"    ( "+
						"        SELECT monthcode, panel_id, mo_n_factor, mo_p_factor "+
						"        FROM   tb_smart_month_panel_seg "+
						"        WHERE  monthcode = '"+monthcode+"' "+
						"    ) b "+
						"    WHERE a.panel_id = b.panel_id "+
						"    AND a.monthcode = b.monthcode "+
						"    GROUP BY keyword, a.monthcode "+
						") ";
		
		//System.out.println(query);
		//System.exit(0);
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthSession
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthSession 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthSession(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Session is processing...");
		
		String query = 
				"INSERT   INTO tb_smart_month_session (  "+
				"        monthcode, panel_id, pv_cnt, duration , "+
				"        daily_freq_cnt, mo_n_factor, mo_p_factor, age_cls, sex_cls, "+
				"        education_cls, income_cls, job_cls, ismarried_cls, site_cnt, proc_date  "+
				") "+
				"SELECT  '"+monthcode+"', v_session.panel_id, pv_cnt, duration,                  "+
				"        daily_freq_cnt, mo_n_factor, mo_p_factor, age_cls, sex_cls,  "+
				"        education_cls, income_cls, job_cls, ismarried_cls, nvl(site_cnt,0), sysdate "+
				"FROM     ( select panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, "+
				"                  count(distinct access_day) daily_freq_cnt "+
				"             from tb_smart_day_fact "+
				"            where access_day >= '"+monthcode+"'||'01' "+
				"              and access_day <= fn_month_lastday('"+monthcode+"') "+
				"            group by panel_id "+
				"         ) v_session, "+
				"         ( select panel_id, mo_n_factor, age_cls, sex_cls, education_cls,  "+
				"                  income_cls, job_cls, ismarried_cls, mo_p_factor "+
				"             from tb_smart_month_panel_seg "+
				"            where monthcode = '"+monthcode+"' "+
				"         ) v_panel_seg, "+
				"         ( select panel_id, count(distinct site_id) site_cnt "+
				"             from tb_smart_month_fact "+
				"            where monthcode = '"+monthcode+"' "+
				"              and category_code2 < 'Z' "+
				"            group by panel_id "+
				"         ) v_fact "+
				"WHERE v_session.panel_id = v_panel_seg.panel_id "+
				"AND v_session.panel_id = v_fact.panel_id(+) ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthSection
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthSection lvl1 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthSection(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Section lvl1 is processing...");
		
		String query = 
				"INSERT  INTO TB_SMART_MONTH_SECTION "+
				"(         "+
				"    monthcode, site_id, section_id, site_name, url_link, "+
				"    reach_rate, uu_cnt, pv_cnt, avg_duration, "+
				"    daily_freq_cnt, uu_cnt_adj, pv_cnt_adj, reach_rate_adj,  "+
				"    uu_overall_rank,  "+
				"    pv_overall_rank,  "+
				"    avg_duration_overall_rank, "+
				"    freq_overall_rank, "+
				"    tot_duration_overall_rank, "+
				"    visit_cnt, visit_cnt_adj, tot_duration_adj, proc_date "+
				") "+
				"SELECT  /*+ordered use_hash(b,a)*/ "+
				"        A.monthcode, A.site_id, A.section_id, B.site_name, B.url_link, "+
				"        A.reach_rate, A.uu_cnt, A.pv_cnt, A.avg_duration, "+
				"        A.daily_freq_cnt, A.uu_cnt_adj, A.pv_cnt_adj, A.reach_rate_adj,  "+
				"        rank() over (partition by A.section_id order by uu_cnt_adj desc) uu_overall_rank, "+
				"        rank() over (partition by A.section_id order by pv_cnt_adj desc) pv_overall_rank, "+
				"        rank() over (partition by A.section_id order by avg_duration desc) avg_duration_overall_rank, "+
				"        rank() over (partition by A.section_id order by daily_freq_cnt desc) daily_freq_overall_rank, "+
				"        rank() over (partition by A.section_id order by tot_duration_adj desc) tot_duration_overall_rank, "+
				"        visit_cnt, visit_cnt_adj, tot_duration_adj, sysdate proc_date "+
				"FROM      "+
				"(         "+
				"    SELECT  /*+use_hash(b,a)*/ "+
				"            '"+monthcode+"' monthcode,  "+
				"            A.site_id,  "+
				"            A.section_id, "+
				"            count(*)/fn_smart_month_count('"+monthcode+"')*100 reach_rate,  "+
				"            count(*) uu_cnt, "+
				"            sum(pv_cnt) pv_cnt, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"            sum(B.mo_n_factor) uu_cnt_adj,  "+
				"            sum(A.pv_cnt*B.mo_p_factor) pv_cnt_adj, "+
				"            sum(B.mo_n_factor)/fn_smart_month_nfactor('"+monthcode+"')*100 reach_rate_adj, "+
				"            sum(visit_cnt) visit_cnt, "+
				"            sum(visit_cnt*mo_p_factor) visit_cnt_adj, "+
				"            round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj "+
				"    FROM "+
				"    (         "+
				"            SELECT   site_id, section_id, panel_id, pv_cnt, duration, daily_freq_cnt, visit_cnt "+
				"            FROM     tb_smart_month_section_fact "+
				"            WHERE    monthcode = '"+monthcode+"' "+
				"    ) A, "+
				"    (         "+
				"            SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"            FROM     tb_smart_month_panel_seg "+
				"            WHERE    monthcode = '"+monthcode+"' "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    GROUP BY A.site_id, A.section_id "+
				") A, "+
				"(         "+
				"    SELECT   site_id, site_name, url_link "+
				"    FROM     tb_site_info "+
				"    WHERE    exp_time >= to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 " +
				"	 and      ef_time  <  to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 " +
				") B "+
				"WHERE    A.site_id = B.site_id ";
		//System.out.println(query);
		//System.exit(0);
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthCSection
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthCSection lvl2 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthCSection(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month CSection lvl2 is processing...");
		String query = 
				"INSERT  INTO TB_SMART_MONTH_CSECTION "+
				"(         "+
				"    monthcode, site_id, section_id, site_name, url_link, "+
				"    reach_rate, uu_cnt, pv_cnt, avg_duration, "+
				"    daily_freq_cnt, uu_cnt_adj, pv_cnt_adj, reach_rate_adj,  "+
				"    uu_overall_rank,  "+
				"    pv_overall_rank,  "+
				"    avg_duration_overall_rank,  "+
				"    freq_overall_rank, "+
				"    tot_duration_overall_rank, "+
				"    visit_cnt, visit_cnt_adj, tot_duration_adj, proc_date "+
				") "+
				"SELECT  /*+ordered use_hash(b,a)*/ "+
				"        A.monthcode, A.site_id, A.section_id, B.site_name, B.url_link, "+
				"        A.reach_rate, A.uu_cnt, A.pv_cnt, A.avg_duration, "+
				"        A.daily_freq_cnt, A.uu_cnt_adj, A.pv_cnt_adj, A.reach_rate_adj,  "+
				"        rank() over (partition by A.section_id order by uu_cnt_adj desc) uu_overall_rank, "+
				"        rank() over (partition by A.section_id order by pv_cnt_adj desc) pv_overall_rank, "+
				"        rank() over (partition by A.section_id order by avg_duration desc) avg_duration_overall_rank, "+
				"        rank() over (partition by A.section_id order by daily_freq_cnt desc) daily_freq_overall_rank, "+
				"        rank() over (partition by A.section_id order by tot_duration_adj desc) tot_duration_overall_rank, "+
				"        visit_cnt, visit_cnt_adj, tot_duration_adj, sysdate proc_date "+
				"FROM      "+
				"(         "+
				"    SELECT  /*+use_hash(b,a)*/ "+
				"            '"+monthcode+"' monthcode, A.site_id, A.section_id, "+
				"            count(*)/fn_smart_month_count('"+monthcode+"')*100 reach_rate, "+
				"            count(*) uu_cnt, sum(pv_cnt) pv_cnt,  "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"            sum(B.mo_n_factor) uu_cnt_adj, sum(A.pv_cnt*B.mo_p_factor) pv_cnt_adj, "+
				"            sum(B.mo_n_factor)/fn_smart_month_nfactor('"+monthcode+"')*100 reach_rate_adj, "+
				"            sum(visit_cnt) visit_cnt, "+
				"            sum(visit_cnt*mo_p_factor) visit_cnt_adj, "+
				"            round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj "+
				"    FROM "+
				"    (         "+
				"        SELECT   site_id, section_id, panel_id, pv_cnt, duration, daily_freq_cnt, visit_cnt "+
				"        FROM     tb_smart_month_csection_fact "+
				"        WHERE    monthcode = '"+monthcode+"' "+
				"    ) A, "+
				"    (         "+
				"        SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"        FROM     tb_smart_month_panel_seg "+
				"        WHERE    monthcode = '"+monthcode+"' "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    GROUP BY A.site_id, A.section_id "+
				") A, "+
				"(         "+
				"    SELECT   /*+ index(a, pk_site_info) */  "+
				"             site_id, site_name, url_link "+
				"    FROM     tb_site_info a "+
				"    WHERE    exp_time >= to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"	 AND      ef_time  < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+	
				") B "+
				"WHERE    A.site_id = B.site_id ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthSectionURL
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthSectionURL 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthSectionURL(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Section URL is processing...");
		executeQueryExecute("delete tb_smart_temp_section_fact where access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') ");
		
		String query = 
//				"INSERT  INTO tb_smart_temp_section_fact "+
//				"(access_day, site_id, section_id, panel_id, path_url, pv_cnt, duration, proc_date) "+
//				"SELECT  /*+use_hash(b,a) ordered(b,a)*/ "+
//				"        access_day, a.site_id, a.section_id, a.panel_id, path_url, sum(pv_cnt) pv_cnt, sum(duration) duration, sysdate "+
//				"FROM    tb_smart_month_temp_section a, tb_smart_section_path b         "+
//				"WHERE   a.site_id = b.site_id                 "+
//				"and     a.section_id = b.section_id                 "+
//				"and     a.path_id = b.path_id                 "+
//				"and     b.type_cd in ('P','Q')                 "+
//				"AND   	a.access_day >= '"+monthcode+"'||'01' "+
//				"AND   	a.access_day <= fn_month_lastday('"+monthcode+"') "+
//				"GROUP BY a.access_day, a.site_id, a.section_id, a.panel_id, b.path_url "+
//				"having sum(pv_cnt) > 0 ";
				"INSERT  INTO tb_smart_temp_section_fact  "+
				"(access_day, site_id, section_id, panel_id, path_url, pv_cnt, duration, proc_date)  "+
				"SELECT  /*+use_hash(b,a) ordered(b,a)*/  "+
				"        access_day, site_id, section_id, panel_id, DOMAIN_URL, sum(pv_cnt) pv_cnt, sum(duration) duration, sysdate  "+
				"FROM    tb_smart_month_temp_section "+
				"WHERE   access_day >= '"+monthcode+"'||'01'  "+
				"AND   	access_day <= fn_month_lastday('"+monthcode+"')  "+
				"GROUP BY access_day, site_id, section_id, panel_id, DOMAIN_URL  "+
				"having sum(pv_cnt) > 0 ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String query2 = 
				"INSERT  INTO TB_SMART_MONTH_SECTION_URL "+
				"( "+
				"        monthcode, site_id, path_url, section_id, "+
				"        category_code1, category_code2, category_code3, "+
				"        reach_rate, uu_cnt, pv_cnt, avg_duration, "+
				"        daily_freq_cnt, uu_cnt_adj, pv_cnt_adj, "+
				"        reach_rate_adj, tot_duration_adj, proc_date "+
				") "+
				"SELECT  /*+use_hash(b,a)*/  "+
				"        A.monthcode, A.site_id, A.path_url, A.section_id, "+
				"        B.category_code1, B.category_code2, B.category_code3, "+
				"        A.reach_rate, A.uu_cnt, A.pv_cnt, A.avg_duration, "+
				"        A.daily_freq_cnt, A.uu_cnt_adj, A.pv_cnt_adj, "+
				"        A.reach_rate_adj, A.tot_duration_adj, sysdate proc_date "+
				"FROM "+
				"(        "+
				"        SELECT  /*+use_hash(b,a)*/  "+
				"                '"+monthcode+"' monthcode,  "+
				"                A.site_id, A.path_url, A.section_id, "+
				"                count(*)/fn_smart_month_count('"+monthcode+"')*100 reach_rate, "+
				"                count(*) uu_cnt, sum(pv) pv_cnt,  "+
				"                round(decode(sum(B.mo_n_factor),0,1, sum(dt*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"                round(decode(sum(B.mo_n_factor),0,1, sum(fq*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"                sum(B.mo_n_factor) uu_cnt_adj, sum(A.pv*B.mo_p_factor) pv_cnt_adj, "+
				"                sum(B.mo_n_factor)/fn_smart_month_nfactor('"+monthcode+"')*100 reach_rate_adj, "+
				"                sum(A.dt*B.mo_p_factor) tot_duration_adj "+
				"        FROM "+
				"        (         "+
				"                SELECT   site_id, section_id, path_url, panel_id, sum(pv_cnt) pv, sum(duration) dt, count(distinct access_day) fq "+
				"                FROM     tb_smart_temp_section_fact "+
				"                WHERE    access_day >= '"+monthcode+"'||'01' "+
				"                AND      access_day <= fn_month_lastday('"+monthcode+"') "+
				"                GROUP BY site_id, section_id, path_url, panel_id "+
				"        ) A, "+
				"        (         "+
				"                SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"                FROM     tb_smart_month_panel_seg "+
				"                WHERE    monthcode = '"+monthcode+"' "+
				"        ) B "+
				"        WHERE    A.panel_id = B.panel_id "+
				"        GROUP BY A.site_id, A.section_id, A.path_url "+
				") A, "+
				"(         "+
				"        SELECT   site_id, category_code1, category_code2, category_code3 "+
				"        FROM     tb_site_info "+
				"        WHERE    exp_time >= to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 " +
				"		 and      ef_time  < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				") B "+
				"WHERE    A.site_id = B.site_id ";
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		//System.out.println(query2);
		//System.exit(0);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekSectionURL
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSectionURL 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekSectionURL(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Week Section URL is processing...");
		String queryT = 
				"truncate table TB_SMART_WTEMP_SECTION_FACT ";
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		String query = 
//				"INSERT  INTO TB_SMART_WTEMP_SECTION_FACT "+
//				"(access_day, site_id, section_id, panel_id, path_url, pv_cnt, duration, proc_date) "+
//				"SELECT  /*+parallel(a,8) use_hash(b,a) ordered(b,a)*/ "+
//				"        access_day, a.site_id, a.section_id, a.panel_id, path_url, sum(pv_cnt) pv_cnt, sum(duration) duration, sysdate "+
//				"FROM    tb_smart_week_temp_section a, tb_smart_section_path b         "+
//				"WHERE   a.site_id = b.site_id                 "+
//				"and     a.section_id = b.section_id                 "+
//				"and     a.path_id = b.path_id                 "+
//				"and     b.type_cd in ('P','Q')                 "+
//				"AND   	 a.access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
//				"AND   	 a.access_day <= '"+accessday+"' "+
//				"GROUP BY a.access_day, a.site_id, a.section_id, a.panel_id, b.path_url "+
//				"having sum(pv_cnt) > 0 ";
				"INSERT  INTO TB_SMART_WTEMP_SECTION_FACT  "+
				"(access_day, site_id, section_id, panel_id, path_url, pv_cnt, duration, proc_date)  "+
				"SELECT  /*+use_hash(b,a) ordered(b,a)*/  "+
				"        access_day, site_id, section_id, panel_id, DOMAIN_URL, sum(pv_cnt) pv_cnt, sum(duration) duration, sysdate  "+
				"FROM    tb_smart_week_temp_section "+
				"WHERE 	 access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"AND   	 access_day <= '"+accessday+"' "+
				"GROUP BY access_day, site_id, section_id, panel_id, DOMAIN_URL  "+
				"having sum(pv_cnt) > 0 ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String query2 = 
				"INSERT  INTO TB_SMART_WEEK_SECTION_URL "+
				"( "+
				"        weekcode, site_id, path_url, section_id, "+
				"        category_code1, category_code2, category_code3, "+
				"        reach_rate, uu_cnt, pv_cnt, avg_duration, "+
				"        daily_freq_cnt, uu_cnt_adj, pv_cnt_adj, "+
				"        reach_rate_adj, tot_duration_adj, proc_date "+
				") "+
				"SELECT  /*+use_hash(b,a)*/  "+
				"        A.weekcode, A.site_id, A.path_url, A.section_id, "+
				"        B.category_code1, B.category_code2, B.category_code3, "+
				"        A.reach_rate, A.uu_cnt, A.pv_cnt, A.avg_duration, "+
				"        A.daily_freq_cnt, A.uu_cnt_adj, A.pv_cnt_adj, "+
				"        A.reach_rate_adj, A.tot_duration_adj, sysdate proc_date "+
				"FROM "+
				"(        "+
				"        SELECT  /*+use_hash(b,a)*/  "+
				"                A.weekcode,  "+
				"                A.site_id, A.path_url, A.section_id, "+
				"                count(*)/fn_smart_week_count(A.weekcode)*100 reach_rate, "+
				"                count(*) uu_cnt, sum(pv) pv_cnt,  "+
				"                round(decode(sum(B.mo_n_factor),0,1, sum(dt*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"                round(decode(sum(B.mo_n_factor),0,1, sum(fq*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"                sum(B.mo_n_factor) uu_cnt_adj, sum(A.pv*B.mo_p_factor) pv_cnt_adj, "+
				"                sum(B.mo_n_factor)/fn_smart_week_nfactor(A.weekcode)*100 reach_rate_adj, "+
				"                sum(A.dt*B.mo_p_factor) tot_duration_adj "+
				"        FROM "+
				"        (         "+
				"                SELECT   /*+ordered*/ fn_weekcode(access_day) weekcode, site_id, section_id, path_url, panel_id,  "+
				"                         sum(pv_cnt) pv, sum(duration) dt, count(distinct access_day) fq "+
				"                FROM     TB_SMART_WTEMP_SECTION_FACT "+
				"                where    access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"                AND      access_day <= '"+accessday+"' "+
				"                GROUP BY fn_weekcode(access_day), site_id, section_id, path_url, panel_id "+
				"        ) A, "+
				"        ( "+
				"                SELECT   weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"                FROM     tb_smart_panel_seg "+
				"                WHERE weekcode = fn_weekcode('"+accessday+"') "+
				"        ) B "+
				"        WHERE    A.panel_id = B.panel_id "+
				"        and      A.weekcode = B.weekcode "+
				"        GROUP BY A.weekcode, A.site_id, A.section_id, A.path_url "+
				") A, "+
				"(         "+
				"        SELECT   /*+index(a,PK_SITE_INFO)*/site_id, category_code1, category_code2, category_code3 "+
				"        FROM     tb_site_info "+
				"        WHERE    exp_time >= sysdate "+
				") B "+
				"WHERE    A.site_id = B.site_id ";
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
//		System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthSectionSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthSectionSum 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthSectionSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Section Summary is processing...");
		String query = 
				"insert into TB_SMART_MONTH_CSECTION_SUM "+
				"SELECT      A.monthcode, A.SECTION_ID, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"            rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"            round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"            round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"            round(sum(B.mo_n_factor)/FN_SMART_MONTH_NFACTOR('"+monthcode+"')*100,5) reach_rate_adj, "+
				"            round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"            round(sum(A.visit_cnt*B.mo_p_factor),5) visit_cnt_adj, "+
				"            sysdate "+
				"FROM "+
				"(        SELECT   MONTHCODE, PANEL_ID, SECTION_ID, sum(VISIT_CNT) VISIT_CNT,  "+
				"                  SUM(PV_CNT) PV_CNT, SUM(DURATION) DURATION, SUM(DAILY_FREQ_CNT) DAILY_FREQ_CNT "+
				"         FROM     tb_smart_month_csection_fact "+
				"         WHERE    monthcode = '"+monthcode+"' "+
				"         group by MONTHCODE, PANEL_ID, SECTION_ID "+
				") A, "+
				"(        SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"         FROM     tb_smart_month_panel_seg "+
				"         WHERE    monthcode = '"+monthcode+"' "+
				") B "+
				"WHERE    A.panel_id = B.panel_id "+
				"GROUP BY A.monthcode, A.SECTION_ID ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String query2 = 
				"insert into TB_SMART_MONTH_SECTION_SUM "+
				"SELECT      A.monthcode, A.SECTION_ID, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"            rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"            round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"            round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"            round(sum(B.mo_n_factor)/FN_SMART_MONTH_NFACTOR('"+monthcode+"')*100,5) reach_rate_adj, "+
				"            round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"            round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"            round(sum(A.visit_cnt*B.mo_p_factor),5) visit_cnt_adj, "+
				"            sysdate "+
				"FROM "+
				"(        SELECT   MONTHCODE, PANEL_ID, SECTION_ID, sum(VISIT_CNT) VISIT_CNT,  "+
				"                  SUM(PV_CNT) PV_CNT, SUM(DURATION) DURATION, SUM(DAILY_FREQ_CNT) DAILY_FREQ_CNT "+
				"         FROM     tb_smart_month_section_fact "+
				"         WHERE    monthcode = '"+monthcode+"' "+
				"         group by MONTHCODE, PANEL_ID, SECTION_ID "+
				") A, "+
				"(        SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"         FROM     tb_smart_month_panel_seg "+
				"         WHERE    monthcode = '"+monthcode+"' "+
				") B "+
				"WHERE    A.panel_id = B.panel_id "+
				"GROUP BY A.monthcode, A.SECTION_ID ";
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
//		System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppSeg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 월간 인구통계 교차 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppSeg(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month App Seg is processing...");
		String query = 
				"insert  into TB_SMART_month_SEG_APP "+
				"select b.monthcode, b.kc_seg_id, b.smart_id, b.package_name, APP_CATEGORY_CD1, APP_CATEGORY_CD2, age_cls, sex_cls, income_cls, job_cls, "+
				"    education_cls, ismarried_cls, region_cd, nvl(uu_cnt,0) uu_cnt, nvl(uu_est_cnt,0) uu_est_cnt, nvl(avg_duration,0) avg_duration, "+
				"    nvl(duration_est,0) duration_est, nvl(avg_daily_freq_cnt,0) avg_daily_freq_cnt, nvl(app_cnt,0) app_cnt, "+
				"    nvl(app_est_cnt,0) app_est_cnt, nvl(avg_pv_est_cnt,0) avg_pv_est_cnt, nvl(reach_rate,0) reach_rate, sysdate, iu_cnt, iu_est_cnt "+
				"from "+
				"( "+
				"    select  v_fact.monthcode, "+
				"            kc_seg_id, "+
				"            v_fact.smart_id, "+
				"            count(distinct v_fact.panel_id) uu_cnt, "+
				"            round(sum(v_panel_seg.mo_n_factor),5) uu_est_cnt, "+
				"            round(decode(sum(mo_n_factor),0,0, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"            round(sum(duration*v_panel_seg.mo_p_factor),5) duration_est, "+
				"            round(decode(sum(mo_n_factor),0,0, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"            sum(app_cnt) app_cnt, "+
				"            round(sum(app_cnt*v_panel_seg.mo_p_factor),5) app_est_cnt, "+
				"            round(decode(sum(mo_n_factor),0,0, sum(app_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				"            round(sum(v_panel_seg.mo_n_factor)/ fn_smart_month_nfactor(v_fact.monthcode)*100, 5) reach_rate, "+
				"            sysdate "+
				"    from "+
				"    ( "+
				"            select  /*+ index(a,PK_SMART_month_APP_FACT)*/ monthcode, smart_id, package_name, panel_id, duration, daily_freq_cnt, app_cnt "+
				"            from    tb_smart_month_app_fact a "+
				"            where   monthcode = '"+monthcode+"' "+
				"    ) v_fact, "+
				"    ( "+
				"            select  monthcode, panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"            from    tb_smart_month_panel_seg "+
				"            where   monthcode = '"+monthcode+"' "+
				"    ) v_panel_seg "+
				"    where v_fact.monthcode = v_panel_seg.monthcode "+
				"    and v_fact.panel_id   = v_panel_seg.panel_id "+
				"    group by v_fact.monthcode, v_panel_seg.kc_seg_id, v_fact.smart_id, package_name "+
				")a, "+               
				"( "+
				"    select  v_setup.monthcode, "+
				"            kc_seg_id, "+
				"            v_setup.smart_id, "+
				"            v_setup.package_name, "+
				"            min(APP_CATEGORY_CD1) APP_CATEGORY_CD1, "+
				"            min(APP_CATEGORY_CD2) APP_CATEGORY_CD2, "+
				"            min(v_panel_seg.age_cls)        age_cls, "+
				"            min(v_panel_seg.sex_cls)        sex_cls, "+
				"            min(v_panel_seg.income_cls)     income_cls, "+  
				"            min(v_panel_seg.job_cls)        job_cls, "+
				"            min(v_panel_seg.education_cls)  education_cls, "+
				"            min(v_panel_seg.ismarried_cls ) ismarried_cls, "+
				"            min(v_panel_seg.region_cd )     region_cd, "+
				"            count(*) iu_cnt, "+
				"            round(sum(v_panel_seg.mo_n_factor)*fn_month_modifier(v_setup.monthcode),5) iu_est_cnt "+
				"    from "+  
				"    ( "+
				"            select  /*+leading(b,a) index(a,PK_SMART_month_APP_FACT) index(b,PK_SMART_MONTH_APP_SUM)*/ b.monthcode, b.smart_id, b.package_name, panel_id, "+
				"                     APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				"            from    tb_smart_month_setup_fact a, tb_smart_month_app_sum b "+
				"            where    b.monthcode = '"+monthcode+"' "+
				"            and      a.monthcode = '"+monthcode+"' "+
				"            AND      a.smart_id = b.smart_id "+
				"    ) v_setup, "+
				"    ( "+
				"            select  panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"            from    tb_smart_month_panel_seg "+
				"            where   monthcode = '"+monthcode+"' "+
				"    ) v_panel_seg "+
				"    where v_setup.panel_id   = v_panel_seg.panel_id "+
				"    group by monthcode, v_panel_seg.kc_seg_id, v_setup.smart_id, v_setup.package_name "+
				")b "+
				"where a.monthcode(+) = b.monthcode "+
				"and a.smart_id(+) = b.smart_id "+
				"and a.kc_seg_id(+) = b.kc_seg_id";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
	
//		System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthAppSum 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		
		System.out.print("The batch - Month Application Summary is processing...");
		
		String queryT = "truncate table tb_temp_smart_app_info";
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		System.out.println("truncate tb_temp_smart_app_info DONE.");
		
		
		queryT = "insert into tb_temp_smart_app_info "+
				"SELECT   PRO_ID,SMART_ID,PACKAGE_NAME,APP_NAME,APP_CATEGORY_CD1,APP_CATEGORY_CD2,EF_TIME,EXP_TIME,SITE_ID,ENG_APP_NAME,P_SMART_ID, 'ALL' TYPE "+
				"FROM     tb_smart_app_info b "+
				"WHERE    exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+ 
				"AND      ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"and      p_smart_id is null ";
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		
		queryT = "insert into tb_temp_smart_app_info "+
				"SELECT   PRO_ID,SMART_ID,PACKAGE_NAME,APP_NAME,APP_CATEGORY_CD1,APP_CATEGORY_CD2,EF_TIME,EXP_TIME,SITE_ID,ENG_APP_NAME,P_SMART_ID, 'EQUAL' TYPE "+
				"FROM     tb_smart_app_info b "+
				"WHERE    exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+ 
				"AND      ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"and      p_smart_id is not null  ";
		
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		System.out.println("Insertion tb_temp_smart_app_info DONE.");
		
		
		String query = 
				"insert into tb_smart_month_app_sum "+
				"select a.monthcode, a.smart_id, a.package_name, a.app_name, a.pro_id, a.site_id, "+
				"       app_category_cd1, app_category_cd2, reach_rate, a.uu_cnt, uu_overall_rank, uu_1level_rank, uu_2level_rank, "+
				"       avg_duration, avg_duration_overall_rank, avg_duration_1level_rank, avg_duration_2level_rank, daily_freq_cnt, "+
				"       a.uu_cnt_adj, reach_rate_adj, freq_overall_rank, tot_duration_overall_rank, tot_duration_adj, "+
				"       b.uu_cnt install_uu_cnt, b.uu_cnt_adj install_uu_cnt_adj, round(a.uu_cnt/b.uu_cnt*100,2) install_rate, "+
				"       round(a.uu_cnt_adj/b.uu_cnt_adj*100,2) install_rate_adj, sysdate, app_cnt_adj, P_UU_CNT_ADJ, P_TOT_DURATION_ADJ, null rr_error, null tts_error "+
				"from "+
				"( "+
				"    SELECT   /*+ordered use_hash(b,a)*/ "+
				"             A.monthcode,  SMART_ID, PACKAGE_NAME, APP_NAME, PRO_ID, SITE_ID, "+
				"             min(app_category_cd1) app_category_cd1, min(app_category_cd2) app_category_cd2, "+
				"             round(count(A.panel_id)/FN_SMART_MONTH_COUNT('"+monthcode+"')*100,2) reach_rate, "+
				"             count(A.panel_id) uu_cnt, "+
				"             rank() over (partition by type order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"             rank() over (partition by type, min(app_category_cd1) order by sum(B.mo_n_factor) desc) uu_1level_rank, "+
				"             rank() over (partition by type, min(app_category_cd2) order by sum(B.mo_n_factor) desc) uu_2level_rank, "+
				"             round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"             rank() over (partition by type order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"             rank() over (partition by type, min(app_category_cd1) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_1level_rank, "+
				"             rank() over (partition by type, min(app_category_cd2) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_2level_rank, "+
				"             round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"             round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"             round(sum(B.mo_n_factor)/FN_SMART_MONTH_NFACTOR('"+monthcode+"')*100,5) reach_rate_adj, "+
				"             rank() over (partition by type order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"             rank() over (partition by type order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
				"             round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"             round(sum(app_cnt*mo_p_factor),5) app_cnt_adj, "+
				"             null P_UU_CNT_ADJ, "+
				"             null P_TOT_DURATION_ADJ "+
				"    FROM "+
				"    ( "+
				"        SELECT   /*+use_hash(b,a) index(a,PK_SMART_MONTH_APP_FACT)*/ MONTHCODE, a.SMART_ID, a.PACKAGE_NAME, PANEL_ID, DURATION,  "+
				"                 DAILY_FREQ_CNT, PRO_ID, APP_NAME,  "+
				"                 APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, app_cnt, 'ALL' type "+
				"        FROM     tb_smart_month_app_fact a, tb_temp_smart_app_info b "+
				"        WHERE    monthcode = '"+monthcode+"' "+
				"        AND      a.smart_id = b.smart_id "+
				"        and      type = 'ALL' "+
				"        union all "+
				"        SELECT   /*+use_hash(b,a) index(a,PK_SMART_MONTH_APP_FACT)*/ MONTHCODE, a.SMART_ID, a.PACKAGE_NAME, PANEL_ID, DURATION,  "+
				"                 DAILY_FREQ_CNT, PRO_ID, APP_NAME,  "+
				"                 APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, app_cnt, 'EQUAL' type "+ 
				"        FROM     tb_smart_month_app_fact a, tb_temp_smart_app_info b "+
				"        WHERE    monthcode = '"+monthcode+"' "+
				"        AND      a.smart_id = b.smart_id "+
				"        and      type = 'EQUAL' "+		
				"    ) A, "+
				"    ( "+
				"        SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"        FROM     tb_smart_month_panel_seg "+
				"        WHERE    monthcode = '"+monthcode+"' "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    GROUP BY A.monthcode, SMART_ID, PACKAGE_NAME, APP_NAME, PRO_ID, SITE_ID, TYPE "+
				") a, "+
				"( "+
				"    select  /*+use_hash(b,a)*/  "+
				"            a.monthcode, smart_id, "+
				"            count(A.panel_id) uu_cnt, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj "+
				"    from "+
				"    ( "+
				"        select /*+ use_hash(b,a) full(b) */ monthcode, panel_id, a.smart_id "+
				"        from tb_smart_month_setup_FACT a, tb_temp_smart_app_info b "+
				"        where monthcode = '"+monthcode+"' "+
				"        and a.smart_id = b.smart_id "+
				"        group by monthcode, panel_id, a.smart_id "+
				"    ) a, "+
				"    ( "+
				"        select MONTHCODE, PANEL_ID, mo_n_factor "+
				"        from tb_smart_month_panel_seg "+
				"        where monthcode = '"+monthcode+"' "+
				"    ) b "+
				"    where a.monthcode=b.monthcode "+
				"    and a.panel_id=b.panel_id "+
				"    group by a.monthcode, smart_id "+
				") b "+
				"where a.monthcode = b.monthcode "+
				"and   a.smart_id = b.smart_id ";
//		System.out.println(query);
//		System.exit(0);
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		
		String query1 =
				"update (select /*+index(tb_smart_month_app_sum,pk_smart_month_app_sum)*/ "+
						"            monthcode, smart_id , uu_cnt_adj, tot_duration_adj ,p_tot_duration_adj, p_uu_cnt_adj "+
						"        from  tb_smart_month_app_sum where monthcode ='"+monthcode+"') a "+
						"set (p_uu_cnt_adj, p_tot_duration_adj) = "+
						"(   "+
						"    select   uv - a.uu_cnt_adj, tts - a.tot_duration_adj "+
						"    from "+
						"    ( "+
						"        select /*+use_hash(b,a)*/ "+
						"            a.monthcode, smart_id, package_name, round(sum(mo_n_factor),5) uv, round(sum(mo_p_factor*duration),5) tts "+
						"        from "+
						"        ( "+
						"            select monthcode, panel_id, smart_id, package_name, sum(duration) duration "+
						"            from  "+
						"            ( "+
						"                select /*+index(a,pk_smart_month_app_fact)*/ monthcode, panel_id, smart_id, package_name, duration "+
						"                from tb_smart_month_app_fact a"+
						"                where monthcode = '"+monthcode+"' "+
						"                union all "+
						"                select fn_monthcode(access_day) monthcode, panel_id, smart_id, package_name, duration "+
						"                from tb_smart_day_push_panel_fact "+
						"                where access_day like '"+monthcode+"%' "+
						"            ) "+
						"            group by monthcode, panel_id, smart_id, package_name "+
						"        )a, "+
						"        ( "+
						"            select monthcode, panel_id, mo_n_factor, mo_p_factor "+
						"            from tb_smart_month_panel_seg "+
						"            where monthcode = '"+monthcode+"' "+
						"        )b "+
						"        where a.monthcode = b.monthcode "+
						"        and a.panel_id = b.panel_id "+
						"        group by a.monthcode, smart_id, package_name "+
						"    )b "+
						"    where a.monthcode = b.monthcode "+
						"    and a.smart_id = b.smart_id "+
						") ";
		
        this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: findAppTablename
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: String
	 *		설명			: TASK의 날짜에 따른 Table명 변경에 따른 Method
	 *************************************************************************/
	
	public String findAppTablename(String accessday){
		String table_name = "tb_smart_task_itrack";

		ResultSet rs = null;
		String sql = "select tname " +
				     "from tab " +
				     "where tname like '%TB_SMART_TASK_ITRACK%' " +
				     "and tname like '%"+accessday.substring(0, 6)+"%'";
		
		try {
			this.pstmt = connection.prepareStatement(sql);
			rs = this.pstmt.executeQuery(sql);
			
			while (rs.next()) {	
				if(rs.getString("tname") != null){
					table_name = rs.getString("tname");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return table_name;
	}
	
	/**************************************************************************
	 *		메소드명		: findSetupTablename
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: String
	 *		설명			: TASK의 날짜에 따른 Table명 변경에 따른 Method
	 *************************************************************************/
	
	public String findSetupTablename(String monthcode){
		String table_name = "tb_smart_app_itrack";

		ResultSet rs = null;
		String sql = "select tname " +
				     "from tab " +
				     "where tname like '%TB_SMART_APP_ITRACK%' " +
				     "and tname like '%"+monthcode+"%'";
		
		try {
			this.pstmt = connection.prepareStatement(sql);
			rs = this.pstmt.executeQuery(sql);
			
			while (rs.next()) {	
				if(rs.getString("tname") != null){
					table_name = rs.getString("tname");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return table_name;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekDomainFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekDomainFact 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekDomainFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Weekly Domain Fact is processing...");
		
		String query = 
			"insert into TB_SMART_WEEK_DOMAIN_FACT "+
			"SELECT   fn_weekcode(access_day) monthcode, C.site_id, C.REQ_DOMAIN, C.panel_id, "+
			"         sum(C.pv_cnt) pv_cnt, sum(C.duration) duration, count(distinct C.access_day) daily_freq_cnt, sysdate "+
			"FROM      "+
			"(         "+
			"    SELECT   * "+
			"    FROM     tb_site_info "+
			"    WHERE    ef_time  < to_date('"+accessday+"','yyyymmdd') "+
			"    AND      exp_time > to_date('"+accessday+"','yyyymmdd') "+
			") B, "+
			"(        "+
			"    select access_day, REQ_SITE_ID site_id, REQ_DOMAIN, PANEL_ID, count(*) pv_cnt, sum(duration) duration "+
			"    from tb_smart_browser_itrack "+
			"    where result_cd = 'S' "+
			"    and panel_flag in ('D','V') "+
			"    and access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd')" +
			"    and access_day <= '"+accessday+"'"+
			"    and REQ_SITE_ID > 0 "+
			"    group by access_day, REQ_DOMAIN, REQ_SITE_ID, PANEL_ID "+
			") C "+
			"WHERE    C.site_id  = B.site_id "+
			"group by fn_weekcode(access_day), C.site_id, C.REQ_DOMAIN, C.panel_id ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		//System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeDomainFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Daytime_Visit 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthDomainFact(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Domain Fact is processing...");
		
		if(countTable(monthcode, "m", "tb_smart_month_domain_fact")){
			String query = 
				"insert into tb_smart_month_domain_fact "+
				"SELECT   substr(access_day,1,6) monthcode, C.site_id, C.REQ_DOMAIN, C.panel_id, "+
				"         sum(C.pv_cnt) pv_cnt, sum(C.duration) duration, count(distinct C.access_day) daily_freq_cnt, sysdate "+
				"FROM      "+
				"(         "+
				"    SELECT   * "+
				"    FROM     tb_site_info "+
				"    WHERE    ef_time  < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"    AND      exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				") B, "+
				"(        "+
				"    select access_day, REQ_SITE_ID site_id, REQ_DOMAIN, PANEL_ID, count(*) pv_cnt, sum(duration) duration "+
				"    from tb_smart_browser_itrack "+
				"    where result_cd = 'S' "+
				"    and panel_flag in ('D','V') "+
				"    and access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and REQ_SITE_ID > 0 "+
				"    group by access_day, REQ_DOMAIN, REQ_SITE_ID, PANEL_ID "+
				") C "+
				"WHERE    C.site_id  = B.site_id "+
				"group by substr(access_day,1,6), C.site_id, C.REQ_DOMAIN, C.panel_id ";
			
	        this.pstmt = connection.prepareStatement(query);
			this.pstmt.executeUpdate();
			
			//System.out.println(query);
			System.out.println("INSERTION DONE.");
			if(this.pstmt!=null) this.pstmt.close();
		} else {
			System.out.print("Monthly Domain Fact already exists.");
			System.exit(0);
		}
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekDomainSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekDomainSum 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekDomainSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Weekly Domain Sum is processing...");
		
		String query = 
				"INSERT   INTO tb_smart_week_domain_sum "+
				"(        weekcode, site_id, domain_url, "+
				"         category_code1, category_code2, category_code3, "+
				"         reach_rate, uu_cnt, UU_OVERALL_RANK, UU_1LEVEL_RANK, UU_2LEVEL_RANK, "+
				"         pv_cnt, PV_OVERALL_RANK, PV_1LEVEL_RANK, PV_2LEVEL_RANK,  "+
				"         avg_duration, AVG_DURATION_OVERALL_RANK, AVG_DURATION_1LEVEL_RANK, AVG_DURATION_2LEVEL_RANK, "+
				"         daily_freq_cnt, uu_cnt_adj, pv_cnt_adj, "+
				"         reach_rate_adj, proc_date, tot_duration_adj "+
				") "+
				"SELECT  A.weekcode, A.site_id, A.domain_url, "+
				"        category_code1, category_code2, category_code3, "+
				"        count(distinct a.panel_id)/fn_smart_week_count(A.weekcode)*100 reach_rate, "+
				"        count(distinct a.panel_id) uu_cnt,  "+
				"        rank() over (partition by A.weekcode order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"        rank() over (partition by a.weekcode, min(category_code1) order by sum(B.mo_n_factor) desc) uu_1level_rank, "+
				"        rank() over (partition by a.weekcode, min(category_code2) order by sum(B.mo_n_factor) desc) uu_2level_rank, "+
				"        sum(PV_CNT) pv_cnt,  "+
				"        rank() over (partition by A.weekcode order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_overall_rank, "+
				"        rank() over (partition by a.weekcode, min(category_code1) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_1level_rank, "+
				"        rank() over (partition by a.weekcode, min(category_code2) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_2level_rank, "+
				"        round(decode(sum(B.mo_n_factor),0,1, sum(DURATION*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"        rank() over (partition by A.weekcode order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"        rank() over (partition by a.weekcode, min(category_code1) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_1level_rank, "+
				"        rank() over (partition by a.weekcode, min(category_code2) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_2level_rank, "+
				"        round(decode(sum(B.mo_n_factor),0,1, sum(DAILY_FREQ_CNT*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"        sum(B.mo_n_factor) uu_cnt_adj, sum(A.PV_CNT*B.mo_p_factor) pv_cnt_adj, "+
				"        sum(B.mo_n_factor)/fn_smart_week_nfactor(A.weekcode)*100 reach_rate_adj, "+
				"        sysdate proc_date, "+
				"        sum(A.DURATION*B.mo_p_factor) tot_duration_adj "+
				"FROM "+
				"(     SELECT   /*+use_hash(a,b) index(a,PK_SMART_WEEK_DOMAIN_FACT)*/ a.weekcode, a.site_id, CATEGORY_CODE1, CATEGORY_CODE2, CATEGORY_CODE3,  "+
				"               domain_url, panel_id, PV_CNT, DURATION, DAILY_FREQ_CNT "+
				"      FROM     tb_smart_week_domain_fact a, tb_site_info b "+
				"      WHERE    a.WEEKCODE = fn_weekcode('"+accessday+"') "+
				"      AND      a.site_id = b.site_id "+
				"      AND      b.exp_time > sysdate "+
				"      AND      b.ef_time < sysdate "+
				") A, "+
				"(     SELECT   weekcode, panel_id, mo_n_factor, mo_p_factor "+
				"      FROM     tb_smart_panel_seg "+
				"      WHERE    WEEKCODE = fn_weekcode('"+accessday+"') "+
				") B "+
				"WHERE    A.panel_id = B.panel_id "+
				"AND      A.weekcode = B.weekcode "+
				"GROUP BY A.weekcode, A.site_id, A.domain_url, category_code1, category_code2, category_code3 ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeeklyLoyalty
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeeklyLoyalty 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeeklyLoyalty(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Weekly Loyalty is processing...");
		
		String query = 
				"INSERT   INTO tb_smart_day_loyalty_sum "+
				"SELECT   /*+index(a,PK_SMART_WEEK_FACT)*/fn_weekcode('"+accessday+"') weekcode, site_id, daily_freq_cnt no_of_day, count(*) panel_cnt, sysdate proc_date "+
				"FROM     tb_smart_week_fact a "+
				"WHERE    weekcode = fn_weekcode('"+accessday+"') "+
				"GROUP BY site_id, daily_freq_cnt ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppLoyalty
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeeklyLoyalty 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekAppLoyalty(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Week App Loyalty is processing...");
		//占쎈�잞옙��
		String query = 
				"insert into tb_smart_day_app_loyalty_sum "+
				"SELECT   /*+ leading(b) use_hash(b,a) index(a,pk_smart_week_app_fact)*/fn_weekcode('"+accessday+"') weekcode, smart_id, daily_freq_cnt no_of_day, count(*) panel_cnt, sysdate proc_date  "+
				"FROM     tb_smart_week_app_fact a, tb_smart_panel_Seg b "+
				"WHERE    a.weekcode = fn_weekcode('"+accessday+"') "+
				"and      a.weekcode = b.weekcode "+
				"and      a.panel_id = b.panel_id "+
				"and      b.weekcode = fn_weekcode('"+accessday+"') "+
				"GROUP BY a.smart_id, daily_freq_cnt ";

        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		//System.out.println(query);
		//System.exit(0);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekSiteSwitch
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSiteSwitch 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekSiteSwitch(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Week Site Switch is processing...");
		
		String query = 
				"INSERT   INTO tb_smart_week_site_switch "+
				"(WEEKCODE, SITE_ID, SITE_NAME, LAST_UV_ADJ, THIS_UV_ADJ, KEEP_RATE, NEW_RATE, LEAVE_RATE) "+
				"SELECT   /*+use_hash(b,a) use_hash(c,a)*/  "+
				"         fn_weekcode('"+accessday+"'),  A.site_id, B.site_name, last_uv_adj, this_uv_adj, "+
				"         round(last_keep_uv/(last_keep_uv+leave_uv)*100,2) keep_rate,  "+
				"         round(new_uv/(this_keep_uv+new_uv)*100,2) new_rate,  "+
				"         round(leave_uv/(last_keep_uv+leave_uv)*100,2) leave_rate "+
				"FROM      "+
				"( "+
				"    SELECT  /*+use_hash(b,a)*/ A.site_id, "+
				"             round(sum(case when cnt=2 then kcnf1 else 0 end)*fn_week_modifier(fn_before_weekcode(fn_weekcode('"+accessday+"'),1))) last_keep_uv,       "+
				"             round(sum(case when cnt=2 then kcnf2 else 0 end)*fn_week_modifier(fn_weekcode('"+accessday+"'))) this_keep_uv,  "+
				"             round(sum(case when cnt=1 and max_weekcode=fn_before_weekcode(fn_weekcode('"+accessday+"'),1) then kcnf1 else 0 end) "+
				"                                                                                 *fn_week_modifier(fn_before_weekcode(fn_weekcode('"+accessday+"'),1))) leave_uv, "+
				"             round(sum(case when cnt=1 and max_weekcode=fn_weekcode('"+accessday+"') then kcnf2 else 0 end) "+
				"                                                                                  *fn_week_modifier(fn_weekcode('"+accessday+"'))) new_uv "+
				"    FROM "+
				"    (         "+
				"         SELECT   site_id, panel_id, count(*) cnt, max(weekcode) max_weekcode "+
				"         FROM     tb_smart_week_fact "+
				"         WHERE    weekcode in (fn_before_weekcode(fn_weekcode('"+accessday+"'),1), fn_weekcode('"+accessday+"')) "+
				"         GROUP BY site_id, panel_id  "+
				"     ) A, "+
				"     (         "+
				"          SELECT   panel_id, "+
				"                   sum(case when weekcode=fn_before_weekcode(fn_weekcode('"+accessday+"'),1) then mo_n_factor end) kcnf1, "+
				"                   sum(case when weekcode=fn_weekcode('"+accessday+"') then mo_n_factor end) kcnf2 "+
				"          FROM     tb_smart_panel_seg "+
				"          WHERE    weekcode in (fn_before_weekcode(fn_weekcode('"+accessday+"'),1), fn_weekcode('"+accessday+"'))  "+
				"          GROUP BY panel_id "+
				"          HAVING   count(*) >= 2 "+
				"      ) B "+
				"      WHERE    A.panel_id = B.panel_id "+
				"      GROUP BY A.site_id "+
				") A, "+
				"(         "+
				"    SELECT   /*+index(a,PK_SMART_WEEK_SITE_SUM)*/site_id, site_name, uu_cnt_adj this_uv_adj                   "+
				"    FROM     tb_smart_week_site_sum a "+
				"    WHERE    weekcode = fn_weekcode('"+accessday+"') "+
				") B, "+
				"(         "+
				"    SELECT   /*+index(a,PK_SMART_WEEK_SITE_SUM)*/site_id, site_name, uu_cnt_adj  last_uv_adj "+
				"    FROM     tb_smart_week_site_sum a "+
				"    WHERE    weekcode = fn_before_weekcode(fn_weekcode('"+accessday+"'),1) "+
				") C "+
				"WHERE    A.site_id = B.site_id "+
				"AND      A.site_id = C.site_id "+
				"AND	 last_keep_uv+leave_uv> 0 "+
				"AND	 this_keep_uv+new_uv  > 0 ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekDurationTimeSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 웹 이용시간 분포 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekDurationTimeSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Week Site Duration Time Sum is processing...");
		
		String query = 
				"insert into tb_smart_week_duration_time "+
				"select   weekcode, a.site_id, site_name, uu_cnt_adj, avg_duration, "+
				"         d01, d02, d03, d04, d05, d06, d07, d08, d09, sysdate "+
				"from "+
				"( "+
				"    select   weekcode, site_id, site_name, uu_cnt_adj, avg_duration "+
				"    from     tb_smart_week_site_sum "+
				"    where    weekcode = fn_weekcode('"+accessday+"') "+
				"    and      category_code1 != 'Z' "+
				") a, "+
				"( "+
				"    select    site_id, "+
				"              sum(case when daily_avg_dt <= 20   then mo_n_factor else 0 end) d01, "+
				"              sum(case when daily_avg_dt > 20   and daily_avg_dt <= 40   then mo_n_factor else 0 end) d02, "+
				"              sum(case when daily_avg_dt > 40   and daily_avg_dt <= 60   then mo_n_factor else 0 end) d03, "+
				"              sum(case when daily_avg_dt > 60   and daily_avg_dt <= 600  then mo_n_factor else 0 end) d04, "+
				"              sum(case when daily_avg_dt > 600  and daily_avg_dt <= 1200 then mo_n_factor else 0 end) d05, "+
				"              sum(case when daily_avg_dt > 1200 and daily_avg_dt <= 1800 then mo_n_factor else 0 end) d06, "+
				"              sum(case when daily_avg_dt > 1800 and daily_avg_dt <= 2400 then mo_n_factor else 0 end) d07, "+
				"              sum(case when daily_avg_dt > 2400 and daily_avg_dt <= 3600 then mo_n_factor else 0 end) d08, "+
				"              sum(case when daily_avg_dt > 3600 then mo_n_factor else 0 end) d09 "+
				"    from "+     
				"    ( "+
				"        select   site_id, panel_id, round(sum(avg_dt)/7) daily_avg_dt "+
				"        from "+     
				"        ( "+
				"            select  /*+ use_hash(a,b)*/ "+
				"                   a.access_day, site_id, a.panel_id, "+
				"                   round((duration*B.mo_p_factor)/(B.mo_n_factor),2) avg_dt "+
				"            from "+    
				"            ( "+
				"                select  access_day, site_id, panel_id, pv_cnt, duration "+
				"                from    tb_smart_day_fact "+
				"                where   access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"                and     access_day <= '"+accessday+"' "+
				"            ) a, "+
				"            ( "+
				"                select  access_day, panel_id, mo_n_factor, mo_p_factor "+
				"                from    tb_smart_day_panel_seg "+
				"                where   access_day = '"+accessday+"' "+
				"            ) b "+
				"            where   a.panel_id = b.panel_id "+
				"        ) "+
				"        group by site_id, panel_id "+
				"    ) a, "+
				"    ( "+
				"        select   panel_id, mo_n_factor "+
				"        from     tb_smart_panel_seg "+
				"        where    weekcode = fn_weekcode('"+accessday+"') "+
				"    ) b "+
				"    where    a.panel_id = b.panel_id "+
				"    group by site_id "+
				") b "+
				"where    a.site_id   = b.site_id";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekSiteSwitch
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekSiteSwitch 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekAppSwitch(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Week App Switch is processing...");
		
		String query = 
				"INSERT   INTO tb_smart_week_app_switch "+
				"(WEEKCODE, smart_id, app_name, LAST_UV_ADJ, THIS_UV_ADJ, KEEP_RATE, NEW_RATE, LEAVE_RATE) "+
				"SELECT   /*+use_hash(b,a) use_hash(c,a)*/  "+
				"         fn_weekcode('"+accessday+"'),  A.smart_id, B.app_name, last_uv_adj, this_uv_adj, "+
				"         round(last_keep_uv/(last_keep_uv+leave_uv)*100,2) keep_rate,  "+
				"         round(new_uv/(this_keep_uv+new_uv)*100,2) new_rate,  "+
				"         round(leave_uv/(last_keep_uv+leave_uv)*100,2) leave_rate "+
				"FROM      "+
				"( "+
				"    SELECT  /*+use_hash(b,a)*/ A.smart_id, "+
				"             round(sum(case when cnt=2 then kcnf1 else 0 end)*fn_week_modifier(fn_before_weekcode(fn_weekcode('"+accessday+"'),1))) last_keep_uv,       "+
				"             round(sum(case when cnt=2 then kcnf2 else 0 end)*fn_week_modifier(fn_weekcode('"+accessday+"'))) this_keep_uv,  "+
				"             round(sum(case when cnt=1 and max_weekcode=fn_before_weekcode(fn_weekcode('"+accessday+"'),1) then kcnf1 else 0 end) "+
				"                                                                                 *fn_week_modifier(fn_before_weekcode(fn_weekcode('"+accessday+"'),1))) leave_uv, "+
				"             round(sum(case when cnt=1 and max_weekcode=fn_weekcode('"+accessday+"') then kcnf2 else 0 end) "+
				"                                                                                  *fn_week_modifier(fn_weekcode('"+accessday+"'))) new_uv "+
				"    FROM "+
				"    (         "+
				"         SELECT   smart_id, panel_id, count(*) cnt, max(weekcode) max_weekcode "+
				"         FROM     tb_smart_week_app_fact "+
				"         WHERE    weekcode in (fn_before_weekcode(fn_weekcode('"+accessday+"'),1), fn_weekcode('"+accessday+"')) "+
				"         GROUP BY smart_id, panel_id  "+
				"     ) A, "+
				"     (         "+
				"          SELECT   panel_id, "+
				"                   sum(case when weekcode=fn_before_weekcode(fn_weekcode('"+accessday+"'),1) then mo_n_factor end) kcnf1, "+
				"                   sum(case when weekcode=fn_weekcode('"+accessday+"') then mo_n_factor end) kcnf2 "+
				"          FROM     tb_smart_panel_seg "+
				"          WHERE    weekcode in (fn_before_weekcode(fn_weekcode('"+accessday+"'),1), fn_weekcode('"+accessday+"'))  "+
				"          GROUP BY panel_id "+
				"          HAVING   count(*) >= 2 "+
				"      ) B "+
				"      WHERE    A.panel_id = B.panel_id "+
				"      GROUP BY A.smart_id "+
				") A, "+
				"(         "+
				"    SELECT   smart_id, app_name, uu_cnt_adj this_uv_adj                   "+
				"    FROM     tb_smart_week_app_sum "+
				"    WHERE    weekcode = fn_weekcode('"+accessday+"') "+
				") B, "+
				"(         "+
				"    SELECT   smart_id, app_name, uu_cnt_adj  last_uv_adj "+
				"    FROM     tb_smart_week_app_sum "+
				"    WHERE    weekcode = fn_before_weekcode(fn_weekcode('"+accessday+"'),1) "+
				") C "+
				"WHERE    A.smart_id = B.smart_id "+
				"AND      A.smart_id = C.smart_id "+
				"AND	 last_keep_uv+leave_uv> 0 "+
				"AND	 this_keep_uv+new_uv  > 0 ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekAppDurationTimeSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 앱 이용시간 분포 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekAppDurationTimeSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Week App Duration Time Sum is processing...");
		
		String query = 
				"insert into TB_SMART_WEEK_APP_TIME_SUM "+
				"select   weekcode, a.smart_id, a.package_name, app_name, a.site_id, uu_cnt_adj, avg_duration, "+
				"         d01, d02, d03, d04, d05, d06, d07, d08, d09, sysdate "+
				"from "+     
				"( "+
				"    select   weekcode, smart_id, package_name, app_name, site_id, uu_cnt_adj, avg_duration "+
				"    from     tb_smart_week_app_sum "+
				"    where    weekcode = fn_weekcode('"+accessday+"') "+
				") a, "+
				"( "+
				"    select    smart_id, "+
				"              sum(case when daily_avg_dt <= 20   then mo_n_factor else 0 end) d01, "+
				"              sum(case when daily_avg_dt > 20   and daily_avg_dt <= 40   then mo_n_factor else 0 end) d02, "+
				"              sum(case when daily_avg_dt > 40   and daily_avg_dt <= 60   then mo_n_factor else 0 end) d03, "+
				"              sum(case when daily_avg_dt > 60   and daily_avg_dt <= 600  then mo_n_factor else 0 end) d04, "+
				"              sum(case when daily_avg_dt > 600  and daily_avg_dt <= 1200 then mo_n_factor else 0 end) d05, "+
				"              sum(case when daily_avg_dt > 1200 and daily_avg_dt <= 1800 then mo_n_factor else 0 end) d06, "+
				"              sum(case when daily_avg_dt > 1800 and daily_avg_dt <= 2400 then mo_n_factor else 0 end) d07, "+
				"              sum(case when daily_avg_dt > 2400 and daily_avg_dt <= 3600 then mo_n_factor else 0 end) d08, "+
				"              sum(case when daily_avg_dt > 3600 then mo_n_factor else 0 end) d09 "+
				"    from "+     
				"    ( "+
				"        select   smart_id, panel_id, round(sum(avg_dt)/7) daily_avg_dt "+
				"        from "+     
				"        ( "+
				"            select  /*+ use_hash(a,b)*/ "+
				"                   a.access_day, smart_id, a.panel_id, "+
				"                   round((duration*B.mo_p_factor)/(B.mo_n_factor),2) avg_dt "+
				"            from "+    
				"            ( "+
				"                select  access_day, smart_id, panel_id, duration "+
				"                from    tb_smart_day_app_fact "+
				"                where   access_day >= to_char(to_date('"+accessday+"','YYYYMMDD')-6,'YYYYMMDD') "+
				"                and     access_day <= '"+accessday+"' "+
				"            ) a, "+
				"            ( "+
				"                select  access_day, panel_id, mo_n_factor, mo_p_factor "+
				"                from    tb_smart_day_panel_seg "+
				"                where   access_day = '"+accessday+"' "+
				"            ) b "+
				"            where   a.panel_id = b.panel_id "+
				"        ) "+
				"        group by smart_id, panel_id "+
				"    ) a, "+
				"    ( "+
				"        select   panel_id, mo_n_factor "+
				"        from     tb_smart_panel_seg "+
				"        where    weekcode = fn_weekcode('"+accessday+"') "+
				"    ) b "+
				"    where    a.panel_id = b.panel_id "+
				"    group by smart_id "+
				") b "+
				"where    a.smart_id   = b.smart_id";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeDomainFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Daytime_Visit 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthDomainSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Domain Sum is processing...");
		
		if(countTable(monthcode, "m", "tb_smart_month_domain_sum")){
			String query = 
					"INSERT   INTO tb_smart_month_domain_sum "+
					"(        monthcode, site_id, domain_url, "+
					"         category_code1, category_code2, category_code3, "+
					"         reach_rate, uu_cnt, UU_OVERALL_RANK, UU_1LEVEL_RANK, UU_2LEVEL_RANK, "+
					"         pv_cnt, PV_OVERALL_RANK, PV_1LEVEL_RANK, PV_2LEVEL_RANK,  "+
					"         avg_duration, AVG_DURATION_OVERALL_RANK, AVG_DURATION_1LEVEL_RANK, AVG_DURATION_2LEVEL_RANK, "+
					"         daily_freq_cnt, uu_cnt_adj, pv_cnt_adj, "+
					"         reach_rate_adj, proc_date, tot_duration_adj "+
					") "+
					"SELECT  A.monthcode, A.site_id, A.domain_url, "+
					"        category_code1, category_code2, category_code3, "+
					"        count(distinct a.panel_id)/fn_smart_month_count('"+monthcode+"')*100 reach_rate, "+
					"        count(distinct a.panel_id) uu_cnt,  "+
					"        rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
					"        rank() over (partition by min(category_code1) order by sum(B.mo_n_factor) desc) uu_1level_rank, "+
					"        rank() over (partition by min(category_code2) order by sum(B.mo_n_factor) desc) uu_2level_rank, "+
					"        sum(PV_CNT) pv_cnt,  "+
					"        rank() over (order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_overall_rank, "+
					"        rank() over (partition by min(category_code1) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_1level_rank, "+
					"        rank() over (partition by min(category_code2) order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_2level_rank, "+
					"        round(decode(sum(B.mo_n_factor),0,1, sum(DURATION*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
					"        rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
					"        rank() over (partition by min(category_code1) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_1level_rank, "+
					"        rank() over (partition by min(category_code2) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_2level_rank, "+
					"        round(decode(sum(B.mo_n_factor),0,1, sum(DAILY_FREQ_CNT*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
					"        sum(B.mo_n_factor) uu_cnt_adj, sum(A.PV_CNT*B.mo_p_factor) pv_cnt_adj, "+
					"        sum(B.mo_n_factor)/fn_smart_month_nfactor('"+monthcode+"')*100 reach_rate_adj, "+
					"        sysdate proc_date, "+
					"        sum(A.DURATION*B.mo_p_factor) tot_duration_adj "+
					"FROM "+
					"(     SELECT   a.monthcode, a.site_id, CATEGORY_CODE1, CATEGORY_CODE2, CATEGORY_CODE3,  "+
					"               domain_url, panel_id, PV_CNT, DURATION, DAILY_FREQ_CNT "+
					"      FROM     tb_smart_month_domain_fact a, tb_site_info b "+
					"      WHERE    a.monthcode = '"+monthcode+"' "+
					"      AND      a.site_id = b.site_id "+
					"      AND      b.exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd') "+
					"      AND      b.ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd') "+
					") A, "+
					"(        SELECT   monthcode, panel_id, mo_n_factor, mo_p_factor "+
					"      FROM     tb_smart_month_panel_seg "+
					"      WHERE    monthcode = '"+monthcode+"' "+
					") B "+
					"WHERE    A.panel_id = B.panel_id "+
					"AND      A.monthcode = B.monthcode "+
					"GROUP BY A.monthcode, A.site_id, A.domain_url, category_code1, category_code2, category_code3 ";
			
	        this.pstmt = connection.prepareStatement(query);
			this.pstmt.executeUpdate();
			
			//System.out.println(query);
			System.out.println("INSERTION DONE.");
			if(this.pstmt!=null) this.pstmt.close();
		} else {
			System.out.print("Monthly Domain Sum already exists.");
			System.exit(0);
		}
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthLoyalty
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Daytime_Visit 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthLoyalty(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Loyalty is processing...");

		String query = 
				"INSERT   INTO tb_smart_week_loyalty_sum "+
				"SELECT fn_valid_weekcode('"+monthcode+"') weekcode, site_id, no_of_week, count(*) panel_cnt, sysdate proc_date "+
				"FROM "+
				"( "+
				"    SELECT site_id, panel_id, count(*) no_of_week "+
				"    FROM "+
				"    ( "+
				"        SELECT b.weekcode, site_id, a.panel_id "+
				"        FROM   tb_smart_day_fact a, tb_smart_panel_seg b "+
				"        WHERE  access_day between '"+monthcode+"'||'01' and to_char(next_day(to_date('"+monthcode+"'||'01','yyyymmdd'),2)-1,'yyyymmdd') "+
				"        AND    a.panel_id=b.panel_id "+
				"        AND    b.weekcode=fn_weekcode(to_char(next_day(to_date('"+monthcode+"'||'01','yyyymmdd'),2)-1,'yyyymmdd')) "+
				"        GROUP BY b.weekcode, site_id, a.panel_id "+
				"        UNION ALL "+
				"        SELECT weekcode, site_id, panel_id "+
				"        FROM   tb_smart_week_fact a "+
				"        WHERE  weekcode between fn_weekcode(to_char(next_day(to_date('"+monthcode+"'||'01','yyyymmdd'),2),'yyyymmdd')) and fn_valid_weekcode('"+monthcode+"') "+
				"        GROUP BY weekcode, site_id, panel_id "+
				"    ) "+
				"    GROUP BY site_id, panel_id "+
				") "+
				"GROUP BY site_id, no_of_week ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		//System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
			
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthSiteSwitch
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthSiteSwitch 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthSiteSwitch(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Site Switch is processing...");

		String query = 
				"INSERT   INTO tb_smart_month_site_switch "+
				"(MONTHCODE, SITE_ID, SITE_NAME, LAST_UV_ADJ, THIS_UV_ADJ, KEEP_RATE, NEW_RATE, LEAVE_RATE) "+
				"SELECT   /*+use_hash(b,a) use_hash(c,a)*/ '"+monthcode+"',  A.site_id, B.site_name, last_uv_adj, this_uv_adj, "+
				"         round(last_keep_uv/(last_keep_uv+leave_uv)*100,2) keep_rate,  "+
				"         round(new_uv/(this_keep_uv+new_uv)*100,2) new_rate,  "+
				"         round(leave_uv/(last_keep_uv+leave_uv)*100,2) leave_rate "+
				"FROM      "+
				"( "+
				"    SELECT   /*+use_hash(b,a)*/ A.site_id, "+
				"             round(sum(case when cnt=2 then kcnf1 else 0 end)*fn_month_modifier(to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm'))) last_keep_uv,       "+
				"             round(sum(case when cnt=2 then kcnf2 else 0 end)*fn_month_modifier('"+monthcode+"')) this_keep_uv,  "+
				"             round(sum(case when cnt=1 and max_monthcode=to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm') then kcnf1 else 0 end) "+
				"                                                                                     *fn_month_modifier(to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm'))) leave_uv, "+
				"             round(sum(case when cnt=1 and max_monthcode='"+monthcode+"' then kcnf2 else 0 end) "+
				"                                                                                     *fn_month_modifier('"+monthcode+"')) new_uv "+
				"    FROM "+
				"    (         "+
				"        SELECT   site_id, panel_id, count(*) cnt, max(monthcode) max_monthcode "+
				"        FROM     tb_smart_month_fact "+
				"        WHERE    monthcode in (to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm'), '"+monthcode+"') "+
				"        GROUP BY site_id, panel_id  "+
				"    ) A, "+
				"    (        "+
				"        SELECT   panel_id, "+
				"                 sum(case when monthcode=to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm') then mo_n_factor end) kcnf1, "+
				"                 sum(case when monthcode='"+monthcode+"' then mo_n_factor end) kcnf2 "+
				"        FROM     tb_smart_month_panel_seg "+
				"        WHERE    monthcode in (to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm'), '"+monthcode+"')  "+
				"        GROUP BY panel_id "+
				"        HAVING   count(*) >= 2 "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    GROUP BY A.site_id "+
				") A, "+
				"(         "+
				"    SELECT   site_id, site_name, uu_cnt_adj this_uv_adj                   "+
				"    FROM     tb_smart_month_site_sum "+
				"    WHERE    monthcode = '"+monthcode+"' "+
				") B, "+
				"(         "+
				"    SELECT   site_id, site_name, uu_cnt_adj  last_uv_adj "+
				"    FROM     tb_smart_month_site_sum "+
				"    WHERE    monthcode = to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm') "+
				") C "+
				"WHERE    A.site_id = B.site_id "+
				"AND      A.site_id = C.site_id "+
				"AND	 last_keep_uv+leave_uv > 0 "+
				"AND	 this_keep_uv+new_uv > 0 ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		//System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
			
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppSumError
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Monthly Person Seg 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppSumError(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Month Site Summary Error is processing...");
		
		String queryT = "TRUNCATE TABLE tb_temp_error ";
        this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		String query = 
				"INSERT INTO tb_temp_error (code,site_id,rr_error)  "+
				"SELECT monthcode, smart_id,   "+
				"       round(fn_month_modifier(monthcode)*1.96*sqrt((1/power(sum(netizen_cnt),2))*sum(UV_VAR)),4) Reach_E  "+
				"FROM  "+
				"    (  "+
				"    SELECT '"+monthcode+"' monthcode, smart_id,   "+
				"           loc_cd, region_cd, sex_cls, age_cls,   "+
				"           netizen_cnt,  "+
				"           netizen_cnt*rr UV_S,  "+
				"           power(netizen_cnt,2)*((netizen_cnt-panel_cnt)/netizen_cnt)*(rr*(1-rr)/(decode(panel_cnt,1,1.000000000000001,panel_cnt)-1)) UV_VAR  "+
				"    FROM (  "+
				"          SELECT a.smart_id, a.loc_cd, a.region_cd, a.sex_cls, a.age_cls, panel_cnt, netizen_cnt, decode(cnt/panel_cnt,null,0,cnt/panel_cnt) rr  "+
				"          FROM   "+
				"            (  "+
				"                SELECT /*+use_hash(a,b) index(a,PK_SMART_month_PERSON_SEG)*/ "+
				"                       smart_id, loc_cd, region_cd, sex_cls, age_cls, max(panel_cnt) panel_cnt, max(netizen_cnt) netizen_cnt  "+
				"                FROM   tb_smart_month_person_seg a, tb_smart_month_app_sum b  "+
				"                WHERE  a.monthcode = '"+monthcode+"' "+
				"                AND    a.monthcode = b.monthcode "+         
				"                and    b.smart_id in (select smart_id "+
				"                                      from (select smart_id, rank()over(order by UU_CNT_ADJ desc) rnk "+
				"                                            from tb_smart_month_app_sum b "+
				"                                            where monthcode = '"+monthcode+"' "+
				"                                            and package_name <> 'kclick_equal_app') "+
				"                                      where rnk <=200 ) "+
				"                GROUP BY smart_id, loc_cd, region_cd, sex_cls, age_cls  "+
				"            ) a, "+ 
				"            (  "+
				"                SELECT /*+use_hash(b,a) index(b,PK_SMART_month_PERSON_SEG)*/ "+
				"                       a.monthcode, smart_id, b.loc_cd, b.region_cd, b.sex_cls, b.age_cls, count(*) cnt  "+
				"                FROM   tb_smart_month_app_fact a, tb_smart_month_person_seg b  "+
				"                WHERE  a.monthcode = '"+monthcode+"' "+
				"                AND    a.monthcode = b.monthcode "+
				"                AND    a.panel_id = b.panel_id  "+
				"                GROUP BY a.monthcode, smart_id, b.loc_cd, b.region_cd, b.sex_cls, b.age_cls  "+
				"            ) b  "+
				"         WHERE a.age_cls=b.age_cls(+)  "+
				"         AND   a.sex_cls=b.sex_cls(+)  "+
				"         AND   a.loc_cd=b.loc_cd(+)  "+
				"         AND   a.region_cd=b.region_cd(+)  "+
				"         AND   a.smart_id = b.smart_id(+)  "+
				"        )  "+
				"      )  "+
				"GROUP BY monthcode, smart_id ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String queryU = "UPDATE tb_smart_month_app_sum a "+
						"SET    rr_error = (select rr_error from tb_temp_error b where a.smart_id = b.site_id and a.monthcode = b.code) "+
						"WHERE  monthcode = '"+monthcode+"' "+
						"AND    smart_id in ( "+
						"                        select smart_id from tb_smart_month_app_sum "+
						"                        where monthcode = '"+monthcode+"' "+
						"                        and   smart_id in (select smart_id "+
						"                                           from (select smart_id, rank()over(order by UU_CNT_ADJ desc) rnk "+
						"                                                 from tb_smart_month_app_sum b "+
						"                                                 where monthcode = '"+monthcode+"' "+
						"                                                 and package_name <> 'kclick_equal_app') "+
						"                                           where rnk <=200 ) "+
						"                    ) ";
        this.pstmt = connection.prepareStatement(queryU);
		this.pstmt.executeUpdate();
		
		this.pstmt = connection.prepareStatement(queryT);
		this.pstmt.executeUpdate();
		
		String query2 = 
				"INSERT INTO tb_temp_error (code,site_id,tts_error)  "+
				"SELECT  monthcode, smart_id, "+
				"        round(1.96*sqrt(sum(D_var))/60,2) TTS_E "+
				"FROM ( "+
				"    SELECT b.monthcode monthcode, "+
				"           smart_id,  "+
				"           sex_cls, age_cls, loc_cd, region_cd,  "+
				"           max(netizen_cnt) netizen_cnt,  "+
				"           sum(kc_n_factor) kc_n_factor_S, "+
				"           sum(duration*kc_p_factor) duration_s, "+
				"           ((sum(kc_n_factor)*(sum(kc_n_factor)-count(a.panel_id)))/count(a.panel_id)*((sum(power(duration,2))-power(sum(duration),2)/count(a.panel_id))/(decode(count(a.panel_id),1,1.000000001,count(a.panel_id))-1))) d_var "+
				"    FROM    ( "+
				"                SELECT smart_id, panel_id, duration  "+
				"                FROM   tb_smart_month_app_fact "+
				"                WHERE  monthcode = '"+monthcode+"' "+
				"                AND    smart_id in ( "+
				"                    SELECT smart_id  "+
				"                    FROM   tb_smart_month_app_sum  "+
				"                    WHERE  monthcode = '"+monthcode+"' "+
				"                    and   smart_id in (select smart_id "+
				"                                       from (select smart_id, rank()over(order by UU_CNT_ADJ desc) rnk "+
				"                                             from tb_smart_month_app_sum b "+
				"                                             where monthcode = '"+monthcode+"' "+
				"                                             and package_name <> 'kclick_equal_app') "+
				"                                       where rnk <=200 ) "+
				"                ) "+
				"            ) a, "+
				"            ( "+
				"                SELECT b.monthcode, a.panel_id, "+
				"                       a.SEX_CLS, a.AGE_CLS, a.LOC_CD, a.REGION_CD, P_person kc_p_factor,person kc_n_factor, netizen_cnt, panel_cnt "+
				"                FROM   tb_smart_month_person_seg a, tb_smart_month_panel_seg b "+
				"                WHERE  a.monthcode = b.monthcode "+
				"                AND    b.monthcode = '"+monthcode+"' "+
				"                AND    a.panel_id = b.panel_id "+
				"            ) b "+
				"    WHERE    b.panel_id=a.panel_id "+
				"    GROUP BY b.monthcode, a.smart_id, sex_cls, age_cls, loc_cd, region_cd "+
				"    ) "+
				"GROUP BY monthcode, smart_id ";
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		String queryU2 ="UPDATE tb_smart_month_app_sum a "+
						"SET    tts_error = (SELECT tts_error from tb_temp_error b WHERE a.smart_id = b.site_id AND a.monthcode = b.code) "+
						"WHERE  monthcode = '"+monthcode+"' "+
						"AND    smart_id in ( "+
						"                        select smart_id from tb_smart_month_app_sum "+
						"                        where monthcode = '"+monthcode+"' "+
						"                        and   smart_id in (select smart_id "+
						"                                           from (select smart_id, rank()over(order by UU_CNT_ADJ desc) rnk "+
						"                                                 from tb_smart_month_app_sum b "+
						"                                                 where monthcode = '"+monthcode+"' "+
						"                                                 and package_name <> 'kclick_equal_app') "+
						"                                           where rnk <=200 ) "+
						"                    )";

		this.pstmt = connection.prepareStatement(queryU2);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}	
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppLoyalty
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthAppLoyalty 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppLoyalty(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly App Loyalty is processing...");

		String query = 
				"INSERT   INTO tb_smart_week_app_loyalty_sum "+
				"SELECT fn_valid_weekcode('"+monthcode+"') weekcode, smart_id, no_of_week, count(*) panel_cnt, sysdate proc_date "+
				"FROM "+
				"( "+
				"    SELECT smart_id, panel_id, count(*) no_of_week "+
				"    FROM "+
				"    ( "+
				"        SELECT b.weekcode, smart_id, a.panel_id "+
				"        FROM   tb_smart_day_app_fact a, tb_smart_panel_seg b "+
				"        WHERE  access_day between '"+monthcode+"'||'01' and to_char(next_day(to_date('"+monthcode+"'||'01','yyyymmdd'),2)-1,'yyyymmdd') "+
				"        AND    a.panel_id=b.panel_id "+
				"        AND    b.weekcode=fn_weekcode(to_char(next_day(to_date('"+monthcode+"'||'01','yyyymmdd'),2)-1,'yyyymmdd')) "+
				"        GROUP BY b.weekcode, smart_id, a.panel_id "+
				"        UNION ALL "+
				"        SELECT a.weekcode, smart_id, a.panel_id "+
				"        FROM   tb_smart_week_app_fact a, tb_smart_panel_seg b "+
				"        WHERE  a.weekcode between fn_weekcode(to_char(next_day(to_date('"+monthcode+"'||'01','yyyymmdd'),2),'yyyymmdd')) and fn_valid_weekcode('"+monthcode+"') "+
				"        and a.weekcode = b.weekcode "+
				"        and a.panel_id = b.panel_id "+
				"        GROUP BY a.weekcode, a.smart_id, a.panel_id "+
				"    ) "+
				"    GROUP BY smart_id, panel_id "+
				") "+
				"GROUP BY smart_id, no_of_week";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		//System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
			
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppSwitch
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthAppSwitch 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppSwitch(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly App Switch is processing...");

		String query = 
				"INSERT   INTO tb_smart_month_app_switch "+
				"(MONTHCODE, SMART_ID, APP_NAME, LAST_UV_ADJ, THIS_UV_ADJ, KEEP_RATE, NEW_RATE, LEAVE_RATE) "+
				"SELECT   /*+use_hash(b,a) use_hash(c,a)*/ '"+monthcode+"',  A.SMART_ID, B.APP_NAME, last_uv_adj, this_uv_adj, "+
				"         round(last_keep_uv/(last_keep_uv+leave_uv)*100,2) keep_rate,  "+
				"         round(new_uv/(this_keep_uv+new_uv)*100,2) new_rate,  "+
				"         round(leave_uv/(last_keep_uv+leave_uv)*100,2) leave_rate "+
				"FROM      "+
				"( "+
				"    SELECT   /*+use_hash(b,a)*/ A.SMART_ID, "+
				"             round(sum(case when cnt=2 then kcnf1 else 0 end)*fn_month_modifier(to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm'))) last_keep_uv,       "+
				"             round(sum(case when cnt=2 then kcnf2 else 0 end)*fn_month_modifier('"+monthcode+"')) this_keep_uv,  "+
				"             round(sum(case when cnt=1 and max_monthcode=to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm') then kcnf1 else 0 end) "+
				"                                                                                     *fn_month_modifier(to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm'))) leave_uv, "+
				"             round(sum(case when cnt=1 and max_monthcode='"+monthcode+"' then kcnf2 else 0 end) "+
				"                                                                                     *fn_month_modifier('"+monthcode+"')) new_uv "+
				"    FROM "+
				"    (         "+
				"        SELECT   SMART_ID, panel_id, count(*) cnt, max(monthcode) max_monthcode "+
				"        FROM     tb_smart_month_app_fact "+
				"        WHERE    monthcode in (to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm'), '"+monthcode+"') "+
				"        GROUP BY SMART_ID, panel_id  "+
				"    ) A, "+
				"    (        "+
				"        SELECT   panel_id, "+
				"                 sum(case when monthcode=to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm') then mo_n_factor end) kcnf1, "+
				"                 sum(case when monthcode='"+monthcode+"' then mo_n_factor end) kcnf2 "+
				"        FROM     tb_smart_month_panel_seg "+
				"        WHERE    monthcode in (to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm'), '"+monthcode+"')  "+
				"        GROUP BY panel_id "+
				"        HAVING   count(*) >= 2 "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    GROUP BY A.SMART_ID "+
				") A, "+
				"(         "+
				"    SELECT   SMART_ID, APP_NAME, uu_cnt_adj this_uv_adj                   "+
				"    FROM     tb_smart_month_app_sum "+
				"    WHERE    monthcode = '"+monthcode+"' "+
				") B, "+
				"(         "+
				"    SELECT   SMART_ID, APP_NAME, uu_cnt_adj  last_uv_adj "+
				"    FROM     tb_smart_month_app_sum "+
				"    WHERE    monthcode = to_char(add_months(to_date('"+monthcode+"','yyyymm'),-1),'yyyymm') "+
				") C "+
				"WHERE    A.SMART_ID = B.SMART_ID "+
				"AND      A.SMART_ID = C.SMART_ID "+
				"AND	 last_keep_uv+leave_uv > 0 "+
				"AND	 this_keep_uv+new_uv > 0 ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		//System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
			
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppDurationTimeSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 모바일 앱 이용시간 분포 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppDurationTimeSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly App Duration Time Sum is processing...");

		String query = 
				"insert into TB_SMART_month_APP_TIME_SUM "+
				"select   monthcode, a.smart_id, a.package_name, app_name, a.site_id, uu_cnt_adj, avg_duration, "+
				"         d01, d02, d03, d04, d05, d06, d07, d08, d09, sysdate "+
				"from "+     
				"( "+
				"    select   monthcode, smart_id, package_name, app_name, site_id, uu_cnt_adj, avg_duration "+
				"    from     tb_smart_month_app_sum "+
				"    where    monthcode = '"+monthcode+"' "+
				") a, "+
				"( "+
				"    select    smart_id, "+
				"              sum(case when daily_avg_dt <= 20   then mo_n_factor else 0 end) d01, "+
				"              sum(case when daily_avg_dt > 20   and daily_avg_dt <= 40   then mo_n_factor else 0 end) d02, "+
				"              sum(case when daily_avg_dt > 40   and daily_avg_dt <= 60   then mo_n_factor else 0 end) d03, "+
				"              sum(case when daily_avg_dt > 60   and daily_avg_dt <= 600  then mo_n_factor else 0 end) d04, "+
				"              sum(case when daily_avg_dt > 600  and daily_avg_dt <= 1200 then mo_n_factor else 0 end) d05, "+
				"              sum(case when daily_avg_dt > 1200 and daily_avg_dt <= 1800 then mo_n_factor else 0 end) d06, "+
				"              sum(case when daily_avg_dt > 1800 and daily_avg_dt <= 2400 then mo_n_factor else 0 end) d07, "+
				"              sum(case when daily_avg_dt > 2400 and daily_avg_dt <= 3600 then mo_n_factor else 0 end) d08, "+
				"              sum(case when daily_avg_dt > 3600 then mo_n_factor else 0 end) d09 "+
				"    from "+     
				"    ( "+
				"        select   smart_id, panel_id, round(sum(avg_dt)/to_number(substr(fn_month_lastday('"+monthcode+"'),7,8))) daily_avg_dt "+
				"        from "+     
				"        ( "+
				"            select  /*+ use_hash(a,b)*/ "+
				"                   a.access_day, smart_id, a.panel_id, "+
				"                   round((duration*B.mo_p_factor)/(B.mo_n_factor),2) avg_dt "+
				"            from "+
				"            ( "+
				"                select  /*+index(a,pk_smart_day_app_fact)*/ access_day, smart_id, panel_id, duration "+
				"                from    tb_smart_day_app_fact a "+
				"                where access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"            ) a, "+
				"            ( "+
				"                select  access_day, panel_id, mo_n_factor, mo_p_factor "+
				"                from    tb_smart_day_panel_seg "+
				"                where   access_day = fn_month_lastday('"+monthcode+"') "+
				"            ) b "+
				"            where   a.panel_id = b.panel_id "+
				"        ) "+
				"        group by smart_id, panel_id "+
				"    ) a, "+
				"    ( "+
				"        select   panel_id, mo_n_factor "+
				"        from     tb_smart_month_panel_seg "+
				"        where    monthcode = '"+monthcode+"' "+
				"    ) b "+
				"    where    a.panel_id = b.panel_id "+
				"    group by smart_id "+
				") b "+
				"where    a.smart_id   = b.smart_id ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		//System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
			
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthSiteDurationTimeSum
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 월간 모바일 웹 이용시간 분포 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthSiteDurationTimeSum(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Site Duration Time Sum is processing...");

		String query = 
				"insert into TB_SMART_MONTH_DURATION_TIME "+
				"select   monthcode, a.site_id, site_name, uu_cnt_adj, avg_duration, "+
				"         d01, d02, d03, d04, d05, d06, d07, d08, d09, sysdate "+
				"from     ( "+
				"         select   monthcode, site_id, site_name, uu_cnt_adj, avg_duration "+
				"         from     tb_smart_month_site_sum "+
				"         where    monthcode = '"+monthcode+"' "+
				"         and      category_code1 != 'Z' "+
				"         ) a, "+
				"         ( "+
				"         select   site_id, "+
				"                  sum(case when daily_avg_dt <= 20   then mo_n_factor else 0 end) d01, "+
				"                  sum(case when daily_avg_dt > 20   and daily_avg_dt <= 40   then mo_n_factor else 0 end) d02, "+
				"                  sum(case when daily_avg_dt > 40   and daily_avg_dt <= 60   then mo_n_factor else 0 end) d03, "+
				"                  sum(case when daily_avg_dt > 60   and daily_avg_dt <= 600  then mo_n_factor else 0 end) d04, "+
				"                  sum(case when daily_avg_dt > 600  and daily_avg_dt <= 1200 then mo_n_factor else 0 end) d05, "+
				"                  sum(case when daily_avg_dt > 1200 and daily_avg_dt <= 1800 then mo_n_factor else 0 end) d06, "+
				"                  sum(case when daily_avg_dt > 1800 and daily_avg_dt <= 2400 then mo_n_factor else 0 end) d07, "+
				"                  sum(case when daily_avg_dt > 2400 and daily_avg_dt <= 3600 then mo_n_factor else 0 end) d08, "+
				"                  sum(case when daily_avg_dt > 3600 then mo_n_factor else 0 end) d09 "+
				"         from     ( "+
				"                  select   site_id, panel_id, round(sum(avg_dt)/to_number(substr(fn_month_lastday('"+monthcode+"'),7,8))) daily_avg_dt "+
				"                  from     ( "+
				"                           select  /*+ use_hash(a,b)*/ "+
				"                                   a.access_day, site_id, a.panel_id, "+
				"                                   round((duration*B.mo_p_factor)/(B.mo_n_factor),2) avg_dt "+
				"                           from    ( "+
				"                                       select  access_day, site_id, panel_id, pv_cnt, duration "+
				"                                       from    tb_smart_day_fact "+
				"                                       where access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"                                   ) a, "+
				"                                   ( "+
				"                                       select  access_day, panel_id, mo_n_factor, mo_p_factor "+
				"                                       from    tb_smart_day_panel_seg "+
				"                                       where   access_day = fn_month_lastday('"+monthcode+"') "+
				"                                   ) b "+
				"                           where   a.panel_id = b.panel_id "+
				"                           ) "+
				"                  group by site_id, panel_id "+
				"                  ) a, "+
				"                  ( "+
				"                  select   panel_id, mo_n_factor "+
				"                  from     tb_smart_month_panel_seg "+
				"                  where    monthcode = '"+monthcode+"' "+
				"                  ) b "+
				"         where    a.panel_id = b.panel_id "+
				"         group by site_id "+
				"         ) b "+
				"where    a.site_id   = b.site_id";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		//System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
			
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppSession
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthAppSession 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppSession(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Application Session is processing...");

		String query = 
				"insert into TB_SMART_MONTH_APP_SESSION "+
				"select /*+ordered*/a.MONTHCODE, a.PANEL_ID, APP_CNT, SETUP_CNT,  "+
				"       DAILY_FREQ_CNT, tot_duration DURATION, MO_N_FACTOR, MO_P_FACTOR,  "+
				"       AGE_CLS, SEX_CLS, NVL(INCOME_CLS,0), NVL(JOB_CLS,0), NVL(EDUCATION_CLS,0), NVL(ISMARRIED_CLS,0), "+
				"       REGION_CD, NVL(LIFESTYLE_CLS,0), KC_SEG_ID, RI_SEG_ID, sysdate, "+
				"       media_duration, laun_duration "+
				"from ( "+
				"    select substr(access_day,1,6) monthcode, panel_id,  "+
				"           count(distinct smart_id) app_cnt,  "+
				"           count(distinct access_day) DAILY_FREQ_CNT, "+
				"           sum(case when flag = 'TOTAL' then duration else 0 end) tot_duration, "+
				"           sum(case when flag = 'MEDIA' then duration else 0 end) media_duration, "+
				"           sum(case when flag = 'LAUN'  then duration else 0 end) laun_duration "+
				"    from  "+
				"    (         "+
				"        select /*+index(a,pk_smart_day_app_fact)*/ access_day, smart_id, panel_id, duration, 'TOTAL' flag "+
				"        from   tb_smart_day_app_fact a "+
				"        where  access_day like '"+monthcode+"'||'%' "+
				"        and    package_name != 'kclick_equal_app' "+
				"         "+
				"        union all "+
				"         "+
				"        select /*+index(a,pk_smart_day_app_fact) use_hash(b,a)*/ access_day, a.smart_id, panel_id, duration, 'MEDIA' flag "+
				"        from   tb_smart_day_app_fact a, TB_SMART_APP_MEDIA_LIST b "+
				"        where  access_day like '"+monthcode+"'||'%' "+
				"        and    a.smart_id = b.smart_id "+
	            "        and    b.ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
	            "        and    b.exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+ 					
				"         "+
				"        union all "+
				"         "+
				"        select /*+index(a,pk_smart_day_app_fact) use_hash(b,a)*/ access_day, a.smart_id, panel_id, duration, 'LAUN' flag "+
				"        from   tb_smart_day_app_fact a, TB_SMART_APP_LAUN_LIST b "+
				"        where  access_day like '"+monthcode+"'||'%' "+
				"        and    a.smart_id = b.smart_id "+
	            "        and    b.ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
	            "        and    b.exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+ 					
				"    ) "+
				"    group  by substr(access_day,1,6), panel_id "+
				") a, "+
				"( "+
				"    select monthcode, panel_id, count(distinct smart_id) SETUP_CNT  "+
				"    from  "+
				"    (         "+
				"        select /*+ index(a,pk_smart_month_setup_fact) */ monthcode, smart_id, panel_id "+
				"        from   tb_smart_month_setup_fact a "+
				"        where  monthcode = '"+monthcode+"' "+
				"        AND    package_name != 'kclick_equal_app' "+
				"    ) "+
				"    group  by monthcode, panel_id "+
				") b, "+
				"( "+
				"    select * "+
				"    from tb_smart_month_panel_seg "+
				"    where monthcode = '"+monthcode+"' "+
				") c "+
				"where a.monthcode = c.monthcode "+
				"and a.monthcode = b.monthcode "+
				"and a.panel_id = c.panel_id "+
				"and a.panel_id = b.panel_id ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppLVL1
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthAppLVL1 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppLVL1(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Application Level1 is processing...");
		
		String query = 
				"insert into TB_SMART_MONTH_APPLVL1_SUM "+
				"select a.monthcode, a.app_category_cd1, reach_rate, a.uu_cnt, uu_overall_rank, avg_duration, avg_duration_overall_rank, "+
				"       daily_freq_cnt, a.uu_cnt_adj, reach_rate_adj, freq_overall_rank, tot_duration_overall_rank, tot_duration_adj, "+
				"       round(a.uu_cnt/b.uu_cnt*100,2) install_rate, round(a.uu_cnt_adj/b.uu_cnt_adj*100,2) install_rate_adj, "+
				"       sysdate, b.uu_cnt install_uu_cnt, b.uu_cnt_adj install_uu_cnt_adj, app_cnt_adj, null keyuser_uv "+
				"from "+
				" ( "+
				"    SELECT   /*+use_hash(b,a)*/ "+
				"             A.monthcode,  app_category_cd1, "+
				"             round(count(A.panel_id)/FN_SMART_MONTH_COUNT('"+monthcode+"')*100,2) reach_rate, "+
				"             count(A.panel_id) uu_cnt, "+
				"             rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"             round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"             rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"             round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"             round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"             round(sum(B.mo_n_factor)/FN_SMART_MONTH_NFACTOR('"+monthcode+"')*100,5) reach_rate_adj, "+
				"             rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"             rank() over (order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
				"             round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"             round(sum(app_cnt*mo_p_factor),5) app_cnt_adj "+
//				"             round(sum(decode(keyuser_cd, 'H', mo_p_factor, 0)),5) keyuser_uv "+
				"    FROM "+
			    "	 ( "+
				"		 SELECT   /*+index(a,PK_SMART_MONTH_APPLVL1_FACT)*/ monthcode, PANEL_ID, APP_CATEGORY_CD1, DURATION, DAILY_FREQ_CNT, app_cnt, keyuser_cd  "+
				"		 FROM     tb_smart_month_applvl1_fact a "+
				"		 WHERE    monthcode = '"+monthcode+"' "+
				"    ) A, "+
				"    ( "+
				"        SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"        FROM     tb_smart_month_panel_seg "+
				"        WHERE    monthcode = '"+monthcode+"' "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    GROUP BY A.monthcode, APP_CATEGORY_CD1 "+
				") a, "+
				"( "+
				"    select  /*+use_hash(b,a)*/ "+
				"            a.monthcode, APP_CATEGORY_CD1, "+
				"            count(A.panel_id) uu_cnt, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj "+
				"    from "+
				"    ( "+
				"        select /*+ordered use_hash(b,a)*/monthcode, panel_id, APP_CATEGORY_CD1 "+
				"        from 	tb_smart_month_setup_FACT a, tb_smart_app_info b "+
				"        where 	monthcode = '"+monthcode+"' "+
				"        and 	a.smart_id = b.smart_id "+
				"  		 AND      b.EXP_TIME > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				" 		 AND      b.EF_TIME < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"        group by monthcode, panel_id, APP_CATEGORY_CD1 "+
				"    ) a, "+
				"    ( "+
				"        select MONTHCODE, PANEL_ID, mo_n_factor "+
				"        from tb_smart_month_panel_seg "+
				"        where monthcode = '"+monthcode+"' "+
				"    ) b "+
				"    where a.monthcode=b.monthcode "+
				"    and a.panel_id=b.panel_id "+
				"    group by a.monthcode, APP_CATEGORY_CD1 "+
				") b "+
				"where a.monthcode = b.monthcode "+
				"and   a.APP_CATEGORY_CD1 = b.APP_CATEGORY_CD1(+) ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppLVL2
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthAppLVL2 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppLVL2(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Application Level2 is processing...");

		String query = 
				"insert into TB_SMART_MONTH_APPLVL2_SUM "+
				"select a.monthcode, a.app_category_cd1, a.app_category_cd2, "+
				"       reach_rate, a.uu_cnt, uu_overall_rank, uu_1level_rank, avg_duration, avg_duration_overall_rank, avg_duration_1level_rank, "+
				"       daily_freq_cnt, a.uu_cnt_adj, reach_rate_adj, freq_overall_rank, tot_duration_overall_rank, tot_duration_adj, "+
				"       round(a.uu_cnt/b.uu_cnt*100,2) install_rate, round(a.uu_cnt_adj/b.uu_cnt_adj*100,2) install_rate_adj, sysdate, "+
				"       b.uu_cnt install_uu_cnt, b.uu_cnt_adj install_uu_cnt_adj, app_cnt_adj, null keyuser_uv "+
				"from "+
				"( "+
				"    SELECT   /*+use_hash(b,a)*/ "+
				"             A.monthcode,  app_category_cd1, APP_CATEGORY_CD2, "+
				"             round(count(A.panel_id)/FN_SMART_MONTH_COUNT('"+monthcode+"')*100,2) reach_rate, "+
				"             count(A.panel_id) uu_cnt, "+
				"             rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"             rank() over (partition by min(app_category_cd1) order by sum(B.mo_n_factor) desc) uu_1level_rank, "+
				"             round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"             rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"             rank() over (partition by min(app_category_cd1) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_1level_rank, "+
				"             round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"             round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"             round(sum(B.mo_n_factor)/FN_SMART_MONTH_NFACTOR('"+monthcode+"')*100,5) reach_rate_adj, "+
				"             rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"             rank() over (order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank, "+
				"             round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"             round(sum(app_cnt*mo_p_factor),5) app_cnt_adj "+
	//			"             round(sum(decode(keyuser_cd, 'H', mo_p_factor, 0)),5) keyuser_uv "+
				"    FROM "+
			    "	 ( "+
				"		 SELECT   /*+index(a,pk_smart_month_applvl2_fact)*/ monthcode, PANEL_ID, APP_CATEGORY_CD1, APP_CATEGORY_CD2, DURATION, DAILY_FREQ_CNT, app_cnt, keyuser_cd "+
				"		 FROM     tb_smart_month_applvl2_fact a "+
				"		 WHERE    monthcode = '"+monthcode+"' "+
				"    ) A, "+
				"    ( "+
				"        SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"        FROM     tb_smart_month_panel_seg "+
				"        WHERE    monthcode = '"+monthcode+"' "+
				"    ) B "+
				"    WHERE    A.panel_id = B.panel_id "+
				"    GROUP BY A.monthcode, APP_CATEGORY_CD1, APP_CATEGORY_CD2 "+
				") a, "+
				"( "+
				"    select  /*+use_hash(b,a)*/ "+
				"            a.monthcode, APP_CATEGORY_CD2, "+
				"            count(A.panel_id) uu_cnt, "+
				"            round(sum(B.mo_n_factor),5) uu_cnt_adj "+
				"    from "+
				"    ( "+
				"        select /*+ordered use_hash(b,a)*/monthcode, panel_id, APP_CATEGORY_CD2 "+
				"        from 	tb_smart_month_setup_FACT a, tb_smart_app_info b "+
				"        where 	monthcode = '"+monthcode+"' "+
				"        and 	a.smart_id = b.smart_id "+
				"  		 AND      b.EXP_TIME > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				" 		 AND      b.EF_TIME < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"        group by monthcode, panel_id, APP_CATEGORY_CD2 "+
				"    ) a, "+
				"    ( "+
				"        select MONTHCODE, PANEL_ID, mo_n_factor "+
				"        from tb_smart_month_panel_seg "+
				"        where monthcode = '"+monthcode+"' "+
				"    ) b "+
				"    where a.monthcode=b.monthcode "+
				"    and a.panel_id=b.panel_id "+
				"    group by a.monthcode, APP_CATEGORY_CD2 "+
				") b "+
				"where a.monthcode = b.monthcode "+
				"and   a.APP_CATEGORY_CD2 = b.APP_CATEGORY_CD2(+) ";
		
		//System.out.println(query);
		//System.exit(0);
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppLv1Seg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthAppLv1Seg 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppLv1Seg(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Application Level1 Seg is processing...");

		String query = 
				"insert  into TB_SMART_MONTH_SEG_APPLV1 "+
				"select  '"+monthcode+"' monthcode, "+
				"        kc_seg_id, "+
				"        v_fact.APP_CATEGORY_CD1,  "+
				"        min(v_panel_seg.age_cls)        age_cls, "+
				"        min(v_panel_seg.sex_cls)        sex_cls, "+
				"        min(v_panel_seg.income_cls)     income_cls, "+  
				"        min(v_panel_seg.job_cls)        job_cls, "+
				"        min(v_panel_seg.education_cls)  education_cls, "+
				"        min(v_panel_seg.ismarried_cls ) ismarried_cls,  "+
				"        min(v_panel_seg.region_cd )     region_cd, "+  
				"        count(distinct v_fact.panel_id) uu_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor),5) uu_est_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"        round(sum(duration*v_panel_seg.mo_p_factor),5) duration_est, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"        sum(app_cnt) app_cnt, "+
				"        round(sum(app_cnt*v_panel_seg.mo_p_factor),5) app_est_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(app_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor)/ max(sum_kc_nfactor)*100, 5) reach_rate, "+
				"        sysdate "+
				"from "+  
				"( "+
				"        select  /*+use_hash(b,a) index(a,pk_smart_month_applvl1_fact)*/ monthcode, APP_CATEGORY_CD1, panel_id, duration, daily_freq_cnt, app_cnt "+
				"        from    tb_smart_month_applvl1_fact a "+
				"        where   monthcode = '"+monthcode+"' "+
				") v_fact, "+
				"( "+
				"        select  panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"        from    tb_smart_month_panel_seg "+
				"        where   monthcode = '"+monthcode+"' "+
				") v_panel_seg, "+
				"( "+
				"        select  sum(mo_n_factor) sum_kc_nfactor "+
				"        from    tb_smart_month_panel_seg "+
				"        where   monthcode = '"+monthcode+"' "+
				") v_total_panel_seg "+
				"where v_fact.panel_id   = v_panel_seg.panel_id "+
				"group by v_panel_seg.kc_seg_id, v_fact.APP_CATEGORY_CD1 ";
		
		//System.out.println(query);
		//System.exit(0);
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppLv2Seg
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthAppLv2Seg 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppLv2Seg(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Application Level2 Seg is processing...");

		String query = 
				"insert  into TB_SMART_month_SEG_APPLV2 "+
				"select  '"+monthcode+"' monthcode, "+
				"        kc_seg_id, "+
				"        v_fact.APP_CATEGORY_CD2, "+
				"        min(v_panel_seg.age_cls)        age_cls, "+
				"        min(v_panel_seg.sex_cls)        sex_cls, "+
				"        min(v_panel_seg.income_cls)     income_cls, "+  
				"        min(v_panel_seg.job_cls)        job_cls, "+
				"        min(v_panel_seg.education_cls)  education_cls, "+
				"        min(v_panel_seg.ismarried_cls ) ismarried_cls,  "+
				"        min(v_panel_seg.region_cd )     region_cd, "+  
				"        count(distinct v_fact.panel_id) uu_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor),5) uu_est_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_p_factor*duration)/sum(mo_n_factor)),2) avg_duration, "+
				"        round(sum(duration*v_panel_seg.mo_p_factor),5) duration_est, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(mo_n_factor*daily_freq_cnt)/sum(mo_n_factor)),2) avg_daily_freq_cnt, "+
				"        sum(app_cnt) app_cnt, "+
				"        round(sum(app_cnt*v_panel_seg.mo_p_factor),5) app_est_cnt, "+
				"        round(decode(sum(mo_n_factor),0,0, sum(app_cnt*mo_p_factor)/ sum(mo_n_factor)),2) avg_pv_est_cnt, "+
				"        round(sum(v_panel_seg.mo_n_factor)/ max(sum_kc_nfactor)*100, 5) reach_rate, "+
				"        sysdate "+
				"from  "+
				"( "+
				"        select  /*+use_hash(b,a) index(a,pk_smart_month_applvl2_fact)*/ monthcode, APP_CATEGORY_CD2, panel_id, duration, daily_freq_cnt, app_cnt "+
				"        from    tb_smart_month_applvl2_fact a "+
				"        where   monthcode = '"+monthcode+"' "+
				") v_fact, "+
				"( "+
				"        select  panel_id, kc_seg_id, mo_n_factor, mo_p_factor, age_cls, sex_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd "+
				"        from    tb_smart_month_panel_seg "+
				"        where   monthcode = '"+monthcode+"' "+
				") v_panel_seg, "+
				"( "+
				"        select  sum(mo_n_factor) sum_kc_nfactor "+
				"        from    tb_smart_month_panel_seg "+
				"        where   monthcode = '"+monthcode+"' "+
				") v_total_panel_seg "+
				"where v_fact.panel_id   = v_panel_seg.panel_id "+
				"group by v_panel_seg.kc_seg_id, v_fact.APP_CATEGORY_CD2";
		
		//System.out.println(query);
		//System.exit(0);
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthAppSumSite
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthAppSumSite 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthAppSumSite(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Application Sum Site is processing...");

		String query = 
				"insert into tb_smart_month_app_sum_site "+
				"SELECT   /*+ordered*/ "+
				"         A.monthcode,  SITE_ID, "+
				"         round(count(A.panel_id)/FN_SMART_MONTH_COUNT('"+monthcode+"')*100,2) reach_rate, "+
				"         count(A.panel_id) uu_cnt, "+
				"         rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"         round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"         rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"         round(decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)),2) daily_freq_cnt, "+
				"         round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"         round(sum(B.mo_n_factor)/FN_SMART_MONTH_NFACTOR('"+monthcode+"')*100,5) reach_rate_adj, "+
				"         rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
				"         rank() over (order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
				"         round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"         sysdate, "+
				"         round(sum(app_cnt*mo_p_factor),5) app_cnt_adj "+
				"FROM "+
				"( "+
				"	SELECT   /*+index(a,pk_smart_day_app_fact) use_hash(b,a)*/" +
				"            substr(access_day,1,6) MONTHCODE, site_id, panel_id, "+
				"            count(distinct access_day) daily_freq_cnt, sum(duration) duration, sum(app_cnt) app_cnt "+
				"   FROM     tb_smart_day_app_fact a, tb_smart_app_info b "+
				"   WHERE    access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"   AND      a.smart_id = b.smart_id "+
				"	and      b.package_name != 'kclick_equal_app' "+
				"   AND      b.EXP_TIME > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd') "+
				"   AND      b.EF_TIME < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd') "+
				"   AND      b.site_id is not null "+
				"   group by substr(access_day,1,6), site_id, panel_id "+
				") A, "+
				"( "+
				"   SELECT   panel_id, mo_n_factor, mo_p_factor "+
				"   FROM     tb_smart_month_panel_seg "+
				"   WHERE    monthcode = '"+monthcode+"' "+
				") B "+
				"WHERE    A.panel_id = B.panel_id "+
				"GROUP BY A.monthcode, SITE_ID ";	
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeDayEnterService
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthEnterService 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeDayEnterService(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daily Entertainment Service is processing...");
		
		String query = 
				"insert into tb_smart_day_service_fact "+
				"select access_day, site_id, code_num, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, tab_flag, sysdate proc_date "+
				"from "+
				"( "+
				"    select  access_day, site_id,  "+
				"            case when category_code1='E' then '1' "+
				"                 when category_code2='DJ' then '2' "+
				"                 when category_code2='DB' then '3' "+
				"            else 'Bad' end code_num, "+
				"            panel_id, pv_cnt, duration, 'W' tab_flag "+
				"    from    tb_smart_day_fact "+
				"    where   access_day = '"+accessday+"' "+
				"    and     (category_code1='E' or category_code2 in ('DJ','DB')) "+
//				"    union all "+
//				"    select /*+use_hash(b,a)*/ "+ 
//				"           access_day, site_id, "+ 
//				"           case when psection_id=8  then '1' "+ 
//				"                when psection_id=26 then '2' "+ 
//				"                when section_id=402 then '3' "+ 
//				"           else 'Bad' end code_num, "+ 
//				"           panel_id, 1 pv_cnt, duration, 'S' tab_flag "+ 
//				"    from   tb_smart_browser_itrack a, tb_smart_section_path b "+ 
//				"    where  (psection_id in (8,26) or section_id = 402)" +
//				"	 and    access_day = '"+accessday+"' " +
//				"	 and    panel_flag in ('D','V') " +
//				"	 and    result_cd = 'S' " +
//				"	 and    req_site_id = site_id " +
//				"	 and    ((TYPE_CD='D' and req_domain = path_url) " +
//				"	      or (TYPE_CD='P' and req_domain||decode(req_page, NULL, NULL, '/'||req_page) = path_url)) "+ 
				"    union all "+
				"    select /*+use_hash(b,a)*/access_day, site_id, "+
				"           case when b.app_category_cd1='C' then '1' "+
				"                when b.app_category_cd2='113' then '2' "+
				"                when b.app_category_cd2='114' then '3' "+
				"           else 'Bad' end code_num, "+
				"           panel_id, 0 pv_cnt, duration, 'A' tab_flag "+
				"    from   tb_smart_day_app_fact a, tb_smart_app_info b "+
				"    where  a.smart_id=b.smart_id "+
				"    and    a.access_day = '"+accessday+"' "+
				//"    and    b.site_id is not null "+
				"    and    b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
				"    and    b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
				"    and    (b.app_category_cd1 = 'C' or b.app_category_cd2 in ('113','114')) "+
				") "+
				"group by access_day, code_num, site_id, panel_id, tab_flag ";	
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	/**************************************************************************
	 *		메소드명		: executeMonthEnterService
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthEnterService 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthEnterService(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Entertainment Service is processing...");
		
		String queryFact = 
				"insert into TB_SMART_MONTH_SERVICE_FACT "+
				"select access_day, site_id, code_num, panel_id,  "+
				"       sum(pv_cnt) pv_cnt,  "+
				"       sum(duration) duration,  "+
				"       tab_flag, sysdate "+
				"from ( "+
				"    select  /*+index(a,pk_smart_day_app_fact) */ access_day, site_id,  "+
				"            case when category_code1='E' then '1' "+
				"                 when category_code2='DJ' then '2' "+
				"                 when category_code2='DB' then '3' "+
				"			      when category_code1='C' then '4' "+
                "				  when category_code1='Q' then '5' "+
				"            else 'Bad' end code_num, "+
				"            panel_id, pv_cnt, duration, 'W' tab_flag "+
				"    from    tb_smart_day_fact a "+
				"    where   access_day between '"+monthcode+"'||'01' and  '"+monthcode+"'||'31' "+
				"    and     (category_code1 in ('E','C','Q') or category_code2 in ('DJ','DB')) "+
				"    union all "+
				"    select /*+use_hash(b,a) index(a,pk_smart_day_app_fact) */access_day, site_id, "+
				"           case when b.app_category_cd1='C' then '1' "+
				"                when b.app_category_cd2='176' then '2' "+ //카테고리 변경 작업으로 인한 113->176 변경
				"                when b.app_category_cd2='114' then '3' "+
				"				 when b.app_category_cd1='B' then '4' "+
                " 				 when b.app_category_cd1='L' then '5' "+
				"           else 'Bad' end code_num, "+
				"           panel_id, 0 pv_cnt, duration, 'A' tab_flag "+
				"    from   tb_smart_day_app_fact a, tb_smart_app_info b "+
				"    where  a.smart_id=b.smart_id "+
				"    and    access_day between '"+monthcode+"'||'01' and '"+monthcode+"'||'31'"+
				"    and    a.package_name != 'kclick_equal_app' "+
				//"    and    b.site_id is not null "+
				"    and    b.ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"    and    b.exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"    and    (b.app_category_cd1 in ('C','B','L') or b.app_category_cd2 in ('176','114')) "+
				"    union all "+
				"    select access_day, SITE_ID, "+
				"           case when section_id=8   then '1' "+
				"                when section_id=26  then '2' "+
				"  				 when section_id=402  then '3' "+
				"                when psection_id=5 then '4' "+
				"                when psection_id=6 then '5' "+
				"           else 'Bad' end code_num, "+
				"           panel_id, pv_cnt, duration, 'S' tab_flag "+
				"    from   tb_smart_month_temp_section "+
				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and    (section_id in (8,26,402) or psection_id = 5 or (psection_id=6 and site_id != 262)) "+
//				"    union all "+
//				"    select access_day, SITE_ID, "+
//				"           '3' code_num, "+
//				"           panel_id, pv_cnt, duration, 'S' tab_flag "+
//				"    from   tb_smart_month_temp_section "+
//				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
//				"    and    section_id = 402 "+
				") "+
				"group by access_day, site_id, code_num, panel_id, tab_flag ";

		this.pstmt = connection.prepareStatement(queryFact);
		this.pstmt.executeUpdate();
		
		String query1 = 
				"insert into tb_smart_month_service_sum "+
				"select  a.monthcode, code_num, "+
				"        round(sum(B.monf),5) uu_cnt_adj, "+
				"        round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"        round(decode(sum(B.monf),0,1, sum(A.duration * B.mo_p_factor)/sum(B.monf)),2) avg_duration, "+
				"        round(decode(sum(B.mo_p_factor),0,1, sum(daily_freq_cnt*B.monf)/sum(B.monf)),2) daily_freq_cnt, "+
				"        round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"        round(sum(B.monf)/fn_smart_month_nfactor(a.monthcode)*100,5) reach_rate_adj, "+
				"        decode(round(sum(B.monf),5), 0, 0, round(round(sum(A.pv_cnt*B.mo_p_factor),5)/round(sum(B.monf),5), 2)) avg_pv, "+
				"        sysdate proc_date "+
				"from "+
				"( "+
				"    select substr(access_day,1,6) monthcode, code_num, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt "+
				"    from   TB_SMART_MONTH_SERVICE_FACT "+
				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    group by substr(access_day,1,6), code_num, panel_id "+
				") a, "+
				"( "+
				"    select monthcode, panel_id, mo_n_factor monf, mo_p_factor "+
				"    from   tb_smart_month_panel_seg "+
				"    where  monthcode = '"+monthcode+"' "+
				") b "+
				"where a.monthcode=b.monthcode "+
				"and   a.panel_id=b.panel_id "+
				"group by a.monthcode, code_num ";
		
        this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		
		String query2 = 
				"insert into tb_smart_month_service_site "+
				"select  a.monthcode, code_num, site_id, "+
				"        round(sum(B.monf),5) uu_cnt_adj, "+
				"        round(sum(A.pv_cnt*B.mo_p_factor),5) pv_cnt_adj, "+
				"        round(decode(sum(B.monf),0,1, sum(A.duration * B.mo_p_factor)/sum(B.monf)),2) avg_duration, "+
				"        round(decode(sum(B.mo_p_factor),0,1, sum(daily_freq_cnt*B.monf)/sum(B.monf)),2) daily_freq_cnt, "+
				"        round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"        round(sum(B.monf)/fn_smart_month_nfactor(a.monthcode)*100,5) reach_rate_adj, "+
				"        decode(round(sum(B.monf),5), 0, 0, round(round(sum(A.pv_cnt*B.mo_p_factor),5)/round(sum(B.monf),5), 2)) avg_pv, "+
				"        sysdate proc_date "+
				"from "+
				"( "+
				"    select substr(access_day,1,6) monthcode, code_num, site_id, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt "+
				"    from   TB_SMART_MONTH_SERVICE_FACT "+
				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') " +
				"	 and    site_id > 0 "+
				"    group by substr(access_day,1,6), code_num, site_id, panel_id "+
				") a, "+
				"( "+
				"    select monthcode, panel_id, mo_n_factor monf, mo_p_factor "+
				"    from   tb_smart_month_panel_seg "+
				"    where  monthcode = '"+monthcode+"' "+
				") b "+
				"where a.monthcode=b.monthcode "+
				"and   a.panel_id=b.panel_id "+
				"group by a.monthcode, code_num, site_id ";
		
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		query2 =
				"insert into TB_month_TOTAL_SERVICE_FACT "+
				"select monthcode, access_day, site_id, code_num, panel_id,   max(mo_n_factor) uu_cnt_adj, sum(pv_cnt) pv_cnt_adj, sum(duration) tot_duration_adj, sysdate, "+
					"sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls "+
				"from "+
				"( "+
				"    select a.monthcode, access_day, a.panel_id, site_id, code_num, mo_n_factor, pv_cnt*mo_p_factor pv_cnt, duration*mo_p_factor duration, "+
				"        sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls "+
				"    from "+
				"    ( "+
				"         select /*+index(a,PK_SMART_month_SERVICE_FACT) */ substr(access_day,1,6) monthcode, access_day, panel_id, site_id, code_num, pv_cnt, duration "+
				"         from tb_smart_month_service_fact a "+
				"         WHERE   access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"')  "+
				"         and code_num in (1,2,3,5) "+
				"    )a, "+
				"    ( "+
				"        select monthcode, panel_id, mo_n_factor, mo_p_factor, sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls "+
				"        from tB_smart_month_panel_seg "+
				"        where monthcode = '"+monthcode+"' "+
				"    )b "+
				"    where a.monthcode = b.monthcode "+
				"    and a.panel_id = b.panel_id "+
				"    union all "+
				"    select a.monthcode, access_day, a.panel_id, site_id, code_num, kc_n_factor, pv_cnt*kc_p_factor pv_cnt, duration*kc_p_factor duration, "+
				"      sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls  "+
				"    from "+
				"    ( "+
				"        select substr(access_day,1,6)  monthcode, access_day, panel_id, site_id, code_num, sum(pv_cnt) pv_cnt, sum(duration) duration "+
				"        from "+
				"        ( "+
				"            SELECT  /*+parallel(c 8)*/ "+
				"                    access_day,  case when section_id in ('22','23') then '5' "+
				"                                      when psection_id ='26' then '2' "+
				"                                      when psection_id = '8' then '1' "+
				"                                      when section_id = '402' then '3' end code_num, panel_id, site_id, to_number(pv_cnt) pv_cnt, duration "+
				"            FROM    tb_month_temp_section c "+
				"            WHERE   access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"            AND     ( (section_id in ('22','23') "+
				"            ANd     site_id not in (261,290,324,340,384,386,1182,436)) "+
				"            OR (psection_id in ('26')) "+
				"            OR (psection_id in ('8') "+
				"            AND     site_id not in (261,290,340,384,405,436,1154,1182,1183,3062,7703,14116)) "+
				"            OR (section_id in ('402') "+
				"            AND     site_id not in (436))) "+
				"            UNION ALL "+
				"            SELECT  access_day, case when category_code2 = 'EC' then '1' "+
				"                                     when category_code2 = 'DJ' then '2' "+
				"                                     when category_code2 = 'DB' then '3' "+
				"                                     when category_code2 in ('QA','QB') then '5' end code_num, panel_id, site_id, pv_cnt, duration "+
				"            FROM    tb_day_fact "+
				"            WHERE   access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"')  "+
				"            AND     category_code2 in ('DB','EC','DJ','QA','QB') "+
				"        ) "+
				"        group by access_day, code_num, panel_id, site_id "+
				"    )a, "+
				"    ( "+
				"        select monthcode, panel_id, kc_n_factor, kc_p_factor, sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls  "+
				"        from tB_month_panel_seg "+
				"        where monthcode = '"+monthcode+"' "+
				"    )b "+
				"    where a.monthcode = b.monthcode "+
				"    and a.panel_id = b.panel_id "+
				") "+
				"group by monthcode, access_day, panel_id, site_id, code_num, sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls  ";
	
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		query2 =
				"insert into TB_month_TOTAL_SERVICE_SUM "+
						"select monthcode, code_num, "+
						"    sum(uu_cnt_adj) uu_cnt_adj, "+
						"    sum(pv_cnt_adj) pv_cnt_daj, "+
						"    round(sum(pv_cnt_adj)/sum(uu_cnt_adj),2) avg_pv, "+
						"    sum(tot_duration_adj) tot_duration_adj, "+
						"    round(sum(tot_duration_adj)/sum(uu_cnt_adj),2) avg_duration, "+
						"    round(sum(daily_freq_cnt*uu_cnt_adj)/sum(uu_cnt_adj),2) daily_freq_cnt, "+
						"    round(sum(uu_cnt_adj)/FN_PCANDROID_month_NFACTOR(monthcode)*100,5) reach_rate_adj, "+
						"    sysdate "+
						"from "+
						"( "+
						"    select /*+index(a,PK_MONTH_TOTAL_SERVICE_FACT) */ substr(access_day,1,6) monthcode, panel_id, code_num, max(uu_cnt_adj) uu_cnt_adj, sum(pv_cnt_adj) pv_cnt_adj, sum(tot_duration_adj) tot_duration_adj, "+
						"        count(distinct access_day) daily_freq_cnt "+
						"    from TB_month_TOTAL_SERVICE_FACT a "+
						"    WHERE   access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
						"    group by substr(access_day,1,6), panel_id, code_num "+
						") "+
						"group by monthcode, code_num ";
		
		
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		
		query2 =
				"insert into TB_month_TOTAL_SERVICE_SITE "+
				"select monthcode, code_num, site_id, "+
				"    sum(uu_cnt_adj) uu_cnt_adj, "+
				"    sum(pv_cnt_adj) pv_cnt_daj, "+
				"    round(sum(pv_cnt_adj)/sum(uu_cnt_adj),2) avg_pv, "+
				"    sum(tot_duration_adj) tot_duration_adj, "+
				"    round(sum(tot_duration_adj)/sum(uu_cnt_adj),2) avg_duration, "+
				"    round(sum(daily_freq_cnt*uu_cnt_adj)/sum(uu_cnt_adj),2) daily_freq_cnt, "+
				"    round(sum(uu_cnt_adj)/FN_PCANDROID_month_NFACTOR(monthcode)*100,5) reach_rate_adj, "+
				"    sysdate "+ 
				"from "+
				"( "+
				"    select  /*+index(a,PK_MONTH_TOTAL_SERVICE_FACT) */ substr(access_day,1,6) monthcode, panel_id, code_num, site_id, max(uu_cnt_adj) uu_cnt_adj, sum(pv_cnt_adj) pv_cnt_adj, sum(tot_duration_adj) tot_duration_adj, "+
				"        count(distinct access_day) daily_freq_cnt "+
				"    from TB_month_TOTAL_SERVICE_FACT a "+
				"    WHERE   access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    group by substr(access_day,1,6), panel_id, code_num, site_id "+
				") "+
				"group by monthcode, code_num, site_id ";
		
		
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthTotalSession
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthTotalSession 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthTotalSession(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Total Session is processing...");

		String query = 
//				"INSERT   INTO TB_TOTAL_MONTH_SESSION (  "+
//				"        MONTHCODE, PANEL_ID, PV_CNT, DURATION , "+
//				"        DAILY_FREQ_CNT, KC_N_FACTOR, KC_P_FACTOR, AGE_CLS, SEX_CLS, "+
//				"        EDUCATION_CLS, INCOME_CLS, JOB_CLS, ISMARRIED_CLS, SITE_CNT, PROC_DATE  "+
//				") "+
//				"SELECT  '"+monthcode+"', A.PANEL_ID, PV_CNT, DURATION,                  "+
//				"        DAILY_FREQ_CNT, KC_N_FACTOR, KC_P_FACTOR, AGE_CLS, SEX_CLS,  "+
//				"        EDUCATION_CLS, INCOME_CLS, JOB_CLS, ISMARRIED_CLS, NVL(SITE_CNT,0), SYSDATE "+
//				"FROM     (  "+
//				"    SELECT  PANEL_ID, SUM(PV_CNT) PV_CNT, SUM(DURATION) DURATION, "+
//				"            COUNT(DISTINCT ACCESS_DAY) DAILY_FREQ_CNT "+
//				"    FROM    TB_SMART_DAY_TOTAL_FACT "+
//				"    WHERE   ACCESS_DAY >= '"+monthcode+"'||'01' "+
//				"    AND     ACCESS_DAY <= FN_MONTH_LASTDAY('"+monthcode+"') "+
//				"    AND     TAB_CD IN ('PW','MW') "+
//				"    GROUP BY PANEL_ID "+
//				") A, "+
//				"( "+
//				"    SELECT  PANEL_ID, KC_N_FACTOR, AGE_CLS, SEX_CLS, EDUCATION_CLS,  "+
//				"            INCOME_CLS, JOB_CLS, ISMARRIED_CLS, KC_P_FACTOR "+
//				"    FROM    tb_month_total_panel_seg "+
//				"    WHERE   MONTHCODE = '"+monthcode+"' "+
//				") B, "+
//				"( "+
//				"    SELECT  PANEL_ID, COUNT(DISTINCT SITE_ID) SITE_CNT "+
//				"    FROM    TB_SMART_MONTH_TOTSITE_FACT "+
//				"    WHERE   MONTHCODE = '"+monthcode+"' "+
//				"    GROUP BY PANEL_ID "+
//				") C "+
//				"WHERE A.PANEL_ID = B.PANEL_ID "+
//				"AND   A.PANEL_ID = C.PANEL_ID(+) ";
				
				"insert into tb_total_month_session "+ 
				"select /*+ordered*/substr(access_day,1,6) monthcode, panel_id, "+ 
				"       age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls, "+ 
				"       sum(pv_cnt_adj) pv_cnt_adj, sum(tot_duration_adj) tot_duration_adj, sum(laun_duration_adj) laun_duration_adj, sum(media_duration_adj) media_duration_adj, max(kc_n_factor) kc_n_factor, "+ 
				"       count(distinct access_day) daily_freq_cnt, sysdate proc_date "+ 
				"from "+ 
				"( "+ 
				"    select /*+leading(b) use_hash(b,a)*/access_day, a.panel_id, "+ 
				"           pv_cnt, duration, pv_cnt*kc_p_factor pv_cnt_adj, duration*kc_p_factor tot_duration_adj, 0 laun_duration_adj, 0 media_duration_adj, "+ 
				"           kc_n_factor, kc_p_factor, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls "+ 
				"    from "+ 
				"    ( "+ 
				"        select /*+index(a,pk_day_session)*/ access_day, panel_id, pv_cnt, duration "+ 
				"        from   tb_day_session a "+ 
				"        WHERE  access_day between '"+monthcode+"'||'01' and FN_MONTH_LASTDAY('"+monthcode+"') "+ 
				"    ) a, "+ 
				"    ( "+ 
				"        select panel_id, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls, kc_n_factor, kc_p_factor "+ 
				"        from tb_month_panel_seg "+ 
				"        where monthcode = '"+monthcode+"' "+ 
				"    ) b "+ 
				"    where a.panel_id=b.panel_id "+ 
				"    union all "+ 
				"    select /*+use_hash(b,a)*/access_day, a.panel_id, "+ 
				"           pv_cnt, duration, pv_cnt*mo_p_factor pv_cnt_adj, duration*mo_p_factor tot_duration_adj, laun_duration*mo_p_factor laun_duration_adj, "+ 
				"           media_duration*mo_p_factor media_duration_adj, mo_n_factor, mo_p_factor, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls "+ 
				"    from "+ 
				"    ( "+ 
				"        select access_day, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, sum(laun_duration) laun_duration, sum(media_duration) media_duration "+ 
				"        from "+ 
				"        ( "+ 
//				"            select /*+index(a,pk_smart_day_fact)*/ access_day, panel_id, pv_cnt, duration, 0 laun_duration, 0 media_duration "+ 
//				"            from   tb_smart_day_fact a "+ 
//				"            WHERE  access_day between '"+monthcode+"'||'01' and FN_MONTH_LASTDAY('"+monthcode+"') "+ 
//				"            union all "+ 
				"            select /*+index(a,pk_smart_day_app_fact)*/ access_day, panel_id, 0 pv_cnt, duration, "+ 
				"                   0 laun_duration, 0 media_duration "+ 
				"            from   tb_smart_day_app_fact a "+ 
				"            WHERE  access_day between '"+monthcode+"'||'01' and FN_MONTH_LASTDAY('"+monthcode+"') "+
		        "			 and    package_name != 'kclick_equal_app' "+				
		        "            union all "+
		        "            select /*+index(a,pk_smart_day_app_fact) use_hash(b,a)*/access_day, panel_id, 0 pv_cnt, 0 duration, "+
		        "                   duration laun_duration, 0 media_duration "+
		        "            from   tb_smart_day_app_fact a, tb_smart_app_laun_list b "+
		        "            WHERE  access_day between '"+monthcode+"'||'01' and FN_MONTH_LASTDAY('"+monthcode+"') "+ 
		        "            and    a.smart_id = b.smart_id "+
	            "            and    b.ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
	            "            and    b.exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+ 		
		        "            union all "+
		        "            select /*+index(a,pk_smart_day_app_fact) use_hash(b,a)*/access_day, panel_id, 0 pv_cnt, 0 duration, "+
		        "                   0 laun_duration, duration media_duration "+
		        "            from   tb_smart_day_app_fact a, tb_smart_app_media_list b "+
		        "            WHERE  access_day between '"+monthcode+"'||'01' and FN_MONTH_LASTDAY('"+monthcode+"') "+ 
		        "            and    a.smart_id = b.smart_id "+
	            "            and    b.ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
	            "            and    b.exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+ 	                
				"        ) "+ 
				"        group by access_day, panel_id "+ 
				"    ) a, "+ 
				"    ( "+ 
				"        select panel_id, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls, mo_n_factor, mo_p_factor "+ 
				"        from tb_smart_month_panel_seg "+ 
				"        where monthcode = '"+monthcode+"' "+ 
				"    ) b "+ 
				"    where a.panel_id=b.panel_id "+ 
				") "+ 
				"group by substr(access_day,1,6), panel_id, age_cls, sex_cls, region_cd, income_cls, job_cls, education_cls, ismarried_cls ";		
		//System.out.println(query);
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeMonthTotalSession
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthTotalSession 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthWebAppSession(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Web App Session is processing...");

		String query = 
				"insert into tb_smart_month_webapp_session " + 
				"select monthcode, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duration, sum(laun_duration) laun_duration, sum(media_duration) media_duration, " + 
				"       sum(web_daily_freq_cnt) web_daily_freq_cnt, sum(app_daily_freq_cnt) app_daily_freq_cnt, max(mo_n_factor) mo_n_factor, max(mo_p_factor) mo_p_factor, " + 
				"       age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls " + 
				"from " + 
				"( " + 
				"    select monthcode, panel_id, pv_cnt, duration, laun_duration, 0 media_duration, daily_freq_cnt web_daily_freq_cnt, 0 app_daily_freq_cnt, mo_n_factor, mo_p_factor, " + 
				"           age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls " + 
				"    from tb_smart_month_session " + 
				"    where monthcode = '"+monthcode+"' " + 
				"    union all " + 
				"    select monthcode, panel_id, 0 pv_cnt, duration, laun_duration, media_duration, 0, daily_freq_cnt, mo_n_factor, mo_p_factor, " + 
				"           age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls " + 
				"    from tb_smart_month_app_session " + 
				"    where monthcode = '"+monthcode+"' " + 
				") " + 
				"group by monthcode, panel_id, age_cls, sex_cls, income_cls, job_cls, education_cls, ismarried_cls ";
		//System.out.println(query);
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}	
	/**************************************************************************
	 *		메소드명		: executeMonthTotal
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthTotal 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthTotal(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Total Sum is processing...");
		
		executeQueryExecute("truncate table tb_temp_day_wapp_fact");
		String query1 = 
				"insert into tb_temp_day_wapp_fact "+
				"select  /*+use_hash(b,a)*/ "+
				"        a.access_day,a.site_id,a.app_id,a.panel_id, 0 pv_cnt,  "+
				"        case when ( a.duration - b.duration ) < 0 then 0 else ( a.duration - b.duration ) end duration, "+
				"        sysdate "+
				"from "+
				"( "+
				"    select access_day, site_id, app_id, panel_id, duration  "+
				"    from tb_day_app_fact  "+
				"    where access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				") a, "+
				"( "+
				"    select access_day, site_id, app_id, panel_id, duration  "+
				"    from tb_day_wapp_fact  "+
				"    where access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				") b "+
				"where a.access_day = b.access_day "+
				"and a.site_id = b.site_id "+
				"and a.app_id = b.app_id "+
				"and a.panel_id = b.panel_id ";
        this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		
		executeQueryExecute("truncate table tb_temp_month_day_siteapp_fact");
		String query = 
				"insert into tb_temp_month_day_siteapp_fact "+
				"select access_day, site_id, panel_id, tab_cd, sum(pv_cnt) pv_cnt, sum(duration) duration, sysdate "+
				"from "+
				"( "+
				"    select  access_day, site_id, panel_id, 'PW' tab_cd, pv_cnt, duration "+
				"    from    tb_day_fact  "+
				"    where   access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    AND     category_code1 <> 'Z' "+
				"    union all "+
				"    select  /*+use_hash(b,a)*/ access_day, b.site_id, panel_id, 'PA' tab_cd, 0 pv_cnt, duration "+
				"    from    tb_day_app_fact a, tb_app_info b "+
				"    where   access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and     a.app_id = b.app_id "+
				"    and     b.ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"    and     b.exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"    and     (a.access_day,a.site_id,a.app_id,a.panel_id ) not in ( "+
				"                                                                    select access_day,site_id,app_id,panel_id  "+
				"                                                                    from TB_TEMP_DAY_WAPP_FACT  "+
				"                                                                    where access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"                                                                    group by access_day,site_id,app_id,panel_id  "+
				"                                                                 ) "+
				"    AND    a.app_type_cd != 'Z' "+
				"    AND    a.app_id not in (2656,2657,1764,2653,3114,3192) "+
				"    union all "+
				"    select  /*+use_hash(b,a)*/access_day, a.site_id, panel_id, 'PWA' tab_cd, pv_cnt, 0 duration "+
				"    from    tb_day_vapp_fact  a, tb_app_info b "+
				"    where   access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and    a.site_id = b.site_id "+
				"    AND    a.app_id  = b.app_id "+
				"    and    b.ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"    and    b.exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"    AND    b.app_type_cd < 'Z' "+
				"    AND    RESULT_CD='Y' "+
				"    AND    a.app_id not in (2656,2657,1764,2653,3114,3192) "+
				"    union all "+
				"    select  access_day, site_id, panel_id, 'PWA' tab_cd, pv_cnt, duration "+
				"    from    TB_TEMP_DAY_WAPP_FACT  "+
				"    where   access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    union all "+
				"    SELECT access_day, to_number(code_etc) site_id, panel_id, 'PWA' tab_cd, 0 pv_cnt, duration "+
				"    FROM   tb_day_mess_fact a, (select code, case when '"+monthcode+"'  <  201306 then code_etc else code_modify end code_etc from tb_codebook b where meta_code = 'MESSENGER') b "+
				"    WHERE  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    AND    a.messenger = code "+
				") "+
				"group by access_day, site_id, panel_id, tab_cd ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String queryFact = 
				"insert into TB_SMART_MONTH_TOTAL_FACT "+
				"select access_day, site_id, panel_id, tab_cd, sum(pv_cnt), sum(duration), sysdate "+
				"from ( "+
				"    select access_day, site_id, panel_id, tab_cd, pv_cnt, duration "+
				"    from   tb_temp_month_day_siteapp_fact "+
				"    where  access_day like '"+monthcode+"%'  "+
//				"    union all "+
////				"    select * "+
////				"    from   tb_smart_day_total_fact "+
////				"    where  substr(access_day,1,6) = '"+monthcode+"' "+
////				"    and    tab_cd in ('MW','MA') "+
//				"    select /*+index(a,pk_smart_day_fact)*/ access_day, site_id, panel_id, 'MW' tab_cd, pv_cnt, duration "+
//				"    from   tb_smart_day_fact a "+
//				"    where  access_day like '"+monthcode+"%' " +
//				"    and    site_id in (select site_id from tb_site_info where exp_time > sysdate and category_code1 <>'Z') "+
//				"	 and    panel_id in ( " +
//				"	 	select panel_id " +
//				"		from   tb_smart_month_panel_seg " +
//				"		where  monthcode = '"+monthcode+"' " +
//				"	 ) "+
				"    union all "+
				"    select /*+use_hash(b,a) index(a,pk_smart_day_app_fact)*/   "+
				"           access_day, b.site_id, panel_id, 'MA' tab_cd, 0 pv_cnt, duration "+
				"    from   tb_smart_day_app_fact a, tb_smart_app_info b "+
				"    where  access_day like '"+monthcode+"%' "+
				"    and    a.smart_id = b.smart_id "+
				"    and    b.site_id > 0 "+
				"    and    b.ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 "+
				"    and    b.exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd')+1 " +
				"    and    b.package_name != 'kclick_equal_app' "+
				"	 and    panel_id in ( " +
				"	 	select panel_id " +
				"		from   tb_smart_month_panel_seg " +
				"		where  monthcode = '"+monthcode+"' " +
				"	 ) "+
				") "+
				"group by access_day, site_id, panel_id, tab_cd ";
				
		this.pstmt = connection.prepareStatement(queryFact);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query3 = 
//				"insert into tb_smart_month_totsiteapp_fact "+
//				"select /*+index(a,idx_day_totsiteapp_fact)*/substr(access_day,1,6) monthcode, site_id, panel_id, "+
//				"       sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate proc_date "+
//				"from  TB_SMART_MONTH_TOTAL_FACT a "+
//				"where substr(access_day,1,6) = '"+monthcode+"' "+
//				"group by substr(access_day,1,6), site_id, panel_id ";
				
				"insert into TB_SMART_MONTH_TOTSITEAPP_FACT                                      "+
				"select substr(access_day,1,6) monthcode, site_id, panel_id,                     "+
				"       sum(pv_cnt) pv_cnt, sum(duration) duration,                              "+
				"       count(distinct access_day) daily_freq_cnt, sysdate,                      "+
				"       max(kc_n_factor) kc_n_factor,                                            "+
				"       sum(adj_pv_cnt) adj_pv_cnt, sum(adj_duration) adj_duration               "+
				"from (                                                                          "+
				"    select /*+ index(a,PK_MONTH_TOTAL_FACT) */ access_day,                      "+
				"           site_id, a.panel_id, pv_cnt, duration, kc_n_factor, kc_p_factor,     "+
				"           pv_cnt*kc_p_factor adj_pv_cnt, duration*kc_p_factor adj_duration     "+
				"    from   TB_SMART_MONTH_TOTAL_FACT a, tb_month_panel_seg b                    "+
				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and    monthcode = '"+monthcode+"'                                               "+
				"    and    a.panel_id = b.panel_id                                              "+
				"    and    tab_cd like 'P%'                                                     "+
				"                                                                                "+
				"    union all                                                                   "+
				"                                                                                "+
				"    select /*+ index(a,PK_MONTH_TOTAL_FACT) */ access_day,                      "+
				"           site_id, a.panel_id, pv_cnt, duration, mo_n_factor, mo_p_factor,     "+
				"           pv_cnt*mo_p_factor adj_pv_cnt, duration*mo_p_factor adj_duration     "+
				"    from   TB_SMART_MONTH_TOTAL_FACT a, tb_smart_month_panel_seg b              "+
				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and    monthcode = '"+monthcode+"'                                               "+
				"    and    a.panel_id = b.panel_id                                              "+
				"    and    tab_cd = 'MA'                                            			 "+
				")                                                                               "+
				"group by substr(access_day,1,6), site_id, panel_id                              ";
		
		this.pstmt = connection.prepareStatement(query3);
		this.pstmt.executeUpdate();
		
		String query4 = 
//				"insert into tb_smart_month_totsiteapp_sum "+
//				"select  /*+use_hash(b,a)*/ "+
//				"        a.monthcode monthcode, "+
//				"        site_id, "+
//				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
//				"        round(sum(pv_cnt * kc_p_factor),5) PV_CNT_ADJ, "+
//				"        round(decode(sum(kc_n_factor),0,0,round(sum(pv_cnt * kc_p_factor),5)/sum(kc_n_factor)),2) AVG_PV, "+
//				"        round(decode(sum(B.kc_n_factor),0,1, sum(A.duration * B.kc_p_factor)/sum(B.kc_n_factor)),2) AVG_DURATION, "+
//				"        round(sum(duration * kc_p_factor),5) tot_duration_adj, "+
//				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
//				"        round(sum(B.kc_n_factor)/FN_TOTAL_MONTH_NFACTOR('"+monthcode+"')*100,5) reach_rate_adj, "+
//				"        rank() over (order by sum(B.kc_n_factor) desc) uu_overall_rank, "+
//				"        rank() over (order by sum(A.pv_cnt*B.kc_p_factor) desc) pv_overall_rank, "+
//				"        rank() over (order by decode(sum(B.kc_p_factor),0,1, sum(A.duration*B.kc_p_factor)/sum(B.kc_n_factor)) desc) avg_duration_overall_rank, "+
//				"        rank() over (order by sum(A.duration*B.kc_p_factor) desc) tot_duration_overall_rank,     "+
//				"        rank() over (order by decode(sum(B.kc_n_factor),0,1, sum(daily_freq_cnt*B.kc_n_factor)/sum(B.kc_n_factor)) desc) freq_overall_rank, "+
//				"        rank() over (order by decode(sum(kc_n_factor),0,0,sum(pv_cnt * kc_p_factor)/sum(kc_n_factor)) desc) avg_pv_overall_rank, "+
//				"        sysdate "+
//				"from  "+
//				"( "+
//				"    select /*+parallel(a,8) index(a,idx_day_totsiteapp_fact)*/monthcode, site_id, panel_id, pv_cnt, duration, daily_freq_cnt "+
//				"    from tb_smart_month_totsiteapp_fact a "+
//				"    where monthcode = '"+monthcode+"' "+
//				") a, "+
//				"( "+
//				"    select monthcode, panel_id, kc_n_factor, kc_p_factor "+
//				"    from   tb_month_panel_seg "+
//				"    where  monthcode = '"+monthcode+"' "+
//				") b "+
//				"where a.monthcode = b.monthcode "+
//				"and   a.panel_id  = b.panel_id "+
//				"group by a.monthcode, site_id ";
				"insert into tb_smart_MONTH_totsiteapp_sum "+
				"select  /*+index(a,PK_SMART_MONTH_TOTSITEAPP) */ "+
				"        a.monthcode monthcode, "+
				"        site_id, "+
				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
				"        round(sum(adj_pv_cnt),5) PV_CNT_ADJ, "+
				"        round(decode(sum(kc_n_factor),0,0,round(sum(adj_pv_cnt),5)/sum(kc_n_factor)),2) AVG_PV, "+
				"        round(decode(sum(kc_n_factor),0,1, sum(adj_duration)/sum(kc_n_factor)),2) AVG_DURATION, "+
				"        round(sum(adj_duration),5) tot_duration_adj, "+
				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
				"        round(sum(kc_n_factor)/FN_PCANDROID_month_NFACTOR(a.monthcode)*100,5) reach_rate_adj, "+
				"        rank() over (partition by a.monthcode order by sum(kc_n_factor) desc) uu_overall_rank, "+
				"        rank() over (partition by a.monthcode order by sum(adj_pv_cnt) desc) pv_overall_rank, "+
				"        rank() over (partition by a.monthcode order by decode(sum(adj_duration),0,1, sum(adj_duration)/sum(kc_n_factor)) desc) avg_duration_overall_rank, "+
				"        rank() over (partition by a.monthcode order by sum(adj_duration) desc) tot_duration_overall_rank,     "+
				"        rank() over (partition by a.monthcode order by decode(sum(kc_n_factor),0,1,sum(daily_freq_cnt*kc_n_factor)/sum(kc_n_factor)) desc) freq_overall_rank, "+
				"        rank() over (partition by a.monthcode order by decode(sum(kc_n_factor),0,0,sum(adj_pv_cnt)/sum(kc_n_factor)) desc) avg_pv_overall_rank, "+
				"        sysdate "+
				"from  tb_smart_MONTH_totsiteapp_fact a "+
				"where monthcode = '"+monthcode+"' "+
				"group by monthcode, site_id ";
		
		this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();
		
//		String query5 = 
//				"insert into tb_smart_month_siteapp_fact "+
//				"select /*+index(a,PK_MONTH_TOTAL_FACT) */ substr(access_day,1,6) monthcode, site_id, panel_id, sum(pv_cnt) pv_cnt, sum(duration) duartion, count(distinct access_day) DAILY_FREQ_CNT, sysdate "+
//				"from  TB_SMART_MONTH_TOTAL_FACT a "+
//				"where access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
//				"and tab_cd in ('MW','MA') "+
//				"group by substr(access_day,1,6), site_id, panel_id ";
//		this.pstmt = connection.prepareStatement(query5);
//		this.pstmt.executeUpdate();
//		
//		String query6 = 
//				"insert into tb_smart_month_siteapp_sum "+
//				"select  /*+use_hash(b,a)*/ "+
//				"        a.monthcode monthcode, "+
//				"        site_id, "+
//				"        round(sum(mo_n_factor),5) UU_CNT_ADJ, "+
//				"        round(sum(pv_cnt * mo_p_factor),5) PV_CNT_ADJ, "+
//				"        round(decode(sum(B.mo_n_factor),0,1, sum(A.duration * B.mo_p_factor)/sum(B.mo_n_factor)),2) AVG_DURATION, "+
//				"        round(sum(duration * mo_p_factor),5) tot_duration_adj, "+
//				"        round(sum(daily_freq_cnt * mo_n_factor)/sum(mo_n_factor),2) daily_freq_cnt, "+
//				"        round(sum(B.mo_n_factor)/fn_smart_month_nfactor(a.monthcode)*100,5) reach_rate_adj, "+
//				"        rank() over (order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
//				"        rank() over (order by sum(A.pv_cnt*B.mo_p_factor) desc) pv_overall_rank, "+
//				"        rank() over (order by decode(sum(B.mo_p_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
//				"        rank() over (order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank,     "+
//				"        rank() over (order by decode(sum(B.mo_n_factor),0,1, sum(daily_freq_cnt*B.mo_n_factor)/sum(B.mo_n_factor)) desc) freq_overall_rank, "+
//				"        sysdate proc_date "+
//				"from  "+
//				"( "+
//				"    select /*+index(a,PK_SMART_MONTH_SITEAPP_FACT)*/ monthcode, site_id, panel_id, pv_cnt, duration, daily_freq_cnt "+
//				"    from tb_smart_month_siteapp_fact a "+
//				"    where monthcode = '"+monthcode+"' "+
//				") a, "+
//				"( "+
//				"    select monthcode, panel_id, mo_n_factor, mo_p_factor "+
//				"    from   tb_smart_month_panel_seg "+
//				"    where  monthcode = '"+monthcode+"' "+
//				") b "+
//				"where a.monthcode = b.monthcode "+
//				"and   a.panel_id  = b.panel_id "+
//				"group by a.monthcode, site_id ";
//		this.pstmt = connection.prepareStatement(query6);
//		this.pstmt.executeUpdate();
		
		String query7 = 
//				"insert into tb_smart_month_pcmobile_sum "+
//				"select substr(access_day,1,6) monthcode, panel_id, site_id, tab_cd, "+
//				"       sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate proc_date "+
//				"from  TB_SMART_MONTH_TOTAL_FACT "+
//				"where substr(access_day,1,6) = '"+monthcode+"' "+
//				"group by substr(access_day,1,6), panel_id, site_id, tab_cd ";
				"insert into tb_smart_MONTH_pcmobile_sum "+
				"select substr(access_day,1,6) monthcode, panel_id, site_id, tab_cd, "+
				"       sum(pv_cnt) pv_cnt, sum(duration) duration, "+
				"       count(distinct access_day) daily_freq_cnt, sysdate, "+
				"       max(kc_n_factor) kc_n_factor,  "+
				"       sum(adj_pv_cnt) adj_pv_cnt, sum(adj_duration) adj_duration "+
				"from ( "+
				"    select /*+ index(a,PK_MONTH_TOTAL_FACT) */ access_day, tab_cd, "+
				"           site_id, a.panel_id, pv_cnt, duration, kc_n_factor, kc_p_factor, "+
				"           pv_cnt*kc_p_factor adj_pv_cnt, duration*kc_p_factor adj_duration "+
				"    from   TB_SMART_MONTH_TOTAL_FACT a, tb_month_panel_seg b "+
				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and    monthcode = '"+monthcode+"' "+
				"    and    a.panel_id = b.panel_id "+
				"    and    tab_cd like 'P%' "+
				" "+
				"    union all "+
				" "+
				"    select /*+ index(a,PK_MONTH_TOTAL_FACT) */ access_day, tab_cd, "+
				"           site_id, a.panel_id, pv_cnt, duration, mo_n_factor, mo_p_factor, "+
				"           pv_cnt*mo_p_factor adj_pv_cnt, duration*mo_p_factor adj_duration "+
				"    from   TB_SMART_MONTH_TOTAL_FACT a, tb_smart_month_panel_seg b "+
				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and    monthcode = '"+monthcode+"' "+
				"    and    a.panel_id = b.panel_id "+
				"    and    tab_cd = 'MA' "+
				") "+
				"group by substr(access_day,1,6), site_id, panel_id, tab_cd ";
		
		this.pstmt = connection.prepareStatement(query7);
		this.pstmt.executeUpdate();
		
//		String query8 = 
////				"insert into tb_smart_month_totsite_fact "+
////				"select /*+index(a,idx_day_totsiteapp_fact)*/substr(access_day,1,6) monthcode, site_id, panel_id, "+
////				"       sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate "+
////				"from  TB_SMART_MONTH_TOTAL_FACT a "+
////				"where substr(access_day,1,6) = '"+monthcode+"' "+
////				"and   tab_cd in ('PW','MW') "+
////				"group by substr(access_day,1,6), site_id, panel_id ";
//				"insert into tb_smart_MONTH_totsite_fact "+
//				"select substr(access_day,1,6) monthcode, site_id, panel_id, "+
//				"       sum(pv_cnt) pv_cnt, sum(duration) duration, "+
//				"       count(distinct access_day) daily_freq_cnt, sysdate, "+
//				"       max(kc_n_factor) kc_n_factor,  "+
//				"       sum(adj_pv_cnt) adj_pv_cnt, sum(adj_duration) adj_duration "+
//				"from ( "+
//				"    select /*+ index(a,PK_MONTH_TOTAL_FACT) */ access_day, "+
//				"           site_id, a.panel_id, pv_cnt, duration, kc_n_factor, kc_p_factor, "+
//				"           pv_cnt*kc_p_factor adj_pv_cnt, duration*kc_p_factor adj_duration "+
//				"    from   TB_SMART_MONTH_TOTAL_FACT a, tb_month_panel_seg b "+
//				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
//				"    and    monthcode = '"+monthcode+"' "+
//				"    and    a.panel_id = b.panel_id "+
//				"    and    tab_cd = 'PW' "+
//				" "+
//				"    union all "+
//				" "+
//				"    select /*+ index(a,PK_MONTH_TOTAL_FACT) */ access_day, "+
//				"           site_id, a.panel_id, pv_cnt, duration, mo_n_factor, mo_p_factor, "+
//				"           pv_cnt*mo_p_factor adj_pv_cnt, duration*mo_p_factor adj_duration "+
//				"    from   TB_SMART_MONTH_TOTAL_FACT a, tb_smart_month_panel_seg b "+
//				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
//				"    and    monthcode = '"+monthcode+"' "+
//				"    and    a.panel_id = b.panel_id "+
//				"    and    tab_cd = 'MW' "+
//				") "+
//				"group by substr(access_day,1,6), site_id, panel_id ";
//		
//		this.pstmt = connection.prepareStatement(query8);
//		this.pstmt.executeUpdate();
	
//		String query9 = 
////				"insert into tb_smart_month_totsite_sum "+
////				"select  /*+use_hash(b,a)*/ "+
////				"        a.monthcode monthcode, "+
////				"        site_id, "+
////				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
////				"        round(sum(pv_cnt * kc_p_factor),5) PV_CNT_ADJ, "+
////				"        round(decode(sum(kc_n_factor),0,0,round(sum(pv_cnt * kc_p_factor),5)/sum(kc_n_factor)),2) AVG_PV, "+
////				"        round(decode(sum(B.kc_n_factor),0,1, sum(A.duration * B.kc_p_factor)/sum(B.kc_n_factor)),2) AVG_DURATION, "+
////				"        round(sum(duration * kc_p_factor),5) tot_duration_adj, "+
////				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
////				"        round(sum(B.kc_n_factor)/FN_TOTAL_MONTH_NFACTOR(substr(a.monthcode,1,6))*100,5) reach_rate_adj, "+
////				"        rank() over (order by sum(B.kc_n_factor) desc) uu_overall_rank, "+
////				"        rank() over (order by sum(A.pv_cnt*B.kc_p_factor) desc) pv_overall_rank, "+
////				"        rank() over (order by decode(sum(B.kc_p_factor),0,1, sum(A.duration*B.kc_p_factor)/sum(B.kc_n_factor)) desc) avg_duration_overall_rank, "+
////				"        rank() over (order by sum(A.duration*B.kc_p_factor) desc) tot_duration_overall_rank,     "+
////				"        rank() over (order by decode(sum(B.kc_n_factor),0,1, sum(daily_freq_cnt*B.kc_n_factor)/sum(B.kc_n_factor)) desc) freq_overall_rank, "+
////				"        rank() over (order by decode(sum(kc_n_factor),0,0,sum(pv_cnt * kc_p_factor)/sum(kc_n_factor)) desc) avg_pv_overall_rank, "+
////				"        sysdate proc_date "+
////				"from  "+
////				"( "+
////				"    select monthcode,site_id,panel_id,pv_cnt,duration,daily_freq_cnt "+
////				"    from tb_smart_month_totsite_fact "+
////				"    where monthcode = '"+monthcode+"' "+
////				") a, "+
////				"( "+
////				"    select monthcode, panel_id, kc_n_factor, kc_p_factor "+
////				"    from   tb_month_panel_seg "+
////				"    where  monthcode = '"+monthcode+"' "+
////				") b "+
////				"where a.monthcode = b.monthcode "+
////				"and   a.panel_id  = b.panel_id "+
////				"group by a.monthcode, site_id ";
//				"insert into tb_smart_MONTH_totsite_sum "+
//				"select  /*+index(a,PK_MONTH_TOTSITE_FACT)*/  "+
//				"        a.monthcode monthcode, "+
//				"        site_id, "+
//				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
//				"        round(sum(adj_pv_cnt),5) PV_CNT_ADJ, "+
//				"        round(decode(sum(kc_n_factor),0,0,round(sum(adj_pv_cnt),5)/sum(kc_n_factor)),2) AVG_PV, "+
//				"        round(decode(sum(kc_n_factor),0,1, sum(adj_duration)/sum(kc_n_factor)),2) AVG_DURATION, "+
//				"        round(sum(adj_duration),5) tot_duration_adj, "+
//				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
//				"        round(sum(kc_n_factor)/FN_PCANDROID_month_NFACTOR(a.monthcode)*100,5) reach_rate_adj, "+
//				"        rank() over (partition by a.monthcode order by sum(kc_n_factor) desc) uu_overall_rank, "+
//				"        rank() over (partition by a.monthcode order by sum(adj_pv_cnt) desc) pv_overall_rank, "+
//				"        rank() over (partition by a.monthcode order by decode(sum(adj_duration),0,1, sum(adj_duration)/sum(kc_n_factor)) desc) avg_duration_overall_rank, "+
//				"        rank() over (partition by a.monthcode order by sum(adj_duration) desc) tot_duration_overall_rank,     "+
//				"        rank() over (partition by a.monthcode order by decode(sum(kc_n_factor),0,1, sum(daily_freq_cnt*kc_n_factor)/sum(kc_n_factor)) desc) freq_overall_rank, "+
//				"        rank() over (partition by a.monthcode order by decode(sum(kc_n_factor),0,0,sum(adj_pv_cnt)/sum(kc_n_factor)) desc) avg_pv_overall_rank, "+
//				"        sysdate proc_date "+
//				"from    tb_smart_MONTH_totsite_fact a "+
//				"where   monthcode = '"+monthcode+"' "+
//				"group by monthcode, site_id ";
//		
//		this.pstmt = connection.prepareStatement(query9);
//		this.pstmt.executeUpdate();
		
		String query10 = 
//				"insert into tb_smart_month_totapp_fact "+
//				"select /*+index(a,idx_day_totsiteapp_fact)*/substr(access_day,1,6) monthcode, site_id, panel_id, "+
//				"       sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate "+
//				"from  TB_SMART_MONTH_TOTAL_FACT a "+
//				"where substr(access_day,1,6) = '"+monthcode+"' "+
//				"and   tab_cd in ('PA','MA') "+
//				"group by substr(access_day,1,6), site_id, panel_id ";
				"insert into tb_smart_MONTH_totapp_fact "+
				"select substr(access_day,1,6) monthcode, site_id, panel_id, "+
				"       sum(pv_cnt) pv_cnt, sum(duration) duration, "+
				"       count(distinct access_day) daily_freq_cnt, sysdate, "+
				"       max(kc_n_factor) kc_n_factor,  "+
				"       sum(adj_pv_cnt) adj_pv_cnt, sum(adj_duration) adj_duration "+
				"from ( "+
				"    select /*+ index(a,PK_MONTH_TOTAL_FACT) */ access_day, "+
				"           site_id, a.panel_id, pv_cnt, duration, kc_n_factor, kc_p_factor, "+
				"           pv_cnt*kc_p_factor adj_pv_cnt, duration*kc_p_factor adj_duration "+
				"    from   TB_SMART_MONTH_TOTAL_FACT a, tb_month_panel_seg b "+
				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and    monthcode = '"+monthcode+"' "+
				"    and    a.panel_id = b.panel_id "+
				"    and    tab_cd = 'PA' "+
				" "+
				"    union all "+
				" "+
				"    select /*+ index(a,PK_MONTH_TOTAL_FACT) */ access_day, "+
				"           site_id, a.panel_id, pv_cnt, duration, mo_n_factor, mo_p_factor, "+
				"           pv_cnt*mo_p_factor adj_pv_cnt, duration*mo_p_factor adj_duration "+
				"    from   TB_SMART_MONTH_TOTAL_FACT a, tb_smart_month_panel_seg b "+
				"    where  access_day between '"+monthcode+"'||'01' and fn_month_lastday('"+monthcode+"') "+
				"    and    monthcode = '"+monthcode+"' "+
				"    and    a.panel_id = b.panel_id "+
				"    and    tab_cd = 'MA' "+
				") "+
				"group by substr(access_day,1,6), site_id, panel_id ";
		this.pstmt = connection.prepareStatement(query10);
		this.pstmt.executeUpdate();
		
		String query11 = 
//				"insert into tb_smart_month_totapp_sum "+
//				"select  /*+use_hash(b,a)*/ "+
//				"        a.monthcode monthcode, "+
//				"        site_id, "+
//				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
//				"        round(decode(sum(B.kc_n_factor),0,1, sum(A.duration * B.kc_p_factor)/sum(B.kc_n_factor)),2) AVG_DURATION, "+
//				"        round(sum(duration * kc_p_factor),5) tot_duration_adj, "+
//				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
//				"        round(sum(B.kc_n_factor)/FN_TOTAL_MONTH_NFACTOR(a.monthcode)*100,5) reach_rate_adj, "+
//				"        rank() over (order by sum(B.kc_n_factor) desc) uu_overall_rank, "+
//				"        rank() over (order by decode(sum(B.kc_p_factor),0,1, sum(A.duration*B.kc_p_factor)/sum(B.kc_n_factor)) desc) avg_duration_overall_rank, "+
//				"        rank() over (order by sum(A.duration*B.kc_p_factor) desc) tot_duration_overall_rank,     "+
//				"        rank() over (order by decode(sum(B.kc_n_factor),0,1, sum(daily_freq_cnt*B.kc_n_factor)/sum(B.kc_n_factor)) desc) freq_overall_rank, "+
//				"        sysdate proc_date "+
//				"from  "+
//				"( "+
//				"    select monthcode,site_id,panel_id,pv_cnt,duration,daily_freq_cnt "+
//				"    from tb_smart_month_totapp_fact "+
//				"    where monthcode = '"+monthcode+"' "+
//				") a, "+
//				"( "+
//				"    select monthcode, panel_id, kc_n_factor, kc_p_factor "+
//				"    from   tb_month_panel_seg "+
//				"    where  monthcode = '"+monthcode+"' "+
//				") b "+
//				"where a.monthcode = b.monthcode "+
//				"and   a.panel_id  = b.panel_id "+
//				"group by a.monthcode, site_id ";
				"insert into tb_smart_MONTH_totapp_sum "+
				"select  /*+index(a,PK_MONTH_TOTAPP_FACT)*/  "+
				"        a.monthcode monthcode, "+
				"        site_id, "+
				"        round(sum(kc_n_factor),5) UU_CNT_ADJ, "+
				"        round(decode(sum(kc_n_factor),0,1, sum(adj_duration)/sum(kc_n_factor)),2) AVG_DURATION, "+
				"        round(sum(adj_duration),5) tot_duration_adj, "+
				"        round(sum(daily_freq_cnt * kc_n_factor)/sum(kc_n_factor),2) daily_freq_cnt, "+
				"        round(sum(kc_n_factor)/FN_PCANDROID_month_NFACTOR(a.monthcode)*100,5) reach_rate_adj, "+
				"        rank() over (partition by a.monthcode order by sum(kc_n_factor) desc) uu_overall_rank, "+
				"        rank() over (partition by a.monthcode order by decode(sum(adj_duration),0,1, sum(adj_duration)/sum(kc_n_factor)) desc) avg_duration_overall_rank, "+
				"        rank() over (partition by a.monthcode order by sum(adj_duration) desc) tot_duration_overall_rank,     "+
				"        rank() over (partition by a.monthcode order by decode(sum(kc_n_factor),0,1, sum(daily_freq_cnt*kc_n_factor)/sum(kc_n_factor)) desc) freq_overall_rank, "+
				"        sysdate proc_date "+
				"from    tb_smart_MONTH_totapp_fact a "+
				"where monthcode = '"+monthcode+"' "+
				"group by a.monthcode, site_id ";
		
		this.pstmt = connection.prepareStatement(query11);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeDayTotal
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeDayTotal 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeDayTotal(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daily Total Fact is processing...");
		
//		String queryT = "truncate table tb_temp_day_wapp_fact";
//        this.pstmt = connection.prepareStatement(queryT);
//		this.pstmt.executeUpdate();
//			
//		String query1 = 
//				"insert into tb_temp_day_wapp_fact "+
//				"select  /*+use_hash(b,a)*/ "+
//				"        a.access_day,a.site_id,a.app_id,a.panel_id, 0 pv_cnt,  "+
//				"        case when ( a.duration - b.duration ) < 0 then 0 else ( a.duration - b.duration ) end duration, "+
//				"        sysdate "+
//				"from "+
//				"( "+
//				"    select access_day, site_id, app_id, panel_id, duration  "+
//				"    from tb_day_app_fact  "+
//				"    where access_day = '"+accessday+"' "+
//				") a, "+
//				"( "+
//				"    select access_day, site_id, app_id, panel_id, duration  "+
//				"    from tb_day_wapp_fact  "+
//				"    where access_day = '"+accessday+"' "+
//				") b "+
//				"where a.access_day = b.access_day "+
//				"and a.site_id = b.site_id "+
//				"and a.app_id = b.app_id "+
//				"and a.panel_id = b.panel_id ";
//        this.pstmt = connection.prepareStatement(query1);
//		this.pstmt.executeUpdate();
//		
		String query2 = 
				"insert into tb_smart_day_total_fact "+
				"select access_day, site_id, panel_id, tab_cd, sum(pv_cnt) pv_cnt, sum(duration) duration, sysdate "+
				"from "+
				"( "+
//				"    select  access_day, site_id, panel_id, 'PW' tab_cd, pv_cnt, duration "+
//				"    from    tb_day_fact  "+
//				"    where   access_day = '"+accessday+"' "+
//				"    AND     category_code1 <> 'Z' "+
//				"    union all "+
//				"    select  /*+use_hash(b,a)*/ access_day, b.site_id, panel_id, 'PA' tab_cd, 0 pv_cnt, duration "+
//				"    from    tb_day_app_fact a, tb_app_info b "+
//				"    where   access_day = '"+accessday+"' "+
//				"    and     a.app_id = b.app_id "+
//				"    and     b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
//				"    and     b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
//				"    and     (a.access_day,a.site_id,a.app_id,a.panel_id ) not in ( "+
//				"                                                                    select access_day,site_id,app_id,panel_id  "+
//				"                                                                    from TB_TEMP_DAY_WAPP_FACT  "+
//				"                                                                    where access_day = '"+accessday+"' "+
//				"                                                                    group by access_day,site_id,app_id,panel_id  "+
//				"                                                                 ) "+
//				"    AND    a.app_type_cd != 'Z' "+
//				"    AND    a.app_id not in (2656,2657,1764,2653) "+
//				"    union all "+
//				"    select  /*+use_hash(b,a)*/access_day, a.site_id, panel_id, 'PW' tab_cd, pv_cnt, 0 duration "+
//				"    from    tb_day_vapp_fact  a, tb_app_info b "+
//				"    where   access_day = '"+accessday+"' "+
//				"    and    a.site_id = b.site_id "+
//				"    AND    a.app_id  = b.app_id "+
//				"    and    b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
//				"    and    b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
//				"    AND    b.app_type_cd < 'Z' "+
//				"    AND    RESULT_CD='Y' "+
//				"    union all "+
//				"    select  access_day, site_id, panel_id, 'PW' tab_cd, pv_cnt, duration "+
//				"    from    TB_TEMP_DAY_WAPP_FACT  "+
//				"    where   access_day = '"+accessday+"' "+
//				"    union all "+
//				"    SELECT access_day, to_number(code_etc) site_id, panel_id, 'PA' tab_cd, 0 pv_cnt, duration "+
//				"    FROM   tb_day_mess_fact a, (select code, code_etc from tb_codebook where meta_code = 'MESSENGER') b "+
//				"    WHERE  access_day = '"+accessday+"' "+
//				"    AND    a.messenger = code "+
//				"    union all "+
				"    select access_day, site_id, panel_id, 'MW' tab_cd, pv_cnt, duration "+
				"    from   tb_smart_day_fact "+
				"    where  access_day = '"+accessday+"' "+
				"    union all "+
				"    select /*+use_hash(b,a)*/ "+
				"           access_day, b.site_id, panel_id, 'MA' tab_cd, 0 pv_cnt, duration "+
				"    from   tb_smart_day_app_fact a, tb_smart_app_info b "+
				"    where  access_day = '"+accessday+"' "+
				"    and    a.smart_id = b.smart_id "+
				"    and    b.site_id > 0 "+
				"    and    b.ef_time < to_date(a.access_day,'yyyymmdd')+1 "+
				"    and    b.exp_time > to_date(a.access_day,'yyyymmdd')+1 "+
				") "+
				"group by access_day, site_id, panel_id, tab_cd ";
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}
	
	
	
	public String executeGetWeekcode(String accessday) throws SQLException 
	{
		String weekcode = "";
		String sql = 
		         "select fn_weekcode('"+accessday+"') weekcode " +
		         "from   dual ";   
		try {
		    ResultSet rs = null;
		    this.pstmt = connection.prepareStatement(sql);
		    rs = this.pstmt.executeQuery(sql);	
		    rs.next();
		    weekcode = rs.getString("WEEKCODE");
		}  catch (SQLException e) {
		    e.printStackTrace();
		}
		return weekcode; 
	}
	
	
	public String executeGetMonthLastDay(String month) throws SQLException 
	{
		String lastday = "";
		String sql = 
		         "select fn_month_lastday('"+month+"') lastday " +
		         "from   dual ";   
		try {
		    ResultSet rs = null;
		    this.pstmt = connection.prepareStatement(sql);
		    rs = this.pstmt.executeQuery(sql);	
		    rs.next();
		    lastday = rs.getString("lastday");
		}  catch (SQLException e) {
		    e.printStackTrace();
		}
		return lastday; 
	}	
	
	
	
	/**************************************************************************
	 *		메소드명		: executeWeekPanel
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeWeekPanel 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekPanel(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Weekly Panel Seg is processing...");
		
		String query = 
			"insert into TB_SMART_PANEL_SEG "+
			"select /*+use_hash(b,a)*/ WEEKCODE, PANEL_ID, KC_N_FACTOR, " +
			"		FN_SMART_PFACTOR(sex_cls, age_cls,'"+accessday+"')*KC_N_FACTOR MO_P_FACTOR, " +
			"		SEX_CLS, AGE_CLS, JOB_CLS, EDUCATION_CLS, INCOME_CLS, ISMARRIED_CLS, REGION_CD, LIFESTYLE_CLS, KC_SEG_ID, RI_SEG_ID, PROC_DATE, TELECOM_CD, DEVICE_CD "+
			"from   tb_week_total_panel_seg "+
			"where  weekcode = fn_weekcode('"+accessday+"') "+
			"and device_cd in ('10','30','60','70') ";
//			"and panel_id in ( "+
////			"    select panel_id from tb_smart_month_doner_panel where monthcode = substr('"+accessday+"',1,6) and panel_flag = 'Y' "+
////			"    union all "+
////			"    select v_panel_id from tb_smart_month_recipient_panel where monthcode = substr('"+accessday+"',1,6) "+
////			"    select panel_id from tb_smart_doner_panel where exp_time > sysdate and panel_flag = 'Y' "+
////			"    union all "+
////			"    select v_panel_id from tb_smart_recipient_panel where exp_time > sysdate "+
//			"	select panel_id "+
//			"	from   tb_week_total_loc_panel_seg "+
//			"	where  weekcode = fn_weekcode('"+accessday+"') " +
//			"	and    mobile_cd = '10'"+
//			") ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String query2 =
			"insert into TB_SMART_WEEK_NETIZEN_CNT "+
			"select a.weekcode, NETIZEN_CNT, panel_cnt "+
			"from "+
			"( "+
			"    select fn_weekcode('"+accessday+"') weekcode, sum(NETIZEN_CNT) NETIZEN_CNT "+
			"    from tb_nielsen_netizen "+
			"    where exp_time > to_date('"+accessday+"','yyyymmdd') "+
			"    and   ef_time < to_date('"+accessday+"','yyyymmdd') + 1"+
			//"    and   mobile_cd = '10' "+
			"	 and device_cd in ('10','30','60','70') "+
			")a, "+
			"( "+
			"    select weekcode, count(*) panel_cnt "+
			"    from tb_smart_panel_seg "+
			"    where weekcode = fn_weekcode('"+accessday+"') "+
			"    group by weekcode "+
			")b "+
			"where a.weekcode = b.weekcode ";
		this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();		

		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeMonthPanel
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: executeMonthPanel 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeMonthPanel(String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Monthly Panel Seg is processing...");
		
		if(!countTable(monthcode, "m", "tb_smart_month_panel_seg")){
			String queryD = 
				"delete tb_smart_month_panel_seg where monthcode = '"+monthcode+"' ";
			
	        this.pstmt = connection.prepareStatement(queryD);
			this.pstmt.executeUpdate();
		}
		
		String query = 
			"insert into tb_smart_month_panel_seg "+
			"select /*+use_hash(b,a)*/ " +
			"		MONTHCODE, PANEL_ID, KC_N_FACTOR, " +
			"		FN_SMART_PFACTOR(sex_cls, age_cls,fn_month_lastday('"+monthcode+"'))*KC_N_FACTOR MO_P_FACTOR, " +
			"		SEX_CLS, AGE_CLS, JOB_CLS, EDUCATION_CLS, INCOME_CLS, ISMARRIED_CLS, REGION_CD, LIFESTYLE_CLS, PROC_DATE, KC_SEG_ID, RI_SEG_ID, TELECOM_CD, DEVICE_CD "+
			"from   tb_month_total_panel_seg "+
			"where  MONTHCODE = '"+monthcode+"' "+
			"and device_cd in ('10','30','60','70') ";
//			"and panel_id in ( "+
//	//		"    select panel_id from tb_smart_month_doner_panel where monthcode = substr('"+accessday+"',1,6) and panel_flag = 'Y' "+
//	//		"    union all "+
//	//		"    select v_panel_id from tb_smart_month_recipient_panel where monthcode = substr('"+accessday+"',1,6) "+
//	//		"    select panel_id from tb_smart_doner_panel where exp_time > sysdate and panel_flag = 'Y' "+
//	//		"    union all "+
//	//		"    select v_panel_id from tb_smart_recipient_panel where exp_time > sysdate "+
//			"	select panel_id "+
//			"	from   tb_month_total_loc_panel_seg "+
//			"	where  MONTHCODE = '"+monthcode+"' " +
//			"	and    mobile_cd = '10'"+
//			") ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String query2 =
			"insert into TB_SMART_MONTH_NETIZEN_CNT "+
			"select a.monthcode, NETIZEN_CNT, panel_cnt "+
			"from "+
			"( "+
			"    select '"+monthcode+"' monthcode, sum(NETIZEN_CNT) NETIZEN_CNT "+
			"    from tb_nielsen_netizen "+
			"    where exp_time > to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd') "+
			"    and   ef_time < to_date(fn_month_lastday('"+monthcode+"'),'yyyymmdd') +1 "+
			//"    and   mobile_cd = '10' "+
			"	 and device_cd in ('10','30','60','70') "+
			")a, "+
			"( "+
			"    select monthcode, count(*) panel_cnt "+
			"    from tb_smart_month_panel_seg "+
			"    where monthcode = '"+monthcode+"' "+
			"    group by monthcode "+
			")b "+
			"where a.monthcode = b.monthcode ";	
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();

		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	public Calendar executeDayPanel(String period) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daily Panel Seg is processing...");
		
		if(countTable(period, "d", "tb_day_total_panel_seg")){
			SmsSender.Send("Mobile_Manual_Batch_Daily_"+period+"_is_ERROR._Uncompleted_executeDayPanel");
			System.exit(0);	        
		}	
		
		if(countNfactor(period, "tb_day_total_panel_seg")){	 // kc_n_factor == 0 일 경우 data 처리 X
			SmsSender.Send("Mobile_Manual_Batch_Daily_"+period+"_is_ERROR._Uncompleted_nfactor");
			System.exit(0);	        
		}	 
		
		if(!countTable(period, "d", "tb_smart_day_panel_seg")){
			String queryD = 
				"delete tb_smart_day_panel_seg where access_day = '"+period+"' ";
			
	        this.pstmt = connection.prepareStatement(queryD);
			this.pstmt.executeUpdate();
		}
		
		String query = 
				"INSERT INTO tb_smart_day_panel_seg "+
						"SELECT access_day, panel_id, kc_n_factor, KC_N_FACTOR*FN_SMART_PFACTOR(sex_cls, age_cls,'"+period+"') mo_p_factor, "+
						" sex_cls, age_cls, job_cls, education_cls, income_cls, ismarried_cls, region_cd, lifestyle_cls, kc_seg_id, ri_seg_id, proc_date, telecom_cd, device_cd "+
						"FROM tb_day_total_panel_seg "+
						"WHERE access_day = '"+period+"' "+
						"and device_cd in ('10','30','60','70')";
						//"AND panel_id IN (SELECT panel_id FROM tb_day_total_loc_panel_seg WHERE access_day = '"+period+"' AND mobile_cd = '10') ";
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		String query2 =
				"insert into TB_SMART_DAY_NETIZEN_CNT "+
				"select a.access_Day, NETIZEN_CNT, panel_cnt "+
				"from "+
				"( "+
				"    select '"+period+"' access_day, sum(NETIZEN_CNT) NETIZEN_CNT "+
				"    from tb_nielsen_netizen "+
				"    where exp_time > to_date('"+period+"','yyyymmdd') "+
				"    and   ef_time < to_date('"+period+"','yyyymmdd') + 1 "+
				//"    and   mobile_cd = '10' "+
				"    and device_cd in ('10','30','60','70')"+
				")a, "+
				"( "+
				"    select access_Day, count(*) panel_cnt "+
				"    from tb_smart_day_panel_seg "+
				"    where access_day = '"+period+"' "+
				"    group by access_day "+
				")b "+
				"where a.access_Day = b.access_Day ";	
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();

		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: insertVitual
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 각 테이블 별로 Vitual Table에 insert를 시킨다.
	 *************************************************************************/
	
	public Calendar insertBrowserVitual(String period) throws SQLException {
		Calendar eachPt = Calendar.getInstance();
		Collection columns = new ArrayList<String>();
		String table_name = "TB_SMART_BROWSER_ITRACK";
		ResultSet rs = null;
		String timeset = "";
		String columnnames = "";
		
		String sqltest = 
			"select count(*) CNT "+
			"from TB_SMART_BROWSER_ITRACK "+
			"where panel_flag in ('N','V') "+
			"and access_day = '"+period+"'";
		
		try {
			this.pstmt = connection.prepareStatement(sqltest);
			rs = this.pstmt.executeQuery(sqltest);
			
			while (rs.next()) {	
				if(rs.getString("CNT") != null){
					int cnt = rs.getInt("CNT");
					if(cnt > 0){
						System.out.println("Browser Virtual Insertion already has been done.");
						System.exit(0);
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	
		String sql1 = 
			"select COLUMN_NAME "+
			"from USER_TAB_COLUMNS "+
			"where table_name = '"+table_name+"' "+
			"order by COLUMN_ID ";
		
		try {
			this.pstmt = connection.prepareStatement(sql1);
			rs = this.pstmt.executeQuery(sql1);
			
			while (rs.next()) {	
				if(rs.getString("COLUMN_NAME") != null){
					String column_name = rs.getString("COLUMN_NAME");
					columns.add(column_name);
					if(column_name.equals("ACCESS_DAY")){
						timeset = column_name;
					} else if(column_name.equals("MONTHCODE")){
						timeset = column_name;
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}		
		rs = null;
		
		if(columns != null){
			Iterator column = columns.iterator();
			while(column.hasNext()){
				String columnname = (String)column.next()+", ";
				if(columnname.equals("PANEL_ID, ")){
					columnname="b.V_PANEL_ID, ";
				}
				if(columnname.equals("PANEL_FLAG, ")){
					columnname="'V' PANEL_FLAG, ";
				}
				columnnames+=columnname;
			}
			columnnames = columnnames.substring(0,columnnames.lastIndexOf(","));
		}
		
		String query2 = "update TB_SMART_BROWSER_ITRACK a "+
						"set PANEL_FLAG = 'N' "+
						"where not exists (" +
//						"		select panel_id " +
//						"		from tb_smart_month_doner_panel " +
//						"		where monthcode = substr('"+period+"',1,6) " +
//						"		and   panel_flag = 'Y' " +
						"		select panel_id " +
						"		from tb_smart_doner_panel b " +
						"		where exp_time > to_date('"+period+"','yyyymmdd') " +
						"		and   ef_time  < to_date('"+period+"','yyyymmdd')+1 "+
						"		and   b.panel_flag = 'Y' " +
						"       and   a.panel_id = b.panel_id "+
//						"		select panel_id " +
//						"		from tb_smart_doner_panel " +
//						"		where ef_time  = to_date('20120801','yyyymmdd') "+
//						"		and   panel_flag = 'Y' " +
						") " +
						"and "+timeset+" = '"+period+"'";
//		System.out.println(query2);
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();

		if(this.pstmt!=null) this.pstmt.close();
		
		String query4 = 
				"insert into TB_SMART_BROWSER_ITRACK "+
//				"select " +columnnames+" "+
//				"from  TB_SMART_BROWSER_ITRACK a, tb_smart_month_recipient_panel b "+
//				"where a.panel_id = b.panel_id "+
//				"and   a.panel_id in ( " +
//				"	select panel_id " +
//				"	from tb_smart_month_doner_panel " +
//				"	where monthcode = substr('"+period+"',1,6) " +
//				") "+
//				"and   "+timeset+" = '"+period+"' " +
//				"and   b.monthcode = substr('"+period+"',1,6) ";
				"select " +columnnames+" "+
				"from  TB_SMART_BROWSER_ITRACK a, tb_smart_recipient_panel b "+
				"where a.panel_id = b.panel_id "+
				"and   a.panel_id in ( " +
				"	select panel_id " +
				"	from   tb_smart_doner_panel " +
				"	where  exp_time > to_date('"+period+"','yyyymmdd') " +
				"	and   ef_time  < to_date('"+period+"','yyyymmdd')+1 "+
//				"	select panel_id " +
//				"	from tb_smart_doner_panel " +
//				"	where ef_time  = to_date('20120801','yyyymmdd') "+
				") "+
				"and   "+timeset+" = '"+period+"' " +
				"and   exp_time > to_date('"+period+"','yyyymmdd') "+
				"and   ef_time  < to_date('"+period+"','yyyymmdd')+1 ";
//				"and   ef_time  = to_date('20120801','yyyymmdd') ";
//		System.out.println(query4);
        this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();

		if(this.pstmt!=null) this.pstmt.close();
		
//		String query5 = "update TB_SMART_BROWSER_ITRACK "+
//						"set PANEL_FLAG = 'N' "+
//						"where panel_id in " +
//						"(select panel_id from tb_smart_month_doner_panel " +
//						"where monthcode = substr('"+period+"',1,6)" +
//						"and   panel_flag = 'N') " +
//						"and "+timeset+" = '"+period+"'";
//		//System.out.println(query2);
//		this.pstmt = connection.prepareStatement(query5);
//		this.pstmt.executeUpdate();
//		
//		if(this.pstmt!=null) this.pstmt.close();

		System.out.println("DONE.");
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: insertVitual
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 각 테이블 별로 Vitual Table에 insert를 시킨다.
	 *************************************************************************/
	
	public Calendar insertVitual(String period, String table_name) throws SQLException {
		Calendar eachPt = Calendar.getInstance();
		Collection columns = new ArrayList<String>();
		String vitual_table = "";
		ResultSet rs = null;
		String timeset = "";
		String code = "";
		String columnnames = "";
		String monthcode = period.substring(0,6);
		String condition = "";
		
		String sql1 = "select COLUMN_NAME "+
					  "from USER_TAB_COLUMNS "+
					  "where lower(table_name) = lower('"+table_name+"') "+
					  "order by COLUMN_ID ";
		
		try {
			this.pstmt = connection.prepareStatement(sql1);
			rs = this.pstmt.executeQuery(sql1);
			
			while (rs.next()) {	
				if(rs.getString("COLUMN_NAME") != null){
					String column_name = rs.getString("COLUMN_NAME");
					columns.add(column_name);
					if(column_name.equals("ACCESS_DAY")){
						timeset = column_name;
						code = "D";
						condition = "'"+period+"'";
						period = "'"+period+"'";
					} else if(column_name.equals("MONTHCODE")){
						timeset = column_name;
						code = "M";
						condition = "fn_month_lastday("+monthcode+")";
						period = "'"+period+"'";
					} else if(column_name.equals("WEEKCODE")){
						timeset = column_name;
						code = "W";
						condition = "'"+period+"'";
						period = "fn_weekcode('"+period+"')";
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}		
		rs = null;
		String sql2 = "select 	vitual_table " +
				      "from 	tb_smart_vitual_table_info " +
				      "where 	lower(fact_table) = lower('"+table_name+"')";
		
		try {
			this.pstmt = connection.prepareStatement(sql2);
			rs = this.pstmt.executeQuery(sql2);
			
			while (rs.next()) {	
				if(rs.getString("vitual_table") != null){
					vitual_table = rs.getString("vitual_table");
				} else {
					System.out.println("Virtual Table information does not exist.");
					System.exit(0);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		if(columns != null){
			Iterator column = columns.iterator();
			while(column.hasNext()){
				String columnname = (String)column.next()+", ";
				if(columnname.equals("PANEL_ID, ")){
					columnname="b.V_PANEL_ID, ";
				} else if (columnname.equals("MONTHCODE, ")){
					columnname="a.MONTHCODE, ";
				}
				columnnames+=columnname;
			}
			columnnames = columnnames.substring(0,columnnames.lastIndexOf(","));
		}
		
		String query1 = "insert into "+vitual_table+" "+
//	    		"select a.* from "+table_name+" a, tb_smart_month_doner_panel b " +
//	    		"where a."+timeset+" = "+period+" " +
//	    		"and   b.monthcode = '"+monthcode+"' "+
//	    		"and   a.panel_id = b.panel_id " +
//	    		"and   b.panel_flag = 'Y' ";
				"select /*+ use_hash(b,a) leading(b) index(a,PK_SMART_DAYTIME_APP_WGT) */ a.* from "+table_name+" a, tb_smart_doner_panel b " +
				"where a."+timeset+" = "+period+" " +
				"and   b.exp_time > to_date("+condition+",'yyyymmdd') " +
				"and   b.ef_time  < to_date("+condition+",'yyyymmdd')+1 "+
				"and   a.panel_id = b.panel_id " +
				"and   b.panel_flag = 'Y' ";
//		System.out.println(query1);
//		System.exit(0);
        this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query4 = 
				"insert into "+vitual_table+" "+
//				"select " +columnnames+" "+
//				"from "+table_name+" a, tb_smart_month_recipient_panel b "+
//				"where a.panel_id = b.panel_id "+
//				"and   a."+timeset+" = "+period+" "+
//				"and   b.monthcode = '"+monthcode+"' " +
//				"and   a.panel_id in ( " +
//				"	select panel_id " +
//				"	from tb_smart_month_doner_panel " +
//				"	where monthcode = '"+monthcode+"' " +
//				") ";
				"select /*+ use_hash(b,a) leading(b) index(a,PK_SMART_DAYTIME_APP_WGT) */ " +columnnames+" "+
				"from "+table_name+" a, tb_smart_recipient_panel b "+
				"where a.panel_id = b.panel_id "+
				"and   a."+timeset+" = "+period+" "+
				"and   b.exp_time > to_date("+condition+",'yyyymmdd') " +
				"and   b.ef_time  < to_date("+condition+",'yyyymmdd')+1 "+
				"and   a.panel_id in ( " +
				"	select panel_id " +
				"	from tb_smart_doner_panel " +
				"	where exp_time > to_date("+condition+",'yyyymmdd') " +
				"	and   ef_time  < to_date("+condition+",'yyyymmdd')+1 " +
				") ";
		
		//System.out.println(query4);
        this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
//		String query5 = "delete "+vitual_table+" "+
//						"where panel_id in " +
//						"(	select panel_id " +
//						"	from tb_smart_month_doner_panel " +
//						"	where monthcode = '"+monthcode+"' " +
//						"	and   panel_flag = 'N' ) "+
////						"(	select panel_id " +
////						"	from  tb_smart_doner_panel " +
////						"	where exp_time > to_date('"+period+"','yyyymmdd')+1 " +
////						"	and   ef_time  < to_date('"+period+"','yyyymmdd')+1 " +
////						"	and   panel_flag = 'N' ) "+
//						"and "+timeset+" = '"+period+"'";
//		//System.out.println(query5);
//		this.pstmt = connection.prepareStatement(query5);
//		this.pstmt.executeUpdate();
//		
//		if(this.pstmt!=null) this.pstmt.close();

		System.out.println("DONE.");
		return eachPt;
	}
	
	
	public Calendar executeDayAppNavi(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daily tb_smart_day_app_navi_fact is processing...");
		
		String query1 = 
				"insert into tb_smart_day_app_navi_fact "+
				"select access_day, smart_id, package_name, vpanel_id panel_id, next_smart_id, next_package_name, move_cnt "+
				"from "+
				"( "+
				"    select access_day, smart_id, package_name, panel_id, next_smart_id, next_package_name, count(*) move_cnt "+
				"    from "+
				"    ( "+
				"        select access_day, smart_id, package_name, panel_id, "+
				"               lead(smart_id,1)over(partition by access_day, panel_id order by register_Date) next_smart_id, "+
				"               lead(package_name,1)over(partition by access_day, panel_id order by register_Date) next_package_name "+
				"        from "+
				"        ( "+
				"            select  /*+parallel(a,8) leading(b)*/ "+
				"                    access_day, b.smart_id, b.package_name, panel_id, register_Date "+
				"            from    tb_smart_env_itrack a, "+
				"            ( "+
				"                select PACKAGE_NAME o_package_name,decode(p_smart_id,null,PACKAGE_NAME,'kclick_equal_app')  PACKAGE_NAME, decode(p_smart_id,null,SMART_ID,p_smart_id) SMART_ID "+
				"                from tb_smart_app_info "+
				"                where exp_time > to_date('"+accessday+"','yyyymmdd') +1 "+
				"                and   ef_time  <  to_date('"+accessday+"','yyyymmdd') +1 "+
				"                and   ((smart_id not in (select smart_id from TB_SMART_APP_LAUN_LIST where exp_time > sysdate)) and (smart_id not in (652,2005,26977,170223))) "+
				"            ) b "+
				"            WHERE   access_day = '"+accessday+"' "+
				"            AND     a.ITEM_VALUE is not null "+
				"            AND     a.ITEM_VALUE = b.o_package_name "+
				"            AND     a.SUBJECT in ('APP', 'MEDIA', 'CHANNEL') "+
				"            AND     duration > 0 "+
				"            AND     to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
				"        ) "+
				"    ) "+
				"    where smart_id <> next_smart_id "+
				"    group by access_day, smart_id, package_name, panel_id, next_smart_id, next_package_name "+
				")a, tb_smart_drv_panel b "+
				"where exp_time > to_date('"+accessday+"','yyyymmdd') "+
				"and ef_time < to_date('"+accessday+"','yyyymmdd') +1 "+
				"and a.panel_id = b.panel_id ";
		
        this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();		

		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}	
	
	
	public Calendar executeDayAppSum(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daily tb_smart_day_app_sum is processing...");
		
		String query1 = 
				"insert into TB_SMART_DAY_APP_SUM "+
				"SELECT   /*+ordered*/  "+
				"     A.access_day,  SMART_ID, PACKAGE_NAME, APP_NAME, PRO_ID, SITE_ID, "+
				"     min(app_category_cd1) app_category_cd1, min(app_category_cd2) app_category_cd2, "+
				"     round(count(A.panel_id)/FN_SMART_day_COUNT(A.access_day)*100,2) reach_rate, "+
				"     count(A.panel_id) uu_cnt, "+
				"     rank() over (partition by A.access_day, type order by sum(B.mo_n_factor) desc) uu_overall_rank, "+
				"     rank() over (partition by A.access_day, type, min(app_category_cd1) order by sum(B.mo_n_factor) desc) uu_1level_rank, "+
				"     rank() over (partition by A.access_day, type, min(app_category_cd2) order by sum(B.mo_n_factor) desc) uu_2level_rank, "+
				"     round(decode(sum(B.mo_n_factor),0, 0, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)),2) avg_duration, "+
				"     rank() over (partition by A.access_day, type order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_overall_rank, "+
				"     rank() over (partition by A.access_day, type,  min(app_category_cd1) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_1level_rank, "+ 
				"     rank() over (partition by A.access_day, type,  min(app_category_cd2) order by decode(sum(B.mo_n_factor),0,1, sum(A.duration*B.mo_p_factor)/sum(B.mo_n_factor)) desc) avg_duration_2level_rank, "+
				"     round(sum(B.mo_n_factor),5) uu_cnt_adj, "+
				"     round(sum(B.mo_n_factor)/FN_SMART_day_NFACTOR(A.access_day)*100,5) reach_rate_adj, "+
				"     rank() over (partition by A.access_day, type order by sum(A.duration*B.mo_p_factor) desc) tot_duration_overall_rank, "+
				"     round(sum(A.duration*B.mo_p_factor),5) tot_duration_adj, "+
				"     sysdate, "+
				"     round(sum(app_cnt*mo_p_factor),5) app_cnt_adj "+
				"FROM "+ 
				"( "+
				"    SELECT   /*+index(a,pk_smart_day_app_fact)*/ access_day, a.SMART_ID, a.PACKAGE_NAME, PANEL_ID, DURATION, "+
				"             PRO_ID, APP_NAME, "+
				"             APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, app_cnt, 'All' type "+
				"    FROM     tb_smart_day_app_fact a, tb_smart_app_info b "+
				"    WHERE    a.access_day = '"+accessday+"' "+
				"    AND      a.smart_id = b.smart_id "+ 
				"    AND      b.p_smart_id is null "+
				"    AND      b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
				"    AND      b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
				"    union all "+
				"    SELECT   /*+index(a,pk_smart_day_app_fact)*/ access_day, a.SMART_ID, a.PACKAGE_NAME, PANEL_ID, DURATION, "+
				"             PRO_ID, APP_NAME, "+
				"             APP_CATEGORY_CD1, APP_CATEGORY_CD2, SITE_ID, app_cnt, 'Equal' type "+
				"    FROM     tb_smart_day_app_fact a, tb_smart_app_info b "+
				"    WHERE    a.access_day = '"+accessday+"' "+
				"    AND      a.smart_id = b.smart_id "+
				"    AND      b.p_smart_id is not null "+
				"    AND      b.exp_time > to_date('"+accessday+"','yyyymmdd')+1 "+
				"    AND      b.ef_time < to_date('"+accessday+"','yyyymmdd')+1 "+
				") A, "+ 
				"( "+
				"SELECT   access_day, panel_id, mo_n_factor, mo_p_factor "+
				"FROM     tb_smart_day_panel_seg  "+
				"WHERE    access_day = '"+accessday+"' "+
				") B "+
				"WHERE    A.access_day = B.access_day "+
				"and      A.panel_id = B.panel_id "+
				"GROUP BY A.access_day, SMART_ID, PACKAGE_NAME, APP_NAME, PRO_ID, SITE_ID , type";
        this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();		

		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}	
	
	
	
	public Calendar executePushVirtual(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daily tb_smart_day_push_panel_fact is processing...");
		
		String query1 = "insert into tb_smart_day_push_panel_fact " +
				        "select /*+ use_hash(b,a) leading(b) */ a.* " +
				        "from ( " +
						        "select  * " +
						        "from tb_smart_day_push_panel "+
						        "where access_day = '"+accessday+"' "+
						        "and (smart_id, panel_id ) in "+ 
						        "    ( "+
						        "        select smart_id, panel_id  "+
						        "        from tb_smart_day_push_panel "+
						        "        where access_day = '"+accessday+"' "+
						        "        minus "+
						        "        select smart_id, panel_id "+
						        "        from tb_smart_day_nopush_panel "+
						        "        where access_day = '"+accessday+"' "+        
						        "    ) "+
				        ")a, tb_smart_doner_panel b "+  
						"where a.access_day = '"+accessday+"' "+
						"and   b.exp_time > to_date('"+accessday+"','yyyymmdd') "+ 
						"and   b.ef_time  < to_date('"+accessday+"','yyyymmdd')+1 "+ 
						"and   a.panel_id = b.panel_id "+  
						"and   b.panel_flag = 'Y' "; 
        this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();		
		
		String query2 = 
						"insert into tb_smart_day_push_panel_fact "+
						"select  /*+ use_hash(b,a) leading(b) */ access_day, smart_id, package_name, v_panel_id, duration, proc_date, app_cnt "+
						"from ( "+
						"        select  * "+
						"        from tb_smart_day_push_panel "+
						"        where access_day = '"+accessday+"' "+
						"        and (smart_id, panel_id ) in "+ 
						"            ( "+
						"                select smart_id, panel_id "+
						"                from tb_smart_day_push_panel "+
						"                where access_day = '"+accessday+"' "+
						"                minus "+
						"                select smart_id, panel_id "+
						"                from tb_smart_day_nopush_panel "+
						"                where access_day = '"+accessday+"' "+        
						"            ) "+
						"     )a, tb_smart_recipient_panel b "+ 
						"where a.panel_id = b.panel_id "+
						"and   a.access_day = '"+accessday+"' "+
						"and   b.exp_time > to_date('"+accessday+"','yyyymmdd')  "+
						"and   b.ef_time  < to_date('"+accessday+"','yyyymmdd')+1 "+ 
						"and   a.panel_id in ( "+ 
						"	select panel_id  "+
						"	from tb_smart_doner_panel  "+
						"	where exp_time > to_date('"+accessday+"','yyyymmdd') "+ 
						"	and   ef_time  < to_date('"+accessday+"','yyyymmdd')+1 "+  
						")";
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		return eachPt;
	}	
	
	public boolean countTable(String period, String code, String table_name){
		ResultSet rs = null;
		int count = 0;
		String sql = "select count(*) cnt " +
				     "from " +table_name + " ";
		if(code.equalsIgnoreCase("d")){
			sql = sql+"where access_day = '"+period+"' ";
		} else {
			sql = sql+"where monthcode = '"+period+"' ";
		}
		try {
			this.pstmt = connection.prepareStatement(sql);
			rs = this.pstmt.executeQuery(sql);
			
			while (rs.next()) {	
				if(rs.getString("cnt") != null){
					count = rs.getInt("cnt");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(count > 0){
			return false;
		} else {
			return true;
		}
	}
	
	public boolean countNfactor(String period, String table_name){
		ResultSet rs = null;
		int count = 0;
		String sql = "select count(*) cnt " +
				     "from " +table_name + " " +
				     "where access_day = '"+period+"' " +
				     "and KC_N_FACTOR = 0  ";
		try {
			this.pstmt = connection.prepareStatement(sql);
			rs = this.pstmt.executeQuery(sql);
			
			while (rs.next()) {	
				if(rs.getString("cnt") != null){
					count = rs.getInt("cnt");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(count > 0){
			return true;
		} else {
			return false;
		}
	}
	
	public boolean countTableTask(String period){
		ResultSet rs = null;
		int count = 0;
		String sql = "select count(*) cnt " +
				     "from tb_smart_env_itrack "+
				     "where item_name = 'TASK' ";
		try {
			this.pstmt = connection.prepareStatement(sql);
			rs = this.pstmt.executeQuery(sql);
			
			while (rs.next()) {	
				if(rs.getString("cnt") != null){
					count = rs.getInt("cnt");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(count > 0){
			return false;
		} else {
			return true;
		}
	}

	public Calendar executeEnvDelete(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "";
		
		//N 버전 추가로 인한 백그라운드 뮤직 데이터처리 수정 --kwshin 20170501
				System.out.print("The batch - tb_smart_env_itrack delete is processing...");
				query = "delete from tb_smart_env_itrack where access_day = '"+accessday+"' and track_version > '6' and track_version not in ('6','7','8','9')";
				
				
		        this.pstmt = connection.prepareStatement(query);
				this.pstmt.executeUpdate();	
				
				System.out.println("Delete DONE.");
				
		return eachPt;
	}

	
	/**************************************************************************
	 *		메소드명		: executeAppDaytimeFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Daytime_Fact 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeAppDaytimeFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "";
		
		//N 버전 추가로 인한 백그라운드 뮤직 데이터처리 수정 --kwshin 20170501
				System.out.print("The batch - App Daytime Fact is processing...");
				query = "truncate table tb_smart_except_fg_app_list ";
				
				
		        this.pstmt = connection.prepareStatement(query);
				this.pstmt.executeUpdate();	
				
				System.out.println("TRUNCATE DONE.");
				if(this.pstmt!=null) this.pstmt.close();
				
				query = "insert into tb_smart_except_fg_app_list "+
						"select package_name "+
						"from tb_smart_app_info "+
						"where exp_time > sysdate "+
						"and ( (APP_CATEGORY_CD1 in ('C','L','A')) "+
						"or (APP_CATEGORY_CD2 in ('169','127','153','176','115','116','122','123'))) "+
						"union all "+
						"select 'com.android.chrome' from dual union all "+
						"select 'com.sec.android.app.sbrowser' from dual union all "+
						"select 'com.android.browser' from dual union all "+
						"select 'com.zum.android.swing' from dual union all "+
						"select 'org.mozilla.firefox' from dual union all "+
						"select 'com.cashslide' from dual union all "+
						"select 'com.skmc.okcashbag.home_google' from dual ";

		        this.pstmt = connection.prepareStatement(query);
				this.pstmt.executeUpdate();		
				
				System.out.println("INSERT tb_smart_except_fg_app_list DONE.");
				if(this.pstmt!=null) this.pstmt.close();
				
				query = "update tb_smart_env_itrack a set duration = 0 "+
						"where access_day = '"+accessday+"' "+
						"and flag = '3' "+
						"and duration > 0 "+
						"and track_version in ('20','21.2.0','21.2.1') "+
						"and exists (select 1 from tb_smart_except_fg_app_list b where a.fg_package_name = b.package_name) ";

		        this.pstmt = connection.prepareStatement(query);
				this.pstmt.executeUpdate();	
				
				System.out.println("UPDATE DONE.");
				if(this.pstmt!=null) this.pstmt.close();
		
		if(countTable(accessday, "d", "tb_smart_daytime_app_wgt")){
//			query =
//				"insert into tb_smart_daytime_app_wgt "+
//				"SELECT access_day, time_cd, smart_id, package_name, panel_id,  "+
//				"       count(distinct server_date)*180 duration, sysdate "+
//				"FROM ( "+
//				"    SELECT  /*+parallel(a,8)*/ "+
//				"            access_day, b.smart_id, a.package_name, panel_id, to_char(SERVER_DATE,'HH24') time_cd, "+
//				"            SERVER_DATE, sysdate "+
//				"    FROM    "+table_name+"  a, "+
//				"            (  "+
//				"                select a.PACKAGE_NAME, SMART_ID, b.package_name media "+
//				"                from (select package_name, smart_id  "+
//				"                      from  tb_smart_app_info "+
//				"                      where exp_time >= to_date('"+indate+"','yyyy/mm/dd') "+
//				"                      and   ef_time < to_date('"+indate+"','yyyy/mm/dd')) a, "+
//				"                     (select package_name "+
//				"                      from  tb_smart_app_media_list "+
//				"                      where exp_time >= to_date('"+indate+"','yyyy/mm/dd'))b "+
//				"                where   a.package_name = b.package_name(+) "+
//				"            ) b "+
//				"    WHERE   access_day = '"+accessday+"' "+
//				"    AND     a.PACKAGE_NAME is not null "+
//				"    AND     a.PACKAGE_NAME = b.PACKAGE_NAME "+
//				"    AND     ((media is null and screen = '1' and app_status = '100') or "+
//				"             (media is not null)) "+
//				"    and     TRACK_VERSION >= (select min(TRACK_VERSION) from TB_SMART_TRACK_VER WHERE exp_time >= to_date('20120305','yyyy/mm/dd')) "+
//				"    AND     RESULT_CD='Y' "+
//				") "+
//				"GROUP BY access_day, package_name, smart_id, panel_id, time_cd, sysdate ";
			query = "INSERT  INTO tb_smart_daytime_app_wgt "+
					"select access_day, time_cd, smart_id, package_name,  "+
					"       panel_id, sum(duration) duration, sysdate, sum(app_cnt) app_cnt "+
					"from " +
					"( "+
					"    select  /*+parallel(a,8) leading(b)*/  "+
					"            access_day, to_char(REGISTER_DATE,'HH24') time_cd, "+
					"            b.smart_id, a.ITEM_VALUE package_name, panel_id, duration, "+
					"            case when to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 and screen = '1' and flag in ('1','6') then 1 else 0 end app_cnt "+
					"    from    tb_smart_env_itrack a," +
					"    ( "+
					"        select PACKAGE_NAME, SMART_ID "+
					"        from tb_smart_app_info  "+
					"        where exp_time >= to_date('"+accessday+"','yyyymmdd') +1 "+
					"        and   ef_time  <  to_date('"+accessday+"','yyyymmdd') +1 "+
					"    ) b "+
					"    WHERE   access_day = '"+accessday+"' "+
					"    AND     a.ITEM_VALUE is not null "+
					"    AND     a.ITEM_VALUE = b.PACKAGE_NAME "+
//					"    AND     a.SUBJECT = 'APP' "+
					"    AND     a.SUBJECT in ('APP', 'MEDIA', 'CHANNEL') "+
					"    AND     duration > 0 "+
					"    AND     to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 "+
//					" "+
//					"    union all "+
//					" "+
//					"    select  /*+use_hash(b,a)*/  "+
//					"            access_day, to_char(SERVER_DATE,'HH24') time_cd, "+
//					"            b.smart_id, a.PACKAGE_NAME, panel_id, 180 "+
//					"    from    TB_SMART_TASK_ITRACK a, ( "+
//					"                select PACKAGE_NAME, SMART_ID "+
//					"                from  tb_smart_app_media_list  "+
//					"                where exp_time >= to_date('"+accessday+"','yyyymmdd') "+
//					"                and   ef_time < to_date('"+accessday+"','yyyymmdd') "+
//					"            ) b "+
//					"    WHERE   access_day = '"+accessday+"' "+
//					"    AND     a.PACKAGE_NAME is not null "+
//					"    AND     a.PACKAGE_NAME = b.PACKAGE_NAME "+
//					"    AND     TRACK_VERSION in ('3','4') "+
//					"    AND     RESULT_CD='Y' "+
//					"    AND     ( APP_STATUS = '130' or ( APP_STATUS = '100' and screen = 0) ) "+
					") "+
					"group by access_day, time_cd, smart_id, package_name, panel_id ";
		} else {
			System.out.print("App Daytime Fact already exists");
			System.exit(0);
		}
		//System.out.println(query);
		//System.exit(0);
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();

		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}

	
	//20131010 �빊遺쏙옙 - mskim
	public Calendar executeAppDayDivideDuration(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "";
		String TruncateQuery = "";
		
		System.out.print("The batch - App DivideDuration is processing...");
		
			TruncateQuery =	"truncate table tb_temp_env_itrack";
			this.pstmt = connection.prepareStatement(TruncateQuery);
			this.pstmt.executeUpdate();
			if(this.pstmt!=null) this.pstmt.close();
			System.out.print("truncate table tb_temp_env_itrack");
			
			TruncateQuery =	"truncate table tb_temp_env_itrack_new";
			this.pstmt = connection.prepareStatement(TruncateQuery);
			this.pstmt.executeUpdate();
			if(this.pstmt!=null) this.pstmt.close();
			System.out.print("truncate table tb_temp_env_itrack_new");
			
			TruncateQuery =	"truncate table tb_temp_env_itrack_diff";
			this.pstmt = connection.prepareStatement(TruncateQuery);
			this.pstmt.executeUpdate();
			if(this.pstmt!=null) this.pstmt.close();
			System.out.print("truncate table tb_temp_env_itrack_diff");
			
			TruncateQuery =	"truncate table tb_temp_env_itrack_day";
			this.pstmt = connection.prepareStatement(TruncateQuery);
			this.pstmt.executeUpdate();
			if(this.pstmt!=null) this.pstmt.close();
			System.out.print("truncate table tb_temp_env_itrack_day");
		
			//insert tb_temp_env_itrack
			System.out.print("The batch - insert tb_temp_env_itrack ");
			
			query = "insert into tb_temp_env_itrack " +
			"select /*+ leading(b) use_hash(b,a)*/ " +
			       "a.access_day, a.panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, wifistatus, time_gap, " +
			       "case when b.flag = 'Y' and a.duration > 1 then round(new_duration*(a.duration/b.duration)) else a.duration end duration, " +
			       "screen, a.flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, b.flag extreme_flag " +
			"from " +   
			"( " +
			    "select /*+parallel(a,8)*/access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, to_char(register_date,'hh24') time_cd, " +
			           "track_version, server_date, wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap " +
			   "from tb_smart_env_itrack a " +
			    "where access_day = '" + accessday + "' " +
			    "and item_value is not null " +
//			    "and subject = 'APP' " +
			    "and subject in ('APP', 'MEDIA', 'CHANNEL') " +
			    "and duration > 0 " +
			    "and to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 " +
			") a, " +
			"( " +
			    "select /*+ index(a,PK_SMART_DAYTIME_APP_NEW_WGT) */ access_day, time_cd, smart_id, package_name, panel_id, duration, new_duration, flag " +
			    "from tb_smart_daytime_app_new_wgt a " +
			    "where access_day = '" + accessday + "' " +
			") b " +
			"where  a.access_day=b.access_day " +
			"and    a.panel_id = b.panel_id " +
			"and    a.item_value = b.package_name " +
			"and    a.time_cd=b.time_cd";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		
		//insert tb_temp_env_itrack_new_1
		System.out.print("The batch - insert tb_temp_env_itrack_new_1 ");
		
		query = "insert into tb_temp_env_itrack_new " + 
				"select rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " + 
		        "wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time, 0 term " + 
		"from " + 
		"( " + 
		    "select /*+parallel(a,8)*/ " + 
		           "rowid rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " + 
		           "wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, " + 
		           "register_date start_time, register_date + duration/24/60/60 end_time, " + 
		           "to_char(register_date,'yyyymmddhh') start_day, to_char(register_date + duration/24/60/60,'yyyymmddhh') end_day " + 
		    "from tb_temp_env_itrack a " + 
		    "where access_day = '" + accessday + "' " +
		    "and item_value is not null " + 
//		    "and subject = 'APP' " +
		    "and subject in ('APP', 'MEDIA', 'CHANNEL') " + 
		    "and duration > 0 " + 
		    "and to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= (select min(to_number(TRACK_VERSION)) from tb_smart_track_ver where exp_time >= sysdate) " + 
		") " + 
		"where start_day = end_day";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		
		//insert tb_temp_env_itrack_diff
		System.out.print("The batch - insert tb_temp_env_itrack_diff ");
		
		query =	"insert into tb_temp_env_itrack_diff " +
		"select rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " +
		       "wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time " +
		"from " +
		"( " +
		    "select /*+parallel(a,8)*/ " +
		           "rowid rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " +
		           "wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, " +
		           "register_date start_time, register_date + duration/24/60/60 end_time, " +
		           "to_char(register_date,'yyyymmddhh') start_day, to_char(register_date + duration/24/60/60,'yyyymmddhh') end_day " +
		    "from tb_temp_env_itrack a " +
		    "where access_day = '" + accessday + "' " +
		    "and item_value is not null " +
//		    "and subject = 'APP' " +
		    "and subject in ('APP', 'MEDIA', 'CHANNEL') " +
		    "and duration > 0 " +
		    "and to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= (select min(to_number(TRACK_VERSION)) from tb_smart_track_ver where exp_time >= sysdate) " +
		") " +
		"where start_day <> end_day";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		
		//insert tb_temp_env_itrack_day
		//占쎌뵬揶쏉옙 �겫袁る막
		System.out.print("The batch - insert tb_temp_env_itrack_day ");
		query = "insert ALL " + 
		"when start_day = end_day then " + 
			"into tb_temp_env_itrack_day " + 
		        "values(rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " + 
		               "wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time) " + 
		"else " + 
			"into tb_temp_env_itrack_day " +  
		        "values(rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " + 
		               "wifistatus, time_gap, s_duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, start_zero_time) " + 
			"into tb_temp_env_itrack_day " +
		        "values(rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, end_zero_time, track_version, server_date, " + 
		               "wifistatus, time_gap, E_duration, screen, non_flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, end_zero_time, end_time) " + 
		"select  rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " +
		        "wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time, " +
		        "to_char(start_time,'yyyymmdd') start_day, to_char(end_time,'yyyymmdd') end_day, " +
		        "to_date(to_char(register_date,'yyyymmdd'),'yyyymmdd')+1 start_zero_time, " +
		        "to_date(to_char(end_time,'yyyymmdd')||' 00:00:00','yyyy/mm/dd hh24:mi:ss') end_zero_time, " +
		        "s_hour*60*60+s_minute*60+s_second S_duration, e_hour*60*60+e_minute*60+e_second E_duration, '5' non_flag " +
		"from " +
		"( " +
		    "select rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " +
		           "wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time, " +
		           "abs(TRUNC(MOD((to_date(to_char(end_time,'yyyymmdd')||' 00:00:00','yyyy/mm/dd hh24:mi:ss')-start_time),1)*24)) S_hour, " +
		           "abs(TRUNC(MOD((to_date(to_char(end_time,'yyyymmdd')||' 00:00:00','yyyy/mm/dd hh24:mi:ss')-start_time)*24,1)*60)) S_minute, " +
		           "abs(TRUNC(round(MOD((to_date(to_char(end_time,'yyyymmdd')||' 00:00:00','yyyy/mm/dd hh24:mi:ss')-start_time)*24*60,1)*60))) S_second, " +
		           "abs(TRUNC(MOD((to_date(to_char(end_time,'yyyymmdd')||' 00:00:00','yyyy/mm/dd hh24:mi:ss')-end_time),1)*24)) E_hour, " +
		           "abs(TRUNC(MOD((to_date(to_char(end_time,'yyyymmdd')||' 00:00:00','yyyy/mm/dd hh24:mi:ss')-end_time)*24,1)*60)) E_minute, " +
		           "abs(TRUNC(round(MOD((to_date(to_char(end_time,'yyyymmdd')||' 00:00:00','yyyy/mm/dd hh24:mi:ss')-end_time)*24*60,1)*60))) E_second " +
		    "from tb_temp_env_itrack_diff " +
		    "where access_day = '" + accessday + "' " +
		")";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		
		
		//insert tb_temp_env_itrack_new_2
		System.out.print("The batch - insert tb_temp_env_itrack_new_2 ");		
		query = "insert ALL " +
		"when term = 1 then " +
			"into tb_temp_env_itrack_new " +
		        "values(rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " +
		               "wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time, term) " +
		"else " +
			"into tb_temp_env_itrack_new " + 
		        "values(rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, start_time1, track_version, server_date, " +
		               "wifistatus, time_gap, duration1, screen, flag1, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time1, end_time1, term) " +
			"into tb_temp_env_itrack_new " + 
		        "values(rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, start_time2, track_version, server_date, " +
		               "wifistatus, time_gap, duration2, screen, flag2, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time2, end_time2, term) " +
		"select rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " +
		       "wifistatus, time_gap, screen, flag, flag1, flag2, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, term, " +
		       "shour_end1, ehour_end1, shour_end2, ehour_end2, duration, " +
		       "abs(TRUNC(MOD((shour_end1-start_time),1)*24))*60*60+abs(TRUNC(MOD((shour_end1-start_time)*24,1)*60))*60+abs(TRUNC(ROUND(MOD((shour_end1-start_time)*24*60,1)*60))) duration1, " +
		       "abs(TRUNC(MOD((end_time-ehour_end2),1)*24))*60*60+abs(TRUNC(MOD((end_time-ehour_end2)*24,1)*60))*60+abs(TRUNC(ROUND(MOD((end_time-ehour_end2)*24*60,1)*60))) duration2, " +
		       "start_time, " +
		       "register_date start_time1, " +
		       "ehour_end2 start_time2, " +
		       "end_time, " +
		       "register_date + (abs(TRUNC(MOD((shour_end1-start_time),1)*24))*60*60+abs(TRUNC(MOD((shour_end1-start_time)*24,1)*60))*60+abs(TRUNC(ROUND(MOD((shour_end1-start_time)*24*60,1)*60))))/24/60/60 end_time1, " +
		       "ehour_end2 + (abs(TRUNC(MOD((end_time-ehour_end2),1)*24))*60*60+abs(TRUNC(MOD((end_time-ehour_end2)*24,1)*60))*60+abs(TRUNC(ROUND(MOD((end_time-ehour_end2)*24*60,1)*60))))/24/60/60 end_time2 " +
		"from " +
		"( " +
		    "select rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " +
		           "wifistatus, time_gap, duration, screen, " + 
		           "case when access_day = to_char(register_date,'yyyymmdd') then flag else '5' end flag, flag flag1, '5' flag2, " +
		           "rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time, " +
		           "abs(TRUNC(MOD((to_date(to_char(end_time,'yyyymmddhh24'),'yyyymmddhh24')-(to_date(to_char(start_time,'yyyymmddhh24'),'yyyymmddhh24')-1/24/60/60)),1)*24))+1 term, " +
		           "to_date(to_char(start_time,'yyyymmdd')||to_char(start_time,'hh24')||'0000','yyyymmddhh24miss')+1/24 shour_end1, " +
		           "to_date(to_char(start_time,'yyyymmdd')||to_char(end_time,'hh24')||'0000','yyyymmddhh24miss') ehour_end1, " +
		           "to_date(to_char(start_time,'yyyymmdd')||to_char(start_time,'hh24')||'0000','yyyymmddhh24miss')+1/24 shour_end2, " +
		           "to_date(to_char(start_time,'yyyymmdd')||to_char(end_time,'hh24')||'0000','yyyymmddhh24miss') ehour_end2 " +
		    "from tb_temp_env_itrack_day " +
		    "where access_day = '" + accessday + "' " +
		")";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		
		//insert tb_temp_env_itrack_new_2
		System.out.print("The batch - insert tb_temp_env_itrack_new_3 ");	

		query = "insert into tb_temp_env_itrack_new " + 
		"select rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " + 
		       "wifistatus, time_gap, duration, screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, " + 
		       "register_date start_time, register_date + duration/24/60/60 end_time, term " + 
		"from " + 
		"( " + 
		    "select rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, " + 
		           "to_date(access_day||to_char(start_time+raw_rnk/24,'hh24')||'0000','yyyymmddhh24miss') register_date, track_version, server_date, " + 
		           "wifistatus, time_gap, " + 
		           "abs(TRUNC(MOD((to_date(access_day||to_char(start_time+raw_rnk/24,'hh24')+1||'0000','yyyymmddhh24miss') " + 
		                         "-to_date(access_day||to_char(start_time+raw_rnk/24,'hh24')||'0000','yyyymmddhh24miss')),1)*24))*60*60+ " + 
		           "abs(TRUNC(MOD((to_date(access_day||to_char(start_time+raw_rnk/24,'hh24')+1||'0000','yyyymmddhh24miss') " + 
		                         "-to_date(access_day||to_char(start_time+raw_rnk/24,'hh24')||'0000','yyyymmddhh24miss'))*24,1)*60))*60+ " + 
		           "abs(TRUNC(round(MOD((to_date(access_day||to_char(start_time+raw_rnk/24,'hh24')+1||'0000','yyyymmddhh24miss') " + 
		                         "-to_date(access_day||to_char(start_time+raw_rnk/24,'hh24')||'0000','yyyymmddhh24miss'))*24*60,1)*60))) duration, " + 
		           "screen, flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time, term " + 
		    "from " + 
		    "( " + 
		        "select rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " + 
		               "wifistatus, time_gap, duration, screen, '5' flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time, " + 
		               "a.term, row_number() over(partition by rid order by rid) raw_rnk " + 
		        "from " + 
		        "( " + 
		            "select rid, access_day, panel_id, panel_device_code, subject, item_name, item_value, register_date, track_version, server_date, " + 
		                   "wifistatus, time_gap, duration, screen, '5' flag, rxbyte, txbyte, battlevel, rxbyte_gap, txbyte_gap, start_time, end_time, " + 
		                   "abs(TRUNC(MOD((to_date(to_char(end_time,'yyyymmddhh24'),'yyyymmddhh24')-(to_date(to_char(start_time,'yyyymmddhh24'),'yyyymmddhh24')-1/24/60/60)),1)*24))+1 term " + 
		            "from tb_temp_env_itrack_day " + 
		            "where access_day = '" + accessday + "' " + 
		        ") a, " + 
		        "( " + 
		            "select term from tb_env_copy_timeraw where term >= 3 " + 
		        ") b " + 
		        "where a.term=b.term " + 
		    ") " + 
		") ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		

		//insert temp_mskim_daytime_app_wgt
		System.out.print("The batch - insert tb_smart_daytime_app_wgt ");

		query =	"delete from tb_smart_daytime_app_wgt where access_day = '"+ accessday + "'";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		

		query = "INSERT  INTO tb_smart_daytime_app_wgt " +
		"select access_day, time_cd, smart_id, package_name, " + 
		       "panel_id, sum(duration) duration, sysdate, sum(app_cnt) app_cnt " +
		"from " +
		"( " +
		    "select  /*+parallel(a,8) leading(b)*/ " + 
		            "access_day, " +
		            "case when access_day = to_char(register_date,'yyyymmdd') then to_char(REGISTER_DATE,'HH24') " +
		            "else '24' end time_cd, " +
		            "b.smart_id, a.ITEM_VALUE package_name, panel_id, duration, " +
		            "case when to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= 6 and screen = '1' and flag in ('1','6') then 1 else 0 end app_cnt " +
		    "from    tb_temp_env_itrack_new a, " +
		    "( " +
		        "select PACKAGE_NAME, SMART_ID " +
		        "from tb_smart_app_info  " +
		        "where exp_time >= to_date('"+accessday+"','yyyymmdd') +1 " +
		        "and   ef_time  <  to_date('"+accessday+"','yyyymmdd') +1" +
		    ") b " +
		    "WHERE   access_day = '"+ accessday + "' " +
		    "AND     a.ITEM_VALUE is not null " +
		    "AND     a.ITEM_VALUE = b.PACKAGE_NAME " +
//		    "AND	 a.SUBJECT = 'APP' " +
		    "AND     a.SUBJECT in ('APP', 'MEDIA', 'CHANNEL') " +
		    "AND     duration > 0 " +
		    "AND     to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= (select min(to_number(TRACK_VERSION)) from TB_SMART_TRACK_VER WHERE exp_time >= sysdate) " +
		") " +
		"group by access_day, time_cd, smart_id, package_name, panel_id ";

        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		//음악앱 Duration 3600초가 넘는 데이터는 3600 으로 업데이트 -- by kwshin 20160902 
		query = "update tb_smart_daytime_app_wgt set duration = 3600 "+
				"WHERE   access_day = '"+ accessday + "' "+
		        "AND duration > 3600 ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	/**************************************************************************
	 * 		생성 날짜 		: 2014.05.02
	 *		메소드명		: executePushPanelProccess
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: PUSH 패널 (일간 app_cnt=0) 중에 00:01, 00:20분의 강제로그를 보내는 패널을 제외하는 로직 
	 * @return 
	 *************************************************************************/	
	
	
	public Calendar executePushPanelProccess(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "";
		
		System.out.print("The batch - App PushPanelProccess is processing...");	

		query = "insert into tb_smart_day_nopush_panel "+
				"select /*+use_hash(b,a) leading(b) */ access_day, to_char(register_Date,'hh24') time_cd, smart_id, package_name, panel_id, sum(duration) duration, sysdate  "+
				"from tb_temp_env_itrack_new a, "+
				"( "+
				"    select PACKAGE_NAME, SMART_ID "+
				"    from tb_smart_app_info "+
				"    where exp_time >= to_date('"+accessday+"','yyyymmdd') +1 "+
				"    and   ef_time  <  to_date('"+accessday+"','yyyymmdd') +1 "+
				 "   and   smart_id not in ( "+
				 "                           select smart_id "+
				 "                           from tb_smart_app_media_list "+
				 "                           where exp_time >= to_date('"+accessday+"','yyyymmdd') +1 "+
				 "                           and   ef_time  <  to_date('"+accessday+"','yyyymmdd') +1 "+
				 "                         ) "+
				") b "+
				"where rid in ( "+
				"                select /*+parallel(a,8)*/ rid "+
				"                from tb_temp_env_itrack_new a "+
				"                WHERE access_day = '"+accessday+"' "+
				"                and to_char(register_Date,'hh24:mi') in ('00:01','00:20') "+
				"                and flag = '0' "+
				"                and duration > 0 "+
				"             ) "+
				"AND a.ITEM_VALUE = b.PACKAGE_NAME "+
//				"AND a.SUBJECT = 'APP' "+
				"AND a.SUBJECT in ('APP', 'MEDIA', 'CHANNEL') "+
				"AND to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= (select min(to_number(TRACK_VERSION)) from TB_SMART_TRACK_VER WHERE exp_time >= sysdate) "+
				"group by access_day, to_char(register_Date,'hh24'), smart_id,  package_name, panel_id ";
	
	    this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();

		System.out.println("INSERTING tb_smart_day_nopush_panel  DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		query = "insert into tb_smart_day_push_panel "+
				"select /*+ index(a,PK_SMART_daytime_app_wgt) */ access_day, smart_id, package_name, panel_id, " +
				" sum(duration) duration, sysdate, sum(app_cnt) app_cnt "+
				"from tb_smart_daytime_app_wgt a "+
				"where access_day = '"+accessday+"' "+
				"   and   smart_id not in ( "+
				"                           select smart_id "+
				"                           from tb_smart_app_media_list "+
				"                           where exp_time >= to_date('"+accessday+"','yyyymmdd') +1 "+
				"                           and   ef_time  <  to_date('"+accessday+"','yyyymmdd') +1 "+
				"                         ) "+				
				"group by access_day, panel_id, smart_id, package_name "+
				"having sum(app_cnt) = 0 ";
		
	    this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();

		System.out.println("INSERTING tb_smart_day_push_panel  DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		query = "delete /*+ index(a,pk_smart_daytime_app_wgt) */ " +
				"from tb_smart_daytime_app_wgt a " +
				"where access_day = '"+accessday+"' " +
				"and (panel_id, smart_id) in " +
				"( " +
				"    select panel_id, smart_id " +
				"    from tb_smart_day_push_panel " +
				"    where access_day = '"+accessday+"' " +
				"    minus " +
				"    select panel_id, smart_id " +
				"    from tb_smart_day_nopush_panel " +
				"    where access_day = '"+accessday+"' " +
				")";
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("Delete tb_smart_daytime_app_wgt  DONE.");
		if(this.pstmt!=null) this.pstmt.close();		
		
//		query =	"insert into tb_smart_daytime_app_wgt "+
//				"select /*+use_hash(b,a) index(a,PK_SMART_DAY_PUSH_PANEL) index(b,PK_SMART_DAY_NOPUSH_PANEL)*/ "+
//				"    a.access_day, b.time_cd, a.smart_id, a.package_name, a.panel_id, b.duration, sysdate, app_cnt "+
//				"from tb_smart_day_push_panel a, TB_SMART_day_nopush_panel b "+
//				"where a.access_day = '"+accessday+"' "+
//				"and a.access_day = b.access_day "+
//				"and a.smart_id = b.smart_id "+
//				"and a.panel_id = b.panel_id";
		
//		this.pstmt = connection.prepareStatement(query);
//		this.pstmt.executeUpdate();
		
//		System.out.println("Inserting tb_smart_daytime_app_wgt  DONE.");
//		if(this.pstmt!=null) this.pstmt.close();	
		
		System.out.println("The batch - App PushPanelProccess is Done...");	
		return eachPt;
		
		
	}
	
	/**************************************************************************
	 *		메소드명		: executeEqualAppDaytime
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 동일앱 Insert 배치문 (INSERT)
	 * @return 
	 *************************************************************************/

	public Calendar executeEqualAppDaytime(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "";
		

		System.out.println("Insert Equal_App is Inserting....");
		if(this.pstmt!=null) this.pstmt.close();
		
		query = "insert into tb_smart_daytime_app_wgt "+ 
				"select /*+index(a,pk_smart_daytime_app_fact) use_hash(b,a)*/ "+ 
				"    access_Day, time_cd, p_smart_id smart_id, 'kclick_equal_app' package_name, panel_id, sum(duration) duration, sysdate proc_date, sum(app_cnt) app_cnt "+ 
				"from tb_smart_daytime_app_wgt a, tb_smart_app_info b "+ 
				"where access_day = '"+accessday+"' "+ 
				"and   exp_time >= to_date('"+accessday+"','yyyymmdd') +1 "+ 
				"and   ef_time  <  to_date('"+accessday+"','yyyymmdd') +1 "+ 
				"and   p_smart_id is not null "+ 
				"and   a.smart_id = b.smart_id "+ 
				"group by access_Day, time_cd, p_smart_id, panel_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
		System.out.println("Insert Equal_App Insert Done.");
		if(this.pstmt!=null) this.pstmt.close();		

		return eachPt;
	}
	
	
	/**************************************************************************
	 *		메소드명		: executeAppFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Day_Fact 배치문 (INSERT)
	 * @return 
	 *************************************************************************/
	
	public Calendar executeAppFact(String code, String period) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String query = "";
		if(code.equalsIgnoreCase("d")&&countTable(period,"d","tb_smart_day_app_wgt")){
			query = "INSERT   INTO tb_smart_day_app_wgt "+
					"         (access_day, smart_id, package_name, panel_id, duration, proc_date, app_cnt) "+
					"SELECT   /*+index(a,pk_smart_daytime_app_wgt)*/ "+
					"		  access_day, smart_id, package_name, panel_id, sum(duration), sysdate, sum(app_cnt) app_cnt "+
					"FROM     tb_smart_daytime_app_wgt a "+
					"WHERE    access_day = '"+period+"' "+
					"AND      a.PACKAGE_NAME is not null "+
					"GROUP BY access_day, smart_id, package_name, panel_id ";
			
			System.out.print("The batch - App Day Fact is processing...");
		} else if (code.equalsIgnoreCase("m")&&countTable(period,"m","tb_smart_month_app_wgt")){
			query = "INSERT   INTO tb_smart_month_app_wgt "+
					"         ( monthcode, smart_id, package_name, panel_id, duration, daily_freq_cnt, proc_date, app_cnt)     "+
					"SELECT   substr(access_day,1,6) monthcode, smart_id, package_name, panel_id, sum(duration) duration, "+
					"         count(distinct access_day), sysdate,  sum(app_cnt) app_cnt "+
					"FROM     tb_smart_day_app_wgt a "+
					"WHERE    access_day >= '"+period+"01' "+
					"AND      access_day <= fn_month_lastday('"+period+"') "+
					"GROUP BY substr(access_day,1,6), smart_id, package_name, panel_id ";
			
			System.out.print("The batch - Month Fact is processing...");
		} else {
			System.out.print("App Fact already exists");
			System.exit(0);
		}
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	
	
	public Calendar executeWifi(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daily App networtk_wgt is processing...");
		
		//20140122 rxbyte_gap, txbyte_gap �빊遺쏙옙 筌뤴뫗�읅占쎌몵嚥∽옙 雅뚯눘苑� 筌ｌ꼶�봺 占쎄퉱 �뜎�눖�봺 占쎌읅占쎌뒠  MSKIM
//		String query = 
//					"insert into tb_smart_day_wifi_wgt "+
//	                "select access_day, smart_id, package_name, panel_id, wifistatus, sum(duration) duration, sysdate proc_date "+
//					"from "+
//					"( "+
//					"    select  /*+parallel(a,8)*/  "+
//					"            access_day, b.smart_id, a.ITEM_VALUE package_name, panel_id, "+
//					            /*0:bad, 1:LTE, 2:3G, 3:WIFI, 4:wimax, 5:none*/
//					"            case when wifistatus = 'mobile:13' then 1 "+
//					"                 when wifistatus <> 'mobile:13' and wifistatus like 'mobile:%' then 2 "+
//					"                 when wifistatus like 'wifi%' then 3 "+
//					"                 when wifistatus like 'wimax%' then 4 "+
//					"                 when wifistatus = 'mobile' then 5 "+
//					"                 when wifistatus like 'none%' then 5 "+                 
//					"            else 0 end wifistatus, duration "+
//					"    from    tb_smart_env_itrack a, "+
//					"    ( "+
//					"        select PACKAGE_NAME, SMART_ID "+
//					"        from tb_smart_app_info "+
//					"        where exp_time >= to_date('"+accessday+"','yyyymmdd') "+
//					"        and   ef_time  <  to_date('"+accessday+"','yyyymmdd') "+
//					"    ) b "+
//					"    WHERE   access_day = '"+accessday+"' "+
//					"    AND     a.ITEM_VALUE is not null "+
//					"    AND     a.ITEM_VALUE = b.PACKAGE_NAME "+
//					"    AND     a.SUBJECT = 'APP' "+
//					"    AND     duration > 0 "+
//					"    AND     TRACK_VERSION >= (select min(to_number(TRACK_VERSION)) from TB_SMART_TRACK_VER WHERE exp_time >= sysdate) "+
//					") "+
//					"group by access_day, smart_id, package_name, panel_id, wifistatus ";
		
		//20140122 rxbyte_gap, txbyte_gap �빊遺쏙옙 筌뤴뫗�읅占쎌몵嚥∽옙 占쎄퉱 �뜎�눖�봺 占쎌읅占쎌뒠 MSKIM
		String query = 
					"insert into tb_smart_day_wifi_wgt "+
	                "select access_day, smart_id, package_name, panel_id, wifistatus, sum(duration) duration, sysdate proc_date, "+
	                "		sum(rxbyte_gap) rxbyte_gap, sum(txbyte_gap) txbyte_gap "+
					"from "+
					"( "+
					"    select  /*+parallel(a,8)*/  "+
					"            access_day, b.smart_id, a.ITEM_VALUE package_name, panel_id, "+
					            /*0:bad, 1:LTE, 2:3G, 3:WIFI, 4:wimax, 5:none*/
					"            case when wifistatus = 'mobile:13' then 1 "+
					"                 when wifistatus <> 'mobile:13' and wifistatus like 'mobile:%' then 2 "+
					"                 when wifistatus like 'wifi%' then 3 "+
					"                 when wifistatus like 'wimax%' then 4 "+
					"                 when wifistatus = 'mobile' then 5 "+
					"                 when wifistatus like 'none%' then 5 "+                 
					"            else 0 end wifistatus, duration, rxbyte_gap, txbyte_gap "+
					"    from    tb_smart_env_itrack a, "+
					"    ( "+
					"        select PACKAGE_NAME, SMART_ID "+
					"        from tb_smart_app_info "+
					"        where exp_time >= to_date('"+accessday+"','yyyymmdd') "+
					"        and   ef_time  <  to_date('"+accessday+"','yyyymmdd') "+
					"    ) b "+
					"    WHERE   access_day = '"+accessday+"' "+
					"    AND     a.ITEM_VALUE is not null "+
					"    AND     a.ITEM_VALUE = b.PACKAGE_NAME "+
//					"	 AND 	 a.SUBJECT = 'APP' "+
					"    AND     a.SUBJECT in ('APP', 'MEDIA', 'CHANNEL') "+
					"    AND     duration > 0 "+
					"    AND     to_number(substr(TRACK_VERSION,0,decode(instr(TRACK_VERSION,'.',-1), 0 , length(TRACK_VERSION), instr(TRACK_VERSION,'.',-1)-1))) >= (select min(to_number(TRACK_VERSION)) from TB_SMART_TRACK_VER WHERE exp_time >= sysdate) "+
					") "+
					"group by access_day, smart_id, package_name, panel_id, wifistatus ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		
		//DA.updateAppExtreme();占쎈퓠占쎄퐣 占쎌뵠占쎈짗 域밸갭�뼊揶쏉옙 筌ｌ꼶�봺
		query = 
					 "insert into TB_SMART_DAY_WIFI_NEW_WGT "+	
			         "select a.ACCESS_DAY, WIFISTATUS, a.SMART_ID, a.PACKAGE_NAME, a.PANEL_ID, a.DURATION, "+ 
			         "       case when flag = 'Y' and a.duration > 1 then round(NEW_DURATION*(a.DURATION/b.DURATION)) "+ 
			         "       else a.duration end as new_duration, "+
			         "       a.PROC_DATE, "+
			         "       FLAG, "+
			         "       RXBYTE_GAP, TXBYTE_GAP "+
			         "from   tb_smart_day_wifi_wgt a, tb_smart_day_app_new_wgt b "+
			         "where  a.access_day = '"+accessday+"' "+
			         "and    a.panel_id = b.panel_id "+
			         "and    a.smart_id = b.smart_id ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		
		
		//delete previous daytime wgt, pure down new daytime wgt
		query = "delete tb_smart_day_wifi_wgt where access_day = '"+accessday+"' ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		
		query = "insert into tb_smart_day_wifi_wgt "+
				"select ACCESS_DAY, SMART_ID, PACKAGE_NAME, PANEL_ID, WIFISTATUS, NEW_DURATION, PROC_DATE, RXBYTE_GAP, TXBYTE_GAP "+
				"from  TB_SMART_DAY_WIFI_NEW_WGT "+
				"where access_day = '"+accessday+"' ";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		//push 占쎈솭占쎄섯 占쎄텣占쎌젫 �빊遺쏙옙 - 20140502 KWSHIN
		query = "delete /*+ index(a,pk_smart_day_wifi_wgt) */ from tb_smart_day_wifi_wgt a " +
				"where access_day = '"+accessday+"' " +
				"and (panel_id, smart_id) in " +
				"( " +
				"    select panel_id, smart_id " +
				"    from tb_smart_day_push_panel " +
				"    where access_day = '"+accessday+"' " +
				"    minus " +
				"    select panel_id, smart_id " +
				"    from tb_smart_day_nopush_panel " +
				"    where access_day = '"+accessday+"' " +
				")";
		
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		System.out.println("WIFI INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();

		return eachPt;
	}
	
	public Calendar executeAppTOTFact(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Daily App Total Fact is processing...");
		String query = 
				"insert into tb_smart_day_tot_fact "+
				"select access_day, "+
				"       a.panel_id, "+
				"       sum(duration), "+
				"       sysdate, "+
				"       decode(b.panel_id,null,'R','D') "+
				"from   tb_smart_day_app_fact a, tb_smart_doner_panel b "+
				"where  access_day = '"+accessday+"' "+
				"and exp_time(+) > to_date('"+accessday+"', 'yyyymmdd') "+
				"and    ef_time(+) < to_date('"+accessday+"', 'yyyymmdd')+1 "+
				"and a.panel_id = b.panel_id(+) "+
				"group by access_day, a.panel_id , b.panel_id ";
		
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		
//		System.out.println(query);
//		System.exit(0);
		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();

		return eachPt;
	}
	
	public boolean donerTest(String accessday) throws SQLException {
		ResultSet rs = null;
		double count = 0;
		String sql = 
				"select round(doner_cnt*100/doner,2) doner_per "+
				"from ( "+
				"    select count(*) doner_cnt "+
				"    from   tb_smart_day_tot_fact "+
				"    where  access_day = '"+accessday+"' "+
				"    and    panel_flag = 'D' "+
				") a,( "+
				"    select count(*) doner "+
				"    from   tb_smart_doner_panel "+
				"    where  exp_time > sysdate "+
				"    and    ef_time  < sysdate "+
				"    and    panel_flag = 'Y' "+
				") b ";
		try {
			this.pstmt = connection.prepareStatement(sql);
			rs = this.pstmt.executeQuery(sql);
			
			while (rs.next()) {	
				if(rs.getString("doner_per") != null){
					count = rs.getDouble("doner_per");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Doner Percentage is: "+count+"%.");
		if(count < 90) {
			return true;
		} else {
			return false;
		}
	}
	
	/**************************************************************************
	 *		메소드명		: executeKeywordFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 월간 Keyword Fact 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeKeywordFact(String mode, String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		
		String temptablename = "";
		String tablename = "";
		if(mode.equalsIgnoreCase("w")){
			temptablename = "TB_SMART_TEMP_SECTION_WGT";
			tablename = "TB_SMART_MONTH_KEYWORD_WGT";
		} else if(mode.equalsIgnoreCase("v")) {
			temptablename = "TB_SMART_MONTH_TEMP_SECTION";
			tablename = "TB_SMART_MONTH_KEYWORD_FACT";
		} else {
			System.out.println("Error");
			System.exit(0);
		}
		
		String query = "";
		if(countTable(monthcode,"m",tablename)){
			query = 
					"INSERT INTO "+tablename+" " +  
					"SELECT monthcode, site_id, section_id, lower(query_decode), panel_id, count(*), sum(query_cnt) " +
					"FROM " +
					"( " +
					"    select '"+monthcode+"' monthcode, site_id, section_id, query_decode, panel_id, rid, case when sum(query_cnt) > 0 then 1 else 0 end query_cnt " +
					"    from " +
					"        ( " +
					"        select rid,  site_id, section_id, query_decode, panel_id, " +
					"           case when query_decode1||query_decode2 in ('MM','MT','TM','TT','TF','FT') then 1 else 0 end query_cnt, path_rank " +
					"        from " +
					"        ( " +
					"            select rid , a.site_id, b.section_id, query_decode, panel_id, domain_url, page, parameter, " +
					"                   case when add_page1 is null then 'M' " +
					"                        when instr(parameter, add_page1) = 1 then 'T' " +
					"                   else case when instr(parameter, add_page1) <> 0 and instr(parameter, '&'||add_page1) > 1 then 'T' " +
					"                        else case when (add_page2 is not null and instr(parameter,add_page2) <> 0 and " +
					"                                           (instr(parameter,add_page2) > 0 or instr(parameter,'&'||add_page2) > 0)) or " +
					"                                       (add_page3 is not null and (instr(parameter,add_page3) > 0 or instr(parameter,'&'||add_page3) > 0)) or " +
					"                                       (add_page4 is not null and (instr(parameter,add_page4) > 0 or instr(parameter,'&'||add_page4) > 0)) or " +
					"                                       (add_page5 is not null and (instr(parameter,add_page5) > 0 or instr(parameter,'&'||add_page5) > 0)) then 'T' " +
					"                             else 'F' " +
					"                             end " +
					"                        end " +
					"                   end query_decode1, " +
					"                   case when instr(parameter, nvl(del_page1,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page1,'pgheo')) > 0 then 'F' " +
					"                        when instr(parameter, nvl(del_page1,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page1,'pgheo')) > 0 then 'F' " +
					"                        when (instr(parameter, nvl(del_page1,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page1,'pgheo')) = 0) or " +
					"                             (instr(parameter ,nvl(del_page1,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page1,'pgheo')) = 0) " +
					"                        then case when instr(parameter, nvl(del_page2,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page2,'pgheo')) > 0 then 'F' " +
					"                                  when instr(parameter, nvl(del_page2,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page2,'pgheo')) > 0 then 'F' " +
					"                                  when (instr(parameter, nvl(del_page2,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page2,'pgheo')) = 0) or " +
					"                                       (instr(parameter, nvl(del_page2,'pgheo')) > 0 and instr(parameter, '&'||nvl(del_page2,'pgheo')) = 0) " +
					"                                  then case when instr(parameter, nvl(del_page3,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page3,'pgheo')) > 0 then 'F' " +
					"                                            when instr(parameter, nvl(del_page3,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page3,'pgheo')) > 0 then 'F' " +
					"                                            when (instr(parameter, nvl(del_page3,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page3,'pgheo')) = 0) or " +
					"                                                 (instr(parameter, nvl(del_page3,'pgheo')) > 0 and instr(parameter, '&'||nvl(del_page3,'pgheo')) = 0) " +
					"                                            then case when instr(parameter, nvl(del_page4,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page4,'pgheo')) > 0 then 'F' " +
					"                                                      when instr(parameter, nvl(del_page4,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page4,'pgheo')) > 0 then 'F' " +
					"                                                      when (instr(parameter, nvl(del_page4,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page4,'pgheo')) = 0) or " +
					"                                                           (instr(parameter, nvl(del_page4,'pgheo')) > 0 and instr(parameter, '&'||nvl(del_page4,'pgheo')) = 0) " +
					"                                                      then case when instr(parameter, nvl(del_page5,'pgheo')) = 1 or instr(parameter, '&'||nvl(del_page5,'pgheo')) > 0 then 'F' " +
					"                                                                when instr(parameter, nvl(del_page5,'pgheo')) > 1 and instr(parameter, '&'||nvl(del_page5,'pgheo')) > 0 then 'F' " +
					"                                                                when (instr(parameter, nvl(del_page5,'pgheo')) = 0 and instr(parameter, '&'||nvl(del_page5,'pgheo')) = 0) or " +
					"                                                                     (instr(parameter, nvl(del_page5,'pgheo')) > 0 and instr(parameter, '&'||nvl(del_page5,'pgheo')) = 0) then 'T' " +
					"                                                           end " +
					"                                                 end " +
					"                                       end " +
					"                             end " +
					"                   else 'T' " +
					"                   end query_decode2, " +
					"                   rank() over (partition by a.domain_url||decode(page, null, null, '/'||page)||decode(parameter, null, null, '?'||parameter) order by lengthb(b.path_url) desc ) path_rank " +
					"            from " +
					"            ( " +
					"                select /*+parallel(a,8)*/ " +
					"                       rowid rid, access_day, site_id, section_id, query_decode, panel_id, domain_url, page, parameter org_parameter, " +
					"                       case when site_id in (43339,5033) and instr(page,'#') > 0 " +
					"                                then replace(substr(page,instr(page,'#')+1)||parameter,'#','&') " +
					"                            when site_id in (43339,5033) " +
					"                                then replace(parameter,'#','&') " +
					"                            when site_id = 178 and domain_url like 'http://%dic.naver.com' " +
					"                                and instr(decode(substr(page,length(page),1),'/',substr(page,1,length(page)-1),page),'/',1,3) > 0 " +
					"                                then substr(page,instr(page,'/',1,2))||'&'||parameter " +
                    "                  		     when site_id = 1173 and domain_url = 'https://m.search.daum.net' and page ='search' "+
                    "                         		 then replace(parameter,'#','&') "+
					"                       else parameter end parameter " +
					"                from   "+temptablename+" a " +
					"                where  access_day like'"+monthcode+"%' " +
					"                and    query_decode is not null " +
					"                and    query_decode <> '=' " +
					"                and    pv_cnt > 0 " +
					"            ) a, " +
					"            ( " +
					"                SELECT /*+use_hash(b,a)*/ " +
					"                       b.site_id, b.section_id, b.path_url, b.add_page1, b.add_page2, b.add_page3, b.add_page4, b.add_page5,  b.del_page1, b.del_page2, b.del_page3, b.del_page4, b.del_page5 " +
					"                FROM   tb_section_info b " +
					"                WHERE  b.exp_time > sysdate " +
					"                AND    section_id in (select code from tb_codebook a where meta_code='SECTION' and (code_etc='2' or code=2)) " +
					"                AND    b.query is not null " +
					"            ) b " +
					"            where a.site_id = b.site_id " +
					"            and   a.section_id = b.section_id " +
					"            and   a.domain_url||decode(page||org_parameter, null, null, '/'||page)||decode(org_parameter, null, null, '?'||org_parameter) like b.path_url||'%' " +
					"        ) " +
					"    ) " +
					"    WHERE path_rank = 1 " +
					"    group by site_id, section_id, query_decode, panel_id, rid " +
					") " +
					"GROUP BY monthcode, site_id, section_id, lower(query_decode), panel_id " ;
		} else {
			System.out.println("Keyword Fact already exists.");
			System.exit(0);
		}
//		System.out.println(query);
//		System.exit(0);
		System.out.print("The batch - Keyword Fact is processing...");
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();

		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeKeywordFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 월간 Keyword Fact 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeQueryFact(String mode, String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		String tablename = "";
		String keytablename = "";
		if(mode.equalsIgnoreCase("w")){
			tablename = "TB_SMART_MONTH_QUERY_WGT";
			keytablename = "TB_SMART_MONTH_KEYWORD_WGT";
		} else if(mode.equalsIgnoreCase("v")) {
			tablename = "TB_SMART_MONTH_QUERY_FACT";
			keytablename = "TB_SMART_MONTH_KEYWORD_FACT";
		} else {
			System.out.println("Error");
			System.exit(0);
		}
		
		String query = "";
		if(countTable(monthcode,"m",tablename)){
			query = "insert into "+tablename+" "+
						"select MONTHCODE, SITE_ID, SECTION_ID, PANEL_ID , sum(QUERY_CNT) QUERY_CNT "+
						"from "+keytablename+" "+
						"where  monthcode = '"+monthcode+"' "+
						"and    panel_id in ( " +
						"	select panel_id " +
						"	from   tb_smart_month_panel_seg " +
						"	where  monthcode = '"+monthcode+"' " +
						") "+
						"group by MONTHCODE, SITE_ID, SECTION_ID, PANEL_ID "+
						"having sum(QUERY_CNT) > 0 ";
		} else {
			System.out.println("Query Fact already exists.");
			System.exit(0);
		}
//		System.out.println(query);
//		System.exit(0);
		System.out.print("The batch - Query Fact is processing...");
        this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();

		System.out.println("INSERTION DONE.");
		if(this.pstmt!=null) this.pstmt.close();
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeSectionPath
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 월간 Section Path 배치문 (DELETE & INSERT)
	 *************************************************************************/
	
	public Calendar executeSectionPath(String code, String period) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Section Path is processing...");
		
		//占쏙옙甕곕뜄�뼎 Path占쎄텣占쎌젫
		String queryD = "Delete TB_SMART_SECTION_PATH";
		this.pstmt = connection.prepareStatement(queryD);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		//占쎌뵠甕곕뜄�뼎 Path占쎄땜占쎌뿯
		String query2 = "INSERT INTO TB_SMART_SECTION_PATH  "+
						"       ( site_id, psection_id, section_id, path_url, type_cd, path_id) "+
						"SELECT a.site_id, b.psection_id, a.section_id, a.domain_url path_url, 'D' type_cd, 0 path_id "+
						"from   tb_domain_info a, vi_section_code b "+
						"where  ( a.site_id, a.section_id ) in ( select site_id, section_id from tb_section_site_new where type_cd = 'D') "+
						"and    a.section_id = b.section_id "+
						"and    a.site_id in ( select site_id from tb_smart_section_portal_info where EXP_TIME > sysdate ) "+
						"and    a.ef_time < sysdate "+
						"and    a.EXP_TIME > sysdate "+
						"and    b.section_id != 17 " +
						"and    a.section_id != 99 "+
						"union all "+
						"select a.site_id, b.psection_id, a.section_id, a.path_url, 'P', path_id "+
						"from   tb_section_info a, vi_section_code b "+
						"where  ( a.site_id, a.section_id ) "+
						"       in ( select site_id, section_id from tb_section_site_new where type_cd = 'P') "+
						"and    a.section_id = b.section_id "+
						"and    a.site_id in ( select site_id from tb_smart_section_portal_info where EXP_TIME > sysdate ) "+
						"and    a.ef_time < sysdate "+
						"and    a.EXP_TIME > sysdate "+
						"and    b.section_id != 17 "+
						"union all "+
						"select a.site_id, b.psection_id, a.section_id, a.path_url, 'Q', path_id "+
						"from   tb_section_info a, vi_section_code b "+
						"where  ( a.site_id, a.section_id ) "+
						"       in ( select site_id, section_id from tb_section_site_new where type_cd = 'Q') "+
						"and    a.section_id = b.section_id "+
						"and    a.site_id in ( select site_id from tb_smart_section_portal_info where EXP_TIME > sysdate ) "+
						"and    a.ef_time < sysdate "+
						"and    a.EXP_TIME > sysdate "+
						"and    b.section_id != 17 ";
		//System.out.println(query2);
		//System.exit(0);
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query3 = "";
		if(code.equalsIgnoreCase("m")) {
			query3 = 
				"INSERT INTO TB_SMART_SECTION_PATH_URL "+
				"       (monthcode, site_id, psection_id, section_id, path_url, type_cd, path_id, proc_date ) "+
				"SELECT '"+period+"', site_id, psection_id, section_id, path_url, type_cd, path_id, sysdate "+
				"FROM   TB_SMART_SECTION_PATH ";
		} else {
			query3 = 
				"INSERT INTO TB_SMART_SECTION_PATH_URL "+
				"       (monthcode, site_id, psection_id, section_id, path_url, type_cd, proc_date, path_id, weekcode ) "+
				"SELECT '', site_id, psection_id, section_id, path_url, type_cd, sysdate, path_id, fn_weekcode('"+period+"') "+
				"FROM   TB_SMART_SECTION_PATH ";
		}
        this.pstmt = connection.prepareStatement(query3);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();

		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeWeekSectionTemp
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 주간 Section Temp 배치문 (TRUNCATE & INSERT)
	 *************************************************************************/
	
	public Calendar executeWeekSectionTemp(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Weekly Section Temp is processing...");
		
		String query2 = 
				"INSERT INTO tb_smart_week_temp_section c "+
				"SELECT * "+
				"FROM ( "+
				"    SELECT   /*+parallel(a,4) use_hash(b,a)*/ "+
				"             access_day, "+
				"             panel_id,  "+
				"             b.psection_id psection_id, "+
				"             b.section_id section_id, "+
				"             b.psection_id visit_psection_id, "+
				"		      b.section_id visit_section_id, "+
				"             1 pv_cnt, "+
				"             duration, "+
				"             req_date server_date, "+
				"             a.req_site_id site_id, "+
				"             req_domain domain_url,  "+
				"             req_page page,  "+
				"             req_parameter parameter, "+
				"             REQ_QUERY_DECODE QUERY_DECODE, "+
				"             sysdate, "+
				"			  path_id "+	
				"    FROM    tb_smart_browser_itrack a,  "+
				"             ( "+
//				"             select SITE_ID, PSECTION_ID, SECTION_ID, PATH_URL, TYPE_CD, PATH_ID "+
//				"             from tb_smart_section_path "+
//				"             where type_cd='D' "+
				"             select /*+index(a,IDX_SMART_SECTION_PATH_URL2)*/SITE_ID, PSECTION_ID, SECTION_ID, PATH_URL, TYPE_CD, PATH_ID "+
				"             from tb_smart_section_path_url a"+
				"             where type_cd='D' " +
				"			  and   weekcode = fn_weekcode('"+accessday+"') "+
				"             ) b "+
				"    WHERE   b.site_id>0  "+
				"    AND     a.result_cd = 'S' " +
				"	 AND     a.panel_flag in ('D','V') "+
				"    AND     a.req_site_id=b.site_id "+
				"    AND     a.req_domain=b.path_url "+
				"    AND     A.access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"    AND     A.access_day <= '"+accessday+"' "+
				") "+
				"WHERE PSECTION_ID IS NOT NULL "+
				"AND   SECTION_ID IS NOT NULL ";
		//System.out.println(query2);
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query3 = 
				"INSERT INTO tb_smart_week_temp_section c "+
				"SELECT access_day, panel_id, psection_id, section_id, psection_id visit_psection_id, section_id visit_section_id, "+
				"	   1 pv_cnt, duration, server_date, site_id, domain_url, page,  parameter,  query_decode, sysdate, path_id "+
				"FROM ( "+
				"    SELECT   /*+parallel(a,4) use_hash(b,a)*/ "+
				"             access_day, "+
				"             panel_id,  "+
				"             case when rank() over ( partition by req_domain||decode(req_page, NULL, NULL, '/'||req_page) "+
				"             order by lengthb(path_url) desc ) = 1 then b.psection_id end psection_id, "+
				"             case when rank() over ( partition by req_domain||decode(req_page, NULL, NULL, '/'||req_page) "+
				"             order by lengthb(path_url) desc ) = 1 then b.section_id end section_id,    "+
				"             1 pv_cnt, "+
				"             duration, "+
				"             req_date server_date, "+
				"             a.req_site_id site_id, "+
				"             req_domain domain_url,  "+
				"             req_page page,  "+
				"             req_parameter parameter, "+
				"             REQ_QUERY_DECODE QUERY_DECODE, "+
				"             sysdate, PATH_ID "+
				"    FROM    tb_smart_browser_itrack a,  "+
				"             ( "+
//				"             select SITE_ID, PSECTION_ID, SECTION_ID, PATH_URL, TYPE_CD, PATH_ID "+
//				"             from tb_smart_section_path "+
//				"             where type_cd='P' "+
				"             select /*+index(a,IDX_SMART_SECTION_PATH_URL2)*/SITE_ID, PSECTION_ID, SECTION_ID, PATH_URL, TYPE_CD, PATH_ID "+
				"             from tb_smart_section_path_url a"+
				"             where type_cd='P' " +
				"			  and   weekcode = fn_weekcode('"+accessday+"') "+
				"             ) b "+
				"    WHERE   b.site_id>0  "+
				"    AND     a.result_cd = 'S' "+
				"	 AND     a.panel_flag in ('D','V') "+
				"    AND     a.req_site_id=b.site_id "+
				"    AND     a.req_domain||decode(req_page, NULL, NULL, '/'||req_page) like b.path_url||'%' "+
				"    AND     A.access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"    AND     A.access_day <= '"+accessday+"' "+
				") "+
				"WHERE PSECTION_ID IS NOT NULL "+
				"AND   SECTION_ID IS NOT NULL ";
		//System.out.println(query3);
        this.pstmt = connection.prepareStatement(query3);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query4 = 
				"INSERT INTO tb_smart_week_temp_section c "+
				"SELECT access_day, panel_id, psection_id, section_id, psection_id visit_psection_id, section_id visit_section_id, "+
				"	   1 pv_cnt, duration, server_date, site_id, domain_url, page, parameter, query_decode, sysdate, path_id "+
				"FROM ( "+
				"    SELECT   /*+parallel(a,4) use_hash(b,a)*/ "+
				"             access_day, "+
				"             panel_id,  "+
				"             case when rank() over ( partition by req_domain||decode(req_page||req_parameter, NULL, NULL, '/'||req_page)|| "+
				"             decode(req_parameter, NULL, NULL, '?'||req_parameter) "+
				"             order by lengthb(path_url) desc ) = 1 then b.psection_id end psection_id, "+
				"             case when rank() over ( partition by req_domain||decode(req_page||req_parameter, NULL, NULL, '/'||req_page)|| "+
				"             decode(req_parameter, NULL, NULL, '?'||req_parameter) "+
				"             order by lengthb(path_url) desc ) = 1 then b.section_id end section_id,   "+
				"             1 pv_cnt, "+
				"             duration, "+
				"             req_date server_date, "+
				"             a.req_site_id site_id, "+
				"             req_domain domain_url,  "+
				"             req_page page,  "+
				"             req_parameter parameter, "+
				"             REQ_QUERY_DECODE QUERY_DECODE, "+
				"             sysdate, path_id "+
				"    FROM    tb_smart_browser_itrack a,  "+
				"             ( "+
//				"             select SITE_ID, PSECTION_ID, SECTION_ID, replace(PATH_URL,chr(95),'@!') PATH_URL, TYPE_CD, PATH_ID "+
//				"             from tb_smart_section_path "+
//				"             where type_cd='Q' "+
				"             select /*+index(a,IDX_SMART_SECTION_PATH_URL2)*/SITE_ID, PSECTION_ID, SECTION_ID, replace(PATH_URL,chr(95),'@!') PATH_URL, TYPE_CD, PATH_ID "+
				"             from tb_smart_section_path_url a"+
				"             where type_cd='Q' " +
				"			  and   weekcode = fn_weekcode('"+accessday+"') "+
				"             ) b "+
				"    WHERE   b.site_id>0  "+
				"    AND     a.result_cd = 'S' "+
				"	 AND     a.panel_flag in ('D','V') "+
				"    AND     a.req_site_id=b.site_id "+
				"    AND     a.req_domain||decode(req_page||replace(req_parameter,chr(95),'@!'), NULL, NULL, '/'||req_page)||decode(replace(req_parameter,chr(95),'@!'), NULL, NULL, '?'||replace(req_parameter,chr(95),'@!')) like b.path_url||'%' "+
				"    AND     A.access_day >= to_char(to_date('"+accessday+"','yyyymmdd')-6,'yyyymmdd') "+
				"    AND     A.access_day <= '"+accessday+"' "+
				") "+
				"WHERE PSECTION_ID IS NOT NULL "+
				"AND   SECTION_ID IS NOT NULL ";
		//System.out.println(query4);	
		//System.exit(0);
        this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeSectionTemp
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 월간 Section Temp 배치문 (TRUNCATE & INSERT)
	 *************************************************************************/
	
	public Calendar executeSectionTemp(String mode, String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Section Temp is processing...");
		String condition = "";
		String tablename = "";
		if(mode.equalsIgnoreCase("w")){
			condition = "    AND     a.panel_flag in ('D','N') ";
			tablename = "TB_SMART_TEMP_SECTION_WGT";
		} else if(mode.equalsIgnoreCase("v")) {
			condition = "    AND     a.panel_flag in ('D','V') ";
			tablename = "TB_SMART_MONTH_TEMP_SECTION";
			//tablename = "TEMP_HWLEE_TEMP_SECTION";
		} else {
			System.out.println("Error");
			System.exit(0);
		}
		
//		String query1 = 
//				"truncate table "+tablename;
//		
//        this.pstmt = connection.prepareStatement(query1);
//		this.pstmt.executeUpdate();
//		if(this.pstmt!=null) this.pstmt.close();
		
		String query2 = 
				"INSERT INTO "+tablename+" c "+
				"SELECT * "+
				"FROM ( "+
				"    SELECT   /*+parallel(a,4) use_hash(b,a)*/ "+
				"             access_day, "+
				"             panel_id,  "+
				"             b.psection_id psection_id, "+
				"             b.section_id section_id, "+
				"             b.psection_id visit_psection_id, "+
				"		      b.section_id visit_section_id, "+
				"             1 pv_cnt, "+
				"             duration, "+
				"             req_date server_date, "+
				"             a.req_site_id site_id, "+
				"             req_domain domain_url,  "+
				"             req_page page,  "+
				"             req_parameter parameter, "+
				"             REQ_QUERY_DECODE QUERY_DECODE, "+
				"             sysdate, "+
				"			  path_id "+	
				"    FROM    tb_smart_browser_itrack a,  "+
				"             ( "+
//				"             select SITE_ID, PSECTION_ID, SECTION_ID, PATH_URL, TYPE_CD, PATH_ID "+
//				"             from tb_smart_section_path "+
//				"             where type_cd='D' "+
				"             select SITE_ID, PSECTION_ID, SECTION_ID, PATH_URL, TYPE_CD, PATH_ID "+
				"             from   tb_smart_section_path_url "+
				"             where  type_cd='D' " +
				"			  and    monthcode = '"+monthcode+"' "+
				"             ) b "+
				"    WHERE   b.site_id>0  "+
				"    AND     a.result_cd = 'S' "+ condition +
				"    AND     a.req_site_id=b.site_id "+
				"    AND     a.req_domain=b.path_url "+
				"    AND     A.access_day >= '"+monthcode+"01' "+
				"    AND     A.access_day <= FN_MONTH_LASTDAY('"+monthcode+"') "+
				") "+
				"WHERE PSECTION_ID IS NOT NULL "+
				"AND   SECTION_ID IS NOT NULL ";
		System.out.println(query2);
        this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query3 = 
				"INSERT INTO "+tablename+" c "+
				"SELECT access_day, panel_id, psection_id, section_id, psection_id visit_psection_id, section_id visit_section_id, "+
				"	   1 pv_cnt, duration, server_date, site_id, domain_url, page,  parameter,  query_decode, sysdate, path_id "+
				"FROM ( "+
				"    SELECT   /*+parallel(a,4) use_hash(b,a)*/ "+
				"             access_day, "+
				"             panel_id,  "+
				"             case when rank() over ( partition by req_domain||decode(req_page, NULL, NULL, '/'||req_page) "+
				"             order by lengthb(path_url) desc ) = 1 then b.psection_id end psection_id, "+
				"             case when rank() over ( partition by req_domain||decode(req_page, NULL, NULL, '/'||req_page) "+
				"             order by lengthb(path_url) desc ) = 1 then b.section_id end section_id,    "+
				"             1 pv_cnt, "+
				"             duration, "+
				"             req_date server_date, "+
				"             a.req_site_id site_id, "+
				"             req_domain domain_url,  "+
				"             req_page page,  "+
				"             req_parameter parameter, "+
				"             REQ_QUERY_DECODE QUERY_DECODE, "+
				"             sysdate, PATH_ID "+
				"    FROM    tb_smart_browser_itrack a,  "+
				"             ( "+
//				"             select SITE_ID, PSECTION_ID, SECTION_ID, PATH_URL, TYPE_CD, PATH_ID "+
//				"             from tb_smart_section_path "+
//				"             where type_cd='P' "+
				"             select SITE_ID, PSECTION_ID, SECTION_ID, PATH_URL, TYPE_CD, PATH_ID "+
				"             from   tb_smart_section_path_url "+
				"             where  type_cd='P' " +
				"			  and    monthcode = '"+monthcode+"' "+
				"             ) b "+
				"    WHERE   b.site_id>0  "+
				"    AND     a.result_cd = 'S' "+ condition +
				"    AND     a.req_site_id=b.site_id "+
				"    AND     a.req_domain||decode(req_page, NULL, NULL, '/'||req_page) like b.path_url||'%' "+
				"    AND     A.access_day >= '"+monthcode+"01' "+
				"    AND     A.access_day <= FN_MONTH_LASTDAY('"+monthcode+"') "+
				") "+
				"WHERE PSECTION_ID IS NOT NULL "+
				"AND   SECTION_ID IS NOT NULL ";
		System.out.println(query3);
        this.pstmt = connection.prepareStatement(query3);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		String query4 = 
				"INSERT INTO "+tablename+" c "+
				"SELECT access_day, panel_id, psection_id, section_id, psection_id visit_psection_id, section_id visit_section_id, "+
				"	   1 pv_cnt, duration, server_date, site_id, domain_url, page, parameter, query_decode, sysdate, path_id "+
				"FROM ( "+
				"    SELECT   /*+parallel(a,4) use_hash(b,a)*/ "+
				"             access_day, "+
				"             panel_id,  "+
				"             case when rank() over ( partition by req_domain||decode(req_page||req_parameter, NULL, NULL, '/'||req_page)|| "+
				"             decode(req_parameter, NULL, NULL, '?'||req_parameter) "+
				"             order by lengthb(path_url) desc ) = 1 then b.psection_id end psection_id, "+
				"             case when rank() over ( partition by req_domain||decode(req_page||req_parameter, NULL, NULL, '/'||req_page)|| "+
				"             decode(req_parameter, NULL, NULL, '?'||req_parameter) "+
				"             order by lengthb(path_url) desc ) = 1 then b.section_id end section_id,   "+
				"             1 pv_cnt, "+
				"             duration, "+
				"             req_date server_date, "+
				"             a.req_site_id site_id, "+
				"             req_domain domain_url,  "+
				"             req_page page,  "+
				"             req_parameter parameter, "+
				"             REQ_QUERY_DECODE QUERY_DECODE, "+
				"             sysdate, path_id "+
				"    FROM    tb_smart_browser_itrack a,  "+
				"             ( "+
//				"             select SITE_ID, PSECTION_ID, SECTION_ID, replace(PATH_URL,chr(95),'@!') PATH_URL, TYPE_CD, PATH_ID "+
//				"             from tb_smart_section_path "+
//				"             where type_cd='Q' "+
				"             select SITE_ID, PSECTION_ID, SECTION_ID, replace(PATH_URL,chr(95),'@!') PATH_URL, TYPE_CD, PATH_ID "+
				"             from   tb_smart_section_path_url "+
				"             where  type_cd='Q' " +
				"			  and    monthcode = '"+monthcode+"' "+
				"             ) b "+
				"    WHERE   b.site_id>0  "+
				"    AND     a.result_cd = 'S' "+ condition +
				"    AND     a.req_site_id=b.site_id "+
				"    AND     a.req_domain||decode(req_page||replace(req_parameter,chr(95),'@!'), NULL, NULL, '/'||req_page)||decode(replace(req_parameter,chr(95),'@!'), NULL, NULL, '?'||replace(req_parameter,chr(95),'@!')) like b.path_url||'%' "+
				"    AND     A.access_day >= '"+monthcode+"01' "+
				"    AND     A.access_day <= FN_MONTH_LASTDAY('"+monthcode+"') "+
				") "+
				"WHERE PSECTION_ID IS NOT NULL "+
				"AND   SECTION_ID IS NOT NULL ";
		System.out.println(query4);	
        this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();
		
		//_ 占쎈땾占쎌젟 �뜎�눖�봺(2015.06월 까지 적용)
		//String query5 = 
		//		"update "+tablename+" "+
		//		"set section_id = 301 "+
		//		"where site_id = 178 "+
		//		"AND access_day >= '"+monthcode+"01' "+
		//		"AND access_day <= FN_MONTH_LASTDAY('"+monthcode+"') "+				
		//		"and section_id = 310 "+
		//		"and parameter not like '%where=m#_%' escape '#' "+
		//		"and parameter like '%where=m%' ";
		//System.out.println(query5);	
        //this.pstmt = connection.prepareStatement(query5);
		//this.pstmt.executeUpdate();
		//if(this.pstmt!=null) this.pstmt.close();
		
		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeSectionSiteFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: 월간 Section Site Fact 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeSectionSiteFact(String mode, int level, String monthcode) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		System.out.print("The batch - Section Fact is processing...");
		
		String temptablename = "";
		String tablename = "";
		String ctablename = "";
		if(mode.equalsIgnoreCase("w")){
			temptablename = "TB_SMART_TEMP_SECTION_WGT";
			tablename = "TB_SMART_MONTH_SECTION_WGT";
			ctablename = "TB_SMART_MONTH_CSECTION_WGT";
		} else if(mode.equalsIgnoreCase("v")) {
			temptablename = "TB_SMART_MONTH_TEMP_SECTION";
			tablename = "TB_SMART_MONTH_SECTION_FACT";
			ctablename = "TB_SMART_MONTH_CSECTION_FACT";
		} else {
			System.out.println("Error");
			System.exit(0);
		}
		
		if(level==1&&(!countTable(monthcode,"m",tablename))){
			System.out.println("Month Section Fact already exists.");
			System.exit(0);
		} else if(level==2&&(!countTable(monthcode,"m",ctablename))) {
			System.out.println("Month Section Fact already exists.");
			System.exit(0);
		}
		String query = "";
		String base_query = 
				"INSERT INTO "+tablename+" "+
				"(monthcode, site_id, panel_id, section_id, visit_cnt, pv_cnt, duration, daily_freq_cnt, proc_date) "+
				"SELECT   '"+monthcode+"', site_id, panel_id, psection_id, sum(visit_cnt) visit_cnt, "+
				"         sum(pv_cnt) pv_cnt, sum(duration) duration, count(distinct access_day) daily_freq_cnt, sysdate proc_date "+
				"FROM "+
				"(         "+
				"    SELECT  panel_id,  psection_id, site_id, "+
				"            case when round((server_date - lag(server_date, 1) over (partition by site_id, visit_psection_id, "+
				"            panel_id order by server_date))*60* 60*24) > 60*30 "+
				"            or lag(server_date, 1) over (partition by site_id, visit_psection_id, panel_id "+
				"            order by server_date) is NULL then 1  end visit_cnt, "+
				"            pv_cnt, "+
				"            duration, "+
				"            access_day "+
				"    FROM "+
				"    (       "+
				"        SELECT "+
				"               panel_id, psection_id, site_id, server_date, "+
				"               psection_id visit_psection_id, pv_cnt, duration, access_day "+
				"        FROM   "+temptablename+" a ";
		
		String foot_query = 
				"    	 WHERE  ACCESS_DAY between '"+monthcode+"01' and fn_month_lastday('"+monthcode+"') "+
				"	 ) "+
				") "+
				"where    panel_id in ( " +
				"	select panel_id " +
				"	from   tb_smart_month_panel_seg " +
				"	where  monthcode = '"+monthcode+"' " +
				") "+
				"GROUP  BY site_id, panel_id, psection_id "+
				"having sum(pv_cnt) > 0 ";
		//占쎄쉰占쎈�� 2占쎌쟿甕곤옙 揶쏉옙 筌ｌ꼶�봺.
		if(level == 2){
			base_query = base_query.replace(tablename,ctablename);
			base_query = base_query.replace("psection_id","section_id");
			query = base_query+
					"	 	WHERE  section_id in (19,20,21,22,23,24,25,41,42,43,44,51,52,53,54,55,61,62,301,302,303,304,305,306,307,308,309,310,311,312,313,401,402,403,404)"+
					"    	AND    ACCESS_DAY between '"+monthcode+"01' and fn_month_lastday('"+monthcode+"') "+
					"    ) " +
					") "+
					"where    panel_id in ( " +
					"	select panel_id " +
					"	from   tb_smart_month_panel_seg " +
					"	where  monthcode = '"+monthcode+"' " +
					") "+
					"GROUP  BY site_id, panel_id, section_id "+
					"having sum(pv_cnt) > 0 ";
		} else if(level == 1){
			query = base_query+foot_query;
		}
		
//		System.out.println(query);
//		System.exit(0);
		this.pstmt = connection.prepareStatement(query);
		this.pstmt.executeUpdate();
		if(this.pstmt!=null) this.pstmt.close();

		System.out.println("INSERTION DONE.");
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeAppByte
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: Day_App_byte 배치문 (INSERT)
	 *************************************************************************/
	
	public Calendar executeAppByte(String accessday) throws SQLException 
	{
		Calendar eachPt = Calendar.getInstance();
		
		if(countTable(accessday, "d", "tb_smart_day_app_byte")){
			String query = "insert  into tb_smart_day_app_byte "+
					"select access_day, smart_id, package_name,  "+
					"       panel_id, sum(rxbyte_gap) rxbyte_gap, sum(txbyte_gap) txbyte_gap, sysdate proc_date "+
					"from ( "+
					"    select  /*+use_hash(b,a)*/  "+
					"            access_day, rxbyte_gap, txbyte_gap, "+
					"            b.smart_id, a.item_value package_name, panel_id "+
					"    from    tb_smart_env_itrack a, ( "+
					"                select package_name, smart_id "+
					"                from tb_smart_app_info "+
					"                where exp_time >= to_date('"+accessday+"','yyyymmdd') "+
					"                and ef_time < to_date('"+accessday+"','yyyymmdd') "+
					"            ) b "+
					"    where   access_day = '"+accessday+"' "+
					"    and     a.item_value is not null "+
					"    and     a.rxbyte_gap is not null "+
					"    and     a.txbyte_gap is not null "+
					"    and     a.item_value = b.package_name "+
					") "+
					"group by access_day, smart_id, package_name, panel_id ";
			
			System.out.print("The batch - Day App Byte is processing...");
	        this.pstmt = connection.prepareStatement(query);
			this.pstmt.executeUpdate();
			
//					System.out.println(query);
			System.out.println("INSERTION DONE.");
			if(this.pstmt!=null) this.pstmt.close();
		} else {
			System.out.print("Day App Byte already exists.");
			System.exit(0);
		}
		
		return eachPt;
	}
	
	/**************************************************************************
	 *		메소드명		: executeDeletePanelUpdate
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: delete panel update 배치문 
	 *************************************************************************/
	
	public Calendar executeDeletePanelUpdate(String accessday) throws SQLException
	{
		Calendar eachPt = Calendar.getInstance();
		String query1 =  "insert into tb_panel_delete "+ 
						"select  a.panelid, panel_gubun types, "+ 
						"        decode(panel_gubun,0,'"+accessday+"', "+
						"										 3,to_char(ADD_MONTHS(to_date('"+accessday+"','yyyymmdd'),+36),'yyyymmdd'), "+
						"										 5,to_char(ADD_MONTHS(to_date('"+accessday+"','yyyymmdd'),+60),'yyyymmdd')) due_date, "+ 
						"        to_date('99990101','yyyymmdd') delete_date, "+ 
						"        sysdate ef_time, "+ 
						"        to_date('99990101','yyyymmdd') exp_time "+ 
						"from "+ 
						"( "+ 
						"    select  * "+ 
						"    from    tb_panel "+ 
						"    where   panelid not like 'tset%' "+ 
						"    and     panel_status_cd in ('CRQ','SUR','NIS','INS') "+ 
						"    and     recent_itrack_date < add_months(to_date('"+accessday+"','yyyymmdd'),-12) "+ 
						"    and     not exists (select 'X' from tb_smart_panel where status_env >= add_months(to_date('"+accessday+"','yyyymmdd'),-12) and panel_id = panelid) "+ 
						")a, "+ 
						"( "+ 
						"    select  a.panel_id, "+ 
						"            case when nvl(sweepstakes_check,0) = 5 then 5 "+ 
						"                 when delete_flag = 5 then 5 "+ 
						"                 when nvl(sweepstakes_check,0) = 3 then 3 "+ 
						"                 else delete_flag end panel_gubun "+ 
						"    from "+ 
						"    ( "+ 
						"        select  panel_id, "+ 
						"                case when used_mileage <> 0 then 5 "+ 
						"                     when used_mileage = 0 and total_mileage <> 0 then 3 "+ 
						"                     else 0 end delete_flag "+ 
						"        from    tb_panel_mileage "+ 
						"    )a, "+ 
						"    ( "+ 
						"        select  panel_id, max(bank_pay_check) sweepstakes_check "+ 
						"        from "+ 
						"        ( "+ 
						"            select  panel_id, decode(bank_pay_check,'Y',5,'y',5,3) bank_pay_check "+ 
						"            from    tb_sweepstakes "+ 
						"            where   amount > 10000 "+ 
						"        ) "+ 
						"        group by panel_id "+ 
						"    )b "+ 
						"    where   a.panel_id = b.panel_id(+) "+ 
						")b "+ 
						"where a.panelid = b.panel_id(+) "+ 
						"and panelid not in (select panel_id from tb_panel_delete where exp_time > sysdate) ";
		
		this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		
		String query2 = "update  tb_panel_delete "+ 
		                "set     exp_time = to_date('"+accessday+"','yyyymmdd') "+ 
		                "where   exp_time > sysdate "+ 
		                "and     delete_date > sysdate "+ 
		                "and     panel_id in ( "+ 
		                "    select panelid from tb_panel where recent_itrack_date >= to_date('"+accessday+"','yyyymmdd')-2 "+ 
		                "    union all "+ 
		                "    select panel_id from tb_smart_panel where status_env >= to_date('"+accessday+"','yyyymmdd')-2 "+ 
		                "    union all "+ 
		                "    select panel_id from tb_smart_panel_backup where status_env >= to_date('"+accessday+"','yyyymmdd')-2 "+ 
		                ") ";
		
		this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();		
		
		String query3 = "update  tb_panel_delete "+ 
						"set     exp_time = to_date('"+accessday+"','yyyymmdd') "+ 
						"where   exp_time > sysdate "+ 
						"and     delete_date > sysdate "+ 
						"and     types = 0 "+ 
						"and     panel_id in (select panel_id from tb_panel_mileage where total_mileage > 0) ";

		this.pstmt = connection.prepareStatement(query3);
		this.pstmt.executeUpdate();		
		
		String query4 = "insert into tb_panel_delete "+ 
						"select  panel_id, 3 types, to_char(ADD_MONTHS(to_date('"+accessday+"','yyyymmdd'),+36),'yyyymmdd') due_date, to_date('99990101','yyyymmdd') delete_date, "+ 
						"        to_date('"+accessday+"','yyyymmdd') ef_time, to_date('99990101','yyyymmdd') exp_time "+ 
						"from    tb_panel_delete "+ 
						"where   exp_time = to_date('"+accessday+"','yyyymmdd') "+ 
						"and     delete_date > sysdate "+ 
						"and     types = 0 "+ 
						"and     panel_id in (select panel_id from tb_panel_mileage where total_mileage > 0) "+ 
						"and     panel_id not in (select panel_id from tb_panel_delete where exp_time > sysdate) "+ 
						"group by panel_id ";
		
		this.pstmt = connection.prepareStatement(query4);
		this.pstmt.executeUpdate();	
		
		
		String query5 = "update  tb_panel_delete "+ 
						"set     exp_time = to_date('"+accessday+"','yyyymmdd') "+ 
						"where   exp_time > sysdate "+ 
						"and     delete_date > sysdate "+ 
						"and     types = 3 "+ 
						"and     panel_id in ( "+ 
						"    select panel_id from tb_panel_mileage where used_mileage <> 0 "+ 
						"    union all "+ 
						"    select panel_id from tb_sweepstakes where amount > 10000 and bank_pay_check in ('Y','y') "+ 
						") ";
		
		this.pstmt = connection.prepareStatement(query5);
		this.pstmt.executeUpdate();	
		
		String query6 = "insert into tb_panel_delete "+ 
						"select  panel_id, 5 types, to_char(ADD_MONTHS(to_date('"+accessday+"','yyyymmdd'),+60),'yyyymmdd') due_date, to_date('99990101','yyyymmdd') delete_date, "+ 
						"        to_date('"+accessday+"','yyyymmdd') ef_time, to_date('99990101','yyyymmdd') exp_time "+ 
						"from    tb_panel_delete "+ 
						"where   exp_time = to_date('"+accessday+"','yyyymmdd')"+ 
						"and     delete_date > sysdate "+ 
						"and     types = 3 "+ 
						"and     panel_id in ( "+ 
						"    select panel_id from tb_panel_mileage where used_mileage <> 0 "+ 
						"    union all "+ 
						"    select panel_id from tb_sweepstakes where amount > 10000 and bank_pay_check in ('Y','y') "+ 
						") "+ 
						"group by panel_id ";
		
		this.pstmt = connection.prepareStatement(query6);
		this.pstmt.executeUpdate();			
		
		
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("DONE.");
		return eachPt;
	}
	
	
	
	/**************************************************************************
	 *		메소드명		: executeDsMobileFact
	 *		인자			: query ( SQL 문 )
	 *		리턴형		: void
	 *		설명			: delete panel update 배치문 
	 *************************************************************************/
	
	public Calendar executeDsMobileFact(String accessday) throws SQLException
	{
		Calendar eachPt = Calendar.getInstance();
		String query1 =  "insert into temp_ds_weekly_data "+
						"select a.ACCESS_DAY, count(distinct a.panel_id), count(distinct b.panel_id) doner_panel, null,null,null "+
						"from tb_smart_day_panel_seg a, tb_smart_doner_panel b "+
						"where access_day ='"+accessday+"' "+
						"and b.PANEL_FLAG(+) = 'Y' "+
						"and a.panel_id = b.panel_Id(+) "+
						"and   b.exp_time(+) > to_date(a.access_day,'yyyymmdd') "+
						"and   b.ef_time(+)  < to_date(a.access_day,'yyyymmdd')+1 "+
						"group by ACCESS_DAY ";  
		
		this.pstmt = connection.prepareStatement(query1);
		this.pstmt.executeUpdate();
		
		String query2 = "update /*+ BYPASS_UJVC */ "+
						"( "+
						"     select a.*, app_cnt, web_cnt, total_cnt "+
						"     from temp_ds_weekly_data a, "+
						"     ( "+
						"          select access_day, count(distinct app_panel) app_cnt, count(distinct web_panel) web_cnt, count(distinct total_panel) total_cnt "+
						"          from "+
						"          ( "+
						"               select access_day, case when flag = 'app' then panel_id else null end app_panel "+
						"                                , case when flag = 'web' then panel_id else null end web_panel "+
						"                                , case when flag = 'web+app' then panel_id else null end total_panel "+
						"               from "+
						"               ( "+
						"                       select a.access_day, nvl(flag, 'web+app') flag, a.panel_id "+
						"                       from "+
						"                       ( "+
						"                           select  access_day, 'app' flag, PANEL_ID "+
						"                           FROM    tb_smart_day_app_fact "+    
						"                           WHERE   access_day = '"+accessday+"' "+
						"                           group by  access_day, PANEL_ID "+
						"                           union all "+
						"                           select  access_day, 'web' flag, PANEL_ID "+
						"                           FROM    tb_smart_day_fact "+
						"                           WHERE   access_day ='"+accessday+"' "+
						"                           group by  access_day, PANEL_ID "+
						"                       ) a, tb_smart_day_panel_seg b "+
						"                       where a.panel_id = b.panel_id "+
						"                       and a.access_day = b.access_day "+
						"                       group by a.access_day, cube(flag), a.PANEL_ID "+
						"                 ) "+
						"          ) "+
						"          group by access_day "+
						"     )b "+
						"     where a.access_day = b.access_day "+
						") "+
						"set app_panel = app_cnt, web_panel = web_cnt, total_panel= total_cnt";
		
		this.pstmt = connection.prepareStatement(query2);
		this.pstmt.executeUpdate();		
		
				
		if(this.pstmt!=null) this.pstmt.close();
		System.out.println("DONE.");
		return eachPt;
	}
	
	
	
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		connection.setAutoCommit(autoCommit);
	}
	
	public boolean getAutoCommit() throws SQLException {
		return connection.getAutoCommit();
	}
	
	public void close() throws SQLException 
	{
		if (result != null) result.close();
		if (pstmt != null) pstmt.close();
		if (connection != null) connMgr.freeConnection("kcdb", connection);
	}
	
	public void partclose() throws SQLException 
	{
		if (result != null) result.close();
		if (pstmt != null) pstmt.close();
	}
	
	/**************************************************************************
	 *		메소드명		: finalize
	 *		인자			: 없음
	 *		리턴형		: void
	 *		설명			: 웹서버가 종료될때 자원을 회수하기 위한 메소드
	 *************************************************************************/
	protected void finalize() throws Throwable
	{
		this.close();
	}
}
