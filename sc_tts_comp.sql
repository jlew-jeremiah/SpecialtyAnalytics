WITH user_input AS (
	SELECT 
	  9::int AS custom_start_fisc_wk_of_yr_nbr -- replace the 0 with e.g. 1 to override
	, 13::int AS custom_end_fisc_wk_of_yr_nbr   -- replace the 0 with e.g. 10 to override
)
, fiscDay AS (
	SELECT cd.fisc_yr_skey AS current_fisc_yr, cd.fisc_mo_id AS current_fisc_mo_id, cd.fisc_mo_of_yr_nbr AS current_fisc_mo_of_yr, cd.fisc_day_of_yr_nbr AS current_fisc_day_of_yr, cd.fisc_wk_of_yr_nbr AS current_fisc_wk_of_yr, cd.day_of_wk AS current_weekday, cd.fisc_wk_of_yr_nbr * 10 + cd.day_of_wk AS current_wk_and_day, cd.fisc_qtr_of_yr_nbr AS current_qtr, cd.fisc_wk_of_mo_nbr AS current_wk_of_mo, lw.fisc_wk_of_mo_nbr AS last_wk_of_mo, 
	  CASE WHEN cd.fisc_qtr_of_yr_nbr = 1 THEN 4 ELSE (cd.fisc_qtr_of_yr_nbr - 1) % 4 END AS previous_qtr
	, cd.fisc_qtr_id AS current_qtr_id
	, CASE WHEN cd.fisc_qtr_of_yr_nbr = 1 THEN (cd.fisc_yr_skey - 1) * 100 + 4 ELSE cd.fisc_yr_skey * 100 + (cd.fisc_qtr_of_yr_nbr - 1) % 4 END AS previous_qtr_id
	, cd.day_of_wk::numeric::numeric(18,0) - 1.0 AS num_of_days
	, cd.fisc_wk_id AS current_fisc_wk_id
	, cd.day_dt AS dt
	, lw.fisc_wk_of_yr_nbr AS last_fw
	, lw.fisc_wk_id AS last_fisc_wk_id
	, (nw.fisc_yr_skey - 2) * 100 + LEAST(nw.fisc_wk_of_yr_nbr, 52) AS nxt_wk_2yrs_ago
	, cd.fisc_yr_skey - 1 AS last_fisc_yr
	, (cd.fisc_yr_skey - 1) * 100 + LEAST(nw.fisc_wk_of_yr_nbr, 52) AS last_yr_nxt_wk
	, CASE WHEN cd.fisc_mo_of_yr_nbr = 1  THEN 12 ELSE cd.fisc_mo_of_yr_nbr - 1 END AS previous_fisc_mo_of_yr
	, CASE WHEN cd.fisc_mo_of_yr_nbr = 1  THEN (cd.fisc_yr_skey - 1) * 100 + 12 ELSE cd.fisc_yr_skey * 100 + cd.fisc_mo_of_yr_nbr - 1 END AS previous_fisc_mo_id
	, CASE WHEN cd.fisc_mo_of_yr_nbr = 12 THEN cd.fisc_yr_skey * 100 + 1 ELSE (cd.fisc_yr_skey - 1) * 100 + cd.fisc_mo_of_yr_nbr + 1 END AS rolling_12_mo_period_stop
	, CASE WHEN cd.fisc_mo_of_yr_nbr = 12 THEN (cd.fisc_yr_skey - 1) * 100 + 1 ELSE (cd.fisc_yr_skey - 2) * 100 + cd.fisc_mo_of_yr_nbr + 1 END AS rolling_24_mo_period_stop
	, tyb1.fisc_mo_id AS prev_2yr_upper_range
	, CASE WHEN tyb1.fisc_mo_of_yr_nbr = 1 THEN (tyb1.fisc_yr_skey-1) * 100 + 12 else tyb1.fisc_mo_id-1 END AS prev_2yr_lower_range
	FROM edwp.cal_day_dim cd
	JOIN edwp.cal_day_dim lw ON (cd.day_dt - 7) = lw.day_dt
	JOIN edwp.cal_day_dim nw ON (cd.day_dt + 7) = nw.day_dt
	JOIN edwp.cal_day_dim tyb1 ON dateadd(YEAR, -2, cd.day_dt) = tyb1.day_dt
	WHERE cd.day_dt = ('now'::character varying::date - 1)
)
, fisc_wks_to_run AS (
	SELECT
	  CASE WHEN ui.custom_start_fisc_wk_of_yr_nbr = 0 THEN 1
		 ELSE ui.custom_start_fisc_wk_of_yr_nbr
	  END AS start_fisc_wk_of_yr_nbr
	, CASE WHEN ui.custom_end_fisc_wk_of_yr_nbr = 0 THEN fd.current_fisc_wk_of_yr
		 ELSE ui.custom_end_fisc_wk_of_yr_nbr
	  END AS end_fisc_wk_of_yr_nbr
	FROM user_input AS ui
	CROSS JOIN fiscDay AS fd
)
,scd as(
	select site_cust_id,s.fisc_wk_id,s.fisc_yr_skey,region
	,s.ship_qty,sales_nm,sales_cd,emple_job_cd,sales_job_classification,cust_acct_typ
	from ts_spcl_edwp.sc_dashboard_data s
	join edwp.cal_fisc_wk_dim w on s.fisc_wk_id=w.fisc_wk_id 
	cross join fisc_wks_to_run r 
	cross join fiscDay f 
	where w.fisc_yr_skey in (f.current_fisc_yr-1,f.current_fisc_yr)
	and w.fisc_wk_of_yr_nbr between r.start_fisc_wk_of_yr_nbr and r.end_fisc_wk_of_yr_nbr
	and s.emple_job_cd is not null 
--	and cust_acct_typ in ('TRS','TRP','LCC','BID','OTH') 
	and sales_job_classification='SC'
	and left(bu,1) in ('B','F')
--	;
)
, base_TTS_data1 AS (
	select --count(*) /*
	a.bu,
	a.bu_id,
	a.rgn,
	a.site_cust_id,
	coalesce(a.clstr_id,b.clstr_id) as clstr_id,
	a.cust_ship_to_clstr_skey as clstr_skey,
	a.lcl_acct_typ AS acct_typ,
	upper(a.lcl_cust_nm) AS cust_nm,
	upper(coalesce(scd.sales_nm,sc.sc,a.sc,sf.territory_name)) as sc,
	upper(coalesce(scd.sales_cd,sc.sc_user_id,sf.sc_saml_federation_id)) as sc_id,
	upper(a.dsm) as dsm,
	w.fisc_yr_skey,
	w.fisc_wk_id,
	-- BU cluster totals
	case when a.bu_id=3 then a.pcs_sold_qty else a.cs_eq_sold_qty end as uom,
	max(fp_qty_ty) over (partition by b.clstr_skey) as fp_cluster_cases_sold_ty,
	max(fp_qty_ly) over (partition by b.clstr_skey) as fp_cluster_cases_sold_ly,
	max(bn_qty_ty) over (partition by b.clstr_skey) as mg_cluster_pieces_sold_ty,
	max(bn_qty_ly) over (partition by b.clstr_skey) as mg_cluster_pieces_sold_ly,
	max(bl_qty_ty) over (partition by b.clstr_skey) as bl_cluster_cases_sold_ty,
	max(bl_qty_ly) over (partition by b.clstr_skey) as bl_cluster_cases_sold_ly
	-- Cluster total excluding self
--	case a.bu_id when 2 then clstr_qty_sold_ex_fp_ty when 3 then clstr_qty_sold_ex_bn_ty else 0 end as cluster_cases_sold_excl_self_ty,
--	case a.bu_id when 2 then clstr_qty_sold_ex_fp_ly when 3 then clstr_qty_sold_ex_bn_ly else 0 end as cluster_cases_sold_excl_self_ly--*/
	FROM ts_spcl_edwp.tts2_clstr_sale_head_fact a
	left join ts_spcl_edwp.tts2_sc sc on a.site_cust_id=sc.acct_id and a.bu_id in (2,3)  
	left join ts_spcl_edwp.sf_hierarchy sf on a.site_cust_id=sf.acct_id and a.bu_id in (2,3)  
	left join scd on a.fisc_wk_id=scd.fisc_wk_id and a.site_cust_id=scd.site_cust_id 
	CROSS JOIN fiscDay fd
	CROSS JOIN fisc_wks_to_run AS wr
	JOIN edwp.cal_fisc_wk_dim w 
	    ON a.fisc_wk_id = w.fisc_wk_id
	   AND w.fisc_yr_skey IN (fd.current_fisc_yr, fd.current_fisc_yr - 1)
	   AND w.fisc_wk_of_yr_nbr BETWEEN wr.start_fisc_wk_of_yr_nbr AND wr.end_fisc_wk_of_yr_nbr
	right JOIN (
		select t.cust_ship_to_clstr_skey as clstr_skey,t.clstr_id
--		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr   then case bu_id when 3 then pcs_sold_qty when 2 then cs_eq_sold_qty else 0 end else 0 end) clstr_qty_sold_ex_bl_ty --pcs for bn, cs_eq for all else 
--		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr-1 then case bu_id when 3 then pcs_sold_qty when 2 then cs_eq_sold_qty else 0 end else 0 end) clstr_qty_sold_ex_bl_ly
--		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr   then case bu_id when 3 then pcs_sold_qty when 1 then cs_eq_sold_qty else 0 end else 0 end) clstr_qty_sold_ex_fp_ty
--		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr-1 then case bu_id when 3 then pcs_sold_qty when 1 then cs_eq_sold_qty else 0 end else 0 end) clstr_qty_sold_ex_fp_ly
--		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr   then case bu_id when 1 then cs_eq_sold_qty when 2 then cs_eq_sold_qty else 0 end else 0 end) clstr_qty_sold_ex_bn_ty
--		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr-1 then case bu_id when 1 then cs_eq_sold_qty when 2 then cs_eq_sold_qty else 0 end else 0 end) clstr_qty_sold_ex_bn_ly
		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr   then case when bu_id=1 then cs_eq_sold_qty else 0 end else 0 end) bl_qty_ty
		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr-1 then case when bu_id=1 then cs_eq_sold_qty else 0 end else 0 end) bl_qty_ly
		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr   then case when bu_id=2 then cs_eq_sold_qty else 0 end else 0 end) fp_qty_ty
		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr-1 then case when bu_id=2 then cs_eq_sold_qty else 0 end else 0 end) fp_qty_ly
		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr   then case when bu_id=3 then pcs_sold_qty else 0 end else 0 end) bn_qty_ty
		,sum(case when w.fisc_yr_skey=fd.current_fisc_yr-1 then case when bu_id=3 then pcs_sold_qty else 0 end else 0 end) bn_qty_ly
		from ts_spcl_edwp.tts2_clstr_sale_head_fact t
		CROSS JOIN fiscDay fd
		CROSS JOIN fisc_wks_to_run AS wr
		JOIN edwp.cal_fisc_wk_dim w 
	    ON t.fisc_wk_id = w.fisc_wk_id
	   	AND w.fisc_yr_skey IN (fd.current_fisc_yr, fd.current_fisc_yr - 1)
	   	AND w.fisc_wk_of_yr_nbr BETWEEN wr.start_fisc_wk_of_yr_nbr AND wr.end_fisc_wk_of_yr_nbr 
		/* and t.acct_typ in ('TRS','TRP','LCC','BID','OTH') -- this is excluded because all */
		group by 1,2
	) b  
    ON  a.cust_ship_to_clstr_skey = b.clstr_skey 
   where 
   (
   	(a.bu_id in (2,3) 
   	and 
   	a.lcl_acct_typ in ('TRS','TRP','LCC','BID','OTH')
   	)
   	or a.cust_ship_to_clstr_skey is null -- 
   )
	-------------------------------------------------------------------------------------------------------
--	and a.clstr_id='CA012572' and a.bu_id=3
--	and a.clstr_id='CA019429' --and rt.bu='BL'
--	and a.clstr_id='TX110055'
	-------------------------------------------------------------------------------------------------------
--	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
--	;
)
,base_tts_data as (
	select bu
	,bu_id
	,clstr_id
	,clstr_skey
	,sc_id as sc_id
	,max(rgn) as rgn
	,max(sc) as sc
	,max(dsm) as dsm
	,listagg(distinct site_cust_id,' / ') within group (order by site_cust_id) as site_cust_id
	,listagg(distinct acct_typ,' / ') within group (order by site_cust_id) as acct_typ
	,listagg(distinct upper(cust_nm),' / ') within group (order by site_cust_id) as cust_nm
	,sum(case when fisc_yr_skey=fd.current_fisc_yr   then uom else 0 end) as uom_ty
	,sum(case when fisc_yr_skey=fd.current_fisc_yr-1 then uom else 0 end) as uom_ly
	,max(fp_cluster_cases_sold_ty) as fp_cluster_cases_sold_ty
	,max(fp_cluster_cases_sold_ly) as fp_cluster_cases_sold_ly
	,max(mg_cluster_pieces_sold_ty) as mg_cluster_pieces_sold_ty
	,max(mg_cluster_pieces_sold_ly) as mg_cluster_pieces_sold_ly
	,max(bl_cluster_cases_sold_ty) as bl_cluster_cases_sold_ty
	,max(bl_cluster_cases_sold_ly) as bl_cluster_cases_sold_ly
--	,max(cluster_cases_sold_excl_self_ty) as cluster_cases_sold_excl_self_ty
--	,max(cluster_cases_sold_excl_self_ly) as cluster_cases_sold_excl_self_ly
	from base_tts_data1
	CROSS JOIN fiscDay fd
	where sc_id is not null 
	---------------------------------------------------------------------------------------
--	and clstr_id in ('TX060331')
	---------------------------------------------------------------------------------------
	group by 1,2,3,4,5
--	order by clstr_id 
--	;
)
,qual_sc as (
	select max(region) region, sc.sales_cd,max(sc.sales_nm) as sales_nm,max(sales_job_classification) as sales_job_classification
	,sum(case when w.fisc_yr_skey=f.current_fisc_yr   then sc.ship_qty else 0 end) qty_ty
	,sum(case when w.fisc_yr_skey=f.current_fisc_yr-1 then sc.ship_qty else 0 end) qty_ly
	,sum(case when w.fisc_yr_skey=f.current_fisc_yr   then sc.ship_qty else 0 end)-sum(case when w.fisc_yr_skey=f.current_fisc_yr-1 then sc.ship_qty else 0 end) as qty_diff
	,sum(case when w.fisc_yr_skey=f.current_fisc_yr   then sc.ship_qty else 0 end)>sum(case when w.fisc_yr_skey=f.current_fisc_yr-1 then sc.ship_qty else 0 end) as qual
	from scd as sc -- select * from ts_spcl_edwp.sc_dashboard_data limit 10  
	join edwp.cal_fisc_wk_dim w on sc.fisc_wk_id=w.fisc_wk_id
	cross join fiscDay f
--	where sales_cd='SANXM074'
	group by /*1,*/2/*,3,4*/
--	;
)
, SC_D_new1 AS (
	SELECT scd.bu
	,case left(scd.bu,1) when 'F' then 2 else 3 end as bu_id
	,sd.rgn_index AS rgn
	,scd.site_cust_id
	,tsc.cust_ship_to_clstr_id as clstr_id
	,tsc.cust_ship_to_clstr_skey as clstr_skey
	,scd.cust_acct_typ AS acct_typ
	,upper(RIGHT(cust_ship_to_nm, length(cust_ship_to_nm)-length(cust_id)-3)) AS cust_nm
	,scd.dsm_nm
	,upper(scd.sales_nm) AS SC
	,upper(scd.sales_cd) AS SC_ID
	,scd.fisc_yr_skey
	,scd.source_ind_cd AS Data_source
	,sum(scd.ship_qty) AS UOM
	FROM ts_spcl_edwp.sc_dashboard_data AS scd 
	join ts_spcl_edwp.hr_job_code_classification hr on scd.emple_job_code_1=hr.hr_job_code and hr.classification='SC'
	join ts_spcl_edwp.tts_specialty_cust tsc on scd.site_cust_id=tsc.site_cust_id  
  	CROSS JOIN fiscDay AS fd
	CROSS JOIN fisc_wks_to_run AS wr
	JOIN ts_spcl_edwp.site_details AS sd ON scd.co_skey=sd.site_nbr
	WHERE 1=1
--		and source_ind_cd <> 'ERP'
		AND fisc_wk_id%100 BETWEEN wr.start_fisc_wk_of_yr_nbr AND wr.end_fisc_wk_of_yr_nbr AND scd.bu IN ('Buckhead/Newport', 'FreshPoint') 
		AND fisc_yr_skey IN (fd.current_fisc_yr,fd.current_fisc_yr-1) --AND initcap(sales_nm) IN ('Richard Rieman','Matthew Reinhart')
		and scd.cust_acct_typ in ('TRS','TRP','LCC','BID','OTH')
	GROUP BY scd.bu, sd.rgn_index, scd.site_cust_id, tsc.cust_ship_to_clstr_id,tsc.cust_ship_to_clstr_skey,scd.cust_acct_typ, RIGHT(cust_ship_to_nm, length(cust_ship_to_nm)-length(cust_id)-3), scd.dsm_nm, scd.sales_nm, scd.sales_cd, scd.fisc_yr_skey, scd.source_ind_cd
--	ORDER BY sales_nm, site_cust_id
--	;
)
,sc_d_new as (
	select bu
	,bu_id
	,clstr_id
	,clstr_skey
	,sc_id 
	,max(rgn) as rgn
	,max(dsm_nm) as dsm_nm
	,max(sc) as sc
--	,fisc_yr_skey
	,listagg(distinct site_cust_id,' / ') within group (order by site_cust_id) as site_cust_id 
	,listagg(distinct acct_typ,' / ') within group (order by site_cust_id) as acct_typ
	,listagg(distinct cust_nm,' / ') within group (order by site_cust_id) as cust_nm
	,listagg(distinct data_source,' / ') within group (order by site_cust_id) as data_source
--	,sum(uom) as uom
	from sc_d_new1 
	where sc_id is not null 
	---------------------------------------------------------------------------------
--	and 
	---------------------------------------------------------------------------------
	group by 1,2,3,4,5
--	;
)
, almost AS (
	SELECT 
--	count(*)/*
	COALESCE(sc.bu_id, rt.bu_id) AS bu,
	COALESCE(sc.rgn, rt.rgn) AS rgn,
	coalesce(sc.clstr_id,rt.clstr_id) as clstr_id,
	upper(COALESCE(sc.sc,rt.sc,hr.emple_medm_desc,hr1.emple_medm_desc)) AS sc,
	upper(coalesce(sc.sc_id,rt.sc_id)) as sc_id,
	coalesce(hr.emple_nbr,hr1.emple_nbr) AS sc_workday_nbr,
	CASE WHEN rt.sc IS NOT NULL THEN 'TTS' ELSE 'SC Dash.' END AS data_source,
	max(upper(COALESCE(sc.dsm_nm,rt.dsm)))  AS dsm,
	max(coalesce(sc.site_cust_id,rt.site_cust_id)) AS site_cust_id,
	max(coalesce(sc.acct_typ,rt.acct_typ)) as acct_typ,
	max(coalesce(sc.cust_nm,rt.cust_nm)) as cust_nm,
	max(COALESCE(rt.uom_ty,0)) AS UOM_ty, --leave out sc.uom
	max(COALESCE(rt.uom_ty,0)) AS UOM_ly, 
	max(COALESCE(fp_cluster_cases_sold_ty,0))  AS fp_cluster_cases_sold_ty,
	max(COALESCE(fp_cluster_cases_sold_ly,0))  AS fp_cluster_cases_sold_ly,
	max(COALESCE(bl_cluster_cases_sold_ty,0))  AS bl_cluster_cases_sold_ty,
	max(COALESCE(bl_cluster_cases_sold_ly,0))  AS bl_cluster_cases_sold_ly,
	max(COALESCE(mg_cluster_pieces_sold_ty,0))  AS mg_cluster_pieces_sold_ty,
	max(COALESCE(mg_cluster_pieces_sold_ly,0))  AS mg_cluster_pieces_sold_ly
	--*/
	FROM base_TTS_data AS rt
	left /*full*//*right*/ outer join SC_D_new AS sc ON rt.clstr_skey=sc.clstr_skey and rt.bu_id=sc.bu_id and rt.sc_id=sc.sc_id
	LEFT JOIN edwp.org_emple_hr_mstr_dim AS hr ON sc.sc_id = hr.emple_user_nm AND hr.curr_rec_ind = 'Y' AND hr.emple_user_nm <> ''
	LEFT JOIN edwp.org_emple_hr_mstr_dim AS hr1 ON rt.sc_id = hr1.emple_user_nm AND hr1.curr_rec_ind = 'Y' AND hr1.emple_user_nm <> ''
	WHERE upper(COALESCE(sc.sc, rt.sc)) NOT IN ('ENTERPRISE APIUSER', '')
	------------------------------------------------------------------------------
--	and coalesce(sc.clstr_id,rt.clstr_id)= 'CA012572' 
	------------------------------------------------------------------------------
	GROUP BY 1,2,3,4,5,6,7
--	;
)
,so_close as (
	select case almost.bu when 2 then 'FP' else 'B|N' end as bu
	,rgn
	,clstr_id
	,sc_id 
	,sc_workday_nbr
	,qual_sc.qty_ty
	,qual_sc.qty_ly
	,qual_sc.qty_diff
	,qual_sc.qual 
	,site_cust_id as site_cust_id
	,acct_typ as acct_typ
	,cust_nm as cust_nm
	,sc as sc
	,dsm as dsm
	,data_source as naming_data_source
--	,uom_ty as sc_uom_ty
--	,uom_ly as sc_uom_ly
	,case when bu=2 then fp_cluster_cases_sold_ty when bu=3 then mg_cluster_pieces_sold_ty end as uom_ty
	,case when bu=2 then fp_cluster_cases_sold_ly when bu=3 then mg_cluster_pieces_sold_ly end as uom_ly
	,fp_cluster_cases_sold_ty as fp_cluster_cases_sold_ty
	,fp_cluster_cases_sold_ly as fp_cluster_cases_sold_ly
	,mg_cluster_pieces_sold_ty as mg_cluster_pieces_sold_ty
	,mg_cluster_pieces_sold_ly as mg_cluster_pieces_sold_ly
	,bl_cluster_cases_sold_ty as bl_cluster_cases_sold_ty
	,bl_cluster_cases_sold_ly as bl_cluster_cases_sold_ly
	,fp_cluster_cases_sold_ty+mg_cluster_pieces_sold_ty+bl_cluster_cases_sold_ty-case when bu=2 then fp_cluster_cases_sold_ty when bu=3 then mg_cluster_pieces_sold_ty end as ttl_ex_self_ty
	,fp_cluster_cases_sold_ly+mg_cluster_pieces_sold_ly+bl_cluster_cases_sold_ly-case when bu=2 then fp_cluster_cases_sold_ly when bu=3 then mg_cluster_pieces_sold_ly end as ttl_ex_self_ly
	from almost
	join qual_sc on almost.sc_id=qual_sc.sales_cd --and qual=true 
	cross join fiscday f 
--	where clstr_id='TX060331' 
--	order by 3,4
--	;
)
,sc_not_sharing as(
	select region,sales_cd as sc_id,sales_nm as sc_nm,coalesce(sales_job_classification,'') as sales_job_classification
	,qty_ty as sc_ttl_qty_ty,qty_ly as sc_ttl_qty_ly,qty_diff as sc_ttl_qty_diff,qual as ttl_terr_qual
	from qual_sc 
	where sales_cd not in (select distinct sc_id from so_close)
--	;
)
--,fnl as (
SELECT fc.bu
, sd.market
, fc.rgn as rgn_id
, sd.rgn
, fc.clstr_id
, site_cust_id as site_cust_id
, acct_typ
, cust_nm 
, sc
, sc_id
, sc_workday_nbr
, dsm
, naming_data_source
, qual ttl_terr_qual
,qty_ty as ttl_vol_ty
,qty_ly as ttl_vol_ly
,qty_diff as ttl_diff_ty
--,sc_uom_ty as "SC Cluster Volume TY"
--,sc_uom_ly as "SC Cluster Volume LY"
--,sc_uom_ty-sc_uom_ly as "SC Cluster Volume Growth"
,uom_ty AS "BU Cluster Volume TY",
uom_ly AS "BU Cluster Volume LY",
uom_ty-uom_ly AS "BU Cluster Volume Growth",
fp_cluster_cases_sold_ty AS "Shared FP CS TY",
fp_cluster_cases_sold_ly AS "Shared FP CS LY",
fp_cluster_cases_sold_ty -fp_cluster_cases_sold_ly AS "Shared FP CS Growth",
mg_cluster_pieces_sold_ty AS "Shared B|N pcs TY",
mg_cluster_pieces_sold_ly AS "Shared B|N pcs LY",
mg_cluster_pieces_sold_ty-mg_cluster_pieces_sold_ly AS "Shared B|N pcs Growth",
bl_cluster_cases_sold_ty AS "Shared BL CS TY",
bl_cluster_cases_sold_ly AS "Shared BL CS LY",
bl_cluster_cases_sold_ty-bl_cluster_cases_sold_ly AS "Shared BL CS Growth",
ttl_ex_self_ty AS "Total Shared Excl BU TY",
ttl_ex_self_ly AS "Total Shared Excl BU LY",
ttl_ex_self_ty-ttl_ex_self_ly AS "Total Shared Growth"
FROM so_close AS fc
LEFT join (select distinct market,rgn,rgn_index from ts_spcl_edwp.site_details where bu_id<>1) AS sd ON fc.rgn=sd.rgn_index
--where clstr_id='PA087311'
order by 1,3,4,5
--) select sc_id,clstr_id,count(*) cnt from fnl group by 1,2 having count(*)>1 order by cnt desc 
;
