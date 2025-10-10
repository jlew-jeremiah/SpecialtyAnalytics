/*
This is a quick and dirty approach to getting the SC TTS compensation. Eventually, it will likely be phased out to simply be on a dashboard. 
*/

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
	select distinct --count(*) /*
	a.bu,
	a.bu_id,
	a.rgn,
	a.site_cust_id,
	a.clstr_id,
	a.cust_ship_to_clstr_skey as clstr_skey,
	a.lcl_acct_typ AS acct_typ,
	upper(a.lcl_cust_nm) AS cust_nm,
	upper(coalesce(scd.sales_nm,sc.sc,a.sc,sf.territory_name)) as sc,
	upper(coalesce(scd.sales_cd,sc.sc_user_id,sf.sc_saml_federation_id)) as sc_id,
	upper(a.dsm) as dsm,
	w.fisc_yr_skey,
	-- BU cluster totals
	case when a.bu_id=3 then a.pcs_sold_qty else a.cs_eq_sold_qty end as uom,
	fp_qty AS fp_cluster_cases_sold,
	bn_qty AS mg_cluster_cases_sold,
	bl_qty AS bl_cluster_cases_sold,
	-- Cluster total excluding self
	case a.bu_id when 1 then clstr_qty_sold_ex_bl when 2 then clstr_qty_sold_ex_fp when 3 then clstr_qty_sold_ex_bn else 0 end as cluster_cases_sold_excl_self --*/
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
	LEFT JOIN (
		select t.cust_ship_to_clstr_skey as clstr_skey,w.fisc_yr_skey as fisc_yr_skey
		,sum(case bu_id when 3 then pcs_sold_qty when 2 then cs_eq_sold_qty else 0 end) clstr_qty_sold_ex_bl --pcs for bn, cs_eq for all else 
		,sum(case bu_id when 3 then pcs_sold_qty when 1 then cs_eq_sold_qty else 0 end) clstr_qty_sold_ex_fp
		,sum(case bu_id when 1 then cs_eq_sold_qty when 2 then cs_eq_sold_qty else 0 end) clstr_qty_sold_ex_bn
		,sum(case when bu_id=1 then cs_eq_sold_qty else 0 end) bl_qty
		,sum(case when bu_id=2 then cs_eq_sold_qty else 0 end) fp_qty
		,sum(case when bu_id=3 then pcs_sold_qty else 0 end) bn_qty
		from ts_spcl_edwp.tts2_clstr_sale_head_fact t
		CROSS JOIN fiscDay fd
		CROSS JOIN fisc_wks_to_run AS wr
		JOIN edwp.cal_fisc_wk_dim w 
	    ON t.fisc_wk_id = w.fisc_wk_id
	   	AND w.fisc_yr_skey IN (fd.current_fisc_yr, fd.current_fisc_yr - 1)
	   	AND w.fisc_wk_of_yr_nbr BETWEEN wr.start_fisc_wk_of_yr_nbr AND wr.end_fisc_wk_of_yr_nbr 
	   	and t.lcl_acct_typ in ('TRS','TRP','LCC','BID','OTH')
		group by 1,2
	) b  
    ON  a.cust_ship_to_clstr_skey = b.clstr_skey 
    AND w.fisc_yr_skey = b.fisc_yr_skey
   where a.bu_id in (2,3) 
   and a.lcl_acct_typ in ('TRS','TRP','LCC','BID','OTH')
	-------------------------------------------------------------------------------------------------------
--	and a.clstr_id='FL000307'
--	and a.clstr_id='CA019429' --and rt.bu='BL'
--	and a.clstr_id='TX110055'
	-------------------------------------------------------------------------------------------------------
--	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
--	;
)
,base_tts_data as (
select bu
,bu_id
,max(rgn) as rgn
,clstr_id
,clstr_skey
,max(sc) as sc
,sc_id as sc_id
,max(dsm) as dsm
,fisc_yr_skey
,listagg(distinct site_cust_id,' / ') within group (order by site_cust_id) as site_cust_id
,listagg(distinct acct_typ,' / ') within group (order by site_cust_id) as acct_typ
,listagg(distinct upper(cust_nm),' / ') within group (order by site_cust_id) as cust_nm
,sum(uom) as uom
,max(fp_cluster_cases_sold) as fp_cluster_cases_sold
,max(mg_cluster_cases_sold) as mg_cluster_cases_sold
,max(bl_cluster_cases_sold) as bl_cluster_cases_sold
,max(cluster_cases_sold_excl_self) as cluster_cases_sold_excl_self
from base_tts_data1
where sc_id is not null 
group by 1,2/*,3*/,4,5/*,6*/,7/*,8*/,9
--;
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
	,max(rgn) as rgn
	,clstr_id
	,clstr_skey
	,max(dsm_nm) as dsm_nm
	,max(sc) as sc
	,sc_id 
	,fisc_yr_skey
	,listagg(distinct site_cust_id,' / ') within group (order by site_cust_id) as site_cust_id 
	,listagg(distinct acct_typ,' / ') within group (order by site_cust_id) as acct_typ
	,listagg(distinct cust_nm,' / ') within group (order by site_cust_id) as cust_nm
	,listagg(distinct data_source,' / ') within group (order by site_cust_id) as data_source
	,sum(uom) as uom
	from sc_d_new1 
	where sc_id is not null 
	group by 1,2,/*3,*/4,5/*,6,7*/,8,9
--	;
)
, almost AS (
	SELECT 
--	count(*)/*
	COALESCE(sc.bu_id, rt.bu_id) AS bu,
	COALESCE(sc.rgn, rt.rgn) AS rgn,
	coalesce(sc.clstr_id,rt.clstr_id) as clstr_id,
	max(coalesce(sc.site_cust_id,rt.site_cust_id)) AS site_cust_id,
	max(coalesce(sc.acct_typ,rt.acct_typ)) as acct_typ,
	max(coalesce(sc.cust_nm,rt.cust_nm)) as cust_nm,
	upper(COALESCE(sc.sc,rt.sc,hr.emple_medm_desc,hr1.emple_medm_desc)) AS sc,
	upper(coalesce(sc.sc_id,rt.sc_id)) as sc_id,
	max(upper(COALESCE(sc.dsm_nm,rt.dsm)))  AS dsm,
	coalesce(hr.emple_nbr,hr1.emple_nbr) AS sc_workday_nbr,
	CASE WHEN rt.sc IS NOT NULL THEN 'TTS' ELSE 'SC Dash.' END AS data_source,
	coalesce(sc.fisc_yr_skey,rt.fisc_yr_skey) as fisc_yr,
	sum(COALESCE(rt.UOM,0)) AS UOM, --leave out sc.uom
	max(COALESCE(fp_cluster_cases_sold,0))  AS fp_cluster_cases_sold,
	max(COALESCE(bl_cluster_cases_sold,0))  AS bl_cluster_cases_sold,
	max(COALESCE(mg_cluster_cases_sold,0))  AS mg_cluster_cases_sold,
	max(COALESCE(cluster_cases_sold_excl_self,0)) AS cluster_cases_sold_excl_self --*/
	FROM base_TTS_data AS rt
	left /*full*//*right*/ outer join SC_D_new AS sc ON rt.clstr_skey=sc.clstr_skey AND rt.fisc_yr_skey=sc.fisc_yr_skey and rt.bu_id=sc.bu_id and rt.sc_id=sc.sc_id
	LEFT JOIN edwp.org_emple_hr_mstr_dim AS hr ON sc.sc_id = hr.emple_user_nm AND hr.curr_rec_ind = 'Y' AND hr.emple_user_nm <> ''
	LEFT JOIN edwp.org_emple_hr_mstr_dim AS hr1 ON rt.sc_id = hr1.emple_user_nm AND hr1.curr_rec_ind = 'Y' AND hr1.emple_user_nm <> ''
	WHERE upper(COALESCE(sc.sc, rt.sc)) NOT IN ('ENTERPRISE APIUSER', '')
	------------------------------------------------------------------------------
--	and coalesce(sc.clstr_id,rt.clstr_id)= 'CA003082' 
	------------------------------------------------------------------------------
	GROUP BY 1,2,3,7,8,/*9,*/10,11,12
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
	,max(site_cust_id) as site_cust_id
	,max(acct_typ) as acct_typ
	,max(cust_nm) as cust_nm
	,max(sc) as sc
	,max(dsm) as dsm
	,max(data_source) as naming_data_source
	,sum(case when fisc_yr=f.current_fisc_yr   then uom else 0 end) as uom_ty
	,sum(case when fisc_yr=f.current_fisc_yr-1 then uom else 0 end) as uom_ly
	,sum(case when fisc_yr=f.current_fisc_yr   then fp_cluster_cases_sold else 0 end) as fp_cluster_cases_sold_ty
	,sum(case when fisc_yr=f.current_fisc_yr-1 then fp_cluster_cases_sold else 0 end) as fp_cluster_cases_sold_ly
	,sum(case when fisc_yr=f.current_fisc_yr   then bl_cluster_cases_sold else 0 end) as bl_cluster_cases_sold_ty
	,sum(case when fisc_yr=f.current_fisc_yr-1 then bl_cluster_cases_sold else 0 end) as bl_cluster_cases_sold_ly
	,sum(case when fisc_yr=f.current_fisc_yr   then mg_cluster_cases_sold else 0 end) as mg_cluster_cases_sold_ty
	,sum(case when fisc_yr=f.current_fisc_yr-1 then mg_cluster_cases_sold else 0 end) as mg_cluster_cases_sold_ly
	,sum(case when fisc_yr=f.current_fisc_yr   then cluster_cases_sold_excl_self else 0 end) as cluster_cases_sold_excl_self_ty
	,sum(case when fisc_yr=f.current_fisc_yr-1 then cluster_cases_sold_excl_self else 0 end) as cluster_cases_sold_excl_self_ly
	from almost
	join qual_sc on almost.sc_id=qual_sc.sales_cd --and qual=true 
	cross join fiscday f 
--	where clstr_id='CA003082' 
	group by 1,2,3,4,5,6,7,8,9
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
,uom_ty AS "Volume TY",
uom_ly AS "Volume LY",
uom_ty-uom_ly AS "Volume Growth",
uom_ty>uom_ly AS "Growth Eligibility_old",
fp_cluster_cases_sold_ty AS "Shared FP CS TY",
fp_cluster_cases_sold_ly AS "Shared FP CS LY",
fp_cluster_cases_sold_ty -fp_cluster_cases_sold_ly AS "Shared FP CS Growth",
bl_cluster_cases_sold_ty AS "Shared BL CS TY",
bl_cluster_cases_sold_ly AS "Shared BL CS LY",
bl_cluster_cases_sold_ty-bl_cluster_cases_sold_ly AS "Shared BL CS Growth",
mg_cluster_cases_sold_ty AS "Shared B|N CS TY",
mg_cluster_cases_sold_ly AS "Shared B|N CS LY",
bl_cluster_cases_sold_ty-bl_cluster_cases_sold_ly AS "Shared B|N CS Growth",
cluster_cases_sold_excl_self_ty AS "Total Shared CS TY",
cluster_cases_sold_excl_self_ly AS "Total Shared CS LY",
cluster_cases_sold_excl_self_ty-cluster_cases_sold_excl_self_ly AS "Total Shared CS Growth",
CASE WHEN "Total Shared CS Growth" > 0 THEN TRUE ELSE FALSE END AS "TTS Growth Eligibility_old",
CASE WHEN "Total Shared CS Growth" > 0 AND "Volume Growth" > 0 THEN "Total Shared CS Growth" * 0.2 ELSE 0 END AS "TTS Bonus_old"
FROM so_close AS fc
--CROSS JOIN fiscDay AS fd
LEFT join (select distinct market,rgn,rgn_index from ts_spcl_edwp.site_details where bu_id<>1) AS sd ON fc.rgn=sd.rgn_index
--where clstr_id='PA087311'
--order by 1,3,4,5
--) select sc_id,clstr_id,count(*) cnt from fnl group by 1,2 having count(*)>1 order by cnt desc 
;
