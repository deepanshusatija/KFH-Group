create or replace 
PACKAGE BODY PACK_KFH_BATCH
AS
FUNCTION return_context_id(
    VV_TARGET_RD    DATE,
    VV_WORKSPACE_ID VARCHAR2,
    POS             NUMBER)
  RETURN VARCHAR2
IS
  v_context_id NUMBER(5);
BEGIN
  SELECT context_id
  INTO v_context_id
  FROM CONTEXTS
  WHERE TO_CHAR(reporting_date, 'DD/MM/RRRR') = TO_CHAR(VV_TARGET_RD, 'DD/MM/YYYY')
  AND WORKSPACE_ID                            = VV_WORKSPACE_ID
  AND POSITION                                = POS;
  RETURN v_context_id;
EXCEPTION
WHEN OTHERS THEN
  RETURN 'Error !!! No data returned for the specified combination of reporting date, Workspace and Position';
END;
FUNCTION return_partition_key(
    VV_TARGET_RD    DATE,
    VV_TABLE_NAME   VARCHAR2,
    VV_WORKSPACE_ID NUMBER,
    POS             NUMBER)
  RETURN VARCHAR2
IS
  V_PART_KEY CONTEXTS.Pk_Rd_Ws%TYPE;
  T_TABLE_TYPE CD_FDW_STRUCTURE.TABLE_TYPE%TYPE;
BEGIN
  SELECT TABLE_TYPE
  INTO T_TABLE_TYPE
  FROM CD_FDW_STRUCTURE
  WHERE TABLE_NAME = VV_TABLE_NAME;
  SELECT DECODE(T_TABLE_TYPE, 'DATA', PK_RD, 'CALC', PK_RD_WS)
  INTO V_PART_KEY
  FROM CONTEXTS
  WHERE reporting_date = VV_TARGET_RD
  AND WORKSPACE_ID     = VV_WORKSPACE_ID
  AND POSITION         = pos;
  RETURN V_PART_KEY;
EXCEPTION
WHEN OTHERS THEN
  pack_log.log_write(v_function => 'PACK_KFH_BATCH.return_partition_key', v_message => 'Error while fetching the Partition key', v_step => 0, v_log_type => 'E', v_parameters => SQLCODE || ' :: ' || SQLERRM);
  raise;
END;
PROCEDURE CONTEXT_CREATE_KFH(
P_RD varchar2, -- yyyymmdd e.g.20160331
P_ADJUSTMENT VARCHAR2 DEFAULT 'N') ---- IF 'N' THEN MONTHLY.

is
V_CHECK_CONTEXT_GRP number:=0;
V_CHECK_CONTEXT_BAH number:=0;
V_CHECK_CONTEXT_MA NUMBER:=0;
-- Copy from variables
V_RD_FROM_GRP VARCHAR2(8);
V_POSITION_FROM NUMBER;
V_WORKSPACE_GRP NUMBER :=11;
V_WORKSPACE_BAH NUMBER :=10;
V_WORKSPACE_MA  NUMBER :=12;
--Copy to variables
V_POSITION_TO NUMBER;
V_WORKSPACE_TO NUMBER;
V_STEP NUMBER :=200;
BEGIN
PACK_LOG.LOG_BEGIN('PACK_KFH_BATCH.CREATE_CONTEXT_KFH');
-- Default values - copy context from master context 2
V_RD_FROM_GRP := '20181231';
V_POSITION_TO := 101;
V_POSITION_FROM :=101;
--- Check if the GROUP MONTH END context already exists ----
SELECT COUNT(1)
INTO V_CHECK_CONTEXT_GRP
FROM CONTEXTS
WHERE REPORTING_DATE = TO_DATE(P_RD,'YYYYMMDD')
AND POSITION = V_POSITION_TO
and WORKSPACE_ID = V_WORKSPACE_GRP;
--- Check if BAH MONTH END context already exist---
SELECT COUNT(1)
INTO V_CHECK_CONTEXT_BAH
FROM CONTEXTS
WHERE REPORTING_DATE = TO_DATE(P_RD,'YYYYMMDD')
AND POSITION = V_POSITION_TO
and WORKSPACE_ID = V_WORKSPACE_BAH;

--- check if MAL MONTH END context already exists--
SELECT COUNT(1)
INTO V_CHECK_CONTEXT_MA
FROM CONTEXTS
WHERE REPORTING_DATE = TO_DATE(P_RD,'YYYYMMDD')
and POSITION = V_POSITION_TO
and WORKSPACE_ID = V_WORKSPACE_MA;

IF P_RD is not null and P_ADJUSTMENT = 'N' and V_CHECK_CONTEXT_GRP = 0 
THEN
--- To Create MONTH END GROUP Context ---
v_Step:=201;
pack_log.log_write(v_function => 'PACK_KFH_BATCH.CREATE_CONTEXT_KFH', v_message => 'invoke group context creation for reporting date VV_TARGET_RD', v_step => v_step, v_log_type => 'I', v_parameters => '');

PACK_CONTEXT.CONTEXT_CREATE( V_REPORTING_DATE => TO_DATE(P_RD,'YYYYMMDD'), V_WORKSPACE_ID => V_WORKSPACE_GRP, V_CREATE_FROM_RD => TO_DATE(V_RD_FROM_GRP,'YYYYMMDD'), V_CREATE_FROM_WS => V_WORKSPACE_GRP,V_INCLUDE_IMPORT => 'N',V_DESCRIPTION => 'KUWAIT_MONTHEND_CONTEXT',V_POSITION => V_POSITION_TO, V_CREATE_FROM_POSITION => V_POSITION_FROM, V_COLLECT_STAT_ON_NEW_CONTEXT => 'Y', V_TABLESPACE_DATA => 'APPDATA', V_TABLESPACE_INDEX => 'APPINDEXES', V_TABLE_TYPES_TO_GATHER => 'DATA,PARAM,CALC,TEMP,WORKSPACE', V_DEFER_CREATION => 'Y',V_PARALLEL_STATS => 1,V_PARALLEL =>1);
END IF;

IF P_RD is not null and P_ADJUSTMENT = 'N' and V_CHECK_CONTEXT_BAH = 0 
THEN
v_step:=202;
pack_log.log_write(v_function => 'PACK_KFH_BATCH.CREATE_CONTEXT_KFH', v_message => 'invoke bahrain context creation for reporting date VV_TARGET_RD', v_step => v_step, v_log_type => 'I', v_parameters => '');
PACK_CONTEXT.CONTEXT_CREATE( V_REPORTING_DATE => TO_DATE(P_RD,'YYYYMMDD'), V_WORKSPACE_ID => V_WORKSPACE_BAH, V_CREATE_FROM_RD => TO_DATE(V_RD_FROM_GRP,'YYYYMMDD'), V_CREATE_FROM_WS => V_WORKSPACE_BAH,V_INCLUDE_IMPORT => 'N',V_DESCRIPTION => 'BAHRAIN_MONTHEND_CONTEXT',V_POSITION => V_POSITION_TO, V_CREATE_FROM_POSITION => V_POSITION_FROM, V_COLLECT_STAT_ON_NEW_CONTEXT => 'Y', V_TABLESPACE_DATA => 'APPDATA', V_TABLESPACE_INDEX => 'APPINDEXES',V_TABLE_TYPES_TO_GATHER => 'DATA,PARAM,CALC,TEMP,WORKSPACE',V_DEFER_CREATION => 'Y',V_PARALLEL_STATS =>1,V_PARALLEL =>1);
END IF;

IF P_RD is not null and P_ADJUSTMENT = 'N' and V_CHECK_CONTEXT_MA = 0 
THEN
v_step:=203;
pack_log.log_write(v_function => 'PACK_KFH_BATCH.CREATE_CONTEXT_KFH', v_message => 'invoke malaysia context creation for reporting date VV_TARGET_RD', v_step => v_step, v_log_type => 'I', v_parameters => '');
PACK_CONTEXT.CONTEXT_CREATE( V_REPORTING_DATE => TO_DATE(P_RD,'YYYYMMDD'), V_WORKSPACE_ID => V_WORKSPACE_MA, V_CREATE_FROM_RD => TO_DATE(V_RD_FROM_GRP,'YYYYMMDD'), V_CREATE_FROM_WS => V_WORKSPACE_MA,V_INCLUDE_IMPORT => 'N',V_DESCRIPTION => 'MALAYSIA_MONTHEND_CONTEXT',V_POSITION => V_POSITION_TO, V_CREATE_FROM_POSITION => V_POSITION_FROM, V_COLLECT_STAT_ON_NEW_CONTEXT => 'Y',V_TABLESPACE_DATA => 'APPDATA', V_TABLESPACE_INDEX => 'APPINDEXES', V_TABLE_TYPES_TO_GATHER => 'DATA,PARAM,CALC,TEMP,WORKSPACE',V_DEFER_CREATION => 'Y',V_PARALLEL_STATS =>1,V_PARALLEL =>1);
pack_context.contextid_create(v_context_id =>null,v_parallel_stats=>1,v_parallel=>1);
v_step:=204;
pack_log.log_write(v_function => 'PACK_KFH_BATCH.CREATE_CONTEXT_KFH', v_message => 'Successfully created context(s) for reporting date VV_TARGET_RD', v_step => v_step, v_log_type => 'I', v_parameters => '');
END IF;
COMMIT;

EXCEPTION
WHEN OTHERS THEN
pack_log.log_write(v_function => 'PACK_KFH_BATCH.CREATE_CONTEXT_KFH', v_message => 'Context creation failed for VV_TARGET_RD', v_step => v_step, v_log_type => 'E', v_parameters => '');
PACK_LOG.LOG_END('CREATE_CONTEXT_KFH');
END;

PROCEDURE POST_ETL_PROCESS(
    VV_TARGET_RD DATE,
    VV_RESULT OUT VARCHAR2)
IS
  v_step NUMBER := 100;
  v_context_id contexts.context_id%TYPE;
BEGIN
  pack_context.contextid_open(1);
  v_step := 101;
  pack_log.log_write(v_function => 'PACK_KFH_BATCH.POST_ETL_PROCESS', v_message => 'Start post ETL process for reporting date -' || TO_CHAR(VV_TARGET_RD, 'DD-MM-YYYY'), v_step => v_step, v_log_type => 'I', v_parameters => '');
  v_step := 102;
  pack_context.contextid_open(v_context_id);
  pack_log.log_write(v_function => 'PACK_KFH_BATCH.POST_ETL_PROCESS', v_message => 'Context selected is :' || v_context_id || ' Trigger post ETL process ' || TO_CHAR(VV_TARGET_RD, 'DD-MM-YYYY'), v_step => v_step, v_log_type => 'I', v_parameters => '');
  v_step := 103;
  pack_context.contextid_open(v_context_id);
  pack_log.log_write(v_function => 'PACK_KFH_BATCH.POST_ETL_PROCESS', v_message => 'Trigger check errors for RCO tables', v_step => v_step, v_log_type => 'I', v_parameters => '');
  FOR i IN
  (SELECT DISTINCT table_name
  FROM IMPORT_TABLES
  WHERE import_set_code IN ('Market Data', 'RCO Checks','SCN_TRANSF')
  )
  LOOP
    pack_batch.recheck_table(I.TABLE_NAME,null,null,null,'M',NULL,8,8);
  END LOOP;
  v_step := 104;
  pack_log.log_write(v_function => 'PACK_KFH_BATCH.POST_ETL_PROCESS', v_message => 'Disable triggers for table - SAE_INTERIM_BASEDATA', v_step => v_step, v_log_type => 'I', v_parameters => '');
  pack_ddl.enable_disable_trigger('SAE_INTERIM_BASEDATA', 'N');
  v_step := 105;
  pack_log.log_write(v_function => 'PACK_KFH_BATCH.POST_ETL_PROCESS', v_message => 'Post_etl_process completed successfully', v_step => v_step, v_log_type => 'I', v_parameters => '');
  VV_RESULT := 'SUCCESS';
  RETURN ; --XXXX12092018 -- call not returning to the calling environment
EXCEPTION
WHEN OTHERS THEN
  pack_log.log_write(v_function => 'PACK_KFH_BATCH.POST_ETL_PROCESS', v_message => 'Trigger post ETL process ' || TO_CHAR(VV_TARGET_RD, 'DD-Mon-RRRR'), v_step => v_step, v_log_type => 'I', v_parameters => '');
  VV_RESULT := 'FAILED';
  RETURN ; --XXXX12092018 -- call not returning to the calling environment
END;

PROCEDURE KFH_CREATE_ALL_CONTEXTS is
curr_time date;

v_function varchar(100):='PACK_KFH_BATCH.KFH_CREATE_ALL_CONTEXTS';

v_context_creation_rd date;
v_golden_context_date date;

v_golden_ws_bahrain number;
v_golden_ws_malaysia number;
v_golden_ws_kuwait number;

v_pos_my_bah number;
v_pos_kwt number;

v_ws_id_my_ret number;
v_ws_id_my_whs number;
v_ws_id_bah_ret number;
v_ws_id_bah_whs number;
v_ws_id_kwt_cbk_ret number;
v_ws_id_kwt_cbk_whs number;
v_ws_id_my_cbk_ret number;
v_ws_id_my_cbk_whs number;
v_ws_id_bah_cbk_ret number;
v_ws_id_bah_cbk_whs number;
v_ws_id_my_results number;
v_ws_id_bah_results number;
v_ws_id_kwt_results number;

v_copy_set_id number;

begin

select to_date(value,'YYYYMMDD') into v_context_creation_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
select to_date(value,'YYYYMMDD') into v_golden_context_date from kfh_custom_parameters where parameter = 'GOLDEN_CONTEXT_DATE';

select value into v_golden_ws_bahrain from kfh_custom_parameters where parameter like 'GOLDEN_WORKSPACE_ID_BAHRAIN';
select value into v_golden_ws_malaysia from kfh_custom_parameters where parameter like 'GOLDEN_WORKSPACE_ID_MALAYSIA';
select value into v_golden_ws_kuwait from kfh_custom_parameters where parameter like 'GOLDEN_WORKSPACE_ID_KUWAIT';

select position into v_pos_my_bah from position where description like 'IFRS9 - UAT';
select position into v_pos_kwt from position where description like 'Final - Adjusted Maturities';
pack_log.log_write('I','F',v_function,'Step 0','Context creation RD: ' || v_context_creation_rd || ', MY/BAH position: ' || v_pos_my_bah || ', KWT position: ' || v_pos_kwt || '.');

select set_id into v_copy_set_id from context_set where code like 'CONTEXT_CREATION_COPY_SET';

curr_time:=sysdate;
select workspace_id into v_ws_id_my_ret from workspaces where name like 'MALAYSIA_RETAIL';
pack_log.log_write('I','F',v_function,'Step 1','Starting to create context Malaysia Retail on workspace:' || v_ws_id_my_ret || ' and position: ' || v_pos_my_bah || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_malaysia, v_create_from_position => v_pos_my_bah, v_workspace_id => v_ws_id_my_ret, v_position => v_pos_my_bah, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Malaysia Retail Context');
pack_log.log_write('I','F',v_function,'Step 2','Malaysia Retail created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_my_whs from workspaces where name like 'MALAYSIA';
pack_log.log_write('I','F',v_function,'Step 3','Starting to create context Malaysia Wholesale on workspace:' || v_ws_id_my_whs || ' and position: ' || v_pos_my_bah || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_malaysia, v_create_from_position => v_pos_my_bah, v_workspace_id => v_ws_id_my_whs, v_position => v_pos_my_bah, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Malaysia Wholesale Context');
pack_log.log_write('I','F',v_function,'Step 4','Malaysia Wholesale created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_ret from workspaces where name like 'BAHRAIN_RETAIL';
pack_log.log_write('I','F',v_function,'Step 5','Starting to create context Bahrain Retail on workspace:' || v_ws_id_bah_ret || ' and position: ' || v_pos_my_bah || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_bahrain, v_create_from_position => v_pos_my_bah, v_workspace_id => v_ws_id_bah_ret, v_position => v_pos_my_bah, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Bahrain Retail Context');
pack_log.log_write('I','F',v_function,'Step 6','Bahrain Retail created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_whs from workspaces where name like 'BAHRAIN';
pack_log.log_write('I','F',v_function,'Step 7','Starting to create context Bahrain Wholesale on workspace:' || v_ws_id_bah_whs || ' and position: ' || v_pos_my_bah || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_bahrain, v_create_from_position => v_pos_my_bah, v_workspace_id => v_ws_id_bah_whs, v_position => v_pos_my_bah, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Bahrain Wholesale Context');
pack_log.log_write('I','F',v_function,'Step 8','Bahrain Wholesale created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_kwt_cbk_ret from workspaces where name like 'KUWAIT_RETAIL';
pack_log.log_write('I','F',v_function,'Step 9','Starting to create context Kuwait CBK Retail on workspace:' || v_ws_id_kwt_cbk_ret || ' and position: ' || v_pos_kwt || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_kuwait, v_create_from_position => v_pos_kwt, v_workspace_id => v_ws_id_kwt_cbk_ret, v_position => v_pos_kwt, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Kuwait CBK Retail Context');
pack_log.log_write('I','F',v_function,'Step 10','Kuwait CBK Retail created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_kwt_cbk_whs from workspaces where name like 'KUWAIT';
pack_log.log_write('I','F',v_function,'Step 11','Starting to create context Kuwait CBK Wholesale on workspace:' || v_ws_id_kwt_cbk_whs || ' and position: ' || v_pos_kwt || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_kuwait, v_create_from_position => v_pos_kwt, v_workspace_id => v_ws_id_kwt_cbk_whs, v_position => v_pos_kwt, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Kuwait CBK Wholesale Context');
pack_log.log_write('I','F',v_function,'Step 12','Kuwait CBK Wholesale created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_my_cbk_ret from workspaces where name like 'MALAYSIA_CBK_RETAIL';
pack_log.log_write('I','F',v_function,'Step 13','Starting to create context Malaysia CBK Retail on workspace:' || v_ws_id_my_cbk_ret || ' and position: ' || v_pos_kwt || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_kuwait, v_create_from_position => v_pos_kwt, v_workspace_id => v_ws_id_my_cbk_ret, v_position => v_pos_kwt, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Malaysia CBK Retail Context');
pack_log.log_write('I','F',v_function,'Step 14','Malaysia CBK Retail created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_my_cbk_whs from workspaces where name like 'MALAYSIA_CBK';
pack_log.log_write('I','F',v_function,'Step 15','Starting to create context Malaysia CBK Wholesale on workspace:' || v_ws_id_my_cbk_whs || ' and position: ' || v_pos_kwt || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_kuwait, v_create_from_position => v_pos_kwt, v_workspace_id => v_ws_id_my_cbk_whs, v_position => v_pos_kwt, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Malaysia CBK Wholesale Context');
pack_log.log_write('I','F',v_function,'Step 16','Malaysia CBK Wholesale created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_cbk_ret from workspaces where name like 'BAHRAIN_CBK_RETAIL';
pack_log.log_write('I','F',v_function,'Step 17','Starting to create context Bahrain CBK Retail on workspace:' || v_ws_id_bah_cbk_ret || ' and position: ' || v_pos_kwt || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_kuwait, v_create_from_position => v_pos_kwt, v_workspace_id => v_ws_id_bah_cbk_ret, v_position => v_pos_kwt, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Bahrain CBK Retail Context');
pack_log.log_write('I','F',v_function,'Step 18','Bahrain CBK Retail created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_cbk_whs from workspaces where name like 'BAHRAIN_CBK';
pack_log.log_write('I','F',v_function,'Step 19','Starting to create context Bahrain CBK Wholesale on workspace:' || v_ws_id_bah_cbk_whs || ' and position: ' || v_pos_kwt || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_kuwait, v_create_from_position => v_pos_kwt, v_workspace_id => v_ws_id_bah_cbk_whs, v_position => v_pos_kwt, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Bahrain CBK Wholesale Context');
pack_log.log_write('I','F',v_function,'Step 20','Bahrain CBK Wholesale created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_my_results from workspaces where name like 'MALAYSIA_RESULTS';
pack_log.log_write('I','F',v_function,'Step 21','Starting to create context Malaysia RESULTS on workspace:' || v_ws_id_my_results || ' and position: ' || v_pos_my_bah || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_malaysia, v_create_from_position => v_pos_my_bah, v_workspace_id => v_ws_id_my_results, v_position => v_pos_my_bah, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Malaysia RESULTS Context');
pack_log.log_write('I','F',v_function,'Step 22','Malaysia RESULTS created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_results from workspaces where name like 'BAHRAIN_RESULTS';
pack_log.log_write('I','F',v_function,'Step 23','Starting to create context Bahrain RESULTS on workspace:' || v_ws_id_bah_results || ' and position: ' || v_pos_my_bah || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_bahrain, v_create_from_position => v_pos_my_bah, v_workspace_id => v_ws_id_bah_results, v_position => v_pos_my_bah, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Bahrain RESULTS Context');
pack_log.log_write('I','F',v_function,'Step 24','Bahrain RESULTS created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_kwt_results from workspaces where name like 'KUWAIT_RESULTS';
pack_log.log_write('I','F',v_function,'Step 25','Starting to create context Kuwait RESULTS on workspace:' || v_ws_id_kwt_results || ' and position: ' || v_pos_kwt || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.context_create(v_reporting_date => v_context_creation_rd, v_create_from_rd => v_golden_context_date, v_create_from_ws => v_golden_ws_kuwait, v_create_from_position => v_pos_kwt, v_workspace_id => v_ws_id_kwt_results, v_position => v_pos_kwt, v_tablespace_data => 'APPDATA', v_tablespace_index => 'APPINDEXES', v_include_import => 'N', v_include_calc => 'N', v_copy_set => v_copy_set_id, v_defer_creation => 'N', v_description => 'Kuwait RESULTS Context');
pack_log.log_write('I','F',v_function,'Step 26','Kuwait RESULTS created and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the context creation script');

end KFH_CREATE_ALL_CONTEXTS;

PROCEDURE KFH_CHECK_ERROR_ALL_CONTEXTS is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_CHECK_ERROR_ALL_CONTEXTS';

v_context_id number;
v_context_rd date;

v_pos_my_bah number;
v_pos_kwt number;

v_ws_id_my_ret number;
v_ws_id_my_whs number;
v_ws_id_bah_ret number;
v_ws_id_bah_whs number;
v_ws_id_kwt_cbk_ret number;
v_ws_id_kwt_cbk_whs number;
v_ws_id_my_cbk_ret number;
v_ws_id_my_cbk_whs number;
v_ws_id_bah_cbk_ret number;
v_ws_id_bah_cbk_whs number;
v_ws_id_my_results number;
v_ws_id_bah_results number;
v_ws_id_kwt_results number;

v_data_context_my_bh number;
v_data_context_kwt number;

begin

select to_date(value,'YYYYMMDD') into v_context_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';

select position into v_pos_my_bah from position where description like 'IFRS9 - UAT';
select position into v_pos_kwt from position where description like 'Final - Adjusted Maturities';

select workspace_id into v_ws_id_my_ret from workspaces where name like 'MALAYSIA_RETAIL';
select workspace_id into v_ws_id_kwt_cbk_ret from workspaces where name like 'KUWAIT_RETAIL';

select context_id into v_data_context_my_bh from contexts where reporting_date = v_context_rd and workspace_id = v_ws_id_my_ret and position = v_pos_my_bah; -- T_CDR data loaded on MALAYSIA/BAHRAIN position (101)
select context_id into v_data_context_kwt from contexts where reporting_date = v_context_rd and workspace_id = v_ws_id_kwt_cbk_ret and position = v_pos_kwt;

IF v_context_rd IS NOT NULL THEN

pack_log.log_write('I','F',v_function,'Step 0','Starting Check Error on all Contexts with RD: ' || v_context_rd || ', MY/BAH position: ' || v_pos_my_bah || ', KWT position: ' || v_pos_kwt || '.');

curr_time:=sysdate;
select workspace_id into v_ws_id_my_ret from workspaces where name like 'MALAYSIA_RETAIL';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_my_ret and position = v_pos_my_bah and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 1','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 2','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_my_whs from workspaces where name like 'MALAYSIA';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_my_whs and position = v_pos_my_bah and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 3','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 4','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_ret from workspaces where name like 'BAHRAIN_RETAIL';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_bah_ret and position = v_pos_my_bah and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 5','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 6','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_whs from workspaces where name like 'BAHRAIN';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_bah_whs and position = v_pos_my_bah and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 7','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 8','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_kwt_cbk_ret from workspaces where name like 'KUWAIT_RETAIL';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_kwt_cbk_ret and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 9','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 10','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_kwt_cbk_whs from workspaces where name like 'KUWAIT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_kwt_cbk_whs and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 11','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 12','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_my_cbk_ret from workspaces where name like 'MALAYSIA_CBK_RETAIL';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_my_cbk_ret and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 13','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 14','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_my_cbk_whs from workspaces where name like 'MALAYSIA_CBK';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_my_cbk_whs and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 15','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 16','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_cbk_ret from workspaces where name like 'BAHRAIN_CBK_RETAIL';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_bah_cbk_ret and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 17','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 18','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_cbk_whs from workspaces where name like 'BAHRAIN_CBK';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_bah_cbk_whs and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 19','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 20','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_my_results from workspaces where name like 'MALAYSIA_RESULTS';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_my_results and position = v_pos_my_bah and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 21','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 22','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_results from workspaces where name like 'BAHRAIN_RESULTS';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_bah_results and position = v_pos_my_bah and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 23','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 24','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_kwt_results from workspaces where name like 'KUWAIT_RESULTS';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_kwt_results and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 25','Starting Check Error on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks O');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 26','Check Error completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_CHECK_ERROR_ALL_CONTEXTS;


procedure KFH_TABLE_DATA_COPY(v_table IN VARCHAR2, v_src_context IN NUMBER, v_dest_context IN NUMBER, v_identifier_column IN VARCHAR2 default NULL,v_identifier_text IN VARCHAR2 default NULL)
is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_TABLE_DATA_COPY';
v_step varchar2(10);
v_update_table varchar2(50);
v_updated_records number(10);
v_pk_rd_ws varchar2(20);
v_sql varchar2(1000);
v_temp_table varchar2(50);
v_temp_count number(10);
begin
  V_STEP := 'Step 1';
  pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

  v_temp_table := v_table || '_' || v_dest_context || '_TMP';
  pack_context.contextid_open(v_dest_context);
  v_sql := 'create table ' || v_temp_table || ' as select * from ' || v_table;
  execute immediate v_sql;
  v_sql := 'select count(1) from ' || v_temp_table ;
  execute immediate v_sql into v_temp_count;


  v_step := 'Step 2';
  pack_log.log_write('I', 'F',v_function, v_step, 'Temp table: '|| v_temp_table || ' created with ' || v_temp_count || ' records, to store existing data of destination context: '|| v_dest_context );

  pack_context.context_copy_table(v_table_name => v_table, v_dest_context_id =>v_dest_context, v_src_context_id=>v_src_context); -- it truncates destination and puts source data

  V_STEP := 'Step 3';
  pack_log.log_write('I', 'F',v_function, v_step, v_table || ' data copied from source context '|| v_src_context || ' to target context '||v_dest_context );
  
 pack_context.contextid_open(v_dest_context);
  -- fast_update
  v_updated_records := fast_update_from( auxiliary_tabname => v_update_table, table_to_update => v_table
  , update_cols => v_identifier_column, from_sql =>
  'select t.rowid as rowid_, ''' || v_identifier_text || ''' as ' || v_identifier_column || ' FROM ' || v_table || ' t'
  ,key_cols => 'rowid_');
  commit;
  
  V_STEP := 'Step 4';
  pack_log.log_write('I', 'F',v_function, v_step, v_table || '.' || v_identifier_column || ' updated for ' || nvl(v_updated_records,0) || ' rows copied from source context' );
  
  v_sql := 'insert into ' || v_table || ' select * from ' || v_temp_table || '';
  execute immediate v_sql;
  
  V_STEP := 'Step 5';
  pack_log.log_write('I', 'F',v_function, v_step, v_table || ' data copied from temp table '|| v_temp_table || ' to target table of target context '|| v_dest_context);
    
  v_sql := 'drop table ' || v_temp_table;
  execute immediate v_sql;
  
  V_STEP := 'Step 6';
  pack_log.log_write('I', 'F',v_function, v_step, 'Temp table: ' || v_temp_table || ' dropped');
    

  V_STEP := 'Step End';
  pack_log.log_write('I','F',v_function,v_step,v_function || ' procedure ends');
  
  
EXCEPTION
    WHEN OTHERS THEN
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || sqlcode || ' :: ' || sqlerrm ||' at ' || to_char(sysdate,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_TABLE_DATA_COPY;

PROCEDURE KFH_TCDR_FOR_SUBS_CBK is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_TCDR_FOR_SUBS_CBK';

v_table_name varchar2(50):='T_CDR';
v_identity_column varchar2(50):='GENERIC_FIELD9';
v_text_prefix varchar2(1000):='Copied from Data context: ';

v_context_id number;
v_context_rd date;

v_pos_kwt number;

v_ws_id_kwt_cbk_whs number;
v_ws_id_my_cbk_whs number;
v_ws_id_bah_cbk_whs number;

v_data_context_kwt number;

begin

select to_date(value,'YYYYMMDD') into v_context_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';

select position into v_pos_kwt from position where description like 'Final - Adjusted Maturities';

select workspace_id into v_ws_id_kwt_cbk_whs from workspaces where name like 'KUWAIT';

select context_id into v_data_context_kwt from contexts where reporting_date = v_context_rd and workspace_id = v_ws_id_kwt_cbk_whs and position = v_pos_kwt;

IF v_context_rd IS NOT NULL THEN

pack_log.log_write('I','F',v_function,'Step 0','Starting T_CDR data copy from KUWAIT Wholesale CBK Context to BAHRAIN and MALAYSIA Wholesale CBK Contexts for RD: ' || v_context_rd || '.');

curr_time:=sysdate;
select workspace_id into v_ws_id_my_cbk_whs from workspaces where name like 'MALAYSIA_CBK';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_my_cbk_whs and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 1','Starting ' || v_table_name || ' data copy on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_name, v_src_context =>v_data_context_kwt, v_dest_context =>v_context_id, v_identifier_column =>v_identity_column,v_identifier_text =>v_text_prefix||v_data_context_kwt);
pack_batch.recheck_table(v_table_name);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 2',v_table_name || ' data copy completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_cbk_whs from workspaces where name like 'BAHRAIN_CBK';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_bah_cbk_whs and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 3','Starting ' || v_table_name || ' data copy on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_name, v_src_context =>v_data_context_kwt, v_dest_context =>v_context_id, v_identifier_column =>v_identity_column,v_identifier_text =>v_text_prefix||v_data_context_kwt);
pack_batch.recheck_table(v_table_name);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 4',v_table_name || ' data copy completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_TCDR_FOR_SUBS_CBK;

PROCEDURE KFH_TCDR_FOR_LOCAL is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_TCDR_FOR_LOCAL';

v_table_name varchar2(50):='T_CDR';
v_identity_column varchar2(50):='GENERIC_FIELD9';
v_text_prefix varchar2(1000):='Copied from Data context: ';

v_context_id number;
v_context_rd date;

v_pos_my_bah number;
v_pos_kwt number;

v_ws_id_my_whs number;
v_ws_id_bah_whs number;
v_ws_id_my_results number;
v_ws_id_bah_results number;

v_data_context_my_bh number;

begin

select to_date(value,'YYYYMMDD') into v_context_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';

select position into v_pos_my_bah from position where description like 'IFRS9 - UAT';
select position into v_pos_kwt from position where description like 'Final - Adjusted Maturities';

select workspace_id into v_ws_id_my_whs from workspaces where name like 'MALAYSIA';

select context_id into v_data_context_my_bh from contexts where reporting_date = v_context_rd and workspace_id = v_ws_id_my_whs and position = v_pos_my_bah; -- T_CDR data loaded on MALAYSIA/BAHRAIN position (101)

IF v_context_rd IS NOT NULL THEN

pack_log.log_write('I','F',v_function,'Step 0','Starting T_CDR data copy from MALAYSIA Wholesale LOCAL Context to BAHRAIN Wholesale LOCAL Context and BOTH RESULTS Contexts with RD: ' || v_context_rd || ', MY/BAH position: ' || v_pos_my_bah || '.');

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_whs from workspaces where name like 'BAHRAIN';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_bah_whs and position = v_pos_my_bah and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 1','Starting ' || v_table_name || ' data copy on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_name, v_src_context =>v_data_context_my_bh, v_dest_context =>v_context_id, v_identifier_column =>v_identity_column,v_identifier_text =>v_text_prefix||v_data_context_my_bh);
pack_batch.recheck_table(v_table_name);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 2',v_table_name || ' data copy completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_my_results from workspaces where name like 'MALAYSIA_RESULTS';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_my_results and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 3','Starting ' || v_table_name || ' data copy on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_name, v_src_context =>v_data_context_my_bh, v_dest_context =>v_context_id, v_identifier_column =>v_identity_column,v_identifier_text =>v_text_prefix||v_data_context_my_bh);
pack_batch.recheck_table(v_table_name);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 4',v_table_name || ' data copy completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
select workspace_id into v_ws_id_bah_results from workspaces where name like 'BAHRAIN_RESULTS';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_bah_results and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 5','Starting ' || v_table_name || ' data copy on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_name, v_src_context =>v_data_context_my_bh, v_dest_context =>v_context_id, v_identifier_column =>v_identity_column,v_identifier_text =>v_text_prefix||v_data_context_my_bh);
pack_batch.recheck_table(v_table_name);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 6',v_table_name || ' data copy completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_TCDR_FOR_LOCAL;

PROCEDURE KFH_TCDR_FOR_KWT_CBK is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_TCDR_FOR_KWT_CBK';

v_table_name varchar2(50):='T_CDR';
v_identity_column varchar2(50):='GENERIC_FIELD9';
v_text_prefix varchar2(1000):='Copied from Data context: ';

v_context_id number;
v_context_rd date;

v_pos_kwt number;

v_ws_id_kwt_cbk_whs number;
v_ws_id_kwt_results number;

v_data_context_kwt number;

begin

select to_date(value,'YYYYMMDD') into v_context_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';

select position into v_pos_kwt from position where description like 'Final - Adjusted Maturities';

select workspace_id into v_ws_id_kwt_cbk_whs from workspaces where name like 'KUWAIT';

select context_id into v_data_context_kwt from contexts where reporting_date = v_context_rd and workspace_id = v_ws_id_kwt_cbk_whs and position = v_pos_kwt;

IF v_context_rd IS NOT NULL THEN

pack_log.log_write('I','F',v_function,'Step 0','Starting T_CDR data copy from KUWAIT Wholesale Context to KUWAIT RESULTS Context with RD: ' || v_context_rd || ', KWT position: ' || v_pos_kwt || '.');

curr_time:=sysdate;
select workspace_id into v_ws_id_kwt_cbk_whs from workspaces where name like 'KUWAIT_RESULTS';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_kwt_cbk_whs and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 1','Starting ' || v_table_name || ' data copy on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_name, v_src_context =>v_data_context_kwt, v_dest_context =>v_context_id, v_identifier_column =>v_identity_column,v_identifier_text =>v_text_prefix||v_data_context_kwt);
pack_batch.recheck_table(v_table_name);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 2',v_table_name || ' data copy completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_TCDR_FOR_KWT_CBK;

PROCEDURE KFH_CHECK_ERROR_CBK_DATA is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_CHECK_ERROR_CBK_DATA';

v_context_id number;
v_context_rd date;

v_pos_kwt number;

v_ws_id_kwt_cbk_whs number;

begin
select to_date(value,'YYYYMMDD') into v_context_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';

IF v_context_rd IS NOT NULL THEN

select position into v_pos_kwt from position where description like 'Final - Adjusted Maturities';
pack_log.log_write('I','F',v_function,'Step 0','Starting Check Error on DATA tables on CBK position with RD: ' || v_context_rd || ', CBK position: ' || v_pos_kwt || '.');

curr_time:=sysdate;
select workspace_id into v_ws_id_kwt_cbk_whs from workspaces where name like 'KUWAIT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_kwt_cbk_whs and position = v_pos_kwt and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 1','Starting Check Error on DATA tables on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks D');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 2','Check Error completed on DATA tables on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_CHECK_ERROR_CBK_DATA;

PROCEDURE KFH_CHECK_ERROR_LOCAL_DATA is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_CHECK_ERROR_LOCAL_DATA';

v_context_id number;
v_context_rd date;

v_pos_my_bah number;

v_ws_id_my_whs number;

begin
select to_date(value,'YYYYMMDD') into v_context_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';

IF v_context_rd IS NOT NULL THEN

select position into v_pos_my_bah from position where description like 'IFRS9 - UAT';
pack_log.log_write('I','F',v_function,'Step 0','Starting Check Error on DATA tables on LOCAL position with RD: ' || v_context_rd || ', MY/BAH position: ' || v_pos_my_bah || '.');

curr_time:=sysdate;
select workspace_id into v_ws_id_my_whs from workspaces where name like 'MALAYSIA';
select context_id into v_context_id from contexts where workspace_id = v_ws_id_my_whs and position = v_pos_my_bah and reporting_date = v_context_rd;
pack_log.log_write('I','F',v_function,'Step 1','Starting Check Error on DATA tables on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_context_id);
PACK_BATCH.GLOBAL_RECHECK_IMPORT(v_import_set=>'RCO Checks D');
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 2','Check Error completed on DATA tables on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_CHECK_ERROR_LOCAL_DATA;
  
procedure KFH_DM_RCO(v_context_id in number, v_mapping_ids in varchar2 default null, v_alm_process_name in varchar2 default null)
is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_DM_RCO';
v_user_id number;

begin

pack_log.log_write('I','F',v_function,'Step I','Parameters: Context ID: ' || v_context_id || ', Mapping IDs: ' || v_mapping_ids || ' and RCO: ' || v_alm_process_name || '.' );

select user_id into v_user_id from cd_users where user_name like 'SAU_USER1';

IF v_context_id IS NOT NULL THEN
pack_log.log_write('I','F',v_function,'Step 0','Starting Deal Mapping on Context ID: ' || v_context_id || '.');
curr_time:=sysdate;

delete from tokens;
commit;

PACK_INSTAll.SET_user_id(v_user_id);
pack_context.contextid_open(v_context_id);

if v_mapping_ids is not null then
pack_fts.p_procLaunchRCO(v_process_setting_name=>'mappingRule',v_wait=>'Y',
v_mainClass=>'com.moodys.alm.test.ExecutableTest -type dealMapping -runOnGrid true -ignoreDBConfig false  -gridURL FZVTMODAP05:2199 -exitCodeMode true -mappingRuleIds ' || v_mapping_ids || ' -context ' || pack_install.get_context_id, 
v_jarName=>'test-6.2.0-SNAPSHOT.jar',v_execDir=>'RCO/ray-integration',
v_sparkConfigScope=>'RCO',wait_completion=>'Y',user_id=>v_user_id,context_id=> v_context_id
);
dbms_lock.sleep(10);
pack_log.log_write('I','F',v_function,'Step 2','Deal Mapping completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));
end if;

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 2.1','Post Deal Mapping (if executed) and pre RCO on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));

if v_alm_process_name is not null then
curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 3','Starting ALM run on Context: ' || v_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_fts.p_procLaunchRCo(v_PROCESS_SETTING_NAME =>v_alm_process_name, v_wait => 'Y', v_mainClass => 'com.moodys.alm.test.ExecutableTest -type runAnalysis -runOnGrid true -wfCode RAY -ignoreDBConfig false -gridURL FZVTMODAP05:2199 -exitCodeMode true -context ' || pack_install.get_context_id || ' -test "' || v_alm_process_name || '"', v_jarName => 'test-6.2.0-SNAPSHOT.jar', v_execDir => 'RCO/ray-integration', v_sparkConfigScope=>'RCO', wait_completion => 'Y');
dbms_lock.sleep(10);
pack_log.log_write('I','F',v_function,'Step 4','ALM run completed on Context: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));
end if;


END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO;

procedure KFH_DM_RCO_MALAYSIA_RETAIL is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_MALAYSIA_RETAIL';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'MALAYSIA_RETAIL';
select position into v_pos_id from position where description like 'IFRS9 - UAT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'141:344');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KFH_IFRS9_FULL_RUN (Time_Bucket Retail)');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_MALAYSIA_RETAIL;

procedure KFH_DM_RCO_MALAYSIA_WHOLESALE is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_MALAYSIA_WHOLESALE';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'MALAYSIA';
select position into v_pos_id from position where description like 'IFRS9 - UAT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'142:346:281');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KFH_IFRS9_FULL_RUN (Non-GCORR WS)');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

V_STEP := 'Step 6';
pack_log.log_write('I','F',v_function,v_step, 'Starting second RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KFH_IFRS9_FULL_RUN (GCORR WS Final)');
V_STEP := 'Step 7';
pack_log.log_write('I','F',v_function,v_step, 'Second RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_MALAYSIA_WHOLESALE;

procedure KFH_DM_RCO_BAHRAIN_RETAIL is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_BAHRAIN_RETAIL';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'BAHRAIN_RETAIL';
select position into v_pos_id from position where description like 'IFRS9 - UAT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'141:142');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KFH_IFRS9_FULL_RUN (Time bucket for Retail)');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_BAHRAIN_RETAIL;

procedure KFH_DM_RCO_BAHRAIN_WHOLESALE is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_BAHRAIN_WHOLESALE';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'BAHRAIN';
select position into v_pos_id from position where description like 'IFRS9 - UAT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'61:81:142');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KFH_IFRS9_WS_EXCL_GCORR');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

V_STEP := 'Step 6';
pack_log.log_write('I','F',v_function,v_step, 'Starting second RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KFH_IFRS9_WS_GCORR');
V_STEP := 'Step 7';
pack_log.log_write('I','F',v_function,v_step, 'Second RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_BAHRAIN_WHOLESALE;

procedure KFH_DM_RCO_KUWAIT_RETAIL is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_KUWAIT_RETAIL';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'KUWAIT_RETAIL';
select position into v_pos_id from position where description like 'Final - Adjusted Maturities';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'246:247');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KFH_IFRS9_FULL (Time Bucket for KWT RETAIL)');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));
end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_KUWAIT_RETAIL;

procedure KFH_DM_RCO_KUWAIT_WHOLESALE is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_KUWAIT_WHOLESALE';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'KUWAIT';
select position into v_pos_id from position where description like 'Final - Adjusted Maturities';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'61:81:247');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KWT_1_KFH_IFRS9_FULL (WholeSale Pre-PWS)');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

V_STEP := 'Step 6';
pack_log.log_write('I','F',v_function,v_step, 'Starting second RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KWT_2_KFH_IFRS9_FULL (Final WS Flooring) (PWS)');
V_STEP := 'Step 7';
pack_log.log_write('I','F',v_function,v_step, 'Second RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_KUWAIT_WHOLESALE;

procedure KFH_DM_RCO_MALAYSIA_CBK_RETAIL is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_MALAYSIA_CBK_RETAIL';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'MALAYSIA_CBK_RETAIL';
select position into v_pos_id from position where description like 'Final - Adjusted Maturities';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'373:375');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KFH_IFRS9_FULL (Time Bucket for KWT RETAIL)');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_MALAYSIA_CBK_RETAIL;

procedure KFH_DM_RCO_MALAYSIA_CBK_WS is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_MALAYSIA_CBK_WS';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'MALAYSIA_CBK';
select position into v_pos_id from position where description like 'Final - Adjusted Maturities';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'376:378:379');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'1KFH_IFRS9_FULL_NON_GCORR_RUN1 (WholeSale Pre-PWS)');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

V_STEP := 'Step 6';
pack_log.log_write('I','F',v_function,v_step, 'Starting second RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'2KFH_IFRS9_FULL_GCORR_RUN2 (WholeSale Pre-PWS)');
V_STEP := 'Step 7';
pack_log.log_write('I','F',v_function,v_step, 'Second RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


V_STEP := 'Step 8';
pack_log.log_write('I','F',v_function,v_step, 'Starting third RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'3KFH_IFRS9_FULL_NON_GCORR_RUN3 (WS Final PWS)');
V_STEP := 'Step 9';
pack_log.log_write('I','F',v_function,v_step, 'Third RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_MALAYSIA_CBK_WS;

procedure KFH_DM_RCO_BAHRAIN_CBK_RETAIL is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_BAHRAIN_CBK_RETAIL';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'BAHRAIN_CBK_RETAIL';
select position into v_pos_id from position where description like 'Final - Adjusted Maturities';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'368:369');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'KFH_IFRS9_FULL (Time Bucket for KWT RETAIL)');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_BAHRAIN_CBK_RETAIL;

procedure KFH_DM_RCO_BAHRAIN_CBK_WS is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_DM_RCO_BAHRAIN_CBK_WS';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'BAHRAIN_CBK';
select position into v_pos_id from position where description like 'Final - Adjusted Maturities';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting DM for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_mapping_ids=>'370:371:372');
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'Starting first RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'1KFH_IFRS9_FULL_NON_GCORR_RUN1 (WholeSale Pre-PWS)');
V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'First RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

V_STEP := 'Step 6';
pack_log.log_write('I','F',v_function,v_step, 'Starting second RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'2KFH_IFRS9_FULL_GCORR_RUN2 (WholeSale Pre-PWS)');
V_STEP := 'Step 7';
pack_log.log_write('I','F',v_function,v_step, 'Second RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


V_STEP := 'Step 8';
pack_log.log_write('I','F',v_function,v_step, 'Starting third RCO for Context ID: ' || v_context_id);
curr_time:= sysdate;
PACK_KFH_BATCH.KFH_DM_RCO(v_context_id=>v_context_id, v_alm_process_name=>'3KFH_IFRS9_FULL_NON_GCORR_RUN3 (WS Final PWS)');
V_STEP := 'Step 9';
pack_log.log_write('I','F',v_function,v_step, 'Third RCO completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_DM_RCO_BAHRAIN_CBK_WS;

procedure KFH_SAE_BAHRAIN_RETAIL is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_SAE_BAHRAIN_RETAIL';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;
x number;
wf_id number;
sae_status varchar2(50);
process_id number;
c number;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'BAHRAIN_RETAIL';
select position into v_pos_id from position where description like 'IFRS9 - UAT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting SAE for Context ID: ' || v_context_id);
curr_time:= sysdate;
pack_context.contextid_open(v_context_id);
select max(workflow_id) into wf_id from workflow where description like 'SAE_BAHRAIN_RETAIL';
if wf_id is not null then
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'Starting WF:' || wf_id || ' for SAE for Context ID: ' || v_context_id);
x:=pack_fts_client.launch_workflow(v_workflow_id=>wf_id);
select id into process_id from sae_proc_setting where name like 'Bahrain PD Projection Workflow' and active = 'Y';
V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'WF:' || wf_id || ' for SAE started with process ID: ' || process_id || ' for Context ID: ' || v_context_id);

--loop
--dbms_lock.sleep(60);
--select status into sae_status from sae_process where proc_setting_id = process_id;
--pack_log.log_write('I','F',v_function,'Counter '|| c, 'SAE process ID: ' || process_id || ' sae_status: ' || sae_status);
--c:=c+1;
--exit when upper(sae_status) <> 'STARTED';
--dbms_lock.sleep(300);
--end loop;

end if;

V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'SAE completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_SAE_BAHRAIN_RETAIL;

procedure KFH_SAE_MALAYSIA_RETAIL is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_SAE_MALAYSIA_RETAIL';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;
x number;
wf_id number;
sae_status varchar2(50);
process_id number;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'MALAYSIA_RETAIL';
select position into v_pos_id from position where description like 'IFRS9 - UAT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting SAE for Context ID: ' || v_context_id);
curr_time:= sysdate;
pack_context.contextid_open(v_context_id);
select max(workflow_id) into wf_id from workflow where description like 'SAE_MALAYSIA_RETAIL';
if wf_id is not null then
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'Starting WF:' || wf_id || ' for SAE for Context ID: ' || v_context_id);
x:=pack_fts_client.launch_workflow(v_workflow_id=>wf_id);
select id into process_id from sae_proc_setting where name like 'KFH_MALAYSIA_FULL_RUN' and active = 'Y';
V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'WF:' || wf_id || ' for SAE started with process ID: ' || process_id || ' for Context ID: ' || v_context_id);
loop
dbms_lock.sleep(60);
select status into sae_status from sae_process where proc_setting_id = process_id;
exit when upper(sae_status) <> 'STARTED';
dbms_lock.sleep(10);
end loop;
end if;

V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'SAE completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_SAE_MALAYSIA_RETAIL;

procedure KFH_SAE_KUWAIT_RETAIL_PF is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_SAE_KUWAIT_RETAIL_PF';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;
x number;
wf_id number;
sae_status varchar2(50);
process_id number;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'KUWAIT_RETAIL';
select position into v_pos_id from position where description like 'Final - Adjusted Maturities';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting SAE for Context ID: ' || v_context_id);
curr_time:= sysdate;
pack_context.contextid_open(v_context_id);
select max(workflow_id) into wf_id from workflow where description like 'SAE_KUWAIT_RETAIL_PF';
if wf_id is not null then
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'Starting WF:' || wf_id || ' for SAE for Context ID: ' || v_context_id);
x:=pack_fts_client.launch_workflow(v_workflow_id=>wf_id);
select id into process_id from sae_proc_setting where name like 'Kuwait Personal Finance ' and active = 'Y';
V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'WF:' || wf_id || ' for SAE started with process ID: ' || process_id || ' for Context ID: ' || v_context_id);
loop
dbms_lock.sleep(60);
select status into sae_status from sae_process where proc_setting_id = process_id;
exit when upper(sae_status) <> 'STARTED';
dbms_lock.sleep(10);
end loop;
end if;

V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'SAE completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_SAE_KUWAIT_RETAIL_PF;

procedure KFH_SAE_KUWAIT_RETAIL_CC is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_SAE_KUWAIT_RETAIL_CC';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;
x number;
wf_id number;
sae_status varchar2(50);
process_id number;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'KUWAIT_RETAIL';
select position into v_pos_id from position where description like 'Final - Adjusted Maturities';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting SAE for Context ID: ' || v_context_id);
curr_time:= sysdate;
pack_context.contextid_open(v_context_id);
select max(workflow_id) into wf_id from workflow where description like 'SAE_KUWAIT_RETAIL_CC';
if wf_id is not null then
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'Starting WF:' || wf_id || ' for SAE for Context ID: ' || v_context_id);
x:=pack_fts_client.launch_workflow(v_workflow_id=>wf_id);
select id into process_id from sae_proc_setting where name like 'Kuwait Credit Card PD Projection Workflow v0.1' and active = 'Y';
V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'WF:' || wf_id || ' for SAE started with process ID: ' || process_id || ' for Context ID: ' || v_context_id);
loop
dbms_lock.sleep(60);
select status into sae_status from sae_process where proc_setting_id = process_id;
exit when upper(sae_status) <> 'STARTED';
dbms_lock.sleep(10);
end loop;
end if;

V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'SAE completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_SAE_KUWAIT_RETAIL_CC;

procedure KFH_SAE_BAHRAIN_CBK_RETAIL is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_SAE_BAHRAIN_CBK_RETAIL';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;
x number;
wf_id number;
sae_status varchar2(50);
process_id number;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'BAHRAIN_CBK_RETAIL';
select position into v_pos_id from position where description like 'IFRS9 - UAT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting SAE for Context ID: ' || v_context_id);
curr_time:= sysdate;
pack_context.contextid_open(v_context_id);
select max(workflow_id) into wf_id from workflow where description like 'SAE_BAHRAIN_CBK_RETAIL';
if wf_id is not null then
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'Starting WF:' || wf_id || ' for SAE for Context ID: ' || v_context_id);
x:=pack_fts_client.launch_workflow(v_workflow_id=>wf_id);
select id into process_id from sae_proc_setting where name like 'Bahrain PD Projection Workflow' and active = 'Y';
V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'WF:' || wf_id || ' for SAE started with process ID: ' || process_id || ' for Context ID: ' || v_context_id);
loop
dbms_lock.sleep(60);
select status into sae_status from sae_process where proc_setting_id = process_id;
exit when upper(sae_status) <> 'STARTED';
dbms_lock.sleep(10);
end loop;
end if;

V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'SAE completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_SAE_BAHRAIN_CBK_RETAIL;

procedure KFH_SAE_MALAYSIA_CBK_RETAIL is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_SAE_MALAYSIA_CBK_RETAIL';
v_step varchar2(20);
v_rd_id date;
v_ws_id number;
v_pos_id number;
v_context_id number;
curr_time date;
x number;
wf_id number;
sae_status varchar2(50);
process_id number;

begin
V_STEP := 'Step 1';
pack_log.log_write('I','F',v_function,v_step, v_function ||' procedure starts');

select to_date(value,'YYYYMMDD') into v_rd_id from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';
IF v_rd_id IS NOT NULL THEN
select workspace_id into v_ws_id from workspaces where name like 'MALAYSIA_CBK_RETAIL';
select position into v_pos_id from position where description like 'IFRS9 - UAT';
select context_id into v_context_id from contexts where workspace_id = v_ws_id and position = v_pos_id and reporting_date = v_rd_id;

V_STEP := 'Step 2';
pack_log.log_write('I','F',v_function,v_step, 'Starting SAE for Context ID: ' || v_context_id);
curr_time:= sysdate;
pack_context.contextid_open(v_context_id);
select max(workflow_id) into wf_id from workflow where description like 'SAE_MALAYSIA_CBK_RETAIL';
if wf_id is not null then
V_STEP := 'Step 3';
pack_log.log_write('I','F',v_function,v_step, 'Starting WF:' || wf_id || ' for SAE for Context ID: ' || v_context_id);
x:=pack_fts_client.launch_workflow(v_workflow_id=>wf_id);
select id into process_id from sae_proc_setting where name like 'KFH_MALAYSIA_FULL_RUN' and active = 'Y';
V_STEP := 'Step 4';
pack_log.log_write('I','F',v_function,v_step, 'WF:' || wf_id || ' for SAE started with process ID: ' || process_id || ' for Context ID: ' || v_context_id);
loop
dbms_lock.sleep(60);
select status into sae_status from sae_process where proc_setting_id = process_id;
exit when upper(sae_status) <> 'STARTED';
dbms_lock.sleep(10);
end loop;
end if;

V_STEP := 'Step 5';
pack_log.log_write('I','F',v_function,v_step, 'SAE completed for Context ID: ' || v_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end if;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress');

end KFH_SAE_MALAYSIA_CBK_RETAIL;


procedure KFH_SAE_DM_RCO_BAH_RET is
v_function varchar2(100) := 'PACK_KFH_BATCH.KFH_SAE_DM_RCO_BAH_RET';
v_step varchar2(20);
curr_time date;
process_id number;
sae_status varchar2(50);
c number:=1;
v_sql varchar2(2000);

begin
v_step:='Step 1';
pack_log.log_write('I','F',v_function,v_step, 'started at ' || to_char(sysdate,'dd-mon-yyyy hh24:mi:ss'));
curr_time:= sysdate;
pack_kfh_batch.KFH_SAE_BAHRAIN_RETAIL();

select id into process_id from sae_proc_setting where name like 'Bahrain PD Projection Workflow' and active = 'Y';
v_step:='Step 1.1';
pack_log.log_write('I','F',v_function,v_step, 'SAE process ID is: ' || process_id || ' and time taken to reach this step is ' || round(((sysdate-curr_time)*100000/60),2));

dbms_lock.sleep(10);
loop
pack_log.log_write('I','F',v_function,'Cnt:'|| c, 'Enter loop - SAE process ID: ' || process_id || ' sae_status: ' || sae_status);
select status into sae_status from sae_process where proc_setting_id = process_id;

pack_log.log_write('I','F',v_function,'Cnt: '|| c, 'SAE process ID: ' || process_id || ' sae_status: ' || sae_status);
commit;
c:=c+1;
exit when upper(sae_status) = 'COMPLETED';
dbms_lock.sleep(10);
end loop;

v_step:='Step 2';
pack_log.log_write('I','F',v_function,v_step, 'sae completed and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:= sysdate;
pack_kfh_batch.KFH_DM_RCO_BAHRAIN_RETAIL();
v_step:='Step 3';
pack_log.log_write('I','F',v_function,v_step, 'DM RCO completed and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

end KFH_SAE_DM_RCO_BAH_RET;

PROCEDURE KFH_MALAYSIA_RESULTS is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_MALAYSIA_RESULTS';

v_table_1_name varchar2(50):='T_ALM_ANALYTICS';
v_table_1_column varchar2(50):='';

v_table_2_name varchar2(50):='DEAL_RESULT';
v_table_2_column varchar2(50):='';

v_table_3_name varchar2(50):='DIMENSION_RESULT';
v_table_3_column varchar2(50):='';

v_ws_id_my_ret number;
v_ws_id_my_whs number;
v_ws_id_my_results number;

v_pos_local number;

v_context_rd date;
v_dest_context_id number;

v_src_context_my_whs number;
v_src_context_my_ret number;

v_delete_sql varchar2(1000);

begin

select to_date(value,'YYYYMMDD') into v_context_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';

select position into v_pos_local from position where description like 'IFRS9 - UAT';
select workspace_id into v_ws_id_my_results from workspaces where name like 'MALAYSIA_RESULTS';
select context_id into v_dest_context_id from contexts where reporting_date = v_context_rd and workspace_id = v_ws_id_my_results and position = v_pos_local;

pack_context.contextid_open(v_dest_context_id);
v_delete_sql := 'delete from ' || v_table_1_name;
execute immediate v_delete_sql;
v_delete_sql := 'delete from ' || v_table_2_name;
execute immediate v_delete_sql;
v_delete_sql := 'delete from ' || v_table_3_name;
execute immediate v_delete_sql;

IF v_context_rd IS NOT NULL THEN

pack_log.log_write('I','F',v_function,'Step 0','Starting data copy on Malaysia Results Context ID: ' || v_dest_context_id || ' with RD: ' || v_context_rd || ', WS: ' || v_ws_id_my_results || ', pos: ' || v_pos_local || '.');
select workspace_id into v_ws_id_my_whs from workspaces where name like 'MALAYSIA';
select context_id into v_src_context_my_whs from contexts where workspace_id = v_ws_id_my_whs and position = v_pos_local and reporting_date = v_context_rd;

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 1',v_table_1_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_1_name, v_src_context =>v_src_context_my_whs, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 2',v_table_1_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 3',v_table_2_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_2_name, v_src_context =>v_src_context_my_whs, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 4',v_table_2_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 5',v_table_3_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_3_name, v_src_context =>v_src_context_my_whs, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 6',v_table_3_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


select workspace_id into v_ws_id_my_ret from workspaces where name like 'MALAYSIA_RETAIL';
select context_id into v_src_context_my_ret from contexts where workspace_id = v_ws_id_my_ret and position = v_pos_local and reporting_date = v_context_rd;

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 7',v_table_1_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_1_name, v_src_context =>v_src_context_my_ret, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 8',v_table_1_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 9',v_table_2_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_2_name, v_src_context =>v_src_context_my_ret, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 10',v_table_2_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 11',v_table_3_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_3_name, v_src_context =>v_src_context_my_ret, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 12',v_table_3_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_MALAYSIA_RESULTS;

PROCEDURE KFH_BAHRAIN_RESULTS is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_BAHRAIN_RESULTS';

v_table_1_name varchar2(50):='T_ALM_ANALYTICS';
v_table_1_column varchar2(50):='';

v_table_2_name varchar2(50):='DEAL_RESULT';
v_table_2_column varchar2(50):='';

v_table_3_name varchar2(50):='DIMENSION_RESULT';
v_table_3_column varchar2(50):='';

v_ws_id_bah_ret number;
v_ws_id_bah_whs number;
v_ws_id_bah_results number;

v_pos_local number;

v_context_rd date;
v_dest_context_id number;

v_src_context_bah_whs number;
v_src_context_bah_ret number;

v_delete_sql varchar2(1000);

begin

select to_date(value,'YYYYMMDD') into v_context_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';

select position into v_pos_local from position where description like 'IFRS9 - UAT';
select workspace_id into v_ws_id_bah_results from workspaces where name like 'BAHRAIN_RESULTS';
select context_id into v_dest_context_id from contexts where reporting_date = v_context_rd and workspace_id = v_ws_id_bah_results and position = v_pos_local;

pack_context.contextid_open(v_dest_context_id);
v_delete_sql := 'delete from ' || v_table_1_name;
execute immediate v_delete_sql;
v_delete_sql := 'delete from ' || v_table_2_name;
execute immediate v_delete_sql;
v_delete_sql := 'delete from ' || v_table_3_name;
execute immediate v_delete_sql;

IF v_context_rd IS NOT NULL THEN

pack_log.log_write('I','F',v_function,'Step 0','Starting data copy on Malaysia Results Context ID: ' || v_dest_context_id || ' with RD: ' || v_context_rd || ', WS: ' || v_ws_id_bah_results || ', pos: ' || v_pos_local || '.');
select workspace_id into v_ws_id_bah_whs from workspaces where name like 'BAHRAIN';
select context_id into v_src_context_bah_whs from contexts where workspace_id = v_ws_id_bah_whs and position = v_pos_local and reporting_date = v_context_rd;

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 1',v_table_1_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_1_name, v_src_context =>v_src_context_bah_whs, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 2',v_table_1_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 3',v_table_2_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_2_name, v_src_context =>v_src_context_bah_whs, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 4',v_table_2_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 5',v_table_3_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_3_name, v_src_context =>v_src_context_bah_whs, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 6',v_table_3_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


select workspace_id into v_ws_id_bah_ret from workspaces where name like 'BAHRAIN_RETAIL';
select context_id into v_src_context_bah_ret from contexts where workspace_id = v_ws_id_bah_ret and position = v_pos_local and reporting_date = v_context_rd;

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 7',v_table_1_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_1_name, v_src_context =>v_src_context_bah_ret, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 8',v_table_1_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 9',v_table_2_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_2_name, v_src_context =>v_src_context_bah_ret, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 10',v_table_2_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 11',v_table_3_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_3_name, v_src_context =>v_src_context_bah_ret, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 12',v_table_3_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_BAHRAIN_RESULTS;

PROCEDURE KFH_KUWAIT_RESULTS is
curr_time date;
v_function varchar(100):='PACK_KFH_BATCH.KFH_KUWAIT_RESULTS';

v_table_1_name varchar2(50):='T_ALM_ANALYTICS';
v_table_1_column varchar2(50):='';

v_table_2_name varchar2(50):='DEAL_RESULT';
v_table_2_column varchar2(50):='';

v_table_3_name varchar2(50):='DIMENSION_RESULT';
v_table_3_column varchar2(50):='';

v_ws_id_bah_ret number;
v_ws_id_bah_whs number;
v_ws_id_bah_results number;

v_pos_local number;

v_context_rd date;
v_dest_context_id number;

v_src_context_bah_whs number;
v_src_context_bah_ret number;

v_delete_sql varchar2(1000);

begin

select to_date(value,'YYYYMMDD') into v_context_rd from kfh_custom_parameters where parameter = 'MONTHLY_RUN_DATE';

select position into v_pos_local from position where description like 'IFRS9 - UAT';
select workspace_id into v_ws_id_bah_results from workspaces where name like 'BAHRAIN_RESULTS';
select context_id into v_dest_context_id from contexts where reporting_date = v_context_rd and workspace_id = v_ws_id_bah_results and position = v_pos_local;

pack_context.contextid_open(v_dest_context_id);
v_delete_sql := 'delete from ' || v_table_1_name;
execute immediate v_delete_sql;
v_delete_sql := 'delete from ' || v_table_2_name;
execute immediate v_delete_sql;
v_delete_sql := 'delete from ' || v_table_3_name;
execute immediate v_delete_sql;

IF v_context_rd IS NOT NULL THEN

pack_log.log_write('I','F',v_function,'Step 0','Starting data copy on Malaysia Results Context ID: ' || v_dest_context_id || ' with RD: ' || v_context_rd || ', WS: ' || v_ws_id_bah_results || ', pos: ' || v_pos_local || '.');
select workspace_id into v_ws_id_bah_whs from workspaces where name like 'BAHRAIN';
select context_id into v_src_context_bah_whs from contexts where workspace_id = v_ws_id_bah_whs and position = v_pos_local and reporting_date = v_context_rd;

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 1',v_table_1_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_1_name, v_src_context =>v_src_context_bah_whs, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 2',v_table_1_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 3',v_table_2_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_2_name, v_src_context =>v_src_context_bah_whs, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 4',v_table_2_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 5',v_table_3_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_3_name, v_src_context =>v_src_context_bah_whs, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 6',v_table_3_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));


select workspace_id into v_ws_id_bah_ret from workspaces where name like 'BAHRAIN_RETAIL';
select context_id into v_src_context_bah_ret from contexts where workspace_id = v_ws_id_bah_ret and position = v_pos_local and reporting_date = v_context_rd;

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 7',v_table_1_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_1_name, v_src_context =>v_src_context_bah_ret, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 8',v_table_1_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 9',v_table_2_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_2_name, v_src_context =>v_src_context_bah_ret, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 10',v_table_2_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

curr_time:=sysdate;
pack_log.log_write('I','F',v_function,'Step 11',v_table_3_name || ' data copy started on Context: ' || v_dest_context_id || ' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss'));
pack_context.contextid_open(v_dest_context_id);
KFH_TABLE_DATA_COPY(v_table =>v_table_3_name, v_src_context =>v_src_context_bah_ret, v_dest_context =>v_dest_context_id);
pack_context.contextid_disable();
pack_log.log_write('I','F',v_function,'Step 12',v_table_3_name || ' data copy completed on Context: ' || v_dest_context_id || ' and time taken in minutes is '|| round(((sysdate-curr_time)*100000/60),2));

END IF;

EXCEPTION
    WHEN OTHERS THEN
	curr_time:=sysdate;
	pack_log.log_write('I','F',v_function,'Step -99','Exception occured: ' || SQLCODE || ' :: ' || SQLERRM ||' at ' || to_char(curr_time,'dd-mon-yyyy hh24:mi:ss') || '. Check log_table with function as ' || v_function || ' for progress of the check error script');

end KFH_KUWAIT_RESULTS;

END PACK_KFH_BATCH;