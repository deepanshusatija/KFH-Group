create or replace 
PACKAGE "PACK_SAE_UTILS" as

/***************************************************************************************************************
--This procedure will insert all Scenario Sets flagged as 'Copy on new Context' to the newly created context
-- 1.1 (08/05/2012 by ASA)  passing the source context
***************************************************************************************************************/
PROCEDURE INSERT_FOR_NEW_CONTEXT(v_context_id number, v_src_ctx number default null);

/***************************************************************************************************************
--Updates the scenario contents based on the latest version of the configuration
--This will remove scenario types that are no longer relevant from the param settings, and add the new ones
***************************************************************************************************************/
PROCEDURE UPDATE_SCENARIOS_FROM_CONFIG;

/***************************************************************************************************************
--Operations to be performed before loading a dataset
***************************************************************************************************************/
PROCEDURE BEFORE_LOAD_DATASET;

/***************************************************************************************************************
--Operations to be performed after loading a dataset
***************************************************************************************************************/
PROCEDURE AFTER_LOAD_DATASET;

/***************************************************************************************************************
--Disable SA Locks
***************************************************************************************************************/
PROCEDURE DISABLE_CONFIG_LOCKS;

/***************************************************************************************************************
--Enable SA Locks
***************************************************************************************************************/
PROCEDURE ENABLE_CONFIG_LOCKS;

/***************************************************************************************************************
--Remove uninstalled configuration
***************************************************************************************************************/
PROCEDURE REMOVE_UNINSTALLED_CONFIG;

/***************************************************************************************************
Name          : exec_ddl
Parameters    : ddl_statement_str
Description   : Execute DDL from a procedure.
Revision Log  :
	1. 18/Jul/2012 by Rujing: to fix the insufficient privilege error when SAU user is creating temp table from Java.
***************************************************************************************************/
PROCEDURE exec_ddl(ddl_statement_str in varchar2, table_name in varchar2 default null);

/**************************************************************************************************************
Name          : update_simulation_type
Parameters    : v_scope - scope for update; 'all' or 'single',
				v_param1 - simulation type if scope is 'all', simulation id if scope is 'single'
				v_param2 - target simulation type for update
				v_context_id - context in which change needs to be made, -1 for all contexts
Description   : Update simulation type for simulations.
Revision Log  :
	1. 2/12/2013 Vivek.
**************************************************************************************************************/
PROCEDURE update_simulation_type(v_scope varchar2, v_param1 number, v_param2 number, v_context_id number default -1) ;

/**************************************************************************************************************
Name          : sae_log_write
Parameters    : v_log_type - I=information. W=warning. E=error. Null=E.
                v_application - F=FDW C=cad G=Gem R=rrt E=fce A=alm I=ias B=bis P=bcp M=mkr...
                v_function - Where the error occurs (free text): function, package, pb function,...
                v_step - Where the error occurs in function.
                v_message - Error message. If null, oracle error message (with call stack).
                v_parameters - Additional values or information.
                v_tech_func - 'T' by default.
                v_severity - Log severity. Default value:1.
                v_unused - unused
                v_nummsg - Message id (see MESSAGES table), NULL possible
                v_table_name - When the log is linked to a table name and/or contract reference, it is sent to LOG_TABLE_DEAL (not log_table), NULL possible
                v_contract_reference - When the log is linked to a table name and/or contract reference, it is sent to LOG_TABLE_DEAL (not log_table), NULL possible
                v_log_struct_id - Struct id of the log.
Description   : This procedure writes in log_table or log_table_deal (depending on v_table_name,v_contract_reference).This procedure is called in autonomous transaction.
Revision Log  : 1. 10/3/2013. Amit
***************************************************************************************************************/
PROCEDURE sae_log_write(v_log_type IN VARCHAR2 default null, v_application IN VARCHAR2 default null, v_function IN VARCHAR2 default null, v_step IN VARCHAR2 default null, v_message_string IN VARCHAR2 default null, v_parameters IN VARCHAR2 default null,
v_tech_func IN VARCHAR2 DEFAULT 'T', v_severity IN NUMBER DEFAULT 1, v_unused IN CHAR DEFAULT null, v_nummsg in number default null, v_table_name in varchar2 default null, v_contract_reference in varchar2 default null, v_log_struct_id in number default null, v_context_id in number default null);

/**************************************************************************************************************
Name          : sae_log_end
Parameters    : v_unused - No more used.
                v_update_timestamps - To update start and end timestamp of the log struct to match the first log in the session.
                v_struct_id - Struct id of the log which needs to be ended.
Description   : This procedure allows to end current log session. This procedure is in autonomous transaction.
Revision Log  : 1. 10/3/2013. Amit
***************************************************************************************************************/
PROCEDURE sae_log_end(v_unused in char default null, v_update_timestamps in char default 'N', v_log_struct_id in number,v_context_id in number default null);

/**************************************************************************************************************
Name          : sae_update_counters
Parameters    : v_log_struct_id - Struct id of the log which needs to be ended.
Description   : This procedure is used to update the counters.
Revision Log  : 1. 10/3/2013. Amit
***************************************************************************************************************/
PROCEDURE sae_update_counters(v_log_struct_id in number);

/**************************************************************************************************************
Name          : sae_log_begin
Parameters    : v_log_key - session key. If null, a key is automatically created,
                v_unused - No more used.
                v_task_id - Id of the task, NULL possible
                v_log_desc - Description of the session log, NULL possible
Description   : This procedure starts a log session and fixes the key log_key in global variable v_current_log_key.

                This procedure must be called before starting log of any process.
                Procedure log_end must be imperatively called to end log session.
                This procedure is in autonomous transaction.
Revision Log  : 1. 10/3/2013. Amit
***************************************************************************************************************/
FUNCTION sae_log_begin(v_log_key VARCHAR2,v_unused CHAR DEFAULT null,v_task_id number default null,v_log_desc varchar2 default null,v_job_id number default null, v_parent_log_struct_id number default null,  v_erswf_id in number default null, v_erswf_task_id in number default null) return number;

/***************************************************************************************************************
Name          : check_scenario_type_setup
Parameters    :scenario id, product code
Description   : check whether the given scenario has all the scenario types configured for the product
                returns description
Revision Log  :
- 1.0 (31/May/2012 by TonyC)  Newly Created
***************************************************************************************************************/
FUNCTION check_scenario_type_setup(inp_simulation_id number, inp_product_code VARCHAR2) return VARCHAR2;

/***************************************************************************************************************
Name          : check_scentype_ispopulated
Parameters    :inp_sim_id, inp_scentype_id
Description   : check whether the given scenario has all the scenario type configured
Revision Log  :
- 1.0 (31/May/2012 by TonyC)  Newly Created
***************************************************************************************************************/
FUNCTION check_scentype_ispopulated(inp_sim_id number, inp_scentype_id number) return VARCHAR2;

/*
* check whether a simulation is playable
*/
FUNCTION check_sim_isplayable(inp_sim_id number) return VARCHAR2;

/*
* check whether all scenario types of a simulation are populated with required values
*/
FUNCTION check_sim_ispopulated(inp_sim_id number) return VARCHAR2;

/*
* check a scenario type of a simulation is populated with required values
*/
FUNCTION chk_sim_scentype_inp_populated(inp_sim_id number, inp_scentype_id number) return varchar2;

FUNCTION check_scenset_type_setup(inp_scenset_id number, inp_product_code VARCHAR2) return varchar2;

/*
* Encrypt and Decrypt Secured Parameter
*/
PROCEDURE padstring (p_text  IN OUT  VARCHAR2);

FUNCTION encrypt_paramval (p_text  IN  VARCHAR2) RETURN RAW;

FUNCTION decrypt_paramval (p_raw  IN  RAW) RETURN VARCHAR2;

/***************************************************************************************************************
Name          : enable_parallel_processing
Parameters    : v_cores
Description   : enables session specific parallel DDL, DML but NOT QUERY.
				If parameter value is not provided, 16 CPUs are used
***************************************************************************************************************/

PROCEDURE enable_parallel_processing(v_cores NUMBER DEFAULT NULL);

/***************************************************************************************************************
Name          : enable_parallel_processing
Parameters    : v_cores
Description   : disables any session-specific parallel DDL, DML but NOT QUERY.
***************************************************************************************************************/

PROCEDURE disable_parallel_processing;

/***************************************************************************************************************
Name          : SCENDATA_DVIEW_CREATE
Parameters    : v_scenario_id, v_table_name (optional)
Description   : creates view for "temporary" SAE_SCENARIO_DATA_XYZ calculation table
***************************************************************************************************************/

PROCEDURE SCENDATA_DVIEW_CREATE(v_scenario_id NUMBER, v_table_name VARCHAR2 DEFAULT NULL);

/***************************************************************************************************************
Name          : SCENDATA_DVIEW_CREATE
Parameters    : v_scenario_id, v_table_name (optional)
Description   : drops view for "temporary" SAE_SCENARIO_DATA_XYZ calculation table
***************************************************************************************************************/

PROCEDURE SCENDATA_DVIEW_DROP(v_scenario_id NUMBER, v_table_name VARCHAR2 DEFAULT NULL);

/***************************************************************************************************************
Name          : GATHER_SUBPART_STATS
Parameters    : v_tab_name, v_subpart_col_id
Description   : gathers stats at subpartition level, primarily for SAE subpartitioned tables
***************************************************************************************************************/

PROCEDURE GATHER_SUBPART_STATS(v_tab_name VARCHAR2, v_subpart_col_id NUMBER);

/***************************************************************************************************************
Name          : TAB_SUBPART_CREATE
Parameters    : v_tab_name, v_subpart_col_id
Description   : creates subpartition for a specific value, primarily for SAE subpartitioned tables
***************************************************************************************************************/

PROCEDURE TAB_SUBPART_CREATE(v_tab_name VARCHAR2, v_subpart_col_id NUMBER);

/***************************************************************************************************************
Name          : REBUILD_LCLIDX_SUBPART
Parameters    : v_tab_name, v_subpart_col_id
Description   : rebuild local index at subpartition level, primarily for SAE subpartitioned tables
***************************************************************************************************************/

PROCEDURE REBUILD_LCLIDX_SUBPART(v_tab_name VARCHAR2, v_subpart_col_id NUMBER);

/***************************************************************************************************************
Name          : CHK_LCLIDX_SUBPART
Parameters    : v_tab_name, v_subpart_col_id
Description   : Checks local index subpartition is unusable and if it is then waits
***************************************************************************************************************/

PROCEDURE CHK_LCLIDX_SUBPART(v_tab_name VARCHAR2, v_subpart_col_id NUMBER);

/***************************************************************************************************************
Name          : TAB_SUBPART_TRUNC
Parameters    : v_tab_name, v_subpart_col_id
Description   : truncates table data at subpartition level, primarily for SAE subpartitioned tables
***************************************************************************************************************/

PROCEDURE TAB_SUBPART_TRUNC(v_tab_name VARCHAR2, v_subpart_col_id NUMBER);

/***************************************************************************************************************
Name          : TAB_SUBPART_TRUNC
Parameters    : v_tab_name, v_subpart_col_id
Description   : creates "temporary" SAE_SCENARIO_DATA_XYZ calculation table
***************************************************************************************************************/

PROCEDURE SCENDATA_TAB_CREATE(v_scenario_id NUMBER, v_table_name VARCHAR2 DEFAULT NULL, v_loopback_scenario_id NUMBER DEFAULT NULL, v_select VARCHAR2 DEFAULT NULL, v_temp_table VARCHAR2 DEFAULT NULL, v_source_table VARCHAR2 DEFAULT NULL, v_where VARCHAR2 DEFAULT NULL) ;

/***************************************************************************************************************
Name          : TAB_SUBPART_TRUNC
Parameters    : v_tab_name, v_subpart_col_id
Description   : drops "temporary" SAE_SCENARIO_DATA_XYZ calculation table
***************************************************************************************************************/

PROCEDURE SCENDATA_TAB_DROP(v_scenario_id NUMBER, v_table_name VARCHAR2 DEFAULT NULL);

PROCEDURE init_global_var;

PROCEDURE create_exp_temp_tab (p_context_id number, p_scenario_id number ) ;

PROCEDURE drop_exp_temp_tab (p_context_id number, p_scenario_id number ) ;

PROCEDURE REGISTER_TEMP_TABLE ( p_table_name cd_fdw_structure.table_name%type, p_table_type cd_fdw_structure.table_type%type, p_standard_custom VARCHAR2 DEFAULT 'S', p_comp_code VARCHAR2 DEFAULT 'CUSTOM', p_allow_long_name VARCHAR2 DEFAULT 'N' ) ;

PROCEDURE UNREGISTER_TEMP_TABLE ( p_table_name cd_fdw_structure.table_name%type ) ;

PROCEDURE TAB_SUBPART_DROP (v_tab_name VARCHAR2, v_subpart_col_id NUMBER);

PROCEDURE DROP_CANCELED_SUBPART;

PROCEDURE SAU_CREATE_TAB_EXEC ( p_table_name cd_fdw_structure.table_name%type, p_sql_str CLOB ) ;

PROCEDURE RECHECK_IMPORTSET ( p_contextid number, p_import_set varchar2 , p_parallel_flag varchar2 default 'N' ) ;

PROCEDURE mature_ln_null_to_zero(V_WORKFLOW_ID NUMBER,V_SCENARIO_ID NUMBER,V_ENGINE_ID NUMBER);

FUNCTION validate_sql(p_query varchar2) return varchar2 ;

PROCEDURE SCENDATA_SUBPART_SWAP(v_scenario_id NUMBER);

PROCEDURE CRE_SWAP_TAB(v_swap_tname VARCHAR2, v_table_name VARCHAR2);

FUNCTION CHECK_SWAP_TAB(v_table_name VARCHAR2)  return number ;

end PACK_SAE_UTILS;

/
create or replace 
PACKAGE BODY           "PACK_SAE_UTILS" as

PROCEDURE enable_parallel_processing(v_cores NUMBER DEFAULT NULL) IS
	vv_cores NUMBER;
BEGIN
--	vv_cores := NVL(v_cores, 16); -- changes done to handle paralle DML issue
  	vv_cores := NVL(v_cores, 1);
	--EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL QUERY PARALLEL ' || vv_cores;
	EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DDL PARALLEL ' || vv_cores;
	EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DML PARALLEL ' || vv_cores;
END enable_parallel_processing;

PROCEDURE disable_parallel_processing IS
BEGIN
	--EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL QUERY';
	EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
	EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DDL';
END disable_parallel_processing;

PROCEDURE create_tmp_cd_sessions
IS
	v_sql VARCHAR2(4000);
BEGIN
	v_sql := ' CREATE GLOBAL TEMPORARY TABLE TMP_CD_SESSIONS ( '
		  || ' SESSION_ID NUMBER NOT NULL ENABLE ) '
		  || ' ON COMMIT DELETE ROWS ';
	EXECUTE IMMEDIATE v_sql;
EXCEPTION
	WHEN OTHERS THEN NULL;
END create_tmp_cd_sessions;

FUNCTION get_context(v_tab_name VARCHAR2, v_subpart_col_id NUMBER)
RETURN NUMBER
IS
	v_sql 			VARCHAR2(4000);
	v_partition 	CONTEXTS.PK_RD_WS%TYPE;
	v_context_id 	NUMBER;
BEGIN
	IF UPPER(v_tab_name) NOT IN ('SAE_OVERRIDE_EVENT', 'SAE_CALC_AUDIT_LOG', 'SAE_CALC_AUDIT_TOPIC', 'SAE_SCENARIO_DATA', 'SAE_BASELINE_DATA', 'SAE_SCENDATA_OVERRIDE', 'SAE_INTERIM_BASEDATA')
	THEN RAISE_APPLICATION_ERROR(-20095, 'Table ' || v_tab_name || ' needs to be validated by the procedure PACK_SAE_UTILS.GET_CONTEXT to translate its PARTITION_KEY!');
	ELSE
		BEGIN
			v_sql := 'SELECT PARTITION_KEY '
				  || '  FROM ' || CASE WHEN v_tab_name IN ('SAE_BASELINE_DATA', 'SAE_INTERIM_BASEDATA') THEN ' SAE_BASELINE ' ELSE ' SAE_SCENARIO ' END
				  || ' WHERE ID = ' || v_subpart_col_id
				  || '   AND ROWNUM = 1 ';
		EXECUTE IMMEDIATE v_sql INTO v_partition;
		EXCEPTION
			WHEN NO_DATA_FOUND
			THEN v_context_id := 0;
		END;
		IF v_context_id <> 0 OR v_context_id IS NULL
		THEN
-- Because all Scenario Analyzer tables are CALC, we can do this:
			SELECT CONTEXT_ID
			  INTO v_context_id
			  FROM CONTEXTS
			 WHERE PK_RD_WS = v_partition;
		END IF;
	END IF;
	RETURN v_context_id;
END get_context;

/***************************************************************************************************************
--This procedure will insert all Scenario Sets flagged as 'Copy on new Context' to the newly created context
-- 1.1 (18/04/2012 by ASA)  Modified to move Base Simulation to SAE_SIMULATION_SET_SIM
-- 1.2 (27/04/2012 by ASA)  Since we do not know the source context -
							Picking up the latest context's simulation set name if there are duplicates across contexts
							and deleting and re-inserting the simulation set if one with the same name already exists in the target context.
-- 1.3 (08/05/2012 by ASA)  passing the source context and marking new simulation set as copy on new context
-- 1.4 (12/05/2012 by ASA)  revising delete and selections as RCo has a FK from PROCESS_SETTING to SAE_SIMULATION_SET and this causes and error on delete.
-- 1.5 (15/06/2012 by ASA)  revising accesscontrol entry for simulation set as the existing check would never allow a new entry to be added.
***************************************************************************************************************/
PROCEDURE INSERT_FOR_NEW_CONTEXT(v_context_id number, v_src_ctx number default null) IS

--Do not insert Scenarios (for the sets) more than once
v_number		number;
v_base_id		number;
v_new_id		number;
v_scen_id		number;
v_cnt_simsetsim number;

BEGIN

	--delete sets from the target context
  FOR cur_simsetdel in (select ID from SAE_SIMULATION_SET where CONTEXT_ID = v_context_id)
  LOOP
    begin

      delete from SAE_SIMULATION_SET where ID = cur_simsetdel.ID;

      --delete from accesscontrol if above passes
      delete from SAE_ACCESSCONTROL
      where TYPE = 'SIMULATION_SET' and DATA_VALUE = cur_simsetdel.ID;

    exception when others then null;
    end;

  END LOOP;

--Then delete all scenarios that are not used by any remaining set:
	FOR DEL IN (SELECT ID FROM SAE_SIMULATION WHERE CONTEXT_ID = v_context_id AND ID NOT IN
		(SELECT ssss.SIMULATION_ID FROM SAE_SIMULATION_SET_SIM ssss, SAE_SIMULATION_SET sss WHERE sss.ID = ssss.SIMULATION_SET_ID AND sss.CONTEXT_ID = v_context_id)
	)
	LOOP
	BEGIN

      --delete the simulations
      delete from SAE_SIMULATION where ID = DEL.ID;

	  --delete from access control
      delete from SAE_ACCESSCONTROL
      where TYPE = 'SIMULATION' and DATA_VALUE = DEL.ID;

      exception when others then null;

	END;
	END LOOP;

	--we take the simulation set names to be copied from source context
	FOR ST IN (SELECT * FROM SAE_SIMULATION_SET	WHERE COPY_ON_NEW_CONTEXT = 'Y' and CONTEXT_ID = v_src_ctx and upper(NAME) not in (select upper(NAME) from SAE_SIMULATION_SET where CONTEXT_ID = v_context_id))
	LOOP

		SELECT SEQ_SAE_SIMULATION_SET.NEXTVAL INTO v_new_id FROM DUAL;

		INSERT INTO SAE_SIMULATION_SET(id,name,description,created_by,created_date,updated_by,updated_date,role,context_id,copy_on_new_context)
		SELECT	v_new_id,ST.NAME,ST.DESCRIPTION,ST.CREATED_BY,SYSDATE,ST.UPDATED_BY,ST.UPDATED_DATE,ST.ROLE,v_context_id,'Y' FROM DUAL
		WHERE ST.NAME NOT IN (SELECT NAME FROM SAE_SIMULATION_SET WHERE CONTEXT_ID = v_context_id AND ROLE = ST.ROLE);

		Insert into SAE_ACCESSCONTROL (ID,ROLE_NAME,TYPE,DATA_VALUE,CREATED_DATE,CREATED_BY)
		SELECT SYS_GUID(),ST.ROLE,'SIMULATION_SET',v_new_id,SYSDATE,ST.CREATED_BY FROM DUAL
		WHERE ST.NAME IN (SELECT NAME FROM SAE_SIMULATION_SET WHERE CONTEXT_ID = v_context_id AND ROLE = ST.ROLE)
		and v_new_id not in (select DATA_VALUE from SAE_ACCESSCONTROL where TYPE = 'SIMULATION_SET' and ROLE_NAME = ST.ROLE);

		FOR SCEN IN (SELECT ss.*, ssss.IS_BASE_SIMULATION FROM SAE_SIMULATION ss, SAE_SIMULATION_SET_SIM ssss
					WHERE ssss.SIMULATION_SET_ID = ST.ID and ss.ROLE_NAME = ST.ROLE AND ss.ID = ssss.SIMULATION_ID)
		LOOP

			SELECT COUNT(1), MAX(ID) INTO v_number, v_scen_id FROM SAE_SIMULATION WHERE NAME = SCEN.NAME and ROLE_NAME=SCEN.ROLE_NAME and CONTEXT_ID = v_context_id;

			IF v_number = 0 THEN

				--LOOP Sur les Scenarios (non deja existants)
				--Call procedure pour chaque
				PACK_SAE_SCENARIOS.DUPLICATE_SCENARIO(v_new_id, SCEN.ID,SCEN.CREATED_BY,SCEN.ROLE_NAME, 'Y', v_context_id);

			END IF;

		  --need to recheck the count since if we still have no simulation then we only wish to perform the mapping if we have a pre-existing simulation or have inserted one above
		  SELECT COUNT(1), MAX(ID) INTO v_number, v_scen_id FROM SAE_SIMULATION WHERE NAME = SCEN.NAME and ROLE_NAME=SCEN.ROLE_NAME and CONTEXT_ID = v_context_id;

		  IF v_number = 1 THEN

			--check before inserting
			select count(1) into v_cnt_simsetsim from SAE_SIMULATION_SET_SIM where SIMULATION_SET_ID = v_new_id and SIMULATION_ID = v_scen_id;
			IF v_cnt_simsetsim = 0 THEN
				INSERT INTO SAE_SIMULATION_SET_SIM (ID, SIMULATION_SET_ID, SIMULATION_ID, IS_BASE_SIMULATION) SELECT SEQ_SAE_SIMULATION_SET_SIM.NEXTVAL, v_new_id, v_scen_id, SCEN.IS_BASE_SIMULATION FROM DUAL;
			END IF;

		  END IF;

		END LOOP;

	END LOOP;

END INSERT_FOR_NEW_CONTEXT;

/***************************************************************************************************************
--Updates the scenario contents based on the latest version of the configuration
--This will remove scenario types that are no longer relevant from the param settings, and add the new ones
-- 1.1 (09/07/2012 by ASA) Handling for after partitioning
***************************************************************************************************************/
PROCEDURE UPDATE_SCENARIOS_FROM_CONFIG IS

v_number 	number;
v_org_context_id number;

BEGIN

	v_org_context_id := pack_install.get_context_id();
  FOR cur_ctx in (select CONTEXT_ID from CONTEXTS where CONTEXT_TYPE <> 'B')
  LOOP
	pack_context.contextid_open(cur_ctx.CONTEXT_ID);

  --Remove items that are no longer relevant:
    DELETE FROM SAE_SIM_SCENTYPE sss WHERE sss.SCENARIO_TYPE_ID NOT IN
    (SELECT sst.SCENARIO_TYPE_ID FROM SAE_SIMTYPE_SCENTYPE sst, SAE_SIMULATION sim WHERE sim.SIMULATION_TYPE_ID = sst.SIMULATION_TYPE_ID AND sim.ID = sss.SIMULATION_ID);
    DELETE FROM SAE_SIM_SCEN sss WHERE sss.SCENARIO_TYPE_ID NOT IN
    (SELECT sst.SCENARIO_TYPE_ID FROM SAE_SIMTYPE_SCENTYPE sst, SAE_SIMULATION sim WHERE sim.SIMULATION_TYPE_ID = sst.SIMULATION_TYPE_ID AND sim.ID = sss.SIMULATION_ID);

    --Add missing scenario types, by SCEN_TYPE_ID and ADMIN_PARAM_ID
    FOR SCENARIO IN ( SELECT ID, SIMULATION_TYPE_ID FROM SAE_SIMULATION ) LOOP

    --Add missing scenario types
      FOR SCENARIO_TYPE IN ( SELECT scenario_type_id, def_scentype_param_setting FROM SAE_SIMTYPE_SCENTYPE WHERE SIMULATION_TYPE_ID = SCENARIO.SIMULATION_TYPE_ID) LOOP

        SELECT COUNT(1) INTO v_number FROM SAE_SIM_SCENTYPE WHERE SIMULATION_ID = SCENARIO.ID AND SCENARIO_TYPE_ID = SCENARIO_TYPE.SCENARIO_TYPE_ID;

        IF v_number = 0 THEN

          pack_sae_scenarios.SCEN_TYPE_CUSTOM_PARAM(SCENARIO.ID,SCENARIO_TYPE.SCENARIO_TYPE_ID,SCENARIO_TYPE.DEF_SCENTYPE_PARAM_SETTING);

        END IF;

      END LOOP;

    END LOOP;

    COMMIT;

		pack_context.contextid_open(v_org_context_id);

  END LOOP;

END UPDATE_SCENARIOS_FROM_CONFIG;

/***************************************************************************************************************
--Operations to be performed before loading a dataset
-- 1.1 (23/04/2012 by MDU)  Updates moved from DISABLE_CONFIG_LOCKS since DMT force enables all triggers before running the after sql
***************************************************************************************************************/
PROCEDURE BEFORE_LOAD_DATASET IS

BEGIN

	--Updates to keep parent-child relationship in categories
	update sae_category set parent_name = parent_name||'_$bak$'  where parent_name is not null ;
	update sae_category set parent_name = name  where parent_name is null;
	commit;

	--Disable triggers to edit LOCKED rows
	DISABLE_CONFIG_LOCKS();

	--Disabled LOCKS for the purpose of import/update
	UPDATE SAE_SIMULATION_TYPE SET LOCKED = 'N';
	UPDATE SAE_SCENARIO_TYPE SET LOCKED = 'N';
	UPDATE SAE_PROVIDER SET LOCKED = 'N';
	UPDATE SAE_ENGINE SET LOCKED = 'N';
	UPDATE SAE_LOOKUP_PROVIDER SET LOCKED = 'N';

END BEFORE_LOAD_DATASET;

/***************************************************************************************************************
--Operations to be performed after loading a dataset
-- 1.1 (23/04/2012 by MDU) call to DISABLE_CONFIG_LOCKS
-- 1.2 (08/05/2012 by ASA) raising the order additions
-- 1.3 (09/05/2012 by ASA)  adding accesscontrol entries for scenario types and moving inserts to after remove uninstalled config
-- 1.4 (12/05/2012 by ASA)  enable disable triggers on sae_contexts when bringing its data in synch with contexts.
-- 1.5 (08/06/2012 by ASA)  tweaking the scenariotype accesscontrol query and adding refresh measures dimensions after this.
-- 1.6 (11/06/2012 by ASA)  generate default template after reshing the measures dimensions and handle the exceptions thrown.
***************************************************************************************************************/
PROCEDURE AFTER_LOAD_DATASET IS

v_ste_id_old SAE_SCENARIOTYPE_ENGINE.ID%TYPE;
v_ste_id_new SAE_SCENARIOTYPE_ENGINE.ID%TYPE;
v_cnt number :=0;
v_cnt1 number := 0;

BEGIN

	--Disable triggers that are enabled by the DMT
	DISABLE_CONFIG_LOCKS();

	for cur_top_cat in (select parent_name from sae_category where parent_name =name)
	loop     update sae_category set parent_name = null,
			CHILD_ORDER = (select nvl(max(child_order),0)+1 from sae_category where parent_name is null)
			where name = cur_top_cat.parent_name;     commit;
			 end loop;
	for cur_top_cat in (select name,parent_name from sae_category where parent_name like '%$bak$')
	loop
			  update sae_category set parent_name =rtrim(cur_top_cat.parent_name, '_$bak$' ),
			CHILD_ORDER = (select nvl(max(child_order),0)+1 from sae_category where parent_name = rtrim(cur_top_cat.parent_name, '_$bak$' ))
			where name =cur_top_cat.name;
			commit;
	end loop;
			UPDATE  SAE_CATEGORY  CHILD SET PARENT =(SELECT PARENT1.ID FROM SAE_CATEGORY PARENT1
			WHERE  PARENT1.NAME =CHILD.PARENT_NAME);
	for cur in (select measure_value_order,measure_id from sae_measure_value where measure_value_order > 10000)
	loop
			update sae_measure_value set measure_value_order =  (select nvl(max(measure_value_order),0)+1 from sae_measure_value where measure_id = cur.measure_id and measure_value_order < 10000)
			where measure_value_order =cur.measure_value_order and measure_id = cur.measure_id;
			commit;
	end loop;
	for cur in (select ENGINE_ORDER ,scenario_type, priority from SAE_SCENARIOTYPE_ENGINE where ENGINE_ORDER  > 10000)
    loop

             select count(*) into v_cnt1 from SAE_SCENARIOTYPE_ENGINE where ENGINE_ORDER  < 10000 and ENGINE_ORDER  = cur.ENGINE_ORDER - 10000 and scenario_type = cur.scenario_type AND priority = cur.priority;
             if   v_cnt1 = 1  then

				  SELECT ID
					INTO v_ste_id_old
					FROM SAE_SCENARIOTYPE_ENGINE
				   WHERE ENGINE_ORDER  < 10000 AND ENGINE_ORDER  = cur.ENGINE_ORDER - 10000 and scenario_type = cur.scenario_type AND priority = cur.priority;

				  SELECT ID
					INTO v_ste_id_new
					FROM SAE_SCENARIOTYPE_ENGINE
				   WHERE ENGINE_ORDER = cur.ENGINE_ORDER and scenario_type = cur.scenario_type AND priority = cur.priority;

				  delete from SAE_SCENTYP_ENG_LOOKUP where SCENTYPE_ENGINE_ID = v_ste_id_old;
				  update SAE_SCENTYP_ENG_LOOKUP set SCENTYPE_ENGINE_ID = v_ste_id_old where SCENTYPE_ENGINE_ID = v_ste_id_new;

				  delete from SAE_FIELD_ENGINE_INPUT where SCENARIOTYPE_ENGINE_ID = v_ste_id_old;
				  update SAE_FIELD_ENGINE_INPUT set SCENARIOTYPE_ENGINE_ID = v_ste_id_old where SCENARIOTYPE_ENGINE_ID = v_ste_id_new;

				  delete from SAE_FIELD_ENGINE_OUTPUT where SCENARIOTYPE_ENGINE_ID = v_ste_id_old;
				  update SAE_FIELD_ENGINE_OUTPUT set SCENARIOTYPE_ENGINE_ID = v_ste_id_old where SCENARIOTYPE_ENGINE_ID = v_ste_id_new;

                  delete from SAE_SCENARIOTYPE_ENGINE where ENGINE_ORDER  = cur.ENGINE_ORDER and scenario_type = cur.scenario_type AND priority = cur.priority;
             else
                  update SAE_SCENARIOTYPE_ENGINE  set ENGINE_ORDER  =  (select nvl(max(ENGINE_ORDER ),0)+1 from SAE_SCENARIOTYPE_ENGINE  where scenario_type = cur.scenario_type and ENGINE_ORDER  < 10000)
                  where ENGINE_ORDER  =cur.ENGINE_ORDER  and scenario_type = cur.scenario_type;
             end if;
             commit;
    end loop;
	for cur in (select  MEASURE_ORDER ,CATEGORY_ID from SAE_MEASURE_CATEGORY  where  MEASURE_ORDER  > 10000)
	loop
			update SAE_MEASURE_CATEGORY  set  MEASURE_ORDER  =  (select nvl(max( MEASURE_ORDER ),0)+1 from SAE_MEASURE_CATEGORY  where CATEGORY_ID = cur.CATEGORY_ID and  MEASURE_ORDER  < 10000)
			where  MEASURE_ORDER  =cur. MEASURE_ORDER  and CATEGORY_ID = cur.CATEGORY_ID;
			commit;
	end loop;

	COMMIT;

	--Remove configuration that shouldn't be shown on this DB because of product restrictions
	REMOVE_UNINSTALLED_CONFIG();

	--put in accesscontrol after loading dataset and removing the uninstalled ocnfigurations as otherwise we'll get extra entries
	--accesscontrol for simulation types
	for cur in (select id from sae_simulation_type)
		loop
		  select count(1) into v_cnt from sae_accesscontrol where upper(role_name) = 'ADMIN' and upper(type) = 'SIMULATION_TYPE' and data_value = cur.id;
		  if(v_cnt =0) then
			insert into sae_accesscontrol (id,role_name,type,data_value,privilege,created_date,created_by,modified_date,modified_by) values
				(sys_guid(),'ADMIN','SIMULATION_TYPE',cur.id,NULL,sysdate,1,null,null);
		  end if;
	end loop;

	--adding accesscontrol entries for scenariotypes
	insert into sae_accesscontrol (id,role_name,type,data_value,privilege,created_date,created_by,modified_date,modified_by)
	(select sys_guid(),'ADMIN','SCENARIO_TYPE',ID,NULL,sysdate,1,null,null from SAE_SCENARIO_TYPE where ID not in (select DATA_VALUE from SAE_ACCESSCONTROL where upper(TYPE) = 'SCENARIO_TYPE' and ROLE_NAME = 'ADMIN'));

	commit;

	--refreshing measures and dimensions for accesscontrol
	pack_sae.refresh_measures_dimensions();

	--generate the default template for ADMIN
	begin
		pack_sae.generate_default_template('ADMIN');
		exception when others then null;
	end;

	--Update existing scenarios based on the update configuration
	UPDATE_SCENARIOS_FROM_CONFIG();

	--Re-enable all constraints
	ENABLE_CONFIG_LOCKS();

	--Update ADMIN Data
	SELECT COUNT(1) INTO v_cnt FROM SAE_ADMIN_DATA WHERE TYPE = 'ACTIVE_SIMULATION_TYPE_NAME';
	IF v_cnt = 0 THEN
	--Is RCO installed?
	SELECT COUNT(1) INTO v_cnt FROM APPLICATION WHERE PRODUCT = 'V';
	IF v_cnt >= 1 THEN
	--Insert for RCO
	insert into sae_admin_data(ID,TYPE,VALUE,DESCRIPTION) values(seq_sae_admin_data.nextval,'ACTIVE_SIMULATION_TYPE_NAME','RiskConfidence','current active simulation type for products without license');
	ELSE
	  --Is RAY installed?
	  SELECT COUNT(1) INTO v_cnt FROM APPLICATION WHERE PRODUCT IN ('B','M','L');
	  IF v_cnt >= 1 THEN
	  insert into sae_admin_data(ID,TYPE,VALUE,DESCRIPTION) values(seq_sae_admin_data.nextval,'ACTIVE_SIMULATION_TYPE_NAME','RiskAuthority','current active simulation type for products without license');
	  END IF;
	END IF;
	END IF;

END AFTER_LOAD_DATASET;

/***************************************************************************************************************
--Disable SA Locks
-- 1.1 (23/04/2012 by MDU) Updates moved to BEFORE_LOAD_DATASET
***************************************************************************************************************/
PROCEDURE DISABLE_CONFIG_LOCKS IS
BEGIN

PACK_DDL.enable_disable_constraints('SAE_SIMULATION_TYPE','N');
PACK_DDL.enable_disable_constraints('SAE_SIMTYPE_SCENTYPE','N');
PACK_DDL.enable_disable_constraints('SAE_SCENARIO_TYPE','N');
PACK_DDL.enable_disable_constraints('SAE_SCENARIOTYPE_PROVIDER','N');
PACK_DDL.enable_disable_constraints('SAE_FIELD','N');
PACK_DDL.enable_disable_constraints('SAE_SCENARIOTYPE_ENGINE','N');
PACK_DDL.enable_disable_constraints('SAE_SCENTYPE_EXT_OPT','N');
PACK_DDL.enable_disable_constraints('SAE_PROVIDER','N');
PACK_DDL.enable_disable_constraints('SAE_PROVIDER_INPUT','N');
PACK_DDL.enable_disable_constraints('SAE_PROVIDER_OUTPUT','N');
PACK_DDL.enable_disable_constraints('SAE_PROVIDER_PARAMETER','N');
PACK_DDL.enable_disable_constraints('SAE_ENGINE_INPUT','N');
PACK_DDL.enable_disable_constraints('SAE_ENGINE_OUTPUT','N');
PACK_DDL.enable_disable_constraints('SAE_ENGINE_PARAMETER','N');
PACK_DDL.enable_disable_constraints('SAE_ENGINE','N');
PACK_DDL.enable_disable_constraints('SAE_LOOKUP_PROVIDER_PARAM','N');
PACK_DDL.enable_disable_constraints('SAE_LOOKUP_PROVIDER','N');
PACK_DDL.enable_disable_constraints('SAE_MANDATORY_ITEMS','N');

END DISABLE_CONFIG_LOCKS;

/***************************************************************************************************************
--Enable SA Locks
***************************************************************************************************************/
PROCEDURE ENABLE_CONFIG_LOCKS IS
BEGIN

PACK_DDL.enable_disable_constraints('SAE_SIMULATION_TYPE','Y');
PACK_DDL.enable_disable_constraints('SAE_SIMTYPE_SCENTYPE','Y');
PACK_DDL.enable_disable_constraints('SAE_SCENARIO_TYPE','Y');
PACK_DDL.enable_disable_constraints('SAE_SCENARIOTYPE_PROVIDER','Y');
PACK_DDL.enable_disable_constraints('SAE_FIELD','Y');
PACK_DDL.enable_disable_constraints('SAE_SCENARIOTYPE_ENGINE','Y');
PACK_DDL.enable_disable_constraints('SAE_SCENTYPE_EXT_OPT','Y');
PACK_DDL.enable_disable_constraints('SAE_PROVIDER','Y');
PACK_DDL.enable_disable_constraints('SAE_PROVIDER_INPUT','Y');
PACK_DDL.enable_disable_constraints('SAE_PROVIDER_OUTPUT','Y');
PACK_DDL.enable_disable_constraints('SAE_PROVIDER_PARAMETER','Y');
PACK_DDL.enable_disable_constraints('SAE_ENGINE_INPUT','Y');
PACK_DDL.enable_disable_constraints('SAE_ENGINE_OUTPUT','Y');
PACK_DDL.enable_disable_constraints('SAE_ENGINE_PARAMETER','Y');
PACK_DDL.enable_disable_constraints('SAE_ENGINE','Y');
PACK_DDL.enable_disable_constraints('SAE_LOOKUP_PROVIDER_PARAM','Y');
PACK_DDL.enable_disable_constraints('SAE_LOOKUP_PROVIDER','Y');
PACK_DDL.enable_disable_constraints('SAE_MANDATORY_ITEMS','Y');

END ENABLE_CONFIG_LOCKS;

/***************************************************************************************************************
--Remove uninstalled configuration
--cleanup of all underlying tables required since Triggers are disabled
--remove entries for processes that do not exist on the schema
-- 1.3 (15/06/2012 by ASA)  modifications for partitioning addition.
***************************************************************************************************************/
PROCEDURE REMOVE_UNINSTALLED_CONFIG IS

v_del_rc1 NUMBER;
v_del_rc2 NUMBER;

BEGIN

	--Delete Scenario Types which have Mandatory Items - but for which no relevant installed product is found:
	DELETE FROM SAE_SCENARIO_TYPE st WHERE ID IN ( SELECT SCENARIO_TYPE_ID FROM SAE_MANDATORY_ITEMS ) AND NOT EXISTS
	( SELECT 1 FROM APPLICATION a, SAE_MANDATORY_ITEMS m WHERE a.PRODUCT = m.PRODUCT AND st.ID = m.SCENARIO_TYPE_ID );

	v_del_rc1 := SQL%ROWCOUNT;	 ---Added for DE30820

	--Delete Scenario Types which have Mandatory Items - but for which no relevant installed product is found:
	DELETE FROM SAE_SIMULATION_TYPE st WHERE ID IN ( SELECT SIMULATION_TYPE_ID FROM SAE_MANDATORY_ITEMS ) AND NOT EXISTS
	( SELECT 1 FROM APPLICATION a, SAE_MANDATORY_ITEMS m WHERE a.PRODUCT = m.PRODUCT AND st.ID = m.SIMULATION_TYPE_ID );

	v_del_rc2 := SQL%ROWCOUNT; 	 ---Added for DE30820

	IF v_del_rc1 > 0 THEN
		pack_ddl.enable_disable_constraints('SAE_MEASURE','N');

		FOR cur_scentype_del IN (select TABLE_NAME
                            ,decode(instr(CHILD_COLUMNS,'partition_key'),0,CHILD_COLUMNS,substr(CHILD_COLUMNS,instr(CHILD_COLUMNS,'partition_key,')+14)) as CHILD_COLUMNS
                            ,decode(instr(PARENT_COLUMNS,'partition_key'),0,PARENT_COLUMNS,substr(PARENT_COLUMNS,instr(PARENT_COLUMNS,'partition_key,')+14)) as PARENT_COLUMNS
                            ,PARENT_TABLE_NAME
                            from TABLE_CONSTRAINTS
                            where CONSTRAINT_TYPE = 'R'
                            and DELETE_RULE = 'CASCADE'
                            start with PARENT_TABLE_NAME = 'SAE_SCENARIO_TYPE' connect by nocycle prior table_name = parent_table_name)
		LOOP
		execute immediate 'delete from '
                      ||cur_scentype_del.TABLE_NAME
                      ||' where '||cur_scentype_del.CHILD_COLUMNS
                      ||' not in (select '||cur_scentype_del.PARENT_COLUMNS||' from '||cur_scentype_del.PARENT_TABLE_NAME||')';
		END LOOP;
	END IF;

	IF v_del_rc2 > 0 THEN
		FOR cur_simtype_del IN (select TABLE_NAME
                            ,decode(instr(CHILD_COLUMNS,'partition_key'),0,CHILD_COLUMNS,substr(CHILD_COLUMNS,instr(CHILD_COLUMNS,'partition_key,')+14)) as CHILD_COLUMNS
                            ,decode(instr(PARENT_COLUMNS,'partition_key'),0,PARENT_COLUMNS,substr(PARENT_COLUMNS,instr(PARENT_COLUMNS,'partition_key,')+14)) as PARENT_COLUMNS
                            ,PARENT_TABLE_NAME
                            from TABLE_CONSTRAINTS
                            where CONSTRAINT_TYPE = 'R'
                            and DELETE_RULE = 'CASCADE'
                            start with PARENT_TABLE_NAME = 'SAE_SIMULATION_TYPE' connect by nocycle prior table_name = parent_table_name)
		LOOP
		execute immediate 'delete from '
                      ||cur_simtype_del.TABLE_NAME
                      ||' where '
                      ||cur_simtype_del.CHILD_COLUMNS
                      ||' not in (select '||cur_simtype_del.PARENT_COLUMNS||' from '||cur_simtype_del.PARENT_TABLE_NAME||')';
		END LOOP;
	END IF;

	pack_ddl.enable_disable_constraints('SAE_MEASURE','Y');

	--Remove unused Baseline Providers
	DELETE FROM SAE_PROVIDER WHERE ID NOT IN ( SELECT PROVIDER_ID FROM SAE_SCENARIOTYPE_PROVIDER);

	IF SQL%ROWCOUNT > 0 THEN
		FOR cur_prov_del IN (select TABLE_NAME
                        ,decode(instr(CHILD_COLUMNS,'partition_key'),0,CHILD_COLUMNS,substr(CHILD_COLUMNS,instr(CHILD_COLUMNS,'partition_key,')+14)) as CHILD_COLUMNS
                        ,decode(instr(PARENT_COLUMNS,'partition_key'),0,PARENT_COLUMNS,substr(PARENT_COLUMNS,instr(PARENT_COLUMNS,'partition_key,')+14)) as PARENT_COLUMNS
                        ,PARENT_TABLE_NAME
                        from TABLE_CONSTRAINTS
                        where CONSTRAINT_TYPE = 'R'
                        and DELETE_RULE = 'CASCADE'
                        start with PARENT_TABLE_NAME = 'SAE_PROVIDER' connect by nocycle prior table_name = parent_table_name)
		LOOP
		execute immediate 'delete from '
                      ||cur_prov_del.TABLE_NAME
                      ||' where '
                      ||cur_prov_del.CHILD_COLUMNS
                      ||' not in (select '||cur_prov_del.PARENT_COLUMNS||' from '||cur_prov_del.PARENT_TABLE_NAME||')';
		END LOOP;
	END IF;

	--Remove unused Engines
	/*DELETE FROM SAE_ENGINE WHERE ID NOT IN ( SELECT ENGINE_ID FROM SAE_SCENARIOTYPE_ENGINE) and ID not in (select ENGINE_ID from SAE_SIMTYPE_ENGINE);

	IF SQL%ROWCOUNT > 0 THEN
		FOR cur_engine_del IN (select TABLE_NAME
                          ,decode(instr(CHILD_COLUMNS,'partition_key'),0,CHILD_COLUMNS,substr(CHILD_COLUMNS,instr(CHILD_COLUMNS,'partition_key,')+14)) as CHILD_COLUMNS
                          ,decode(instr(PARENT_COLUMNS,'partition_key'),0,PARENT_COLUMNS,substr(PARENT_COLUMNS,instr(PARENT_COLUMNS,'partition_key,')+14)) as PARENT_COLUMNS
                          ,PARENT_TABLE_NAME
                          from TABLE_CONSTRAINTS
                          where CONSTRAINT_TYPE = 'R'
                          and DELETE_RULE = 'CASCADE'
                          start with PARENT_TABLE_NAME = 'SAE_ENGINE' connect by nocycle prior table_name = parent_table_name)
		LOOP
		execute immediate 'delete from '
                      ||cur_engine_del.TABLE_NAME
                      ||' where '
                      ||cur_engine_del.CHILD_COLUMNS
                      ||' not in (select '||cur_engine_del.PARENT_COLUMNS||' from '||cur_engine_del.PARENT_TABLE_NAME||')';
		END LOOP;
	END IF;*/

	--remove unused Lookup Providers
	delete from SAE_LOOKUP_PROVIDER where upper(NAME) not in (select upper(DATA_SOURCE) from SAE_PROVIDER_INPUT where DATA_SOURCE is not null
															  union
															  select upper(DATA_SOURCE) from SAE_SCENTYPE_EXT_OPT where DATA_SOURCE is not null);

	delete from SAE_LOOKUP_PROVIDER_PARAM where LOOKUP_PROVIDER_ID not in (select ID from SAE_LOOKUP_PROVIDER);

  --remove entries for processes that do not exist on the schema
  delete from SAE_MANDATORY_PROCESS where PROCESS not in (select PRODUCT from APPLICATION);

END REMOVE_UNINSTALLED_CONFIG;


/***************************************************************************************************
Name          : exec_ddl
Parameters    : ddl_statement_str
Description   : Execute DDL from a procedure.
Revision Log  :
	1. 18/Jul/2012 by Rujing: to fix the insufficient privilege error when SAU user is creating temp table from Java.
***************************************************************************************************/
PROCEDURE exec_ddl(ddl_statement_str in varchar2, table_name in varchar2 default null)
is
begin
	execute immediate ddl_statement_str;
	if table_name is not null then
		execute immediate 'grant select on ' || table_name || ' to public';
		execute immediate 'grant insert on ' || table_name || ' to public';
		execute immediate 'grant update on ' || table_name || ' to public';
	end if;
end exec_ddl;

/**************************************************************************************************************
Name          : change_simulation_type
Parameters    : v_simulation_id - simulation id of simulation being updated
				v_simulation_type_id - target simulation type for update
				v_context_id - context in which change needs to be made, -1 for all contexts
Description   : Update simulation type for simulations.
Revision Log  :
	1. 2/12/2013 Vivek.
**************************************************************************************************************/
PROCEDURE change_simulation_type(v_simulation_id number, v_simulation_type_id number, v_context_id number default -1)
is
seqid number;
found number;
begin
 if (v_context_id != -1) then
  pack_context.contextid_open(v_context_id);
 end if;
  dbms_output.enable;
  update sae_simulation set simulation_type_id = v_simulation_type_id where id = v_simulation_id;
  for r in (select scenario_type_id from sae_simtype_scentype where simulation_type_id = v_simulation_type_id) loop
    ----dbms_output.put_line('Searching Id : ' || r.scenario_type_id);
    found := 0;
    for s in (select scenario_type_id from sae_sim_scentype where simulation_id = v_simulation_id) loop
      if (r.scenario_type_id = s.scenario_type_id) then
       ----dbms_output.put_line('Id : ' || r.scenario_type_id || ' Present - So Skip');
       found := 1;
       exit;
      end if;
    end loop;
    if (found = 0) then
       ----dbms_output.put_line('Id : ' || r.scenario_type_id || ' Not Present In Source Sim Type - Adding');
       select SEQ_SAE_CUSTOM_PARAM_SET.NEXTVAL into seqid from dual;
       ----dbms_output.put_line('Id : ' || seqid || ' Custom Param Set Id');
       insert into sae_custom_param_set (id, name, scenario_type_id, use_existing_scenario, use_transformation_set, download_new_baseline,save_basedata, to_use, to_compute) values (seqid, 'Custom Param ' || seqid, r.scenario_type_id, '0', '1', '1', '1', '1', '1');
       insert into sae_sim_scentype (id, simulation_id, scenario_type_id, custom_scentype_param_set) values (SEQ_SAE_SIM_SCENTYPE.NEXTVAL, v_simulation_id, r.scenario_type_id, seqid);
       insert into sae_sim_scen (id, simulation_id, scenario_type_id) values (SEQ_SAE_SIM_SCEN.NEXTVAL, v_simulation_id,  r.scenario_type_id);
       --commit;
    end if;
  end loop;

  for r in (select * from sae_sim_scentype where simulation_id = v_simulation_id) loop
    ----dbms_output.put_line('Searching Id : ' || r.scenario_type_id);
    found := 0;
    for s in (select scenario_type_id from sae_simtype_scentype where simulation_type_id = v_simulation_type_id) loop
      if (r.scenario_type_id = s.scenario_type_id) then
       ----dbms_output.put_line('Id : ' || r.scenario_type_id || ' Present - So Skip');
       found := 1;
       exit;
      end if;
    end loop;
    if (found = 0) then  -- target scenario type not present so remove value
       ----dbms_output.put_line('Id : ' || r.scenario_type_id || ' Not Present In Target Sim Type - Removing');
       delete from sae_custom_param_set where id = r.CUSTOM_SCENTYPE_PARAM_SET and scenario_type_id = r.scenario_type_id;
       delete from sae_sim_scentype where simulation_id = v_simulation_id and scenario_type_id = r.scenario_type_id;
       delete from sae_sim_scen where simulation_id = v_simulation_id and scenario_type_id = r.scenario_type_id;
       --commit;
    end if;
  end loop;
end;


/**************************************************************************************************************
Name          : update_simulation_type
Parameters    : v_scope - scope for update; 'all' or 'single',
				v_param1 - simulation type if scope is 'all', simulation id if scope is 'single'
				v_param2 - target simulation type for update
				v_context_id - context in which change needs to be made, -1 for all contexts
Description   : Update simulation type for simulations.
Revision Log  :
	1. 2/12/2013 Vivek.
**************************************************************************************************************/
PROCEDURE update_simulation_type(v_scope varchar2, v_param1 number, v_param2 number, v_context_id number default -1)
is
begin
  if (v_scope = 'all') then
    if (v_context_id != -1) then     -- one particular context
     pack_context.contextid_open(v_context_id);
     for r in (select * from sae_simulation where simulation_type_id = v_param1) loop
       change_simulation_type(r.id, v_param2, v_context_id) ;
     end loop;
    else  -- all contexts
      for s in (select context_id from  contexts where backup_set is null) loop
        pack_context.contextid_open(s.context_id);
        for r in (select * from sae_simulation where simulation_type_id = v_param1) loop
          change_simulation_type(r.id, v_param2, s.context_id) ;
        end loop;
      end loop;
    end if;
  else
    change_simulation_type(v_param1, v_param2, v_context_id) ;
  end if;
  commit;
  exception when others then
    rollback;
    raise;
end;

/**************************************************************************************************************
Name          : sae_log_write
Parameters    : v_log_type - I=information. W=warning. E=error. Null=E.
                v_application - F=FDW C=cad G=Gem R=rrt E=fce A=alm I=ias B=bis P=bcp M=mkr...
                v_function - Where the error occurs (free text): function, package, pb function,...
                v_step - Where the error occurs in function.
                v_message - Error message. If null, oracle error message (with call stack).
                v_parameters - Additional values or information.
                v_tech_func - 'T' by default.
                v_severity - Log severity. Default value:1.
                v_unused - unused
                v_nummsg - Message id (see MESSAGES table), NULL possible
                v_table_name - When the log is linked to a table name and/or contract reference, it is sent to LOG_TABLE_DEAL (not log_table), NULL possible
                v_contract_reference - When the log is linked to a table name and/or contract reference, it is sent to LOG_TABLE_DEAL (not log_table), NULL possible
                v_log_struct_id - Struct id of the log.
Description   : This procedure writes in log_table or log_table_deal (depending on v_table_name,v_contract_reference).This procedure is called in autonomous transaction.
                US109549
Revision Log  : 1. 10/3/2013. Amit
***************************************************************************************************************/
PROCEDURE sae_log_write(
	v_log_type IN VARCHAR2 default null,
	v_application IN VARCHAR2 default null,
	v_function IN VARCHAR2 default null,
	v_step IN VARCHAR2 default null,
	v_message_string IN VARCHAR2 default null,
	v_parameters IN VARCHAR2 default null,
	v_tech_func IN VARCHAR2 DEFAULT 'T',
	v_severity IN NUMBER DEFAULT 1,
	v_unused IN CHAR DEFAULT null,
	v_nummsg in number default null,
	v_table_name in varchar2 default null,
	v_contract_reference in varchar2 default null,
  v_log_struct_id in number default null,  --US109549
  v_context_id in number default null
) is
	pragma autonomous_transaction;
	v_filter boolean:=false;
	v_counter number:=0;
	v_num varchar2(10);
	v_temp number;
  v_timestamp timestamp;
  v_message varchar2(4000);
  v_depth number;
  --US111653
  v_nb_errors number;
  v_nb_warnings number;
  v_nb_infos number;
  v_nb_perfs number;
  v_total_errors number;
  v_total_warnings number;
  v_total_infos number;
  v_total_perfs number;
  v_parent_log_struct_id number;
  v_parent_total_errors number;
  v_parent_total_warnings number;
  v_parent_total_infos number;
  v_parent_total_perfs number;
  v_log_key log_table.log_key%type;
  v_partition_key varchar2(20);
BEGIN
   v_timestamp := systimestamp;
   if v_context_id is not null then
   v_partition_key := pack_install.partition_key('Y', 'Y', v_context_id);
   end if;
   v_message := pack_utils.truncstr(nvl(v_message_string,dbms_utility.format_error_stack()||dbms_utility.format_error_backtrace()),4000);

  	-- TEMPORARY -- i43153
	-- Disable log_write for unplanned log_type : global variable vg_counters(v_log_type) has been initialised only for know log_types.
	-- pack_var.get_var_number($$plsql_unit,'vg_counters',NVL(v_log_type,'E')) ---=> Would raise an exception
	if v_log_type is not null then
		if v_log_type not in ('I','W','E','P','V','M','C') then
			----dbms_output.put_line('pack_log.log_write : unrecognized log_type '||v_log_type);
			return; -- No silent exception in order not to have a 'when others then raise' block hidden others exceptions stack
		end if;
	end if;

	pack_var.set_var($$plsql_unit,'vg_sae_last_log_id',0);
	IF NOT pack_var.get_var_boolean($$plsql_unit,'vg_sae_log_enable') THEN RETURN; END IF;
	IF pack_var.get_var_boolean($$plsql_unit,'vg_sae_log_filter_info') AND v_log_type = 'I' AND v_function <> 'LOG_BEGIN' AND v_function <> 'LOG_END' THEN RETURN; END IF;

	pack_var.set_var($$plsql_unit,'vg_sae_log_write',true);

	-- Gestion de la partition_key pour LOG_TABLE (normalement deja initialisee)
	IF pack_var.get_var_varchar2($$plsql_unit,'vg_sae_partition_key') IS NULL THEN
		IF pack_install.get_context_id IS NULL THEN
			pack_var.set_var($$plsql_unit,'vg_sae_partition_key',pack_install.partition_key('Y', 'Y', pack_install.get_ref_context_id));
		ELSE
			pack_var.set_var($$plsql_unit,'vg_sae_partition_key',pack_install.partition_key('Y', 'Y', pack_install.get_context_id));
		END IF;
	END IF;

	-- incrementer les compteurs
  -- US111653
  if v_log_struct_id is not null then
    begin
		SELECT depth, log_key
		  INTO v_depth, v_log_key
		  FROM log_struct
		 WHERE log_struct_id = v_log_struct_id;
    -- GM_COMMENT  select depth into v_depth from log_struct where log_struct_id = v_log_struct_id;
    exception
      when no_data_found then null;
      when others then null;
    end;

     if nvl(v_depth,0) >0 then
    --  pack_var.set_var($$plsql_unit,'vg_sae_counters',NVL(v_log_type,'E'),nvl(pack_var.get_var_number($$plsql_unit,'vg_sae_counters',NVL(v_log_type,'E')),0)+1);

     if v_log_type in ('E')
      then
        select NB_ERRORS,TOTAL_ERRORS into v_nb_errors,v_total_errors from log_struct where log_struct_id = v_log_struct_id;
        v_nb_errors := v_nb_errors+1;
        v_total_errors := v_total_errors+1;
        update log_struct set NB_ERRORS = v_nb_errors, TOTAL_ERRORS = v_total_errors where log_struct_id = v_log_struct_id;
        COMMIT;
     elsif v_log_type in ('W')
        then
        select NB_WARNINGS,TOTAL_WARNINGS into v_nb_warnings,v_total_warnings from log_struct where log_struct_id = v_log_struct_id;
        v_nb_warnings := v_nb_warnings+1;
        v_total_warnings := v_total_warnings+1;
        update log_struct set NB_WARNINGS = v_nb_warnings, TOTAL_WARNINGS = v_total_warnings where log_struct_id = v_log_struct_id;
        COMMIT;
    elsif v_log_type in ('I')
        then
        select NB_INFOS,TOTAL_INFOS into v_nb_infos,v_total_infos from log_struct where log_struct_id = v_log_struct_id;
        v_nb_infos := v_nb_infos+1;
        v_total_infos := v_total_infos+1;
        update log_struct set NB_INFOS = v_nb_infos, TOTAL_INFOS = v_total_infos where log_struct_id = v_log_struct_id;
        COMMIT;
    elsif v_log_type in ('P')
        then
        select NB_PERFS,TOTAL_PERFS into v_nb_perfs,v_total_perfs from log_struct where log_struct_id = v_log_struct_id;
        v_nb_perfs := v_nb_perfs+1;
        v_total_perfs := v_total_perfs+1;
        update log_struct set NB_PERFS = v_nb_perfs, TOTAL_PERFS = v_total_perfs where log_struct_id = v_log_struct_id;
        COMMIT;
      end if;
    end if;
  else
    --if nvl(pack_var.get_var_number($$plsql_unit,'vg_sae_depth'),0)>0 then
    if nvl(v_depth,0)>0 then
      pack_var.set_var($$plsql_unit,'vg_sae_counters',NVL(v_log_type,'E'),nvl(pack_var.get_var_number($$plsql_unit,'vg_sae_counters',NVL(v_log_type,'E')),0)+1);

    end if;
  end if;


	-- si le nummsg est renseigne
	if v_nummsg is not null and v_nummsg<>0 then
		v_num:=to_char(v_nummsg);

		-- un message a filter ?
		if pack_var.exists_entry($$plsql_unit,'vg_log_filter_counters',v_num) then
			-- incrementer le compteur courant
			v_counter:=nvl(pack_var.get_var_number($$plsql_unit,'vg_log_filter_counters',v_num),0) + 1;
			pack_var.set_var($$plsql_unit,'vg_log_filter_counters',v_num,v_counter);
			if v_counter>pack_var.get_var_number($$plsql_unit,'vg_log_filter_limits',v_num) then return; end if;

		-- si la limite globale est definie, on ajoute le message a la map
		elsif pack_var.get_var_number($$plsql_unit,'vg_sae_max_repeat_nummsg') is not null then
			-- ajouter le message aux map (ce n'est pas un message specifique a filtrer)
			v_counter:=1;
			pack_var.set_var($$plsql_unit,'vg_log_filter_limits',v_num,to_number(pack_var.get_var_number($$plsql_unit,'vg_sae_max_repeat_nummsg')));
			pack_var.set_var($$plsql_unit,'vg_log_filter_counters',v_num,v_counter);
			if v_counter>pack_var.get_var_number($$plsql_unit,'vg_log_filter_limits',v_num) then return; end if;
		end if;
	end if;


	-- si compteur a 0 (ou pas de filtre)
	select seq_log.nextval into v_temp from dual;
	pack_var.set_var($$plsql_unit,'vg_sae_last_log_id',v_temp);
  if v_partition_key is null then
  v_partition_key := pack_var.get_var_varchar2($$plsql_unit,'vg_sae_partition_key');
  end if;
	if v_table_name is null or v_contract_reference is null then
		insert INTO LOG_TABLE(
			partition_key, session_id, log_id, log_key, log_type, datetime, application, function, step, message, parameters, tech_func, severity,
			block_id, sub_block_id, depth, nummsg, root_task_id, task_id, process_desc, machine, log_struct_id, root_log_key, job_id
		) values (
			v_partition_key,
			pack_var.get_var_number($$plsql_unit,'vg_sae_log_session_id'),
			pack_var.get_var_number($$plsql_unit,'vg_sae_last_log_id'),
			v_log_key,
			-- GM_COMMENT pack_var.get_var_varchar2($$plsql_unit,'vg_sae_log_key'),
			NVL(v_log_type,'E'),
			v_timestamp,
			NVL(v_application,'F'),
			pack_utils.truncstr(v_function,100),
			pack_utils.truncstr(v_step,10),
			pack_utils.truncstr(v_message,4000),
			pack_utils.truncstr(v_parameters,4000),
			v_tech_func,
			v_severity,
			pack_var.get_var_number($$plsql_unit,'vg_sae_block_id'),
			pack_var.get_var_number($$plsql_unit,'vg_sae_sub_block_id'),
			--pack_var.get_var_number($$plsql_unit,'vg_sae_depth'),
			v_depth, --US111653
			v_nummsg,
			pack_var.get_var_number($$plsql_unit,'vg_sae_root_task_id'),
			pack_var.get_var_number($$plsql_unit,'vg_sae_task_id'),
			-- decode(pack_var.get_var_number($$plsql_unit,'vg_sae_depth'),1,pack_var.get_var_varchar2($$plsql_unit,'vg_sae_process_desc'),null),
			decode(v_depth,1,pack_var.get_var_varchar2($$plsql_unit,'vg_sae_process_desc'),null), --US111653
			sys_context('userenv','host'),
      v_log_struct_id,  --US109549
			pack_var.get_var_varchar2($$plsql_unit,'vg_sae_root_log_key'),
			pack_var.get_var_number($$plsql_unit,'vg_sae_job_id')
		);
	else
		INSERT INTO LOG_TABLE_DEAL(
			partition_key, session_id, log_id, log_key, log_type, DATETIME, APPLICATION, FUNCTION, step, message, PARAMETERS, tech_func, severity,
			block_id, sub_block_id, depth, nummsg, root_task_id, task_id, process_desc, table_name, contract_reference, machine, log_struct_id,
			root_log_key, job_id
		) values (
			v_partition_key,
			pack_var.get_var_number($$plsql_unit,'vg_sae_log_session_id'),
			pack_var.get_var_number($$plsql_unit,'vg_sae_last_log_id'),
			v_log_key,
			-- GM_COMMENT pack_var.get_var_varchar2($$plsql_unit,'vg_sae_log_key'),
			NVL(v_log_type,'E'),
			v_timestamp,
			NVL(v_application,'F'),
			pack_utils.truncstr(v_function,100),
			pack_utils.truncstr(v_step,10),
			pack_utils.truncstr(v_message,4000),
			pack_utils.truncstr(v_parameters,4000),
			v_tech_func,
			v_severity,
			pack_var.get_var_number($$plsql_unit,'vg_sae_block_id'),
			pack_var.get_var_number($$plsql_unit,'vg_sae_sub_block_id'),
			-- pack_var.get_var_number($$plsql_unit,'vg_sae_depth'),
			v_depth, --US111653
			v_nummsg,
			pack_var.get_var_number($$plsql_unit,'vg_sae_root_task_id'),
			pack_var.get_var_number($$plsql_unit,'vg_sae_task_id'),
			--decode(pack_var.get_var_number($$plsql_unit,'vg_sae_depth'),1,pack_var.get_var_varchar2($$plsql_unit,'vg_sae_process_desc'),null),
			decode(v_depth,1,pack_var.get_var_varchar2($$plsql_unit,'vg_sae_process_desc'),null), --US111653
			v_table_name,
			v_contract_reference,
			sys_context('userenv','host'),
      v_log_struct_id,  --US109549
			pack_var.get_var_varchar2($$plsql_unit,'vg_sae_root_log_key'),
			pack_var.get_var_number($$plsql_unit,'vg_sae_job_id')
		);
	end if;
	COMMIT;
END sae_log_write;


/**************************************************************************************************************
Name          : sae_log_end
Parameters    : v_unused - No more used.
                v_update_timestamps - To update start and end timestamp of the log struct to match the first log in the session.
                v_log_struct_id - Struct id of the log which needs to be ended.
Description   : This procedure allows to end current log session. This procedure is in autonomous transaction.
                US109549
Revision Log  : 1. 10/3/2013. Amit
***************************************************************************************************************/
PROCEDURE sae_log_end(v_unused IN CHAR DEFAULT null, v_update_timestamps in char default 'N', v_log_struct_id in number, v_context_id in number default null) IS
v_start_timestamp timestamp;
v_end_timestamp timestamp;
PRAGMA autonomous_transaction;
v_nb_errors number;
v_nb_warnings number;
v_nb_infos number;
v_nb_perfs number;
v_parent_log_struct_id number;  --US109549
v_depth number;
v_off boolean:=false;
v_temp number;
v_temp_vc varchar2(4000);
--US111653
v_total_errors number;
v_total_warnings number;
v_total_infos number;
v_total_perfs number;

v_nb_contexts number;
v_pk_rd_ws_list varchar2(4000);

v_sql varchar2(32000);
type t_log_message is record(
	log_type char(1),
	message varchar2(4000),
	parameters varchar2(4000),
	nummsg number
);
type t_log_messages is ref cursor;
c_log_messages t_log_messages;
v_rec t_log_message;
v_root_task_id number;
v_log_key log_struct.log_key%type;

BEGIN
	if not pack_var.get_var_boolean($$plsql_unit,'vg_sae_log_enable') then return; end if;
	-- it is more efficient and more readable to store some vars locally and then copy back.
    --US109549
	--v_depth := pack_var.get_var_number($$plsql_unit,'vg_sae_depth');
	-- US111653
	select depth into v_depth from log_struct where log_struct_id = v_log_struct_id;

	if v_depth=1 then

		-- faire un resume des messages filtres
		for rec in (select to_char(nummsg) as nummsg,type as type_msg,text from messages where language is null order by nummsg) loop
			if pack_var.exists_entry($$plsql_unit,'vg_log_filter_counters',rec.nummsg) then
				if pack_var.get_var_number($$plsql_unit,'vg_log_filter_counters',rec.nummsg)>pack_var.get_var_number($$plsql_unit,'vg_log_filter_limits',rec.nummsg) then
					sae_log_write(rec.type_msg,'Z',null,null,'Log message id #'||rec.nummsg||' occurred '||pack_var.get_var_number($$plsql_unit,'vg_log_filter_counters',rec.nummsg)||' time(s) and has been repeated only '||pack_var.get_var_number($$plsql_unit,'vg_log_filter_limits',rec.nummsg)||' time(s)','Original message text is: '||rec.text,null,null,null,null,null,v_log_struct_id,v_context_Id);  --US109549
				end if;
			end if;
		end loop;
	end if;

  select parent_log_struct_id into v_parent_log_struct_id from log_struct where log_struct_id=v_log_struct_id;  --US109549

    if v_parent_log_struct_id is null then  --US109549
		-- faire un resume des message de log_table_deal
		-- on groupe par table_name,nummsg
		-- il faut desactiver le contexte car il se peut qu'il y ait du log deal dans d'autres contextes
		-- on ne desactive le contexte que si c'est necessaire: le curseur c peut etre plombant
		select count(*), csv_list('''' || pack_install.partition_name('Y','Y', context_id) || '''') as pk_rd_ws_list -- r55382
		into v_nb_contexts, v_pk_rd_ws_list
		from (
			select distinct context_id
			from log_struct
			start with log_struct_id=v_log_struct_id
			connect by prior log_struct_id=parent_log_struct_id
		);

		-- Construire le curseur
		v_sql := 'select decode(ltd.log_type,0,''I'',1,''W'',2,''E'') as log_type,' || chr(10);
		v_sql := v_sql || 'case' || chr(10);
		v_sql := v_sql || '	when ltd.nummsg is null then ''Undefined log message''' || chr(10);
		v_sql := v_sql || '	else ''Log message id #''||to_char(ltd.nummsg)' || chr(10);
		v_sql := v_sql || 'end||'' occurred ''||to_char(ltd.counter)||'' time(s) on ''||ltd.table_name||'' contracts table, see log_table_deal for more details'' as message,' || chr(10);
		v_sql := v_sql || 'case when m.nummsg is null then null else ''Original message text is: ''||m.text end as parameters,' || chr(10);
		v_sql := v_sql || 'm.nummsg' || chr(10);
		v_sql := v_sql || 'from (' || chr(10);
		v_sql := v_sql || '	select table_name,nummsg,count(*) as counter,max(decode(log_type,''I'',0,''W'',1,''E'',2)) as log_type' || chr(10);	-- incoherence entre messages.type et le type renseigne au moment du f_logf
		v_sql := v_sql || '	from log_table_deal' || chr(10);
		v_sql := v_sql || '	where log_struct_id in (select log_struct_id from log_struct start with log_struct_id=:v_log_struct_id connect by prior log_struct_id=parent_log_struct_id)' || chr(10);
		v_sql := v_sql || '	and nummsg>0' || chr(10);			-- jsc genere du log_type=P dans log_table_deal avec nummsg=0
		v_sql := v_sql || '	and log_type<>''P''' || chr(10);		-- pour etre sur
		if v_nb_contexts > 1 then
			v_sql := v_sql || '	and partition_key in (' || v_pk_rd_ws_list || ')' || chr(10); -- r55382
		end if;
		v_sql := v_sql || '	group by table_name,nummsg' || chr(10);
		v_sql := v_sql || '	order by table_name,nummsg' || chr(10);
		v_sql := v_sql || '	) ltd, messages m' || chr(10);
		v_sql := v_sql || 'where m.language is null' || chr(10);
		v_sql := v_sql || 'and m.nummsg(+)=ltd.nummsg';

		if v_nb_contexts > 1 then
			pack_context.contextid_disable;
			v_off:=true;
			begin
				open c_log_messages for v_sql using v_log_struct_id;
				pack_context.context_enabled;
				v_off:=false;
			exception when others then
				pack_context.context_enabled;
				v_off:=false;
				raise;
			end;
		else
			open c_log_messages for v_sql using v_log_struct_id;
		end if;
		while true loop
			fetch c_log_messages into v_rec;
			exit when c_log_messages%notfound;
			sae_log_write(v_rec.log_type,'Z',null,null,v_rec.message,v_rec.parameters,null,null,null,null,null,null,v_log_struct_id,v_context_Id);
		end loop;
		close c_log_messages;
	end if;

	-- Ajouter un marqueur de LOG_END
	select log_key into v_log_key from log_struct where log_struct_id=v_log_struct_id;
	v_root_task_id:=pack_var.get_var_number($$plsql_unit,'vg_sae_root_task_id');
	sae_log_write( 'I', 'Z', 'LOG_END', null, 'End '||v_log_key,null,null,null,null,null,null,null,v_log_struct_id,v_context_Id);  --US109549

	-- mettre a jour les compteurs dans log_struct et le timestamp de fin
	-- on recupere les compteurs
	sae_update_counters(v_log_struct_id);  --US109549

	--US111653
	update log_struct
	set start_timestamp = nvl(v_start_timestamp, start_timestamp),
		end_timestamp=nvl(v_end_timestamp, systimestamp)
	where log_struct_id=v_log_struct_id;
	commit;
    --US111653
    select total_errors, total_warnings, total_infos, total_perfs into v_nb_errors, v_nb_warnings, v_nb_infos, v_nb_perfs from log_struct where log_struct_id=v_log_struct_id;

	pack_var.set_var($$plsql_unit,'vg_sae_log_write',false);
	-- GM_COMMENT pack_var.set_var($$plsql_unit,'vg_sae_log_key',to_char(null));

	-- BC 4354: Gestion de la pile des appels de LOG_BEGIN / LOG_END
	IF v_depth > 0 then

		delete
		  from log_stack ls
		 where DEPTH = V_DEPTH
		   and exists (select NULL
						 from LOG_STRUCT LS
						where LS.PARENT_LOG_STRUCT_ID = V_PARENT_LOG_STRUCT_ID
						  and LS.SESSION_ID = LS.SESSION_ID);


		v_depth := v_depth -1;

		if v_depth=0 then
			-- remis a null
			pack_var.set_var($$plsql_unit,'vg_sae_process_desc',to_char(null));
			pack_var.set_var($$plsql_unit,'vg_sae_task_id',to_number(null));
			pack_var.set_var($$plsql_unit,'vg_sae_root_task_id',to_number(null));
			pack_var.set_var($$plsql_unit,'vg_sae_job_id',to_number(null));
			pack_var.set_var($$plsql_unit,'vg_sae_root_log_key',to_char(null));
		end if;

		IF v_depth > 0 THEN
			begin
				SELECT block_id, log_key INTO v_temp, v_temp_vc FROM log_stack WHERE depth = v_depth AND session_id = pack_var.get_var_number($$plsql_unit,'vg_sae_log_session_id');
				pack_var.set_var($$plsql_unit,'vg_sae_sub_block_id',v_temp);
				-- GM_COMMENT pack_var.set_var($$plsql_unit,'vg_sae_log_key',v_temp_vc);
			exception
				when no_data_found then
					-- Il y a eu une erreur dans l'empilement des logs ; on ignore cette erreur pour eviter de planter un process a cause du log
					sae_log_write( 'W', 'Z', 'LOG_END', '1', 'Log_stack error, depth is ' || v_depth || ' and there is no more record in log_stack table with session_id=' || pack_var.get_var_number($$plsql_unit,'vg_sae_log_session_id'), SQLERRM,null,null,null,null,null,v_log_struct_id,v_context_Id);
					pack_var.set_var($$plsql_unit,'vg_sae_log_write',false);
					-- GM_COMMENT pack_var.set_var($$plsql_unit,'vg_sae_log_key',to_char(null));
					v_depth:=0;
			end;
		end if;
	end if;

	-- recuperer le log_struct_id parent

	if v_parent_log_struct_id is not null then
   select TOTAL_ERRORS,TOTAL_WARNINGS,TOTAL_INFOS,TOTAL_PERFS into v_total_errors,v_total_warnings,v_total_infos,v_total_perfs from log_struct where log_struct_id = v_parent_log_struct_id;
		v_total_errors := v_total_errors+v_nb_errors;
    v_total_warnings := v_total_warnings+v_nb_warnings;
    v_total_infos := v_total_infos+v_nb_infos;
    v_total_perfs := v_total_perfs+v_nb_perfs;

    update log_struct
		set total_errors=v_total_errors,
		total_warnings=v_total_warnings,
		total_infos=v_total_infos,
		total_perfs=v_total_perfs,
        end_timestamp = systimestamp
		where log_struct_id=v_parent_log_struct_id
		returning parent_log_struct_id
		into v_temp;
	end if;
	pack_var.set_var($$plsql_unit,'vg_sae_depth',v_depth);

	-- tracking
	if v_root_task_id is not null and nvl(pack_parameters.get_administration_parameters('GENERAL','ENABLE_PERFORMANCE_TRACKING'),'N') = 'Y' then
		execute immediate 'begin pack_tracking.step(:task_id,''A'',:log_key); end;' using v_root_task_id,v_log_key;
	end if;
	COMMIT;
  ----dbms_output.put_line('Ending log '|| v_log_struct_id);  --US109549
END sae_log_end;

/**************************************************************************************************************
Name          : sae_update_counters
Parameters    : v_log_struct_id - Struct id of the log which needs to be ended.
Description   : This procedure is used to update the counters.
Revision Log  : 1. 10/3/2013. Amit
***************************************************************************************************************/
procedure sae_update_counters(v_log_struct_id in number) is
begin

  if v_log_struct_id is null then return; end if;
		-- US111653
		pack_var.set_var($$plsql_unit,'vg_sae_counters','W',0);
		pack_var.set_var($$plsql_unit,'vg_sae_counters','I',0);
		pack_var.set_var($$plsql_unit,'vg_sae_counters','P',0);
end;

/**************************************************************************************************************
Name          : sae_log_begin
Parameters    : v_log_key - session key. If null, a key is automatically created,
                v_unused - No more used.
                v_task_id - Id of the task, NULL possible
                v_log_desc - Description of the session log, NULL possible
Description   : This procedure starts a log session and fixes the key log_key in global variable v_current_log_key.

                This procedure must be called before starting log of any process.
                Procedure log_end must be imperatively called to end log session.
                This procedure is in autonomous transaction.
                US109549
Revision Log  :
1. 10/03/2013 Amit
2. 12/03/2013 GM		    There was an 'intermittent' problem with QA5 being unable to delete/update models - research indicated user_id
--							was being set to -1 and logs indicated that the procedure pack_sae_utils.sae_log_begin where the error originated.
--							The user_id had to be forcibly set to a valid value by opening the context within this procedure
--							to resume normal functioning of DML on the model screen.
3. 12/05/2013 GM			Passed current context_id while calling pack_context.contextid_open().
***************************************************************************************************************/
function sae_log_begin(
	v_log_key IN VARCHAR2,
	v_unused IN CHAR DEFAULT null,
	v_task_id in number default null,
	v_log_desc in varchar2 default null,
	v_job_id in number default null,
    v_parent_log_struct_id number default null,  --US109549
    v_erswf_id in number default null, --US140691
	v_erswf_task_id in number default null  --US140691
) return number IS
PRAGMA autonomous_transaction;
v_block_id NUMBER;
v_parameter_value VARCHAR2(400);
v_flag_purge_log char;
v_pk VARCHAR2(30);
v_root_task_ids t_number_table;
i pls_integer;
v_is_cooperator char:='N';
v_parent_task_id number;
v_purge_last_log char;
c pack_utils.t_ref_cursor;
v_log_key_function varchar2(61);
v_context_id number;
v_nb_alives number;
v_temp number;
v_temp_vc varchar2(4000);
v_rlk log_struct.root_log_key%type;
v_root_task_id number;
v_log_struct_id number;  --US109549
v_depth number; -- US111653
b boolean;
vv_log_key log_struct.log_key%type;
BEGIN

  create_tmp_cd_sessions;

  pack_context.contextid_open(v_context_id=>pack_install.get_context_id);

  -- indicateur pas de log
  pack_var.set_var($$plsql_unit,'vg_sae_log_write',false);
	-- information vg_log_session_id is null
  if pack_var.get_var_number($$plsql_unit,'vg_sae_log_session_id') is null then
		pack_var.set_var($$plsql_unit,'vg_sae_log_session_id',to_number(pack_install.get_fermat_sid()));
	end if;

	-- log Active ?
	pack_var.set_var($$plsql_unit,'vg_sae_log_enable',(to_number(pack_parameters.get_user_parameters('LOG_TABLE','BULK_SIZE'))>0));
	if not pack_var.get_var_boolean($$plsql_unit,'vg_sae_log_enable') then return 'Log is not enabled.'; end if;

	-- update FDW_LOG_QUERY
	pack_var.set_var($$plsql_unit,'vg_sae_save_dynamic_query',(pack_parameters.get_user_parameters('FDW', 'SAVE_DYNAMIC_QUERY') = 'T'));

	-- Update counters in log_struct
  sae_update_counters(v_log_struct_id);  --US109549

	-- It disables the filter is it is active
	pack_var.set_var($$plsql_unit,'vg_sae_log_filter_info',false);

	-- Managing the call stack LOG_BEGIN / LOG_END
	--pack_var.set_var($$plsql_unit,'vg_sae_depth',nvl(pack_var.get_var_number($$plsql_unit,'vg_sae_depth'),0)+1);
    -- US111653
    if v_parent_log_struct_id is not null then
		begin
		  select depth into v_depth from log_struct where log_struct_id = v_parent_log_struct_id;
		  exception when others then v_depth := 0;
		end;
    else
    v_depth := 0;
    end if;

  v_depth := nvl(v_depth,0)+1;

	-- Recover the parent log struct id and pk
	if nvl(v_depth,0)>1 then  -- US111653
      pack_var.set_var($$plsql_unit,'v_parent_log_struct_id',to_number(pack_var.get_var_number($$plsql_unit,'vg_sae_log_struct_id')));  --US109549
	end if;

	-- Managing the partition_key of LOG_TABLE
	-- There may be hcanges in the context of log_begin so it is calculated everytime.
	IF pack_install.get_context_id IS NULL THEN
		pack_var.set_var($$plsql_unit,'vg_sae_partition_key',pack_install.partition_key('Y', 'Y', pack_install.get_ref_context_id));
	ELSE
		pack_var.set_var($$plsql_unit,'vg_sae_partition_key',pack_install.partition_key('Y', 'Y', pack_install.get_context_id));
	END IF;

	-- if a task id is provided, This is to check if the corresponding task parent id is available or not.
	if v_task_id is not null then
		pack_var.set_var($$plsql_unit,'vg_sae_task_id',v_task_id);
		-- The description of the process
		-- it is dynamic sql to avoid the dependencies
		-- we recovered the desc process, parent task, the purge flag
		-- If there is no parent task, on a parent_task_id=task id
		-- we do not remove log when the task belongs to a chain
		open c for 'select p.description,nvl(t.parent_task_id,t.task_id),decode(t.chain_id,null,p.remove_previous_log,''N''),p.log_key_function from process p, task t where t.process_id=p.process_id and t.task_id=:task_id' using v_task_id;
      fetch c into v_temp_vc,v_parent_task_id,v_purge_last_log,v_log_key_function;
      pack_var.set_var($$plsql_unit,'vg_sae_process_desc',v_temp_vc);
		close c;

		-- was a Cooperating? if so, extract the parent_log_struct_id that does not end the session log
		if v_parent_task_id<>pack_var.get_var_number($$plsql_unit,'vg_sae_task_id') then
			v_is_cooperator:='Y';
			select max(log_struct_id)
			into v_temp
			from log_struct
			where task_id=v_parent_task_id
			and end_timestamp is null
			and process_desc is not null;
      pack_var.set_var($$plsql_unit,'v_parent_log_struct_id',v_temp);   --US109549
      update log_struct set nb_coops=nb_coops+1 where log_struct_id=v_parent_log_struct_id;  --US109549
		end if;
	else
		-- we do not want the process desc
		pack_var.set_var($$plsql_unit,'vg_sae_process_desc',to_char(null));
	end if;

	if v_job_id is not null then
		pack_var.set_var($$plsql_unit,'vg_sae_job_id',v_job_id);
	end if;

	-- if there is no key logging
	IF v_log_key IS NULL THEN
		-- if no task id, a temporary key is created
		if v_task_id is null then
			vv_log_key := pack_log.log_create_new_key();
			-- GM_COMMENT pack_var.set_var($$plsql_unit,'vg_sae_log_key',pack_log.log_create_new_key());
  	-- if a function of generation of key
		elsif v_log_key_function is not null then
			vv_log_key := pack_var.get_var_varchar2($$plsql_unit,'vg_sae_process_desc');
			-- GM_COMMENT pack_var.set_var($$plsql_unit,'vg_sae_log_key',nvl(pack_utils.select_fetch_string('select '||v_log_key_function||'('||v_task_id||') from dual'),pack_var.get_var_varchar2($$plsql_unit,'vg_sae_process_desc')));
		-- The process will use the description
		else
			vv_log_key := to_char(pack_var.get_var_varchar2($$plsql_unit,'vg_sae_process_desc'));
			-- GM_COMMENT pack_var.set_var($$plsql_unit,'vg_sae_log_key',to_char(pack_var.get_var_varchar2($$plsql_unit,'vg_sae_process_desc')));
		end if;
	ELSE
		vv_log_key := substr(v_log_key,1,30);
		-- GM_COMMENT pack_var.set_var($$plsql_unit,'vg_sae_log_key',substr(v_log_key,1,30));
	END IF;

	-- Getting the root_log_key and root_task_id
    if v_parent_log_struct_id is not null then
		select root_log_key,root_task_id into v_temp_vc,v_temp from log_struct where log_struct_id=v_parent_log_struct_id;  --US109549
		pack_var.set_var($$plsql_unit,'vg_sae_root_log_key',v_temp_vc);
		v_root_task_id:=v_temp;
		pack_var.set_var($$plsql_unit,'vg_sae_root_task_id',v_root_task_id);
	else
		pack_var.set_var($$plsql_unit, 'vg_sae_root_log_key', vv_log_key);
		-- GM_COMMENT pack_var.set_var($$plsql_unit,'vg_sae_root_log_key',to_char(pack_var.get_var_varchar2($$plsql_unit,'vg_sae_log_key')));
		v_root_task_id:=to_number(pack_var.get_var_number($$plsql_unit,'vg_sae_task_id'));
		pack_var.set_var($$plsql_unit,'vg_sae_root_task_id',v_root_task_id);
	end if;

	SELECT seq_log_block.NEXTVAL INTO v_temp FROM dual;
	pack_var.set_var($$plsql_unit,'vg_sae_sub_block_id',v_temp);
	commit;

	-- Depth 1
		if v_depth = 1 then -- US111653
		-- add sub-partition to root_task_id if necessary
		pack_db_partitioning.add_list_part_subpart2(null,'log_table',to_char(pack_var.get_var_varchar2($$plsql_unit,'vg_sae_root_log_key')));
		pack_db_partitioning.add_list_part_subpart2(null,'log_table_deal',to_char(pack_var.get_var_varchar2($$plsql_unit,'vg_sae_root_log_key')));
		-- Empty the stack because the session_id has perhaps already been used
		delete from log_stack where session_id=pack_var.get_var_number($$plsql_unit,'vg_sae_log_session_id');
		-- US88625 - ORA-00600 [13031]
		EXECUTE IMMEDIATE 'delete from tmp_cd_sessions ';
		-- Non connected sessions
		EXECUTE IMMEDIATE 'insert into tmp_cd_sessions(session_id) select session_id from log_stack minus select session_id from cd_sessions c where exists (select 1 from gv$session g  where c.sid = g.sid and  c.serial# = g.serial# and c.inst_id =  g.inst_id ) ';
		EXECUTE IMMEDIATE 'delete from log_stack where session_id in (select session_id from tmp_cd_sessions)';
		commit;

		-- At level 1, it should be noted the block_id
		pack_var.set_var($$plsql_unit,'vg_sae_block_id',to_number(pack_var.get_var_number($$plsql_unit,'vg_sae_sub_block_id')));
		-- jmv: Now the sub-partitions if requested and only if we truncate is a root
		if v_parent_log_struct_id is null then  --US109549
			-- we look at the flag admin for non-process
			if v_purge_last_log is null then
				v_purge_last_log:=pack_parameters.get_administration_parameters('PURGE_LOG','PURGE_LAST_ACTION_LOG');
			end if;
			if v_purge_last_log='Y' then
				-- check that the session log with the same key are (completed) or (alive but the session id no longer exists)
				v_rlk:=pack_var.get_var_varchar2($$plsql_unit,'vg_sae_root_log_key');
				select count(*)
				into v_nb_alives
				from log_struct ls, (select pack_install.compute_fermat_sid(s.sid, s.serial#, s.inst_id) as fermat_sid from gv$session s) s
				where ls.root_log_key=v_rlk
				and ls.end_timestamp is null
				and ls.session_id=s.fermat_sid;

				if v_nb_alives=0 then
					pack_db_partitioning.truncate_list_part_subpart2(null,'log_table',to_char(pack_var.get_var_varchar2($$plsql_unit,'vg_sae_root_log_key')));
					pack_db_partitioning.truncate_list_part_subpart2(null,'log_table_deal',to_char(pack_var.get_var_varchar2($$plsql_unit,'vg_sae_root_log_key')));
					-- fdw_log_query log_struct should be cleaned.
					v_context_id:=pack_install.get_context_id();
					delete from fdw_log_query where log_struct_id in (select log_struct_id from log_struct where root_log_key=v_rlk and context_id=v_context_id);
					delete from log_struct where root_log_key=v_rlk and context_id=v_context_id;
					commit;
				end if;
			end if;
		END IF;
	END IF;
  --US111653
  INSERT INTO log_stack( session_id, block_id, depth, log_key ) VALUES ( pack_var.get_var_number($$plsql_unit,'vg_sae_log_session_id'), pack_var.get_var_number($$plsql_unit,'vg_sae_sub_block_id'), v_depth, pack_var.get_var_varchar2($$plsql_unit,'vg_sae_log_key') );

	-- update log_struct
	insert into log_struct(
		log_struct_id, start_timestamp, log_key, task_id, depth, root_task_id, session_id, nb_errors, nb_warnings, nb_infos, nb_perfs, process_desc, machine,
		root_log_key, total_errors, total_warnings, total_infos, total_perfs, nb_coops, parent_log_struct_id, is_cooperator, log_desc, context_id, job_id, ers_wf_id, ers_wf_task_id --US140691
	) values (
					seq_log_struct.nextval, systimestamp, vv_log_key, pack_var.get_var_number($$plsql_unit, 'vg_sae_task_id'),
	-- GM_COMMENT	seq_log_struct.nextval, systimestamp, pack_var.get_var_varchar2($$plsql_unit, 'vg_sae_log_key'), pack_var.get_var_number($$plsql_unit, 'vg_sae_task_id'),
    v_depth,v_root_task_id, pack_var.get_var_number($$plsql_unit, 'vg_sae_log_session_id'), --US111653
		0, 0, 0, 0, pack_var.get_var_varchar2($$plsql_unit, 'vg_sae_process_desc'), sys_context('userenv','host'), pack_var.get_var_varchar2($$plsql_unit,'vg_sae_root_log_key'),
    0, 0, 0, 0, 0, v_parent_log_struct_id, v_is_cooperator, substr(v_log_desc,1,100), pack_install.get_context_id(),  --US109549
		pack_var.get_var_number($$plsql_unit, 'vg_sae_job_id'), v_erswf_id, v_erswf_task_id  --US140691
	)
	returning log_struct_id
	into v_temp;
	pack_var.set_var($$plsql_unit,'vg_sae_log_struct_id',v_temp);
  v_log_struct_id := pack_var.get_var_number($$plsql_unit, 'vg_sae_log_struct_id');  --US109549
	-- Set to 0
	pack_var.set_var($$plsql_unit,'vg_sae_counters','E',0);
	pack_var.set_var($$plsql_unit,'vg_sae_counters','W',0);
	pack_var.set_var($$plsql_unit,'vg_sae_counters','I',0);
	pack_var.set_var($$plsql_unit,'vg_sae_counters','P',0);

	-- Add a marker LOG_BEGIN (very important to do the delete)
	sae_log_write( 'I', 'Z', 'LOG_BEGIN', null, 'Begin '|| vv_log_key ,null,null,null,null,null,null,null,v_log_struct_id);  --US109549
	-- GM_COMMENT	sae_log_write( 'I', 'Z', 'LOG_BEGIN', null, 'Begin '||pack_var.get_var_varchar2($$plsql_unit,'vg_sae_log_key'),null,null,null,null,null,null,null,v_log_struct_id);  --US109549
	if v_nb_alives>0 then
		sae_log_write('I','Z','PACK_LOG.LOG_BEGIN',null,to_char(v_nb_alives)||' alive session(s) with "'||pack_var.get_var_varchar2($$plsql_unit,'vg_sae_root_log_key')||'" root log key: clean up has not been done',null,null,null,null,null,null,null,v_log_struct_id);  --US109549
	end if;

	-- load parameter filter
	if v_depth =1 then --US111653
  	-- on indique que les messages a filtrer ont ete charges pour eviter une boucle avec le log_write qui suit
	pack_var.set_var($$plsql_unit,'vg_sae_log_filter_info',false);

	-- filtrer tous les mesages d'infos ?
	b:=(pack_parameters.get_user_parameters('LOG_TABLE','FILTER_INFO')='Y');
	if b then
		sae_log_write('I','Z','PACK_LOG.LOAD_FILTER_PARAMS',null,'All log information messages will be filtered',null,null,null,null,null,null,null,v_log_struct_id);  --US109549
	end if;

	-- supprimer toutes les entrees
	pack_var.delete_entries($$plsql_unit,'vg_log_filter_counters');
	pack_var.delete_entries($$plsql_unit,'vg_log_filter_limits');

	-- nb max de fois ou un nummsg peut se repeter
	pack_var.set_var($$plsql_unit,'vg_sae_max_repeat_nummsg',to_number(pack_parameters.get_user_parameters('LOG_TABLE','MAXNUMBER_REPEATED_MSG')));
	if pack_var.get_var_number($$plsql_unit,'vg_sae_max_repeat_nummsg')<=0 then
		pack_var.set_var($$plsql_unit,'vg_sae_max_repeat_nummsg',to_number(null));
	else
		sae_log_write('I','Z','PACK_LOG.LOAD_FILTER_PARAMS',null,'All log messages with the same id will be repeated only '||pack_var.get_var_number($$plsql_unit,'vg_sae_max_repeat_nummsg')||' times (global parameter)',null,null,null,null,null,null,v_log_struct_id);  --US109549
	end if;

	-- charger les messages specifiques a filtrer
	for rec in (
		select to_char(f.nummsg) as nummsg,m.text,f.max_nb_messages
		from log_filter_msg f, messages m
		where f.expiry_date>=sysdate
		and f.nummsg=m.nummsg
	) loop
		sae_log_write('I','Z','PACK_LOG.LOAD_FILTER_PARAMS',null,'Specific log message to filter (repeat max='||rec.max_nb_messages||'): #'||rec.nummsg,'Original message text is: '||rec.text,null,null,null,null,null,null,v_log_struct_id);
		pack_var.set_var($$plsql_unit,'vg_log_filter_counters',rec.nummsg,0);
		pack_var.set_var($$plsql_unit,'vg_log_filter_limits',rec.nummsg,to_number(rec.max_nb_messages));
	end loop;

	-- maintenant, on applique le filtre
	pack_var.set_var($$plsql_unit,'vg_sae_log_filter_info',b);
  end if;
	-- tracking
	-- when the task id is information we execute actions for key log null (in tracking_config_step)
	if nvl(pack_parameters.get_administration_parameters('GENERAL','ENABLE_PERFORMANCE_TRACKING'),'N') = 'Y' then
		if v_task_id is not null then
			execute immediate 'begin pack_tracking.step(:task_id,''B'',null); end;' using v_task_id;
		elsif v_root_task_id is not null then
			execute immediate 'begin pack_tracking.step(:task_id,''B'',:log_key); end;' using v_root_task_id, vv_log_key;
		-- GM_COMMENT	execute immediate 'begin pack_tracking.step(:task_id,''B'',:log_key); end;' using v_root_task_id,pack_var.get_var_varchar2($$plsql_unit,'vg_sae_log_key');
		end if;
	end if;
	COMMIT;
  return v_log_struct_id;  --US109549
END sae_log_begin;

FUNCTION check_scenario_type_setup(inp_simulation_id number, inp_product_code VARCHAR2) return varchar2 is
v_prod_desc PARAMETERS.description%type;
v_result varchar2(1000);
v_simulation_type_id sae_simulation.simulation_type_id% TYPE;
v_scen_type_name sae_scenario_type.name% TYPE;
v_index number;
v_count number;
v_support_flag VARCHAR2(1);
BEGIN
  select simulation_type_id into v_simulation_type_id from sae_simulation where id=inp_simulation_id;

  v_support_flag:= 'Y';

  select count(description) into v_count
  from PARAMETERS where 1=1
  and TYPE_PARAMETER = 'APP_PRODUCTS'
  and TYPE_PARAMETER_2 = 'FORMULA'
  and data_used = inp_product_code;

  if v_count=0 then
    return 'N, Process is not mapped';
  end if;

  select description into v_prod_desc
  from PARAMETERS where 1=1
  and TYPE_PARAMETER = 'APP_PRODUCTS'
  and TYPE_PARAMETER_2 = 'FORMULA'
  and data_used = inp_product_code;

  v_result := 'For Product:'||v_prod_desc||'    Scenario Type:';

  v_index := 1;
  select count(*) into v_count from SAE_MANDATORY_PROCESS smp, sae_simtype_scentype sss where 1=1
  and smp.simtype_scentype_id=sss.id
  and sss.simulation_type_id = v_simulation_type_id
  and smp.process = inp_product_code ;

  if v_count=0 then
    return 'N, Process is not mapped';
  end if;

  FOR SIMTPYE_SCENTYPE IN
  (select
  smp.simtype_scentype_id,
  smp.process product_code,
  smp.is_mandatory,
  sss.simulation_type_id,
  sss.scenario_type_id
  from SAE_MANDATORY_PROCESS smp, sae_simtype_scentype sss where 1=1
  and smp.simtype_scentype_id=sss.id
  and sss.simulation_type_id = v_simulation_type_id
  and smp.process = inp_product_code
  )
  LOOP


    select name into v_scen_type_name
    from sae_scenario_type where 1=1
    and id = SIMTPYE_SCENTYPE.scenario_type_id;

    v_result := v_result||v_scen_type_name;

    if SIMTPYE_SCENTYPE.IS_MANDATORY='Y' then
      if check_scentype_ispopulated(inp_simulation_id, SIMTPYE_SCENTYPE.scenario_type_id) = 'N' then
        v_result := v_result||' Required But not populated';
        v_support_flag:= 'N';
      else
        v_result := v_result||' Required and populated';
      end if;
    else
      v_result := v_result||' Optional';
    end if;

    if v_index<v_count then
      v_result := v_result||',';
    end if;

    v_index:= v_index+1;

  END LOOP;

  if v_support_flag='Y' THEN
    v_result:= v_support_flag;
  else
    v_result:= v_support_flag||'    '||v_result;
  end if;

RETURN(v_result);

END check_scenario_type_setup;

FUNCTION check_sim_isplayable(inp_sim_id number) return VARCHAR2 is
v_isplayable varchar2(1);
v_checkpopulate varchar2(1);
v_simtype_id number;
v_count number;
BEGIN
  v_checkpopulate:='N';
  v_isplayable:= 'Y';
  select simulation_type_id into v_simtype_id from sae_simulation where id=inp_sim_id;

  for simtype_scentype in (select * from sae_simtype_scentype where simulation_type_id=v_simtype_id)
  loop
    for mandatory_mapping in (select smp.process, sss.simulation_type_id, sss.scenario_type_id, smp.is_mandatory
    from sae_mandatory_process smp, sae_simtype_scentype sss
    where smp.simtype_scentype_id=sss.id
    and sss.simulation_type_id = v_simtype_id
    and scenario_type_id=simtype_scentype.scenario_type_id)
    loop
      select count(*) into v_count from application where product=mandatory_mapping.process;

      if v_count>0 then
        if mandatory_mapping.is_mandatory='Y' then
          v_checkpopulate:= 'Y';
          if v_checkpopulate='Y' then
            if check_scentype_ispopulated(inp_sim_id, simtype_scentype.scenario_type_id)='N' then
              v_isplayable:='N';
            end if;
          end if;
        end if;
      end if;
    end loop;
  end loop;
  return v_isplayable;
END check_sim_isplayable;

FUNCTION check_sim_ispopulated(inp_sim_id number) return VARCHAR2 is
v_result varchar2(1);
BEGIN
v_result := 'Y';
for sim_scentype in (select * from sae_sim_scentype where simulation_id=inp_sim_id)
loop
  if check_scentype_ispopulated(inp_sim_id, sim_scentype.scenario_type_id) = 'N' then
    v_result:='N';
  end if;
end loop;
return v_result;
END check_sim_ispopulated;

FUNCTION check_scentype_ispopulated(inp_sim_id number, inp_scentype_id number) return VARCHAR2 is
v_custom_param_set_id number;
v_sae_custom_param_set sae_custom_param_set%rowtype;
v_result varchar2(100);
BEGIN
  v_result := 'N';
  select custom_scentype_param_set into v_custom_param_set_id from sae_sim_scentype where simulation_id =inp_sim_id and scenario_type_id=inp_scentype_id;

  select * into v_sae_custom_param_set from sae_custom_param_set where id=v_custom_param_set_id;

  ----dbms_output.put_line('[use_existing_scenario]='||v_sae_custom_param_set.use_existing_scenario);
  ----dbms_output.put_line('[download_new_baseline]='||v_sae_custom_param_set.download_new_baseline);
  ----dbms_output.put_line('[baseline_id]='||v_sae_custom_param_set.baseline_id);

  ----dbms_output.put_line(v_sae_custom_param_set.use_existing_scenario);

  if v_sae_custom_param_set.to_use = 1 then

    if v_sae_custom_param_set.use_existing_scenario = 1 then
      ----dbms_output.put_line('check for selected value');
      if v_sae_custom_param_set.exsisting_scen_id is not null then
        ----dbms_output.put_line('use exist scenario, and value already selected');
        v_result := 'Y';
      end if;
    end if;

    if v_sae_custom_param_set.use_existing_scenario = 0 then
      if v_sae_custom_param_set.download_new_baseline = 0 then
        if v_sae_custom_param_set.baseline_id is null then
          ----dbms_output.put_line('existing baseline id missing');
          v_result := 'N';
        else
          ----dbms_output.put_line('use existing baseline');
          v_result := 'Y';
        end if;
      end if;
      if v_sae_custom_param_set.download_new_baseline = 1 then
        ----dbms_output.put_line('check baseline mandatory input');
        if chk_sim_scentype_inp_populated(inp_sim_id, inp_scentype_id) = 'Y' then
          v_result := 'Y';
        end if;
      end if;
    end if;

  end if;

  if v_sae_custom_param_set.to_use = 2 then
    ----dbms_output.put_line('process externally');
    if v_sae_custom_param_set.external_setting = 0 then
      ----dbms_output.put_line('use transformation only');
      v_result := 'Y';
    end if;
    if v_sae_custom_param_set.external_setting = 1 then
      ----dbms_output.put_line('use external setting');
      if chk_sim_scentype_inp_populated(inp_sim_id, inp_scentype_id) = 'Y' then
        v_result := 'Y';
      end if;

    end if;
  end if;

  return v_result;

END check_scentype_ispopulated;

FUNCTION check_scenset_type_setup(inp_scenset_id number, inp_product_code VARCHAR2) return varchar2 is
v_result varchar2(1000);
v_support_flag VARCHAR2(1);
v_simu_id sae_simulation.id%type;
v_count number;
BEGIN
  select count(description) into v_count
  from PARAMETERS where 1=1
  and TYPE_PARAMETER = 'APP_PRODUCTS'
  and TYPE_PARAMETER_2 = 'FORMULA'
  and data_used = inp_product_code;

  if v_count=0 then
    return 'N  product code not found';
  end if;

  select count(*) into v_count from sae_simulation_set_sim where simulation_set_id=inp_scenset_id;
  if v_count=0 then
    return 'N, no scenario in scenario set';
  end if;

  select count(*) into v_count from
  sae_simulation_set_sim ssss,
  sae_simulation ss,
  sae_simtype_scentype sss,
  sae_mandatory_process smp
  where
  simulation_set_id=inp_scenset_id and
  ss.ID = ssss.SIMULATION_ID and
  ss.SIMULATION_TYPE_ID = sss.SIMULATION_TYPE_ID and
  sss.ID = smp.SIMTYPE_SCENTYPE_ID
  and smp.PROCESS =inp_product_code;
  if v_count=0 then
    return 'N, product code not found in scenario set';
  end if;

  v_support_flag:='Y';
  v_result:= '';
  FOR simulation_set_sim IN
  (select * from sae_simulation_set_sim where simulation_set_id=inp_scenset_id)
  LOOP
    ----dbms_output.put_line(simulation_set_sim.simulation_id);
    if check_scenario_type_setup(simulation_set_sim.simulation_id, inp_product_code) <> 'Y' then
      v_result:= v_result ||' scenario id '||simulation_set_sim.simulation_id||',';
      v_support_flag := 'N';
    end if;
  END LOOP;
  v_result:= v_support_flag || v_result;
  if v_support_flag='N' then
    v_result:= v_result || ' Not populated';
  end if;
return v_result;
END check_scenset_type_setup;

function chk_sim_scentype_inp_populated(inp_sim_id number, inp_scentype_id number) return varchar2 is
v_result varchar2(1);
v_external number;
v_to_use number;
v_custom_paramset_id number;
v_count number;
BEGIN
  v_result:= 'Y' ;

  select custom_scentype_param_set into v_custom_paramset_id from sae_sim_scentype where simulation_id=inp_sim_id and scenario_type_id = inp_scentype_id;
  select to_use, external_setting into v_to_use, v_external from sae_custom_param_set where id=v_custom_paramset_id;

  ----dbms_output.put_line('v_to_use='||v_to_use);
  if v_to_use=0 or v_to_use=1 then
    select count(*) into v_count from sae_scenariotype_provider ssp, sae_provider_input spi,sae_custom_param_set scps,sae_param_set_base_params spsbp where 1=1
    and scps.id=v_custom_paramset_id
    and spsbp.custom_param_set_id = scps.id
    and spi.key = spsbp.parameter_name
    and scps.scenario_type_id = ssp.scenario_type
    and ssp.provider_id=spi.provider_id;

    ----dbms_output.put_line('v_to_use 0 or 1 v_count'||v_count);
    if v_count=0 then
      v_result := 'N';
    else
      for sim_base_param in (
      select spi.key param_key, spi.is_mandatory is_mandatory, spsbp.parameter_value param_value
      from
      sae_scenariotype_provider ssp,
      sae_provider_input spi,
      sae_custom_param_set scps,
      sae_param_set_base_params spsbp
      where 1=1
      and scps.id=v_custom_paramset_id
      and spsbp.custom_param_set_id = scps.id
      and spi.key = spsbp.parameter_name
      and scps.scenario_type_id = ssp.scenario_type
      and ssp.provider_id=spi.provider_id)
      loop
        if sim_base_param.is_mandatory='Y' then
          if sim_base_param.param_value is null then
            v_result := 'N';
          end if;
        end if;
      end loop;
    end if;
  end if;

  if v_to_use = 2 then
    if v_external = 1 then
      select count(*) into v_count from sae_scenariotype_provider ssp,sae_provider_input spi,sae_custom_param_set scps,sae_param_set_ext_options spseo where 1=1
      and scps.id=v_custom_paramset_id
      and spseo.param_set_id = scps.id
      and scps.scenario_type_id = ssp.scenario_type
      and ssp.provider_id=spi.provider_id;

      if v_count=0 then
        v_result := 'N';
      else
        for sim_ext_param in (
        select spseo.parameter_name param_key, spi.is_mandatory is_mandatory, spseo.parameter_value param_value
        from
        sae_scenariotype_provider ssp,
        sae_provider_input spi,
        sae_custom_param_set scps,
        sae_param_set_ext_options spseo
        where 1=1
        and scps.id=v_custom_paramset_id
        and spseo.param_set_id = scps.id
        and scps.scenario_type_id = ssp.scenario_type
        and ssp.provider_id=spi.provider_id)
        loop
          if sim_ext_param.is_mandatory='Y' then
            if sim_ext_param.param_value is null then
              v_result := 'N';
            end if;
          end if;
        end loop;
      end if;
    end if;
  end if;
  return v_result;
end chk_sim_scentype_inp_populated;

PROCEDURE padstring (p_text  IN OUT  VARCHAR2) IS
    l_units  NUMBER;
    g_pad_chr VARCHAR2(1) := ' ';
BEGIN
    IF LENGTH(p_text) MOD 8 > 0 THEN
      l_units := TRUNC(LENGTH(p_text)/8) + 1;
      p_text  := RPAD(p_text, l_units * 8, g_pad_chr);
    END IF;
END;

FUNCTION encrypt_paramval (p_text  IN  VARCHAR2) RETURN RAW IS
   l_text       VARCHAR2(32767) := p_text;
   l_encrypted  RAW(32767);
   g_key     RAW(32767)  := UTL_RAW.cast_to_raw('12345678');
BEGIN
    padstring(l_text);
    DBMS_OBFUSCATION_TOOLKIT.DESEncrypt(input          => UTL_RAW.cast_to_raw(l_text),
                                        key            => g_key,
                                        encrypted_data => l_encrypted);

    RETURN l_encrypted;
END encrypt_paramval;

FUNCTION decrypt_paramval (p_raw  IN  RAW) RETURN VARCHAR2 IS
    l_decrypted  VARCHAR2(32767);
    g_key     RAW(32767)  := UTL_RAW.cast_to_raw('12345678');
    g_pad_chr VARCHAR2(1) := ' ';
BEGIN
    DBMS_OBFUSCATION_TOOLKIT.desdecrypt(input => p_raw,
                                        key   => g_key,
                                        decrypted_data => l_decrypted);

    RETURN RTrim(UTL_RAW.cast_to_varchar2(l_decrypted), g_pad_chr);
END;

PROCEDURE SCENDATA_DVIEW_CREATE(v_scenario_id NUMBER, v_table_name VARCHAR2 DEFAULT NULL)
IS

v_tab_name CD_FDW_STRUCTURE.TABLE_NAME%TYPE;
v_view_name VARCHAR2(30);
v_scenario_type SAE_SCENARIO_TYPE.ID%TYPE;
v_sql_text CLOB := ' SELECT 1 AS dataexists, ';

BEGIN

SELECT SCENARIO_TYPE
  INTO v_scenario_type
  FROM SAE_SCENARIO
 WHERE ID = v_scenario_id;

IF v_table_name IS NULL
THEN v_tab_name := 'SAE_SCENARIO_DATA_' || v_scenario_id;
ELSE v_tab_name := v_table_name;
END IF;

v_view_name := 'V_' || v_tab_name;

v_sql_text := ' CREATE OR REPLACE VIEW '  || v_view_name || ' AS '
		   || ' SELECT 1 AS dataexists, ssd.* '
		   || '   FROM ' || v_tab_name || ' ssd ';

EXECUTE IMMEDIATE v_sql_text;

PACK_SAE_UTILS.exec_ddl('SELECT * FROM DUAL', v_view_name);

EXCEPTION
WHEN OTHERS
THEN RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.SCENDATA_DVIEW_CREATE : ' || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));

END SCENDATA_DVIEW_CREATE;

PROCEDURE SCENDATA_DVIEW_DROP(v_scenario_id NUMBER, v_table_name VARCHAR2 DEFAULT NULL) IS

v_target_name VARCHAR2(30);

BEGIN

IF   v_table_name IS NULL
THEN v_target_name := 'V_SAE_SCENARIO_DATA_'||v_scenario_id;
ELSE v_target_name := 'V_'||v_table_name;
END  IF;

EXECUTE IMMEDIATE 'DROP VIEW ' || v_target_name;

EXCEPTION
WHEN OTHERS
THEN RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.SCENDATA_DVIEW_DROP : ' || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));

END SCENDATA_DVIEW_DROP;

PROCEDURE GATHER_SUBPART_STATS(v_tab_name VARCHAR2, v_subpart_col_id NUMBER) IS
v_subpart_name 			VARCHAR2(30);
v_exists				NUMBER;
v_recursion_level_max 	NUMBER := 60;
v_wait_time				NUMBER := 60;
e_lock_timeout 			EXCEPTION;
PRAGMA EXCEPTION_INIT(e_lock_timeout, -4021);
BEGIN

IF get_context(v_tab_name, v_subpart_col_id) <> 0 THEN

-- Step 0: Open context for given scenario
	pack_context.contextid_open(get_context(v_tab_name, v_subpart_col_id));

-- Step 1: Select sub-partition name for scenario Id belonging to context currently connected to.
	SELECT pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_tab_name, vv_value => v_subpart_col_id, v_default  => 'N', v_prefix => 'N', v_parenthesis => 'N')
	  INTO v_subpart_name
	  FROM dual;

	enable_parallel_processing;

-- Step 2: Gather stats at the correct level.
	IF   v_subpart_name IS NOT NULL
	THEN pack_stats.gather_table_stats(v_tab_name, v_subpart_name, degree => 16);
	ELSE pack_stats.gather_table_stats(v_tab_name, degree => 16);
	END  IF;

	disable_parallel_processing;

	pack_var.set_var($$plsql_unit,'vg_sae_recursion_level',0);

ELSE NULL;
END IF;

EXCEPTION
WHEN e_lock_timeout
THEN IF   pack_var.get_var_number($$plsql_unit,'vg_sae_recursion_level') <= v_recursion_level_max
	 THEN disable_parallel_processing;
		  pack_var.set_var($$plsql_unit,'vg_sae_recursion_level', pack_var.get_var_number($$plsql_unit,'vg_sae_recursion_level') + 1);
		  DBMS_LOCK.SLEEP(v_wait_time);
		  GATHER_SUBPART_STATS(v_tab_name, v_subpart_col_id);
	 ELSE disable_parallel_processing;
		  pack_var.set_var($$plsql_unit,'vg_sae_recursion_level',0);
		  RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.GATHER_SUBPART_STATS : ' || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));
	 END IF;
WHEN OTHERS
THEN disable_parallel_processing;
	 RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.GATHER_SUBPART_STATS : ' || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));
END GATHER_SUBPART_STATS;

PROCEDURE TAB_SUBPART_CREATE(v_tab_name VARCHAR2, v_subpart_col_id NUMBER) IS
v_subpart_name_1 	VARCHAR2(30);
v_subpart_name_2 	VARCHAR2(30);
v_subpart_name_default VARCHAR2(30);
v_step				NUMBER;
BEGIN

IF get_context(v_tab_name, v_subpart_col_id) <> 0
THEN

	v_step := 0;
	-- Step 0: Open context for given scenario
	pack_context.contextid_open(get_context(v_tab_name, v_subpart_col_id));

	v_step := 1;
	-- Step 1: Select sub-partition name for Id belonging to context currently connected to - this is to check before creation.
	SELECT pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_tab_name, vv_value => v_subpart_col_id, v_default  => 'N', v_prefix => 'N', v_parenthesis => 'N')
	  INTO v_subpart_name_1
	  FROM dual;

	v_step := 2;
	-- Step 2: Create sub-partition corresponding to necessary partition if does not exist.
	pack_db_partitioning.add_list_part_subpart2(v_owner => null, v_table_name => v_tab_name, v_value => v_subpart_col_id, v_handle_slave_tables => 'Y');

	v_step := 3;
	-- Step 3: Again, select sub-partition name for Id belonging to context currently connected to - this is to check if it got created.
	SELECT pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_tab_name, vv_value => v_subpart_col_id, v_default  => 'N', v_prefix => 'N', v_parenthesis => 'N')
	  INTO v_subpart_name_2
	  FROM dual;

	v_step := 4;
	-- Step 4: Rebuild index if the subpartition only got here!
	IF v_subpart_name_1 IS NULL AND v_subpart_name_2 IS NOT NULL
	THEN REBUILD_LCLIDX_SUBPART(v_tab_name => v_tab_name, v_subpart_col_id => v_subpart_col_id);
	ELSE CHK_LCLIDX_SUBPART(v_tab_name => v_tab_name, v_subpart_col_id => v_subpart_col_id);
	END IF;

    v_step :=5;
    --Step 5: Gather stats for the default subpartition and the subpartition created
    --        because when default subpartition is splitted statistics are deleted
    --        for both subpartitions (default and new), this is the normal behaviour

    --get default_supartition name
    SELECT replace (pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_tab_name, vv_value => v_subpart_col_id, v_default  => 'Y', v_prefix => 'N', v_parenthesis => 'N'), '"','')
    INTO v_subpart_name_default
    FROM dual;

    IF v_subpart_name_1 IS NULL AND v_subpart_name_2 IS NOT NULL THEN
      pack_stats.gather_table_stats(v_tab_name,partname=>v_subpart_name_default);
    --  pack_stats.gather_table_stats(v_tab_name,partname=>v_subpart_name_2);

    END IF;

ELSE NULL;
END IF;

EXCEPTION
WHEN OTHERS
THEN RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.TAB_SUBPART_CREATE @ STEP : ' || v_step || CHR(10) || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));

END TAB_SUBPART_CREATE;
------------------------
PROCEDURE REBUILD_LCLIDX_SUBPART(v_tab_name VARCHAR2, v_subpart_col_id NUMBER) IS
v_subpart_name 	VARCHAR2(30);
BEGIN

-- Only recreate for enterprise

IF pack_db_object.is_oracle_enterprise() = 'Y' AND get_context(v_tab_name, v_subpart_col_id) <> 0
THEN

-- Step 0: Open context for given scenario
pack_context.contextid_open(get_context(v_tab_name, v_subpart_col_id));

-- Step 1: Select sub-partition name for scenario Id belonging to context currently connected to.
	SELECT pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_tab_name, vv_value => v_subpart_col_id, v_default  => 'N', v_prefix => 'N', v_parenthesis => 'N')
	  INTO v_subpart_name
	  FROM dual;

-- Step 2: Rebuild local index on this specific sub-partition as it otherwise creates ORA-600 [qernsRowP]
	FOR i IN (SELECT TABLE_NAME FROM CD_FDW_STRUCTURE WHERE HIST_TABLE_NAME = v_tab_name AND OBJECT_TYPE = 'INDEX' AND TABLE_TYPE = 'PRIMARY') LOOP

		EXECUTE IMMEDIATE ' ALTER INDEX ' || i.TABLE_NAME || ' MODIFY SUBPARTITION ' || v_subpart_name || ' UNUSABLE';
		EXECUTE IMMEDIATE ' ALTER INDEX ' || i.TABLE_NAME || ' REBUILD SUBPARTITION ' || v_subpart_name;

	END LOOP;
ELSE NULL;
END IF;

EXCEPTION
	WHEN OTHERS
	THEN RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.REBUILD_LCLIDX_SUBPART : ' || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));

END REBUILD_LCLIDX_SUBPART;
------------------------
PROCEDURE CHK_LCLIDX_SUBPART(v_tab_name VARCHAR2, v_subpart_col_id NUMBER) IS
v_subpart_name 	VARCHAR2(30);
v_unusable_cnt  number;
BEGIN

	-- Only for enterprise
	IF pack_db_object.is_oracle_enterprise() = 'Y' AND get_context(v_tab_name, v_subpart_col_id) <> 0
	THEN
		-- Step 0: Open context for given scenario
		pack_context.contextid_open(get_context(v_tab_name, v_subpart_col_id));

		-- Step 1: Select sub-partition name for scenario Id belonging to context currently connected to.
		SELECT pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_tab_name, vv_value => v_subpart_col_id, v_default  => 'N', v_prefix => 'N', v_parenthesis => 'N')
		INTO v_subpart_name
		FROM dual;

		-- check if any subpartition is unusable
		-- may be loop here for 5 sec each iteration upto 30 sec max ??
		SELECT count(*)
		INTO v_unusable_cnt
		FROM user_ind_subpartitions
		WHERE index_name in ( SELECT TABLE_NAME FROM CD_FDW_STRUCTURE WHERE HIST_TABLE_NAME = v_tab_name AND OBJECT_TYPE = 'INDEX' AND TABLE_TYPE = 'PRIMARY')
					AND status != 'USABLE';
		--  wait
		IF v_unusable_cnt > 0 THEN
			dbms_lock.sleep(30);
		--ELSE log_me('CHK_LCLIDX_SUBPART','NO WAIT...!!');
		END IF;

	END IF;

END CHK_LCLIDX_SUBPART;
-------------------
PROCEDURE TAB_SUBPART_TRUNC(v_tab_name VARCHAR2, v_subpart_col_id NUMBER) IS
v_subpart_name 	VARCHAR2(30);
v_where_clause	VARCHAR2(4000);

BEGIN

v_where_clause := ' SCENARIO_ID = ' || v_subpart_col_id;

IF get_context(v_tab_name, v_subpart_col_id) <> 0

THEN

	-- Step 0: Open context for given scenario
	pack_context.contextid_open(get_context(v_tab_name, v_subpart_col_id));

	-- Step 1: Select sub-partition name for scenario Id belonging to context currently connected to.
	SELECT pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_tab_name, vv_value => v_subpart_col_id, v_default  => 'N', v_prefix => 'N', v_parenthesis => 'N')
	  INTO v_subpart_name
	  FROM dual;

	-- Step 2: Truncate subpartition if exists or Delete data if no subpartition exists

	IF   v_subpart_name IS NOT NULL
	THEN PACK_DB_PARTITIONING.TRUNCATE_LIST_PART_SUBPART2( v_owner	=> NULL, V_TABLE_NAME => v_tab_name,V_VALUE =>  v_subpart_col_id );
	ELSE PACK_DB_PARTITIONING.TRUNCATE_OR_DELETE_FROM_WHERE(v_owner => NULL, v_table_name => v_tab_name, v_where => v_where_clause, v_log => 'Y');
	END  IF;

	-- Step 3: Gather stats on sub-partition
	PACK_STATS.GATHER_TABLE_STATS(v_tab_name, v_subpart_name);

END IF;

EXCEPTION
WHEN OTHERS
THEN RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.TAB_SUBPART_TRUNC : ' || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));

END TAB_SUBPART_TRUNC;

PROCEDURE SCENDATA_TAB_CREATE_GTT(v_scenario_id NUMBER, v_table_name VARCHAR2 DEFAULT NULL)
IS

v_sd_tname CD_FDW_STRUCTURE.TABLE_NAME%TYPE := 'SAE_SCENARIO_DATA';
v_sd_pk	   CD_FDW_STRUCTURE.TABLE_NAME%TYPE := 'PK_SAE_SCENARIO_DATA';
v_tab_name CD_FDW_STRUCTURE.TABLE_NAME%TYPE;
v_pk_name  CD_FDW_STRUCTURE.TABLE_NAME%TYPE;
v_release  APPLICATION.RELEASE_NUMBER%TYPE;
t_sd 	   CD_FDW_STRUCTURE%ROWTYPE;

TYPE t_tab_col IS TABLE OF table_columns%ROWTYPE;
v_tc t_tab_col;

BEGIN

IF v_table_name IS NULL
THEN v_tab_name := v_sd_tname || '_' || v_scenario_id;
	 v_pk_name  := 'PK_SCENDATA_'|| v_scenario_id;
ELSE v_tab_name := v_table_name;
	 v_pk_name  := 'PK_'|| SUBSTR(v_table_name, 1, 26);
END IF;

select max(pack_utils.format_release(release_number)) into v_release from application;

SELECT * INTO t_sd FROM cd_fdw_structure
 WHERE table_name = v_sd_tname AND ROWNUM < 2;

INSERT INTO CD_FDW_STRUCTURE (TABLE_NAME,TABLE_TYPE,HIST_TABLE_NAME,OBJECT_TYPE,GROUP_INSTALL,INSTALLATION_ORDER,INIT_MODE,PARAMETER
,STANDARD_CUSTOM,SIZE_INITIAL,SIZE_NEXT,MODEL_OBJECT,DESCRIPTION,TABLESPACE_NAME,AUDIT_TRIGGER_INSERT,AUDIT_TRIGGER_UPDATE
,AUDIT_TRIGGER_DELETE,MASTER_TABLE,TO_IMPORT_TABLE,ROL_FERMAT_PRIVILEGES,RELEASE,RELEVANT_FOR,FPM_ID,DOC_REQUIRED,DEPRECATED_RELEASE
,BUFFER_POOL,PCT_FREE,LOGGING,INI_TRANS,COMPRESSION,IMPORTABLE,RECHECK_CONSTRAINTS,RECHECK_MODE,PARTITIONING_TYPE,PARTITIONING_COLUMN,
IS_PARTITIONED,FUNCTION_BASED,CHECKSUM,INIT_ON_INSTALL,INIT_ON_UPGRADE,INIT_ON_DEMAND,INIT_DATA_TYPE,INIT_MIGRATION_SET,IS_VERSIONED,
DW_NAME,IS_NOT_DEPARTITIONABLE,GENERATED,VISIBILITY,HASH_PARTITIONS_COUNT,COMP_CODE,OBJECT_TO_UPGRADE)
SELECT v_tab_name, 'SESSION', t_sd.hist_table_name, t_sd.object_type, t_sd.group_install, t_sd.installation_order, t_sd.init_mode, t_sd.parameter, t_sd.standard_custom, t_sd.size_initial, t_sd.size_next, t_sd.model_object, 'Temporary table for scenario ID: ' || TO_CHAR(v_scenario_id),t_sd.TABLESPACE_NAME,t_sd.AUDIT_TRIGGER_INSERT,t_sd.AUDIT_TRIGGER_UPDATE
,t_sd.AUDIT_TRIGGER_DELETE,t_sd.MASTER_TABLE,t_sd.TO_IMPORT_TABLE,t_sd.ROL_FERMAT_PRIVILEGES,v_release,t_sd.RELEVANT_FOR,'SAETMP',t_sd.DOC_REQUIRED,t_sd.DEPRECATED_RELEASE, t_sd.BUFFER_POOL,t_sd.PCT_FREE,t_sd.LOGGING,t_sd.INI_TRANS,t_sd.COMPRESSION,t_sd.IMPORTABLE,t_sd.RECHECK_CONSTRAINTS,
t_sd.RECHECK_MODE, NULL,NULL,'N',t_sd.FUNCTION_BASED,t_sd.CHECKSUM,t_sd.INIT_ON_INSTALL,t_sd.INIT_ON_UPGRADE,t_sd.INIT_ON_DEMAND,
t_sd.INIT_DATA_TYPE,t_sd.INIT_MIGRATION_SET,t_sd.IS_VERSIONED,t_sd.DW_NAME,'N',
t_sd.GENERATED,t_sd.VISIBILITY,t_sd.HASH_PARTITIONS_COUNT,t_sd.COMP_CODE,'N'
FROM dual;

SELECT * INTO t_sd FROM cd_fdw_structure
 WHERE table_name = v_sd_pk AND ROWNUM < 2;

INSERT INTO CD_FDW_STRUCTURE (TABLE_NAME,TABLE_TYPE,HIST_TABLE_NAME,OBJECT_TYPE,GROUP_INSTALL,INSTALLATION_ORDER,INIT_MODE,PARAMETER
,STANDARD_CUSTOM,SIZE_INITIAL,SIZE_NEXT,MODEL_OBJECT,DESCRIPTION,TABLESPACE_NAME,AUDIT_TRIGGER_INSERT,AUDIT_TRIGGER_UPDATE
,AUDIT_TRIGGER_DELETE,MASTER_TABLE,TO_IMPORT_TABLE,ROL_FERMAT_PRIVILEGES,RELEASE,RELEVANT_FOR,FPM_ID,DOC_REQUIRED,DEPRECATED_RELEASE
,BUFFER_POOL,PCT_FREE,LOGGING,INI_TRANS,COMPRESSION,IMPORTABLE,RECHECK_CONSTRAINTS,RECHECK_MODE,PARTITIONING_TYPE,PARTITIONING_COLUMN,
IS_PARTITIONED,FUNCTION_BASED,CHECKSUM,INIT_ON_INSTALL,INIT_ON_UPGRADE,INIT_ON_DEMAND,INIT_DATA_TYPE,INIT_MIGRATION_SET,IS_VERSIONED,
DW_NAME,IS_NOT_DEPARTITIONABLE,GENERATED,VISIBILITY,HASH_PARTITIONS_COUNT,COMP_CODE,OBJECT_TO_UPGRADE)
SELECT v_pk_name, t_sd.table_type, v_tab_name, t_sd.object_type, t_sd.group_install, t_sd.installation_order, t_sd.init_mode,  REGEXP_REPLACE(t_sd.parameter, 'partition_key,', '', 1, 1, 'i'),t_sd.standard_custom, t_sd.size_initial, t_sd.size_next, v_pk_name, 'Temporary PK for scenario ID: ' || TO_CHAR(v_scenario_id),t_sd.TABLESPACE_NAME,t_sd.AUDIT_TRIGGER_INSERT,t_sd.AUDIT_TRIGGER_UPDATE
,t_sd.AUDIT_TRIGGER_DELETE,t_sd.MASTER_TABLE,t_sd.TO_IMPORT_TABLE,t_sd.ROL_FERMAT_PRIVILEGES,v_release,t_sd.RELEVANT_FOR,'PKSAETMP',t_sd.DOC_REQUIRED,t_sd.DEPRECATED_RELEASE, t_sd.BUFFER_POOL,t_sd.PCT_FREE,t_sd.LOGGING,t_sd.INI_TRANS,t_sd.COMPRESSION,t_sd.IMPORTABLE,t_sd.RECHECK_CONSTRAINTS,
t_sd.RECHECK_MODE, NULL,NULL,'N',t_sd.FUNCTION_BASED,t_sd.CHECKSUM,t_sd.INIT_ON_INSTALL,t_sd.INIT_ON_UPGRADE,t_sd.INIT_ON_DEMAND,
t_sd.INIT_DATA_TYPE,t_sd.INIT_MIGRATION_SET,t_sd.IS_VERSIONED,t_sd.DW_NAME,t_sd.IS_NOT_DEPARTITIONABLE,
t_sd.GENERATED,t_sd.VISIBILITY,t_sd.HASH_PARTITIONS_COUNT,t_sd.COMP_CODE,'N'
FROM dual;

WITH v
  AS (
		SELECT column_name
		  FROM TABLE_COLUMNS
		 WHERE TABLE_NAME = 'SAE_SCENARIO_DATA'
		   AND ((COLUMN_NAME NOT LIKE 'NUMBER_ATTRIBUTE_%'
		   AND COLUMN_NAME NOT LIKE 'STRING_ATTRIBUTE_%'
		   AND COLUMN_NAME NOT LIKE 'DATE_ATTRIBUTE_%')
			OR (COLUMN_NAME IN (SELECT DISTINCT COLUMN_NAME
								  FROM SAE_FIELD
								  JOIN SAE_SCENARIO
									ON SAE_FIELD.SCENARIO_TYPE = SAE_SCENARIO.SCENARIO_TYPE
								 WHERE SAE_SCENARIO.ID = v_scenario_id
								   AND SAE_FIELD.FIELD_TYPE = 'W'
								))))
SELECT v_tab_name, t.COLUMN_NAME,DISPLAY_NAME,COLUMN_DESC,v_release,DATA_TYPE,NULLABLE,COLUMN_ID,DEFAULT_VALUE,AUDIT_FLAG_INSERT,AUDIT_FLAG_UPDATE,
AUDIT_FLAG_DELETE,PRODUCTS,'SAETMP',DEPRECATED_RELEASE,COMPUTE_STATS,HISTOGRAM_SIZE,SEQUENCE_NAME,IN_OUT_PARAM_COLUMN,INDEX_AUDIT,FORMAT
  BULK COLLECT INTO v_tc
  FROM table_columns t
  JOIN v ON t.COLUMN_NAME = v.COLUMN_NAME
 WHERE TABLE_NAME = v_sd_tname;

FORALL i IN v_tc.FIRST..v_tc.LAST
INSERT INTO TABLE_COLUMNS (table_name,column_name,display_name,column_desc,release,data_type,nullable,column_id,default_value,audit_flag_insert,audit_flag_update,audit_flag_delete,products,fpm_id,deprecated_release,compute_stats,histogram_size,sequence_name,in_out_param_column,index_audit,format)
values (v_tc(i).table_name,v_tc(i).column_name,v_tc(i).display_name,v_tc(i).column_desc,v_tc(i).release,v_tc(i).data_type,v_tc(i).nullable,v_tc(i).column_id
,v_tc(i).default_value,v_tc(i).audit_flag_insert,v_tc(i).audit_flag_update,v_tc(i).audit_flag_delete,v_tc(i).products,v_tc(i).fpm_id
,v_tc(i).deprecated_release,v_tc(i).compute_stats,v_tc(i).histogram_size,v_tc(i).sequence_name,v_tc(i).in_out_param_column,v_tc(i).index_audit
,v_tc(i).format);

COMMIT;

PACK_DDL.create_table(v_tab_name);
EXEC_DDL('SELECT * FROM DUAL', v_tab_name);

EXCEPTION
WHEN OTHERS
THEN RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.SCENDATA_TAB_CREATE : ' || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));

END SCENDATA_TAB_CREATE_GTT;

FUNCTION SCENDATA_TAB_RENAME(v_scenario_id NUMBER, v_table_name VARCHAR2 DEFAULT NULL, v_pk_name VARCHAR2, v_append VARCHAR2)
RETURN VARCHAR2
IS
v_original_table VARCHAR2(30);
v_renamed_table VARCHAR2(30);
BEGIN
	v_original_table := NVL(v_table_name, 'SAE_SCENARIO_DATA' || v_scenario_id);
	v_renamed_table := NVL(v_table_name, 'SAE_SCENARIO_DATA' || v_scenario_id) || v_append;
	BEGIN
		EXECUTE IMMEDIATE 'ALTER TABLE ' || v_original_table || ' RENAME TO ' || v_renamed_table;
	EXCEPTION
		WHEN OTHERS
		THEN RAISE_APPLICATION_ERROR(-20001, 'Failed during attempt to rename table ' || v_original_table || ' to ' || v_renamed_table);
	END;
	BEGIN
		EXECUTE IMMEDIATE 'ALTER INDEX ' || v_pk_name || ' RENAME TO ' || v_pk_name || v_append ;
	EXCEPTION
		WHEN OTHERS
		THEN EXECUTE IMMEDIATE 'ALTER TABLE ' || v_renamed_table || ' RENAME TO ' || v_original_table;
			 RAISE_APPLICATION_ERROR(-20002, 'Failed during attempt to rename index ' || v_pk_name || ' to ' || v_pk_name || v_append);
	END;
	BEGIN
		EXECUTE IMMEDIATE 'ALTER TABLE ' || v_renamed_table || ' RENAME CONSTRAINT ' || v_pk_name || ' TO ' || v_pk_name || v_append ;
	EXCEPTION
		WHEN OTHERS
		THEN EXECUTE IMMEDIATE 'ALTER TABLE ' || v_renamed_table || ' RENAME TO ' || v_original_table;
			 RAISE_APPLICATION_ERROR(-20003, 'Failed during attempt to rename constraint ' || v_renamed_table || '.' || v_pk_name || ' to ' || v_renamed_table || '.' || v_pk_name || v_append);
	END;
	RETURN v_renamed_table;
END SCENDATA_TAB_RENAME;

PROCEDURE SCENDATA_TAB_CREATE(v_scenario_id NUMBER,
							  v_table_name VARCHAR2 DEFAULT NULL,
							  v_loopback_scenario_id NUMBER DEFAULT NULL,
							  v_select VARCHAR2 DEFAULT NULL,
							  v_temp_table VARCHAR2 DEFAULT NULL,
							  v_source_table VARCHAR2 DEFAULT NULL,
							  v_where VARCHAR2 DEFAULT NULL)
IS

v_sd_tname 				CD_FDW_STRUCTURE.TABLE_NAME%TYPE := 'SAE_SCENARIO_DATA';
v_en_tname 				CD_FDW_STRUCTURE.TABLE_NAME%TYPE := 'SAE_ENTITY';
v_sd_pk	   				CD_FDW_STRUCTURE.TABLE_NAME%TYPE := 'PK_SAE_SCENARIO_DATA';
v_tab_name 				CD_FDW_STRUCTURE.TABLE_NAME%TYPE;
v_renamed_temp_table 	CD_FDW_STRUCTURE.TABLE_NAME%TYPE;
v_pk_name  				CD_FDW_STRUCTURE.TABLE_NAME%TYPE;
v_release  				APPLICATION.RELEASE_NUMBER%TYPE;
t_sd							CD_FDW_STRUCTURE%ROWTYPE;
v_ctx_id   				NUMBER;
v_part_key 				VARCHAR2(30);

v_tc_alias 			CLOB;
v_insert_col		CLOB;
v_sql 				CLOB;
v_cap_plan_parms 	BOOLEAN;
v_regular_parms 	BOOLEAN;

v_baseline_id 		SAE_BASELINE.ID%TYPE;
v_timeline_id 		SAE_TIMELINE.ID%TYPE;
v_scenariotype_id 	SAE_SCENARIO_TYPE.ID%TYPE;
v_counter NUMBER 	:= 0;
v_log_key         log_table.log_key%type := 'SCENDATA_TAB_'||v_scenario_id;
v_log_struct_id   number;
v_function        log_table.function%type := 'PACK_SAE_UTILS.SCENDATA_TAB_CREATE';
v_step            log_table.step%type ;
v_application     log_table.application%type := 'Y';
v_msg_param_str       varchar2(4000);

c_fpm_id  CD_FDW_STRUCTURE.FPM_ID%TYPE := 'SAETMP';

e_no_data EXCEPTION;

FUNCTION v_tc_alias_init(v_parameter NUMBER, v_tab_name CD_FDW_STRUCTURE.TABLE_NAME%TYPE) RETURN CLOB
AS
v_tc_alias 			CLOB;
BEGIN

	v_tc_alias := EMPTY_CLOB();

	IF v_parameter = 1
	THEN
		WITH ext AS (SELECT TRIM(REGEXP_SUBSTR (pair, '[^~]+', 1, 1,'i')) ssdtempcol, TRIM(REGEXP_SUBSTR (pair, '[^~]+', 1, 2,'i')) extcol
					  FROM (SELECT REGEXP_SUBSTR (v_select, '[^,]+', 1, ROWNUM,'i') pair
									 FROM DUAL CONNECT BY LEVEL <= REGEXP_COUNT(v_select, '[,]',1,'i') + 1)
						   )
		SELECT CSV_LIST_CLOB(COLUMN_NAME)
		  INTO v_tc_alias
		  FROM (  SELECT 'CAST(ssdtemp.' || TABLE_COLUMNS.COLUMN_NAME || ' AS ' || TABLE_COLUMNS.DATA_TYPE || ') AS ' || TABLE_COLUMNS.COLUMN_NAME COLUMN_NAME
					FROM TABLE_COLUMNS
				   WHERE TABLE_NAME = v_tab_name
					 AND COLUMN_NAME NOT IN (SELECT ssdtempcol
											   FROM ext)
				   UNION
				  SELECT 'CAST(NVL(ext.' || ext.extcol || ', ssdtemp.' || ext.ssdtempcol || ') AS ' || TABLE_COLUMNS.DATA_TYPE || ') AS ' || ext.ssdtempcol COLUMN_NAME
					FROM TABLE_COLUMNS
					JOIN ext
					  ON TABLE_COLUMNS.COLUMN_NAME = ext.ssdtempcol
				   WHERE TABLE_NAME = v_tab_name);
	ELSIF v_parameter = 2
	THEN
		WITH ext AS (SELECT TRIM(REGEXP_SUBSTR (pair, '[^~]+', 1, 1,'i')) ssdtempcol, TRIM(REGEXP_SUBSTR (pair, '[^~]+', 1, 2,'i')) extcol
					  FROM (SELECT REGEXP_SUBSTR (v_select, '[^,]+', 1, ROWNUM,'i') pair
									 FROM DUAL CONNECT BY LEVEL <= REGEXP_COUNT(v_select, '[,]',1,'i') + 1)
						   )
		SELECT CSV_LIST_CLOB(COLUMN_NAME)
		  INTO v_tc_alias
		  FROM (  SELECT COLUMN_NAME
		            FROM (
						  SELECT 'CAST(ssdtemp.' || TABLE_COLUMNS.COLUMN_NAME || ' AS ' || TABLE_COLUMNS.DATA_TYPE || ') AS ' || TABLE_COLUMNS.COLUMN_NAME COLUMN_NAME
								 ,TABLE_COLUMNS.COLUMN_ID
							FROM TABLE_COLUMNS
						   WHERE TABLE_NAME = v_tab_name
							 AND COLUMN_NAME NOT IN (SELECT ssdtempcol
													   FROM ext)
						   UNION
						  SELECT 'CAST( ssdtemp.' || ext.ssdtempcol || ' AS ' || TABLE_COLUMNS.DATA_TYPE || ') AS ' || ext.ssdtempcol COLUMN_NAME
								,TABLE_COLUMNS.COLUMN_ID
							FROM TABLE_COLUMNS
							JOIN ext
							  ON TABLE_COLUMNS.COLUMN_NAME = ext.ssdtempcol
						   WHERE TABLE_NAME = v_tab_name
					ORDER BY COLUMN_ID,COLUMN_NAME
					   ) );
	ELSIF v_parameter = 3
	THEN
		WITH ext AS (SELECT TRIM(REGEXP_SUBSTR (pair, '[^~]+', 1, 1,'i')) ssdtempcol, TRIM(REGEXP_SUBSTR (pair, '[^~]+', 1, 2,'i')) extcol
					  FROM (SELECT REGEXP_SUBSTR (v_select, '[^,]+', 1, ROWNUM,'i') pair
									 FROM DUAL CONNECT BY LEVEL <= REGEXP_COUNT(v_select, '[,]',1,'i') + 1)
						   )
		SELECT CSV_LIST_CLOB(COLUMN_NAME)
		  INTO v_tc_alias
		  FROM (  SELECT COLUMN_NAME
		            FROM (
						  SELECT TABLE_COLUMNS.COLUMN_NAME COLUMN_NAME
						        ,TABLE_COLUMNS.COLUMN_ID
							FROM TABLE_COLUMNS
						   WHERE TABLE_NAME = v_tab_name
							 AND COLUMN_NAME NOT IN (SELECT ssdtempcol
													   FROM ext)
						   UNION
						  SELECT ext.ssdtempcol COLUMN_NAME
						        ,TABLE_COLUMNS.COLUMN_ID
							FROM TABLE_COLUMNS
							JOIN ext
							  ON TABLE_COLUMNS.COLUMN_NAME = ext.ssdtempcol
						   WHERE TABLE_NAME = v_tab_name)
					ORDER BY COLUMN_ID,COLUMN_NAME);
	ELSE RAISE_APPLICATION_ERROR(-20001, 'v_tc_alias_init parameter ' || v_parameter || ' is invalid');
	END IF;

  RETURN v_tc_alias;

END;

BEGIN


v_log_struct_id := sae_log_begin (	v_log_key => v_log_key );

v_step := '1.0';
v_msg_param_str := substr('v_scenario_id: '||v_scenario_id||',v_table_name:'||NVL(v_table_name,'NULL')||',v_loopback_scenario_id:'||NVL(to_char(v_loopback_scenario_id),'NULL')||',v_select:'||NVL(v_select,'NULL')||',v_temp_table:'||NVL(v_temp_table,'NULL')||',v_source_table:'||NVL(v_source_table,'NULL')||',v_where:'||NVL(v_where,'NULL'), 1, 4000);
sae_log_write ('I',v_application,v_function,v_step ,'Parameters',v_msg_param_str,'T',1,null,null,null,null,v_log_struct_id);


v_cap_plan_parms := v_select IS NOT NULL AND v_source_table IS NOT NULL AND v_where IS NOT NULL;
v_regular_parms	 := v_select IS NULL AND v_source_table IS NULL AND v_where IS NULL;

IF v_regular_parms THEN NULL;
ELSIF v_cap_plan_parms THEN	NULL;
ELSE RAISE_APPLICATION_ERROR(-20003, 'Parameters are invalid!');
END IF;

IF v_table_name IS NULL
THEN v_tab_name := v_sd_tname || '_' || v_scenario_id;
	 v_pk_name  := 'PK_SCENDATA_'|| v_scenario_id;
ELSE v_tab_name := v_table_name;
	 v_pk_name  := 'PK_'|| SUBSTR(v_table_name, 1, 26);
END IF;

IF v_cap_plan_parms
THEN
	pack_stats.gather_table_stats(NVL(v_temp_table, v_sd_tname || '_' || v_scenario_id), degree => 16);
	v_renamed_temp_table := SCENDATA_TAB_RENAME(v_scenario_id, v_temp_table, v_pk_name, '_');
	 BEGIN
		SCENDATA_TAB_DROP(v_scenario_id);
	 EXCEPTION
		WHEN OTHERS THEN NULL;
	 END;
ELSE NULL;
END IF;

select max(pack_utils.format_release(release_number)) into v_release from application;

SELECT * INTO t_sd FROM cd_fdw_structure
 WHERE table_name = v_sd_tname AND ROWNUM < 2;

SELECT * INTO t_sd FROM cd_fdw_structure
 WHERE table_name = v_sd_pk AND ROWNUM < 2;

  v_step := '1.1';
  v_msg_param_str := substr('v_sd_tname: '||v_sd_tname||',v_release:'||NVL(v_release,'NULL'), 1, 4000);
	sae_log_write ('I',v_application,v_function,v_step ,v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);


  for i in (	WITH v
				  AS (  SELECT column_name
						  FROM TABLE_COLUMNS
						 WHERE TABLE_NAME = v_sd_tname
						   AND ((COLUMN_NAME NOT LIKE 'NUMBER_ATTRIBUTE_%'
						   AND COLUMN_NAME NOT LIKE 'STRING_ATTRIBUTE_%'
						   AND COLUMN_NAME NOT LIKE 'DATE_ATTRIBUTE_%')
							OR (COLUMN_NAME IN (SELECT DISTINCT COLUMN_NAME
												  FROM SAE_FIELD
												  JOIN SAE_SCENARIO
													ON SAE_FIELD.SCENARIO_TYPE = SAE_SCENARIO.SCENARIO_TYPE
												 WHERE SAE_SCENARIO.ID = v_scenario_id
												   AND SAE_FIELD.FIELD_TYPE  in ('W','R','G')
												))))
								, se
							  AS (SELECT v.column_name
									FROM v
									JOIN (SELECT column_name
											FROM TABLE_COLUMNS
										   WHERE TABLE_NAME = v_en_tname) se2
									ON v.column_name = se2.column_name)
					, al
				  AS (SELECT v_tab_name 				AS table_name,
							 CASE WHEN v.column_name = 'SCENARIO_ID'
								  THEN TO_CHAR(v_scenario_id)
								  ELSE 'sibd.' || v.column_name
							  END column_name_alias,
							 v.column_name 				AS column_name_orig,
							 t.DISPLAY_NAME,
							 t.COLUMN_DESC,
							 v_release release,
							 t.DATA_TYPE,
							 t.NULLABLE,
							 t.DEFAULT_VALUE,
							 t.AUDIT_FLAG_INSERT,
							 t.AUDIT_FLAG_UPDATE,
							 t.AUDIT_FLAG_DELETE,
							 t.PRODUCTS,
							 'SAETMP' fpm_id,
							 t.DEPRECATED_RELEASE,
							 t.COMPUTE_STATS,
							 t.HISTOGRAM_SIZE,
							 t.SEQUENCE_NAME,
							 t.IN_OUT_PARAM_COLUMN,
							 t.INDEX_AUDIT,
							 t.FORMAT
						FROM v
						JOIN table_columns t
						  ON t.COLUMN_NAME = v.column_name
					   WHERE t.TABLE_NAME = v_sd_tname
					   UNION ALL
					  SELECT v_tab_name 				AS table_name,
							 'se.' || se.column_name 	AS column_name_alias,
							 'E_' || se.column_name 	AS column_name_orig,
							 t.DISPLAY_NAME,
							 t.COLUMN_DESC,
							 v_release release,
							 t.DATA_TYPE,
							 t.NULLABLE,
							 t.DEFAULT_VALUE,
							 t.AUDIT_FLAG_INSERT,
							 t.AUDIT_FLAG_UPDATE,
							 t.AUDIT_FLAG_DELETE,
							 t.PRODUCTS,
							 'SAETMP' fpm_id,
							 t.DEPRECATED_RELEASE,
							 t.COMPUTE_STATS,
							 t.HISTOGRAM_SIZE,
							 t.SEQUENCE_NAME,
							 t.IN_OUT_PARAM_COLUMN,
							 t.INDEX_AUDIT,
							 t.FORMAT
						FROM se
						JOIN table_columns t
						  ON t.COLUMN_NAME = se.column_name
					   WHERE t.TABLE_NAME = v_en_tname
					     AND se.column_name <> 'PARTITION_KEY')
			  SELECT al.*
				FROM al) loop

	  v_counter := v_counter + 1;

	IF NVL(v_loopback_scenario_id, 0) = 0
	THEN
		IF v_counter = 1
		THEN v_tc_alias := i.column_name_alias || ' as ' || i.column_name_orig;
		ELSE v_tc_alias := v_tc_alias || ',' || i.column_name_alias || ' as ' || i.column_name_orig;
		END IF;
	ELSE
		IF v_counter = 1
		THEN v_tc_alias := i.column_name_orig;
		ELSE v_tc_alias := v_tc_alias || ','  || i.column_name_orig;
		END IF;
	END IF;

  END LOOP;

  COMMIT;

  v_step := '1.2';
  v_msg_param_str := 'v_tc_alias: '||substr(v_tc_alias, 1, 3975);
	sae_log_write ('I',v_application,v_function,v_step ,v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);


   BEGIN

	  SELECT BASELINE_ID, TIMELINE_ID, SCENARIO_TYPE
		INTO v_baseline_id, v_timeline_id, v_scenariotype_id
		FROM SAE_SCENARIO
	   WHERE ID = v_scenario_id;

   EXCEPTION
        when no_data_found then RAISE e_no_data;
		when others then RAISE;
   END;

   v_ctx_id := get_context(v_sd_tname, v_scenario_id);

   --DBMS_OUTPUT.PUT_LINE(v_ctx_id);

   SELECT q'[']' || pk_rd_ws || q'[']'
     INTO v_part_key
	 FROM contexts
	WHERE context_id = v_ctx_id;

   --enable_parallel_processing; -- changes made on 4jan2021 to handle parallel execution DML failure issue
   disable_parallel_processing;

  v_step := '1.3';
  v_msg_param_str := 'after parallel processing enabled. v_part_key : '||v_part_key||', v_baseline_id: '|| to_char(v_baseline_id)||', v_timeline_id: '|| to_char(v_timeline_id)||', v_scenariotype_id: '|| to_char(v_scenariotype_id);
	sae_log_write ('I',v_application,v_function,v_step ,v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);


   IF v_ctx_id <> 0
   THEN
	   pack_context.contextid_open(v_ctx_id);

	   IF v_cap_plan_parms
	   THEN

		  v_tc_alias := v_tc_alias_init(1, v_tab_name);

		  v_sql := ' CREATE TABLE ' || v_tab_name || ' PCTFREE 50 NOLOGGING '
				|| ' AS '
				|| ' SELECT DISTINCT /*+ leading(ext ssdtemp) use_hash(ssdtemp) swap_join_inputs(ssdtemp) */ /*+ parallel (ext 1)*/ ' || v_tc_alias
				|| '   FROM ' || v_renamed_temp_table || ' ssdtemp '
				|| '   JOIN ' || v_source_table || ' ext '
				|| '	 ON ' || v_where;

       v_step := '2.1';
       v_msg_param_str := 'v_cap_plan_parms  : TRUE. v_sql :'||substr(v_sql, 1, 3975);
			 sae_log_write ('I',v_application,v_function,v_step ,v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);

		   BEGIN
				--dbms_output.put_line(v_sql);
				EXECUTE IMMEDIATE v_sql;
		   EXCEPTION
				WHEN OTHERS THEN
         v_msg_param_str := SQLERRM;
         v_step := '2.1.1';
				 sae_log_write ('I',v_application,v_function,v_step ,'Exception creating table '||v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);
         RAISE_APPLICATION_ERROR(-20004, 'Please check the SQL: ' || v_sql);
		   END;

		  v_tc_alias := v_tc_alias_init(2, v_tab_name);
		  v_insert_col := v_tc_alias_init(3, v_tab_name);
		  pack_stats.gather_table_stats(v_tab_name, degree => 16);

		  v_sql := ' INSERT INTO ' || v_tab_name ||' ( ' ||v_insert_col||' ) '
				|| ' SELECT /*+ leading(ext ssdtemp) use_hash(ssdtemp) swap_join_inputs(ssdtemp) */ ' || v_tc_alias
				|| '   FROM ' || v_renamed_temp_table || ' ssdtemp '
				|| '  WHERE NOT EXISTS ( SELECT /*+HASH_AJ*/ /*+ parallel (ext 1)*/ 1 '
				|| '					   FROM ' || v_tab_name || ' ext '
				|| '					  WHERE ssdtemp.scenario_id = ext.scenario_id '
				|| '						AND ssdtemp.entity_id   = ext.entity_id '
				|| '						AND ssdtemp.period_id    = ext.period_id '
				|| '						AND ssdtemp.partition_key = ext.partition_key )';

       v_step := '2.2';
       v_msg_param_str := 'v_cap_plan_parms  : TRUE. v_sql :'||substr(v_sql, 1, 3975);
			 sae_log_write ('I',v_application,v_function,v_step ,v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);

		   BEGIN
				--dbms_output.put_line(v_sql);
				EXECUTE IMMEDIATE v_sql;
				COMMIT;
		   EXCEPTION
				WHEN OTHERS THEN
          v_msg_param_str := SQLERRM;
          v_step := '2.2.1';
				  sae_log_write ('I',v_application,v_function,v_step ,'Exception inserting into table '||v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);
         ROLLBACK;
								 RAISE_APPLICATION_ERROR(-20004, 'Please check the SQL: ' || v_sql);
		   END;

	   ELSE
		   IF NVL(v_loopback_scenario_id, 0) = 0
		   THEN
			  v_sql := ' CREATE TABLE ' || v_tab_name || ' PCTFREE 50 NOLOGGING '
					|| ' AS '
					|| ' SELECT /*+use_hash(sibd se)*/ ' || v_tc_alias
					|| '   FROM SAE_INTERIM_BASEDATA sibd '
					|| '   JOIN SAE_ENTITY se '
					|| '     ON se.ID = sibd.ENTITY_ID '
					|| '  WHERE sibd.BASELINE_ID = ' || v_baseline_id
					|| '    AND sibd.TIMELINE_ID = ' || v_timeline_id
					|| '	AND se.SCENARIO_TYPE = ' || v_scenariotype_id;
		   ELSE
			  v_sql := ' CREATE TABLE ' || v_tab_name || ' PCTFREE 50 NOLOGGING '
					|| ' AS '
					|| ' SELECT ' || v_tc_alias
					|| '   FROM (SELECT ' || v_part_key || ' AS PARTITION_KEY '
					|| '			  , vord.* '
					|| '		   FROM V_SAE_SCENDATA_OVERRIDE vord '
					|| '		  WHERE SCENARIO_ID = ' || v_scenario_id || ' ) ';
		   END IF;

       v_step := '3.1';
       v_msg_param_str := 'v_sql : '||substr(v_sql, 1, 3975);
			 sae_log_write ('I',v_application,v_function,v_step ,v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);

		   BEGIN
				--dbms_output.put_line(v_sql);
				EXECUTE IMMEDIATE v_sql;
		   EXCEPTION
				WHEN OTHERS THEN
         v_msg_param_str := 'Exception while creating table : '||SQLERRM;
         v_step := '3.1.1';
				 sae_log_write ('I',v_application,v_function,v_step ,v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);
				 sae_log_end(null,'N',v_log_struct_id);

         RAISE_APPLICATION_ERROR(-20004, 'Please check the SQL: ' || v_sql);
		   END;

	   END IF;

       v_sql := 'ALTER TABLE '|| v_tab_name ||' MODIFY (PARTITION_KEY DEFAULT SYS_CONTEXT(user,''PARTITION_KEY_RD_WS''))';
     v_step := '3.2';
     v_msg_param_str := 'v_sql : '||v_sql;
		 sae_log_write ('I',v_application,v_function,v_step ,v_msg_param_str,NULL, 'T',1,null,null,null,null,v_log_struct_id);

       --dbms_output.put_line(v_sql1);
       EXECUTE IMMEDIATE v_sql;

ELSE NULL;
END IF;


v_step := '4.1';
v_msg_param_str := 'p_table_name :'||v_tab_name ||', p_table_type : REGULAR, p_standard_custom : S , p_comp_code : CUSTOM , p_allow_long_name : Y' ;
sae_log_write ('I',v_application,v_function,v_step ,' Register_temp_table ',v_msg_param_str, 'T',1,null,null,null,null,v_log_struct_id);

register_temp_table ( p_table_name =>v_tab_name
                     , p_table_type => 'REGULAR'
                     , p_standard_custom => 'S'
                     , p_comp_code => 'CUSTOM'
                     , p_allow_long_name => 'Y' );

  v_step := '4.2';
	sae_log_write ('I',v_application,v_function,v_step ,v_tab_name ||' NOPARALLEL ', 'T',1,null,null,null,null,v_log_struct_id);

EXECUTE IMMEDIATE ' ALTER TABLE '   || v_tab_name || ' NOPARALLEL ';

  v_step := '4.3';
  v_msg_param_str := ' v_table_name: '||v_tab_name||' , v_index_name : '|| v_pk_name||' , v_columns  scenario_id,period_id|,entity_id, v_index_type : PRIMARY, v_fpm_id : '|| c_fpm_id||', v_standard_custom : S ';
	sae_log_write ('I',v_application,v_function,v_step , 'pack_fermat.create_index ',v_msg_param_str, 'T',1,null,null,null,null,v_log_struct_id);

pack_fermat.create_index(
        v_table_name      => v_tab_name,
        v_index_name      => v_pk_name,
        v_columns         => 'scenario_id,period_id,entity_id',
        v_index_type      => 'PRIMARY',
        v_fpm_id          => c_fpm_id,
        v_standard_custom => 'S'
    );

	EXECUTE IMMEDIATE q'[UPDATE cd_fdw_structure SET relevant_for = 'Y', comp_code = 'CUSTOM', generated = 'Y' WHERE table_name = ']' || v_pk_name||q'[']';
commit;

v_step := '4.4';
sae_log_write ('I',v_application,v_function,v_step , ' update cd_fdw_structure for pk. Next rebuild index and NOPARALLEL ',NULL, 'T',1,null,null,null,null,v_log_struct_id);

PACK_DDL.rebuild_index(v_tab_name,NULL);
EXECUTE IMMEDIATE ' ALTER INDEX '   || v_pk_name  || ' NOPARALLEL ';

v_step := '4.5';
sae_log_write ('I',v_application,v_function,v_step , ' disable parallel processing ',NULL, 'T',1,null,null,null,null,v_log_struct_id);
disable_parallel_processing;

IF v_cap_plan_parms
THEN
  EXECUTE IMMEDIATE ' DROP TABLE ' || v_renamed_temp_table || ' PURGE ';
  v_step := '4.6';
	sae_log_write ('I',v_application,v_function,v_step , ' dropped table '||v_renamed_temp_table,NULL, 'T',1,null,null,null,null,v_log_struct_id);

ELSE NULL;
END IF;

sae_log_end ( null,'N', v_log_struct_id );

EXCEPTION
WHEN e_no_data
THEN
sae_log_end ( null,'N', v_log_struct_id );
RAISE_APPLICATION_ERROR(-20002, 'ERROR @ PACK_SAE_UTILS.SCENDATA_TAB_CREATE : No data found in SAE_SCENARIO for SCENARIO_ID =' || v_scenario_id);
WHEN OTHERS
THEN
disable_parallel_processing;
sae_log_end ( null,'N', v_log_struct_id );
RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.SCENDATA_TAB_CREATE : ' || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));

END SCENDATA_TAB_CREATE;

PROCEDURE SCENDATA_TAB_DROP(v_scenario_id NUMBER, v_table_name VARCHAR2 DEFAULT NULL)
IS

v_sd_tname CD_FDW_STRUCTURE.TABLE_NAME%TYPE := 'SAE_SCENARIO_DATA';
v_tab_name CD_FDW_STRUCTURE.TABLE_NAME%TYPE;
v_pk_name  CD_FDW_STRUCTURE.TABLE_NAME%TYPE;
e_table_nonexistent EXCEPTION;
PRAGMA EXCEPTION_INIT(e_table_nonexistent, -942);

BEGIN

IF v_table_name IS NULL
THEN v_tab_name := v_sd_tname || '_' || v_scenario_id;
	 v_pk_name  := 'PK_SCENDATA_'|| v_scenario_id;
ELSE v_tab_name := v_table_name;
	 v_pk_name  := 'PK_'|| SUBSTR(v_table_name, 1, 26);
END IF;

DELETE FROM CD_FDW_STRUCTURE WHERE TABLE_NAME = v_tab_name OR HIST_TABLE_NAME = v_tab_name OR TABLE_NAME = v_pk_name;
COMMIT;

EXECUTE IMMEDIATE 'DROP TABLE ' || v_tab_name || ' PURGE';

EXCEPTION
WHEN e_table_nonexistent THEN NULL;
WHEN OTHERS THEN RAISE;

END SCENDATA_TAB_DROP;

/**
Required for pack_var.
Procedure used to initialize global variables of the package.
*/
procedure init_global_var is
pragma autonomous_transaction;
begin
	pack_var.declare_var($$plsql_unit,'vg_sae_log_key',to_char(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_block_id',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_sub_block_id',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_partition_key',to_char(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_max_repeat_nummsg',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_process_desc',to_char(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_task_id',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_root_task_id',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_job_id',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_root_log_key',to_char(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_log_struct_id',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_last_log_id',to_number(null));
	pack_var.declare_var_boolean($$plsql_unit,'vg_sae_log_write',null);
	pack_var.declare_var_boolean($$plsql_unit,'vg_sae_save_dynamic_query',null);

	pack_var.declare_var($$plsql_unit,'vg_sae_counters','E',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_counters','I',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_counters','W',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_counters','P',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_counters','V',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_counters','M',to_number(null));
	pack_var.declare_var($$plsql_unit,'vg_sae_counters','C',to_number(null));

	-- Default values
	pack_var.declare_var($$plsql_unit,'vg_sae_depth',0);
	pack_var.declare_var($$plsql_unit,'vg_sae_log_filter_info',false);
	pack_var.declare_var($$plsql_unit,'vg_sae_log_enable',true);
	pack_var.declare_var($$plsql_unit,'vg_sae_log_session_id',pack_install.get_fermat_sid);
	pack_var.declare_var($$plsql_unit,'vg_sae_recursion_level',0);
end;

PROCEDURE create_exp_temp_tab (p_context_id number, p_scenario_id number ) IS
	v_count NUMBER;
	v_sql clob;
	v_appendsql clob;
	v_table_name VARCHAR2(30) := 'SAE_EXPORT_TEMP_';
	v_select_sql VARCHAR2(32000);

  v_sd_tname 				CD_FDW_STRUCTURE.TABLE_NAME%TYPE := 'SAE_SCENARIO_DATA';
  v_en_tname 				CD_FDW_STRUCTURE.TABLE_NAME%TYPE := 'SAE_ENTITY';
  v_data_type				TABLE_COLUMNS.DATA_TYPE%TYPE;

  t_sd 	   				  CD_FDW_STRUCTURE%ROWTYPE;
  v_release  				APPLICATION.RELEASE_NUMBER%TYPE;

	v_log_key         log_table.log_key%type := 'CR_EXP_TEMP_'||p_scenario_id;
	v_log_struct_id   number;
	v_function        log_table.function%type := 'PACK_SAE_UTILS.CREATE_EXP_TEMP_TAB';
	v_step            log_table.step%type ;
	v_application     log_table.application%type := 'Y';
	v_msg_param_str   varchar2(4000);

BEGIN

	v_table_name := v_table_name||p_scenario_id;

	pack_context.contextid_open(p_context_id);

	v_log_struct_id := sae_log_begin (	v_log_key => v_log_key );

	v_step := '1.0';
	v_msg_param_str := ' p_context_id: '||p_context_id||' , p_scenario_id : '|| p_scenario_id;
	sae_log_write ('I',v_application,v_function,v_step , 'Parameters ',v_msg_param_str, 'T',1,null,null,null,null,v_log_struct_id);

	-- check if table exists
	SELECT count(*)
	INTO v_count
	FROM user_tables
	WHERE table_name = v_table_name;

	select max(pack_utils.format_release(release_number)) into v_release from application;

	v_step := '1.1';
	v_msg_param_str := ' v_count: '||v_count||' , v_release : '|| v_release;
	sae_log_write ('I',v_application,v_function,v_step , 'Release and table exists query '||v_msg_param_str, null,'T',1,null,null,null,null,v_log_struct_id);

	if (v_count  > 0) then
		EXECUTE IMMEDIATE ' drop table ' || v_table_name;
	end if;

	SELECT * INTO t_sd FROM cd_fdw_structure
	WHERE table_name = v_sd_tname AND ROWNUM < 2;

--	pack_sae_utils.enable_parallel_processing();

	v_sql :=   'SELECT /*+ parallel(ssd) parallel(se) parallel(sibd) use_hash(ssd se) */ ss.ID AS SCENARIO_ID
		   ,ss.NAME                              AS SCENARIO_NAME
		   ,ss.BASELINE_ID
		   ,sibd.ENTITY_ID
		   ,sibd.TIMELINE_ID
		   ,sibd.PERIOD_ID
		   ,se.DATE_ATTRIBUTE_1                  AS E_DATE_ATTRIBUTE_1
		   ,se.DATE_ATTRIBUTE_2                  AS E_DATE_ATTRIBUTE_2
		   ,se.DATE_ATTRIBUTE_3                  AS E_DATE_ATTRIBUTE_3
		   ,se.DATE_ATTRIBUTE_4                  AS E_DATE_ATTRIBUTE_4
		   ,se.STRING_ATTRIBUTE_1                AS E_STRING_ATTRIBUTE_1';

	v_appendsql := ',se.STRING_ATTRIBUTE_2                AS E_STRING_ATTRIBUTE_2
		   ,se.STRING_ATTRIBUTE_3                AS E_STRING_ATTRIBUTE_3
		   ,se.STRING_ATTRIBUTE_4                AS E_STRING_ATTRIBUTE_4
		   ,se.STRING_ATTRIBUTE_5                AS E_STRING_ATTRIBUTE_5
		   ,se.STRING_ATTRIBUTE_6                AS E_STRING_ATTRIBUTE_6
		   ,se.STRING_ATTRIBUTE_7                AS E_STRING_ATTRIBUTE_7
		   ,se.STRING_ATTRIBUTE_8                AS E_STRING_ATTRIBUTE_8
		   ,se.STRING_ATTRIBUTE_9                AS E_STRING_ATTRIBUTE_9
		   ,se.STRING_ATTRIBUTE_10               AS E_STRING_ATTRIBUTE_10';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',se.STRING_ATTRIBUTE_11               AS E_STRING_ATTRIBUTE_11
		   ,se.STRING_ATTRIBUTE_12               AS E_STRING_ATTRIBUTE_12
		   ,se.STRING_ATTRIBUTE_13               AS E_STRING_ATTRIBUTE_13
		   ,se.STRING_ATTRIBUTE_14               AS E_STRING_ATTRIBUTE_14
		   ,se.STRING_ATTRIBUTE_15               AS E_STRING_ATTRIBUTE_15
		   ,se.STRING_ATTRIBUTE_16               AS E_STRING_ATTRIBUTE_16
		   ,se.STRING_ATTRIBUTE_17               AS E_STRING_ATTRIBUTE_17
		   ,se.STRING_ATTRIBUTE_18               AS E_STRING_ATTRIBUTE_18
		   ,se.STRING_ATTRIBUTE_19               AS E_STRING_ATTRIBUTE_19';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ' ,se.STRING_ATTRIBUTE_20               AS E_STRING_ATTRIBUTE_20
		   ,se.STRING_ATTRIBUTE_21               AS E_STRING_ATTRIBUTE_21
		   ,se.STRING_ATTRIBUTE_22               AS E_STRING_ATTRIBUTE_22
		   ,se.STRING_ATTRIBUTE_23               AS E_STRING_ATTRIBUTE_23
		   ,se.STRING_ATTRIBUTE_24               AS E_STRING_ATTRIBUTE_24
		   ,se.STRING_ATTRIBUTE_25               AS E_STRING_ATTRIBUTE_25
		   ,se.STRING_ATTRIBUTE_26               AS E_STRING_ATTRIBUTE_26
		   ,se.STRING_ATTRIBUTE_27               AS E_STRING_ATTRIBUTE_27';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',se.STRING_ATTRIBUTE_28               AS E_STRING_ATTRIBUTE_28
		   ,se.STRING_ATTRIBUTE_29               AS E_STRING_ATTRIBUTE_29
		   ,se.STRING_ATTRIBUTE_30               AS E_STRING_ATTRIBUTE_30
		   ,se.STRING_ATTRIBUTE_31               AS E_STRING_ATTRIBUTE_31
		   ,se.STRING_ATTRIBUTE_32               AS E_STRING_ATTRIBUTE_32
		   ,se.STRING_ATTRIBUTE_33               AS E_STRING_ATTRIBUTE_33
		   ,se.STRING_ATTRIBUTE_34               AS E_STRING_ATTRIBUTE_34
		   ,se.STRING_ATTRIBUTE_35               AS E_STRING_ATTRIBUTE_35
		   ,se.STRING_ATTRIBUTE_36               AS E_STRING_ATTRIBUTE_36
		   ,se.STRING_ATTRIBUTE_37               AS E_STRING_ATTRIBUTE_37
		   ,se.STRING_ATTRIBUTE_38               AS E_STRING_ATTRIBUTE_38';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',se.STRING_ATTRIBUTE_39               AS E_STRING_ATTRIBUTE_39
		   ,se.STRING_ATTRIBUTE_40               AS E_STRING_ATTRIBUTE_40
		   ,se.STRING_ATTRIBUTE_41               AS E_STRING_ATTRIBUTE_41
		   ,se.STRING_ATTRIBUTE_42               AS E_STRING_ATTRIBUTE_42
		   ,se.STRING_ATTRIBUTE_43               AS E_STRING_ATTRIBUTE_43
		   ,se.STRING_ATTRIBUTE_44               AS E_STRING_ATTRIBUTE_44
		   ,se.STRING_ATTRIBUTE_45               AS E_STRING_ATTRIBUTE_45
		   ,se.STRING_ATTRIBUTE_46               AS E_STRING_ATTRIBUTE_46
		   ,se.STRING_ATTRIBUTE_47               AS E_STRING_ATTRIBUTE_47
		   ,se.STRING_ATTRIBUTE_48               AS E_STRING_ATTRIBUTE_48
		   ,se.STRING_ATTRIBUTE_49               AS E_STRING_ATTRIBUTE_49
		   ,se.STRING_ATTRIBUTE_50               AS E_STRING_ATTRIBUTE_50
		   ,se.STRING_ATTRIBUTE_51               AS E_STRING_ATTRIBUTE_51
		   ,se.STRING_ATTRIBUTE_52               AS E_STRING_ATTRIBUTE_52
		   ,se.STRING_ATTRIBUTE_53               AS E_STRING_ATTRIBUTE_53
		   ,se.STRING_ATTRIBUTE_54               AS E_STRING_ATTRIBUTE_54
		   ,se.STRING_ATTRIBUTE_55               AS E_STRING_ATTRIBUTE_55
		   ,se.STRING_ATTRIBUTE_56               AS E_STRING_ATTRIBUTE_56
		   ,se.STRING_ATTRIBUTE_57               AS E_STRING_ATTRIBUTE_57
		   ,se.STRING_ATTRIBUTE_58               AS E_STRING_ATTRIBUTE_58';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql:= ',se.STRING_ATTRIBUTE_59               AS E_STRING_ATTRIBUTE_59
		   ,se.STRING_ATTRIBUTE_60               AS E_STRING_ATTRIBUTE_60
		   ,se.STRING_ATTRIBUTE_61               AS E_STRING_ATTRIBUTE_61
		   ,se.STRING_ATTRIBUTE_62               AS E_STRING_ATTRIBUTE_62
		   ,se.STRING_ATTRIBUTE_63               AS E_STRING_ATTRIBUTE_63
		   ,se.STRING_ATTRIBUTE_64               AS E_STRING_ATTRIBUTE_64
		   ,se.STRING_ATTRIBUTE_65               AS E_STRING_ATTRIBUTE_65
		   ,se.STRING_ATTRIBUTE_66               AS E_STRING_ATTRIBUTE_66
		   ,se.STRING_ATTRIBUTE_67               AS E_STRING_ATTRIBUTE_67
		   ,se.STRING_ATTRIBUTE_68               AS E_STRING_ATTRIBUTE_68
		   ,se.STRING_ATTRIBUTE_69               AS E_STRING_ATTRIBUTE_69
		   ,se.STRING_ATTRIBUTE_70               AS E_STRING_ATTRIBUTE_70
		   ,NVL(ssd.DATE_ATTRIBUTE_1,sibd.DATE_ATTRIBUTE_1)  AS DATE_ATTRIBUTE_1
		   ,NVL(ssd.DATE_ATTRIBUTE_2,sibd.DATE_ATTRIBUTE_2)  AS DATE_ATTRIBUTE_2
		   ,NVL(ssd.DATE_ATTRIBUTE_3,sibd.DATE_ATTRIBUTE_3)  AS DATE_ATTRIBUTE_3
		   ,NVL(ssd.DATE_ATTRIBUTE_4,sibd.DATE_ATTRIBUTE_4)  AS DATE_ATTRIBUTE_4
		   ,NVL(ssd.DATE_ATTRIBUTE_5,sibd.DATE_ATTRIBUTE_5)  AS DATE_ATTRIBUTE_5
		   ,NVL(ssd.DATE_ATTRIBUTE_6,sibd.DATE_ATTRIBUTE_6)  AS DATE_ATTRIBUTE_6
		   ,NVL(ssd.DATE_ATTRIBUTE_7,sibd.DATE_ATTRIBUTE_7)  AS DATE_ATTRIBUTE_7
		   ,NVL(ssd.DATE_ATTRIBUTE_8,sibd.DATE_ATTRIBUTE_8)  AS DATE_ATTRIBUTE_8';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssd.DATE_ATTRIBUTE_9,sibd.DATE_ATTRIBUTE_9)  AS DATE_ATTRIBUTE_9
		   ,NVL(ssd.DATE_ATTRIBUTE_10,sibd.DATE_ATTRIBUTE_10)  AS DATE_ATTRIBUTE_10
		   ,NVL(ssd.DATE_ATTRIBUTE_11,sibd.DATE_ATTRIBUTE_11)  AS DATE_ATTRIBUTE_11
		   ,NVL(ssd.DATE_ATTRIBUTE_12,sibd.DATE_ATTRIBUTE_12)  AS DATE_ATTRIBUTE_12
		   ,NVL(ssd.DATE_ATTRIBUTE_13,sibd.DATE_ATTRIBUTE_13)  AS DATE_ATTRIBUTE_13
		   ,NVL(ssd.DATE_ATTRIBUTE_14,sibd.DATE_ATTRIBUTE_14)  AS DATE_ATTRIBUTE_14
		   ,NVL(ssd.DATE_ATTRIBUTE_15,sibd.DATE_ATTRIBUTE_15)  AS DATE_ATTRIBUTE_15
		   ,NVL(ssd.DATE_ATTRIBUTE_16,sibd.DATE_ATTRIBUTE_16)  AS DATE_ATTRIBUTE_16
		   ,NVL(ssd.DATE_ATTRIBUTE_17,sibd.DATE_ATTRIBUTE_17)  AS DATE_ATTRIBUTE_17
		   ,NVL(ssd.DATE_ATTRIBUTE_18,sibd.DATE_ATTRIBUTE_18)  AS DATE_ATTRIBUTE_18
		   ,NVL(ssd.DATE_ATTRIBUTE_19,sibd.DATE_ATTRIBUTE_19)  AS DATE_ATTRIBUTE_19
		   ,NVL(ssd.DATE_ATTRIBUTE_20,sibd.DATE_ATTRIBUTE_20)  AS DATE_ATTRIBUTE_20
		   ,NVL(ssd.DATE_ATTRIBUTE_21,sibd.DATE_ATTRIBUTE_21)  AS DATE_ATTRIBUTE_21
		   ,NVL(ssd.DATE_ATTRIBUTE_22,sibd.DATE_ATTRIBUTE_22)  AS DATE_ATTRIBUTE_22
		   ,NVL(ssd.DATE_ATTRIBUTE_23,sibd.DATE_ATTRIBUTE_23)  AS DATE_ATTRIBUTE_23
		   ,NVL(ssd.DATE_ATTRIBUTE_24,sibd.DATE_ATTRIBUTE_24)  AS DATE_ATTRIBUTE_24
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_1,NVL(ssd.NUMBER_ATTRIBUTE_1,sibd.NUMBER_ATTRIBUTE_1)) AS NUMBER_ATTRIBUTE_1
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_2,NVL(ssd.NUMBER_ATTRIBUTE_2,sibd.NUMBER_ATTRIBUTE_2)) AS NUMBER_ATTRIBUTE_2
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_3,NVL(ssd.NUMBER_ATTRIBUTE_3,sibd.NUMBER_ATTRIBUTE_3)) AS NUMBER_ATTRIBUTE_3
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_4,NVL(ssd.NUMBER_ATTRIBUTE_4,sibd.NUMBER_ATTRIBUTE_4)) AS NUMBER_ATTRIBUTE_4 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_5,NVL(ssd.NUMBER_ATTRIBUTE_5,sibd.NUMBER_ATTRIBUTE_5)) AS NUMBER_ATTRIBUTE_5
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_6,NVL(ssd.NUMBER_ATTRIBUTE_6,sibd.NUMBER_ATTRIBUTE_6)) AS NUMBER_ATTRIBUTE_6
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_7,NVL(ssd.NUMBER_ATTRIBUTE_7,sibd.NUMBER_ATTRIBUTE_7)) AS NUMBER_ATTRIBUTE_7
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_8,NVL(ssd.NUMBER_ATTRIBUTE_8,sibd.NUMBER_ATTRIBUTE_8)) AS NUMBER_ATTRIBUTE_8
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_9,NVL(ssd.NUMBER_ATTRIBUTE_9,sibd.NUMBER_ATTRIBUTE_9)) AS NUMBER_ATTRIBUTE_9
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_10,NVL(ssd.NUMBER_ATTRIBUTE_10,sibd.NUMBER_ATTRIBUTE_10)) AS NUMBER_ATTRIBUTE_10
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_11,NVL(ssd.NUMBER_ATTRIBUTE_11,sibd.NUMBER_ATTRIBUTE_11)) AS NUMBER_ATTRIBUTE_11
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_12,NVL(ssd.NUMBER_ATTRIBUTE_12,sibd.NUMBER_ATTRIBUTE_12)) AS NUMBER_ATTRIBUTE_12
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_13,NVL(ssd.NUMBER_ATTRIBUTE_13,sibd.NUMBER_ATTRIBUTE_13)) AS NUMBER_ATTRIBUTE_13
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_14,NVL(ssd.NUMBER_ATTRIBUTE_14,sibd.NUMBER_ATTRIBUTE_14)) AS NUMBER_ATTRIBUTE_14
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_15,NVL(ssd.NUMBER_ATTRIBUTE_15,sibd.NUMBER_ATTRIBUTE_15)) AS NUMBER_ATTRIBUTE_15
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_16,NVL(ssd.NUMBER_ATTRIBUTE_16,sibd.NUMBER_ATTRIBUTE_16)) AS NUMBER_ATTRIBUTE_16
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_17,NVL(ssd.NUMBER_ATTRIBUTE_17,sibd.NUMBER_ATTRIBUTE_17)) AS NUMBER_ATTRIBUTE_17
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_18,NVL(ssd.NUMBER_ATTRIBUTE_18,sibd.NUMBER_ATTRIBUTE_18)) AS NUMBER_ATTRIBUTE_18
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_19,NVL(ssd.NUMBER_ATTRIBUTE_19,sibd.NUMBER_ATTRIBUTE_19)) AS NUMBER_ATTRIBUTE_19
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_20,NVL(ssd.NUMBER_ATTRIBUTE_20,sibd.NUMBER_ATTRIBUTE_20)) AS NUMBER_ATTRIBUTE_20
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_21,NVL(ssd.NUMBER_ATTRIBUTE_21,sibd.NUMBER_ATTRIBUTE_21)) AS NUMBER_ATTRIBUTE_21
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_22,NVL(ssd.NUMBER_ATTRIBUTE_22,sibd.NUMBER_ATTRIBUTE_22)) AS NUMBER_ATTRIBUTE_22
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_23,NVL(ssd.NUMBER_ATTRIBUTE_23,sibd.NUMBER_ATTRIBUTE_23)) AS NUMBER_ATTRIBUTE_23
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_24,NVL(ssd.NUMBER_ATTRIBUTE_24,sibd.NUMBER_ATTRIBUTE_24)) AS NUMBER_ATTRIBUTE_24 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_25,NVL(ssd.NUMBER_ATTRIBUTE_25,sibd.NUMBER_ATTRIBUTE_25)) AS NUMBER_ATTRIBUTE_25
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_26,NVL(ssd.NUMBER_ATTRIBUTE_26,sibd.NUMBER_ATTRIBUTE_26)) AS NUMBER_ATTRIBUTE_26
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_27,NVL(ssd.NUMBER_ATTRIBUTE_27,sibd.NUMBER_ATTRIBUTE_27)) AS NUMBER_ATTRIBUTE_27
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_28,NVL(ssd.NUMBER_ATTRIBUTE_28,sibd.NUMBER_ATTRIBUTE_28)) AS NUMBER_ATTRIBUTE_28
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_29,NVL(ssd.NUMBER_ATTRIBUTE_29,sibd.NUMBER_ATTRIBUTE_29)) AS NUMBER_ATTRIBUTE_29
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_30,NVL(ssd.NUMBER_ATTRIBUTE_30,sibd.NUMBER_ATTRIBUTE_30)) AS NUMBER_ATTRIBUTE_30
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_31,NVL(ssd.NUMBER_ATTRIBUTE_31,sibd.NUMBER_ATTRIBUTE_31)) AS NUMBER_ATTRIBUTE_31
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_32,NVL(ssd.NUMBER_ATTRIBUTE_32,sibd.NUMBER_ATTRIBUTE_32)) AS NUMBER_ATTRIBUTE_32
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_33,NVL(ssd.NUMBER_ATTRIBUTE_33,sibd.NUMBER_ATTRIBUTE_33)) AS NUMBER_ATTRIBUTE_33
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_34,NVL(ssd.NUMBER_ATTRIBUTE_34,sibd.NUMBER_ATTRIBUTE_34)) AS NUMBER_ATTRIBUTE_34
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_35,NVL(ssd.NUMBER_ATTRIBUTE_35,sibd.NUMBER_ATTRIBUTE_35)) AS NUMBER_ATTRIBUTE_35
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_36,NVL(ssd.NUMBER_ATTRIBUTE_36,sibd.NUMBER_ATTRIBUTE_36)) AS NUMBER_ATTRIBUTE_36
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_37,NVL(ssd.NUMBER_ATTRIBUTE_37,sibd.NUMBER_ATTRIBUTE_37)) AS NUMBER_ATTRIBUTE_37
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_38,NVL(ssd.NUMBER_ATTRIBUTE_38,sibd.NUMBER_ATTRIBUTE_38)) AS NUMBER_ATTRIBUTE_38
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_39,NVL(ssd.NUMBER_ATTRIBUTE_39,sibd.NUMBER_ATTRIBUTE_39)) AS NUMBER_ATTRIBUTE_39
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_40,NVL(ssd.NUMBER_ATTRIBUTE_40,sibd.NUMBER_ATTRIBUTE_40)) AS NUMBER_ATTRIBUTE_40
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_41,NVL(ssd.NUMBER_ATTRIBUTE_41,sibd.NUMBER_ATTRIBUTE_41)) AS NUMBER_ATTRIBUTE_41
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_42,NVL(ssd.NUMBER_ATTRIBUTE_42,sibd.NUMBER_ATTRIBUTE_42)) AS NUMBER_ATTRIBUTE_42
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_43,NVL(ssd.NUMBER_ATTRIBUTE_43,sibd.NUMBER_ATTRIBUTE_43)) AS NUMBER_ATTRIBUTE_43
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_44,NVL(ssd.NUMBER_ATTRIBUTE_44,sibd.NUMBER_ATTRIBUTE_44)) AS NUMBER_ATTRIBUTE_44';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_45,NVL(ssd.NUMBER_ATTRIBUTE_45,sibd.NUMBER_ATTRIBUTE_45)) AS NUMBER_ATTRIBUTE_45
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_46,NVL(ssd.NUMBER_ATTRIBUTE_46,sibd.NUMBER_ATTRIBUTE_46)) AS NUMBER_ATTRIBUTE_46
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_47,NVL(ssd.NUMBER_ATTRIBUTE_47,sibd.NUMBER_ATTRIBUTE_47)) AS NUMBER_ATTRIBUTE_47
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_48,NVL(ssd.NUMBER_ATTRIBUTE_48,sibd.NUMBER_ATTRIBUTE_48)) AS NUMBER_ATTRIBUTE_48
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_49,NVL(ssd.NUMBER_ATTRIBUTE_49,sibd.NUMBER_ATTRIBUTE_49)) AS NUMBER_ATTRIBUTE_49
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_50,NVL(ssd.NUMBER_ATTRIBUTE_50,sibd.NUMBER_ATTRIBUTE_50)) AS NUMBER_ATTRIBUTE_50
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_51,NVL(ssd.NUMBER_ATTRIBUTE_51,sibd.NUMBER_ATTRIBUTE_51)) AS NUMBER_ATTRIBUTE_51
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_52,NVL(ssd.NUMBER_ATTRIBUTE_52,sibd.NUMBER_ATTRIBUTE_52)) AS NUMBER_ATTRIBUTE_52
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_53,NVL(ssd.NUMBER_ATTRIBUTE_53,sibd.NUMBER_ATTRIBUTE_53)) AS NUMBER_ATTRIBUTE_53
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_54,NVL(ssd.NUMBER_ATTRIBUTE_54,sibd.NUMBER_ATTRIBUTE_54)) AS NUMBER_ATTRIBUTE_54
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_55,NVL(ssd.NUMBER_ATTRIBUTE_55,sibd.NUMBER_ATTRIBUTE_55)) AS NUMBER_ATTRIBUTE_55
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_56,NVL(ssd.NUMBER_ATTRIBUTE_56,sibd.NUMBER_ATTRIBUTE_56)) AS NUMBER_ATTRIBUTE_56
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_57,NVL(ssd.NUMBER_ATTRIBUTE_57,sibd.NUMBER_ATTRIBUTE_57)) AS NUMBER_ATTRIBUTE_57
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_58,NVL(ssd.NUMBER_ATTRIBUTE_58,sibd.NUMBER_ATTRIBUTE_58)) AS NUMBER_ATTRIBUTE_58
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_59,NVL(ssd.NUMBER_ATTRIBUTE_59,sibd.NUMBER_ATTRIBUTE_59)) AS NUMBER_ATTRIBUTE_59
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_60,NVL(ssd.NUMBER_ATTRIBUTE_60,sibd.NUMBER_ATTRIBUTE_60)) AS NUMBER_ATTRIBUTE_60
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_61,NVL(ssd.NUMBER_ATTRIBUTE_61,sibd.NUMBER_ATTRIBUTE_61)) AS NUMBER_ATTRIBUTE_61
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_62,NVL(ssd.NUMBER_ATTRIBUTE_62,sibd.NUMBER_ATTRIBUTE_62)) AS NUMBER_ATTRIBUTE_62
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_63,NVL(ssd.NUMBER_ATTRIBUTE_63,sibd.NUMBER_ATTRIBUTE_63)) AS NUMBER_ATTRIBUTE_63
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_64,NVL(ssd.NUMBER_ATTRIBUTE_64,sibd.NUMBER_ATTRIBUTE_64)) AS NUMBER_ATTRIBUTE_64 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_65,NVL(ssd.NUMBER_ATTRIBUTE_65,sibd.NUMBER_ATTRIBUTE_65)) AS NUMBER_ATTRIBUTE_65
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_66,NVL(ssd.NUMBER_ATTRIBUTE_66,sibd.NUMBER_ATTRIBUTE_66)) AS NUMBER_ATTRIBUTE_66
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_67,NVL(ssd.NUMBER_ATTRIBUTE_67,sibd.NUMBER_ATTRIBUTE_67)) AS NUMBER_ATTRIBUTE_67
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_68,NVL(ssd.NUMBER_ATTRIBUTE_68,sibd.NUMBER_ATTRIBUTE_68)) AS NUMBER_ATTRIBUTE_68
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_69,NVL(ssd.NUMBER_ATTRIBUTE_69,sibd.NUMBER_ATTRIBUTE_69)) AS NUMBER_ATTRIBUTE_69
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_70,NVL(ssd.NUMBER_ATTRIBUTE_70,sibd.NUMBER_ATTRIBUTE_70)) AS NUMBER_ATTRIBUTE_70
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_71,NVL(ssd.NUMBER_ATTRIBUTE_71,sibd.NUMBER_ATTRIBUTE_71)) AS NUMBER_ATTRIBUTE_71
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_72,NVL(ssd.NUMBER_ATTRIBUTE_72,sibd.NUMBER_ATTRIBUTE_72)) AS NUMBER_ATTRIBUTE_72
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_73,NVL(ssd.NUMBER_ATTRIBUTE_73,sibd.NUMBER_ATTRIBUTE_73)) AS NUMBER_ATTRIBUTE_73
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_74,NVL(ssd.NUMBER_ATTRIBUTE_74,sibd.NUMBER_ATTRIBUTE_74)) AS NUMBER_ATTRIBUTE_74
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_75,NVL(ssd.NUMBER_ATTRIBUTE_75,sibd.NUMBER_ATTRIBUTE_75)) AS NUMBER_ATTRIBUTE_75
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_76,NVL(ssd.NUMBER_ATTRIBUTE_76,sibd.NUMBER_ATTRIBUTE_76)) AS NUMBER_ATTRIBUTE_76
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_77,NVL(ssd.NUMBER_ATTRIBUTE_77,sibd.NUMBER_ATTRIBUTE_77)) AS NUMBER_ATTRIBUTE_77
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_78,NVL(ssd.NUMBER_ATTRIBUTE_78,sibd.NUMBER_ATTRIBUTE_78)) AS NUMBER_ATTRIBUTE_78
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_79,NVL(ssd.NUMBER_ATTRIBUTE_79,sibd.NUMBER_ATTRIBUTE_79)) AS NUMBER_ATTRIBUTE_79
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_80,NVL(ssd.NUMBER_ATTRIBUTE_80,sibd.NUMBER_ATTRIBUTE_80)) AS NUMBER_ATTRIBUTE_80
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_81,NVL(ssd.NUMBER_ATTRIBUTE_81,sibd.NUMBER_ATTRIBUTE_81)) AS NUMBER_ATTRIBUTE_81
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_82,NVL(ssd.NUMBER_ATTRIBUTE_82,sibd.NUMBER_ATTRIBUTE_82)) AS NUMBER_ATTRIBUTE_82
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_83,NVL(ssd.NUMBER_ATTRIBUTE_83,sibd.NUMBER_ATTRIBUTE_83)) AS NUMBER_ATTRIBUTE_83
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_84,NVL(ssd.NUMBER_ATTRIBUTE_84,sibd.NUMBER_ATTRIBUTE_84)) AS NUMBER_ATTRIBUTE_84';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_85,NVL(ssd.NUMBER_ATTRIBUTE_85,sibd.NUMBER_ATTRIBUTE_85)) AS NUMBER_ATTRIBUTE_85
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_86,NVL(ssd.NUMBER_ATTRIBUTE_86,sibd.NUMBER_ATTRIBUTE_86)) AS NUMBER_ATTRIBUTE_86
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_87,NVL(ssd.NUMBER_ATTRIBUTE_87,sibd.NUMBER_ATTRIBUTE_87)) AS NUMBER_ATTRIBUTE_87
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_88,NVL(ssd.NUMBER_ATTRIBUTE_88,sibd.NUMBER_ATTRIBUTE_88)) AS NUMBER_ATTRIBUTE_88
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_89,NVL(ssd.NUMBER_ATTRIBUTE_89,sibd.NUMBER_ATTRIBUTE_89)) AS NUMBER_ATTRIBUTE_89
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_90,NVL(ssd.NUMBER_ATTRIBUTE_90,sibd.NUMBER_ATTRIBUTE_90)) AS NUMBER_ATTRIBUTE_90
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_91,NVL(ssd.NUMBER_ATTRIBUTE_91,sibd.NUMBER_ATTRIBUTE_91)) AS NUMBER_ATTRIBUTE_91
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_92,NVL(ssd.NUMBER_ATTRIBUTE_92,sibd.NUMBER_ATTRIBUTE_92)) AS NUMBER_ATTRIBUTE_92
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_93,NVL(ssd.NUMBER_ATTRIBUTE_93,sibd.NUMBER_ATTRIBUTE_93)) AS NUMBER_ATTRIBUTE_93
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_94,NVL(ssd.NUMBER_ATTRIBUTE_94,sibd.NUMBER_ATTRIBUTE_94)) AS NUMBER_ATTRIBUTE_94
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_95,NVL(ssd.NUMBER_ATTRIBUTE_95,sibd.NUMBER_ATTRIBUTE_95)) AS NUMBER_ATTRIBUTE_95
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_96,NVL(ssd.NUMBER_ATTRIBUTE_96,sibd.NUMBER_ATTRIBUTE_96)) AS NUMBER_ATTRIBUTE_96
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_97,NVL(ssd.NUMBER_ATTRIBUTE_97,sibd.NUMBER_ATTRIBUTE_97)) AS NUMBER_ATTRIBUTE_97
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_98,NVL(ssd.NUMBER_ATTRIBUTE_98,sibd.NUMBER_ATTRIBUTE_98)) AS NUMBER_ATTRIBUTE_98
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_99,NVL(ssd.NUMBER_ATTRIBUTE_99,sibd.NUMBER_ATTRIBUTE_99)) AS NUMBER_ATTRIBUTE_99
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_100,NVL(ssd.NUMBER_ATTRIBUTE_100,sibd.NUMBER_ATTRIBUTE_100)) AS NUMBER_ATTRIBUTE_100
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_101,NVL(ssd.NUMBER_ATTRIBUTE_101,sibd.NUMBER_ATTRIBUTE_101)) AS NUMBER_ATTRIBUTE_101
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_102,NVL(ssd.NUMBER_ATTRIBUTE_102,sibd.NUMBER_ATTRIBUTE_102)) AS NUMBER_ATTRIBUTE_102
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_103,NVL(ssd.NUMBER_ATTRIBUTE_103,sibd.NUMBER_ATTRIBUTE_103)) AS NUMBER_ATTRIBUTE_103
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_104,NVL(ssd.NUMBER_ATTRIBUTE_104,sibd.NUMBER_ATTRIBUTE_104)) AS NUMBER_ATTRIBUTE_104 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_105,NVL(ssd.NUMBER_ATTRIBUTE_105,sibd.NUMBER_ATTRIBUTE_105)) AS NUMBER_ATTRIBUTE_105
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_106,NVL(ssd.NUMBER_ATTRIBUTE_106,sibd.NUMBER_ATTRIBUTE_106)) AS NUMBER_ATTRIBUTE_106
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_107,NVL(ssd.NUMBER_ATTRIBUTE_107,sibd.NUMBER_ATTRIBUTE_107)) AS NUMBER_ATTRIBUTE_107
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_108,NVL(ssd.NUMBER_ATTRIBUTE_108,sibd.NUMBER_ATTRIBUTE_108)) AS NUMBER_ATTRIBUTE_108
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_109,NVL(ssd.NUMBER_ATTRIBUTE_109,sibd.NUMBER_ATTRIBUTE_109)) AS NUMBER_ATTRIBUTE_109
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_110,NVL(ssd.NUMBER_ATTRIBUTE_110,sibd.NUMBER_ATTRIBUTE_110)) AS NUMBER_ATTRIBUTE_110
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_111,NVL(ssd.NUMBER_ATTRIBUTE_111,sibd.NUMBER_ATTRIBUTE_111)) AS NUMBER_ATTRIBUTE_111
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_112,NVL(ssd.NUMBER_ATTRIBUTE_112,sibd.NUMBER_ATTRIBUTE_112)) AS NUMBER_ATTRIBUTE_112
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_113,NVL(ssd.NUMBER_ATTRIBUTE_113,sibd.NUMBER_ATTRIBUTE_113)) AS NUMBER_ATTRIBUTE_113
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_114,NVL(ssd.NUMBER_ATTRIBUTE_114,sibd.NUMBER_ATTRIBUTE_114)) AS NUMBER_ATTRIBUTE_114
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_115,NVL(ssd.NUMBER_ATTRIBUTE_115,sibd.NUMBER_ATTRIBUTE_115)) AS NUMBER_ATTRIBUTE_115
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_116,NVL(ssd.NUMBER_ATTRIBUTE_116,sibd.NUMBER_ATTRIBUTE_116)) AS NUMBER_ATTRIBUTE_116
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_117,NVL(ssd.NUMBER_ATTRIBUTE_117,sibd.NUMBER_ATTRIBUTE_117)) AS NUMBER_ATTRIBUTE_117
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_118,NVL(ssd.NUMBER_ATTRIBUTE_118,sibd.NUMBER_ATTRIBUTE_118)) AS NUMBER_ATTRIBUTE_118
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_119,NVL(ssd.NUMBER_ATTRIBUTE_119,sibd.NUMBER_ATTRIBUTE_119)) AS NUMBER_ATTRIBUTE_119
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_120,NVL(ssd.NUMBER_ATTRIBUTE_120,sibd.NUMBER_ATTRIBUTE_120)) AS NUMBER_ATTRIBUTE_120
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_121,NVL(ssd.NUMBER_ATTRIBUTE_121,sibd.NUMBER_ATTRIBUTE_121)) AS NUMBER_ATTRIBUTE_121
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_122,NVL(ssd.NUMBER_ATTRIBUTE_122,sibd.NUMBER_ATTRIBUTE_122)) AS NUMBER_ATTRIBUTE_122
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_123,NVL(ssd.NUMBER_ATTRIBUTE_123,sibd.NUMBER_ATTRIBUTE_123)) AS NUMBER_ATTRIBUTE_123
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_124,NVL(ssd.NUMBER_ATTRIBUTE_124,sibd.NUMBER_ATTRIBUTE_124)) AS NUMBER_ATTRIBUTE_124';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_125,NVL(ssd.NUMBER_ATTRIBUTE_125,sibd.NUMBER_ATTRIBUTE_125)) AS NUMBER_ATTRIBUTE_125
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_126,NVL(ssd.NUMBER_ATTRIBUTE_126,sibd.NUMBER_ATTRIBUTE_126)) AS NUMBER_ATTRIBUTE_126
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_127,NVL(ssd.NUMBER_ATTRIBUTE_127,sibd.NUMBER_ATTRIBUTE_127)) AS NUMBER_ATTRIBUTE_127
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_128,NVL(ssd.NUMBER_ATTRIBUTE_128,sibd.NUMBER_ATTRIBUTE_128)) AS NUMBER_ATTRIBUTE_128
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_129,NVL(ssd.NUMBER_ATTRIBUTE_129,sibd.NUMBER_ATTRIBUTE_129)) AS NUMBER_ATTRIBUTE_129
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_130,NVL(ssd.NUMBER_ATTRIBUTE_130,sibd.NUMBER_ATTRIBUTE_130)) AS NUMBER_ATTRIBUTE_130
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_131,NVL(ssd.NUMBER_ATTRIBUTE_131,sibd.NUMBER_ATTRIBUTE_131)) AS NUMBER_ATTRIBUTE_131
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_132,NVL(ssd.NUMBER_ATTRIBUTE_132,sibd.NUMBER_ATTRIBUTE_132)) AS NUMBER_ATTRIBUTE_132
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_133,NVL(ssd.NUMBER_ATTRIBUTE_133,sibd.NUMBER_ATTRIBUTE_133)) AS NUMBER_ATTRIBUTE_133
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_134,NVL(ssd.NUMBER_ATTRIBUTE_134,sibd.NUMBER_ATTRIBUTE_134)) AS NUMBER_ATTRIBUTE_134
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_135,NVL(ssd.NUMBER_ATTRIBUTE_135,sibd.NUMBER_ATTRIBUTE_135)) AS NUMBER_ATTRIBUTE_135
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_136,NVL(ssd.NUMBER_ATTRIBUTE_136,sibd.NUMBER_ATTRIBUTE_136)) AS NUMBER_ATTRIBUTE_136
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_137,NVL(ssd.NUMBER_ATTRIBUTE_137,sibd.NUMBER_ATTRIBUTE_137)) AS NUMBER_ATTRIBUTE_137
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_138,NVL(ssd.NUMBER_ATTRIBUTE_138,sibd.NUMBER_ATTRIBUTE_138)) AS NUMBER_ATTRIBUTE_138
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_139,NVL(ssd.NUMBER_ATTRIBUTE_139,sibd.NUMBER_ATTRIBUTE_139)) AS NUMBER_ATTRIBUTE_139
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_140,NVL(ssd.NUMBER_ATTRIBUTE_140,sibd.NUMBER_ATTRIBUTE_140)) AS NUMBER_ATTRIBUTE_140
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_141,NVL(ssd.NUMBER_ATTRIBUTE_141,sibd.NUMBER_ATTRIBUTE_141)) AS NUMBER_ATTRIBUTE_141
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_142,NVL(ssd.NUMBER_ATTRIBUTE_142,sibd.NUMBER_ATTRIBUTE_142)) AS NUMBER_ATTRIBUTE_142
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_143,NVL(ssd.NUMBER_ATTRIBUTE_143,sibd.NUMBER_ATTRIBUTE_143)) AS NUMBER_ATTRIBUTE_143
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_144,NVL(ssd.NUMBER_ATTRIBUTE_144,sibd.NUMBER_ATTRIBUTE_144)) AS NUMBER_ATTRIBUTE_144 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_145,NVL(ssd.NUMBER_ATTRIBUTE_145,sibd.NUMBER_ATTRIBUTE_145)) AS NUMBER_ATTRIBUTE_145
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_146,NVL(ssd.NUMBER_ATTRIBUTE_146,sibd.NUMBER_ATTRIBUTE_146)) AS NUMBER_ATTRIBUTE_146
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_147,NVL(ssd.NUMBER_ATTRIBUTE_147,sibd.NUMBER_ATTRIBUTE_147)) AS NUMBER_ATTRIBUTE_147
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_148,NVL(ssd.NUMBER_ATTRIBUTE_148,sibd.NUMBER_ATTRIBUTE_148)) AS NUMBER_ATTRIBUTE_148
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_149,NVL(ssd.NUMBER_ATTRIBUTE_149,sibd.NUMBER_ATTRIBUTE_149)) AS NUMBER_ATTRIBUTE_149
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_150,NVL(ssd.NUMBER_ATTRIBUTE_150,sibd.NUMBER_ATTRIBUTE_150)) AS NUMBER_ATTRIBUTE_150
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_151,NVL(ssd.NUMBER_ATTRIBUTE_151,sibd.NUMBER_ATTRIBUTE_151)) AS NUMBER_ATTRIBUTE_151
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_152,NVL(ssd.NUMBER_ATTRIBUTE_152,sibd.NUMBER_ATTRIBUTE_152)) AS NUMBER_ATTRIBUTE_152
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_153,NVL(ssd.NUMBER_ATTRIBUTE_153,sibd.NUMBER_ATTRIBUTE_153)) AS NUMBER_ATTRIBUTE_153
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_154,NVL(ssd.NUMBER_ATTRIBUTE_154,sibd.NUMBER_ATTRIBUTE_154)) AS NUMBER_ATTRIBUTE_154
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_155,NVL(ssd.NUMBER_ATTRIBUTE_155,sibd.NUMBER_ATTRIBUTE_155)) AS NUMBER_ATTRIBUTE_155
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_156,NVL(ssd.NUMBER_ATTRIBUTE_156,sibd.NUMBER_ATTRIBUTE_156)) AS NUMBER_ATTRIBUTE_156
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_157,NVL(ssd.NUMBER_ATTRIBUTE_157,sibd.NUMBER_ATTRIBUTE_157)) AS NUMBER_ATTRIBUTE_157
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_158,NVL(ssd.NUMBER_ATTRIBUTE_158,sibd.NUMBER_ATTRIBUTE_158)) AS NUMBER_ATTRIBUTE_158
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_159,NVL(ssd.NUMBER_ATTRIBUTE_159,sibd.NUMBER_ATTRIBUTE_159)) AS NUMBER_ATTRIBUTE_159
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_160,NVL(ssd.NUMBER_ATTRIBUTE_160,sibd.NUMBER_ATTRIBUTE_160)) AS NUMBER_ATTRIBUTE_160
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_161,NVL(ssd.NUMBER_ATTRIBUTE_161,sibd.NUMBER_ATTRIBUTE_161)) AS NUMBER_ATTRIBUTE_161
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_162,NVL(ssd.NUMBER_ATTRIBUTE_162,sibd.NUMBER_ATTRIBUTE_162)) AS NUMBER_ATTRIBUTE_162
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_163,NVL(ssd.NUMBER_ATTRIBUTE_163,sibd.NUMBER_ATTRIBUTE_163)) AS NUMBER_ATTRIBUTE_163
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_164,NVL(ssd.NUMBER_ATTRIBUTE_164,sibd.NUMBER_ATTRIBUTE_164)) AS NUMBER_ATTRIBUTE_164
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_165,NVL(ssd.NUMBER_ATTRIBUTE_165,sibd.NUMBER_ATTRIBUTE_165)) AS NUMBER_ATTRIBUTE_165
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_166,NVL(ssd.NUMBER_ATTRIBUTE_166,sibd.NUMBER_ATTRIBUTE_166)) AS NUMBER_ATTRIBUTE_166
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_167,NVL(ssd.NUMBER_ATTRIBUTE_167,sibd.NUMBER_ATTRIBUTE_167)) AS NUMBER_ATTRIBUTE_167
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_168,NVL(ssd.NUMBER_ATTRIBUTE_168,sibd.NUMBER_ATTRIBUTE_168)) AS NUMBER_ATTRIBUTE_168
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_169,NVL(ssd.NUMBER_ATTRIBUTE_169,sibd.NUMBER_ATTRIBUTE_169)) AS NUMBER_ATTRIBUTE_169
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_170,NVL(ssd.NUMBER_ATTRIBUTE_170,sibd.NUMBER_ATTRIBUTE_170)) AS NUMBER_ATTRIBUTE_170
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_171,NVL(ssd.NUMBER_ATTRIBUTE_171,sibd.NUMBER_ATTRIBUTE_171)) AS NUMBER_ATTRIBUTE_171
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_172,NVL(ssd.NUMBER_ATTRIBUTE_172,sibd.NUMBER_ATTRIBUTE_172)) AS NUMBER_ATTRIBUTE_172
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_173,NVL(ssd.NUMBER_ATTRIBUTE_173,sibd.NUMBER_ATTRIBUTE_173)) AS NUMBER_ATTRIBUTE_173
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_174,NVL(ssd.NUMBER_ATTRIBUTE_174,sibd.NUMBER_ATTRIBUTE_174)) AS NUMBER_ATTRIBUTE_174';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_175,NVL(ssd.NUMBER_ATTRIBUTE_175,sibd.NUMBER_ATTRIBUTE_175)) AS NUMBER_ATTRIBUTE_175
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_176,NVL(ssd.NUMBER_ATTRIBUTE_176,sibd.NUMBER_ATTRIBUTE_176)) AS NUMBER_ATTRIBUTE_176
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_177,NVL(ssd.NUMBER_ATTRIBUTE_177,sibd.NUMBER_ATTRIBUTE_177)) AS NUMBER_ATTRIBUTE_177
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_178,NVL(ssd.NUMBER_ATTRIBUTE_178,sibd.NUMBER_ATTRIBUTE_178)) AS NUMBER_ATTRIBUTE_178
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_179,NVL(ssd.NUMBER_ATTRIBUTE_179,sibd.NUMBER_ATTRIBUTE_179)) AS NUMBER_ATTRIBUTE_179
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_180,NVL(ssd.NUMBER_ATTRIBUTE_180,sibd.NUMBER_ATTRIBUTE_180)) AS NUMBER_ATTRIBUTE_180
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_181,NVL(ssd.NUMBER_ATTRIBUTE_181,sibd.NUMBER_ATTRIBUTE_181)) AS NUMBER_ATTRIBUTE_181
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_182,NVL(ssd.NUMBER_ATTRIBUTE_182,sibd.NUMBER_ATTRIBUTE_182)) AS NUMBER_ATTRIBUTE_182
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_183,NVL(ssd.NUMBER_ATTRIBUTE_183,sibd.NUMBER_ATTRIBUTE_183)) AS NUMBER_ATTRIBUTE_183
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_184,NVL(ssd.NUMBER_ATTRIBUTE_184,sibd.NUMBER_ATTRIBUTE_184)) AS NUMBER_ATTRIBUTE_184
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_185,NVL(ssd.NUMBER_ATTRIBUTE_185,sibd.NUMBER_ATTRIBUTE_185)) AS NUMBER_ATTRIBUTE_185
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_186,NVL(ssd.NUMBER_ATTRIBUTE_186,sibd.NUMBER_ATTRIBUTE_186)) AS NUMBER_ATTRIBUTE_186
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_187,NVL(ssd.NUMBER_ATTRIBUTE_187,sibd.NUMBER_ATTRIBUTE_187)) AS NUMBER_ATTRIBUTE_187
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_188,NVL(ssd.NUMBER_ATTRIBUTE_188,sibd.NUMBER_ATTRIBUTE_188)) AS NUMBER_ATTRIBUTE_188
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_189,NVL(ssd.NUMBER_ATTRIBUTE_189,sibd.NUMBER_ATTRIBUTE_189)) AS NUMBER_ATTRIBUTE_189
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_190,NVL(ssd.NUMBER_ATTRIBUTE_190,sibd.NUMBER_ATTRIBUTE_190)) AS NUMBER_ATTRIBUTE_190
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_191,NVL(ssd.NUMBER_ATTRIBUTE_191,sibd.NUMBER_ATTRIBUTE_191)) AS NUMBER_ATTRIBUTE_191
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_192,NVL(ssd.NUMBER_ATTRIBUTE_192,sibd.NUMBER_ATTRIBUTE_192)) AS NUMBER_ATTRIBUTE_192
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_193,NVL(ssd.NUMBER_ATTRIBUTE_193,sibd.NUMBER_ATTRIBUTE_193)) AS NUMBER_ATTRIBUTE_193
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_194,NVL(ssd.NUMBER_ATTRIBUTE_194,sibd.NUMBER_ATTRIBUTE_194)) AS NUMBER_ATTRIBUTE_194
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_195,NVL(ssd.NUMBER_ATTRIBUTE_195,sibd.NUMBER_ATTRIBUTE_195)) AS NUMBER_ATTRIBUTE_195
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_196,NVL(ssd.NUMBER_ATTRIBUTE_196,sibd.NUMBER_ATTRIBUTE_196)) AS NUMBER_ATTRIBUTE_196
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_197,NVL(ssd.NUMBER_ATTRIBUTE_197,sibd.NUMBER_ATTRIBUTE_197)) AS NUMBER_ATTRIBUTE_197
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_198,NVL(ssd.NUMBER_ATTRIBUTE_198,sibd.NUMBER_ATTRIBUTE_198)) AS NUMBER_ATTRIBUTE_198
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_199,NVL(ssd.NUMBER_ATTRIBUTE_199,sibd.NUMBER_ATTRIBUTE_199)) AS NUMBER_ATTRIBUTE_199
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_200,NVL(ssd.NUMBER_ATTRIBUTE_200,sibd.NUMBER_ATTRIBUTE_200)) AS NUMBER_ATTRIBUTE_200
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_201,NVL(ssd.NUMBER_ATTRIBUTE_201,sibd.NUMBER_ATTRIBUTE_201)) AS NUMBER_ATTRIBUTE_201
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_202,NVL(ssd.NUMBER_ATTRIBUTE_202,sibd.NUMBER_ATTRIBUTE_202)) AS NUMBER_ATTRIBUTE_202
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_203,NVL(ssd.NUMBER_ATTRIBUTE_203,sibd.NUMBER_ATTRIBUTE_203)) AS NUMBER_ATTRIBUTE_203
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_204,NVL(ssd.NUMBER_ATTRIBUTE_204,sibd.NUMBER_ATTRIBUTE_204)) AS NUMBER_ATTRIBUTE_204 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_205,NVL(ssd.NUMBER_ATTRIBUTE_205,sibd.NUMBER_ATTRIBUTE_205)) AS NUMBER_ATTRIBUTE_205
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_206,NVL(ssd.NUMBER_ATTRIBUTE_206,sibd.NUMBER_ATTRIBUTE_206)) AS NUMBER_ATTRIBUTE_206
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_207,NVL(ssd.NUMBER_ATTRIBUTE_207,sibd.NUMBER_ATTRIBUTE_207)) AS NUMBER_ATTRIBUTE_207
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_208,NVL(ssd.NUMBER_ATTRIBUTE_208,sibd.NUMBER_ATTRIBUTE_208)) AS NUMBER_ATTRIBUTE_208
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_209,NVL(ssd.NUMBER_ATTRIBUTE_209,sibd.NUMBER_ATTRIBUTE_209)) AS NUMBER_ATTRIBUTE_209
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_210,NVL(ssd.NUMBER_ATTRIBUTE_210,sibd.NUMBER_ATTRIBUTE_210)) AS NUMBER_ATTRIBUTE_210
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_211,NVL(ssd.NUMBER_ATTRIBUTE_211,sibd.NUMBER_ATTRIBUTE_211)) AS NUMBER_ATTRIBUTE_211
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_212,NVL(ssd.NUMBER_ATTRIBUTE_212,sibd.NUMBER_ATTRIBUTE_212)) AS NUMBER_ATTRIBUTE_212
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_213,NVL(ssd.NUMBER_ATTRIBUTE_213,sibd.NUMBER_ATTRIBUTE_213)) AS NUMBER_ATTRIBUTE_213
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_214,NVL(ssd.NUMBER_ATTRIBUTE_214,sibd.NUMBER_ATTRIBUTE_214)) AS NUMBER_ATTRIBUTE_214
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_215,NVL(ssd.NUMBER_ATTRIBUTE_215,sibd.NUMBER_ATTRIBUTE_215)) AS NUMBER_ATTRIBUTE_215
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_216,NVL(ssd.NUMBER_ATTRIBUTE_216,sibd.NUMBER_ATTRIBUTE_216)) AS NUMBER_ATTRIBUTE_216
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_217,NVL(ssd.NUMBER_ATTRIBUTE_217,sibd.NUMBER_ATTRIBUTE_217)) AS NUMBER_ATTRIBUTE_217
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_218,NVL(ssd.NUMBER_ATTRIBUTE_218,sibd.NUMBER_ATTRIBUTE_218)) AS NUMBER_ATTRIBUTE_218
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_219,NVL(ssd.NUMBER_ATTRIBUTE_219,sibd.NUMBER_ATTRIBUTE_219)) AS NUMBER_ATTRIBUTE_219
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_220,NVL(ssd.NUMBER_ATTRIBUTE_220,sibd.NUMBER_ATTRIBUTE_220)) AS NUMBER_ATTRIBUTE_220
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_221,NVL(ssd.NUMBER_ATTRIBUTE_221,sibd.NUMBER_ATTRIBUTE_221)) AS NUMBER_ATTRIBUTE_221
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_222,NVL(ssd.NUMBER_ATTRIBUTE_222,sibd.NUMBER_ATTRIBUTE_222)) AS NUMBER_ATTRIBUTE_222
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_223,NVL(ssd.NUMBER_ATTRIBUTE_223,sibd.NUMBER_ATTRIBUTE_223)) AS NUMBER_ATTRIBUTE_223
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_224,NVL(ssd.NUMBER_ATTRIBUTE_224,sibd.NUMBER_ATTRIBUTE_224)) AS NUMBER_ATTRIBUTE_224
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_225,NVL(ssd.NUMBER_ATTRIBUTE_225,sibd.NUMBER_ATTRIBUTE_225)) AS NUMBER_ATTRIBUTE_225
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_226,NVL(ssd.NUMBER_ATTRIBUTE_226,sibd.NUMBER_ATTRIBUTE_226)) AS NUMBER_ATTRIBUTE_226
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_227,NVL(ssd.NUMBER_ATTRIBUTE_227,sibd.NUMBER_ATTRIBUTE_227)) AS NUMBER_ATTRIBUTE_227
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_228,NVL(ssd.NUMBER_ATTRIBUTE_228,sibd.NUMBER_ATTRIBUTE_228)) AS NUMBER_ATTRIBUTE_228
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_229,NVL(ssd.NUMBER_ATTRIBUTE_229,sibd.NUMBER_ATTRIBUTE_229)) AS NUMBER_ATTRIBUTE_229
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_230,NVL(ssd.NUMBER_ATTRIBUTE_230,sibd.NUMBER_ATTRIBUTE_230)) AS NUMBER_ATTRIBUTE_230
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_231,NVL(ssd.NUMBER_ATTRIBUTE_231,sibd.NUMBER_ATTRIBUTE_231)) AS NUMBER_ATTRIBUTE_231
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_232,NVL(ssd.NUMBER_ATTRIBUTE_232,sibd.NUMBER_ATTRIBUTE_232)) AS NUMBER_ATTRIBUTE_232
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_233,NVL(ssd.NUMBER_ATTRIBUTE_233,sibd.NUMBER_ATTRIBUTE_233)) AS NUMBER_ATTRIBUTE_233
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_234,NVL(ssd.NUMBER_ATTRIBUTE_234,sibd.NUMBER_ATTRIBUTE_234)) AS NUMBER_ATTRIBUTE_234 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_235,NVL(ssd.NUMBER_ATTRIBUTE_235,sibd.NUMBER_ATTRIBUTE_235)) AS NUMBER_ATTRIBUTE_235
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_236,NVL(ssd.NUMBER_ATTRIBUTE_236,sibd.NUMBER_ATTRIBUTE_236)) AS NUMBER_ATTRIBUTE_236
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_237,NVL(ssd.NUMBER_ATTRIBUTE_237,sibd.NUMBER_ATTRIBUTE_237)) AS NUMBER_ATTRIBUTE_237
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_238,NVL(ssd.NUMBER_ATTRIBUTE_238,sibd.NUMBER_ATTRIBUTE_238)) AS NUMBER_ATTRIBUTE_238
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_239,NVL(ssd.NUMBER_ATTRIBUTE_239,sibd.NUMBER_ATTRIBUTE_239)) AS NUMBER_ATTRIBUTE_239
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_240,NVL(ssd.NUMBER_ATTRIBUTE_240,sibd.NUMBER_ATTRIBUTE_240)) AS NUMBER_ATTRIBUTE_240
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_241,NVL(ssd.NUMBER_ATTRIBUTE_241,sibd.NUMBER_ATTRIBUTE_241)) AS NUMBER_ATTRIBUTE_241
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_242,NVL(ssd.NUMBER_ATTRIBUTE_242,sibd.NUMBER_ATTRIBUTE_242)) AS NUMBER_ATTRIBUTE_242
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_243,NVL(ssd.NUMBER_ATTRIBUTE_243,sibd.NUMBER_ATTRIBUTE_243)) AS NUMBER_ATTRIBUTE_243
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_244,NVL(ssd.NUMBER_ATTRIBUTE_244,sibd.NUMBER_ATTRIBUTE_244)) AS NUMBER_ATTRIBUTE_244
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_245,NVL(ssd.NUMBER_ATTRIBUTE_245,sibd.NUMBER_ATTRIBUTE_245)) AS NUMBER_ATTRIBUTE_245
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_246,NVL(ssd.NUMBER_ATTRIBUTE_246,sibd.NUMBER_ATTRIBUTE_246)) AS NUMBER_ATTRIBUTE_246
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_247,NVL(ssd.NUMBER_ATTRIBUTE_247,sibd.NUMBER_ATTRIBUTE_247)) AS NUMBER_ATTRIBUTE_247
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_248,NVL(ssd.NUMBER_ATTRIBUTE_248,sibd.NUMBER_ATTRIBUTE_248)) AS NUMBER_ATTRIBUTE_248
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_249,NVL(ssd.NUMBER_ATTRIBUTE_249,sibd.NUMBER_ATTRIBUTE_249)) AS NUMBER_ATTRIBUTE_249
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_250,NVL(ssd.NUMBER_ATTRIBUTE_250,sibd.NUMBER_ATTRIBUTE_250)) AS NUMBER_ATTRIBUTE_250
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_251,NVL(ssd.NUMBER_ATTRIBUTE_251,sibd.NUMBER_ATTRIBUTE_251)) AS NUMBER_ATTRIBUTE_251
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_252,NVL(ssd.NUMBER_ATTRIBUTE_252,sibd.NUMBER_ATTRIBUTE_252)) AS NUMBER_ATTRIBUTE_252
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_253,NVL(ssd.NUMBER_ATTRIBUTE_253,sibd.NUMBER_ATTRIBUTE_253)) AS NUMBER_ATTRIBUTE_253
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_254,NVL(ssd.NUMBER_ATTRIBUTE_254,sibd.NUMBER_ATTRIBUTE_254)) AS NUMBER_ATTRIBUTE_254
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_255,NVL(ssd.NUMBER_ATTRIBUTE_255,sibd.NUMBER_ATTRIBUTE_255)) AS NUMBER_ATTRIBUTE_255
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_256,NVL(ssd.NUMBER_ATTRIBUTE_256,sibd.NUMBER_ATTRIBUTE_256)) AS NUMBER_ATTRIBUTE_256
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_257,NVL(ssd.NUMBER_ATTRIBUTE_257,sibd.NUMBER_ATTRIBUTE_257)) AS NUMBER_ATTRIBUTE_257
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_258,NVL(ssd.NUMBER_ATTRIBUTE_258,sibd.NUMBER_ATTRIBUTE_258)) AS NUMBER_ATTRIBUTE_258
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_259,NVL(ssd.NUMBER_ATTRIBUTE_259,sibd.NUMBER_ATTRIBUTE_259)) AS NUMBER_ATTRIBUTE_259
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_260,NVL(ssd.NUMBER_ATTRIBUTE_260,sibd.NUMBER_ATTRIBUTE_260)) AS NUMBER_ATTRIBUTE_260
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_261,NVL(ssd.NUMBER_ATTRIBUTE_261,sibd.NUMBER_ATTRIBUTE_261)) AS NUMBER_ATTRIBUTE_261
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_262,NVL(ssd.NUMBER_ATTRIBUTE_262,sibd.NUMBER_ATTRIBUTE_262)) AS NUMBER_ATTRIBUTE_262
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_263,NVL(ssd.NUMBER_ATTRIBUTE_263,sibd.NUMBER_ATTRIBUTE_263)) AS NUMBER_ATTRIBUTE_263
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_264,NVL(ssd.NUMBER_ATTRIBUTE_264,sibd.NUMBER_ATTRIBUTE_264)) AS NUMBER_ATTRIBUTE_264 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_265,NVL(ssd.NUMBER_ATTRIBUTE_265,sibd.NUMBER_ATTRIBUTE_265)) AS NUMBER_ATTRIBUTE_265
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_266,NVL(ssd.NUMBER_ATTRIBUTE_266,sibd.NUMBER_ATTRIBUTE_266)) AS NUMBER_ATTRIBUTE_266
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_267,NVL(ssd.NUMBER_ATTRIBUTE_267,sibd.NUMBER_ATTRIBUTE_267)) AS NUMBER_ATTRIBUTE_267
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_268,NVL(ssd.NUMBER_ATTRIBUTE_268,sibd.NUMBER_ATTRIBUTE_268)) AS NUMBER_ATTRIBUTE_268
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_269,NVL(ssd.NUMBER_ATTRIBUTE_269,sibd.NUMBER_ATTRIBUTE_269)) AS NUMBER_ATTRIBUTE_269
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_270,NVL(ssd.NUMBER_ATTRIBUTE_270,sibd.NUMBER_ATTRIBUTE_270)) AS NUMBER_ATTRIBUTE_270
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_271,NVL(ssd.NUMBER_ATTRIBUTE_271,sibd.NUMBER_ATTRIBUTE_271)) AS NUMBER_ATTRIBUTE_271
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_272,NVL(ssd.NUMBER_ATTRIBUTE_272,sibd.NUMBER_ATTRIBUTE_272)) AS NUMBER_ATTRIBUTE_272
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_273,NVL(ssd.NUMBER_ATTRIBUTE_273,sibd.NUMBER_ATTRIBUTE_273)) AS NUMBER_ATTRIBUTE_273
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_274,NVL(ssd.NUMBER_ATTRIBUTE_274,sibd.NUMBER_ATTRIBUTE_274)) AS NUMBER_ATTRIBUTE_274
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_275,NVL(ssd.NUMBER_ATTRIBUTE_275,sibd.NUMBER_ATTRIBUTE_275)) AS NUMBER_ATTRIBUTE_275
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_276,NVL(ssd.NUMBER_ATTRIBUTE_276,sibd.NUMBER_ATTRIBUTE_276)) AS NUMBER_ATTRIBUTE_276
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_277,NVL(ssd.NUMBER_ATTRIBUTE_277,sibd.NUMBER_ATTRIBUTE_277)) AS NUMBER_ATTRIBUTE_277
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_278,NVL(ssd.NUMBER_ATTRIBUTE_278,sibd.NUMBER_ATTRIBUTE_278)) AS NUMBER_ATTRIBUTE_278
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_279,NVL(ssd.NUMBER_ATTRIBUTE_279,sibd.NUMBER_ATTRIBUTE_279)) AS NUMBER_ATTRIBUTE_279
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_280,NVL(ssd.NUMBER_ATTRIBUTE_280,sibd.NUMBER_ATTRIBUTE_280)) AS NUMBER_ATTRIBUTE_280
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_281,NVL(ssd.NUMBER_ATTRIBUTE_281,sibd.NUMBER_ATTRIBUTE_281)) AS NUMBER_ATTRIBUTE_281
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_282,NVL(ssd.NUMBER_ATTRIBUTE_282,sibd.NUMBER_ATTRIBUTE_282)) AS NUMBER_ATTRIBUTE_282
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_283,NVL(ssd.NUMBER_ATTRIBUTE_283,sibd.NUMBER_ATTRIBUTE_283)) AS NUMBER_ATTRIBUTE_283
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_284,NVL(ssd.NUMBER_ATTRIBUTE_284,sibd.NUMBER_ATTRIBUTE_284)) AS NUMBER_ATTRIBUTE_284
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_285,NVL(ssd.NUMBER_ATTRIBUTE_285,sibd.NUMBER_ATTRIBUTE_285)) AS NUMBER_ATTRIBUTE_285
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_286,NVL(ssd.NUMBER_ATTRIBUTE_286,sibd.NUMBER_ATTRIBUTE_286)) AS NUMBER_ATTRIBUTE_286
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_287,NVL(ssd.NUMBER_ATTRIBUTE_287,sibd.NUMBER_ATTRIBUTE_287)) AS NUMBER_ATTRIBUTE_287
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_288,NVL(ssd.NUMBER_ATTRIBUTE_288,sibd.NUMBER_ATTRIBUTE_288)) AS NUMBER_ATTRIBUTE_288
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_289,NVL(ssd.NUMBER_ATTRIBUTE_289,sibd.NUMBER_ATTRIBUTE_289)) AS NUMBER_ATTRIBUTE_289
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_290,NVL(ssd.NUMBER_ATTRIBUTE_290,sibd.NUMBER_ATTRIBUTE_290)) AS NUMBER_ATTRIBUTE_290
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_291,NVL(ssd.NUMBER_ATTRIBUTE_291,sibd.NUMBER_ATTRIBUTE_291)) AS NUMBER_ATTRIBUTE_291
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_292,NVL(ssd.NUMBER_ATTRIBUTE_292,sibd.NUMBER_ATTRIBUTE_292)) AS NUMBER_ATTRIBUTE_292
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_293,NVL(ssd.NUMBER_ATTRIBUTE_293,sibd.NUMBER_ATTRIBUTE_293)) AS NUMBER_ATTRIBUTE_293
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_294,NVL(ssd.NUMBER_ATTRIBUTE_294,sibd.NUMBER_ATTRIBUTE_294)) AS NUMBER_ATTRIBUTE_294 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssdo.NUMBER_ATTRIBUTE_295,NVL(ssd.NUMBER_ATTRIBUTE_295,sibd.NUMBER_ATTRIBUTE_295)) AS NUMBER_ATTRIBUTE_295
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_296,NVL(ssd.NUMBER_ATTRIBUTE_296,sibd.NUMBER_ATTRIBUTE_296)) AS NUMBER_ATTRIBUTE_296
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_297,NVL(ssd.NUMBER_ATTRIBUTE_297,sibd.NUMBER_ATTRIBUTE_297)) AS NUMBER_ATTRIBUTE_297
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_298,NVL(ssd.NUMBER_ATTRIBUTE_298,sibd.NUMBER_ATTRIBUTE_298)) AS NUMBER_ATTRIBUTE_298
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_299,NVL(ssd.NUMBER_ATTRIBUTE_299,sibd.NUMBER_ATTRIBUTE_299)) AS NUMBER_ATTRIBUTE_299
		   ,NVL(ssdo.NUMBER_ATTRIBUTE_300,NVL(ssd.NUMBER_ATTRIBUTE_300,sibd.NUMBER_ATTRIBUTE_300)) AS NUMBER_ATTRIBUTE_300
		   ,NVL(ssd.STRING_ATTRIBUTE_1,sibd.STRING_ATTRIBUTE_1) AS STRING_ATTRIBUTE_1
		   ,NVL(ssd.STRING_ATTRIBUTE_2,sibd.STRING_ATTRIBUTE_2) AS STRING_ATTRIBUTE_2
		   ,NVL(ssd.STRING_ATTRIBUTE_3,sibd.STRING_ATTRIBUTE_3) AS STRING_ATTRIBUTE_3
		   ,NVL(ssd.STRING_ATTRIBUTE_4,sibd.STRING_ATTRIBUTE_4) AS STRING_ATTRIBUTE_4
		   ,NVL(ssd.STRING_ATTRIBUTE_5,sibd.STRING_ATTRIBUTE_5) AS STRING_ATTRIBUTE_5
		   ,NVL(ssd.STRING_ATTRIBUTE_6,sibd.STRING_ATTRIBUTE_6) AS STRING_ATTRIBUTE_6
		   ,NVL(ssd.STRING_ATTRIBUTE_7,sibd.STRING_ATTRIBUTE_7) AS STRING_ATTRIBUTE_7
		   ,NVL(ssd.STRING_ATTRIBUTE_8,sibd.STRING_ATTRIBUTE_8) AS STRING_ATTRIBUTE_8
		   ,NVL(ssd.STRING_ATTRIBUTE_9,sibd.STRING_ATTRIBUTE_9) AS STRING_ATTRIBUTE_9
		   ,NVL(ssd.STRING_ATTRIBUTE_10,sibd.STRING_ATTRIBUTE_10) AS STRING_ATTRIBUTE_10
		   ,NVL(ssd.STRING_ATTRIBUTE_11,sibd.STRING_ATTRIBUTE_11) AS STRING_ATTRIBUTE_11
		   ,NVL(ssd.STRING_ATTRIBUTE_12,sibd.STRING_ATTRIBUTE_12) AS STRING_ATTRIBUTE_12
		   ,NVL(ssd.STRING_ATTRIBUTE_13,sibd.STRING_ATTRIBUTE_13) AS STRING_ATTRIBUTE_13
		   ,NVL(ssd.STRING_ATTRIBUTE_14,sibd.STRING_ATTRIBUTE_14) AS STRING_ATTRIBUTE_14
		   ,NVL(ssd.STRING_ATTRIBUTE_15,sibd.STRING_ATTRIBUTE_15) AS STRING_ATTRIBUTE_15
		   ,NVL(ssd.STRING_ATTRIBUTE_16,sibd.STRING_ATTRIBUTE_16) AS STRING_ATTRIBUTE_16
		   ,NVL(ssd.STRING_ATTRIBUTE_17,sibd.STRING_ATTRIBUTE_17) AS STRING_ATTRIBUTE_17
		   ,NVL(ssd.STRING_ATTRIBUTE_18,sibd.STRING_ATTRIBUTE_18) AS STRING_ATTRIBUTE_18
		   ,NVL(ssd.STRING_ATTRIBUTE_19,sibd.STRING_ATTRIBUTE_19) AS STRING_ATTRIBUTE_19
		   ,NVL(ssd.STRING_ATTRIBUTE_20,sibd.STRING_ATTRIBUTE_20) AS STRING_ATTRIBUTE_20
		   ,NVL(ssd.STRING_ATTRIBUTE_21,sibd.STRING_ATTRIBUTE_21) AS STRING_ATTRIBUTE_21
		   ,NVL(ssd.STRING_ATTRIBUTE_22,sibd.STRING_ATTRIBUTE_22) AS STRING_ATTRIBUTE_22
		   ,NVL(ssd.STRING_ATTRIBUTE_23,sibd.STRING_ATTRIBUTE_23) AS STRING_ATTRIBUTE_23
		   ,NVL(ssd.STRING_ATTRIBUTE_24,sibd.STRING_ATTRIBUTE_24) AS STRING_ATTRIBUTE_24 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssd.STRING_ATTRIBUTE_25,sibd.STRING_ATTRIBUTE_25) AS STRING_ATTRIBUTE_25
		   ,NVL(ssd.STRING_ATTRIBUTE_26,sibd.STRING_ATTRIBUTE_26) AS STRING_ATTRIBUTE_26
		   ,NVL(ssd.STRING_ATTRIBUTE_27,sibd.STRING_ATTRIBUTE_27) AS STRING_ATTRIBUTE_27
		   ,NVL(ssd.STRING_ATTRIBUTE_28,sibd.STRING_ATTRIBUTE_28) AS STRING_ATTRIBUTE_28
		   ,NVL(ssd.STRING_ATTRIBUTE_29,sibd.STRING_ATTRIBUTE_29) AS STRING_ATTRIBUTE_29
		   ,NVL(ssd.STRING_ATTRIBUTE_30,sibd.STRING_ATTRIBUTE_30) AS STRING_ATTRIBUTE_30
		   ,NVL(ssd.STRING_ATTRIBUTE_31,sibd.STRING_ATTRIBUTE_31) AS STRING_ATTRIBUTE_31
		   ,NVL(ssd.STRING_ATTRIBUTE_32,sibd.STRING_ATTRIBUTE_32) AS STRING_ATTRIBUTE_32
		   ,NVL(ssd.STRING_ATTRIBUTE_33,sibd.STRING_ATTRIBUTE_33) AS STRING_ATTRIBUTE_33
		   ,NVL(ssd.STRING_ATTRIBUTE_34,sibd.STRING_ATTRIBUTE_34) AS STRING_ATTRIBUTE_34
		   ,NVL(ssd.STRING_ATTRIBUTE_35,sibd.STRING_ATTRIBUTE_35) AS STRING_ATTRIBUTE_35
		   ,NVL(ssd.STRING_ATTRIBUTE_36,sibd.STRING_ATTRIBUTE_36) AS STRING_ATTRIBUTE_36
		   ,NVL(ssd.STRING_ATTRIBUTE_37,sibd.STRING_ATTRIBUTE_37) AS STRING_ATTRIBUTE_37
		   ,NVL(ssd.STRING_ATTRIBUTE_38,sibd.STRING_ATTRIBUTE_38) AS STRING_ATTRIBUTE_38
		   ,NVL(ssd.STRING_ATTRIBUTE_39,sibd.STRING_ATTRIBUTE_39) AS STRING_ATTRIBUTE_39
		   ,NVL(ssd.STRING_ATTRIBUTE_40,sibd.STRING_ATTRIBUTE_40) AS STRING_ATTRIBUTE_40
		   ,NVL(ssd.STRING_ATTRIBUTE_41,sibd.STRING_ATTRIBUTE_41) AS STRING_ATTRIBUTE_41
		   ,NVL(ssd.STRING_ATTRIBUTE_42,sibd.STRING_ATTRIBUTE_42) AS STRING_ATTRIBUTE_42
		   ,NVL(ssd.STRING_ATTRIBUTE_43,sibd.STRING_ATTRIBUTE_43) AS STRING_ATTRIBUTE_43
		   ,NVL(ssd.STRING_ATTRIBUTE_44,sibd.STRING_ATTRIBUTE_44) AS STRING_ATTRIBUTE_44
		   ,NVL(ssd.STRING_ATTRIBUTE_45,sibd.STRING_ATTRIBUTE_45) AS STRING_ATTRIBUTE_45
		   ,NVL(ssd.STRING_ATTRIBUTE_46,sibd.STRING_ATTRIBUTE_46) AS STRING_ATTRIBUTE_46
		   ,NVL(ssd.STRING_ATTRIBUTE_47,sibd.STRING_ATTRIBUTE_47) AS STRING_ATTRIBUTE_47
		   ,NVL(ssd.STRING_ATTRIBUTE_48,sibd.STRING_ATTRIBUTE_48) AS STRING_ATTRIBUTE_48
		   ,NVL(ssd.STRING_ATTRIBUTE_49,sibd.STRING_ATTRIBUTE_49) AS STRING_ATTRIBUTE_49
		   ,NVL(ssd.STRING_ATTRIBUTE_50,sibd.STRING_ATTRIBUTE_50) AS STRING_ATTRIBUTE_50
		   ,NVL(ssd.STRING_ATTRIBUTE_51,sibd.STRING_ATTRIBUTE_51) AS STRING_ATTRIBUTE_51
		   ,NVL(ssd.STRING_ATTRIBUTE_52,sibd.STRING_ATTRIBUTE_52) AS STRING_ATTRIBUTE_52
		   ,NVL(ssd.STRING_ATTRIBUTE_53,sibd.STRING_ATTRIBUTE_53) AS STRING_ATTRIBUTE_53
		   ,NVL(ssd.STRING_ATTRIBUTE_54,sibd.STRING_ATTRIBUTE_54) AS STRING_ATTRIBUTE_54 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssd.STRING_ATTRIBUTE_55,sibd.STRING_ATTRIBUTE_55) AS STRING_ATTRIBUTE_55
		   ,NVL(ssd.STRING_ATTRIBUTE_56,sibd.STRING_ATTRIBUTE_56) AS STRING_ATTRIBUTE_56
		   ,NVL(ssd.STRING_ATTRIBUTE_57,sibd.STRING_ATTRIBUTE_57) AS STRING_ATTRIBUTE_57
		   ,NVL(ssd.STRING_ATTRIBUTE_58,sibd.STRING_ATTRIBUTE_58) AS STRING_ATTRIBUTE_58
		   ,NVL(ssd.STRING_ATTRIBUTE_59,sibd.STRING_ATTRIBUTE_59) AS STRING_ATTRIBUTE_59
		   ,NVL(ssd.STRING_ATTRIBUTE_60,sibd.STRING_ATTRIBUTE_60) AS STRING_ATTRIBUTE_60
		   ,NVL(ssd.STRING_ATTRIBUTE_61,sibd.STRING_ATTRIBUTE_61) AS STRING_ATTRIBUTE_61
		   ,NVL(ssd.STRING_ATTRIBUTE_62,sibd.STRING_ATTRIBUTE_62) AS STRING_ATTRIBUTE_62
		   ,NVL(ssd.STRING_ATTRIBUTE_63,sibd.STRING_ATTRIBUTE_63) AS STRING_ATTRIBUTE_63
		   ,NVL(ssd.STRING_ATTRIBUTE_64,sibd.STRING_ATTRIBUTE_64) AS STRING_ATTRIBUTE_64
		   ,NVL(ssd.STRING_ATTRIBUTE_65,sibd.STRING_ATTRIBUTE_65) AS STRING_ATTRIBUTE_65
		   ,NVL(ssd.STRING_ATTRIBUTE_66,sibd.STRING_ATTRIBUTE_66) AS STRING_ATTRIBUTE_66
		   ,NVL(ssd.STRING_ATTRIBUTE_67,sibd.STRING_ATTRIBUTE_67) AS STRING_ATTRIBUTE_67
		   ,NVL(ssd.STRING_ATTRIBUTE_68,sibd.STRING_ATTRIBUTE_68) AS STRING_ATTRIBUTE_68
		   ,NVL(ssd.STRING_ATTRIBUTE_69,sibd.STRING_ATTRIBUTE_69) AS STRING_ATTRIBUTE_69
		   ,NVL(ssd.STRING_ATTRIBUTE_70,sibd.STRING_ATTRIBUTE_70) AS STRING_ATTRIBUTE_70
		   ,NVL(ssd.STRING_ATTRIBUTE_71,sibd.STRING_ATTRIBUTE_71) AS STRING_ATTRIBUTE_71
		   ,NVL(ssd.STRING_ATTRIBUTE_72,sibd.STRING_ATTRIBUTE_72) AS STRING_ATTRIBUTE_72
		   ,NVL(ssd.STRING_ATTRIBUTE_73,sibd.STRING_ATTRIBUTE_73) AS STRING_ATTRIBUTE_73
		   ,NVL(ssd.STRING_ATTRIBUTE_74,sibd.STRING_ATTRIBUTE_74) AS STRING_ATTRIBUTE_74
		   ,NVL(ssd.STRING_ATTRIBUTE_75,sibd.STRING_ATTRIBUTE_75) AS STRING_ATTRIBUTE_75
		   ,NVL(ssd.STRING_ATTRIBUTE_76,sibd.STRING_ATTRIBUTE_76) AS STRING_ATTRIBUTE_76
		   ,NVL(ssd.STRING_ATTRIBUTE_77,sibd.STRING_ATTRIBUTE_77) AS STRING_ATTRIBUTE_77
		   ,NVL(ssd.STRING_ATTRIBUTE_78,sibd.STRING_ATTRIBUTE_78) AS STRING_ATTRIBUTE_78
		   ,NVL(ssd.STRING_ATTRIBUTE_79,sibd.STRING_ATTRIBUTE_79) AS STRING_ATTRIBUTE_79
		   ,NVL(ssd.STRING_ATTRIBUTE_80,sibd.STRING_ATTRIBUTE_80) AS STRING_ATTRIBUTE_80
		   ,NVL(ssd.STRING_ATTRIBUTE_81,sibd.STRING_ATTRIBUTE_81) AS STRING_ATTRIBUTE_81
		   ,NVL(ssd.STRING_ATTRIBUTE_82,sibd.STRING_ATTRIBUTE_82) AS STRING_ATTRIBUTE_82
		   ,NVL(ssd.STRING_ATTRIBUTE_83,sibd.STRING_ATTRIBUTE_83) AS STRING_ATTRIBUTE_83
		   ,NVL(ssd.STRING_ATTRIBUTE_84,sibd.STRING_ATTRIBUTE_84) AS STRING_ATTRIBUTE_84 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssd.STRING_ATTRIBUTE_85,sibd.STRING_ATTRIBUTE_85) AS STRING_ATTRIBUTE_85
		   ,NVL(ssd.STRING_ATTRIBUTE_86,sibd.STRING_ATTRIBUTE_86) AS STRING_ATTRIBUTE_86
		   ,NVL(ssd.STRING_ATTRIBUTE_87,sibd.STRING_ATTRIBUTE_87) AS STRING_ATTRIBUTE_87
		   ,NVL(ssd.STRING_ATTRIBUTE_88,sibd.STRING_ATTRIBUTE_88) AS STRING_ATTRIBUTE_88
		   ,NVL(ssd.STRING_ATTRIBUTE_89,sibd.STRING_ATTRIBUTE_89) AS STRING_ATTRIBUTE_89
		   ,NVL(ssd.STRING_ATTRIBUTE_90,sibd.STRING_ATTRIBUTE_90) AS STRING_ATTRIBUTE_90
		   ,NVL(ssd.STRING_ATTRIBUTE_91,sibd.STRING_ATTRIBUTE_91) AS STRING_ATTRIBUTE_91
		   ,NVL(ssd.STRING_ATTRIBUTE_92,sibd.STRING_ATTRIBUTE_92) AS STRING_ATTRIBUTE_92
		   ,NVL(ssd.STRING_ATTRIBUTE_93,sibd.STRING_ATTRIBUTE_93) AS STRING_ATTRIBUTE_93
		   ,NVL(ssd.STRING_ATTRIBUTE_94,sibd.STRING_ATTRIBUTE_94) AS STRING_ATTRIBUTE_94
		   ,NVL(ssd.STRING_ATTRIBUTE_95,sibd.STRING_ATTRIBUTE_95) AS STRING_ATTRIBUTE_95
		   ,NVL(ssd.STRING_ATTRIBUTE_96,sibd.STRING_ATTRIBUTE_96) AS STRING_ATTRIBUTE_96
		   ,NVL(ssd.STRING_ATTRIBUTE_97,sibd.STRING_ATTRIBUTE_97) AS STRING_ATTRIBUTE_97
		   ,NVL(ssd.STRING_ATTRIBUTE_98,sibd.STRING_ATTRIBUTE_98) AS STRING_ATTRIBUTE_98
		   ,NVL(ssd.STRING_ATTRIBUTE_99,sibd.STRING_ATTRIBUTE_99) AS STRING_ATTRIBUTE_99
		   ,NVL(ssd.STRING_ATTRIBUTE_100,sibd.STRING_ATTRIBUTE_100) AS STRING_ATTRIBUTE_100
		   ,NVL(ssd.STRING_ATTRIBUTE_101,sibd.STRING_ATTRIBUTE_101) AS STRING_ATTRIBUTE_101
		   ,NVL(ssd.STRING_ATTRIBUTE_102,sibd.STRING_ATTRIBUTE_102) AS STRING_ATTRIBUTE_102
		   ,NVL(ssd.STRING_ATTRIBUTE_103,sibd.STRING_ATTRIBUTE_103) AS STRING_ATTRIBUTE_103
		   ,NVL(ssd.STRING_ATTRIBUTE_104,sibd.STRING_ATTRIBUTE_104) AS STRING_ATTRIBUTE_104
		   ,NVL(ssd.STRING_ATTRIBUTE_105,sibd.STRING_ATTRIBUTE_105) AS STRING_ATTRIBUTE_105
		   ,NVL(ssd.STRING_ATTRIBUTE_106,sibd.STRING_ATTRIBUTE_106) AS STRING_ATTRIBUTE_106
		   ,NVL(ssd.STRING_ATTRIBUTE_107,sibd.STRING_ATTRIBUTE_107) AS STRING_ATTRIBUTE_107
		   ,NVL(ssd.STRING_ATTRIBUTE_108,sibd.STRING_ATTRIBUTE_108) AS STRING_ATTRIBUTE_108
		   ,NVL(ssd.STRING_ATTRIBUTE_109,sibd.STRING_ATTRIBUTE_109) AS STRING_ATTRIBUTE_109
		   ,NVL(ssd.STRING_ATTRIBUTE_110,sibd.STRING_ATTRIBUTE_110) AS STRING_ATTRIBUTE_110
		   ,NVL(ssd.STRING_ATTRIBUTE_111,sibd.STRING_ATTRIBUTE_111) AS STRING_ATTRIBUTE_111
		   ,NVL(ssd.STRING_ATTRIBUTE_112,sibd.STRING_ATTRIBUTE_112) AS STRING_ATTRIBUTE_112
		   ,NVL(ssd.STRING_ATTRIBUTE_113,sibd.STRING_ATTRIBUTE_113) AS STRING_ATTRIBUTE_113
		   ,NVL(ssd.STRING_ATTRIBUTE_114,sibd.STRING_ATTRIBUTE_114) AS STRING_ATTRIBUTE_114 ';

	dbms_lob.append(v_sql,v_appendsql);

	v_appendsql := ',NVL(ssd.STRING_ATTRIBUTE_115,sibd.STRING_ATTRIBUTE_115) AS STRING_ATTRIBUTE_115
		   ,NVL(ssd.STRING_ATTRIBUTE_116,sibd.STRING_ATTRIBUTE_116) AS STRING_ATTRIBUTE_116
		   ,NVL(ssd.STRING_ATTRIBUTE_117,sibd.STRING_ATTRIBUTE_117) AS STRING_ATTRIBUTE_117
		   ,NVL(ssd.STRING_ATTRIBUTE_118,sibd.STRING_ATTRIBUTE_118) AS STRING_ATTRIBUTE_118
		   ,NVL(ssd.STRING_ATTRIBUTE_119,sibd.STRING_ATTRIBUTE_119) AS STRING_ATTRIBUTE_119
		   ,NVL(ssd.STRING_ATTRIBUTE_120,sibd.STRING_ATTRIBUTE_120) AS STRING_ATTRIBUTE_120
		   ,NVL(ssd.STRING_ATTRIBUTE_121,sibd.STRING_ATTRIBUTE_121) AS STRING_ATTRIBUTE_121
		   ,NVL(ssd.STRING_ATTRIBUTE_122,sibd.STRING_ATTRIBUTE_122) AS STRING_ATTRIBUTE_122
		   ,NVL(ssd.STRING_ATTRIBUTE_123,sibd.STRING_ATTRIBUTE_123) AS STRING_ATTRIBUTE_123
		   ,NVL(ssd.STRING_ATTRIBUTE_124,sibd.STRING_ATTRIBUTE_124) AS STRING_ATTRIBUTE_124
		   ,NVL(ssd.STRING_ATTRIBUTE_125,sibd.STRING_ATTRIBUTE_125) AS STRING_ATTRIBUTE_125
			FROM SAE_INTERIM_BASEDATA sibd
			JOIN SAE_SCENARIO ss               ON sibd.BASELINE_ID = ss.BASELINE_ID  AND sibd.TIMELINE_ID = ss.TIMELINE_ID
			JOIN SAE_SCENARIO_DATA ssd         ON ssd.SCENARIO_ID = ss.id AND ssd.PERIOD_ID = sibd.PERIOD_ID AND ssd.ENTITY_ID = sibd.ENTITY_ID
			JOIN SAE_ENTITY se                 ON ss.scenario_type = se.scenario_type   AND sibd.ENTITY_ID = se.ID
			LEFT JOIN SAE_SCENDATA_OVERRIDE ssdo ON ss.ID = ssdo.SCENARIO_ID AND sibd.PERIOD_ID = ssdo.PERIOD_ID  AND sibd.ENTITY_ID = ssdo.ENTITY_ID
      WHERE ssd.SCENARIO_ID ='|| p_scenario_id;

	dbms_lob.append(v_sql,v_appendsql);

	v_step := '2.1';
	v_msg_param_str := substr('CREATE TABLE '|| v_table_name ||' AS ' || v_sql, 1, 4000);
	sae_log_write ('I',v_application,v_function,v_step , v_msg_param_str, null,'T',1,null,null,null,null,v_log_struct_id);

	EXECUTE IMMEDIATE 'CREATE TABLE '|| v_table_name ||' AS ' || v_sql ;

--	pack_sae_utils.disable_parallel_processing;
	v_step := '2.2';
	v_msg_param_str := substr('CREATE INDEX I1_'|| v_table_name ||' ON '|| v_table_name ||' ( ENTITY_ID)', 1, 4000);
	sae_log_write ('I',v_application,v_function,v_step , v_msg_param_str, null,'T',1,null,null,null,null,v_log_struct_id);

	EXECUTE IMMEDIATE 'CREATE INDEX I1_'|| v_table_name ||' ON '|| v_table_name ||' ( ENTITY_ID)';

	register_temp_table ( p_table_name => v_table_name, p_table_type => 'ADMIN');

	v_step := '3.1';
	sae_log_write ('I',v_application,v_function,v_step , 'After Registration ', null,'T',1,null,null,null,null,v_log_struct_id);

	COMMIT;

	sae_log_end ( null,'N', v_log_struct_id );

END create_exp_temp_tab;

PROCEDURE drop_exp_temp_tab (p_context_id number, p_scenario_id number ) IS
	v_count NUMBER;
	v_table_name VARCHAR2(30) := 'SAE_EXPORT_TEMP_';
	v_select_sql VARCHAR2(32000);

BEGIN

	v_table_name := v_table_name||p_scenario_id;
	pack_context.contextid_open(p_context_id);

	-- check if table exists
	SELECT count(*)
	INTO v_count
	FROM user_tables
	WHERE table_name = v_table_name;

	if (v_count  > 0) then
		unregister_temp_table(v_table_name);

		COMMIT;

		EXECUTE IMMEDIATE ' drop table ' || v_table_name;
	end if;

END drop_exp_temp_tab;

PROCEDURE REGISTER_TEMP_TABLE ( p_table_name cd_fdw_structure.table_name%type, p_table_type cd_fdw_structure.table_type%type, p_standard_custom VARCHAR2 DEFAULT 'S', p_comp_code VARCHAR2 DEFAULT 'CUSTOM', p_allow_long_name VARCHAR2 DEFAULT 'N'  ) IS
  v_cnt               NUMBER;
  v_product           VARCHAR2(10) := 'Y';  -- SAE
  v_add_check_error   VARCHAR2(10) := 'N';  -- no check error columns required for temp table
  v_fpm_id            cd_fdw_structure.fpm_id%type := 'SAETMP';
  c_current_component CD_FDW_STRUCTURE.COMP_CODE%TYPE := 'SAE';

BEGIN
	-- check table exist in db
	SELECT count(*)
	INTO v_cnt
	FROM user_tables
	WHERE table_name = p_table_name;

	IF v_cnt = 0 THEN
		RAISE_APPLICATION_ERROR(-20001, 'ERROR @ PACK_SAE_UTILS.REGISTER_DB_OBJECTS : Table does not exists  =' || p_table_name);
	END IF;

	-- check entry for table exist in cd_fdw_parameters

	SELECT count(*)
	INTO v_cnt
	FROM cd_fdw_structure
	WHERE table_name = p_table_name;

	IF v_cnt = 0 THEN
		pack_fermat.set_current_component(c_current_component);

		pack_fermat.create_table(
			v_table_name              => p_table_name,
			v_table_type              => p_table_type,
			v_products                => v_product,
			v_add_check_error         => v_add_check_error,
			v_fpm_id                  => v_fpm_id,
			v_standard_custom         => p_standard_custom,
			v_comp_code               => p_comp_code,
			v_generated               => 'Y',
      v_allow_long_name         => p_allow_long_name
		);

		execute immediate q'[UPDATE cd_fdw_structure SET relevant_for = 'Y' WHERE table_name = ']' || p_table_name||q'[']';
		commit;
		pack_db_script.create_grants_for_object(p_table_name);

		-- Changing default value using schema owner name
		change_owner_default_value(p_table_name);

	END IF;

	-- add entries for columns exists in table_columns
	FOR rec IN (SELECT
								column_id,
								column_name,
								CASE WHEN data_type IN ('CHAR', 'VARCHAR2')
									THEN data_type || '(' || data_length || ')'
								ELSE data_type
								END AS data_type,
								nullable
							FROM user_tab_columns u
							WHERE table_name = p_table_name
							AND NOT EXISTS ( SELECT column_name
															 FROM table_columns c
															 WHERE u.table_name = c.table_name
																 AND u.column_name = c.column_name )
	)
	LOOP
		pack_fermat.add_column(
				v_table_name        => p_table_name,
				v_column_name       => rec.column_name,
				v_data_type         => rec.data_type,
				v_nullable          => rec.nullable,
				v_column_desc       => NULL,
				v_relevant_for      => v_product,
				v_fpm_id            => v_fpm_id
		);
	END LOOP;

END REGISTER_TEMP_TABLE;

PROCEDURE UNREGISTER_TEMP_TABLE ( p_table_name cd_fdw_structure.table_name%type ) IS
	v_cnt               NUMBER;
BEGIN
	-- check id table entry is in cd_fdw_structure
	DELETE FROM table_columns WHERE table_name = p_table_name;
	DELETE FROM cd_fdw_structure WHERE table_name = p_table_name;
	COMMIT;
END;

PROCEDURE TAB_SUBPART_DROP (v_tab_name VARCHAR2, v_subpart_col_id NUMBER) is
v_subpart_name VARCHAR2(30);
v_where_clause  VARCHAR2(4000);

BEGIN

v_where_clause := ' SCENARIO_ID = ' || v_subpart_col_id;

IF get_context(v_tab_name, v_subpart_col_id) <> 0
THEN

     -- Step 0: Open context for given scenario
     pack_context.contextid_open(get_context(v_tab_name, v_subpart_col_id));

     -- Step 1: Select sub-partition name for scenario Id belonging to context currently connected to.
     SELECT pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_tab_name, vv_value => v_subpart_col_id, v_default  => 'N', v_prefix => 'N', v_parenthesis => 'N')
       INTO v_subpart_name
       FROM dual;

     -- Step 2: Truncate subpartition if exists or Delete data if no subpartition exists

     IF   v_subpart_name IS NOT NULL
     THEN
      EXECUTE IMMEDIATE 'ALTER TABLE '||v_tab_name||' DROP SUBPARTITION '||v_subpart_name||' UPDATE INDEXES  ' ;
     END  IF;

END IF;

EXCEPTION
WHEN OTHERS
THEN RAISE_APPLICATION_ERROR(-20001, SUBSTR('ERROR @ PACK_SAE_UTILS.TAB_SUBPART_DROP : ' || SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), 1, 2048));

END TAB_SUBPART_DROP;

PROCEDURE DROP_CANCELED_SUBPART IS
  v_tab_name   varchar2(30) := 'SAE_SCENARIO_DATA';
  v_part_name  varchar2(100);
BEGIN
   FOR rec IN ( SELECT DISTINCT status, scenario_id
                FROM sae_process_task t
                WHERE status = 'Canceled'
                AND scenario_id IS NOT NULL)
   LOOP
      v_part_name := pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_tab_name, vv_value => rec.scenario_id, v_default  => 'N', v_prefix => 'N', v_parenthesis => 'N');
      if (v_part_name IS NOT NULL ) THEN
          EXECUTE IMMEDIATE 'ALTER TABLE '||v_tab_name||' DROP SUBPARTITION '||v_part_name ;
      END IF;
   END LOOP;
END DROP_CANCELED_SUBPART;

PROCEDURE SAU_CREATE_TAB_EXEC ( p_table_name cd_fdw_structure.table_name%type, p_sql_str CLOB ) IS
BEGIN
	EXECUTE IMMEDIATE p_sql_str;
	register_temp_table ( p_table_name, 'ADMIN') ;
	COMMIT;
END;

PROCEDURE RECHECK_IMPORTSET ( p_contextid number, p_import_set varchar2 , p_parallel_flag varchar2 default 'N' ) IS
v_cntr number := 0;
v_exists number;
/*v_max_cnt number := 5; */
BEGIN
	pack_context.contextid_open(p_contextid);
	if p_import_set = '*'
	then
		for rec in (
		SELECT
			cec.part_key,
			cec.table_name,
			cfs.relevant_for,
			cfs.table_type,
			cec.recheck_mode,
			cec.recheck_constraints,
			cec.where_clause
		FROM check_errors_counters cec,
			(SELECT pack_product.relevant_for_label(relevant_for) as relevant_for,table_type,table_name,pack_install.partition_key(table_type,null) as part_key
			 FROM cd_fdw_structure
			 WHERE object_type='TABLE'
			) cfs
		WHERE cec.part_key=cfs.part_key
					AND cec.table_name=cfs.table_name
		--        AND cec.table_name not like '%#%'
		ORDER BY 1
		)
		loop
      if upper(p_parallel_flag) = 'Y' then
        /*v_cntr := v_cntr + 1;
        if ( v_cntr <= v_max_cnt ) then *
            dbms_output.put_line('Table:'||rec.table_name);
            dbms_scheduler.create_job(job_name => 'J'||v_cntr, job_type => 'PLSQL_BLOCK',job_action => 'begin pack_batch.recheck_table(v_table_name => '''||rec.table_name||'''); commit; end;', start_date => sysdate, enabled => TRUE, auto_drop => TRUE, comments => 'recheck_'||rec.table_name);
        *end if;*/
				NULL;
			else
				pack_batch.recheck_table(v_table_name => rec.table_name);
			end if;
		end loop;
	else
    select count(*)
    into v_exists
    from import_tables
    where import_set_code=p_import_set
    and enable='Y';
    if v_exists > 0 then
		  pack_batch.global_recheck_import(v_import_set => p_import_set);
    else
      raise_application_error (-20001, 'Invalid Import Set : '|| p_import_set);
    end if;
	end if;
END RECHECK_IMPORTSET;

/*
  This procedure is for setting NULL values of ENDING BALANCE to 0. Designed to be called as Post SQL from CMM.
  Effort for umpqua story US216358.

  It expects one field name ENDING BALANCE ( case insensitive )
*/
PROCEDURE mature_ln_null_to_zero(V_WORKFLOW_ID NUMBER,V_SCENARIO_ID NUMBER,V_ENGINE_ID NUMBER)
is
v_Query_String VARCHAR2(100);
v_field_name   sae_field.name%type := 'ENDING BALANCE';
v_column_name  sae_field.column_name%type ;
BEGIN
  SELECT column_name
  INTO v_column_name
  FROM sae_field
  WHERE scenario_type = ( select scenario_type from sae_scenario  where id = V_SCENARIO_ID) and UPPER(name) = v_field_name;

  v_Query_String := 'update sae_scenario_data_'|| V_SCENARIO_ID ||' set '||v_column_name||' = 0 WHERE '||v_column_name||' IS NULL';
  EXECUTE immediate v_Query_String;
  COMMIT;
END MATURE_LN_NULL_TO_ZERO;

FUNCTION validate_sql(p_query varchar2) return varchar2
IS
    l_cursor number := dbms_sql.open_cursor;
    v_msg  varchar2(32000);
begin
    execute immediate 'alter session set cursor_sharing=force';
    dbms_sql.parse( l_cursor,P_query, dbms_sql.native );
    execute immediate 'alter session set cursor_sharing=exact';
    return '' ;
exception
    when others then
        v_msg := SQLERRM;
        execute immediate 'alter session set cursor_sharing=exact';
        dbms_sql.close_cursor( l_cursor );
        return v_msg ;
end;

PROCEDURE SCENDATA_SUBPART_SWAP( v_scenario_id number ) IS
 c_chk_point          number := 100000;
 v_swapping_table_id  number ;
 v_subpart_name       varchar2(128)  ;
 v_table_name         varchar2(128) := 'SAE_SCENARIO_DATA';
 v_column_str         varchar2(32000);
 v_swp_tab            varchar2(30) := 'SAE_SWAP_TMP_';
 v_sql_lob            clob;
 v_str                varchar2(32000);
 v_sql       clob;
 v_tmp_tab   varchar2(30) := 'SAE_SCENARIO_DATA_';
 v_function_name varchar2(4000) := 'SCENDATA_SUBPART_SWAP';
 v_step number;

BEGIN
  v_swp_tab  := v_swp_tab||v_scenario_id;
  v_tmp_tab  := v_tmp_tab||v_scenario_id;

	SELECT pack_db_partitioning.list_part_subpart_name2(v_owner => null, v_table_name => v_table_name, vv_value => v_scenario_id, v_default  => 'N', v_prefix => 'N', v_parenthesis => 'N')
  INTO v_subpart_name
  FROM dual;

   dbms_lob.createtemporary(v_sql, true);

  Begin

        --insert into testuser values ('In',1,sysdate);
    --commit;
    PACK_SAE_UTILS.CRE_SWAP_TAB(v_swp_tab,v_table_name);

  Exception
      When others THEN Raise;
  End;

  pack_log.log_write('I', 'F', v_function_name, 1.1, 'Created Swap table '||v_swp_tab, null);

  v_sql_lob := 'INSERT /*+ parallel ('||v_swp_tab||') */ INTO '||v_swp_tab||'(' ;
  --v_sql_lob := 'INSERT /*+ APPEND */ INTO '||v_swp_tab||'(' ;

  select csv_list_clob(column_name) cols
  into v_str
  from ( select column_name from user_tab_columns where table_name = v_tmp_tab and ( column_name not like 'E!_STRING%'  ESCAPE '!' AND column_name not like 'E!_DATE%'  ESCAPE '!'  )
  order by column_id );

  dbms_lob.append(v_sql_lob,v_str||') SELECT /*+ parallel ('||v_tmp_tab||') */ '|| v_str ||' FROM '||v_tmp_tab);
  --dbms_lob.append(v_sql_lob,v_str||') SELECT '|| v_str ||' FROM '||v_tmp_tab);

  pack_log.log_write('I', 'F', v_function_name, 1.2, 'Before Insert -- '||substr(v_sql_lob,1,3980), null);

  --pack_sae_utils.enable_parallel_processing; --commented for perf changes

  Begin

    EXECUTE IMMEDIATE v_sql_lob;
    commit;

    EXCEPTION
				WHEN OTHERS THEN
         EXECUTE IMMEDIATE 'Drop table '||v_swp_tab;
        raise;
   End;

  pack_log.log_write('I', 'F', v_function_name, 1.3, 'Inserted records to the table '||v_swp_tab, null);

  --pack_sae_utils.disable_parallel_processing; --commented for perf changes

  SELECT seq_swapping_table.nextval
  INTO v_swapping_table_id
  FROM DUAL;

  Begin

  INSERT INTO swapping_table (swapping_table_id,original_table_name,swapping_table_name,creator_name,create_time)
  VALUES (v_swapping_table_id, v_table_name, v_swp_tab, user, systimestamp);
  commit;

  EXCEPTION
				WHEN OTHERS THEN
        EXECUTE IMMEDIATE 'Drop table '||v_swp_tab;
        raise;
  END;

 pack_sae_swap.SCENDATA_SUBPART_SWAP1(v_scenario_id,v_swapping_table_id);

 EXCEPTION
				WHEN OTHERS THEN
        raise;

END SCENDATA_SUBPART_SWAP;

PROCEDURE CRE_SWAP_TAB(v_swap_tname VARCHAR2, v_table_name VARCHAR2) IS
v_sql varchar2(4000);
Begin

  v_sql := 'create table '||v_swap_tname||'  as select * from '||v_table_name ||' where 1 = 2';

  Execute immediate v_sql;

  pack_fermat.create_table(v_table_name => v_swap_tname,
                           v_table_type => 'CALC',
                            v_products => 'F',
                            v_fpm_id => NULL,
                            v_is_partitioned=>'N',
                            v_cancel_if_exist => 'Y',
                            v_add_check_error => 'N',
                            v_standard_custom => 'C',
                            v_COMP_CODE => 'CUSTOM');
  EXEC_DDL('SELECT * FROM DUAL', v_swap_tname);

Exception
  When others then
    RAISE;

END CRE_SWAP_TAB;

FUNCTION CHECK_SWAP_TAB(v_table_name VARCHAR2)  return number IS
v_cnt number;
Begin
    select count(*) into v_cnt from user_tables where table_name = v_table_name;

    return(v_cnt);

Exception
  When others then
    RAISE;

END CHECK_SWAP_TAB;

begin
	init_global_var();
end PACK_SAE_UTILS;