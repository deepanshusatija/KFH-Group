create or replace 
PACKAGE PACK_KFH_BATCH AUTHID CURRENT_USER
AS
  FUNCTION return_partition_key(
      VV_TARGET_RD    DATE,
      VV_TABLE_NAME   VARCHAR2,
      VV_WORKSPACE_ID NUMBER,
      POS             NUMBER)
    RETURN VARCHAR2;
  -- Dynamically return the value of the desired context ID based on the value of Reporting Date, WorkSpace ID and Position
  -- Generally the Workspace ID and Position could be considered as Constant/ Static parameters
  FUNCTION return_context_id(
      VV_TARGET_RD    DATE,
      VV_WORKSPACE_ID VARCHAR2,
      POS             NUMBER)
    RETURN VARCHAR2;
  -- Performs Post ETL manual updates and Check Errors on RCO tables
  PROCEDURE CONTEXT_CREATE_KFH(
      P_RD         VARCHAR2,                -- yyyymmdd e.g.20160331
      P_ADJUSTMENT VARCHAR2 DEFAULT 'N'); ---- IF 'N' THEN MONTHLY.   ----This procedure CONTEXT_CREATE_KFH updated by sunil p. 10/22/2019
  PROCEDURE post_etl_process(
      VV_TARGET_RD DATE,
      VV_RESULT OUT VARCHAR2);
	
  procedure KFH_UPDATE_MONTH_END_DATE;
  
	PROCEDURE KFH_CREATE_ALL_CONTEXTS;
  
	PROCEDURE KFH_CHECK_ERROR_ALL_CONTEXTS;
  
  procedure KFH_TABLE_DATA_COPY(v_table IN VARCHAR2, v_src_context IN NUMBER, v_dest_context IN NUMBER, v_identifier_column IN VARCHAR2 default NULL,v_identifier_text IN VARCHAR2 default NULL,v_where_clause IN VARCHAR2 default null);

  PROCEDURE KFH_TCDR_FOR_SUBS_CBK;
  
  PROCEDURE KFH_TCDR_FOR_LOCAL;
  
  PROCEDURE KFH_TCDR_FOR_KWT_CBK;

  PROCEDURE KFH_CHECK_ERROR_CBK_DATA;
  
  PROCEDURE KFH_CHECK_ERROR_LOCAL_DATA;

  procedure KFH_DM_RCO(v_context_id in number, v_mapping_ids in varchar2 default NULL, v_alm_process_name in varchar2 default NULL);
		
  function KFH_DM_RCO_MALAYSIA_RETAIL return number;
  
  function KFH_DM_RCO_MALAYSIA_WHOLESALE return number;

  function KFH_DM_RCO_BAHRAIN_RETAIL return number;
  
  function KFH_DM_RCO_BAHRAIN_WHOLESALE return number;

  function KFH_DM_RCO_KUWAIT_RETAIL return number;
  
  function KFH_DM_RCO_KUWAIT_WHOLESALE return number;
  
  function KFH_DM_RCO_MALAYSIA_CBK_RETAIL return number;

  function KFH_DM_RCO_MALAYSIA_CBK_WS return number;
  
  function KFH_DM_RCO_BAHRAIN_CBK_RETAIL return number;
  
  function KFH_DM_RCO_BAHRAIN_CBK_WS return number;
  
  procedure KFH_MALAYSIA_RET_RESULTS;
  
  PROCEDURE KFH_MALAYSIA_WHS_RESULTS;
 
  PROCEDURE KFH_BAHRAIN_RET_RESULTS;

  PROCEDURE KFH_BAHRAIN_WHS_RESULTS;
  
  PROCEDURE KFH_KUWAIT_WHS_RESULTS;

  PROCEDURE KFH_KUWAIT_RET_RESULTS;
  
  PROCEDURE KFH_MALAYSIA_CBK_WHS_RESULTS;
  
  PROCEDURE KFH_MALAYSIA_CBK_RET_RESULTS;
    
  PROCEDURE KFH_BAHRAIN_CBK_WHS_RESULTS;
  
  PROCEDURE KFH_BAHRAIN_CBK_RET_RESULTS;
    
END PACK_KFH_BATCH;