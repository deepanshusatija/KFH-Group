  CREATE TABLE "KFH_CUSTOM_PARAMETERS" 
   (	"PARAMETER" VARCHAR2(100 BYTE), 
	"VALUE" VARCHAR2(1000 BYTE)
   ) ;
   /
   

Insert into kfh_custom_parameters (PARAMETER,VALUE) values ('MONTHLY_RUN_DATE','20181231');
Insert into kfh_custom_parameters (PARAMETER,VALUE) values ('GOLDEN_WORKSPACE_ID_BAHRAIN','10');
Insert into kfh_custom_parameters (PARAMETER,VALUE) values ('GOLDEN_WORKSPACE_ID_MALAYSIA','12');
Insert into kfh_custom_parameters (PARAMETER,VALUE) values ('GOLDEN_WORKSPACE_ID_KUWAIT','11');
INSERT INTO kfh_custom_parameters (PARAMETER,VALUE) VALUES ('GOLDEN_CONTEXT_DATE','20181231');
commit;