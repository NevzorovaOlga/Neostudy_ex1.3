CREATE TABLE DM.DM_F101_ROUND_F_V2(
	FROM_DATE date,
	TO_DATE date,
	CHAPTER varchar(1),
	LEDGER_ACCOUNT varchar(5),
	CHARACTERISTIC varchar(1),
	BALANCE_IN_RUB numeric(23,8),
	R_BALANCE_IN_RUB numeric(23,8),
	BALANCE_IN_VAL numeric(23,8),
	R_BALANCE_IN_VAL numeric(23,8),
	BALANCE_IN_TOTAL numeric(23,8),
	R_BALANCE_IN_TOTAL numeric(23,8),
	TURN_DEB_RUB numeric(23,8),
	R_TURN_DEB_RUB numeric(23,8),
	TURN_DEB_VAL numeric(23,8),
	R_TURN_DEB_VAL numeric(23,8),
	TURN_DEB_TOTAL numeric(23,8),
	R_TURN_DEB_TOTAL numeric(23,8),
	TURN_CRE_RUB numeric(23,8),
	R_TURN_CRE_RUB numeric(23,8),
	TURN_CRE_VAL numeric(23,8),
	R_TURN_CRE_VAL numeric(23,8),
	TURN_CRE_TOTAL numeric(23,8),
	R_TURN_CRE_TOTAL numeric(23,8),
	BALANCE_OUT_RUB numeric(23,8),
	R_BALANCE_OUT_RUB numeric(23,8),
	BALANCE_OUT_VAL numeric(23,8),
	R_BALANCE_OUT_VAL numeric(23,8),
	BALANCE_OUT_TOTAL numeric(23,8),
	R_BALANCE_OUT_TOTAL numeric(23,8)
);

select * from DM.DM_F101_ROUND_F_V2;

truncate table DM.DM_F101_ROUND_F_V2;

create table dm.logs_csv ( 	
	record_id SERIAL PRIMARY KEY,
    operation VARCHAR(255),
    log_time TIMESTAMP WITHOUT TIME ZONE,
    status VARCHAR(255)	
    );
	
select * from dm.logs_csv; 

truncate table dm.logs_csv; 

CREATE SEQUENCE dm.seq_logs_csv START 1;
