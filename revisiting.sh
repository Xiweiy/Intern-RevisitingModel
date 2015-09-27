#!/bin/bash

# This script up loads the Twitter data the Teradata DB.
#
# Written by: Xiwei Yan
# Date: 2015-07-16

tStart=`date`
echo $0 started at $tStart.


TERADATA="1700"
USER_PWD="xyan0,N3pf79e5yt"
SERVER="TDAdhoc.intra.searshc.com"
PERM_DB="L2_MRKTGANLYS_T"

rm -f powerkeytrs.txt
rm -f POWERKEYS.txt

bteq <<EOF > bteq.log
.LOGON ${SERVER}/${USER_PWD};
.SET WIDTH 1024

-------------------------------------------------------------------------------------
------Step 0. Store the keys with reference to SOAR_NM, Cls_ds ----------------------
-------------------------------------------------------------------------------------

DROP TABLE shc_work_tbls.d5_xy_key;
CREATE TABLE shc_work_tbls.d5_xy_key AS (
	SELECT 
		A.SOAR_NM
		,B.div_no
		,B.div_nm
		,B.ln_no
		,B.ln_ds
		,B.sbl_no
		,B.sbl_ds
		,B.CLS_NO
		,B.CLS_DS
		,CAST(
			(CASE WHEN B.div_no IS NULL THEN '99'  ELSE CAST(CAST(B.div_no AS FORMAT'9(2)') AS CHAR(2))  END
				||'-'||CASE WHEN B.ln_no IS NULL THEN '99'  ELSE CAST(CAST(B.ln_no AS FORMAT'9(2)') AS CHAR(2))  END
				||'-'||CASE WHEN B.sbl_no IS NULL THEN '99'  ELSE CAST(CAST(B.sbl_no AS FORMAT'9(2)') AS CHAR(2))  END
				||'-'||CASE WHEN B.CLS_NO IS NULL THEN '999' ELSE CAST(CAST(B.CLS_NO AS FORMAT '9(3)') AS CHAR(3)) END
					) AS CHAR(12)) AS sortkey
	FROM cbr_mart_tbls.rd_srs_soar_bu A
		INNER JOIN LCI_DW_VIEWS.SPRS_PRODUCT B
			ON A.DIV_NO = B.DIV_NO
			AND A.itm_no = B.itm_no
	GROUP BY 1,2,3,4,5,6,7,8,9,10
	--ORDER BY 1,2,3,4,5,6,7,8,9,10
)WITH DATA PRIMARY INDEX(sortkey);
COLLECT STATS shc_work_tbls.d5_xy_key INDEX(sortkey);


-------------------------------------------------------------------------------------------
------Step 1. Create the table with total Count # of trips for all members
---------------------------------------------------------------------------------------------- 

DROP TABLE shc_work_tbls.d5_xy_total_member_purchase;
CREATE TABLE shc_work_tbls.d5_xy_total_member_purchase AS (
	SELECT 
		CAST(
			(CASE WHEN B.div_no IS NULL THEN '99'  ELSE CAST(CAST(B.div_no AS FORMAT'9(2)') AS CHAR(2))  END
				||'-'||CASE WHEN B.ln_no IS NULL THEN '99'  ELSE CAST(CAST(B.ln_no AS FORMAT'9(2)') AS CHAR(2))  END
				||'-'||CASE WHEN B.sbl_no IS NULL THEN '99'  ELSE CAST(CAST(B.sbl_no AS FORMAT'9(2)') AS CHAR(2))  END
				||'-'||CASE WHEN B.CLS_NO IS NULL THEN '999' ELSE CAST(CAST(B.CLS_NO AS FORMAT '9(3)') AS CHAR(3)) END
					) AS CHAR(12) ) AS sortkey
		,a.lyl_id_no
		,SUM(A.NMS) AS TRS_REV		
		,COUNT(unique A.TRS_DT) as nTrips  --Count Unique TRS DT INSTEAD OF MEMBER ID
	FROM crm_perm_tbls.sywr_sears_sales_ptd a
		LEFT JOIN LCI_DW_VIEWS.SPRS_PRODUCT B
			ON a.div_no = b.div_no
			AND a.itm_no = b.itm_no
	WHERE TRS_DT BETWEEN '2014-03-05' AND '2015-03-05'  --last year transactions
		AND lyl_id_no IS NOT NULL
		AND A.PRD_QTY > 0 ---product quantity >0 indicates the customer is not returning the items
		AND A.NMS > 0  --- price >0 also indicates the customer is not returning the items
	GROUP BY 1,2)
	WITH DATA PRIMARY INDEX(sortkey, lyl_id_no);
COLLECT STATS shc_work_tbls.d5_xy_total_member_purchase INDEX(sortkey, lyl_id_no);


-------------------------------------------------------------------------------------------
--------Step 2. CREATE TABLE WITH # OF MEMBERS WHO BUY MORE THAN 1 TIME ----------------
----------------------------------------------------------------------------------------

DROP TABLE shc_work_tbls.d5_xy_replenish_member_purchase;
CREATE TABLE shc_work_tbls.d5_xy_replenish_member_purchase AS (
	SELECT 
		sortkey,
		lyl_id_no,
		nTrips, 
		TRS_REV
	FROM shc_work_tbls.d5_xy_total_member_purchase
	WHERE nTrips >1)
	WITH DATA PRIMARY INDEX(sortkey,lyl_id_no);
COLLECT STATS shc_work_tbls.d5_xy_replenish_member_purchase INDEX(sortkey, lyl_id_no);

-------------------------------------------------------------------------------------
------Step 3. save the power keys with over 500k annual transactions and ratio > 0.05 
-------------------------------------------------------------------------------------
DROP TABLE shc_work_tbls.d5_xy_powerkey; 
CREATE TABLE shc_work_tbls.d5_xy_powerkey AS ( 
	SELECT 
		A.SORTKEY
		,A.nMBR AS TOTAL_MEMBER
		,B.nMBR AS RPT_MEMBER
		,B.nMBR*1.00/A.nMBR AS MEMBER_RATIO
		,A.nTrips AS TOTAL_TRIPS
		,B.nTrips AS RPT_TRIPS
		,B.nTrips*1.00/A.nTrips AS TRIP_RATIO 
		,A.REVENUE AS TOTAL_REVENUE
		,B.REVENUE AS REPEAT_REVENUE
		,B.REVENUE*1.0/A.REVENUE AS REV_RATIO 
		,C.SOAR_NM
		,C.div_no
		,C.ln_no
		,C.sbl_no
		,C.CLS_NO
		,C.CLS_DS
	FROM (
		SELECT 
			sortkey,
			COUNT(unique lyl_id_no) AS nMBR,
			SUM(nTrips) AS nTrips,
			SUM(TRS_REV) AS REVENUE
		FROM shc_work_tbls.d5_xy_total_member_purchase 
		GROUP BY sortkey) A
	INNER JOIN (
		SELECT
			sortkey,
			COUNT(unique lyl_id_no) AS nMBR,
			SUM(nTrips) AS nTrips,
			SUM(TRS_REV) AS REVENUE
		FROM shc_work_tbls.d5_xy_replenish_member_purchase 
		GROUP BY sortkey) B
		ON A.SORTKEY = B.SORTKEY
	INNER JOIN shc_work_tbls.d5_xy_key C
		ON A.SORTKEY = C.SORTKEY
	WHERE TOTAL_TRIPS > 500000
	AND MEMBER_RATIO > 0.05
	--ORDER BY SOAR_NM, TOTAL_REVENUE DESC
	)WITH DATA PRIMARY INDEX(SORTKEY);
	COLLECT STATS shc_work_tbls.d5_xy_powerkey INDEX(SORTKEY);

----------------------------------------------------------------------------
-- Step #4   Create the table of Last Year Purchase by Members with the selected keys
----------------------------------------------------------------------------

DROP TABLE shc_work_tbls.d5_xy_POWERKEYTRS;
CREATE TABLE shc_work_tbls.d5_xy_POWERKEYTRS AS (
	SELECT 
		A.CUS_IAN_ID_NO,
		A.lyl_id_no,
		A.TRS_DT,
		A.md_amt,
		A.PRD_QTY,
		(42067 - C.day_of_calendar) AS daySince,  --day since march 05, 2015
		CAST((CASE WHEN B.div_no IS NULL THEN '99'  ELSE CAST(CAST(B.div_no AS FORMAT'9(2)') AS CHAR(2))  END
			||'-'||CASE WHEN B.ln_no IS NULL THEN '99'  ELSE CAST(CAST(B.ln_no AS FORMAT'9(2)') AS CHAR(2))  END
			||'-'||CASE WHEN B.sbl_no IS NULL THEN '99'  ELSE CAST(CAST(B.sbl_no AS FORMAT'9(2)') AS CHAR(2))  END
			||'-'||CASE WHEN B.CLS_NO IS NULL THEN '999' ELSE CAST(CAST(B.CLS_NO AS FORMAT '9(3)') AS CHAR(3)) END
			) AS CHAR(12) ) AS sortkey
	FROM crm_perm_tbls.sywr_sears_sales_ptd A
		INNER JOIN LCI_DW_VIEWS.SPRS_PRODUCT B
			ON A.DIV_NO = B.DIV_NO
			AND A.ITM_NO = B.ITM_NO
		INNER JOIN Sys_Calendar.CALENDAR C
			ON a.TRS_DT = C.calendar_date
	WHERE lyl_id_no IS NOT NULL
		AND A.PRD_QTY >0
		AND A.NMS > 0
		AND A.TRS_DT BETWEEN '2014-03-05' AND '2015-03-05'
		AND SORTKEY IN (SELECT sortkey FROM shc_work_tbls.d5_xy_powerkey)
	GROUP BY 1,2,3,4,5,6,7
) WITH DATA PRIMARY INDEX(lyl_id_no);
COLLECT STATS shc_work_tbls.d5_xy_POWERKEYTRS INDEX(lyl_id_no);

----------------------------------------------------------------------------
-- Step #5: Output the Transaction dates by Members WHO REVISIT (WITH DAY INT>0)
----------------------------------------------------------------------------
.EXPORT RESET
.EXPORT FILE powerkeytrs.txt


SELECT 
	lyl_id_no,
	TRS_DT,
	sortkey,
	daySince,
	day_int,
	md_amt,
	PRD_QTY
FROM (
	SELECT 
		lyl_id_no,
		TRS_DT,
		sortkey,
		daySince,
		-(daySince - SUM(daySince) 
			OVER (PARTITION BY lyl_id_no,sortkey ORDER BY daySince ROWS BETWEEN 1 FOLLOWING  AND 1 FOLLOWING )) AS day_int,
		md_amt,
		PRD_QTY
	FROM shc_work_tbls.d5_xy_POWERKEYTRS
) A
WHERE day_int > 0
;

----------------------------------------------------------------------------
-- Step #6: Generate the Powerkeys + SOAR_NM + CLS_DS
----------------------------------------------------------------------------
.EXPORT RESET
.EXPORT FILE POWERKEYS.txt

SELECT 
		trim(SORTKEY)||'|'||trim(SOAR_NM)||'|'||trim(CLS_DS) AS DUMMY
	FROM shc_work_tbls.d5_xy_powerkey;

.LOGOFF;
EOF

RC=$?
echo bteq RC: $RC
if [ $RC -gt 10 ];
then
	echo $0 failed.  Return Code: $RC
	exit 1
else
	sed '2d' powerkeytrs.txt > tmp.txt; mv tmp.txt powerkeytrs.txt
	sed '1,2d' POWERKEYS.txt > tmp.txt; mv tmp.txt POWERKEYS.txt
fi
#

######################################################################################################
##Step 7 - R script fit the curve, rank the probs based on the lastest purchase of each member and assign deciles
######################################################################################################
rm -f deciles.csv
rm -f deciles_2.csv

R CMD BATCH --no-save --no-restore revisiting.r

#Remove the quotation mark come with strings
sed 's/"//g' deciles.csv > deciles_2.csv


######################################################################################################
##Step 8 - Upload the Above Table onto Teradata Server 
######################################################################################################

# Teradata tables.
SLP_TABLE="shc_work_tbls.d5_xy_decile"
SLP_FILE="deciles_2.csv"

#Fastload the dataset output by r with rank and prob
echo Upload member sortkey and probability to Teradata.
fastload <<EOF > import.log
SESSIONS 32;
TENACITY 5;
SLEEP 5;
ERRLIMIT 150;
.LOGON ${SERVER}/${USER_PWD};
DROP TABLE shc_work_tbls.hc_tmp_ET;
DROP TABLE shc_work_tbls.hc_tmp_UV;
DROP TABLE ${SLP_TABLE};
CREATE TABLE ${SLP_TABLE} (
	lyl_id_no DECIMAL(16,0)
	,sortkey CHAR(12)
	,daysSince SmallInt
	,p FLOAT
	,slp_rank SmallInt
) PRIMARY INDEX(lyl_id_no);
.SET RECORD VARTEXT ",";
BEGIN LOADING ${SLP_TABLE}
ERRORFILES shc_work_tbls.hc_tmp_ET, shc_work_tbls.hc_tmp_UV;
DEFINE
	f1(VARCHAR(17))
	,f2(VARCHAR(15))
	,f3(VARCHAR(10))
	,f4(VARCHAR(20))
	,f5(VARCHAR(10))
	FILE=${SLP_FILE};
INSERT INTO ${SLP_TABLE} VALUES (
	:f1
	,:f2
	,:f3
	,:f4
	,:f5
	);	
END LOADING;
LOGOFF;
.QUIT;
EOF

RC=$? # Return Code
if [ $RC -eq 0 ];
then
	echo Fastload completed.
else
	echo Fastload failed.  Return Code: $RC
	exit 1
fi


rm -f validation.txt

bteq <<EOF > bteq.log
.LOGON ${SERVER}/${USER_PWD};
.SET WIDTH 1024

----------------------------------------------------------------------------
-- Step #9   Create the Table of Transactions in the Next 2 week
----------------------------------------------------------------------------
DROP TABLE shc_work_tbls.d5_xy_TWOWEEKS;
CREATE TABLE shc_work_tbls.d5_xy_TWOWEEKS AS (
	SELECT 
		C.CUS_IAN_ID_NO,
		C.lyl_id_no,
		C.TRS_DT,
		C.md_amt,
		C.PRD_QTY,
		C.sortkey,
		D.slp_rank
	FROM ( 
		SELECT
			A.CUS_IAN_ID_NO,
			A.lyl_id_no,
			A.TRS_DT,
			A.md_amt,
			A.PRD_QTY,
			CAST((CASE WHEN B.div_no IS NULL THEN '99'  ELSE CAST(CAST(B.div_no AS FORMAT'9(2)') AS CHAR(2))  END
					||'-'||CASE WHEN B.ln_no IS NULL THEN '99'  ELSE CAST(CAST(B.ln_no AS FORMAT'9(2)') AS CHAR(2))  END
					||'-'||CASE WHEN B.sbl_no IS NULL THEN '99'  ELSE CAST(CAST(B.sbl_no AS FORMAT'9(2)') AS CHAR(2))  END
					||'-'||CASE WHEN B.CLS_NO IS NULL THEN '999' ELSE CAST(CAST(B.CLS_NO AS FORMAT '9(3)') AS CHAR(3)) END
					) AS CHAR(12) ) AS SORTKEY
		FROM crm_perm_tbls.sywr_sears_sales_ptd A
			INNER JOIN LCI_DW_VIEWS.SPRS_PRODUCT B
				ON A.DIV_NO = B.DIV_NO
				AND A.ITM_NO = B.ITM_NO
		WHERE A.TRS_DT BETWEEN '2015-03-05' AND '2015-03-19'
			AND A.PRD_QTY >0
			AND A.NMS > 0
			) C
	INNER JOIN 	shc_work_tbls.d5_xy_decile D
		ON C.SORTKEY = D.sortkey
		AND C.lyl_id_no = D.lyl_id_no
	GROUP BY 1,2,3,4,5,6,7
	) WITH DATA PRIMARY INDEX(lyl_id_no);
	COLLECT STATS shc_work_tbls.d5_xy_TWOWEEKS INDEX(lyl_id_no);


----------------------------------------------------------------------------
-- Step #10   Select the Validation Dataset of Transactions in the Next 1/2 Weeks
----------------------------------------------------------------------------
DROP TABLE shc_work_tbls.d5_xy_VALIDATION;
CREATE TABLE shc_work_tbls.d5_xy_VALIDATION AS (
	SELECT 
	A.ONE_WEEK_NMBR,
	B.TWO_WEEK_NMBR,
	A.ONE_WEEK_TRS,
	B.TWO_WEEK_TRS,
	A.SORTKEY,
	A.slp_rank,
	C.DECILE_TOTAL
FROM (
	SELECT 
		COUNT(UNIQUE lyl_id_no) AS ONE_WEEK_NMBR,
		SORTKEY,
		COUNT(UNIQUE CUS_IAN_ID_NO) AS ONE_WEEK_TRS,
		slp_rank
	FROM shc_work_tbls.d5_xy_TWOWEEKS 
	WHERE TRS_DT BETWEEN '2015-03-05' AND '2015-03-12'  --TRANSACTION IN NEXT WEEK
	GROUP BY SORTKEY, slp_rank
	) A
INNER JOIN (
	SELECT 
		COUNT(UNIQUE lyl_id_no) AS TWO_WEEK_NMBR,
		SORTKEY,
		COUNT(UNIQUE CUS_IAN_ID_NO) AS TWO_WEEK_TRS,
		slp_rank
	FROM shc_work_tbls.d5_xy_TWOWEEKS 
	WHERE TRS_DT BETWEEN '2015-03-05' AND '2015-03-19'  --TRANSACTION IN NEXT TWO WEEKS
	GROUP BY SORTKEY, slp_rank
	) B
	ON A.SORTKEY = B.sortkey
	AND A.slp_rank = B.slp_rank
INNER JOIN (
	SELECT 
		SORTKEY,
		COUNT(UNIQUE lyl_id_no) AS DECILE_TOTAL,
		slp_rank
	FROM shc_work_tbls.d5_xy_decile
	GROUP BY SORTKEY, slp_rank
	) C
	ON A.sortkey = C.sortkey
	AND	A.slp_rank = C.slp_rank
) WITH DATA PRIMARY INDEX(SORTKEY);
COLLECT STATS shc_work_tbls.d5_xy_VALIDATION INDEX(SORTKEY);



.EXPORT RESET
.EXPORT FILE validation.txt

SELECT * FROM shc_work_tbls.d5_xy_VALIDATION
ORDER BY SORTKEY, slp_rank;

.LOGOFF;

EOF
RC=$?
echo bteq RC: $RC
if [ $RC -gt 10 ];
then
	echo $0 failed.  Return Code: $RC
	exit 1
else
	sed '2d' validation.txt > tmp.txt; mv tmp.txt validation.txt

fi
#

######################################################################################################
##Step 11 - R script that construct the validation plot
######################################################################################################

R CMD BATCH --no-save --no-restore validation.r

tEnd=`date`
echo $0 started at $tStart.
echo $0 ended at $tEnd.


#
# The End
