# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 22:33:39 2017

@author: ghb
"""

import sys
import os
import pandas as pd
from Query_obj import Query_obj
from checkFlag import DataValidation
from timeit import default_timer as timer
import dws_config
import cx_Oracle
import datetime
import cif_config
from dateutil.relativedelta import relativedelta
from timeit import default_timer as timer
DBHOST = dws_config.DBHOST
DBPORT = dws_config.DBPORT
DBSID = dws_config.DBSID
DBUSER = dws_config.DBUSER
DBPASS = dws_config.DBPASS
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
g_date_format = "YYYY-MM-DDHH24MISS"
insert_date_format = "DD/MM/YYYY"
table = 'res_dws_02_result'
def to_tyr(row):
    row["MATURITY_DATE"] = row["MATURITY_DATE"] + relativedelta(years=+543)   
    #row["MATURITY_DATE"] = row["MATURITY_DATE"].strftime('%Y-%m-%d%H%M%S')
    return row

if __name__ == '__main__':
    #print len(sys.argv)
    if (len(sys.argv) <= 1):
        sys.exit("ERROR:Not found JOB_ID \nEx " + sys.argv[0] + " <JOB_ID>")

    job_id = sys.argv[1]
    process_id = os.getpid()
    
    print "Start Script ...DWS_02 "
    print "Process ID = " + str(process_id)
    
    print 'Create query obj'
    db1 = Query_obj(cif_config.username,cif_config.password,cif_config.txtcnt)
    
    
    # Get SYSDATE for START_DATE
    print 'start get sysdate'
    sql = "SELECT TO_CHAR(SYSDATE,'" + g_date_format + "') FROM DUAL"
    
    try:
        job_start_date_time = db1.get_scalar(sql)
    except:
        print "Unexpected error:", sys.exc_info()[0]
    
    
    # STAMP START JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='P' \
             , PROGRESS='0' \
             , JOB_START_DATE_TIME=TO_DATE('" + job_start_date_time + "','" + g_date_format + "') \
             , PROCESS_ID='" + str(process_id) + "'  \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    db1.update_rows(sql)

    start = timer()
    print("Start Checking Clearing")
    clearing = DataValidation(pd.DataFrame(), "SOL_ID,CLG_ZONE_CODE,CLG_ZONE_DATE,TOT_CR_PART_TRAN_AMT,ZONE_STAT","tbaadm","OZH")
    clearing.checkClearing()
    centralTime = DataValidation(pd.DataFrame(), "DB_STAT_DATE","tbaadm","GCT")
    centralTime.df = centralTime.query2("SELECT " + centralTime.colStatement + " FROM tbaadm.gct", "2")
    CheckingDate = centralTime.df.DB_STAT_DATE[0] + relativedelta(days=-2)
    clearing.df = clearing.df.loc[(clearing.df.CLG_ZONE_DATE < CheckingDate)]
    #50%
    clearing.df = clearing.df.reset_index(drop=True)
    for i in range(clearing.df.shape[0]):
    #for i in range(0, 100):    
        sql_2 = '''
        INSERT INTO RES_DWS_02_RESULT
        select sol_desc, '{3}', '{4}', TO_DATE('{5}','{2}'), '{6}', '{7}', '{0}', TO_DATE('{1}','{2}')
        from
        (select sol_desc, sol_id from tbaadm.sol 
        where sol_id = '{3}')
        '''.format(job_id,job_start_date_time, g_date_format,clearing.df.SOL_ID[i],clearing.df.CLG_ZONE_CODE[i],clearing.df.CLG_ZONE_DATE[i].strftime('%Y-%m-%d%H%M%S'),clearing.df.TOT_CR_PART_TRAN_AMT[i],clearing.df.ZONE_STAT[i])
        db1.update_rows(sql_2)
        if i%20 == 0:
            print "Done: "+ str(i)
    end = timer()
    clearing.elaspedTime = end - start
    print( "checkClearing Completed, Time Elapsed : " + str(clearing.elaspedTime) + " seconds")
    # STAMP FINISH JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='S' \
             , PROGRESS='100' \
             , JOB_END_DATE_TIME=SYSDATE \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    cur = db1.update_rows(sql)
    