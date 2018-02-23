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
DBHOST = dws_config.DBHOST
DBPORT = dws_config.DBPORT
DBSID = dws_config.DBSID
DBUSER = dws_config.DBUSER
DBPASS = dws_config.DBPASS
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
g_date_format = "YYYY-MM-DDHH24MISS"
insert_date_format = "DD/MM/YYYY"
table = 'res_dws_05b_result'
def convT(t):
    return t.strftime('%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    #print len(sys.argv)
    if (len(sys.argv) <= 1):
        sys.exit("ERROR:Not found JOB_ID \nEx " + sys.argv[0] + " <JOB_ID>")

    job_id = sys.argv[1]
    #job_id = 44
    process_id = os.getpid()
    
    print "Start Script ...DWS_05B "
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
    print("Start Checking Mode Of Opper")
    opper = DataValidation(pd.DataFrame(), "GAM.SOL_ID,FORACID,CIF_ID,ACCT_NAME,SCHM_TYPE,MODE_OF_OPER_CODE","tbaadm","ITC")
    opper.checkOper()
    opper.df.rename(columns={'GAM.SOL_ID': 'SOL_ID'}, inplace=True)  
    #50%
    SOL = DataValidation(pd.DataFrame(), "SOL_DESC, SOL_ID","tbaadm","CBT")
    SOL.df = SOL.query2("SELECT " + SOL.colStatement + " FROM tbaadm.SOL", "2")
    opper.df = pd.merge(opper.df,SOL.df,on = ['SOL_ID'],how = 'left')
    #75%
    tns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=' + DBHOST + ')(PORT=' + DBPORT + '))(CONNECT_DATA= (SID='+ DBSID +')))'
    con = cx_Oracle.connect(DBUSER,DBPASS,tns)
    cursor = con.cursor()     
    sql = '''INSERT INTO GHB_CLEANSING.RES_DWS_05B_RESULT VALUES(:SOL_DESC, :SOL_ID, :FORACID, :CIF_ID, :ACCT_NAME, :SCHM_TYPE, :MODE_OF_OPER_CODE, '{0}', TO_DATE('{1}', '{2}'))'''.format(job_id, job_start_date_time, g_date_format)
    cursor.executemany(sql, opper.df.values.tolist())
    con.commit()
    cursor.close()
    end = timer()
    opper.elaspedTime = end - start
    print( "Checking Mode Of Opper Completed, Time Elapsed : " + str(opper.elaspedTime) + " seconds")
    # STAMP FINISH JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='S' \
             , PROGRESS='100' \
             , JOB_END_DATE_TIME=SYSDATE \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    cur = db1.update_rows(sql)
    