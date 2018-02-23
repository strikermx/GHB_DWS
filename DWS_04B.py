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
table = 'res_dws_04b_result'

if __name__ == '__main__':
    #print len(sys.argv)
    if (len(sys.argv) <= 1):
        sys.exit("ERROR:Not found JOB_ID \nEx " + sys.argv[0] + " <JOB_ID>")

    job_id = sys.argv[1]
    process_id = os.getpid()
    
    print "Start Script ...DWS_04B "
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
    print("Start Checking Wrong WTAX PCNT")
    PCNT = DataValidation(pd.DataFrame(), "SOL_ID,ACC.CUST_TYPE_CODE,FORACID,CIF_ID,GAM.ACCT_NAME,GAM.SCHM_CODE,GSP.INTEREST_CODE,WTAX_PCNT,GSP.WITHOLDING_TAX","tbaadm","GAM")
    PCNT.checkPCNTFlag()
    PCNT.df.rename(columns={'WTAX_PCNT': 'WRONG_PCNT', 'GSP.WITHOLDING_TAX': 'CORRECT_PCNT', 'ACC.CUST_TYPE_CODE': 'CUST_TYPE_CODE','GAM.ACCT_NAME':'ACCT_NAME','GAM.SCHM_CODE':'SCHM_CODE','GSP.INTEREST_CODE':'INTEREST_CODE'}, inplace=True)
    #50%
    SOL = DataValidation(pd.DataFrame(), "SOL_DESC, SOL_ID","tbaadm","CBT")
    SOL.df = SOL.query2("SELECT " + SOL.colStatement + " FROM tbaadm.SOL", "2")
    PCNT.df = pd.merge(PCNT.df,SOL.df,on = ['SOL_ID'],how = 'left')
    #75%
    #tns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=' + DBHOST + ')(PORT=' + DBPORT + '))(CONNECT_DATA= (SID='+ DBSID +')))'
    #con = cx_Oracle.connect(DBUSER,DBPASS,tns)
    #cursor = con.cursor()

    #sql = '''INSERT INTO res_dws_04b_result VALUES(:SOL_DESC, :SOL_ID, :FORACID, :CIF_ID, :ACCT_NAME, :SCHM_CODE, :INTEREST_CODE, :CUST_TYPE_CODE, :CORRECT_PCNT, :WRONG_PCNT, '{0}', TO_DATE('{1}', '{2}'))'''.format(job_id, job_start_date_time, g_date_format)
    #cursor.executemany(sql, PCNT.df.values.tolist())
    #con.commit()
    #cursor.close()
    PCNT.df = PCNT.df.reset_index(drop=True)
    for i in range(PCNT.df.shape[0]):
    #for i in range(0, 100):    
        sql_2 = '''
        INSERT INTO res_dws_04b_result 
		values('{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{0}', TO_DATE('{1}','{2}'))
        
        '''.format(job_id, job_start_date_time, g_date_format,PCNT.df.SOL_DESC[i],PCNT.df.SOL_ID[i],PCNT.df.FORACID[i],PCNT.df.CIF_ID[i],PCNT.df.ACCT_NAME[i],PCNT.df.SCHM_CODE[i],PCNT.df.INTEREST_CODE[i],PCNT.df.CUST_TYPE_CODE[i],PCNT.df.CORRECT_PCNT[i],PCNT.df.WRONG_PCNT[i])
        db1.update_rows(sql_2)
        if i%20 == 0:
            print "Done: "+ str(i)
    end = timer()
    PCNT.elaspedTime = end - start
    print( "checkWTAXPCNT Completed, Time Elapsed : " + str(PCNT.elaspedTime) + " seconds")
    # STAMP FINISH JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='S' \
             , PROGRESS='100' \
             , JOB_END_DATE_TIME=SYSDATE \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    cur = db1.update_rows(sql)
    