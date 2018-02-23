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
table = 'res_dws_05d_result'
def convT(t):
    return t.strftime('%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    #print len(sys.argv)
    if (len(sys.argv) <= 1):
        sys.exit("ERROR:Not found JOB_ID \nEx " + sys.argv[0] + " <JOB_ID>")

    job_id = sys.argv[1]
    #job_id = 44
    process_id = os.getpid()
    
    print "Start Script ...DWS_05D "
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
    print("Start Checking ITCintCode")
    transITC = DataValidation(pd.DataFrame(), "SOL_ID, FORACID, CIF_ID, ACCT_NAME, SCHM_CODE, SCHM_TYPE, INT_TBL_CODE, INT_TBL_CODE_SRL_NUM", "tbaadm", "GAM")
    transITC.checkINTtransaction()
    transITC.df.drop(['key', 'Wrong_INT'], axis=1, inplace=True)
    #50%
    SOL = DataValidation(pd.DataFrame(), "SOL_DESC, SOL_ID","tbaadm","CBT")
    SOL.df = SOL.query2("SELECT " + SOL.colStatement + " FROM tbaadm.SOL", "2")
    transITC.df = pd.merge(transITC.df,SOL.df,on = ['SOL_ID'],how = 'left')
    #75%
    #tns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=' + DBHOST + ')(PORT=' + DBPORT + '))(CONNECT_DATA= (SID='+ DBSID +')))'
    #con = cx_Oracle.connect(DBUSER,DBPASS,tns)
    #cursor = con.cursor()     
    #sql = '''INSERT INTO GHB_CLEANSING.RES_DWS_05D_RESULT VALUES(:SOL_DESC, :SOL_ID, :FORACID, :CIF_ID, :ACCT_NAME, :SCHM_CODE, :SCHM_TYPE, :INT_TBL_CODE, :INT_TBL_CODE_SRL_NUM, '{0}', TO_DATE('{1}', '{2}'))'''.format(job_id, job_start_date_time, g_date_format)
    #cursor.executemany(sql, transITC.df.values.tolist())
    #con.commit()
    #cursor.close()
    transITC.df = transITC.df.reset_index(drop=True)
    for i in range(transITC.df.shape[0]):
    #for i in range(0, 100):    
        sql_2 = '''
        INSERT INTO res_dws_05d_result 
		values('{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{0}', TO_DATE('{1}','{2}'))
        
        '''.format(job_id, job_start_date_time, g_date_format,transITC.df.SOL_DESC[i],transITC.df.SOL_ID[i],transITC.df.FORACID[i],transITC.df.CIF_ID[i],transITC.df.ACCT_NAME[i],transITC.df.SCHM_CODE[i],transITC.df.SCHM_TYPE[i],transITC.df.INT_TBL_CODE[i],transITC.df.INT_TBL_CODE_SRL_NUM[i])
        db1.update_rows(sql_2)
        if i%20 == 0:
            print "Done: "+ str(i)
	end = timer()
    transITC.elaspedTime = end - start
    print( "Checking ITCintCode Completed, Time Elapsed : " + str(transITC.elaspedTime) + " seconds")
    # STAMP FINISH JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='S' \
             , PROGRESS='100' \
             , JOB_END_DATE_TIME=SYSDATE \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    cur = db1.update_rows(sql)
    