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
import cif_config
import cx_Oracle
import datetime
DBHOST = dws_config.DBHOST
DBPORT = dws_config.DBPORT
DBSID = dws_config.DBSID
DBUSER = dws_config.DBUSER
DBPASS = dws_config.DBPASS


os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
g_date_format = "YYYY-MM-DDHH24MISS"
insert_date_format = "DD/MM/YYYY"
table = 'res_dws_01a_result'

if __name__ == '__main__':
    #print len(sys.argv)
    if (len(sys.argv) <= 1):
        sys.exit("ERROR:Not found JOB_ID \nEx " + sys.argv[0] + " <JOB_ID>")

    job_id = sys.argv[1]
    process_id = os.getpid()
    
    print "Start Script ...DWS_01A "
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
    print("Start Checking Wrong Pegged FLAG")
    pegged = DataValidation(pd.DataFrame(), "SOL_ID,FORACID,ACCT_NAME,SCHM_CODE,CUST_TYPE_CODE,INT_TBL_CODE,PEGGED_FLG,START_DATE,CIF_ID","tbaadm","ITC")
    pegged.checkPeggedFlag()
    #50%
    #SOL = DataValidation(pd.DataFrame(), "SOL_DESC, SOL_ID","tbaadm","CBT")
    #SOL.query("SELECT " + SOL.colStatement + " FROM tbaadm.SOL", "2")
    #pegged.df = pd.merge(pegged.df,SOL.df,on = ['SOL_ID'],how = 'left') 
    pegged.df.rename(columns={'PEGGED_FLG': 'PEGGED_FLAG', 'INT_TBL_CODE': 'INTEREST_CODE'}, inplace=True)
    pegged.df['JOB_KEY'] = job_id
    pegged.df['START_DATE'] = pd.to_datetime(pegged.df['START_DATE'])
    #75%
    
    #tns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=' + DBHOST + ')(PORT=' + DBPORT + '))(CONNECT_DATA= (SID='+ DBSID +')))'
    #con = cx_Oracle.connect(DBUSER,DBPASS,tns)
    #cursor = con.cursor()
    #sql = "INSERT INTO "+ table +" \
    #values(:SOL_DESC, :SOL_ID, :FORACID, :ACCT_NAME, :SCHM_CODE, :CUST_TYPE_CODE, :INTEREST_CODE, :PEGGED_FLAG, :START_DATE, :JOB_KEY, TO_DATE('{0}','{1}'), :CIF_ID".format(job_start_date_time, g_date_format)
    #cursor.executemany(sql, pegged.df.values.tolist())
    #con.commit()
    #cursor.close()
    pegged.df = pegged.df.reset_index(drop=True)
    for i in range(pegged.df.shape[0]):
    #for i in range(0, 100):    
        sql_2 = '''
        INSERT INTO RES_DWS_01A_RESULT
        select sol_desc, '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', TO_DATE('{9}','{1}'), '{10}', TO_DATE('{0}','{1}'), '{11}'
        from
        (select sol_desc, sol_id from tbaadm.sol 
        where sol_id = '{2}')
        '''.format(job_start_date_time, g_date_format,pegged.df.SOL_ID[i],pegged.df.FORACID[i],pegged.df.ACCT_NAME[i],pegged.df.SCHM_CODE[i],pegged.df.CUST_TYPE_CODE[i],pegged.df.INTEREST_CODE[i],pegged.df.PEGGED_FLAG[i],pegged.df.START_DATE[i].strftime('%Y-%m-%d%H%M%S'),pegged.df.JOB_KEY[i],pegged.df.CIF_ID[i])
        db1.update_rows(sql_2)
        if i%20 == 0:
            print "Done: "+ str(i)    
    end = timer()
    pegged.elaspedTime = end - start
    print( "checkPeggedFlag Completed, Time Elapsed : " + str(pegged.elaspedTime) + " seconds")
    # STAMP FINISH JOB
    sql = "UPDATE JOB_TRN_BACKEND \
           SET STATUS='S' \
             , PROGRESS='100' \
             , JOB_END_DATE_TIME=SYSDATE \
           WHERE JOB_KEY = '" + str(job_id) + "' "
    #print sql
    cur = db1.update_rows(sql)
    