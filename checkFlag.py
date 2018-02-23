# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 22:27:01 2017

@author: ghb
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Feb 08 12:44:08 2017

@author: Thanasit Yiamwinya
Originally Create to validate Duplication and Null field

Modified for flag checking task 
"""
#### GLOBAL VARIABLE

##Cleaning DB
'''
DBHOST = "172.30.9.140"
DBPORT = "1521"
DBSID = "clnsdb"
DBUSER = "GHB_CLEANSING"
DBPASS = "dc2016"
'''



##FIX
MAXDFSIZE = 1000000
####

import cx_Oracle
import pandas as pd
import re
import dws_config
import numpy as np
import xlsxwriter
import datetime
import time
import math
import os
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
from dateutil.relativedelta import relativedelta
from timeit import default_timer as timer
outputPath = "C:/Users/DQ1/Desktop/OutputDWS"
DBHOST = dws_config.DBHOST
DBPORT = dws_config.DBPORT
DBSID = dws_config.DBSID
DBUSER = dws_config.DBUSER
DBPASS = dws_config.DBPASS
def transform_foracid(row):
    row["GAM.FORACID"] = str(row["GAM.FORACID"])[:-4]    
    return row
def transform_iforacid(row):
    row = str(row)[:-4]    
    return row
class DataValidation:
    
    def __init__(self, df, colStatement, schemaName, tableName):
        self.df = df        
        self.colStatement = colStatement
        self.tableName = tableName
        self.schemaName = schemaName
        self.count = 0
        self.columnsList = []
        self.duplicateT1 = pd.DataFrame() ## Type I mean Duplication for all information. AKA Double record
        self.duplicateT2 = pd.DataFrame() ## Type II mean Duplication on only specific unique key. AKA unique key Fault
        self.missing = pd.DataFrame()
        self.NaCount = 0
        self.duplicate = pd.DataFrame()
        self.dup = pd.DataFrame()
        self.dupCount = 0
        self.missFlag = pd.DataFrame()
        self.wrongCount = 0
        self.elaspedTime = 0
    ###########################################################################
    ## query(str): "WHERE rownum between 1 and 10"
    ## usage:
    ## is use for cutting data into smaller piece of frame for validation case
    ## so python could be able to deal with large amount of data
    ## the query will select according to the given range
    ####################################################################### 
    def query(self, query, mode):
        tns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=' + DBHOST + ')(PORT=' + DBPORT + '))(CONNECT_DATA= (SID='+ DBSID +')))'
        con = cx_Oracle.connect(DBUSER,DBPASS,tns)
        cursor = con.cursor()
        sql = ""        
        if mode == "1":
            sql = 'SELECT ' + self.colStatement +' from ' + self.schemaName + '.' + self.tableName
            sql = sql + query
        #Free execution but the selected column must be equal to the given object for instance
        elif mode =="2":
            sql = sql + query
            print sql
        queryRet = cursor.execute(sql)
        self.columnsList = self.colStatement.replace(" ","").split(',')
        self.df = pd.DataFrame(queryRet.fetchall(), columns=self.columnsList)
        cursor.close()
    def query2(self, query, mode):
        tns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=' + DBHOST + ')(PORT=' + DBPORT + '))(CONNECT_DATA= (SID='+ DBSID +')))'
        con = cx_Oracle.connect(DBUSER,DBPASS,tns)
        cursor = con.cursor()
        sql = ""        
        if mode == "1":
            sql = 'SELECT ' + self.colStatement +' from ' + self.schemaName + '.' + self.tableName
            sql = sql + query
        #Free execution but the selected column must be equal to the given object for instance
        elif mode =="2":
            sql = sql + query
            print sql
        queryRet = cursor.execute(sql)
        self.columnsList = self.colStatement.replace(" ","").split(',')
        df = pd.DataFrame(queryRet.fetchall(), columns=self.columnsList)        
        cursor.close()
        return df
    
    def getCount(self,arg):
        tns = '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=' + DBHOST + ')(PORT=' + DBPORT + '))(CONNECT_DATA= (SID='+ DBSID +')))'
        con = cx_Oracle.connect(DBUSER,DBPASS,tns)
        cursor = con.cursor()
        if arg:
            sql = arg
        else:
            sql = 'SELECT ' + 'COUNT(*)' +' from ' + self.schemaName + '.' + self.tableName
        queryRet = cursor.execute(sql)
        self.count = int(re.search(r"\d+",str(queryRet.fetchall())).group())
        cursor.close()

    def validateNA(self, validateCol):
        self.getCount("")
        loopMax = self.count/MAXDFSIZE
        loopMax+=1
        for x in xrange(0, loopMax):
            start = x * MAXDFSIZE
            if x == loopMax-1:
                end = self.count
            else:                
                end = (x+1) * MAXDFSIZE
            self.query( " WHERE rownum BETWEEN " + str(start) + " AND " + str(end), "1" )
            #self.missing = self.df[pd.isnull(self.df).any(axis=1)]
            missingCount = self.df[validateCol].isnull().sum()
            self.NaCount = self.NaCount + missingCount
            nullCol = pd.isnull(self.df[validateCol])
            result = pd.concat([self.df,nullCol],axis=1)
            newColList = self.columnsList
            newColList.append("VALID")
            result.columns = newColList
            result = result.loc[result['VALID']== True]
            result = result.drop('VALID',1)
            frames = [self.missing, result]
            self.missing = pd.concat(frames)
            print "Complete Round : " + str(x)
            print "Missing per Round : " + str(missingCount)
        print "All missing : " + str(self.NaCount)
        
    def validateDup(self, validateCol):
        self.getCount("")
        loopMax = self.count/MAXDFSIZE
        loopMax+=1
        for x in xrange(0, loopMax):
            start = x * MAXDFSIZE
            if x == loopMax-1:
                end = self.count
            else:                
                end = (x+1) * MAXDFSIZE
            print("Start :" + str(x))
            self.query( "SELECT * from (SELECT "+ self.colStatement + " FROM "+ self.schemaName + '.' + self.tableName+" ORDER BY " +  validateCol + " ASC) where rownum BETWEEN " + str(start) +" and "+str(end) , "2" )
            
    def checkBookDupSBA(self):
        
        self.getCount("select count(*) from tbaadm.general_acct_mast_table GAM INNER JOIN tbaadm.CBT BOOK on GAM.ACID = BOOK.ACID where schm_type =\'SBA\' and ACCT_CLS_FLG = \'N\' and CHQ_LVS_STAT = \'U\'")
        print("Finish Counting: "+ str(self.count))        
        loopMax = self.count/MAXDFSIZE
        loopMax+=1
        for x in xrange(0, loopMax):
            start = x * MAXDFSIZE
            if x == loopMax-1:
                end = self.count
            else:                
                end = (x+1) * MAXDFSIZE
            print("Start :" + str(x))
            self.query( "SELECT * FROM (SELECT " + self.colStatement + " FROM tbaadm.general_acct_mast_table GAM INNER JOIN tbaadm.CBT BOOK on GAM.ACID = BOOK.ACID WHERE schm_type =\'SBA\' AND ACCT_CLS_FLG = \'N\' AND CHQ_LVS_STAT = \'U\' ORDER BY BEGIN_CHQ_NUM ASC) WHERE rownum BETWEEN " + str(start) +" AND "+str(end) , "2" )
            validateCol = self.df["BEGIN_CHQ_NUM"]
            self.dup = self.df[validateCol.isin(validateCol[validateCol.duplicated()])]
            count = self.dup.shape[0]
            self.dupCount = self.dupCount + count
            print(count)
            frames = [self.duplicate, self.dup]
            self.duplicate = pd.concat(frames)
        self.duplicate = self.duplicate.sort_values(['SOL_ID','BEGIN_CHQ_NUM'], ascending=[True, True])
    def checkBookDupTDA(self):
        self.getCount("SELECT COUNT(*) FROM C_TDREF TDREF INNER JOIN C_TDM BOOK on TDREF.DEP_REF_NUM = BOOK.DEP_REF_NUM INNER JOIN tbaadm.GAM GAM on TDREF.FORACID = GAM.FORACID WHERE schm_type =\'TDA\' AND ACCT_CLS_FLG = \'N\'")
        print("Finish Counting: "+ str(self.count))        
        loopMax = self.count/MAXDFSIZE
        loopMax+=1
        for x in xrange(0, loopMax):
            start = x * MAXDFSIZE
            if x == loopMax-1:
                end = self.count
            else:                
                end = (x+1) * MAXDFSIZE
            print("Start :" + str(x))
            self.query( "SELECT * FROM (SELECT " + self.colStatement + " FROM C_TDREF TDREF INNER JOIN C_TDM BOOK on TDREF.DEP_REF_NUM = BOOK.DEP_REF_NUM INNER JOIN tbaadm.GAM GAM on TDREF.FORACID = GAM.FORACID WHERE schm_type =\'TDA\' AND ACCT_CLS_FLG = \'N\' ORDER BY BOOK.PASSBOOK_NO ASC) WHERE rownum BETWEEN " + str(start) +" AND "+str(end) , "2" )
            self.df = self.df.apply(transform_foracid, axis=1)
            self.df = self.df.drop_duplicates(subset=['GAM.FORACID'], keep='last')
            validateCol = self.df["BOOK.PASSBOOK_NO"]
            self.dup = self.df[validateCol.isin(validateCol[validateCol.duplicated()])]
            count = self.dup.shape[0]
            self.dupCount = self.dupCount + count
            print(count)
            frames = [self.duplicate, self.dup]
            self.duplicate = pd.concat(frames)
            self.duplicate = self.duplicate.sort_values('BOOK.PASSBOOK_NO')
        self.duplicate = self.duplicate.sort_values(['GAM.SOL_ID','BOOK.PASSBOOK_NO'], ascending=[True, True])
        
    def checkWTAXFlag(self):
        self.getCount("select count(*) from tbaadm.GAM where ACCT_CLS_FLG =\'N\' AND schm_type in (\'SBA\',\'CAA\',\'TDA\') AND CLR_BAL_AMT > 0 AND schm_code != \'SB099\'")
        print("Finish Counting"+ str(self.count))        
        loopMax = self.count/MAXDFSIZE
        loopMax+=1
        for x in xrange(0, loopMax):
            start = x * MAXDFSIZE
            if x == loopMax-1:
                end = self.count
            else:                
                end = (x+1) * MAXDFSIZE
            print("Start :" + str(x))
            self.query( "SELECT * FROM (SELECT " + self.colStatement + " from tbaadm.GAM GAM  where ACCT_CLS_FLG =\'N\' AND schm_type in (\'SBA\',\'CAA\',\'TDA\') AND CLR_BAL_AMT >= 0 AND schm_code != \'SB099\') WHERE rownum BETWEEN " + str(start) +" AND "+str(end) , "2" )
            wrong = self.df[~self.df.WTAX_FLG.str.contains("W")]
            wrong = wrong[~wrong.WTAX_FLG.str.contains("N")]
            count = wrong.shape[0]
            self.wrongCount = self.wrongCount + count
            print(count)
            frames = [self.missFlag, wrong]            
            self.missFlag = pd.concat(frames)
    def checkPCNTFlag(self):
        start = timer()
        print("Start Checking Wrong PCNT FLAG")
        self.query( "SELECT " + self.colStatement + " from tbaadm.GAM GAM INNER JOIN CRMUSER.ACCOUNTS ACC on CUST_ID = CORE_CUST_ID INNER JOIN C_GSP GSP on ACC.CUST_TYPE_CODE = GSP.CUST_TYPE_CODE AND GAM.SCHM_CODE = GSP.SCHM_CODE AND GAM.SCHM_TYPE = GSP.SCHM_TYPE where GAM.schm_type in (\'SBA\',\'CAA\',\'TDA\') AND ACCT_CLS_FLG =\'N\' AND GAM.SCHM_CODE != \'SB009\' AND GAM.SCHM_CODE != \'SB099\' AND GAM.SCHM_CODE != \'FD009\' AND CLR_BAL_AMT >= 0 AND GSP.witholding_tax != WTAX_PCNT", "2" )
        end = timer()
        self.elaspedTime = end - start
        print( "checkPCNTFlag Completed, Time Elapsed : " + str(self.elaspedTime) + " seconds")
    def checkClearing(self):    
        self.query( "SELECT " + self.colStatement + " from tbaadm.OZH where TOT_CR_PART_TRAN_AMT > 0 AND Zone_stat = \'S\'", "2" )
    def checkRenewed(self):    
        self.query( "SELECT " + self.colStatement + " from tbaadm.GAM GAM INNER JOIN TBAADM.TAM TAM on GAM.ACID = TAM.ACID WHERE ACCT_CLS_FLG =\'N\' AND CLR_BAL_AMT > 0 AND schm_code != \'FD009\' ", "2" )
    def checkPeggedFlag(self):
        
        self.getCount( "SELECT count(*) from tbaadm.ITC ITC INNER JOIN tbaadm.GAM on ITC.ENTITY_ID = GAM.ACID INNER JOIN CRMUSER.ACCOUNTS ACC on GAM.CUST_ID = ACC.CORE_CUST_ID WHERE schm_code !=\'FD002\' AND schm_type=\'TDA\' AND ACCT_CLS_FLG =\'N\' ")
        print("Finish Counting")        
        loopMax = self.count/MAXDFSIZE
        loopMax+=1
        combine_df = pd.DataFrame()
        for x in xrange(0, loopMax):
            start = x * MAXDFSIZE
            if x == loopMax-1:
                end = self.count
            else:                
                end = (x+1) * MAXDFSIZE
            print("Start :" + str(x))
            tmp_df = self.query2( "SELECT * FROM (SELECT " + self.colStatement + " from tbaadm.ITC ITC INNER JOIN tbaadm.GAM on ITC.ENTITY_ID = GAM.ACID INNER JOIN CRMUSER.ACCOUNTS ACC on GAM.CUST_ID = ACC.CORE_CUST_ID WHERE schm_code !=\'FD002\' AND schm_type=\'TDA\' AND ACCT_CLS_FLG =\'N\' ORDER BY FORACID ASC) WHERE rownum BETWEEN " + str(start) +" AND "+str(end) , "2")
            lastest = tmp_df.sort_values('START_DATE').drop_duplicates(['FORACID'], keep='last')            
            frames = [combine_df, lastest]
            combine_df = pd.concat(frames)
        self.df = combine_df
        self.df = self.df.sort_values('START_DATE').drop_duplicates(['FORACID'], keep='last')
        self.df = self.df[~self.df.PEGGED_FLG.str.contains("Y")]
        
    def checkINTtransaction(self):
        start = timer()
        print("Start Checking INT_TBL_CODE in ITC")
        self.query("SELECT " + self.colStatement + " from tbaadm.GAM GAM INNER JOIN tbaadm.ITC ITC on GAM.ACID = ITC.ENTITY_ID WHERE ACCT_CLS_FLG =\'N\' AND CLR_BAL_AMT >=0 AND schm_type =\'TDA\' ", "2")
        self.df = self.df.sort_values('INT_TBL_CODE_SRL_NUM').drop_duplicates(['FORACID'], keep='last')
        self.df['key'] = self.df['FORACID'].map(transform_iforacid)
        self.df['Wrong_INT'] = np.where(self.df.groupby('key').INT_TBL_CODE.transform('nunique') > 1, 'T', '')
        self.df = self.df[self.df.Wrong_INT == 'T']
        self.df = self.df.sort_values('FORACID')
        end = timer()
        self.elaspedTime = end - start
        print( "checkINTtransaction Completed, Time Elapsed : " + str(self.elaspedTime) + " seconds")
    def checkOper(self):
        start = timer()
        print("Start Checking Mode of Oper")
        self.query( "SELECT " + self.colStatement + " from tbaadm.GAM WHERE ACCT_CLS_FLG =\'N\' AND CLR_BAL_AMT >=0 AND schm_type =\'TDA\'", "2" )
        self.df['Wrong_flag'] = np.where(self.df.groupby('FORACID').MODE_OF_OPER_CODE.transform('nunique') > 1, 'T', '')
        self.df = self.df[self.df.Wrong_flag == 'T']
        end = timer()
        self.elaspedTime = end - start
        print( "checkOper Completed, Time Elapsed : " + str(self.elaspedTime) + " seconds")
        #self.getCount( "SELECT count(*) from tbaadm.GAM GAM INNER JOIN CRMUSER.ACCOUNTS ACC ON CUST_ID = CORE_CUST_ID INNER JOIN C_GSP GSP ON ACC.CUST_TYPE_CODE = GSP.CUST_TYPE_CODE INNER JOIN TBAADM.TAM TAM ON TAM.ACID = GAM.ACID WHERE ACCT_CLS_FLG =\'N\' AND CLR_BAL_AMT >=0 AND schm_type =\'TDA\'" )
        #print("Finish Counting")        
        #loopMax = self.count/MAXDFSIZE
        #loopMax+=1
        #combine_df = pd.DataFrame()
        #for x in xrange(0, loopMax):
            #start = x * MAXDFSIZE
            #if x == loopMax-1:
                #end = self.count
            #else:                
                #end = (x+1) * MAXDFSIZE
            #print("Start :" + str(x))
            #tmp_df = self.query2( "SELECT * FROM (SELECT " + self.colStatement + " from tbaadm.GAM GAM INNER JOIN CRMUSER.ACCOUNTS ACC ON CUST_ID = CORE_CUST_ID INNER JOIN C_GSP GSP ON ACC.CUST_TYPE_CODE = GSP.CUST_TYPE_CODE INNER JOIN TBAADM.TAM TAM ON TAM.ACID = GAM.ACID WHERE ACCT_CLS_FLG =\'N\' AND CLR_BAL_AMT >=0 AND schm_type =\'TDA\' ORDER BY FORACID ASC) WHERE rownum BETWEEN " + str(start) +" AND "+str(end) , "2")
            #g=tmp_df.groupby('FORACID')['MODE_OF_OPER_CODE'].value_counts()
            #check_key = g[g==1].dropna()
            #check_keydf = check_key.reset_index()
            #wrong = pd.merge(check_keydf,tmp_df,on = ['FORACID'],how = 'left')
           # wrong = tmp_df            
            #wrong['Wrong_flag'] = np.where(tmp_df.groupby('FORACID').MODE_OF_OPER_CODE.transform('nunique') > 1, 'T', '')

            #frames = [combine_df, wrong[wrong.Wrong_flag == 'T']]
            #combine_df = pd.concat(frames)
            #self.export_csv(wrong[wrong.Wrong_flag == 'T'],"C:/Users/DQ1/Desktop/opper" + str(x) + ".csv")
        #self.df = combine_df
    def checkProductExpire(self):
        self.df = self.query2("SELECT " + self.colStatement + " FROM tbaadm.TAM TAM INNER JOIN tbaadm.GAM GAM on TAM.ACID = GAM.ACID WHERE ACCT_CLS_FLG =\'N\' AND CLR_BAL_AMT >=0 AND schm_type =\'TDA\'","2")
    def checkInterestCode(self):
        self.df = self.query2( "SELECT " + self.colStatement + " FROM tbaadm.ITC ITC INNER JOIN tbaadm.GAM on ITC.ENTITY_ID = GAM.ACID INNER JOIN CRMUSER.ACCOUNTS ACCT ON CUST_ID = CORE_CUST_ID WHERE NOT (schm_code =\'FD002\' AND cust_type_code = 60) AND ACCT_CLS_FLG =\'N\' AND CLR_BAL_AMT >=0 AND (schm_type=\'TDA\' OR schm_type=\'SBA\' OR schm_type=\'CAA\') AND (INT_TBL_CODE!=\'AEG\' AND INT_TBL_CODE!=\'AOT\' AND INT_TBL_CODE!=\'BIG\'  AND INT_TBL_CODE!=\'BMA\'  AND INT_TBL_CODE!=\'BP1\'  AND INT_TBL_CODE!=\'BP2\'  AND INT_TBL_CODE!=\'BP3\'  AND INT_TBL_CODE!=\'BP4\'  AND INT_TBL_CODE!=\'DIP\'  AND INT_TBL_CODE!=\'FMKU2\'  AND INT_TBL_CODE!=\'FMO\'  AND INT_TBL_CODE!=\'IPT2\'  AND INT_TBL_CODE!=\'KKU\'  AND INT_TBL_CODE!=\'KSP\'  AND INT_TBL_CODE!=\'MFC\' AND INT_TBL_CODE!=\'NOINT\'  AND INT_TBL_CODE!=\'OD2\'  AND INT_TBL_CODE!=\'PTT1\'  AND INT_TBL_CODE!=\'PTT2\'  AND INT_TBL_CODE!=\'PWO\'  AND INT_TBL_CODE!=\'SAGI\'  AND INT_TBL_CODE!=\'SEAU\'  AND INT_TBL_CODE!=\'SEC1\'  AND INT_TBL_CODE!=\'SEC2\'  AND INT_TBL_CODE!=\'SEC3\'  AND INT_TBL_CODE!=\'SET1\' AND INT_TBL_CODE!=\'SET2\' AND INT_TBL_CODE!=\'SSO1\' AND INT_TBL_CODE!=\'SSO2\' AND INT_TBL_CODE!=\'TG\' AND INT_TBL_CODE!=\'EXP18\' AND INT_TBL_CODE!=\'F02L3\' AND INT_TBL_CODE!=\'F02L7\' AND INT_TBL_CODE!=\'F02L8\' AND INT_TBL_CODE!=\'F02R2\' AND INT_TBL_CODE!=\'F02R3\' AND INT_TBL_CODE!=\'F02R4\' AND INT_TBL_CODE!=\'F02R7\' AND INT_TBL_CODE!=\'F02R8\' AND INT_TBL_CODE!=\'P075\' AND INT_TBL_CODE!=\'P1875\' AND INT_TBL_CODE!=\'S62G1\' AND INT_TBL_CODE!=\'SB163\' AND INT_TBL_CODE!=\'SB85P\' AND INT_TBL_CODE!=\'SBPP\' AND INT_TBL_CODE!=\'SBPRO\' AND INT_TBL_CODE!=\'SDSG1\' AND INT_TBL_CODE!=\'ZERO\')", "2")
        self.df = self.df.sort_values('INT_TBL_CODE_SRL_NUM').drop_duplicates(['ENTITY_ID'], keep='last')
        
        #self.getCount("select count(*) from tbaadm.ITC ITC INNER JOIN tbaadm.GAM on ITC.ENTITY_ID = GAM.ACID WHERE ACCT_CLS_FLG =\'N\' AND CLR_BAL_AMT >=0 AND (schm_type=\'TDA\' OR schm_type=\'SBA\' OR schm_type=\'CAA\')")
        #print("Finish Counting : " + str(self.count))        
        #loopMax = self.count/MAXDFSIZE
        #loopMax+=1
        #combine_df = pd.DataFrame()
        #for x in xrange(0, loopMax):
        #    start = x * MAXDFSIZE
        #    if x == loopMax-1:
        #        end = self.count
        #    else:                
        #        end = (x+1) * MAXDFSIZE
        #    print("Start :" + str(x))
        #    tmp_df = self.query2( "SELECT * FROM (SELECT " + self.colStatement + " FROM tbaadm.ITC ITC INNER JOIN tbaadm.GAM on ITC.ENTITY_ID = GAM.ACID WHERE ACCT_CLS_FLG =\'N\' AND CLR_BAL_AMT >=0 AND (schm_type=\'TDA\' OR schm_type=\'SBA\' OR schm_type=\'CAA\') ORDER BY ENTITY_ID ASC) WHERE rownum BETWEEN " + str(start) +" AND "+str(end) , "2")
        #    lastest = tmp_df.sort('INT_TBL_CODE_SRL_NUM').drop_duplicates(['ENTITY_ID'], keep='last')            
        #    frames = [combine_df, lastest]
        #    combine_df = pd.concat(frames)
        #self.df = combine_df
        #self.df = self.df.sort('INT_TBL_CODE_SRL_NUM').drop_duplicates(['ENTITY_ID'], keep='last')
            
    def export_csv(self, df, path):
        df.to_csv(path,index=False)
    def checkjoint(self):
        print("Start Checking Joint")
        self.query('''SELECT cif_m.SOL_ID as SOL_ID, cif_m.FORACID as FORACID, cif_m.ACCT_NAME AS ACCT_NAME, cif_m.SCHM_TYPE AS SCHM_TYPE, cif_m.SCHM_CODE AS SCHM_CODE, cif_m.cif_m as CIF_M, cif_j2.cif_j2 as CIF_J2, cif_j3.cif_j3 as CIF_J3, cif_j4.cif_j4 as CIF_J4, cif_j5.cif_j5 as CIF_J5, cif_j6.cif_j6 as CIF_J6, cif_j7.cif_j7 as CIF_J7, cif_j8.cif_j8 as CIF_J8, cif_j9.cif_j9 as CIF_J9
FROM

 (SELECT GAM.SOL_ID AS SOL_ID,GAM.FORACID as FORACID, GAM.ACCT_NAME AS ACCT_NAME, GAM.SCHM_TYPE AS SCHM_TYPE, GAM.SCHM_CODE AS SCHM_CODE, GAM.CIF_ID AS CIF_M FROM tbaadm.GAM GAM INNER JOIN tbaadm.AAS 
AAS ON GAM.ACID = AAS.ACID INNER JOIN C_TDREF TDREF ON GAM.FORACID = TDREF.FORACID where GAM.schm_type=\'TDA\' and GAM.schm_code!=\'FD009\' AND ACCT_POA_AS_REC_TYPE = \'M\' AND ACCT_STATUS != 'C' ) cif_m
LEFT JOIN
 (SELECT GAM.FORACID as FORACID, AAS.CUST_ID AS CIF_J2  FROM tbaadm.GAM GAM INNER JOIN tbaadm.AAS AAS 
ON GAM.ACID = AAS.ACID where GAM.schm_type=\'TDA\' and GAM.schm_code!=\'FD009\' AND ACCT_POA_AS_REC_TYPE = \'J\' AND ACCT_POA_AS_SRL_NUM = \'002\') cif_j2
ON
 cif_m.FORACID = cif_j2.FORACID
LEFT JOIN
 (SELECT GAM.FORACID as FORACID, AAS.CUST_ID AS CIF_J3  FROM tbaadm.GAM GAM INNER JOIN tbaadm.AAS AAS 
ON GAM.ACID = AAS.ACID where GAM.schm_type=\'TDA\' and GAM.schm_code!=\'FD009\' AND ACCT_POA_AS_REC_TYPE = \'J\' AND ACCT_POA_AS_SRL_NUM = \'003\') cif_j3
ON
 cif_m.FORACID = cif_j3.FORACID
LEFT JOIN
 (SELECT GAM.FORACID as FORACID, AAS.CUST_ID AS CIF_J4  FROM tbaadm.GAM GAM INNER JOIN tbaadm.AAS AAS 
ON GAM.ACID = AAS.ACID where GAM.schm_type=\'TDA\' and GAM.schm_code!='FD009' AND ACCT_POA_AS_REC_TYPE = \'J\' AND ACCT_POA_AS_SRL_NUM = \'004\') cif_j4
ON
 cif_m.FORACID = cif_j4.FORACID
LEFT JOIN
 (SELECT GAM.FORACID as FORACID, AAS.CUST_ID AS CIF_J5  FROM tbaadm.GAM GAM INNER JOIN tbaadm.AAS AAS 
ON GAM.ACID = AAS.ACID where GAM.schm_type=\'TDA\' and GAM.schm_code!='FD009' AND ACCT_POA_AS_REC_TYPE = \'J\' AND ACCT_POA_AS_SRL_NUM = \'005\') cif_j5
ON
 cif_m.FORACID = cif_j5.FORACID
LEFT JOIN
 (SELECT GAM.FORACID as FORACID, AAS.CUST_ID AS CIF_J6  FROM tbaadm.GAM GAM INNER JOIN tbaadm.AAS AAS 
ON GAM.ACID = AAS.ACID where GAM.schm_type=\'TDA\' and GAM.schm_code!='FD009' AND ACCT_POA_AS_REC_TYPE = \'J\' AND ACCT_POA_AS_SRL_NUM = \'006\') cif_j6
ON
 cif_m.FORACID = cif_j6.FORACID
LEFT JOIN
 (SELECT GAM.FORACID as FORACID, AAS.CUST_ID AS CIF_J7  FROM tbaadm.GAM GAM INNER JOIN tbaadm.AAS AAS 
ON GAM.ACID = AAS.ACID where GAM.schm_type=\'TDA\' and GAM.schm_code!='FD009' AND ACCT_POA_AS_REC_TYPE = \'J\' AND ACCT_POA_AS_SRL_NUM = \'004\') cif_j7
ON
 cif_m.FORACID = cif_j7.FORACID
LEFT JOIN
 (SELECT GAM.FORACID as FORACID, AAS.CUST_ID AS CIF_J8  FROM tbaadm.GAM GAM INNER JOIN tbaadm.AAS AAS 
ON GAM.ACID = AAS.ACID where GAM.schm_type=\'TDA\' and GAM.schm_code!=\'FD009\' AND ACCT_POA_AS_REC_TYPE = \'J\' AND ACCT_POA_AS_SRL_NUM = \'008\') cif_j8
ON
 cif_m.FORACID = cif_j8.FORACID
LEFT JOIN
 (SELECT GAM.FORACID as FORACID, AAS.CUST_ID AS CIF_J9  FROM tbaadm.GAM GAM INNER JOIN tbaadm.AAS AAS 
ON GAM.ACID = AAS.ACID where GAM.schm_type=\'TDA\' and GAM.schm_code!=\'FD009\' AND ACCT_POA_AS_REC_TYPE = \'J\' AND ACCT_POA_AS_SRL_NUM = \'009\') cif_j9
ON
 cif_m.FORACID = cif_j9.FORACID''', "2")
if __name__ == "__main__":
    
    
    '''
    #######Check Pegged Flag
    start = timer()
    print("Start Checking Wrong Pegged FLAG")
    pegged = DataValidation(pd.DataFrame(), "SOL_ID,FORACID,ACCT_NAME,SCHM_CODE,CUST_TYPE_CODE,INT_TBL_CODE,PEGGED_FLG,START_DATE,CIF_ID","tbaadm","ITC")
    pegged.checkPeggedFlag()
    pegged.export_csv(pegged.df,outputPath+"/peggedFlag.csv")
    end = timer()
    pegged.elaspedTime = end - start
    print( "checkPeggedFlag Completed, Time Elapsed : " + str(pegged.elaspedTime) + " seconds")
    
    #########this is use to check for duplicate book in SBA    
    start = timer()
    print("Start Checking Duplicated SBA Booking Number")
    SBABook = DataValidation(pd.DataFrame(), "SOL_ID,GAM.FORACID,CIF_ID,ACCT_NAME,SCHM_CODE,BEGIN_CHQ_NUM,CHQ_ISSU_DATE","tbaadm","CBT")   
    SBABook.checkBookDupSBA() 
    SOL = DataValidation(pd.DataFrame(), "SOL_DESC, SOL_ID","tbaadm","CBT")
    SOL.df = SOL.query2("SELECT " + SOL.colStatement + " FROM tbaadm.SOL", "2")
    SBABook.duplicate = pd.merge(SBABook.duplicate,SOL.df,on = ['SOL_ID'],how = 'left')  
    SBABook.duplicate.rename(columns={'GAM.FORACID': 'FORACID', 'BEGIN_CHQ_NUM': 'BOOK_NUM'}, inplace=True)
    SBABook.export_csv(SBABook.duplicate, outputPath+"/exportDuplicatedSBA.csv")
    end = timer()
    SBABook.elaspedTime = end - start
    print( "checkBookDupSBA Completed, Time Elapsed : " + str(SBABook.elaspedTime) + " seconds")
    
    #########this is use to check for duplicate book in TDA
    start = timer()
    print("Start Checking Duplicated TDA Booking Number")
    TDABOOK = DataValidation(pd.DataFrame(), "SOL_ID,GAM.FORACID,CIF_ID,GAM.ACCT_NAME,SCHM_CODE,BOOK.PASSBOOK_NO","custom","C_TDM")   
    TDABOOK.checkBookDupTDA()
    SOL = DataValidation(pd.DataFrame(), "SOL_DESC, SOL_ID","tbaadm","CBT")
    SOL.df = SOL.query2("SELECT " + SOL.colStatement + " FROM tbaadm.SOL", "2")
    TDABOOK.duplicate = pd.merge(TDABOOK.duplicate,SOL.df,on = ['SOL_ID'],how = 'left')
    TDABOOK.duplicate.rename(columns={'GAM.FORACID': 'FORACID', 'BOOK.PASSBOOK_NO': 'BOOK_NUM'}, inplace=True)
    TDABOOK.export_csv(TDABOOK.duplicate, outputPath+"/exportDuplicatedTDA.csv")
    end = timer()
    TDABOOK.elaspedTime = end - start
    print( "checkBookDupTDA Completed, Time Elapsed : " + str(TDABOOK.elaspedTime) + " seconds")
    
    ##Check Interest code
    start = timer()
    print("Start Checking Interest Code")
    intCode =  DataValidation(pd.DataFrame(), "GAM.SOL_ID,FORACID,CIF_ID,ACCT_NAME,SCHM_CODE, CUST_TYPE_CODE,INT_TBL_CODE,CUST_ID,ENTITY_ID, INT_TBL_CODE_SRL_NUM","tbaadm","ITC")
    intCode.checkInterestCode()
    GSP = DataValidation(pd.DataFrame(), "CUST_TYPE_CODE,GSP.INTEREST_CODE,SCHM_CODE","CUSTOM","C_GSP")
    GSP.df = GSP.query2("SELECT " + GSP.colStatement + " FROM C_GSP GSP WHERE (SCHM_TYPE=\'TDA\' OR SCHM_TYPE=\'SBA\' OR SCHM_TYPE=\'CAA\')","2")
    joined = pd.merge(intCode.df,GSP.df,on = ['CUST_TYPE_CODE','SCHM_CODE'],how = 'left')      
    result = joined[joined['GSP.INTEREST_CODE'] != joined.INT_TBL_CODE]
    GSP.export_csv(result,outputPath+"/WrongIntCode.csv")
    end = timer()
    intCode.elaspedTime = end - start
    print( "checkInterestCode Completed, Time Elapsed : " + str(intCode.elaspedTime) + " seconds")
    #########This is use to check for correctness of WTAX_FLG
    start = timer()
    print("Start Checking Wrong WTAX FLAG")
    WTFLAG = DataValidation(pd.DataFrame(), "SOL_ID,FORACID,CIF_ID,ACCT_NAME,WTAX_PCNT,WTAX_FLG","tbaadm","GAM")
    WTFLAG.checkWTAXFlag()
    WTFLAG.export_csv(WTFLAG.missFlag, outputPath+"/exportWrongWTAX.csv")
    end = timer()
    WTFLAG.elaspedTime = end - start
    print( "checkWTAXFlag Completed, Time Elapsed : " + str(WTFLAG.elaspedTime) + " seconds")    
    '''
    
    '''
    
    
    #check PCNT
    PCNT = DataValidation(pd.DataFrame(), "SOL_ID,FORACID,CIF_ID,GAM.ACCT_NAME,GAM.SCHM_CODE,GSP.INTEREST_CODE,ACC.CUST_TYPE_CODE,WTAX_PCNT,GSP.WITHOLDING_TAX","tbaadm","GAM")
    PCNT.checkPCNTFlag()
	PCNT.df.rename(columns={'WTAX_PCNT': 'WRONG_PCNT', 'GSP.WITHOLDING_TAX': 'CORRECT_PCNT'}, inplace=True)
    PCNT.export_csv(PCNT.df,outputPath+"/exportWrongPCNT.csv")
    
    ### Check Clearing
    start = timer()
    print("Start Checking Clearing")    
    clearing = DataValidation(pd.DataFrame(), "SOL_ID,CLG_ZONE_CODE,CLG_ZONE_DATE,TOT_CR_PART_TRAN_AMT,Zone_stat","tbaadm","OZH")
    clearing.checkClearing()
    centralTime = DataValidation(pd.DataFrame(), "DB_STAT_DATE","tbaadm","GCT")
    centralTime.df = centralTime.query2("SELECT " + centralTime.colStatement + " FROM tbaadm.gct", "2")
    CheckingDate = centralTime.df.DB_STAT_DATE[0] + relativedelta(days=-2)
    clearing.df = clearing.df.loc[(clearing.df.CLG_ZONE_DATE < CheckingDate)]
    clearing.export_csv(clearing.df,outputPath+"/clearingCheck.csv")
    end = timer()
    clearing.elaspedTime = end - start
    print( "checkClearing Completed, Time Elapsed : " + str(clearing.elaspedTime) + " seconds")
    
    #####Check Renew
    start = timer()
    print("Start Checking Renew")
    renewed = DataValidation(pd.DataFrame(), "GAM.SOL_ID,FORACID,CIF_ID,ACCT_NAME,SCHM_CODE,CUST_TYPE_CODE,MATURITY_DATE,GAM.CLR_BAL_AMT","tbaadm","TAM")
    renewed.checkRenewed()
    centralTime = DataValidation(pd.DataFrame(), "DB_STAT_DATE","tbaadm","GCT")
    centralTime.df = centralTime.query2("SELECT " + centralTime.colStatement + " FROM tbaadm.gct", "2")
    CheckingDate = centralTime.df.DB_STAT_DATE[0] + relativedelta(days=-2)
    renewed.df = renewed.df.loc[(renewed.df.MATURITY_DATE < CheckingDate)]
    renewed.export_csv(renewed.df,outputPath+"/renewedCheck.csv")
    end = timer()
    renewed.elaspedTime = end - start
    print( "checkRenewed Completed, Time Elapsed : " + str(renewed.elaspedTime) + " seconds")
    
    #####Check mode of oper code
    opper = DataValidation(pd.DataFrame(), "GAM.SOL_ID,FORACID,CIF_ID,ACCT_NAME,SCHM_TYPE,MODE_OF_OPER_CODE","tbaadm","ITC")
    opper.checkOper()
    opper.export_csv(opper.df,outputPath+"/opper.csv")
    '''
    '''
    
    ##Check All ITC transact int code
    transITC = DataValidation(pd.DataFrame(), "SOL_ID, FORACID, CIF_ID, ACCT_NAME, SCHM_CODE, INT_TBL_CODE", "tbaadm", "GAM")
    transITC.checkINTtransaction()
    transITC.export_csv(transITC.df,outputPath+"/transITC.csv")
    
    ##Check Product expiration
    start = timer()
    print("Start Checking Product Expiration")
    productEx = DataValidation(pd.DataFrame(), "GAM.SOL_ID, FORACID, CIF_ID, ACCT_NAME, SCHM_CODE, MATURITY_DATE", "tbaadm", "GAM")
    productEx.checkProductExpire()
    centralTime = DataValidation(pd.DataFrame(), "DB_STAT_DATE","tbaadm","GCT")
    centralTime.df = centralTime.query2("SELECT " + centralTime.colStatement + " FROM tbaadm.gct", "2")
    productEx.df = productEx.df.loc[(productEx.df.MATURITY_DATE < centralTime.df.DB_STAT_DATE[0])]
    productEx.export_csv(productEx.df, outputPath+"/productExpire.csv")
    end = timer()
    productEx.elaspedTime = end - start
    print( "checkProductExpire Completed, Time Elapsed : " + str(productEx.elaspedTime) + " seconds") 
    ##CHECK AAS transaction
    '''
    joint = DataValidation(pd.DataFrame(), "FORACID", "tbaadm", "GAM")
    print DBHOST