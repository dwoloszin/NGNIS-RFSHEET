import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
from datetime import datetime
import timeit
from os.path import getmtime
import Count
import antenaName
import unique
import ImportDF



def processArchive(filtro,wheretoSave):
    print("\nProcessing " + os.path.basename(__file__) + '...')
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/'+wheretoSave+'/'+ wheretoSave +'.csv')
    inicio = timeit.default_timer()

    SI_fields = ['LOCATION','LATITUDE','LONGITUDE','CS_NAME','CS_STATUS','AZIMUTH','ALTURA','MS_TYPE','SI_PORT_BAND','MECHANICAL_TILT','ANTENA_MODEL','AMD_TILT_E','CST_NAME']
    SI_pathImport = '/export/SI'

    RFSHEET_fields = ['Arquivo','Site','Setor','Setor2','STATUS','Tipo','"Altura(M)"','Azimute (NV)','Tilt Mec','Modelo de Antena','Gain (dBi)','TRX Power (W)','Tilt Elt','Modelo do TRX','Banda','Diplexer1','Diplexer2','EiRP','Triplexer1','Triplexer2','FILTER','Cabo RFS 1/2 SF - Cu (metros)','Cabo RFS 7/8 - Al (metros)','COMBINER (EHCU)','Cabo RFS 1 5/8 - Al (metros)','1/2"','5/8" / 7/8"','Perda(EHCU)_Perda_do_Filter','Dip/Trip','Total da linha','TEC']
    RFSHEET_pathImport = '/export/RFSHEET'


    NETFLOW_fields = ['Elemento ID','ID Ordem Complexa','Nome da atividade do processo']
    NETFLOW_pathImport = '/import/Netflow'
   

    SI = ImportDF.ImportDF(SI_fields,SI_pathImport)
    SI.name = 'SI'
    SI = change_columnsName(SI)

    RFSHEET = ImportDF.ImportDF(RFSHEET_fields,RFSHEET_pathImport)
    RFSHEET.name = 'RFSHEET'
    arquivoName = 'RFSHEET'
    RFSHEET = change_columnsName(RFSHEET)
    RFSHEET = RFSHEET.loc[RFSHEET['STATUS_RFSHEET'].isin(filtro)]
    filtro3 = ['1800 LTE','2100 LTE','2600 LTE','3500 NR','700 LTE','700 NR','LTE 1800MHz','LTE 2100MHz','LTE 2600MHz','LTE 700MHz','NR 3500MHz','NR 700MHz']
    RFSHEET = RFSHEET.loc[RFSHEET['TEC_RFSHEET'].isin(filtro3)]

    NETFLOW = ImportDF.ImportDF3(NETFLOW_fields,NETFLOW_pathImport)
    NETFLOW.name = 'NETFLOW'
    NETFLOW = change_columnsName(NETFLOW)
    filtro = ['2.13 Verifica cadastro de projeto de acesso (RF-Sheet definitivo) no NGNIS','2.16 Verifica cadastro de projeto de acesso (RF-Sheet definitivo) no NGNIS']
    NETFLOW = NETFLOW.loc[NETFLOW['Nome da atividade do processo_NETFLOW'].isin(filtro)]


    Merged = pd.merge(RFSHEET,SI, how='left',left_on=['Setor2_RFSHEET'],right_on=['CS_NAME_SI'],validate="m:m")
    Merged = pd.merge(Merged,NETFLOW, how='left',left_on=['Site_RFSHEET'],right_on=['Elemento ID_NETFLOW'],validate="m:m")

    Merged_OC = Merged.copy()
    Merged_OC = Merged_OC.loc[~Merged_OC['ID Ordem Complexa_NETFLOW'].isna()]

    KeepListCompared = ['Arquivo_RFSHEET','ID Ordem Complexa_NETFLOW']
    locationBase_comparePMO = list(Merged_OC.columns)
    DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
    Merged_OC = Merged_OC.drop(DellListComparede,1)
    Merged_OC = Merged_OC.drop_duplicates()
    Merged_OC = Merged_OC.reset_index(drop=True)

    Merged_OC.rename(columns = {'Arquivo_RFSHEET':'Arquivo_RFSHEET2'}, inplace = True)
    Merged = Merged.drop(['ID Ordem Complexa_NETFLOW'],1)
    Merged = pd.merge(Merged,Merged_OC, how='left',left_on=['Arquivo_RFSHEET'],right_on=['Arquivo_RFSHEET2'],validate="m:m")



    

    #AntenaName
    Merged = antenaName.normalizedAntenaName(Merged,'Modelo de Antena_RFSHEET','ANTENA_MODEL_SI')



    Merged = analise(Merged,arquivoName)

    Merged.insert(len(Merged.columns),'Ref',Merged['Arquivo_RFSHEET'] + Merged['Status'])
    
    CountArquivo = Count.count(Merged,'Arquivo_RFSHEET')
    CountRef = Count.count(Merged,'Ref')

    Merged = pd.merge(Merged,CountArquivo, how='left',left_on=['Arquivo_RFSHEET'],right_on=['Arquivo_RFSHEET'])
    Merged.rename(columns={'count': 'count_Arquivo'}, inplace=True)

    Merged = pd.merge(Merged,CountRef, how='left',left_on=['Ref'],right_on=['Ref'])
    Merged.rename(columns={'count': 'count_Ref'}, inplace=True)

    Merged.insert(len(Merged.columns),'%',"")
    for index, row in Merged.iterrows():
        Merged.at[index,'%'] = str((round((row['count_Ref'] / row['count_Arquivo']),2))) + " (" + row['Status'] + ")"


    
    ArquivoTodo = Merged.copy()
    #Arquivo, Corrigir:
    KeepListCompared = ['Arquivo_RFSHEET','Site_RFSHEET','ID Ordem Complexa_NETFLOW','Nome da atividade do processo_NETFLOW','descricao','%']
    locationBase_comparePMO = list(ArquivoTodo.columns)
    DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
    ArquivoTodo = ArquivoTodo.drop(DellListComparede,1)

    ArquivoTodo.insert(0,'Arquivo',ArquivoTodo['Arquivo_RFSHEET'])
    ArquivoTodo = ArquivoTodo.fillna('').groupby(['Arquivo_RFSHEET'], as_index=True).agg('|'.join)
    
    removefromloop = []
    locationBase_top = list(ArquivoTodo.columns)
    res = list(set(locationBase_top)^set(removefromloop))
    for i in res: 
        for index, row in ArquivoTodo.iterrows():
            ArquivoTodo.at[index, i] = '|'.join(unique.unique_list(ArquivoTodo.at[index, i].split('|'))) 


    #ArquivoTodo = ArquivoTodo.drop_duplicates()
    ArquivoTodo = ArquivoTodo.reset_index(drop=True)
    
    csv_path2 = os.path.join(script_dir, 'export/'+wheretoSave+'/'+'ToDo_'+ wheretoSave +'.csv')
    ArquivoTodo.to_csv(csv_path2,index=True,header=True,sep=';')

    #Merged = Merged.drop_duplicates()
    #Merged = Merged.reset_index(drop=True)
    Merged = Merged.loc[Merged['%'] != '1.0 (OK)']
    Merged.to_csv(csv_path,index=True,header=True,sep=';')
    fim = timeit.default_timer()
    print ('duracao: %f' % ((fim - inicio)/60) + ' min')


'''
def processArchive1():
    print("\nProcessing " + os.path.basename(__file__) + '...')
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/Merged/'+'Merged'+'.csv')
    inicio = timeit.default_timer()

    SI_fields = ['LOCATION','LATITUDE','LONGITUDE','CS_NAME','CS_STATUS','AZIMUTH','ALTURA','MS_TYPE','SI_PORT_BAND','MECHANICAL_TILT','ANTENA_MODEL','AMD_TILT_E','CST_NAME']
    SI_pathImport = '/export/SI'

    RFSHEET_fields = ['Arquivo','Site','Setor','Setor2','STATUS','Tipo','"Altura(M)"','Azimute (NV)','Tilt Mec','Modelo de Antena','Gain (dBi)','TRX Power (W)','Tilt Elt','Modelo do TRX','Banda','Diplexer1','Diplexer2','EiRP','Triplexer1','Triplexer2','FILTER','Cabo RFS 1/2 SF - Cu (metros)','Cabo RFS 7/8 - Al (metros)','COMBINER (EHCU)','Cabo RFS 1 5/8 - Al (metros)','1/2"','5/8" / 7/8"','Perda(EHCU)_Perda_do_Filter','Dip/Trip','Total da linha','TEC']
    RFSHEET_pathImport = '/export/RFSHEET'

    NETFLOW_fields = ['Elemento ID','ID Ordem Complexa','Nome da atividade do processo']
    NETFLOW_pathImport = '/import/Netflow'


    SI = ImportDF(SI_fields,SI_pathImport)
    SI.name = 'SI'
    SI = change_columnsName(SI)

    RFSHEET = ImportDF(RFSHEET_fields,RFSHEET_pathImport)
    RFSHEET.name = 'RFSHEET'
    RFSHEET = change_columnsName(RFSHEET)

    NETFLOW = ImportDF(NETFLOW_fields,NETFLOW_pathImport)
    NETFLOW.name = 'NETFLOW'
    NETFLOW = change_columnsName(NETFLOW)

    Merged = pd.merge(RFSHEET,SI, how='left',left_on=['Setor2_RFSHEET'],right_on=['CS_NAME_SI'])
    Merged = pd.merge(Merged,NETFLOW, how='left',left_on=['Site_RFSHEET'],right_on=['Elemento ID_NETFLOW'])

    #AntenaName
    Merged = antenaName.normalizedAntenaName(Merged,'Modelo de Antena_RFSHEET','ANTENA_MODEL_SI')



    Merged = analise(Merged)

    Merged.insert(len(Merged.columns),'Ref',Merged['Arquivo_RFSHEET'] + Merged['Status'])
    
    CountArquivo = Count.count(Merged,'Arquivo_RFSHEET')
    CountRef = Count.count(Merged,'Ref')

    Merged = pd.merge(Merged,CountArquivo, how='left',left_on=['Arquivo_RFSHEET'],right_on=['Arquivo_RFSHEET'])
    Merged.rename(columns={'count': 'count_Arquivo'}, inplace=True)

    Merged = pd.merge(Merged,CountRef, how='left',left_on=['Ref'],right_on=['Ref'])
    Merged.rename(columns={'count': 'count_Ref'}, inplace=True)

    Merged.insert(len(Merged.columns),'%',"")
    for index, row in Merged.iterrows():
        Merged.at[index,'%'] = str((round((row['count_Ref'] / row['count_Arquivo']),2))) + " (" + row['Status'] + ")"
        #Merged.at[index,'%'] = str((round((row['count_Ref'] / row['count_Arquivo']),2)*100)).replace('.',',')[:6] + " (" + row['Status'] + ")"


    
    ArquivoTodo = Merged.copy()
    #Arquivo, Corrigir:
    KeepListCompared = ['Arquivo_RFSHEET','Site_RFSHEET','ID Ordem Complexa_NETFLOW','Nome da atividade do processo_NETFLOW','descricao','%']
    locationBase_comparePMO = list(ArquivoTodo.columns)
    DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
    ArquivoTodo = ArquivoTodo.drop(DellListComparede,1)
    ArquivoTodo.insert(0,'Arquivo',ArquivoTodo['Arquivo_RFSHEET'])
     
    ArquivoTodo = ArquivoTodo.fillna('').groupby(['Arquivo_RFSHEET'], as_index=True).agg('|'.join)
    
    removefromloop = []
    locationBase_top = list(ArquivoTodo.columns)
    res = list(set(locationBase_top)^set(removefromloop))
    for i in res: 
        for index, row in ArquivoTodo.iterrows():
            ArquivoTodo.at[index, i] = '|'.join(unique.unique_list(ArquivoTodo.at[index, i].split('|'))) 


    #ArquivoTodo = ArquivoTodo.drop_duplicates()
    ArquivoTodo = ArquivoTodo.reset_index(drop=True)
    
    csv_path2 = os.path.join(script_dir, 'export/Merged/'+'ToDo'+'.csv')
    ArquivoTodo.to_csv(csv_path2,index=True,header=True,sep=';')

    Merged.to_csv(csv_path,index=True,header=True,sep=';')
    fim = timeit.default_timer()
    print ('duracao: %f' % ((fim - inicio)/60) + ' min')
'''


def analise(df,ArquivoName1):
    analised = df.copy()
    analised.insert(len(analised.columns),'Status','')
    analised.insert(len(analised.columns),'descricao','')
    analised.insert(len(analised.columns),'Error','')


    arquivoProcessado = []
    for index, row in analised.iterrows():
        verificar = []
        logErro = []
        logErro.append(row['Arquivo_' + ArquivoName1])
        logErro.append(row['Setor_' + ArquivoName1])
        if row['Arquivo_' + ArquivoName1] not in arquivoProcessado:
            arquivoProcessado.append(row['Arquivo_' + ArquivoName1])
        try:
            if float(row['Azimute (NV)_' + ArquivoName1]) != float(row['AZIMUTH_SI']):
                verificar.append('Azimute')
        except:
            logErro.append('Azimute: ' + row['Azimute (NV)_' + ArquivoName1])
        
        try:
            if float(row['"Altura(M)"_' + ArquivoName1]) != float(row['ALTURA_SI']):
                verificar.append('Altura')
        except:
            logErro.append('Altura: '+row['"Altura(M)"_' + ArquivoName1])

        try:
            if float(row['Tilt Mec_' + ArquivoName1]) != float(row['MECHANICAL_TILT_SI']):
                verificar.append('TiltMec')
        except:
            logErro.append('TiltMec: '+row['Tilt Mec_' + ArquivoName1])
        '''
        try:
            if float(row['Tilt Elt_' + ArquivoName1]) != float(row['AMD_TILT_E_SI']):
                verificar.append('TiltEle')   
        except:
            logErro.append('TiltEle: '+row['Tilt Elt_' + ArquivoName1])
        '''
        try:
            if row['Modelo de Antena_' + ArquivoName1] != row['ANTENA_MODEL_SI']:
                verificar.append('ModeloAntena')
        except:
            logErro.append('ModeloAntena: '+row['Modelo de Antena_' + ArquivoName1])

        try:            
            if len(str(row['LOCATION_SI'])) <= 3:
                verificar = "Setor_ nao Localizado no SI, verificar Cadastro"
        except:
            logErro = "Setor_ nao localizado no NGNIS"

        try:
            statuss = ['In Service','Planned','Implementation']
            if len(row['Setor2_' + ArquivoName1]) == 7 and row['Setor2_' + ArquivoName1][:2] == 'SP' and row['Setor2_' + ArquivoName1][-1:] in ['I','J','K','1','2','3']:
                print(row['Setor2_' + ArquivoName1])
                if row['CS_STATUS_SI'] in statuss:
                    verificar = 'Layer 3G nao existente, remover do arquivo e corrigir NGNIS: CS_STATUS = Deactivated'
                else:
                    verificar = 'Layer 3G nao existente, remover do arquivo'
                #analised.at[index,'STATUS_RFSHEET'] = 'EXISTENTE'
                

        except:

            print('Teste')      



        if len(verificar) == 0:
            analised.at[index,'descricao'] = 'OK'
            analised.at[index,'Status'] = 'OK'

        else:
            analised.at[index,'descricao'] = verificar
            analised.at[index,'Status'] = 'NOK'

        if len(logErro) > 2:
            print('\n Verificar arquivos abaixo: ')
            print(logErro)
   
        analised.at[index,'Error'] = logErro

    return analised





def change_columnsName(df):
    for i in df.columns:
        df.rename(columns={i:i + '_' + df.name},inplace=True)
    return df


'''
def ImportDF(fields, pathImport):
    pathImportSI = os.getcwd() + pathImport
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []

    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None, encoding="UTF-8",header=0, error_bad_lines=False,dtype=str, sep = ';',decimal=',',iterator=True, chunksize=10000, usecols = fields )
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS
        df2 = df[fields] # ordering labels 
        #df2["dataArchive_Import"] = fileData   
        li.append(df2)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()

    return frameSI    

'''

def CompareNewAll():
    print("\nProcessing " + os.path.basename(__file__) + '...')
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    pathImport = script_dir + '/export/Merged/ToDo_Merged.csv'
    pathImport2 = script_dir + '/export/Merged_CadastroNovo/ToDo_Merged_CadastroNovo.csv'
    

    print(pathImport)
    csv_path = os.path.join(script_dir, 'export/Merged_CadastroNovo/'+'ToDo_Merged_SEM_CadastroNovo'+'.csv')
    inicio = timeit.default_timer()




    dataFrame1 =  pd.read_csv(pathImport, sep=';',header=0)
    dataFrame2 =  pd.read_csv(pathImport2, sep=';',header=0)

    #Remover

    
    KeepListCompared = ['Arquivo']
    locationBase_comparePMO = list(dataFrame2.columns)
    DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
    dataFrame2 = dataFrame2.drop(DellListComparede,1)
    dataFrame2.insert(len(dataFrame2.columns),'Arquivo2',dataFrame2['Arquivo'])


    print(dataFrame2['Arquivo'].name)
    
    Merged = pd.merge(dataFrame1,dataFrame2, how='left',left_on=['Arquivo'],right_on=['Arquivo'])
    Merged = Merged.loc[Merged['Arquivo2'].isna()]
    Merged = Merged.reset_index(drop=True)
    Merged = Merged.drop(['Arquivo2','Unnamed: 0'],1)
    #Merged = Merged.astype({'ID Ordem Complexa_NETFLOW': int})


    Merged.to_csv(csv_path,index=True,header=True,sep=';')






    fim = timeit.default_timer()
    print ('duracao: %f' % ((fim - inicio)/60) + ' min')







def processArchiveAsBuilt():
    print("\nProcessing " + os.path.basename(__file__) + '...')
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/'+'Merged_AsBuilt'+'/'+ 'Merged_AsBuilt' +'.csv')
    inicio = timeit.default_timer()

    SI_fields = ['LOCATION','LATITUDE','LONGITUDE','CS_NAME','CS_STATUS','AZIMUTH','ALTURA','MS_TYPE','SI_PORT_BAND','MECHANICAL_TILT','ANTENA_MODEL','AMD_TILT_E','CST_NAME']
    SI_pathImport = '/export/SI'

    AsBuilt_fields = ['Arquivo','Site','Setor','RNC/BSC/MME','Setor2','"Altura(M)"','Azimute (NV)','Logical Azimute (NV)','Downtilt (E+M)','Modelo de Antena','Tilt Elt','Tilt Mec','STATUS']
    AsBuilt_pathImport = '/export/AsBuilt'

    NETFLOW_fields = ['Elemento ID','ID Ordem Complexa','Nome da atividade do processo']
    NETFLOW_pathImport = '/import/Netflow'


    SI = ImportDF.ImportDF(SI_fields,SI_pathImport)
    print(SI)
    SI.name = 'SI'
    SI = change_columnsName(SI)
    

    AsBuilt = ImportDF.ImportDF(AsBuilt_fields,AsBuilt_pathImport)
    AsBuilt.name = 'AsBuilt'
    arquivoName = 'AsBuilt'
    AsBuilt = change_columnsName(AsBuilt)
  
    NETFLOW = ImportDF.ImportDF3(NETFLOW_fields,NETFLOW_pathImport)
    NETFLOW.name = 'NETFLOW'
    NETFLOW = change_columnsName(NETFLOW)
    filtro = ['7.3.12 Verifica cadastro de Initial Tunning no NGNIS']
    NETFLOW = NETFLOW.loc[NETFLOW['Nome da atividade do processo_NETFLOW'].isin(filtro)]


    Merged = pd.merge(AsBuilt,SI, how='left',left_on=['Setor2_AsBuilt'],right_on=['CS_NAME_SI'],validate="m:m")
    Merged = pd.merge(Merged,NETFLOW, how='left',left_on=['Site_AsBuilt'],right_on=['Elemento ID_NETFLOW'],validate="m:m")
    

    #AntenaName
    Merged = antenaName.normalizedAntenaName(Merged,'Modelo de Antena_AsBuilt','ANTENA_MODEL_SI')



    Merged = analise(Merged,arquivoName)

    Merged.insert(len(Merged.columns),'Ref',Merged['Arquivo_AsBuilt'] + Merged['Status'])
    
    CountArquivo = Count.count(Merged,'Arquivo_AsBuilt')
    CountRef = Count.count(Merged,'Ref')

    Merged = pd.merge(Merged,CountArquivo, how='left',left_on=['Arquivo_AsBuilt'],right_on=['Arquivo_AsBuilt'])
    Merged.rename(columns={'count': 'count_Arquivo'}, inplace=True)

    Merged = pd.merge(Merged,CountRef, how='left',left_on=['Ref'],right_on=['Ref'])
    Merged.rename(columns={'count': 'count_Ref'}, inplace=True)

    Merged.insert(len(Merged.columns),'%',"")
    for index, row in Merged.iterrows():
        Merged.at[index,'%'] = str((round((row['count_Ref'] / row['count_Arquivo']),2))) + " (" + row['Status'] + ")"


    
    ArquivoTodo = Merged.copy()
    #Arquivo, Corrigir:
    KeepListCompared = ['Arquivo_AsBuilt','Site_AsBuilt','ID Ordem Complexa_NETFLOW','Nome da atividade do processo_NETFLOW','descricao','%']
    locationBase_comparePMO = list(ArquivoTodo.columns)
    DellListComparede = list(set(locationBase_comparePMO)^set(KeepListCompared))
    ArquivoTodo = ArquivoTodo.drop(DellListComparede,1)

    ArquivoTodo.insert(0,'Arquivo',ArquivoTodo['Arquivo_AsBuilt'])
    ArquivoTodo = ArquivoTodo.fillna('').groupby(['Arquivo_AsBuilt'], as_index=True).agg('|'.join)
    
    removefromloop = []
    locationBase_top = list(ArquivoTodo.columns)
    res = list(set(locationBase_top)^set(removefromloop))
    for i in res: 
        for index, row in ArquivoTodo.iterrows():
            ArquivoTodo.at[index, i] = '|'.join(unique.unique_list(ArquivoTodo.at[index, i].split('|'))) 


    #ArquivoTodo = ArquivoTodo.drop_duplicates()
    ArquivoTodo = ArquivoTodo.reset_index(drop=True)
    
    csv_path2 = os.path.join(script_dir, 'export/'+'Merged_AsBuilt'+'/'+'ToDo_'+ 'AsBuilt' +'.csv')
    ArquivoTodo.to_csv(csv_path2,index=True,header=True,sep=';')

    Merged = Merged.loc[Merged['%'] != '1.0 (OK)']
    Merged.to_csv(csv_path,index=True,header=True,sep=';')
    fim = timeit.default_timer()
    print ('duracao: %f' % ((fim - inicio)/60) + ' min')


