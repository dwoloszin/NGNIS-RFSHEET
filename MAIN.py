import RfSheet_Reader
import SI
import MergedCompare
import AsBuilt_Reader


#ASBuilt
#SI.processArchive()
AsBuilt_Reader.processArchive()
MergedCompare.processArchiveAsBuilt()

#Mover para old
#C:\Users\f8059678\OneDrive - TIM\Dario\@_PYTHON\NGNIS-RFSHEET\export\RFSHEET

'''
#RFSHEET NOVO
SI.processArchive()
RfSheet_Reader.processArchive() #Substituir Novo-> NOVO

MergedCompare.processArchive(['NOVO','Novo','VERIFICAR'],'Merged_CadastroNovo')
MergedCompare.processArchive(['NOVO','Novo','EXISTENTE','Existente','VERIFICAR'],'Merged')

#Verifica os casos que não tem NEW
MergedCompare.CompareNewAll()

'''

#Merged = Merged.loc[Merged['%'] != '1.0 (OK)']
