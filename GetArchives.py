import os
import sys
import pandas as pd
import shutil
import requests
import ImportDF

script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')

def download_archives(df, link_column, download_directory): # download from web
    """
    Downloads archives from URLs in a specified Pandas DataFrame column and saves them to a specified directory.
    
    Args:
    - df (pandas.DataFrame): the DataFrame containing the links to download
    - link_column (str): the name of the column in df that contains the links to download
    - download_directory (str): the directory to save the downloaded archives in
    
    Returns:
    - None
    """
    
    # Create the download directory if it doesn't already exist
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)
    
    # Iterate through the links in the specified column of the DataFrame
    for link in df[link_column]:
        
        # Make a request to the link
        response = requests.get(link)
        
        # Extract the filename from the URL
        filename = os.path.basename(link)
        
        # Save the response content to a file in the specified download directory
        with open(os.path.join(download_directory, filename), 'wb') as f:
            f.write(response.content)
            
    print('Download complete!')


def download_files(df, file_column, download_directory):#download from folther
    """
    Copies files from paths in a specified Pandas DataFrame column and saves them to a specified directory.
    
    Args:
    - df (pandas.DataFrame): the DataFrame containing the file paths to copy
    - file_column (str): the name of the column in df that contains the file paths to copy
    - download_directory (str): the directory to save the copied files in
    
    Returns:
    - None
    """
    
    # Create the download directory if it doesn't already exist
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)
    
    # Iterate through the file paths in the specified column of the DataFrame
    for file_path in df[file_column]:
        
        # Extract the filename from the file path
        filename = os.path.basename(file_path)
        
        # Copy the file to the specified download directory
        shutil.copy(file_path, os.path.join(download_directory, filename))
            
    print('Download complete!')





def get_file_info(folder_path):
    files = os.listdir(folder_path)
    file_info = []
    for file in files:
        file_path = os.path.join(folder_path, file)
        SiteName = file[-14:-5]
        if os.path.isfile(file_path):
            modified_time = os.path.getmtime(file_path)
            modified_time_str = pd.to_datetime(modified_time, unit='s')
            file_info.append({'Name': file,'Site':SiteName, 'Date Updated': modified_time_str,'FileLink':file_path})
    df = pd.DataFrame(file_info)
    return df
csv_path = os.path.join(script_dir+'/export/AsBuilt_FileRede/', 'AsBuildArchives_REDE'+'.csv')
folder_path = '//internal/FileServer/TBR/Network/NetworkAssurance/RegionalNetworkAssurance-SP/RSQA/10.Organização da rede/Governança/Projetos 2020/Netflow/AsBuilt'
df = get_file_info(folder_path)
df.to_csv(csv_path,index=True,header=True,sep=';')

csv_path2 = os.path.join(script_dir+'/export/AsBuilt_FileRede/', 'AsBuildArchives_MAQUINA'+'.csv')
folder_path2 = 'C:/Users/f8059678/OneDrive - TIM/Dario/@_PYTHON/NGNIS-RFSHEET/import/AsBuilt/old'
df2 = get_file_info(folder_path2)
df2.to_csv(csv_path2,index=True,header=True,sep=';')


csv_path3 = os.path.join(script_dir+'/export/AsBuilt_FileRede/', 'AsBuildArchives_MERGED'+'.csv')
merged_df = pd.merge(df, df2, on="Site", how="outer", indicator=True)
merged_df.to_csv(csv_path3,index=True,header=True,sep=';',encoding='utf-8-sig')

NETFLOW_fields = ['Elemento ID','ID Ordem Complexa','Nome da atividade do processo']
NETFLOW_pathImport = '/import/Netflow'

NETFLOW = ImportDF.ImportDF3(NETFLOW_fields,NETFLOW_pathImport)
NETFLOW.name = 'NETFLOW'
filtro = ['7.3.12 Verifica cadastro de Initial Tunning no NGNIS']
NETFLOW = NETFLOW.loc[NETFLOW['Nome da atividade do processo'].isin(filtro)]

#NETFLOW_Merged = pd.merge(NETFLOW, merged_df, on="Site", how="outer", indicator=True)

NETFLOW_Merged = pd.merge(NETFLOW,merged_df, how='left',left_on=['Elemento ID'],right_on=['Site'],validate="m:m")


filtro2 = ['left_only','both']
NETFLOW_Merged = NETFLOW_Merged.loc[NETFLOW_Merged['_merge'].isin(filtro2)]
print(NETFLOW_Merged)

download_files(NETFLOW_Merged, 'FileLink_x', 'downloads')



# Filter out rows with the same value in "col_name"
#different_df = merged_df[merged_df["_merge"] != "both"]

# Drop the "_merge" column
#different_df.drop(columns="_merge", inplace=True)

'''
for index, row in merged_df.iterrows():
    # Get the URL from the link column
    url = row["link_column_name"]
    
    # Send a GET request to the URL and download the content
    response = requests.get(url)
    content = response.content
    
    # Write the content to a file
    with open(f"{index}.pdf", "wb") as f:
        f.write(content)
'''



