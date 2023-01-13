import pandas as pd



def header(df,ref):

    df.columns = df.iloc[0]
    data = df[1:]
    print(data.columns)



    return df