import pandas as pd
import os
from pandas import DataFrame
import pyodbc
import sqlalchemy 
from sqlalchemy import create_engine, MetaData, Table, select, inspect
from sqlalchemy.orm import sessionmaker
import pprint as pp


def main():
    """
    Once project has been successfully run, remove the source file from the /source_data folder as
    the source file exceeds 100MB which prevents the project from being pushed to GitHub

    """
    #This is the route to the original source file being processed
    odmfilepath = 'source_data\\'
    #This is the name of the source file which is going to be processed.
    odmfilename = 'Final_ODM_201819_v4.csv'

    odmoutputpath = 'output\\'
    odmfileoutputname = 'odm_dimensions_201819_v4.xlsx'
    odmfileoutputsourcefile = '309_ODMCH7_201718_v4_nonan.csv'

    dtypedict = {
        'Mode':'category',
        'Orig':'category',
		'Group_orig':'category',
        'Dest':'category',
        'Group_dest_code':'category',
        'Group_dest':'category',
        'Route':'category',
        'Flag':'float64',
        'Dist':'float64',
        'm_all':'float64',
        'r_all':'float64',
        'j_all':'float64',
        'r_full':'float64',
        'j_full':'float64',
        'r_Reduced_ex_apex':'float64',
        'j_Reduced_ex_apex':'float64',
        'r_seas':'float64',
        'j_seas':'float64',
        'r_Apex':'float64',
        'j_Apex':'float64',
        'r_All_Red':'float64',
        'j_All_Red':'float64',
        'r_1st_Full':'float64',
        'r_1st_Red':'float64',
        'r_1st_Seas':'float64',
        'r_1st_Apex':'float64',
        'j_1st_Full':'float64',
        'j_1st_Red':'float64',
        'j_1st_Seas':'float64',
        'j_1st_Apex':'float64',
        'r_Std_Full':'float64',
        'r_Std_Red':'float64',
        'r_Std_Seas':'float64',
        'r_Std_Apex':'float64',
        'j_Std_Full':'float64',
        'j_Std_Red':'float64',
        'j_Std_Seas':'float64',
        'j_Std_Apex':'float64',
        'OrigName':'category',
        'OrigRegion':'category',
        'OrigCounty':'category',
        'OrigDistrict':'category',
        'OrigNUTS2_Code':'category',
        'OrigNUTS2_Desc':'category',
        'DestName':'category',
        'DestRegion':'category',
        'DestCounty':'category',
        'DestDistrict':'category',
        'DestNUTS2_Code':'category',
        'DestNUTS2_Desc':'category',
        'routedesc':'category'
        #initial name supplied in 2019
        #'Route Description':'category'

        
        }
    print("Reading in the ODM file.  please be patient.\n\n")

    odm = pd.read_csv(odmfilepath + odmfilename,dtype= dtypedict)

    # fudge the data
    odm_revised = fudgeTheData(odm, 20172018)

    #replace na value
    odm_revised_nonan = nonan(odm_revised)

    print("The first five rows of odm are\n")
    print(odm_revised_nonan.head())

    print("The datatypes being used are \n")
    print(odm_revised_nonan.info())

    print("The summary information is being calculated\n")
    summary =  odm_revised_nonan.describe()

    print("The distinct values for Mode are \n")
    mode = odm_revised_nonan['Mode'].drop_duplicates()
    print(mode)

    print("The distict values for NUTS 2 are \n")
    NUTS2 = odm_revised_nonan[['DestNUTS2_Code','DestNUTS2_Desc']].drop_duplicates()
    print(NUTS2)

    print("The distict values for route are \n")
    
    #route = odm_revised_nonan[['Route','routedesc']].drop_duplicates()
    #initial name in 2019
    route = odm[['Route','Route Description']].drop_duplicates()
    print(route)
    

    
    print("The distict values for station are \n")
    station = odm_revised_nonan[['Orig','OrigName','OrigRegion','OrigCounty','OrigDistrict','OrigNUTS2_Code','OrigNUTS2_Desc']].drop_duplicates()
    print(station)

    print("getting aggregated checksums")
    #get DW data
    print("getting DW data.  This is rather slow, with a loading time of 15-25 minutes. /n Please be patient.  Perhaps go and have a nice cup of tea?")
    #change the source_item_id to match the previous year you want to compare against.
    dwloadeddata = getDWdata('ORR','factt_309_odm_ch7',9430)

    print("getting aggregated checksums")
    
    #aggregate DW by Region
    print("getting DW regions aggregation")
    dwregionaggresult = dwloadeddata.groupby(['origregion','destregion']).agg({'Dist':'sum','mall':'sum','rall':'sum','jall':'sum'})

    #aggregate DW by NUTS
    print("getting DW NUTS aggregation")
    dwnutsaggresult = dwloadeddata.groupby(['origNUTS2_Code','destNUTS2_Code']).agg({'Dist':'sum','mall':'sum','rall':'sum','jall':'sum'})

    

    #aggregate source by Region
    print("getting source regions aggregation")
    sourceregionaggresult = odm_revised_nonan.groupby(['OrigRegion','DestRegion']).agg({'Dist':'sum','mall':'sum','r_all':'sum','j_all':'sum'})

    #aggregate source by NUTS
    print("getting source regions aggregation")
    sourcenutsaggresult = odm_revised_nonan.groupby(['OrigNUTS2_Code','DestNUTS2_Code']).agg({'Dist':'sum','mall':'sum','r_all':'sum','j_all':'sum'})

    #checknuts = dwnutsaggresult - sourcenutsaggresult
    #checkregion = dwregionaggresult - sourceregionaggresult


    print(f"Exporting odm dimension info to {odmoutputpath} as {odmfileoutputname}")
    with pd.ExcelWriter(odmoutputpath + odmfileoutputname ) as writer:
        mode.to_excel(writer, sheet_name='odm_mode')
        NUTS2.to_excel(writer,sheet_name='odm_NUTS')
        route.to_excel(writer,sheet_name='odm_route')
        station.to_excel(writer,sheet_name='odm_station')
        summary.to_excel(writer,sheet_name='summary_data_for_QA')
        
        sourceregionaggresult.to_excel(writer,sheet_name='region checksum from source')
        dwregionaggresult.to_excel(writer,sheet_name='region checksum from DW')
        #checkregion.to_excel(writer,sheet_name='region check')


        sourcenutsaggresult.to_excel(writer,sheet_name='nuts checksum from source')
        dwnutsaggresult.to_excel(writer,sheet_name='nuts checksum from DW')
        #checknuts.to_excel(writer,sheet_name= 'nuts check')

        writer.save()

    #export new odm file
    print("Exporting the revised source file, please be patient")
    odm.to_csv(odmoutputpath + odmfileoutputsourcefile,index=False)



def getDWdata(schema_name,table_name,source_item_id):
    """
    This uses SQL Alchemy to connect to SQL Server via a trusted connection and extract a filtered table, which is then coverted into a dataframe.
    This is intended for getting the partial table for fact data.

    Parameters
    schema_name:    A string represetnting the schema of the table
    table_name:     A string representing the name of the table
    source_item_id: An integer representing the source_item_id needed

    returns:        A dataframe containing the table   
    """
    engine = sqlalchemy.create_engine('mssql+pyodbc://AZORRDWSC01/ORR_DW?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes')
    
    conn = engine.connect()

    metadata = MetaData()

    example_table = Table(table_name, metadata,autoload=True, autoload_with=engine, schema=schema_name)

    #get raw table data, filtered by source_item_id
    query = select([example_table]).where(example_table.c.source_item_id == source_item_id)

    df = pd.read_sql(query, conn)
    return df


def fudgeTheData(df, fy):
    """
    This applies a series of changes to the source file to enable loading of the data using the ETL package
    Add financial year column
    Renames "m_all" to "mall"
    Puts 0 in tlag column where flag is NULL or negative and converts to integer.

    Parameters:
    df:     This is a dataframe holding the ODM data.
    fy:     This is an integer holding the financial year key of the data.

    Returns:
    df:     A dataframe with the amended values.
    """
    
    print("Fudging the data")
    df.insert(0,"Financial_year",fy)

    # change name of column 'm_all' to 'mall'
    df.rename(columns={"m_all":"mall"},inplace=True)



    # amend Flag column to 0 if blank
    df['Flag'].fillna(0, inplace=True)

    #amend Flag column to 0 if value is negative
    df['Flag'][df['Flag'] < 0 ] = 0

    #amend Flag column to integer
    df['Flag'] = df['Flag'].astype(int)

    return df


def nonan(df):
    print("replacing nan with 0 on numerical fields")
    df['mall'].fillna(0, inplace=True)
    df['r_all'].fillna(0, inplace=True)
    df['j_all'].fillna(0, inplace=True)
    df['r_full'].fillna(0, inplace=True)
    df['j_full'].fillna(0, inplace=True)
    df['r_Reduced_ex_apex'].fillna(0, inplace=True)
    df['j_Reduced_ex_apex'].fillna(0, inplace=True)
    df['r_seas'].fillna(0, inplace=True)
    df['j_seas'].fillna(0, inplace=True)
    df['r_Apex'].fillna(0, inplace=True)
    df['j_Apex'].fillna(0, inplace=True)
    df['r_All_Red'].fillna(0, inplace=True)
    df['j_All_Red'].fillna(0, inplace=True)
    df['r_1st_Full'].fillna(0, inplace=True)
    df['r_1st_Red'].fillna(0, inplace=True)
    df['r_1st_Seas'].fillna(0, inplace=True)
    df['r_1st_Apex'].fillna(0, inplace=True)
    df['j_1st_Full'].fillna(0, inplace=True)
    df['j_1st_Red'].fillna(0, inplace=True)
    df['j_1st_Seas'].fillna(0, inplace=True)
    df['j_1st_Apex'].fillna(0, inplace=True)
    df['r_Std_Full'].fillna(0, inplace=True)
    df['r_Std_Red'].fillna(0, inplace=True)
    df['r_Std_Seas'].fillna(0, inplace=True)
    df['r_Std_Apex'].fillna(0, inplace=True)
    df['j_Std_Full'].fillna(0, inplace=True)
    df['j_Std_Red'].fillna(0, inplace=True)
    df['j_Std_Seas'].fillna(0, inplace=True)
    df['j_Std_Apex'].fillna(0, inplace=True)

    return df


if __name__ == '__main__':
    main()
