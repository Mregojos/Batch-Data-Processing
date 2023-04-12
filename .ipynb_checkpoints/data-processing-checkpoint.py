# data-processsing.py

#---------------Libraries-------------------------#
from prefect import flow, task
import urllib.request
import pandas as pd
import os

#---------------Variables------------------------#
url_data = ['https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97/resource/cff0477d-8ab1-4252-b56a-2cd96b057049/download/tfwp_2022q4_pos_en.xlsx',
       'https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97/resource/b369ae20-0c7e-4d10-93ca-07c86c91e6fe/download/tfwp_2022q3_positive_en.xlsx',
       'https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97/resource/dd627095-614a-45df-a7ef-df0a4a46a812/download/tfwp_2022q2_positive_en.xlsx',
       'https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97/resource/8da7aa91-8df9-469e-9120-87ddf12c9944/download/tfwp_2022q1_positive_en.xlsx']

filenames = ['tfwp_2022q4_pos_en.xlsx',
             'tfwp_2022q3_positive_en.xlsx',
             'tfwp_2022q2_positive_en.xlsx',
             'tfwp_2022q1_positive_en.xlsx']

# Canada has only 10 provinces and 3 Territories
Provinces = ['Newfoundland and Labrador', 'Prince Edward Island', 'Nova Scotia', 'New Brunswick', 'Quebec', 'Ontario', 'Manitoba', 'Saskatchewan', 'Alberta', 'British Columbia']
Territories = ['Yukon', 'Northwest Territories', 'Nunavut']

folder = 'original-data/'

#---------------Extract: Download files-----------------#
@task(log_prints=True, name='Download Files')
def download():
    for url, filename in zip(url_data, filenames):
        urllib.request.urlretrieve(url,f"{folder}{filename}")

#---------------Transform: Data Processing-------------#
@task(log_prints=True, name='Convert xlsv to csv')
def xlsx_to_csv():
    for file in os.listdir("original-data"):
        if (file[-5:-1] + file[-1]) == ".xlsx":
            data = pd.read_excel("original-data/" + file, header=1, index_col=1)
            data.to_csv("processed-data/" + file.rstrip(".xlsx") + ".csv")

@task(log_prints=True, name='Create DataFrame')
def dataframe():
    df_2022q1 = pd.read_csv("processed-data/tfwp_2022q1_positive_en.csv")
    df_2022q2 = pd.read_csv("processed-data/tfwp_2022q2_positive_en.csv")
    df_2022q3 = pd.read_csv("processed-data/tfwp_2022q3_positive_en.csv")
    df_2022q4 = pd.read_csv("processed-data/tfwp_2022q4_pos_en.csv")
    return df_2022q1, df_2022q2, df_2022q3, df_2022q4

@task(log_prints=True, name='Data Cleaning')
def data_cleaning(df_2022q1, df_2022q2, df_2022q3, df_2022q4):
    # Add 1 column in our dataframes (`Quarter` Column)
    df_2022q1["Quarter"]=1
    df_2022q2["Quarter"]=2
    df_2022q3["Quarter"]=3
    df_2022q4["Quarter"]=4
    
    # Remove rows in Province/Territory Column not in Provinces and Territories
    df_2022q1_edited = df_2022q1[df_2022q1["Province/Territory"].isin(Provinces + Territories)]
    df_2022q2_edited= df_2022q2[df_2022q2["Province/Territory"].isin(Provinces + Territories)]
    df_2022q3_edited = df_2022q3[df_2022q3["Province/Territory"].isin(Provinces + Territories)]
    df_2022q4_edited = df_2022q4[df_2022q4["Province/Territory"].isin(Provinces + Territories)]
    
    # Change the df_2022q2's column name Requested LMIAs & Requested Positions to Approved LMIAs & Approved Positions for consistency
    df_2022q2_edited = df_2022q2_edited.rename(columns={"Requested LMIAs":"Approved LMIAs", "Requested Positions":"Approved Positions"})
    
    # Remove whitespaces in Dataframe columns, Split Occupations into two Columns: Code and Job Title, and rearrange the columns
    # df_2022q1_edited
    df_2022q1_edited.columns = df_2022q1_edited.columns.str.strip()
    df_2022q1_edited[['Code', 'Job Title']] = df_2022q1_edited['Occupation'].str.split('-', n=1, expand=True)
    df_2022q1_edited = df_2022q1_edited[["Program Stream", "Province/Territory", "Employer", "Address", "Occupation", "Code", "Job Title", "Incorporate Status", "Approved LMIAs", "Approved Positions", "Quarter"]]

    # df_2022q2_edited
    df_2022q2_edited.columns = df_2022q2_edited.columns.str.strip()
    df_2022q2_edited[['Code', 'Job Title']] = df_2022q2_edited['Occupation'].str.split('-', n=1, expand=True)
    df_2022q2_edited = df_2022q2_edited[["Program Stream", "Province/Territory", "Employer", "Address", "Occupation", "Code", "Job Title", "Incorporate Status", "Approved LMIAs", "Approved Positions", "Quarter"]]

    # df_2022q3_edited
    df_2022q3_edited.columns = df_2022q3_edited.columns.str.strip()
    df_2022q3_edited[['Code', 'Job Title']] = df_2022q3_edited['Occupation'].str.split('-', n=1, expand=True)
    df_2022q3_edited = df_2022q3_edited[["Program Stream", "Province/Territory", "Employer", "Address", "Occupation", "Code", "Job Title", "Incorporate Status", "Approved LMIAs", "Approved Positions", "Quarter"]]

    # df_2022q4_edited
    df_2022q4_edited.columns = df_2022q4_edited.columns.str.strip()
    df_2022q4_edited[['Code', 'Job Title']] = df_2022q4_edited['Occupation'].str.split('-', n=1, expand=True)
    df_2022q4_edited = df_2022q4_edited[["Program Stream", "Province/Territory", "Employer", "Address", "Occupation", "Code", "Job Title", "Incorporate Status", "Approved LMIAs", "Approved Positions", "Quarter"]]
    
    # Concatenate all the datasets and name this new dataset to df_2022
    df_2022 = pd.concat([df_2022q1_edited, df_2022q2_edited, df_2022q3_edited, df_2022q4_edited])
    
    return df_2022, df_2022q1_edited, df_2022q2_edited, df_2022q3_edited, df_2022q4_edited

#---------------Load------------------------------#
@task(log_prints=True, name='Load the Cleaned Data')
def load(df_2022, df_2022q1_edited, df_2022q2_edited, df_2022q3_edited, df_2022q4_edited):
    # Save the edited/cleaned data
    df_2022q1_edited.to_csv("processed-data/df_2022q1_cleaned.csv", index=0)
    df_2022q2_edited.to_csv("processed-data/df_2022q2_cleaned.csv", index=0)
    df_2022q3_edited.to_csv("processed-data/df_2022q3_cleaned.csv", index=0)
    df_2022q4_edited.to_csv("processed-data/df_2022q4_cleaned.csv", index=0)
    df_2022.to_csv("processed-data/df_2022_cleaned.csv", index=0)
    
#---------------Workflow-------------------------#
@ flow(log_prints=True, name='Prefect-Workflow')
def workflow():
    download()
    xlsx_to_csv()
    df_2022q1, df_2022q2, df_2022q3, df_2022q4 = dataframe()
    df_2022, df_2022q1_edited, df_2022q2_edited, df_2022q3_edited, df_2022q4_edited = data_cleaning(df_2022q1, df_2022q2, df_2022q3, df_2022q4)
    load(df_2022, df_2022q1_edited, df_2022q2_edited, df_2022q3_edited, df_2022q4_edited)
        
#---------------Execution------------------------#
if __name__=="__main__":
    workflow()


# To run data-processing.py
# python ./data-processing.py
