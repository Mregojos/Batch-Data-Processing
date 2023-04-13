# Batch Data Processing with Orchestration

## Objetive
* To process and clean the data (Canada TFWP's Positive LMIA Employers List of 2022)
* To build a simple data pipeline

## Data
* Canada TFWP's Positive LMIA Employers List of 2022

## Tech Stack
* Python, Pandas, Prefect, Prefect Cloud

## Tasks
```sh
# Login to Prefect Cloud and create a workflow and token
# In CLI
prefect cloud login
# Use your token

# Install requirements.txt
pip install -U -r requirements.txt

# Run data-processing.py
python ./data-processing.py
```

### Prefect Cloud Workflow
![](https://github.com/Mregojos/Batch-Data-Processing/blob/main/images/image.png)
> It runs sucessfully!

### Data Sources
* Canada TFWP's Positive LMIA Employers List of 2022 Data Link: https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97
* [Quarter 4 Download Link](https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97/resource/cff0477d-8ab1-4252-b56a-2cd96b057049/download/tfwp_2022q4_pos_en.xlsx)
* [Quarter 3 Download Link](https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97/resource/b369ae20-0c7e-4d10-93ca-07c86c91e6fe/download/tfwp_2022q3_positive_en.xlsx)   
* [Quarter 2 Download Link](https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97/resource/dd627095-614a-45df-a7ef-df0a4a46a812/download/tfwp_2022q2_positive_en.xlsx)
* [Quarter 1 Download Link ](https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97/resource/8da7aa91-8df9-469e-9120-87ddf12c9944/download/tfwp_2022q1_positive_en.xlsx)
