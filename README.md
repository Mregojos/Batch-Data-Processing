# Batch Data Processing with Orchestration

* Tech Stack: Python, Pandas, Prefect, Prefect Cloud
* Data: Canada's Approved LMIA of 2022 dataset

### Objetive
* To process and clean the data (Canada's Approved LMIA of 2022 dataset)
* To build a simple data pipeline

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
