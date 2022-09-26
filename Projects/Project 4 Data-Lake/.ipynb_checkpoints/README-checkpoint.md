# Data Lake Project
### Summary of the Project
The main goal of this project is to build the ETL pipeline which extracts and process the data from S3 and then loads the output back into the S3 ouput directory. The final output will be used for the further analysis on the songs users listen to. In this project, `EMR clsuter`, `EC2`, `S3 bucket` and `IAM role` is used from AWS.

### Dataset
The dataset is divided into `song` and `log` data and they are from the imaginary music startup Sparkify. Its log data shows the user activities on the Sparkify app, and the song data shows the summary of the songs and artists.

The dimensional table we will see in the output will be in the **schema** of:
![schema_data_lake](https://user-images.githubusercontent.com/79597984/129142430-5dc0cdf1-927a-4841-a33d-c09ca4d05611.jpeg)

### How to run the Python file
1. Create an **IAM user** with Administrator permissions, and put the credentials in the `dl.cfg` file.
2. Create a **S3 bucket** where the dataset and the output will reside.
3. Create an **EMR cluster** with the **EC2 key pair** in the **.pem** format.
4. Make sure to copy the `etl.py` and `dl.cfg` files to hadoop, and run `etl.py` in your **terminal(in EMR)**.