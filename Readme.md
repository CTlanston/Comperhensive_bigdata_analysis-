# Renewable Energies Production Across Europe: Big Data Analysis Project README

## Project Overview
This project, titled "Renewable Energies Production Across Europe," is a comprehensive big data analysis initiative aimed at providing strategic insights for site selection, forecasting future energy demands, and market entry strategies for renewable energy companies in Europe. The project leverages Python, AWS technologies, and big data tools to analyze a vast dataset of renewable energy capacities across Europe.

## Key Components

1. **Data Preparation and Ingestion**
   - Data related to various renewable energy types (Bioenergy, Solar, Wind, Geothermal,

Hydro, and Marine Energy) is sourced and uploaded to an AWS S3 bucket.
   - The data is transformed into Parquet format for optimized storage and access.

2. **Data Processing with AWS EMR and Hive**
   - Utilizing Python’s Boto3 library, an EMR cluster is created to handle the large-scale data.
   - The data is then uploaded into Hive’s metastore for efficient querying and analysis.

3. **Data Analysis with Presto and Python**
   - A Presto connection is established to facilitate data analysis.
   - Exploratory data analysis is performed using Python, focusing on identifying trends in renewable energy adoption and capacity across different regions in Europe.

4. **Forecasting with Prophet**
   - Meta’s open-source Prophet library is used to forecast future trends in renewable energy production.
   - This predictive modeling aims to provide valuable insights for investors and companies looking to invest in clean energy alternatives.

## Objective
The project aims to equip renewable energy companies and investors with data-driven insights for making strategic decisions in the European market. This includes identifying attractive markets for investment, understanding

geographical distributions, technological trends, and energy demand trends. The analysis focuses on renewable energy types like Bioenergy, Solar, Wind, Geothermal, Hydro, and Marine Energy.

## Methodology and Tools
- **Python**: Used for data ingestion, interaction with AWS services, and data processing.
- **AWS Services**: S3 for data storage, EMR for big data processing, and Boto3 for AWS interactions.
- **Hive**: For managing and querying large datasets.
- **Presto**: To perform exploratory data analysis.
- **Prophet Library**: For forecasting energy production trends.

## Outcomes and Benefits
- **Strategic Insights**: Provides actionable insights for strategic site selection and market entry strategies in the renewable energy sector.
- **Forecasting Trends**: Helps in forecasting future energy demand and trends, aiding investment decisions in the renewable energy market in Europe.

This project showcases the application of big data tools and technologies in analyzing and forecasting trends in the renewable energy sector, offering valuable insights for strategic decision-making in this rapidly evolving industry.



# What to do
Before the code is run: please do the below.
Set up the project code and directories as is.
## AWS CONSOLE
Navigate to AWS console and start it
Copy the aws_access_key_id, aws_secret_access_key, aws_session_token

Replace the parameters in the auth/auth.json file with these respectively.
Next navigate to the security group of the Master and add two Inbound/Ingress rules

Allow SSH traffic on port 22, choose Either My Ip or Custom.

For Custom, input the subnet mask for all IPs from which traffic is expected.
For My Ip, your IP will be in put automatically.
Allow All TCP traffic from the either presto port usually (8889) and set up as specified above.

If you are unsure about the port, simply choose the entire port range (0 - 63555).
FOR THIS IT IS CRUCIAL THAT YOU LIMIT THE TRAFFIC TO COME FROM EITHER YOUR IP ONLY OR THE CUSTOM SUBNET MASK.
IF YOU DONT, IT ALLOWS ALL PUBLIC TRAFFIC AND PUBLIC TRAFFIC IS VERY SUSCEPTIBLE TO HACKS.
After the above the code should run fine.

## CODE EXECUTION
Make the changes in the auth/auth.json
Run the Main_File.py
An analysis is done on the data in the bigdata-project-analysis.ipynb
These are the only two files that need to be run
