# ETL Workshop 3

This repository contains the development of Workshop 3 for the ETL course, created by Valentina Morales.

The objective of this project is to build an integrated ETL and streaming pipeline using historical World Happiness datasets from 2015 to 2019. The project includes data profiling, data cleaning, schema harmonization, feature engineering, machine learning model training, Kafka-based streaming inference, prediction storage in a MySQL data warehouse, and a Power BI dashboard.

## 1. Exploratory Data Analysis

An exploratory data analysis was performed for each dataset from 2015 to 2019. The analysis included the review of dataset structure, missing values, duplicated records, inconsistent column names, data types, possible outliers, and schema differences between years.

### 2015 Dataset

For the 2015 dataset, no missing values or duplicated records were identified. No country or region names appeared to be written inconsistently. Some names were similar, but they represented different countries. No significant outliers were found. Some values equal to zero were identified, but these were not considered errors because the variables represent contributions to the happiness score. A value of zero may indicate that the indicator did not contribute to the score for that specific record.

### 2016 Dataset

For the 2016 dataset, no missing values or duplicated records were identified. No inconsistencies were found in country or region names. Similar names were reviewed, but they represented different countries. No significant outliers were found. As in the previous dataset, values equal to zero were interpreted as valid because they may represent no contribution of that indicator to the happiness score.

### 2017 Dataset

For the 2017 dataset, no missing values or duplicated records were identified. No country names were found to be written inconsistently. Some similar names were reviewed, but they represented different countries. No significant outliers were found. Values equal to zero were considered valid because these variables represent contributions to the happiness score.

### 2018 Dataset

For the 2018 dataset, one missing value was identified in the `Perceptions of corruption` column. No duplicated records were found. The `Country or region` column was compared with the regions from previous years, and it was concluded that the values corresponded to countries rather than regions. No inconsistent country names were identified. Some similar names were reviewed, but they represented different countries. No significant outliers were found. Values equal to zero were considered valid because they may indicate that the indicator did not contribute to the happiness score.

### 2019 Dataset

For the 2019 dataset, no missing values or duplicated records were identified. The `Country or region` column was also reviewed and compared with previous regional categories. It was concluded that the values corresponded to countries. No inconsistencies in country names were identified. Some similar names were reviewed, but they represented different countries. No significant outliers were found. Values equal to zero were considered valid because they may represent no contribution to the happiness score.

## 2. Schema Differences Between Years

The datasets do not share exactly the same schema. The 2015 and 2016 datasets are very similar, although 2015 includes `Standard Error`, while 2016 includes `Lower Confidence Interval` and `Upper Confidence Interval`.

The 2017 dataset contains similar variables, but most column names are written differently compared to previous years. The 2018 and 2019 datasets have the same column structure and the same column names.

Because of these schema differences, it was necessary to analyze and harmonize the datasets before merging them into a unified analytical dataset.

## 3. Unified Schema Proposal

To merge the datasets, key columns were identified and standardized based on their meaning across the different years. The following decisions were made:

1. A `year` column was created to identify the source year of each record.

2. The country column was standardized as `country`.  
   The original columns were:
   - `Country`
   - `Country or region`

3. The ranking column was standardized as `happiness_rank`.  
   The original columns were:
   - `Happiness Rank`
   - `Happiness.Rank`
   - `Overall rank`

4. The happiness score column was standardized as `happiness_score`.  
   The original columns were:
   - `Happiness Score`
   - `Happiness.Score`
   - `Score`

5. The GDP column was standardized as `gdp`.  
   The original columns were:
   - `Economy (GDP per Capita)`
   - `Economy..GDP.per.Capita.`
   - `GDP per capita`

6. The social support column was standardized as `social_support`.  
   The original columns were:
   - `Family`
   - `Social support`

7. The health column was standardized as `health`.  
   The original columns were:
   - `Health (Life Expectancy)`
   - `Health..Life.Expectancy.`
   - `Healthy life expectancy`

8. The freedom column was standardized as `freedom`.  
   The original columns were:
   - `Freedom`
   - `Freedom to make life choices`

9. The corruption perception column was standardized as `corruption`.  
   The original columns were:
   - `Trust (Government Corruption)`
   - `Trust..Government.Corruption.`
   - `Perceptions of corruption`

10. The generosity column was standardized as `generosity`, since all datasets used the same column name:
   - `Generosity`

## Data Cleaning and Harmonization

### Unified Schema Proposal

To integrate the datasets from 2015 to 2019, a unified schema was defined. Since the datasets had different column names depending on the year, equivalent columns were identified and renamed using a common final name.

| Final name | 2015 | 2016 | 2017 | 2018 | 2019 |
|---|---|---|---|---|---|
| year | Created column | Created column | Created column | Created column | Created column |
| country | Country | Country | Country | Country or region | Country or region |
| happiness_rank | Happiness Rank | Happiness Rank | Happiness.Rank | Overall rank | Overall rank |
| happiness_score | Happiness Score | Happiness Score | Happiness.Score | Score | Score |
| gdp | Economy (GDP per Capita) | Economy (GDP per Capita) | Economy..GDP.per.Capita. | GDP per capita | GDP per capita |
| social_support | Family | Family | Family | Social support | Social support |
| health | Health (Life Expectancy) | Health (Life Expectancy) | Health..Life.Expectancy. | Healthy life expectancy | Healthy life expectancy |
| freedom | Freedom | Freedom | Freedom | Freedom to make life choices | Freedom to make life choices |
| corruption | Trust (Government Corruption) | Trust (Government Corruption) | Trust..Government.Corruption. | Perceptions of corruption | Perceptions of corruption |
| generosity | Generosity | Generosity | Generosity | Generosity | Generosity |

### Cleaning Rules

| Problem found | Column | Cleaning rule | Action if it fails |
|---|---|---|---|
| Missing values | Perceptions of corruption | There must be no missing values in the selected model features | Discard the row |
| Different column names | All datasets | Columns with the same meaning were renamed to a unified schema | Rename columns before merging |
| Missing year column | All datasets | A `year` column must be created from the source file year | Add the year before merging |
| Non-selected columns | Columns not shared across all years | Columns that were not consistent across all datasets were not included in the unified dataset | Exclude from the final merge |
| Zero values | Numeric contribution columns | Zero values were not automatically treated as errors because they may represent no contribution to the happiness score | Keep the value if it is contextually valid |

### Justification of Cleaning Decisions

Rows with missing values were discarded because the number of affected records was very small and removing them was not expected to significantly alter the results. In this case, the only relevant missing value was found in the `Perceptions of corruption` column.

Column names were normalized by defining a general name for each equivalent variable across the different years. Although many columns represented the same concept, their names varied between datasets. For example, the happiness score, GDP, health, freedom, and corruption variables had different names depending on the year.

The `year` column was created because it did not exist in the original datasets. This column is necessary to identify the year associated with each record after merging all datasets.

Some columns were not included in the final unified dataset because they were not present in all years or were not relevant for the machine learning model. Additionally, zero values were not considered inconsistencies because, based on the context of the dataset, they may represent the absence of contribution of that variable to the happiness score rather than an error.

## 4. Data Cleaning Decisions

The main cleaning decision was to standardize column names across all datasets to create a unified analytical schema. Numeric columns were converted to numeric data types, and the `year` column was added to preserve the temporal origin of each record.

The missing value found in the 2018 `Perceptions of corruption` column was handled during the cleaning process. Values equal to zero were not automatically removed because they may represent valid cases where a specific indicator did not contribute to the happiness score.

## 5. Initial Conclusion

The exploratory analysis showed that the main challenge was not the presence of duplicated records or severe missing values, but the inconsistency in column names and schema structure across the datasets. Therefore, schema harmonization was required before integrating the data and preparing it for machine learning.
