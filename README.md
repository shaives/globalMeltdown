# OA3802: Computational Methods for Data Analytics
## Geospacial Vegetation Analysis for the Brushfire Prediction Model
Team Members: <br>
Maj Jason Stisser <br>
LCDR Alex Bedley <br>
MAJ Conrad Urban <br>
CAPT Gary Tyler <br>

BLUF: This project aims to overcome the challenges of integrating large-scale satellite vegetation land cover data into bushfire prediction models for southeastern Australia by processing and merging up to 300GB of complex geospatial data sourced from AWS S3 to inform better environmental and risk assessments.

During the preparation of data for a wider study on fire predictors in southeastern Australia, significant challenges were met when attempting to include vegetation Land Cover. These challenges stem from the geospatial nature of the data, its vast size, and difference in structure as compared to other data used. This project aims to address these challenges and will involve accessing and processing largescale satellite data of vegetation land cover in Australia, with the purpose of informing Bushfire predictions. Data are in the form of many TIFF files stored on a public AWS server (S3 bucket) within a complex file structure. This data will then be joined with a previously created bushfire prediction data frame. This will involve accessing, storing, geoprocessing (joins/merges), and statistically analyzing very large amounts of data.

## Data Source & Description
* Source: AWS S3 bucket: dea-public-data <br>
  - Public Data Set from Digital Earth Australia <br>
  - Access via: https://knowledge.dea.ga.gov.au/data/product/dea-fractional-cover-percentiles-landsat/ <br>
* Content: The dataset includes aggregate satellite imagery data, specifically focused on land cover classification across Australia. <br>
* Expected Size: Up to 300 GB of data. This includes approximately 60,000 separate files in a folder structure based on time and location of the observations. The geographic and temporal range of these files will be heavily truncated for analysis; as such final size of the data used will likely be significantly less. <br>
* Previous work has created a geographic data frame with approximately 3M rows. The intent is to merge Fractional Cover data to the corresponding locations and times within this central data frame. <br>
