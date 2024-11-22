# OA3802: Computational Methods for Data Analytics
## Geospacial Vegetation Analysis for the Brushfire Prediction Model
Team Members: <br>
MAJ Jason Stisser <br>
LCDR Alex Bedley <br>
MAJ Conrad Urban <br>
CAPT Gary Tyler <br>

BLUF: This project aims to overcome the challenges of integrating large-scale satellite vegetation land cover data into bushfire prediction models for southeastern Australia by processing up to 300GB of complex geospatial data sourced from AWS S3 to inform better environmental and risk assessments.

During the preparation of data for a wider study on fire predictors in southeastern Australia, significant challenges were met when attempting to include vegetation land cover. These challenges stem from the geospatial nature of the data, its vast size, and difference in structure as compared to other data used. This project aims to address these challenges and will involve accessing and processing largescale satellite data of vegetation land cover in Australia, with the purpose of informing Bushfire predictions. The data is in the form of many TIFF files stored on a public AWS server (S3 bucket) within a complex file structure. Ultimately, this data will then be joined with a previously created bushfire prediction data frame.

### Major Files and Description:
* Tutorial.html - Tutorial describing the project and the process. <br>
* Brief.pdf - Presentation given by the group. <br>
* meltdown.py - Python Script that creates a SQLite database, performs the API calls and appends the database.<br>
* firefighter.sh - Shell script to run the meltdown.py script on HPC. <br>
* Firetrucks_PowerBI.pbix - PowerBI file for the project
