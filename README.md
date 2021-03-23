# ELMS_Data_Quality

## Repo for ELMS Data Quality Tool 

## Last Updated: March 2021 

### The automated backend of the dashboard has been built in [this repo](https://github.com/Defra-Data-Science-Centre-of-Excellence/elmsMetadata) and all data is stored in AWS S3 s3-ranch-20 
### Dashboard link: http://elmsmodelling.int.sce.network/shiny/ELMS_Data_Quality/

### All documentation relating to the tool can be found on Sharepoint [here](https://sp.demeter.zeus.gsi.gov.uk/Sites/aa02/elm/evidanaly/Forms/AllItems.aspx?RootFolder=%2FSites%2Faa02%2Felm%2Fevidanaly%2F4%2E9%5FWorkstream%5FAreas%5FModelling%5FStrategy%2F4%2E9%2E3%2E12%5FModelling%5FPartner%2F4%2E9%2E3%2E12%2E1%5FQ12021%5FCapgemini%2F5%2E%20Sprint%204%2FData%20Quality&FolderCTID=0x012000E5A4A7CDBE65E34EAF2C6C179BAB66F6&View=%7B6161011C%2DA4EE%2D4355%2D9406%2D7513026BD08B%7D)

### List of packages used in the dashboard for reproducibility
Obtained through sessionInfo()

Package          | Version
-------------    | -------------
tidyverse        | 1.3.0 
plotly           | 4.9.3
shinydashboard   | 0.7.1
shiny            | 1.6.0 
shinyWidgets     | 0.5.7
shinyjs          | 2.0.0
DT               | 0.17
readxl           | 1.31
dplyr            | 1.0.4
aws.s3           | 0.3.21
aws.ec2metadata  | 0.2.0
plyr             | 1.8.6


#### Running under: Ubuntu 20.04.2 LTS
