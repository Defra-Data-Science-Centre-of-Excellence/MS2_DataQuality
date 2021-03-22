### Packages ###

## Data Manipulation and Plotting ##
library(tidyverse)
library(plotly)

## Packages used in Building Shiny App ## 
library(shinydashboard) # can be used with shiny 
library(shiny)
library(shinyWidgets) ## Used for more UI friendly drop down menus ##
library(shinyjs) ## Used to incorporate JS functionality (e.g reset buttons)
library(DT)

## Import pre-processed data ## 
source('Data_Landing.R')

ui <- dashboardPage( 
  
  skin = "green",
  
  dashboardHeader(title = "Data Quality"),
  
  dashboardSidebar(collapsed = TRUE,
                     sidebarMenu()),
  
  dashboardBody(
  ## Generate boxes on the LHS of the App 
  ## 1 - Information 
  ## 2 - Documentation 
  ## 3 - Dropdown menus (which provides interactivity to the App )
    
  fluidRow(
    column(
      width = 5,
      
      box(title = "ELMS Data Quality Tool",
          width = 12,
          solidHeader = TRUE,
          status = "success",
          
          helpText("This tool evaluates data quality metrics for each dataset in AWS S3.", br(), 
                   "These metrics are based on dimensions recommended in the 2020 Government Data Quality Framework.", br(),
                   strong("The completed data dictionaries and data sources mapping tools can be found ",
          tags$a(href="https://sp.demeter.zeus.gsi.gov.uk/Sites/aa02/elm/evidanaly/Forms/AllItems.aspx?RootFolder=%2FSites%2Faa02%2Felm%2Fevidanaly%2F4%2E9%5FWorkstream%5FAreas%5FModelling%5FStrategy%2F4%2E9%2E3%2E12%5FModelling%5FPartner%2F4%2E9%2E3%2E12%2E1%5FQ12021%5FCapgemini%2F4%2E%20Sprint%203%2FData%20Dictionaries&FolderCTID=0x012000E5A4A7CDBE65E34EAF2C6C179BAB66F6&View=%7B6161011C%2DA4EE%2D4355%2D9406%2D7513026BD08B%7D", 
                 "here"),
          tags$a(href="https://defra.sharepoint.com/sites/MST-Defra-E.L.M.ModellingSubGroup/_layouts/15/Doc.aspx?OR=teams&action=edit&sourcedoc={82A2AD46-3985-494A-A542-E850E2DC8E80}", 
                 "and here"),
          "respectively.")),
          
          #img(src='Defra.png', align = "centre"),
          #img(src='DQHub_white_background-final.png', align = "centre")
          
          ),
      
      fluidRow(
        column(width = 12,
               box(title = strong("Documentation"),
                   width = 12,
                   status = "success",
                   
                   helpText(strong("Full information on the metrics/rules and dimensions of this Data Quality report are available",
                                   tags$a(href="https://sp.demeter.zeus.gsi.gov.uk/Sites/aa02/elm/_layouts/15/WopiFrame.aspx?sourcedoc=/Sites/aa02/elm/evidanaly/4.9_Workstream_Areas_Modelling_Strategy/4.9.3.12_Modelling_Partner/4.9.3.12.1_Q12021_Capgemini/5.%20Sprint%204/Data%20Quality/Data%20Quality%20Feature%20List%20and%20Backlog.docx&action=default", 
                                    "here."), br(), br(),
                                   ),
                            strong("Note, this tool evaluates datasets based on their dataset name ('Dataset') and extension ('FileExt'). 
                                   Dataset names must be unique and the extension must adhere to one of the four file types below")
                                   
                            )))),
      
      fluidRow(
        column(width = 12,
               box(title = strong("Choose your dataset and file type:"),
                   span("Note this tool processes the quality metrics associated with 4 types of data file: CSV, GeoJSON, GeoPackage, GIS Shapefiles (.shx)."),
                   br(),
                   width = 12,
                   status = "success",
                   br(),
                   column(width = 7,
                          
                          pickerInput("Dataset_picker",
                                      "ELMS Dataset",
                                      choices = unique(Data$Dataset),
                                      selected = unique(Data$Dataset)[1],
                                      options = list(`actions-box` = TRUE), multiple = F)), ## F = Do not allow multiple choices 
                   column(width = 5,
                          
                          pickerInput("Dataset_ext",
                                      "File type (extension)",
                                      choices = unique(Data$FileExt),
                                      selected = unique(Data$FileExt)[1],
                                      options = list(`actions-box` = TRUE), multiple = F)), ## F = Do not allow multiple choices 
                   
               ))),
    
      
      ),
    
    ## The UI component of the DQ Report ##
    ## Each row of 'table' provides metrics for each column in a given dataset 
    ## Therefore if we have 'M' DQ metrics and 'N' columns in a given dataset, 
    ## the report/table will have N*(M+1) structure (M+1, includes a column for the column name)
    
    column(width = 6,
    fluidRow(
      width = 12,
      shiny::h3(strong("Metrics at field level (Exportable) - Top 5 rows")),
      DT::dataTableOutput("table"),
  
    ## Outputs metrics for the geometry column and metrics which do not vary across columns 
    ## (eg 'uniqueness' which is a dataframe wide metric)
    
    fluidRow(
      shiny::h3(strong("Metrics associated with data set:")),
      shiny::h4(textOutput("Unique_rows"), style="color:#000000"),
      shiny::h4(textOutput("Contains_geo"), style="color:#000000"),
      shiny::h4(textOutput("geo_polygon"), style="color:#000000"),
      shiny::h4(textOutput("geo_invalid"), style="color:#000000")
    ))),
    
    tags$head(tags$style(HTML('
                                /* body */
                                .content-wrapper, .right-side {
                                background-color: #fff5f5; 
                                }

                                ')))) ##fff5f5 gives a pink hue to the Shiny dashboard 
    
    
    ))



