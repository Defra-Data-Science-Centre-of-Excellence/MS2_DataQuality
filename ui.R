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

source('Data_Landing.R')

ui <- dashboardPage( ## Using fluid Page > Dashboard page --> One page dashboard 
  
  skin = "green",
  
  dashboardHeader(title = "Data Quality"),
  
  dashboardSidebar(collapsed = TRUE,
                     sidebarMenu()),
  
  dashboardBody(
  fluidRow(
    column(
      width = 5,
      
      box(title = "ELMS Data Quality Tool",
          width = 12,
          solidHeader = TRUE,
          status = "success",
          
          helpText("This tool evaluates data quality metrics for each dataset in AWS S3", br(), 
                   "These metrics are based on dimensions recommended in the 2020 Government Data Quality Framework", br(),
                   strong("The completed data dictionaries and data sources mapping tools can be found ",
          tags$a(href="https://sp.demeter.zeus.gsi.gov.uk/Sites/aa02/elm/evidanaly/Forms/AllItems.aspx?RootFolder=%2FSites%2Faa02%2Felm%2Fevidanaly%2F4%2E9%5FWorkstream%5FAreas%5FModelling%5FStrategy%2F4%2E9%2E3%2E12%5FModelling%5FPartner%2F4%2E9%2E3%2E12%2E1%5FQ12021%5FCapgemini%2F4%2E%20Sprint%203%2FData%20Dictionaries&FolderCTID=0x012000E5A4A7CDBE65E34EAF2C6C179BAB66F6&View=%7B6161011C%2DA4EE%2D4355%2D9406%2D7513026BD08B%7D", 
                 "here"),
          tags$a(href="https://defra.sharepoint.com/sites/MST-Defra-E.L.M.ModellingSubGroup/_layouts/15/Doc.aspx?OR=teams&action=edit&sourcedoc={82A2AD46-3985-494A-A542-E850E2DC8E80}", 
                 "and here"),
          "respectively")),
          
          #img(src='Defra.png', align = "centre"),
          #img(src='DQHub_white_background-final.png', align = "centre")
          
          ),
      
      fluidRow(
        column(width = 12,
               box(title = strong("Documentation"),
                   width = 12,
                   status = "success",
                   
                   helpText(strong("Full information on the metrics/rules and dimensions of this Data Quality report are available",
                                   tags$a(href="https://sp.demeter.zeus.gsi.gov.uk/Sites/aa02/elm/evidanaly/4.9_Workstream_Areas_Modelling_Strategy/4.9.3.12_Modelling_Partner/4.9.3.12.1_Q12021_Capgemini/5.%20Sprint%204/Data%20Quality/Data%20Quality%20Feature%20List.docx?Web=1", 
                                    "here"),
                                   
                            ))))),
      
      fluidRow(
        column(width = 12,
               box(title = strong("Choose your dataset and file type"),
                   width = 12,
                   status = "success",
                   column(width = 7,
                          
                          pickerInput("Dataset_picker",
                                      "ELMS Dataset",
                                      choices = c(unique(Data$Dataset)),
                                      selected = unique(Data$Dataset)[1],
                                      options = list(`actions-box` = TRUE), multiple = F)), ## F = Do not allow multiple choices 
                   column(width = 5,
                          
                          pickerInput("Dataset_ext",
                                      "File type (extension)",
                                      choices = c(unique(Data$Dataset_extension)),
                                      selected = unique(Data$Dataset_extension)[1],
                                      options = list(`actions-box` = TRUE), multiple = F)), ## F = Do not allow multiple choices 
                   
               ))),
    
      
      ),
    
    column(width = 6,
    fluidRow(
      width = 12,
      shiny::h4(strong("Data Quality Report (Exportable) - Top 10 rows")),
      DT::dataTableOutput("table"),
  
    
    
    fluidRow(
      shiny::h3("Dataset level metrics:"),
      shiny::h4(textOutput("Unique_rows"), style="color:#b35900"),
      shiny::h4(textOutput("Contains_geo"), style="color:#009933")
    ))),
    
    tags$head(tags$style(HTML('
                                /* body */
                                .content-wrapper, .right-side {
                                background-color: #fff5f5;
                                }

                                '))))
    
    
    ))



