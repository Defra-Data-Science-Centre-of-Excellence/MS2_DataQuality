### Packages ###

## Data Manipulation and Plotting ##
library(tidyverse)
library(plotly)

## Packages used in Building Shiny App ## 
library(shinydashboard) # can be used with shiny 
library(shiny) 
library(shinyWidgets) ## Used for more UI friendly drop down menus ##
library(shinyjs) ## Used to incorporate JS functionality (e.g reset buttons)

# source('Data_Landing.R')

## Server function builds the actual visuals in the app ##
server <- function (input, output, session) {
  
  ## This adjusts the dataset selection options, conditional on the data extension (e.g. .csv)
  observeEvent(input$Dataset_ext,{

    updatePickerInput(session,
                      "Dataset_picker",
                      choices=unique(na.omit(Data$Dataset[Data$FileExt==input$Dataset_ext])))
  })

  ## Simple function which rounds numbers to a prespecified number of decimal functions ##
  Rounder <- function(j, decimal_place){
    rounded <-  round(j, decimal_place)
    return(rounded)
  }

  ## Simple reactive function based on the two selection inputs in the drop down menus ##
  Data_filtered <- reactive({

    DF <- Data %>% dplyr::filter(Dataset==input$Dataset_picker, FileExt==input$Dataset_ext) %>%
      
                   ## ADD NEW COLUMNS HERE WHEN APPROPRIATE ##
                   dplyr::select(Column, Null.pct, One_character, Data_types, Percent, LastModified, ReportGenerated, ContainsGeometry) %>%
                   arrange(Column) %>% ## Alphabetical order %>%
                   mutate(Null.pct = Rounder(Null.pct, 1)) ## Round to nearest integer 

    return(DF)

  })

## Render the DQ Report ## 
## Note, we are using the DT package here ##
## This package provides an R interface to JavaScript DataTables library ##
## DT allows us to build interactive tables and export them to clipboard or MS Excel ##
  
  output$table  <- DT::renderDataTable(server=FALSE, {

    DT::datatable(Dataframe <- Data_filtered() %>%
                  ## RENAME NEW COLUMNS HERE WHEN APPROPRIATE ##
                  dplyr::rename(`Percent Missing` = Null.pct,
                         `Missing alphanumeric` = One_character,
                         `Data Type` = Data_types,
                         `Data mismatch` = Percent,
                         `Last Modified` = LastModified,
                         `Report Last Generated` = ReportGenerated) %>%
                   dplyr::select
                  (-ContainsGeometry), ## Not required for the table 
                  filter='top', selection="multiple", escape=FALSE, ## Enables column specific searching
                  rownames = FALSE,
                  extensions = 'Buttons',
                  options = list(
                    searching = FALSE, ## Disables globalsearch filter
                    fixedColumns = FALSE,
                    autoWidth = TRUE,
                    dom = 'Bfrtip',
                    buttons = c('copy', 'csv', 'excel'),
                    pageLength = 5,
                    scrollX=TRUE,
                    columnDefs = list(list(className = 'dt-center', targets = "_all"))
                  ),
                  class = "display" #if you want to modify via .css
    ) %>% 
      ## ADD NEW COLUMNS BELOW HERE WHERE APPROPRIATE ##
      formatStyle('Column', backgroundColor = '#ffffff') %>%
      formatStyle('Percent Missing', backgroundColor = '#ffffff') %>%
      formatStyle('Missing alphanumeric', backgroundColor = '#ffffff') %>%
      formatStyle('Data Type', backgroundColor = '#ffffff') %>%
      formatStyle('Data mismatch', backgroundColor = '#ffffff') %>%
      formatStyle('Last Modified', backgroundColor = '#ffffff') %>%
      formatStyle('Report Last Generated', backgroundColor = '#ffffff') %>%
      
      ## RAG RATING ##
      ## ADD NEW RAG RATINGS BELOW HERE WHERE APPROPRIATE ##
      ## https://www.rdocumentation.org/packages/DT/versions/0.17/topics/styleInterval ##
      ## Note, styleInterval((5,10)) means:
      ## if value < 5 --> assign colour #ccffff
      ## if value >=5 and <10 --> assign colour ##ffddcc
      ## if value >=10 --> assign colour #ff8080
      ## These colours use HTML Hex Triplets format (hashtag, followed by 6 characters)
      
      formatStyle(
        'Percent Missing',
        backgroundColor  = styleInterval(c(5, 10), c('#28B463', '#F39C12', '#e74c3c'))
      )
      })
  

  
### Function for rendering outputs = Rendertext ##
## Create a function which returns outputs for geom related inputs and dataframe wide 
## related inputs (e.g. uniqueness) ## 
## For non geospatial data, we will just render an NA 
## The alternative is to create conditional logic on reactive values/dataframes
## which increases the complexity of code 

Rendertext <- function(column, Text){
  DF <- Data %>% dplyr::filter(Dataset==input$Dataset_picker, FileExt==input$Dataset_ext) %>%
    dplyr::select(column) 
  
  if(column=='Uniqueness'){
    DF <- DF %>% mutate(Uniqueness = Rounder(Uniqueness, 0)*100)  ## Round to nearest integer 
  }
  
  return(paste0(Text, unique(DF)))
}
  
### NOTE, NEW COLUMNS SHOULD BE ADDED HERE IF THEY ARE NOT IN THE MAIN DQ REPORT TABLE ##
## THE Rendertext FUNCTION TAKES TWO PARTS = (COLUMN IN 'DATA' DATAFRAME, DESCRIPTION )
output$Unique_rows <- renderText({Rendertext('Uniqueness', 'Percentage of rows which are unique = ')})
  
output$Contains_geo <- renderText({Rendertext('ContainsGeometry', 'Data contains a geometry column for spatial analysis = ')})

output$geo_polygon <- renderText({Rendertext('GeomTypesObserved', 'Geometry Type = ')})

output$geo_invalid <- renderText({Rendertext('InvalidGeometriesAtRows', 'Rows with invalid geometries = ')})

}


