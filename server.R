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

server <- function (input, output, session) {
  
  observeEvent(input$Dataset_ext,{

    updatePickerInput(session,
                      "Dataset_picker",
                      choices=unique(na.omit(Data$Dataset[Data$FileExt==input$Dataset_ext])))
  })

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
                   mutate(Null.pct = Rounder(Null.pct, 2)) 
                   #select_if(~!all(is.na(.))) ## Removes columns if all the rows are NA (i.e. geometry related columns for non geospatial data)

    return(DF)

  })

## Render the DQ Report ## 
  output$table  <- DT::renderDataTable(server=FALSE, {

    DT::datatable(Dataframe <- Data_filtered() %>%
                  dplyr::rename(`Percent Missing` = Null.pct,
                         `Missing alphanumeric` = One_character,
                         `Data Type` = Data_types,
                         `Data mismatch` = Percent,
                         `Last Modified` = LastModified,
                         `Report Last Generated` = ReportGenerated) %>%
                   dplyr::select(-ContainsGeometry),
                  rownames = FALSE,
                  extensions = 'Buttons',
                  options = list(
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
      formatStyle('Column', backgroundColor = '#e6f5ff') %>%
      formatStyle('Percent Missing', backgroundColor = '#e6fff9') %>%
      formatStyle('Missing alphanumeric', backgroundColor = '#e6fff9') %>%
      formatStyle('Data Type', backgroundColor = '#ffe6e6') %>%
      formatStyle('Data mismatch', backgroundColor = '#ffe6e6') %>%
      formatStyle('Last Modified', backgroundColor = '#ffdd99') %>%
      formatStyle('Report Last Generated', backgroundColor = '#ffdd99')
      })
  
## Function which returns outputs for geom related inputs and dataframe wide related inputs (e.g. uniqueness) ## 
  Output_metrics <- function(column){
    
    DF <- Data %>% dplyr::filter(Dataset==input$Dataset_picker, FileExt==input$Dataset_ext) %>%
                   dplyr::select(column) 
    
    if(column=='Uniqueness'){
    DF <- DF %>% mutate(Uniqueness = Rounder(Uniqueness, 2)) 
    }
      
    return(DF)
    
  }

## Output other metrics ##
## For non geospatial data, we will just render an NA 
  
## The alternative is to create conditional logic on reactive values/dataframes
## which increases the complexity of code 
  
### Function for rendering text ##

Rendertext <- function(column, Text){
  DF <- Data %>% dplyr::filter(Dataset==input$Dataset_picker, FileExt==input$Dataset_ext) %>%
    dplyr::select(column) 
  
  if(column=='Uniqueness'){
    DF <- DF %>% mutate(Uniqueness = Rounder(Uniqueness, 2)) 
  }
  
  return(paste0(Text, unique(DF)))
}
  
### NOTE, NEW COLUMNS SHOULD BE ADDED HERE IF THEY ARE NOT IN THE MAIN DQ REPORT TABLE ##
## THE Rendertext FUNCTION TAKES TWO PARTS = (COLUMN, DESCRIPTION)
output$Unique_rows <- renderText({Rendertext('Uniqueness', 'Percentage of rows which are unique = ')})
  
output$Contains_geo <- renderText({Rendertext('ContainsGeometry', 'Data contains a geometry column for spatial analysis = ')})

output$geo_polygon <- renderText({Rendertext('GeomTypesObserved', 'Geometry Type =')})

output$geo_invalid <- renderText({Rendertext('InvalidGeometriesAtRows', 'Any invalid geometries? = ')})

}


