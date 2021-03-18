### Packages ###

## Data Manipulation and Plotting ##
library(tidyverse)
library(plotly)

## Packages used in Building Shiny App ## 
library(shinydashboard) # can be used with shiny 
library(shiny)
library(shinyWidgets) ## Used for more UI friendly drop down menus ##
library(shinyjs) ## Used to incorporate JS functionality (e.g reset buttons)

source('Data_Landing.R')

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
  
  ## Simple reactive function based on the two selection inputs ##
  Data_filtered <- reactive({
    
    DF <- Data %>% dplyr::filter(Dataset==input$Dataset_picker, FileExt==input$Dataset_ext) %>%
                   dplyr::select(Column, Null.pct, One_character, Data_types, Percent, LastModified, ReportGenerated) %>%
                   arrange(Column) %>% ## Alphabetical order %>%
                   mutate(Null.pct = Rounder(Null.pct, 2))
    
    return(DF)
    
  })
  
  ## Simple reactive function based on the two selection inputs ##
  Text_filtered <- reactive({
    
    DF <- Data %>% dplyr::filter(Dataset==input$Dataset_picker, FileExt==input$Dataset_ext) %>%
                   dplyr::select(Contains_geom, Uniqueness) %>%
                   mutate(Uniqueness = Rounder(Uniqueness, 2)) %>%
  
    
    return(DF)
    
  })
  
  
  output$table  <- DT::renderDataTable(server=FALSE, {
    
    DT::datatable(Dataframe <- Data_filtered() %>%
                  dplyr::rename(`Percent Missing` = Null.pct,
                         `Missing alphanumeric` = One_character,
                         `Data Type` = Data_types,
                         `Data mismatch` = Percent,
                         `Last Modified` = LastModified,
                         `Report Last Generated` = ReportGenerated),
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
       formatStyle('Column', backgroundColor = '#e6f5ff') %>%
      formatStyle('Percent Missing', backgroundColor = '#e6fff9') %>%
      formatStyle('Missing alphanumeric', backgroundColor = '#e6fff9') %>% 
      formatStyle('Data Type', backgroundColor = '#ffe6e6') %>%
      formatStyle('Data mismatch', backgroundColor = '#ffe6e6') %>%
      formatStyle('Last Modified', backgroundColor = '#ffdd99') %>%
      formatStyle('Report Last Generated', backgroundColor = '#ffdd99')
      })
  
  output$Unique_rows <- renderText({
    paste0("Percentage of rows which are unique = ", unique(Text_filtered()$Uniqueness))
  })
  
  output$Contains_geo <- renderText({
    paste0("Data contains a geometry column for spatial analysis = ", unique(Text_filtered()$Contains_geom))
  })
  
  
}
