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
  
  Rounder <- function(j, decimal_place){
    rounded <-  round(j, decimal_place)
    return(rounded)
  }
  
  ## Simple reactive function based on the two selection inputs ##
  Data_filtered <- reactive({
    
    DF <- Data %>% dplyr::filter(Dataset==input$Dataset_picker, Dataset_extension==input$Dataset_ext) %>%
                   dplyr::select(Column, NA_pct, One_character, Data_types, Percent, `Last Modified`) %>%
                   arrange(Column) %>% ## Alphabetical order %>%
                   mutate(NA_pct = Rounder(NA_pct, 2))
    
    ## TO DO: Rename column headers ##
    
    return(DF)
    
  })
  
  ## Simple reactive function based on the two selection inputs ##
  Text_filtered <- reactive({
    
    DF <- Data %>% dplyr::filter(Dataset==input$Dataset_picker, Dataset_extension==input$Dataset_ext) %>%
                   dplyr::select(Contains_geom, Uniqueness) %>%
                   mutate(Uniqueness = Rounder(Uniqueness, 2)) %>%
  
    
    return(DF)
    
  })
  
  
  output$table  <- DT::renderDataTable(server=FALSE, {
    
    DT::datatable(Data_filtered(), 
                  rownames = FALSE,
                  extensions = 'Buttons',
                  options = list(
                    fixedColumns = FALSE,
                    autoWidth = TRUE,
                    dom = 'Bfrtip',
                    buttons = c('copy', 'csv', 'excel'),
                    pageLength = 10,
                    scrollX=TRUE,
                    columnDefs = list(list(className = 'dt-center', targets = "_all"))
                  ),
                  class = "display" #if you want to modify via .css
    ) %>%
      formatStyle('Column', backgroundColor = '#e6f5ff') %>%
      formatStyle('NA_pct', backgroundColor = '#e6fff9') %>%
      formatStyle('One_character', backgroundColor = '#e6fff9') %>% 
      formatStyle('Data_types', backgroundColor = '#ffe6e6') %>%
      formatStyle('Percent', backgroundColor = '#ffe6e6') %>%
      formatStyle('Last Modified', backgroundColor = '#ffdd99')  

      })
  
  output$Unique_rows <- renderText({
    paste0("Percentage of rows which are unique = ", unique(Text_filtered()$Uniqueness))
  })
  
  output$Contains_geo <- renderText({
    paste0("Data contains a geometry column for spatial analysis = ", unique(Text_filtered()$Contains_geom))
  })
  
  
}


## To do:
## Observe Event for file types --> file names 