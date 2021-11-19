# remove.packages('htmltools')
# install.packages(c(
#   'sass',
#   'bslib',
#   'htmltools',
#   'aws.ec2metadata',
#   'aws.s3',
#   'aws.signature',
#   'plotly',
#   'shinydashboard',
#   'shinyjs',
#   'shinyWidgets'
# ))
# file.copy('/dbfs/mnt/labr/DSET/DataQuality.rds', 'data/DataQuality.rds')


library('shiny')
library('shinyWidgets')
library('DT')
library('dplyr')


data <- readRDS('data/DataQuality.rds')
colgs <- c('Dataset Meta', 'Column Meta', 'Geometry Meta')
cols <- list(
  c('Dataset Name', 'Filepath', 'File Extension', 'File Size (Bytes)', 'Date Modified', 'Report Time', 'Number of Columns', 'Number of Rows', 'Completeness', 'Uniqueness'),
  c('Column Name', 'Data Type', 'Complete', 'Unique', 'Contains AlphaNumeric'),
  c('Coordinate Reference System', 'Geometry Types', 'Geometry Validity', 'Geometry Points')
)


ui <- {fluidPage(
  withMathJax(),
  markdown("
    <style>
      td, th {
        border-bottom: 1px solid #eee;
        padding-left: .5rem;
        padding-right: .5rem;
      }
      th {
        height: 2em;
        border-width: 2px;
      }
      tr:hover {
        background-color: #eee;
      }
    </style>

    # Data Qualities
    Discover more about data you're interested in.
    Contains CDAP data:  /dbfs/mnt/landingr/General Access/
    More information and code on [GitHub](https://github.com/Defra-Data-Science-Centre-of-Excellence/MS2_DataQuality).

    |      | Definition | Equation |
    | :--- | :--------- | -------: |
    | Dataset Name |
    | Column Name |
    | Filepath |
    | File Extension |
    | File Size (Bytes) |
    | Date Modified |
    | Report Time | does not update unless the file is modified |
    | Data Type | *'object' usually means string, often factors* |
    | Number of Columns |
    | Number of Rows |
    | Completeness | normalised 'Complete' for data table | mean(c_i / n) |
    | Uniqueness | normalised 'Unique' for data table | mean(u_i / n} |
    | Complete | none missing values (NA) in the data series | c_i |
    | Unique | unique values in the data series | u_i |
    | Contains AlphaNumeric | *similar to OneCharacter* |
    | Coordinate Reference System |
    | Geometry Types | set of all simple feature geometry types recognised |
    | Geometry Validity | OGR validity check |
    | Geometry Points | Number of coordinates in the geometry |

    #### Examples
    Search *'geometry'* to find all the geospatial columns.
    Or search *'status of bathing'* to find that specific column.
    Or filter columns to unique variants, such as just *'Filepath'* to find the individual files.
  "),
  hr(),
  fluidRow(
    column(width=6,
      pickerInput('cols', 'Filter Columns', choices=names(data), selected=names(data), options=list(`actions-box`=TRUE), multiple=TRUE),
    ),
    column(width=6,
      pickerInput('colgs', 'Filter Columns by Groups', choices=colgs, selected=colgs, options=list(`actions-box`=TRUE), multiple=TRUE),
    )
  ),
  hr(),
  dataTableOutput('table')
)}


server <- function(input, output, session) {
  observeEvent(input$colgs, {
    selected <- c()
    for (i in 1:length(colgs))
      if (!is.na(match(colgs[i], input$colgs)))
        selected <- c(selected, cols[[i]])
    updatePickerInput(session, 'cols', selected=selected)
  })

  output$table = renderDataTable(
    data %>%
      select(input$cols) %>%
      distinct(),
    rownames = FALSE,
    options = list(
      pageLength = 25
      #scrollX = TRUE
    )
  )
}


shinyApp(ui, server)
