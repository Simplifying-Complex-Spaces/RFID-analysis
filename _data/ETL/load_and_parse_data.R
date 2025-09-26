here::i_am('_data/ETL/load_and_parse_data.R')
library(here)

library(roxygen2)
library(tidyverse)
library(DBI)

rm(list = ls())

db_name <- '20250806_RFID.db'
db_path <- here('_data', db_name) 
table_name <- 'RFID'

#' Load all the data for the specified file pattern
#'
#' @param file_path
#' @param col_to_bindA
#' @param col_to_bindB
load_the_data <- function(file_path, col_to_bindA, col_to_bindB) {
    # Use OS-level file matching to get all the files that match the given file pattern
    the_data <- lapply(Sys.glob(file_path), read_csv, col_names = FALSE)
    the_data <- dplyr::bind_rows(the_data)
    new_colA <- rep(col_to_bindA, nrow(the_data))
    new_colB <- rep(col_to_bindB, nrow(the_data))
    the_data <- cbind(the_data, new_colA)
    the_data <- cbind(the_data, new_colB)
    names(the_data) <- c(
        "time",
        "bending",
        "head_to_toe",
        "left_to_right",
        "sensor_id",
        "signal_strength",
        "phase",
        "frequency",
        "activity_class",
        "location",
        "gender"
    )
    return(the_data)
}



# Load the data
room1_men_data <- load_the_data(here('_data/src data/Datasets_Healthy_Older_People/S1_Dataset/*M'), "one", "male") #room one has four RFID sensors
room2_men_data <- load_the_data(here('_data/src data/Datasets_Healthy_Older_People/S2_Dataset/*M'), "two", "male") #room two has three RFID sensors
room1_wommen_data <- load_the_data(here('_data/src data/Datasets_Healthy_Older_People/S1_Dataset/*F'), "one", "female") #room one has four RFID sensors
room2_wommen_data <- load_the_data(here('_data/src data/Datasets_Healthy_Older_People/S2_Dataset/*F'), "two", "female") #room two has three RFID sensors

# Combine the data
all_data <- dplyr::bind_rows(room1_men_data, room2_men_data, room1_wommen_data, room2_wommen_data)
# Free up some memory
rm(room1_men_data, room2_men_data, room1_wommen_data, room2_wommen_data)

all_data <- dplyr::as_tibble(all_data)

# Relocate the activity class to the end
all_data <- all_data %>% dplyr::relocate("activity_class", .after = last_col())

# Create an identifier col we'll use in the db and add it in 
row_id <- c(seq(1,nrow(all_data),1))
all_data <- add_column(all_data, row_id, .before = 'time')

# Open a connection to the database
src_db <- DBI::dbConnect(RSQLite::SQLite(), db_path)

# Write all_data to the database
dbWriteTable(src_db, name=table_name, value=all_data,
             field.types=c(
                 "row_id" = "INTEGER",
                 "time" = "DOUBLE",
                 "bending" = "DOUBLE",
                 "head_to_toe" = "DOUBLE",
                 "left_to_right" = "DOUBLE",
                 "sensor_id" = "INTEGER",
                 "signal_strength" = "DOUBLE",
                 "phase" = "DOUBLE",
                 "frequency" = "DOUBLE",
                 "location" = "TEXT",
                 "gender" = "TEXT", 
                 "activity_class" = "INTEGER"
             ))

# Close the connection to the database
DBI::dbDisconnect(conn = src_db)
