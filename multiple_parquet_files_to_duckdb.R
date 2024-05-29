library(DBI)
library(dplyr)
library(duckdb)
library(arrow)
library(purrr)

file_names <- paste0("data/", list.files("data"))
unlink("fhvhv_tripdata.duckdb")

con <- dbConnect(duckdb(), dbdir = "fhvhv_tripdata.duckdb", read_only = FALSE)
dbListTables(con)

purrr::walk(file_names, \(file) {
    cat(paste0("Processing ", file, "\n"))
    if (!dbExistsTable(con, "tripdata")){
        sql <- paste0("CREATE TABLE tripdata AS SELECT * FROM read_parquet('", file, "');")
        cat(paste0("dbSendQuery for ", sql, "\n"))
        dbSendQuery(con, sql)
    } else {
        sql <- paste0("INSERT INTO tripdata SELECT * FROM read_parquet('", file, "');")
        cat(paste0("dbSendQuery for ", sql, "\n"))
        dbSendQuery(con, sql)
    }
})

dbListTables(con)
dbDisconnect(con)
