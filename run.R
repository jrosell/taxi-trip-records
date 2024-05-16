suppressPackageStartupMessages({
    library(DBI)
    library(data.table)
    library(polars)
    library(ggplot2)
    library(tibble)
    library(dplyr)
    library(tidyr)
    library(Rfast)
    library(duckdb)
    library(dtplyr)
    library(patchwork)
    library(arrow)
    library(tidypolars)
    library(duckplyr)
    library(sparklyr)
})
options(
    # sparklyr.log.console = TRUE,
    # sparklyr.simple.errors = TRUE
    timeout = 9999
)
# Source: 2023 & 2022 High Volume For-Hire Vehicle Trip Records https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
parquet_source <- file_name <- "data/fhvhv_tripdata_*.parquet"
# file_name <- "data/fhvhv_tripdata_2023-12.parquet"
file_names <- paste0("data/", list.files("data"))
threads <- 8
data.table::setDTthreads(threads)
Sys.setenv(POLARS_MAX_THREADS = threads)
arrow::set_cpu_count(threads)
duckdb_set_threads <- \(threads) {
    conn <- duckplyr:::get_default_duckdb_connection()
    dbExecute(conn = conn, paste0("PRAGMA threads='", threads, "'"))
    invisible()
}
duckdb_set_threads(8)
if (nrow(sparklyr::spark_installed_versions()) == 0) {
    spark_available_versions(
        show_hadoop = TRUE,
        show_minor = TRUE,
        show_future = TRUE
    )
    # sparklyr::spark_uninstall(version = "2.4.3", hadoop_version = "2.7")
    sparklyr::spark_install(
        version = "3.2.4",
        hadoop_version = "3.2"
    )
}
sc <- sparklyr::spark_connect(master = "local", version = "3.2.4")

res <- bench::mark(
    dplyr_sparklyr = {
        print("dplyr_sparklyr")
        df <- sparklyr::stream_read_parquet(
                sc,
                path = parquet_source
            ) |>
            select(airport_fee, pickup_datetime) |>
            filter(airport_fee > 0) |>
            mutate(day = as.Date(pickup_datetime)) |>
            summarise(count = n(), .by = day) |>
            arrange(desc(count)) |>
            collect()
        print(as_tibble(df), n = Inf)
        df <- NULL
        gc()
    },
    dplyr_arrow = {
        print("dplyr_arrow")
        df <- arrow::open_dataset(file_names) |>
            select(airport_fee, pickup_datetime) |>
            filter(airport_fee > 0) |>
            mutate(day = as.Date(pickup_datetime)) |>
            summarise(count = n(), .by = day) |>
            arrange(desc(count)) |>
            collect()
        print(as_tibble(df), n = Inf)
        df <- NULL
        gc()
    },
    # dplyr_tidypolars = {
    #     print("dplyr_tidypolars")
    #     df <- polars::pl$scan_parquet(parquet_source)  |>
    #         select(airport_fee, pickup_datetime) |>
    #         filter(airport_fee > 0) |>
    #         mutate(day = as.Date(pickup_datetime)) |> # `tidypolars` doesn't know how to translate this function: `as.Date()`
    #         summarise(count = n(), .by = day) |>
    #         arrange(desc(count)) |>
    #         collect()
    #     print(as_tibble(df), n = Inf)
    #     df <- NULL
    #     gc()
    # },
    dplyr_duckdb = {
        print("dplyr_duckdb")
        df <- arrow::open_dataset(file_names) |>
            to_duckdb()  |>
            select(airport_fee, pickup_datetime) |>
            filter(airport_fee > 0) |>
            mutate(day = as.Date(pickup_datetime)) |>
            summarise(count = n(), .by = day) |>
            arrange(desc(count)) |>
            collect()
        print(as_tibble(df), n = Inf)
        df <- NULL
        gc()
    },
    duckplyr = {
        print("duckplyr")
        df <- duckplyr::duckplyr_df_from_parquet(file_names) |>
            select(airport_fee, pickup_datetime) |>
            filter(airport_fee > 0) |>
            mutate(day = as.Date(pickup_datetime)) |>
            summarise(count = n(), .by = day) |>
            arrange(desc(count)) |>
            collect()
        print(as_tibble(df), n = Inf)
        df <- NULL
        gc()
    },
    polars = {
        print("polars")
        df <- polars::pl$scan_parquet(parquet_source)$
            select("airport_fee", "pickup_datetime")$
            filter(pl$col("airport_fee") > 0)$
            with_columns(
                 pl$date(
                    pl$col("pickup_datetime")$dt$year(),
                    pl$col("pickup_datetime")$dt$month(),
                    pl$col("pickup_datetime")$dt$day()
                )$alias("day")
            )$
            group_by(pl$col("day"))$
            agg(pl$count("airport_fee")$alias("count"))$ # $ #   pl$col("random")$count()$alias("count")
            sort(pl$col("count"), descending = TRUE)$
            collect(streaming = TRUE)
        print(as_tibble(df), n = Inf)
        df <- NULL
        gc()
    },
    memory = FALSE,
    filter_gc = FALSE,
    min_iterations = 3,
    check = FALSE
)
spark_disconnect(sc)

print(res)
# p <- ggplot2::autoplot(res) + labs(title = paste(n, "rows"))
p <- ggplot2::autoplot(res, type = "violin")
dir.create(here::here("output"))
pdf(NULL)
ggsave(
    here::here("output", paste0(Sys.Date(), ".png")),
    plot = p
)

BRRR::skrrrahh(13)
sessionInfo()