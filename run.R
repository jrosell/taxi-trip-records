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
    library(purrr)
    library(testthat)
})
options(
    # sparklyr.log.console = TRUE,
    # sparklyr.simple.errors = TRUE
    timeout = 9999
)
c("data", "output") |> 
    here::here() |>
    walk(\(x) dir.create(x, showWarnings = FALSE))
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
# if (nrow(sparklyr::spark_installed_versions()) == 0) {
#     spark_available_versions(
#         show_hadoop = TRUE,
#         show_minor = TRUE,
#         show_future = TRUE
#     )
#     # sparklyr::spark_uninstall(version = "2.4.3", hadoop_version = "2.7")
#     sparklyr::spark_install(
#         version = "3.2.4",
#         hadoop_version = "3.2"
#     )
# }
# sc <- sparklyr::spark_connect(master = "local", version = "3.2.4")
# Sys.sleep(10)

res <- bench::mark(
    # dplyr_sparklyr = {
    #     print("dplyr_sparklyr")
    #     df <- sparklyr::stream_read_parquet(
    #             sc,
    #             path = parquet_source
    #         ) |>
    #         select(airport_fee, pickup_datetime) |>
    #         filter(airport_fee > 0) |>
    #         mutate(day = as.Date(pickup_datetime)) |>
    #         summarise(count = n(), .by = day) |>
    #         arrange(desc(count)) |>
    #         collect() |>
    #         as_tibble()
    #     print(df, n = Inf)
    #     gc()
    #     dplyr_sparklyr <- df
    # },
    dplyr_arrow = {
        print("dplyr_arrow")
        df <- arrow::open_dataset(file_names) |>
            select(airport_fee, pickup_datetime) |>
            filter(airport_fee > 0) |>
            mutate(day = as.Date(pickup_datetime)) |>
            summarise(count = n(), .by = day) |>
            collect() |> 
            as_tibble() |> 
            mutate(
                day = as.character(day),
                count = as.integer(count)
            ) |> 
            arrange(desc(count), day)
        print(df, n = Inf)
        gc()
        dplyr_arrow <- df
    },
    polars_lazy = {
        print("polars_lazy")
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
            agg(pl$count("day")$alias("count"))$ # $ #   pl$col("random")$count()$alias("count")
            collect(streaming = TRUE) |>
            as_tibble() |> 
            mutate(
                day = as.character(day),
                count = as.integer(count)
            ) |> 
            arrange(desc(count), day)
        print(df, n = Inf)
        gc()
        polars_lazy <- df
    },
    dplyr_duckdb = {
        print("dplyr_duckdb")
        df <- arrow::open_dataset(file_names) |>
            select(airport_fee, pickup_datetime) |>
            filter(airport_fee > 0) |>
            mutate(day = as.Date(pickup_datetime)) |>
            summarise(count = n(), .by = day) |>
            to_duckdb()  |>
            collect() |> 
            as_tibble() |> 
            mutate(
                day = as.character(day),
                count = as.integer(count)
            ) |> 
            arrange(desc(count), day)
        print(df, n = Inf)
        gc()
        dplyr_duckdb <- df
    },
    dplyr_duckplyr = {
        print("dplyr_duckplyr")
        df <- duckplyr::duckplyr_df_from_parquet(parquet_source) |>
            select(airport_fee, pickup_datetime) |>
            filter(airport_fee > 0) |>
            mutate(day = as.Date(pickup_datetime)) |>
            summarise(count = n(), .by = day) |>
            collect() |> 
            as_tibble() |> 
            mutate(
                day = as.character(day),
                count = as.integer(count)
            ) |> 
            arrange(desc(count), day)
        print(df, n = Inf)
        gc()
        dplyr_duckplyr <- df
    },
    memory = TRUE,
    filter_gc = FALSE,
    min_iterations = 10,
    check = TRUE
)
# spark_disconnect(sc)

print(res)
# p <- ggplot2::autoplot(res, type = "violin")
p <- ggplot2::autoplot(res, type = "jitter")
pdf(NULL)
ggsave(
    here::here("output", paste0(Sys.Date(), ".png")),
    plot = p
)

BRRR::skrrrahh(13)
sessionInfo()