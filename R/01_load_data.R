library(tidyverse)
library(httr)
library(glue)
library(lubridate)


#' Retrieve bike counter data
#'
#' @description Retrieve bike counter data from eco-visio.net.
#' @param domain The town to pull the data for. 677 is Cologne, Germany.
#' @param from Start of date range to pull the data. Default is earliest date available.
#' @param from End of date range to pull the data. Default is latest date available.
#' @param interval The time interval to pull the data for.
#'     Available intervals: "1 hour", "4 hours", "1 day", "1 week". Default: "1 day".
#' @param output_format "list" or "df" (data frame). Default: "list"
#'
#' @return A list of counter infos and counter data (output_format = "list") 
#'     or a dataframe with counter infos and data merged.
#' @export
#'
#' @examples
retrieve_counter_data <- function(domain,
                                  from = NULL, to = NULL,
                                  interval = c("1 day", "1 hour", "4 hours", "1 week"),
                                  output_format = c("list", "df")) {
  
  if (missing(interval)) interval <- "1 day"
  if (missing(output_format)) output_format <- "list"
  
  
  # domain <- 677
  # interval <- "1 day"
  # output_format <- "list"
  
  # retrieve counter list 
  message("Retrieving counter list for domain.")
  counter_list_url <- glue("https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpageplus/{domain}?withNull=true")
  counter_list_raw <- GET(counter_list_url) %>% 
    content()
  
  # filter a subset of counter features
  keys_to_keep <- c("idPdc", "cumulFlowId", "lat", "lon", "nom", "debut", "fin", 
                    "externalUrl", "nomOrganisme", "pays", "today", "lastDay", "moyD")
  counter_list <- map(counter_list_raw, `[`, keys_to_keep)
  
  counters_info <- map(counter_list_raw, `[`, keys_to_keep) %>% 
    bind_rows() %>% 
    rename(name = nom, begin = debut, organisation_name = nomOrganisme, country = pays) %>% 
    mutate(across(c(begin, today), parse_date, "%d/%m/%Y"),
           across(c(begin, today), format, "%Y%m%d"),
           idPdc = as.character(idPdc))
  
  # set start and end date
  if (!missing(from)) {
    if (class(from) == "Date") {
      counters_info$begin <- format(from, "%Y%m%d")
    } else {
      warning("Argument `from` must be a Date. Retrieving all data.")
    }
  }  
  if (!missing(to)) {
    if (class(end) == "Date") {
      if (end > date) {
        counters_info$end <- format(to,  "%Y%m%d")        
      } else{
        warning("Argument `end` must be a date after `from`. Retrieving data up until today.")
      }
    } else {
      warning("Argument `end` must be a Date. Retrieving all data.")
      counters_info$end <- counters_info$today
    }
  }
    
  # get the ids of the counters
  counter_ids <- map_chr(counter_list, `[[`, "idPdc")
  
  message("Get authorization tokens for counters.")
  # send request to get tokens for authorization for each counter
  public_url_for_token <- glue("https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpage/{counter_ids}")
  tokens <- map(public_url_for_token, GET) %>% 
    map(content) %>% 
    map_chr(pluck, "token")
  
  # urls to the data from the counters
  step <- switch(interval,
                 "1 hour" = 1,
                 "4 hours" = 2,
                 "1 day" = 4,
                 "1 week" = 5)
  
  counter_data_urls <- counters_info %>% 
    bind_cols(token = tokens) %>% 
    mutate(url = glue("https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpage/data/{cumulFlowId}?begin={begin}&end={today}&step={step}&domain={domain}&withNull=true&t={token}")) %>% 
    pull()
  
  # retrieve counter data
  message("Retrieving counter data from API. This might take a while.")
  responses <- map(counter_data_urls, GET)
  responses <- set_names(responses, counter_ids)
  
  message("Transforming counter data. This might take a while.")
  content <- map(responses, content)
  # dplyr::bind_rows is slow, use vctrs::vec_bind instead (https://github.com/tidyverse/dplyr/issues/5370) 
  # counters_data <- map(content, possibly(bind_rows, otherwise = NULL))
  counters_data <- map(content, 
                       ~vctrs::vec_rbind(!!!.x) %>% 
                         unnest(cols = c(date, comptage, timestamp)))
  
  message("Preparing counter data.")
  prepare_df <- function(df, name) {
    df %>%
      mutate(date = as_date(date),
             id = name) %>%
      select(id, date, timestamp, count = comptage) 
  }
  
  counters_data <- map2(counters_data, names(counters_data), prepare_df)
  
  if (output_format == "df") {
    result <- bind_rows(counters_data) %>% 
      inner_join(counters_info, by = c("id" = "idPdc"))
  } else {
    result <- list("info" = counters_info, "data" = counters_data)
  }
  result
}


# ID for Cologne
domain <- "677"
counters <- retrieve_counter_data(domain, output_format = "list")
write_rds(counters, file.path("data", "counters_677_daily.rds"))

library(tictoc)
tic()
counters_hourly <- retrieve_counter_data(domain, interval = "1 hour", output_format = "list")
write_rds(counters_hourly, file.path("data", "counters_677_hourly.rds"))
toc()

counters <- retrieve_counter_data(domain, from = as_date("2020-01-01"))
