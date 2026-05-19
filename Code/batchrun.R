# PRIME batch run
# scenario 1: Vaccinating 14-year-old 2006-2024, real-world coverage from WHO website
# scenario 2 (what-if): Vaccinating 14-year-old 2006-2024, 90% coverage
library(data.table)
library(readr)
library(doParallel)
library(prime)

setDTthreads(threads = parallel::detectCores())

# Read real world data
batch_data <- read_csv("data/realworld_coverage_firstdose.csv")
batch_data <- as.data.table(batch_data)

batch_run_obs <- batch_data[, .(
  country_code,
  year,
  age_first,
  age_last,
  coverage
)]

batch_run_obs[, `:=`(
  year      = as.integer(year),
  age_first = as.integer(age_first),
  age_last  = as.integer(age_last),
  coverage  = as.numeric(coverage)
)]

exclude <- c("MCO", "NRU") # missing demographics

batch_run_obs <- batch_run_obs[
  !country_code %in% exclude
]

# 1. Real world scenario run
cat("Running OBSERVED rollout scenario...\n")

RegisterBatchData(batch_run_obs, force = TRUE)
.data.batch[, vaccine := "4vHPV"]

cl <- makeCluster(detectCores())
registerDoParallel(cl)

results_observed <- BatchRun(
  countries                       = -1,
  coverage                        = -1,
  agevac                          = -1,
  agecohort                       = -1,
  sens                            = -1,
  year_born                       = -1,
  year_vac                        = -1,
  runs                            = 1,
  vaccine_efficacy_beforesexdebut = 1,
  vaccine_efficacy_aftersexdebut  = 0,
  log                             = -1,
  by_calendaryear                 = FALSE,   # change to TRUE if reporting results by year of impact
  use_proportions                 = TRUE,
  analyseCosts                    = FALSE,
  psa                             = 0,
  psa_vals                        = ".data.batch.psa",
  unwpp_mortality                 = TRUE,
  disability.weights              = "gbd_2017",
  canc.inc                        = "2020",
  vaccine                         = "4vHPV"
)

stopCluster(cl)

dir.create("results", showWarnings = FALSE)

run_date <- format(Sys.Date(), "%Y-%m-%d")

out_file <- paste0(
  "results/realworld_",
  run_date,
  ".csv"
)

fwrite(results_observed, out_file)


# 2. What if scenario run
cat("Running WHAT-IF scenario (2006–2024, age 14, 90%)...\n")

batch_data <- fread("data/coverageallcountry.csv")

batch_whatif <- batch_data[, .(
  country_code,
  year,
  age_first,
  age_last,
  coverage
)]

batch_whatif[, `:=`(
  year      = as.integer(year),
  age_first = as.integer(age_first),
  age_last  = as.integer(age_last),
  coverage  = as.numeric(coverage)
)]

countries <- unique(batch_whatif$country_code)

exclude <- c("MCO","NRU")
# "MCO" and "NRU": ask 1 failed - "Not all values have the required length"

countries_clean <- setdiff(countries, exclude)

batch_whatif_clean <- batch_whatif[!country_code %in% exclude]

RegisterBatchData(batch_whatif_clean, force = TRUE)
.data.batch[, vaccine := "4vHPV"]


cl <- makeCluster(detectCores())
registerDoParallel(cl)

results_whatif <- BatchRun(
  countries                       = -1,
  coverage                        = -1,
  agevac                          = -1,
  agecohort                       = -1,
  sens                            = -1,
  year_born                       = -1,
  year_vac                        = -1,
  runs                            = 1,
  vaccine_efficacy_beforesexdebut = 1,
  vaccine_efficacy_aftersexdebut  = 0,
  log                             = -1,
  by_calendaryear                 = FALSE,   # change to TRUE if reporting results by year of impact
  use_proportions                 = TRUE,
  analyseCosts                    = FALSE,
  psa                             = 0,
  psa_vals                        = ".data.batch.psa",
  unwpp_mortality                 = TRUE,
  disability.weights              = "gbd_2017",
  canc.inc                        = "2020",
  vaccine                         = "4vHPV"
)

stopCluster(cl)

dir.create("results", showWarnings = FALSE)

run_datetime <- format(Sys.time(), "%Y-%m-%d")

out_file_whatif <- paste0(
  "results/whatif_",
  run_datetime,
  ".csv"
)

fwrite(results_whatif, out_file_whatif)
