# Load required libraries
library(bigrquery)
library(curl)
library(dplyr)
library(lubridate)
library(httpuv)
library(base)
library(googledrive)
library(stringr)
library(tidyr)
library(readxl)
unloadNamespace("plyr")

#################################### 
#################################### 
# CONVOSO
#################################### 
#################################### OVERWRITES

# change path for daily call report drop 
upload <- read.csv('/Users/birdieligos/Downloads/log_107910_t30c9p1xfgn6hlxtsih7sig2kqw4pow9_2025-06-13_18_22_14.csv', stringsAsFactors = FALSE)

# format
colnames(upload) <- gsub("\\.", "_", colnames(upload))

upload <- upload %>%
  mutate(
    ts   = ymd_hms(Time_Stamp__PST_),
    Date = as.Date(ts),
    Time = format(ts, "%H:%M:%S")
  ) %>%
  select(-Time_Stamp__PST_, -ts)

upload <- upload %>%
  mutate(
    ts   = ymd_hms(Final_Reached_At),
    Final_Reach_Date = as.Date(ts),
    Final_Reach_Time = format(ts, "%H:%M:%S")
  ) %>%
  select(-Final_Reached_At, -ts)

upload <- upload %>%
  mutate(
    ts   = ymd_hms(Created_At__Time_),
    Created_Lead_Date = as.Date(ts),
    Created_Lead_Time = format(ts, "%H:%M:%S")
  ) %>%
  select(-Created_At__Time_, -ts)

upload <- select(upload, Date, Lead_ID, Number_Dialed, Status_Name, Talk_Time, Cost, Outbound_Called_Count,
                 Campaign_Name,	List_Name, Final_Reach_Date, Final_Reach_Time, Created_Lead_Date,
                 Created_Lead_Time, PROGRAM_RECOGNITION, SCE_CUSTOMER, WORKSHOP_INTEREST)

upload$PLATFORM <- 'Convoso'

# ### calendly temp conversion grab 
# 
# reg <- read.csv('/Users/birdieligos/Downloads/events-export 87.csv', stringsAsFactors = FALSE)
# 
# colnames(reg) <- gsub("\\.", "_", colnames(reg))
# 
# colnames(reg) <- gsub("_+", "_", colnames(reg))
# 
# reg$Text_Reminder_Number <- gsub("\\D+", "", reg$Text_Reminder_Number)
# 
# reg$Text_Reminder_Number <- sub("^1", "", reg$Text_Reminder_Number)


# ### get converted workshops 
# upload_fix <- select(upload, -WORKSHOP_INTEREST)
# 
# reg_fix <- mutate(reg, Number_Dialed = as.numeric(Text_Reminder_Number))
# 
# reg_fix <- reg_fix %>%
#   mutate(Date = as.Date(Event_Created_Date_Time, format = "%Y-%m-%d %I:%M %p"))
# 
# reg_fix <- select(reg_fix, Number_Dialed, Date)
# 
# reg_fix$WORKSHOP_INTEREST <- 'Yes to workshop'
# 
# reg_fix <- reg_fix %>% 
#   distinct()
# 
# ### convoso merge 
# 
# convoso_upload <- left_join(upload_fix, reg_fix)
# 
# # clean 
# convoso_upload <- mutate(convoso_upload, Lead_ID = as.character(Lead_ID))
# convoso_upload <- mutate(convoso_upload, Number_Dialed = as.character(Number_Dialed))
# 
# cols <- c("PROGRAM_RECOGNITION", "SCE_CUSTOMER", "WORKSHOP_INTEREST")
# 
# for (col in cols) {
#   if (is.logical(convoso_upload[[col]])) {
#     convoso_upload[[col]] <- as.character(convoso_upload[[col]])
#   }
# }


################# OVERWRITES write big query table 
library(bigrquery)

# Set up the BigQuery table reference
bq_table <- bq_table(project = "slstrategy", dataset = "EMPOWER_2025", table = "CONVOSO_REPORTRAW")

# Append data to the BigQuery table
bq_table_upload(bq_table, upload, write_disposition = "WRITE_TRUNCATE")


#################################### 
#################################### 
#PDI
#################################### 
#################################### OVERWRITES

# change path for daily PDI report drop 
upload <- read.csv('/Users/birdieligos/Downloads/empower_060225canvassfinal.csv', stringsAsFactors = FALSE)

# format
colnames(upload) <- gsub("\\.", "_", colnames(upload))

colnames(upload) <- gsub("_+", "_", colnames(upload))

upload <- upload %>%
  rename(
    IMPRESSIONS = DOORSKNOCKED,
    REACH = CONTACTS,
    REBATE_INTEREST = ARE_YOU_INTERESTED_IN_APPLYING_FOR_A_REBATE_RIY
  )

upload$AUDIENCE <- "80% or Below AMI"
upload$GEOGRAPHY <- "SECTOR 2"
upload$PLATFORM <- "PDI"
upload$LANGUAGE <- "Bilingual"

print(colnames(upload))

upload <- upload %>%
  rename(PROGRAM_RECOGNITION = ARE_YOU_FAMILIAR_WITH_EMPOWER_GATEWAY_BAY)

upload <- select(upload, DATE, IMPRESSIONS, REACH, PROGRAM_RECOGNITION,
                 REBATE_INTEREST, AUDIENCE, GEOGRAPHY, LANGUAGE, PLATFORM)

############################## OVERWRITES write big query table 
library(bigrquery)

# Set up the BigQuery table reference
bq_table <- bq_table(project = "slstrategy", dataset = "EMPOWER_2025", table = "PDI_REPORTRAW")

# Append data to the BigQuery table
bq_table_upload(bq_table, upload, write_disposition = "WRITE_TRUNCATE")
########################################
########################################
# TEXTING DATA
######################################## 
######################################## APPENDS

# Vector of only the highlighted file paths (replace with the exact filenames you selected)
highlighted_files <- c(
  "/Users/birdieligos/Downloads/extended_report_121738.xlsx",
  "/Users/birdieligos/Downloads/extended_report_121737.xlsx",
  "/Users/birdieligos/Downloads/extended_report_121687.xlsx",
  "/Users/birdieligos/Downloads/extended_report_121686.xlsx",
  "/Users/birdieligos/Downloads/extended_report_121487.xlsx",
  "/Users/birdieligos/Downloads/extended_report_121486.xlsx",
  "/Users/birdieligos/Downloads/extended_report_121247.xlsx",
  "/Users/birdieligos/Downloads/extended_report_121246.xlsx",
  "/Users/birdieligos/Downloads/extended_report_120843.xlsx",
  "/Users/birdieligos/Downloads/extended_report_120842.xlsx",
  "/Users/birdieligos/Downloads/extended_report_120841.xlsx",
  "/Users/birdieligos/Downloads/extended_report_120840.xlsx",
  "/Users/birdieligos/Downloads/extended_report_120837.xlsx",
  "/Users/birdieligos/Downloads/extended_report_120836.xlsx",
  "/Users/birdieligos/Downloads/extended_report_120784.xlsx",
  "/Users/birdieligos/Downloads/extended_report_120783 (1).xlsx"
)

process_file <- function(path) {
  text_ext <- read_excel(path, sheet = "Dialogs") %>%
    select(phone, ts)
  
  list_name <- read_excel(path, sheet = "Totals", range = "B2", col_names = FALSE)[[1,1]]
  
  text_ext %>%
    mutate(
      LIST_NAME    = list_name,
      LANGUAGE     = case_when(
        str_detect(LIST_NAME, regex("ENGLISH", ignore_case = TRUE)) ~ "English",
        str_detect(LIST_NAME, regex("SPANISH", ignore_case = TRUE)) ~ "Spanish",
        TRUE                                                         ~ "English"
      ),
      GEOGRAPHY     = "Sector 1",
      AUDIENCE      = "80% or Below AMI",
      PLATFORM      = "Teletown Hall",
      CAMPAIGN_NAME = "emPOWER Gateway",
      DATE          = as.Date(ymd_hms(ts)),
      TIME          = format(ymd_hms(ts), "%H:%M:%S")  # time only
    ) %>%
    select(-ts) %>%
    rename(NUMBER_DIALED = phone)
}

combined <- purrr::map_dfr(highlighted_files, process_file)

text_ext <- combined %>%
  select(-LIST_NAME)


############################## APPENDS write big query table 
library(bigrquery)

# Set up the BigQuery table reference
bq_table <- bq_table(project = "slstrategy", dataset = "EMPOWER_2025", table = "TTH_REPORTRAW")

# Append data to the BigQuery table
bq_table_upload(bq_table, text_ext, write_disposition = "WRITE_APPEND")


########################################
########################################
# TINY URL TEXTING LINK CLICK DATA
######################################## (OVERWRITES)
########################################

# bring in tiny url metrics in order to get text conversions (link clicks)
url <- read.csv('/Users/birdieligos/Downloads/tinyurl_20250604_070522.csv', stringsAsFactors = FALSE)

url$Date <- as.Date(ymd_hms(url$timestamp))

url <- url %>%
  filter(str_detect(tinyurl, regex("empower", ignore_case = TRUE)))

url <- filter(url, bot == 'false')

url$PLATFORM <- 'Tiny URL'

############################## OVERWRITES write big query table 
library(bigrquery)

bq_table <- bq_table(project = "slstrategy", dataset = "EMPOWER_2025", table = "TINYURL_REPORTRAW")

bq_table_upload(bq_table, url, write_disposition = "WRITE_TRUNCATE")



