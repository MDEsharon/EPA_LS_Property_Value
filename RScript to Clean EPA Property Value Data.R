# Data Cleaning Process

#----------------------------------------------------------------
## 1. Marsh data ##
#----------------------------------------------------------------

#Setting working directory
setwd("C:/Users/szhang/Desktop/EPA Property Value Data")

#install.packages("dplyr")
#install.packages("stringr")

#Irrelevant columns are already deleted prior to loading it into R
#Column names are changed prior to loading it in R
#Columns sorted by oldest to newest licenses

#Loading the data
marsh <- read.csv("marsh_data.csv")

#Data exploration
#----------------
#Started off with 1,479 observations
summary(marsh)
str(marsh)

#Data transformation
#-------------------
#Store approval issue dates as dates
marsh$approval_issued <- as.Date(marsh$approval_issued, format = "%m/%d/%y")

#Some zip codes include ZIP+4 codes. Extracting only the 5-digit codes and changing the column to integers
marsh$postal_code <- substr(marsh$postal_code, 1, 5)
marsh$postal_code <- as.integer(marsh$postal_code)

#Handling irrelevant and duplicate data
#--------------------------------------
#Delete projects with WQC, NT, and PR licenses as they are not relevant to us. Deleted 9 observations. 1,470 observations left.
#These projects were double checked. PR licenses are actually meant to be GL and fixed in ETS. And the WQC and NT licenses have corresponding
#WL licenses that were already included in the spreadsheet.
irrelevant_data <- subset (marsh, grepl("WQC|NT|PR", permit_no))
marsh <- subset(marsh, grepl("WL|GL|WP|GP", permit_no) & !grepl("WQC|NT|PR", permit_no))

#Checking for duplicate marsh dimension values
duplicate_rows <- duplicated(marsh[c("master_ai_id", "Fill_Cubic_Yards", "High_Marsh_Created_Acres", 
                                                    "Length_Feet", "Low_Marsh_Created_Acres", 
                                                    "Total_Marsh_Area_Created_Acres", "Width_Feet")])
duplicate_data <- marsh[duplicate_rows, ]

#Delete duplicates. Deleted 58 observations. 1,412 observations are left.
marsh <- marsh[!duplicate_rows, ]
rm(duplicate_rows)

#Create column with the authorization year of the living shoreline
library(dplyr)
marsh$year <- paste0("20", substr(marsh$permit_no, 1, 2))

#Aggregating marsh dimensions if licenses are authorized within the same year (ensure revisions are also combined together)
combined_marsh <- marsh %>%
  group_by(master_ai_id, year) %>%
  summarize(across(c(Fill_Cubic_Yards, High_Marsh_Created_Acres, 
                     Length_Feet, Low_Marsh_Created_Acres, 
                     Total_Marsh_Area_Created_Acres, Width_Feet), ~sum(.x, na.rm = FALSE)))
View(combined_marsh)

marsh <- merge(marsh, combined_marsh, by = c("master_ai_id", "year"), suffixes = c("", "_agg"))

marsh <- marsh[order(marsh$master_ai_id), ]
rownames(marsh) <- NULL

#Removing the duplicates with the same id and year and removing the old columns. Deleted 121 observations. 1,291 observations are left.
marsh <- marsh[!duplicated(marsh[c("master_ai_id", "year")]), ]

marsh$Fill_Cubic_Yards <- marsh$High_Marsh_Created_Acres <- marsh$Length_Feet <- marsh$Low_Marsh_Created_Acres <- marsh$Total_Marsh_Area_Created_Acres <- marsh$Width_Feet <- NULL

#Handling missing data
#--------------------
#I've previously filled in missing values. Any remaining missing values are truly missing, and a value of 0 means 0.

#Deleting rows with values of 0s or missing values in all columns. Deleted 4 observations. 1,287 observations are left.
missing_marsh <- marsh %>%
  filter(
    rowSums(is.na(select(., "Fill_Cubic_Yards_agg", "High_Marsh_Created_Acres_agg", 
                         "Length_Feet_agg", "Low_Marsh_Created_Acres_agg", 
                         "Total_Marsh_Area_Created_Acres_agg", "Width_Feet_agg"))) == 6 |
      rowSums(select(., "Fill_Cubic_Yards_agg", "High_Marsh_Created_Acres_agg", 
                     "Length_Feet_agg", "Low_Marsh_Created_Acres_agg", 
                     "Total_Marsh_Area_Created_Acres_agg", "Width_Feet_agg")) == 0
  )

View(missing_marsh)

marsh <- anti_join(marsh, missing_marsh)
View(marsh)

#----------------------------------------------------------------
## 2. Sill data ##
#----------------------------------------------------------------

#Irrelevant columns are already deleted prior to loading it into R
#Column names are changed prior to loading it in R
#Columns sorted by oldest to newest licenses

#Loading the data
sill <- read.csv("sill_data.csv")

#Data exploration
#----------------
#Started off with 1,541 observations
summary(sill)
str(sill)

#Data transformation
#-------------------
#Store approval issue dates as dates
sill$approval_issued <- as.Date(sill$approval_issued, format = "%m/%d/%y")

#Some zip codes include ZIP+4 codes. Extracting only the 5-digit codes and changing the column to integers
sill$postal_code <- substr(sill$postal_code, 1, 5)
sill$postal_code <- as.integer(sill$postal_code)

#Handling irrelevant and duplicate data
#--------------------------------------

#Delete projects with WQC, NT, and PR licenses. Deleted 6 observations. 1,534 observations left.
irrelevant_sill <- subset(sill, grepl("WQC|NT|PR", permit_no))
sill <- subset(sill, grepl("WL|GL|WP|GP", permit_no) & !grepl("WQC|NT|PR", permit_no))

#Checking for duplicate sill dimension values
duplicate_sill_rows <- duplicated(sill[c("master_ai_id", "Height_Above_MHWL", "Length_Feet", "Material", 
                                         "Maximum_Extent_Channelward_Feet", "Width_Feet")])

duplicate_sill <- sill[duplicate_sill_rows, ]

#Delete duplicates. Deleted 85 observations. 1,449 observations are left.
sill <- sill[!duplicate_sill_rows, ]
rm(duplicate_sill_rows)

#Create column with the authorization year of the sill
sill$year <- paste0("20", substr(sill$permit_no, 1, 2))

#Counting the number of sill structures each license has
sill <- sill %>%
  group_by(master_ai_id, year) %>%
  mutate(sill_count = n()) %>%
  ungroup()

#Aggregating sill dimensions if licenses are authorized within the same year (ensure revisions are also combined together)
combined_sill <- sill %>%
  group_by(master_ai_id, year) %>%
  summarize(across(c(Height_Above_MHWL, Length_Feet,
                       Maximum_Extent_Channelward_Feet, Width_Feet), ~sum(.x, na.rm = FALSE)))
View(combined_sill)

sill <- merge(sill, combined_sill, by = c("master_ai_id", "year"), suffixes = c("", "_agg"))

sill <- sill[order(sill$master_ai_id), ]
rownames(sill) <- NULL

#Removing the duplicates with the same id and year and removing the old columns. Deleted 295 observations. 1,154 observations are left.
sill <- sill[!duplicated(sill[c("master_ai_id", "year")]), ]

sill$Height_Above_MHWL <- sill$Length_Feet <- sill$Maximum_Extent_Channelward_Feet <- sill$Width_Feet <- NULL

#Handling missing data
#--------------------
#I've previously filled in missing values. Any remaining missing values are truly missing, and a value of 0 means 0.

#Deleting rows with values of 0s or missing values in all columns. Deleted 3 observations. 1,151 observations are left.
missing_sill <- sill %>%
  filter(
    rowSums(is.na(select(., "Height_Above_MHWL_agg", "Length_Feet_agg",
                         "Maximum_Extent_Channelward_Feet_agg", "Width_Feet_agg"))) == 4 |
      rowSums(select(., "Height_Above_MHWL_agg", "Length_Feet_agg",
                     "Maximum_Extent_Channelward_Feet_agg", "Width_Feet_agg")) == 0
  )

View(missing_sill)

sill <- anti_join(sill, missing_sill)
View(sill)

#----------------------------------------------------------------
## 3. Finalize cleaned living shoreline data ##
#----------------------------------------------------------------

#Joining the marsh and sill data by id and year. Licenses with sills not accompanying marsh should not be included. 1,287 observations in total.
merged_data <- merge(marsh, sill[, c("master_ai_id", "year", "Height_Above_MHWL_agg", "Length_Feet_agg",
                                        "Maximum_Extent_Channelward_Feet_agg", "Width_Feet_agg", "sill_count")], by = c("master_ai_id", "year"), all.x = TRUE, all.y = FALSE)

#Fixing structural errors
#------------------------
#Combining address 1 and address 2 columns into one address column
merged_data$address <- ifelse(merged_data$address_2 == "",
                                      paste(merged_data$address_1, merged_data$address_2, sep = " "),
                                      paste(merged_data$address_1, merged_data$address_2, sep = ", "))

merged_data$address_1 <- merged_data$address_2 <- NULL

#Capitalizing only the first letter of every word in the address column.
library(stringr)
merged_data$address <- str_to_title(merged_data$address)

# Converting lat/long coordinates in DMS format to DD format
dms_to_dd <- function(dms) { 
  parts <- strsplit(dms, " ")
  sign <- ifelse(substr(dms, 1, 1) == "-", -1, 1) # Determine the sign based on negative values
  
  degrees <- as.numeric(parts[[1]][1])
  minutes <- as.numeric(parts[[1]][2])
  seconds <- as.numeric(parts[[1]][3])
  
  dd <- degrees + sign*abs(minutes/60 + seconds/3600)
  return(dd)} # Custom function

merged_data$longitude <- ifelse(grepl("\\d+ \\d+ \\d+", merged_data$x_coord_value),
                               sapply(merged_data$x_coord_value, dms_to_dd),
                               as.numeric(merged_data$x_coord_value))

merged_data$latitude <- ifelse(grepl("\\d+ \\d+ \\d+", merged_data$y_coord_value),
                               sapply(merged_data$y_coord_value, dms_to_dd),
                               as.numeric(merged_data$y_coord_value))

merged_data$longitude <- ifelse(!is.na(merged_data$longitude) & merged_data$longitude > 0, -merged_data$longitude, merged_data$longitude)

merged_data$x_coord_value <- merged_data$y_coord_value <- merged_data$coordinate_system <- NULL

#Exporting the clean dataset
write.csv(merged_data, "marsh_data_clean_revised.csv", row.names = FALSE)

#----------------------------------------------------------------
## 4. Manual cleaning ##
#----------------------------------------------------------------

#Checking for outliers
#---------------------
z_scores_marsh <- scale(merged_data[, c("Fill_Cubic_Yards_agg", "High_Marsh_Created_Acres_agg", 
                                          "Length_Feet_agg.x", "Low_Marsh_Created_Acres_agg", 
                                          "Total_Marsh_Area_Created_Acres_agg", "Width_Feet_agg.x")])

z_scores_sill <- scale(merged_data[, c("Height_Above_MHWL_agg", "Length_Feet_agg.y", 
                                       "Maximum_Extent_Channelward_Feet_agg", "Width_Feet_agg.y")])

outliers_marsh <- merged_data[rowSums(abs(z_scores_marsh) > 3) > 0, ]
outliers_sill <- merged_data[rowSums(abs(z_scores_sill) > 3) > 0, ]

write.csv(outliers_marsh, "outliers_marsh.csv", row.names = FALSE)
write.csv(outliers_sill, "outliers_sill.csv", row.names = FALSE)

View(outliers_marsh) #82 observations that may be out of the norm. Most are blank rows? Will manually go through them.
View(outliers_sill) #806 observations that may be out of the norm. Most are blank rows? Will manually go through them.

#Validating and QA
#-----------------
#Checking if high + low marsh = total marsh
non_matching <- merged_data[abs(marsh$High_Marsh_Created_Acres_agg + marsh$Low_Marsh_Created_Acres_agg 
                                    - marsh$Total_Marsh_Area_Created_Acres_agg) >= 0.5, ]
View(non_matching)
write.csv(non_matching, file = "non_matching_rows.csv", row.names = FALSE)

#Made sure all the projects were in MD
#Made sure all the lat/long coordinates were in dd
#Corrected outliers

#----------------------------------------------------------------
## 4. Bulkhead data ##
#----------------------------------------------------------------

#Irrelevant columns are already deleted prior to loading it into R
#Column names are changed prior to loading it in R
#Columns sorted by oldest to newest licenses

#Loading the data
bulkhead <- read.csv("bulkhead_data.csv")

#Data exploration
#----------------
#Started off with 3,394 observations
summary(bulkhead)
str(bulkhead)

#Data transformation
#-------------------
#Store approval issue dates as dates
bulkhead$approval_issued <- as.Date(bulkhead$approval_issued, format = "%m/%d/%y")

#Some zip codes include ZIP+4 codes. Extracting only the 5-digit codes and changing the column to integers
bulkhead$postal_code <- substr(bulkhead$postal_code, 1, 5)
bulkhead$postal_code <- as.integer(bulkhead$postal_code)

#Handling irrelevant and duplicate data
#--------------------------------------

#Delete projects with WQC, NT, and PR licenses. Deleted 68 observations. 3,324 observations left.
irrelevant_bulkhead <- subset(bulkhead, !grepl("WL|GL|WP|GP", permit_no))
bulkhead <- subset(bulkhead, grepl("WL|GL|WP|GP", permit_no))

#Checking for duplicate sill dimension values
duplicate_sill_rows <- duplicated(sill[c("master_ai_id", "Height_Above_MHWL", "Length_Feet", "Material", 
                                         "Maximum_Extent_Channelward_Feet", "Width_Feet")])

duplicate_sill <- sill[duplicate_sill_rows, ]

#Delete duplicates. Deleted 85 observations. 1,449 observations are left.
sill <- sill[!duplicate_sill_rows, ]
rm(duplicate_sill_rows)

#Create column with the authorization year of the sill
sill$year <- paste0("20", substr(sill$permit_no, 1, 2))

#Counting the number of sill structures each license has
sill <- sill %>%
  group_by(master_ai_id, year) %>%
  mutate(sill_count = n()) %>%
  ungroup()

#Aggregating sill dimensions if licenses are authorized within the same year (ensure revisions are also combined together)
combined_sill <- sill %>%
  group_by(master_ai_id, year) %>%
  summarize(across(c(Height_Above_MHWL, Length_Feet,
                     Maximum_Extent_Channelward_Feet, Width_Feet), ~sum(.x, na.rm = FALSE)))
View(combined_sill)

sill <- merge(sill, combined_sill, by = c("master_ai_id", "year"), suffixes = c("", "_agg"))

sill <- sill[order(sill$master_ai_id), ]
rownames(sill) <- NULL

#Removing the duplicates with the same id and year and removing the old columns. Deleted 295 observations. 1,154 observations are left.
sill <- sill[!duplicated(sill[c("master_ai_id", "year")]), ]

sill$Height_Above_MHWL <- sill$Length_Feet <- sill$Maximum_Extent_Channelward_Feet <- sill$Width_Feet <- NULL

#----------------------------------------------------------------
## 5. Revetment data ##
#----------------------------------------------------------------
revetment <- read.csv("revetment_data.csv")
revetment$approval_issued <- as.Date(revetment$approval_issued, format = "%m/%d/%y")
revetment$postal_code <- substr(revetment$postal_code, 1, 5)
revetment$postal_code <- as.integer(revetment$postal_code)
irrelevant_revetment <- subset(revetment, !grepl("WL|GL|WP|GP", permit_no))
revetment <- subset(revetment, grepl("WL|GL|WP|GP", permit_no))

