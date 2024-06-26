# Data Cleaning Process

#----------------------------------------------------------------
## 1. Marsh data ##
#----------------------------------------------------------------

#Setting working directory
setwd("C:/Users/szhang/Desktop/EPA Property Value Data")

#Installing and loading packages
#install.packages("dplyr")
#install.packages("stringr")
library(dplyr)
library(stringr)

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
marsh$approval_issued <- as.Date(marsh$approval_issued, format = "%m/%d/%Y")

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
sill$approval_issued <- as.Date(sill$approval_issued, format = "%m/%d/%Y")

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
#Started off with 3,390 observations
summary(bulkhead)
str(bulkhead)

#Data transformation
#-------------------
#Store approval issue dates as dates
bulkhead$approval_issued <- as.Date(bulkhead$approval_issued, format = "%m/%d/%Y")

#Some zip codes include ZIP+4 codes. Extracting only the 5-digit codes and changing the column to integers
bulkhead$postal_code <- substr(bulkhead$postal_code, 1, 5)
bulkhead$postal_code <- as.integer(bulkhead$postal_code)

#Handling irrelevant and duplicate data
#--------------------------------------
#Delete projects with WQC, NT, NL? and PR licenses. Deleted 70 observations. 3,320 observations left.
irrelevant_bulkhead <- subset(bulkhead, !grepl("WL|GL|WP|GP", permit_no))
bulkhead <- subset(bulkhead, grepl("WL|GL|WP|GP", permit_no))

#Checking for duplicate bulkhead dimension values
duplicate_bulkhead_rows <- duplicated(bulkhead[c("master_ai_id", "Length_Feet", 
                                         "Maximum_Extent_Channelward_Feet", "Height_Above_Water_Feet")])

duplicate_bulkhead <- bulkhead[duplicate_bulkhead_rows, ]

#Delete duplicates. Deleted 98 observations. 3,222 observations are left.
bulkhead <- bulkhead[!duplicate_bulkhead_rows, ]
rm(duplicate_bulkhead_rows)

#Handling missing data
#--------------------
#I've previously filled in missing values. Any remaining missing values are truly missing, and a value of 0 means 0.

#Deleting rows with values of 0s or missing values in all columns. Deleted 1 observations. 3,221 observations are left.
missing_bulkhead <- bulkhead %>%
  filter(
    rowSums(is.na(select(., "Length_Feet", "Maximum_Extent_Channelward_Feet", "Height_Above_Water_Feet"))) == 3 |
      rowSums(select(., "Length_Feet", "Maximum_Extent_Channelward_Feet", "Height_Above_Water_Feet")) == 0
  )

View(missing_bulkhead)

bulkhead <- anti_join(bulkhead, missing_bulkhead)
View(bulkhead)

#Create column with the year of the permit
bulkhead$year <- paste0("20", substr(bulkhead$permit_no, 1, 2))

#Counting the number of bulkhead structures each license has
bulkhead <- bulkhead %>%
  group_by(master_ai_id, year) %>%
  mutate(bulkhead_count = n()) %>%
  ungroup()

#Aggregating dimensions
#--------------------
#Aggregating bulkhead length if they have the same permit year (ensure revisions are also combined together)
combined_bulkhead <- bulkhead %>%
  group_by(master_ai_id, year) %>%
  summarize(across(c(Length_Feet), ~sum(.x, na.rm = FALSE)))

View(combined_bulkhead)

bulkhead <- merge(bulkhead, combined_bulkhead, by = c("master_ai_id", "year"), suffixes = c("", "_agg"))

bulkhead <- bulkhead[order(bulkhead$master_ai_id), ]
rownames(bulkhead) <- NULL

#Removing the duplicates with the same id and year and removing the old columns. Deleted 184 observations. 3,037 observations are left.
bulkhead <- bulkhead[!duplicated(bulkhead[c("master_ai_id", "year")]), ]

bulkhead$Length_Feet <- NULL

#Fixing structural errors
#------------------------
#Combining address 1 and address 2 columns into one address column
bulkhead$address <- ifelse(bulkhead$address_2 == "",
                              paste(bulkhead$address_1, bulkhead$address_2, sep = " "),
                              paste(bulkhead$address_1, bulkhead$address_2, sep = ", "))

bulkhead$address_1 <- bulkhead$address_2 <- NULL

#Capitalizing only the first letter of every word in the address column.
bulkhead$address <- str_to_title(bulkhead$address)

# Converting lat/long coordinates in DMS format to DD format
dms_to_dd <- function(dms) { 
  parts <- strsplit(dms, " ")
  sign <- ifelse(substr(dms, 1, 1) == "-", -1, 1) # Determine the sign based on negative values
  
  degrees <- as.numeric(parts[[1]][1])
  minutes <- as.numeric(parts[[1]][2])
  seconds <- as.numeric(parts[[1]][3])
  
  dd <- degrees + sign*abs(minutes/60 + seconds/3600)
  return(dd)} # Custom function

bulkhead$longitude <- ifelse(grepl("\\d+ \\d+ \\d+", bulkhead$x_coord_value),
                                sapply(bulkhead$x_coord_value, dms_to_dd),
                                as.numeric(bulkhead$x_coord_value))

bulkhead$latitude <- ifelse(grepl("\\d+ \\d+ \\d+", bulkhead$y_coord_value),
                               sapply(bulkhead$y_coord_value, dms_to_dd),
                               as.numeric(bulkhead$y_coord_value))

bulkhead$longitude <- ifelse(!is.na(bulkhead$longitude) & bulkhead$longitude > 0, -bulkhead$longitude, bulkhead$longitude)

bulkhead$x_coord_value <- bulkhead$y_coord_value <- bulkhead$coordinate_system <- NULL

#Exporting the clean dataset
write.csv(bulkhead, "bulkhead_clean.csv", row.names = FALSE)

#Checking for outliers
#---------------------
z_scores_bulkhead <- scale(bulkhead[, c("Length_Feet_agg", "Maximum_Extent_Channelward_Feet", "Height_Above_Water_Feet")])

outliers_bulkhead <- bulkhead[rowSums(abs(z_scores_bulkhead) > 3) > 0, ]

write.csv(outliers_bulkhead, "outliers_bulkhead.csv", row.names = FALSE)

View(outliers_bulkhead) #2,856 observations that may be out of the norm. Most are blank rows? Will manually go through them.

#Validating and QA
#-----------------
#Made sure all the projects were in MD
#Made sure all the lat/long coordinates were in dd
#Corrected outliers

#----------------------------------------------------------------
## 5. Revetment data ##
#----------------------------------------------------------------

#Irrelevant columns are already deleted prior to loading it into R
#Column names are changed prior to loading it in R
#Columns sorted by oldest to newest licenses

#Loading the data
revetment <- read.csv("revetment_data.csv")

#Data exploration
#----------------
#Started off with 3,338 observations
summary(revetment)
str(revetment)

#Data transformation
#-------------------
#Store approval issue dates as dates
revetment$approval_issued <- as.Date(revetment$approval_issued, format = "%m/%d/%Y")

#Some zip codes include ZIP+4 codes. Extracting only the 5-digit codes and changing the column to integers
revetment$postal_code <- substr(revetment$postal_code, 1, 5)
revetment$postal_code <- as.integer(revetment$postal_code)

#Handling irrelevant and duplicate data
#--------------------------------------
#Delete projects with WQC, NT, NL and PR licenses. Deleted 35 observations. 3,303 observations left.
irrelevant_revetment <- subset(revetment, !grepl("WL|GL|WP|GP", permit_no))
revetment <- subset(revetment, grepl("WL|GL|WP|GP", permit_no))

#Checking for duplicate revetment dimension values
duplicate_revetment_rows <- duplicated(revetment[c("master_ai_id", "Length_Feet", 
                                                 "Material_Type", "Maximum_Extent_Channelward_Feet")])

duplicate_revetment <- revetment[duplicate_revetment_rows, ]

#Delete duplicates. Deleted 118 observations. 3,185 observations are left.
revetment <- revetment[!duplicate_revetment_rows, ]
rm(duplicate_revetment_rows)

#Handling missing data
#--------------------
#I've previously filled in missing values. Any remaining missing values are truly missing, and a value of 0 means 0.

#Deleting rows with values of 0s or missing values in all columns. Deleted 20 observations. 3,165 observations are left.
missing_revetment <- revetment %>%
  filter(
    rowSums(is.na(select(., "Length_Feet", "Maximum_Extent_Channelward_Feet"))) == 2 |
      rowSums(select(., "Length_Feet", "Maximum_Extent_Channelward_Feet")) == 0
  )

View(missing_revetment)

revetment <- anti_join(revetment, missing_revetment)
View(revetment)

#Create column with the year of the permit
revetment$year <- paste0("20", substr(revetment$permit_no, 1, 2))

#Counting the number of revetment structures each license has
revetment <- revetment %>%
  group_by(master_ai_id, year) %>%
  mutate(revetment_count = n()) %>%
  ungroup()

#Aggregating dimensions
#--------------------
#Aggregating revetment length if they have the same permit year (ensure revisions are also combined together)
combined_revetment <- revetment %>%
  group_by(master_ai_id, year) %>%
  summarize(across(c(Length_Feet), ~sum(.x, na.rm = FALSE)))

View(combined_revetment)

revetment <- merge(revetment, combined_revetment, by = c("master_ai_id", "year"), suffixes = c("", "_agg"))

revetment <- revetment[order(revetment$master_ai_id), ]
rownames(revetment) <- NULL

#Removing the duplicates with the same id and year and removing the old columns. Deleted 400 observations. 2,765 observations are left.
revetment <- revetment[!duplicated(revetment[c("master_ai_id", "year")]), ]

revetment$Length_Feet <- NULL

#Fixing structural errors
#------------------------
#Combining address 1 and address 2 columns into one address column
revetment$address <- ifelse(revetment$address_2 == "",
                           paste(revetment$address_1, revetment$address_2, sep = " "),
                           paste(revetment$address_1, revetment$address_2, sep = ", "))

revetment$address_1 <- revetment$address_2 <- NULL

#Capitalizing only the first letter of every word in the address column.
revetment$address <- str_to_title(revetment$address)

# Converting lat/long coordinates in DMS format to DD format
dms_to_dd <- function(dms) { 
  parts <- strsplit(dms, " ")
  sign <- ifelse(substr(dms, 1, 1) == "-", -1, 1) # Determine the sign based on negative values
  
  degrees <- as.numeric(parts[[1]][1])
  minutes <- as.numeric(parts[[1]][2])
  seconds <- as.numeric(parts[[1]][3])
  
  dd <- degrees + sign*abs(minutes/60 + seconds/3600)
  return(dd)} # Custom function

revetment$longitude <- ifelse(grepl("\\d+ \\d+ \\d+", revetment$x_coord_value),
                             sapply(revetment$x_coord_value, dms_to_dd),
                             as.numeric(revetment$x_coord_value))

revetment$latitude <- ifelse(grepl("\\d+ \\d+ \\d+", revetment$y_coord_value),
                            sapply(revetment$y_coord_value, dms_to_dd),
                            as.numeric(revetment$y_coord_value))

revetment$longitude <- ifelse(!is.na(revetment$longitude) & revetment$longitude > 0, -revetment$longitude, revetment$longitude)

revetment$x_coord_value <- revetment$y_coord_value <- revetment$coordinate_system <- NULL

#Exporting the clean dataset
write.csv(revetment, "revetment_clean.csv", row.names = FALSE)

#Checking for outliers
#---------------------
z_scores_revetment <- scale(revetment[, c("Length_Feet_agg", "Maximum_Extent_Channelward_Feet")])

outliers_revetment <- revetment[rowSums(abs(z_scores_revetment) > 3) > 0, ]

write.csv(outliers_revetment, "outliers_revetment.csv", row.names = FALSE)

View(outliers_revetment) #43 observations that may be out of the norm. Will manually go through them.

#Validating and QA
#-----------------
#Made sure all the projects were in MD
#Made sure all the lat/long coordinates were in dd
#Corrected outliers