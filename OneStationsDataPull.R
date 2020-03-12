### R script to combine data from various sources for one station until all data can be imported into Labtrack.
library(LabtrackTools)
library(plyr)
library(tidyverse)
library(lubridate)
library(stringr)
library(stringi)
library(scales)
library(xlsx)
library(data.table)
library(magrittr)
setwd("//Fileserver2/KatHome/ocpwaprahamiana/Aprahamian Projects/Unnatural Water Balance/OCFS")

##choose station
values <- list()
values[['AnalysisStartDate']] <- as.Date('05/01/2019', format = c('%m/%d/%Y'))
values[['Station']] <- "K01-12177-1"

##import data
#Raw data stored in County of Orange Box.com folder: OC Watersheds > Environmental Monitoring > WQIP > HPWQC > Unnatural Water Balance > Outfall Capture Feasibility Studies > Data for R
all <- lt_pull_data(finalizedData = FALSE, projectProgramCode = 133, station = values[['Station']], startDate = values[['AnalysisStartDate']])
PPCPs <- read.xlsx("OCWD 68 water samples PPCPs analysis results 122319_R.xlsx", sheetIndex = 1)
HF183 <- read.xlsx("HF183 and PMA results.xlsx", sheetIndex = 1)
WaterIsotopes <- read.xlsx("SDSU_isotopeReport_OCPW2019_TableA1.xlsx", sheetIndex = 1)
# NitrogenIsotopes

##clean data
LogNumberInfo <- all%>%
  select(Station, Date, `QA Type`, MatrixCode, LogNumber)%>%
  unique()%>%
  mutate(Date = as.character(Date),
         Date = ifelse(LogNumber == "WR305533", "2019-09-04 19:20:00",
                       ifelse(LogNumber == "WR306365", "2019-09-25 09:13:00",
                              ifelse(LogNumber == "WR307353", "2019-10-17 20:02:00",
                                     Date))))
allSelect <- all%>%
  mutate(Result = as.character(Result))%>%
  select(LogNumber, Analysis, Parameter, Qualifier, Result, Units)
allSelectnoNAs <- allSelect %>%
  filter(!is.na(Result))

##filter data for station
# filter using LogNumbers; find LogNumbers from "all"
LogNumbers <- all$LogNumber %>% unique()

# PPCPs
PPCPlist <- list()
# Loop through and grab only applicable LogNumber data
for (LogNumber in LogNumbers) {
  PPCPlist[[LogNumber]] <- filter(PPCPs, grepl(LogNumber, PPCPs$Sample.ID))
}
PPCPlists <- rbindlist(PPCPlist)
PPCPdata <- PPCPlists %>% 
  gather(key = "Parameter", value = "Result", -Sample.ID)%>%
  mutate(Analysis = "PPCP",
         Units = "ng/L",
         LogNumber = Sample.ID)%>%
  select(-Sample.ID)

# HF183s
# copies/100 mL results in labtrack but with numbers instead of "DNQ" or "ND"
HF183slist <- list()
# Loop through and grab only applicable LogNumber data
for (LogNumber in LogNumbers) {
  HF183slist[[LogNumber]] <- filter(HF183, grepl(LogNumber, HF183$Log.Number))
}
HF183slists <- rbindlist(HF183slist)
HF183sdata <- HF183slists %>% 
  select(-Station.Name.from.OCPW, -Type, -Date.received.at.UCSB) %>% 
  gather(key = "Parameter", value = "Result", -Log.Number)%>%
  mutate(Analysis = ifelse(grepl("PMA", Parameter), "HF183PMA", "HF183"),
         Units = ifelse(grepl("ng.DNA.mL.", Parameter), "ng DNA/mL", "copies/100 mL"),
         LogNumber = Log.Number)%>%
  filter(Units != "copies/100 mL") %>%
  select(-Log.Number)

# waterisotopes
WaterIsotopeslist <- list()
WaterIsotopes2 <- WaterIsotopes %>%
  mutate(Log.number = as.character(Log.number))
# Loop through and grab only applicable LogNumber data
for (LogNumber in LogNumbers) {
  WaterIsotopeslist[[LogNumber]] <- filter(WaterIsotopes, grepl(LogNumber, WaterIsotopes$Log.number))
}
WaterIsotopeslists <- rbindlist(WaterIsotopeslist)
WaterIsotopesdata <- WaterIsotopeslists %>%
  select(-Station, -Sampling.date, -Latitude, -Longitude, -Source.Type)%>%
  gather(key = "Parameter", value = "Result", -Log.number)%>%
  mutate(Analysis = "Water Isotopes",
         Units = "per mille",
         LogNumber = Log.number,
         Result = as.character(Result))%>%
  select(-Log.number)

# nitrogenisotopes
#`need from Tim


## Binding rows to compile all data
AllData <- bind_rows(allSelectnoNAs, HF183sdata)
AllData2 <- bind_rows(AllData, PPCPdata)
AllData3 <- bind_rows(AllData2, WaterIsotopesdata)

AllDataFinal <- join(LogNumberInfo, AllData3, by = "LogNumber")
