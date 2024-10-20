library(data.table)
library(tidyverse)
library(dplyr)
library(arrow)
options(scipen=999999)

# -----------------
# This file clean the marriage data in Admin database first, include adding few variable we need for the futureresearch
# latest update: 2024/08/29

dt_pers_info <- fread("E:/LouisTseng/pers_info.csv")
dt_pers_info <- setDT(dt_pers_info)
dt_pers_info[,idn:=as.integer(idn)]

for (year in 97:110){
  file_path <- paste0("E:/LouisTseng/marriage_used_info/marriage_panel/",year,"_marriage_panel.parquet")
  df = setDT(read_parquet(file_path, as_data_frame = T))
  df[is.na(marriage_stus), marriage_stus := 0]
  df[,isMarriage := (marriage_stus == 2)]
  df[,age := year - birth_year]
  df[,marr_yr := as.numeric(str_sub(marriage_date,end=3))]
  df[,marr_date := as.numeric(str_sub(marriage_date,-4))]
  df = merge(df,dt_pers_info[,.(idn, sex)],by = "idn", all.x = T)
  df[,cohort := paste0( as.character((age %/% 5) * 5),"_", as.character((age %/% 5) * 5 + 4) )]
  summ_stats_table <- df[, .N, by = .(marriage_stus, cohort)] %>%  group_by(marriage_stus, cohort)
  summ_stats_table$yr=year
  output_path <-  paste0("E:/BenHsieh/marriage_panel_data_clean/",year,"_marriage_panel_clean.parquet")
  write_parquet(df, output_path)
  
  if(year ==97){
    all_summ_stats_table = summ_stats_table
  }
  else{
    all_summ_stats_table = rbind(all_summ_stats_table,summ_stats_table)
  }
}
output_path <-  paste0("E:/BenHsieh/marriage_panel_data_clean/all_marriage_summary_stats.csv")
fwrite(all_summ_stats_table, output_path)
