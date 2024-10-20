library(data.table)
library(tidyverse)
library(dplyr)
library(arrow)
library(ggplot2)
options(scipen=999999)

# -----------------
# This file is try to add the asset data to our marriage data. which get the personal data from merge asset and attach them to each husband or wife for yearly pre-marriage asset and post-marriage asset
# latest update: 2024/09/20

dt_pers_info <- fread("E:/LouisTseng/pers_info.csv")

all_marriage_panel <- read_parquet(paste0("E:/BenHsieh/marriage_panel_all_97_110.parquet"))
all_marriage_panel <- all_marriage_panel[,!c("spouse1_marr_yr", "spouse2_marr_yr", "marr_year_check")]
all_marriage_panel <- all_marriage_panel[marr_year > 100 & marr_year <= 107 & spouse1_sex!= spouse2_sex]

all_marriage_panel <- all_marriage_panel[,':='(m_idn = fifelse(spouse1_sex==1,spouse1_idn,spouse2_idn),f_idn = fifelse(spouse1_sex==2,spouse1_idn,spouse2_idn))]
all_marriage_panel <- all_marriage_panel[,!c("spouse1_idn", "spouse2_idn")]
all_marriage_panel <- merge(all_marriage_panel,dt_pers_info[,.(idn,mother_idn, father_idn)],by.x= "m_idn",by.y="idn",all.x=T,suffixes =  c("_m","_f"))
all_marriage_panel <- merge(all_marriage_panel,dt_pers_info[,.(idn,mother_idn, father_idn)],by.x= "f_idn",by.y="idn",all.x=T,suffixes =  c("_m","_f"))

all_idns <- unique(c(all_marriage_panel$m_idn,all_marriage_panel$f_idn,all_marriage_panel$mother_idn_m,all_marriage_panel$mother_idn_f,all_marriage_panel$father_idn_m,all_marriage_panel$father_idn_f))

asset_files <- list.files(path = "E:/LouisTseng/DataTransfer/merge_asset/",pattern = "merge_assetv2_.*\\.csv",full.names = T)
sample_merge_asset_list <- lapply(asset_files,function(file){
  year <- as.numeric(gsub("merge_assetv2_|\\.csv","",basename(file)))
  asset_data <- fread(file)
  asset_data <- asset_data[pers_idn %in% all_idns]
  asset_data[,year := year]
  asset_data[is.na(saving), saving := 0]
  asset_data[is.na(bond), bond := 0]
  asset_data[is.na(mv_house), mv_house := 0]
  asset_data[is.na(mv_land), mv_land := 0]
  asset_data[is.na(v_car), v_car := 0]
  asset_data[is.na(v_stock), v_stock := 0]
  asset_data[,sum_asset := saving + bond + mv_house + mv_land + v_car + v_stock]
  # asset_data[is.na(sum_asset), sum_asset := 0]
  return(asset_data)
})
merge_asset_sample <- rbindlist(sample_merge_asset_list)
output_path <-  paste0("E:/BenHsieh/merge_asset_sample_100_107.parquet")
write_parquet(merge_asset_sample, output_path)

merge_asset_sample <- setDT(read_parquet("E:/BenHsieh/merge_asset_sample_100_107.parquet",as_data_frame = T))

get_asset_for_year <- function(idn,y){
  asset <- merge_asset_sample[pers_idn == idn & year==y,sum_asset]
  return(sum(asset,na.rm=T))
}

all_marr_panel_long <- melt(all_marriage_panel,measure.vars = c("m_idn","f_idn","mother_idn_m","father_idn_m","mother_idn_f","father_idn_f"))
all_marriage_panel_asset <- all_marriage_panel[, m_asset_b3 := mapply(get_asset_for_year,m_idn,marr_year-3)]
all_marriage_panel_asset <- all_marriage_panel[, m_asset_b3 := mapply(get_asset_for_year,m_idn,marr_year-3)]


all_marriage_panel_asset <- all_marriage_panel[,`:=`(
  m_asset_b3 = get_asset_for_year(m_idn,marr_year-3),
  m_asset_b2 = get_asset_for_year(m_idn,marr_year-2),
  m_asset_b1 = get_asset_for_year(m_idn,marr_year-1),
  m_asset_a0 = get_asset_for_year(m_idn,marr_year),
  m_asset_a1 = get_asset_for_year(m_idn,marr_year+1),
  m_asset_a2 = get_asset_for_year(m_idn,marr_year+2),
  m_asset_a3 = get_asset_for_year(m_idn,marr_year+3),
  f_asset_b3 = get_asset_for_year(f_idn,marr_year-3),
  f_asset_b2 = get_asset_for_year(f_idn,marr_year-2),
  f_asset_b1 = get_asset_for_year(f_idn,marr_year-1),
  f_asset_a0 = get_asset_for_year(f_idn,marr_year),
  f_asset_a1 = get_asset_for_year(f_idn,marr_year+1),
  f_asset_a2 = get_asset_for_year(f_idn,marr_year+2),
  f_asset_a3 = get_asset_for_year(f_idn,marr_year+3),
  m_p_asset_b3 = get_asset_for_year(mother_idn_m,marr_year-3)+get_asset_for_year(father_idn_m,marr_year-3),
  m_p_asset_b2 = get_asset_for_year(mother_idn_m,marr_year-2)+get_asset_for_year(father_idn_m,marr_year-2),
  m_p_asset_b1 = get_asset_for_year(mother_idn_m,marr_year-1)+get_asset_for_year(father_idn_m,marr_year-1),
  m_p_asset_a0 = get_asset_for_year(mother_idn_m,marr_year)+get_asset_for_year(father_idn_m,marr_year),
  m_p_asset_a1 = get_asset_for_year(mother_idn_m,marr_year+1)+get_asset_for_year(father_idn_m,marr_year+1),
  m_p_asset_a2 = get_asset_for_year(mother_idn_m,marr_year+2)+get_asset_for_year(father_idn_m,marr_year+2),
  m_p_asset_a3 = get_asset_for_year(mother_idn_m,marr_year+3)+get_asset_for_year(father_idn_m,marr_year+3),
  f_p_asset_b3 = get_asset_for_year(mother_idn_f,marr_year-3)+get_asset_for_year(father_idn_f,marr_year-3),
  f_p_asset_b2 = get_asset_for_year(mother_idn_f,marr_year-2)+get_asset_for_year(father_idn_f,marr_year-2),
  f_p_asset_b1 = get_asset_for_year(mother_idn_f,marr_year-1)+get_asset_for_year(father_idn_f,marr_year-1),
  f_p_asset_a0 = get_asset_for_year(mother_idn_f,marr_year)+get_asset_for_year(father_idn_f,marr_year),
  f_p_asset_a1 = get_asset_for_year(mother_idn_f,marr_year+1)+get_asset_for_year(father_idn_f,marr_year+1),
  f_p_asset_a2 = get_asset_for_year(mother_idn_f,marr_year+2)+get_asset_for_year(father_idn_f,marr_year+2),
  f_p_asset_a3 = get_asset_for_year(mother_idn_f,marr_year+3)+get_asset_for_year(father_idn_f,marr_year+3)
  )]

all_marriage_panel_asset <- all_marriage_panel_asset[,`:=`(
  m_asset_aver_b = mean(c(m_asset_b3,m_asset_b2,m_asset_b1)),
  f_asset_aver_b = mean(c(f_asset_b3,f_asset_b2,f_asset_b1)),
  asset_aver_a = mean(c(m_asset_a3+f_asset_a3,m_asset_a2+f_asset_a2,m_asset_a1+f_asset_a1)),
  m_p_asset_aver_b = mean(c(m_p_asset_b3,m_p_asset_b2,m_p_asset_b1)),
  f_p_asset_aver_b = mean(c(f_p_asset_b3,f_p_asset_b2,f_p_asset_b1))
  )]

ggplot()+
  geom_density(data=all_marriage_panel_asset,aes(x=m_asset_aver_b),fill="red",alpha=0.5)+
  geom_density(data=all_marriage_panel_asset,aes(x=f_asset_aver_b),fill="blue",alpha=0.5)
  

ggplot(all_marriage_panel_asset,aes(x = end_year, y = N , color = end_reason,group=end_reason )) + geom_line() +xlim(90,110)+theme_minimal()

  
