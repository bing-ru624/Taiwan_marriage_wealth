library(data.table)
library(tidyverse)
library(dplyr)
library(arrow)
library(ggplot2)
options(scipen=999999)

# -----------------
# This file aim to converting the person-base marriage into partners-base data. Each row would record each marriage, from both id, start year ,end year, etc.
# And try to recover some information due to there was a period which use wrong encode method.
# latest update: 2024/09/20

for (year in 97:110){
  file_path <-paste0("E:/BenHsieh/marriage_panel_data_clean/",year,"_marriage_panel_clean.parquet")
  df = setDT(read_parquet(file_path, as_data_frame = T))
  df_marriage = df %>% filter(isMarriage == T)
  df_marriage[,spous_idn:=as.integer(spous_idn)]
  df_marriage <- df_marriage %>% mutate(spouse1_idn = pmin(idn,spous_idn,na.rm = T),spouse2_idn = pmax(idn,spous_idn)) 
  df_marriage <- df_marriage %>% mutate(spouse1_birth = ifelse(idn==spouse1_idn,birth_year,NA),spouse1_death = ifelse(idn==spouse1_idn,dead_year,NA),spouse2_birth = ifelse(idn == spouse2_idn,birth_year,NA),spouse2_death = ifelse(idn==spouse2_idn,dead_year,NA))
  df_marriage <- df_marriage %>% mutate(spouse1_sex = ifelse(idn==spouse1_idn,sex,NA),spouse2_sex = ifelse(idn==spouse2_idn,sex,NA))
  df_marriage <- df_marriage %>% mutate(spouse1_marr_yr = ifelse(idn==spouse1_idn,marr_yr,NA),spouse2_marr_yr = ifelse(idn==spouse2_idn,marr_yr,NA))
  df_marriage <- df_marriage %>% mutate(spouse1_marr_date = ifelse(idn==spouse1_idn,marr_date,NA),spouse2_marr_date = ifelse(idn==spouse2_idn,marr_date,NA))
  
  df_marriage_freq <- df_marriage[, .N, by=.(spouse1_idn,spouse2_idn)] %>% filter(is.na(spouse1_idn) == F & is.na(spouse2_idn) == F)
  
  # check how many couple do not show twice in the data
  # cat(as.numeric(count(df_marriage_freq[N!=2]))," couple do not show twice in",year,"\n")
  
  marriage_current_year = merge(df_marriage_freq[,.(spouse1_idn,spouse2_idn)],df_marriage[is.na(spouse1_sex) == F,.(spouse1_idn,spouse1_birth,spouse1_death,spouse1_sex,spouse1_marr_yr,spouse1_marr_date)],by="spouse1_idn",all.x = T)
  marriage_current_year =  merge(marriage_current_year,df_marriage[is.na(spouse2_sex) == F,.(spouse2_idn,spouse2_birth,spouse2_death,spouse2_sex,spouse2_marr_yr,spouse2_marr_date)],by="spouse2_idn",all.x = T)
  
  marriage_current_year = marriage_current_year %>% mutate(marr_year = pmin(spouse1_marr_yr,spouse2_marr_yr,na.rm = T), marr_year_check = (spouse1_marr_yr==spouse2_marr_yr))
  marriage_current_year = marriage_current_year %>% mutate(marr_date = pmin(spouse1_marr_date,spouse2_marr_date,na.rm = T), marr_date_check = (spouse1_marr_date==spouse2_marr_date))
  
  print(table(marriage_current_year[marr_year<=100 & marr_year>90,marr_year]))
  
  marriage_current_year_valid = marriage_current_year %>% mutate(marr_year = fifelse(is.na(marr_year) | marr_year>113, NA_real_, marr_year))
  
  print(count(marriage_current_year_valid[is.na(marr_year)]))
  
  marriage_current_year_valid$end_year = NA_real_
  marriage_current_year_valid$end_reason = NA_real_
  
  
  if(year==97){
    
    still_marriage_panel = marriage_current_year_valid
    end_marriage_panel =still_marriage_panel[FALSE,]
    
  }
  else{
    
    potential_divorce = still_marriage_panel[marriage_current_year_valid, on = c("spouse1_idn","spouse2_idn"),nomatch=0]
    setnames(potential_divorce,c("marr_year","i.marr_year"),c("old_marr_yr","new_marr_yr"))
    
    old_marriages = potential_divorce[old_marr_yr>new_marr_yr | is.na(old_marr_yr) & !is.na(new_marr_yr)] %>% select("spouse1_idn","spouse2_idn","old_marr_yr","new_marr_yr","spouse1_marr_yr","spouse2_marr_yr","spouse1_marr_date","spouse2_marr_date") 
    new_marriages = potential_divorce[old_marr_yr<new_marr_yr] %>% select("spouse1_idn","spouse2_idn","old_marr_yr","new_marr_yr","spouse1_marr_yr","spouse2_marr_yr","spouse1_marr_date","spouse2_marr_date") 
    cat(as.numeric(count(old_marriages)),"couple have mismatch marriage year in",year,"\n")
    cat(as.numeric(count(new_marriages)),"couple have re-marriage year in",year,"\n")
    print(table(marriage_current_year[marr_year<=100 & marr_year>90,marr_year]))
    
    still_marriage_panel[old_marriages,on=c("spouse1_idn","spouse2_idn"),`:=`(marr_year= new_marr_yr,spouse1_marr_yr= i.spouse1_marr_yr,spouse2_marr_yr= i.spouse2_marr_yr,marr_date= i.spouse1_marr_date,spouse1_marr_date= i.spouse1_marr_date,spouse2_marr_date= i.spouse2_marr_date,end_year= NA_real_,end_reason=NA_real_)]
    still_marriage_panel[new_marriages,on=c("spouse1_idn","spouse2_idn"),`:=`(end_year = pmin(year,new_marr_yr,na.rm = T),end_reason=6)]
    end_marriage_panel = rbind(end_marriage_panel,still_marriage_panel[end_reason ==6])
    still_marriage_panel = still_marriage_panel[end_reason != 6 | is.na(end_reason)]
    
    changed_data = still_marriage_panel[!marriage_current_year_valid,on= c("spouse1_idn","spouse2_idn")]
    new_data = marriage_current_year_valid[!still_marriage_panel,on= c("spouse1_idn","spouse2_idn")]
    
    end_df = merge(changed_data[,.(spouse1_idn,spouse2_idn,marr_year)],df[,.(idn,marriage_stus)],by.x="spouse1_idn",by.y="idn",all.x=T)
    end_df = merge(end_df,df[,.(idn,marriage_stus)],by.x="spouse2_idn",by.y="idn",all.x=T)
    end_df[,`:=`(end_year = year,end_reason = fifelse(marriage_stus.x==4 |marriage_stus.y==4,4,fifelse(marriage_stus.x==5 |marriage_stus.y==5,5,fifelse(marriage_stus.x== 3 |marriage_stus.y== 3,3,fifelse(marriage_stus.x== 2 |marriage_stus.y== 2,2,1)))))]
    changed_data[end_df,on = c("spouse1_idn","spouse2_idn"),`:=`(end_year=i.end_year,end_reason=i.end_reason)]
    end_marriage_panel = rbind(end_marriage_panel,changed_data)
    still_marriage_panel = still_marriage_panel[!changed_data,on = c("spouse1_idn","spouse2_idn")]
    still_marriage_panel = rbind(still_marriage_panel,new_data)
    
    death_in_this_year = still_marriage_panel[spouse1_death== year |spouse2_death == year][,`:=`(end_year=year,end_reason=4)]
    end_marriage_panel = rbind(end_marriage_panel,death_in_this_year)
    still_marriage_panel = still_marriage_panel[!death_in_this_year,on= c("spouse1_idn","spouse2_idn")]
    
  }
  rm(list = setdiff(ls(),c("still_marriage_panel","year","end_marriage_panel")))
  gc()
  
}

all_marriage_panel = rbind(still_marriage_panel,end_marriage_panel)

output_path <-  paste0("E:/BenHsieh/marriage_panel_all_97_110.parquet")
write_parquet(all_marriage_panel, output_path)

all_marriage_panel <- setDT(read_parquet("E:/BenHsieh/marriage_panel_all_97_110.parquet",as_data_frame = T))

stats = data.frame(table(all_marriage_panel[,.(marr_year)]))
ggplot(stats,aes(x=Var1,y=Freq))+geom_bar(stat="identity")
fwrite(stats, "E:/BenHsieh/marriage_year.csv")

# missing a lot of data which marriage year is from 90-99, guess because there is a encode error in JHQ (a lots of 701 801...) can check from
# file_path <-paste0("E:/LouisTseng/marriage_used_info/parquet/jhqh_97.parquet")
# df = setDT(read_parquet(file_path, as_data_frame = T))
# df[,marr_yr := as.numeric(str_sub(marriage_date,end=3))]
# print(table(df[,marr_yr]))

df_end = all_marriage_panel %>% filter(is.na(end_year)==F)
df_end[is.na(end_reason),end_reason := 1]
end_stats =df_end[,.N, by=.(end_year,end_reason)]
end_stats$end_reason = as.factor(end_stats$end_reason)

fwrite(end_stats, "E:/BenHsieh/end_reason_year.csv")
ggplot(end_stats,aes(x = end_year, y = N , color = end_reason,group=end_reason )) + geom_line() +xlim(90,110)+theme_minimal()


