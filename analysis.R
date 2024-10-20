library(data.table)
library(tidyverse)
library(dplyr)
library(arrow)
library(ggplot2)
options(scipen=999999)

# -----------------
# This file is for the replication of one literature by our result
# latest update: 2024/10/18

output_path <-  paste0("E:/BenHsieh/marriage_asset_panel_sample_100_105.parquet")
sample_marriage_panel_asset <- read_parquet("E:/BenHsieh/marriage_asset_panel_sample_100_105.parquet",as_data_frame = T)

summary(sample_marriage_panel_asset[,.(m_age,f_age,aver_asset_b_m,aver_asset_b_f,aver_asset_a_m,aver_asset_a_f)])

create_perc <- function(asset,break_number){
  asset_copy <- asset
  asset_perc <- rep(NA,length(asset_copy))
  asset_perc[asset_copy == 0] = 0
  non_zero =  asset_copy[asset_copy != 0]
  br = quantile(non_zero,prob= seq(0,1,1/break_number),na.rm=T)
  asset_perc[asset_copy != 0] = cut(non_zero,breaks=br,include.lowest = T,labels = 1:break_number)
  return(asset_perc)
}

sample_marriage_panel_asset[,`:=`(decile_asset_b_m = create_perc(aver_asset_b_m,10),
                                  decile_asset_b_f = create_perc(aver_asset_b_f,10),
                                  decile_asset_a_m = create_perc(aver_asset_a_m,10),
                                  decile_asset_a_f = create_perc(aver_asset_a_f,10),
                                  decile_asset_b_m_p = create_perc(aver_asset_b_m_p,10),
                                  decile_asset_b_f_p = create_perc(aver_asset_b_f_p,10),
                                  decile_asset_a_m_p = create_perc(aver_asset_a_m_p,10),
                                  decile_asset_a_f_p = create_perc(aver_asset_a_f_p,10)
)]

sample_marriage_panel_asset[,`:=`(percentile_asset_b_m = create_perc(aver_asset_b_m,100),
                                  percentile_asset_b_f = create_perc(aver_asset_b_f,100),
                                  percentile_asset_a_m = create_perc(aver_asset_a_m,100),
                                  percentile_asset_a_f = create_perc(aver_asset_a_f,100),
                                  percentile_asset_b_m_p = create_perc(aver_asset_b_m_p,100),
                                  percentile_asset_b_f_p = create_perc(aver_asset_b_f_p,100)
)]


marr_asset_summ_decile <- sample_marriage_panel_asset[,.(m_aver_wealth = mean(aver_asset_b_m),w_aver_wealth = mean(aver_asset_b_f),N=.N),by=.(decile_asset_b_m,decile_asset_b_f)]
write.csv(marr_asset_summ_decile,"E:/BenHsieh/marr_asset_summ_decile.csv")
p_marr_asset_summ_decile <- sample_marriage_panel_asset[,.(m_aver_wealth = mean(aver_asset_b_m_p),w_aver_wealth = mean(aver_asset_b_f_p),N=.N),by=.(decile_asset_b_m_p,decile_asset_b_f_p)]
write.csv(p_marr_asset_summ_decile,"E:/BenHsieh/p_marr_asset_summ_decile.csv")

marr_asset_summ_percentile <- sample_marriage_panel_asset[,.(m_aver_wealth = mean(aver_asset_b_m),w_aver_wealth = mean(aver_asset_b_f),N=.N),by=.(percentile_asset_b_m,percentile_asset_b_f)]
write.csv(marr_asset_summ_percentile,"E:/BenHsieh/marr_asset_summ_percentile.csv")
p_marr_asset_summ_percentile <- sample_marriage_panel_asset[,.(m_aver_wealth = mean(aver_asset_b_m_p),w_aver_wealth = mean(aver_asset_b_f_p),N=.N),by=.(percentile_asset_b_m_p,percentile_asset_b_f_p)]
write.csv(p_marr_asset_summ_percentile,"E:/BenHsieh/p_marr_asset_summ_percentile.csv")


heatmap <- sample_marriage_panel_asset[,.N,by=.(decile_asset_b_m,decile_asset_b_f)]
heatmap_exclude = heatmap[decile_asset_b_m!=0 & decile_asset_b_f!=0]
ggplot(heatmap_exclude, aes(x = decile_asset_b_m, y = decile_asset_b_f, fill = N))+
  geom_tile(color= "white")+
  scale_fill_gradient(low="white",high="blue")+
  theme_minimal()


cor(sample_marriage_panel_asset$aver_asset_b_m, sample_marriage_panel_asset$aver_asset_b_f, use="complete.obs")
# 0.3978876
lm_mating = lm(aver_asset_b_f ~ aver_asset_b_m,data = sample_marriage_panel_asset)
summary(lm_mating)
ggplot(sample_marriage_panel_asset, aes(x = aver_asset_b_m, y = aver_asset_b_f))+
  geom_point(alpha=0.6)+
  geom_smooth(method = lm,color="blue")+
  theme_minimal()
# Call:
#   lm(formula = aver_asset_b_f ~ aver_asset_b_m, data = sample_marriage_panel_asset)
# 
# Residuals:
#   Min         1Q     Median         3Q        Max 
# -619267926    -573385    -516176    -224553 1835562952 
# 
# Coefficients:
#   Estimate     Std. Error t value            Pr(>|t|)    
# (Intercept)    516176.0986353   5796.0645261   89.06 <0.0000000000000002 ***
#   aver_asset_b_m      0.1321941      0.0003436  384.69 <0.0000000000000002 ***
#   ---
#   Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1
# 
# Residual standard error: 5105000 on 786781 degrees of freedom
# Multiple R-squared:  0.1583,	Adjusted R-squared:  0.1583 
# F-statistic: 1.48e+05 on 1 and 786781 DF,  p-value: < 0.00000000000000022



heatmap <- sample_marriage_panel_asset[,.N,by=.(decile_asset_b_m_p,decile_asset_b_f_p)]
heatmap_exclude = heatmap[decile_asset_b_m_p!=0 & decile_asset_b_f_p!=0]
ggplot(heatmap, aes(x = decile_asset_b_m_p, y = decile_asset_b_f_p, fill = N))+
  geom_tile(color= "white")+
  scale_fill_gradient(low="white",high="blue")+
  theme_minimal()

ggplot(heatmap_exclude, aes(x = decile_asset_b_m_p, y = decile_asset_b_f_p, fill = N))+
  geom_tile(color= "white")+
  scale_fill_gradient(low="white",high="blue")+
  theme_minimal()


cor(sample_marriage_panel_asset$aver_asset_b_m_p, sample_marriage_panel_asset$aver_asset_b_f_p, use="complete.obs")
# 0.04197937
lm_mating = lm(aver_asset_b_f_p ~ aver_asset_b_m_p,data = sample_marriage_panel_asset)
summary(lm_mating)

# Call:
#   lm(formula = aver_asset_b_f_p ~ aver_asset_b_m_p, data = sample_marriage_panel_asset)
# 
# Residuals:
#   Min          1Q      Median          3Q         Max 
# -1126688386   -10837074    -7774951     -231141 35744175542 
# 
# Coefficients:
#   Estimate     Std. Error t value            Pr(>|t|)    
# (Intercept)      11196301.05789    84461.85562  132.56 <0.0000000000000002 ***
#   aver_asset_b_m_p        0.04285        0.00115   37.27 <0.0000000000000002 ***
#   ---
#   Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1
# 
# Residual standard error: 73510000 on 786781 degrees of freedom
# Multiple R-squared:  0.001762,	Adjusted R-squared:  0.001761 
# F-statistic:  1389 on 1 and 786781 DF,  p-value: < 0.00000000000000022

sample_marriage_panel_asset_outlier = sample_marriage_panel_asset[aver_asset_b_m<100000000 & aver_asset_b_f<100000000]


heatmap <- sample_marriage_panel_asset_outlier[,.N,by=.(decile_asset_b_m,decile_asset_b_f)]
heatmap_exclude = heatmap[decile_asset_b_m!=0 & decile_asset_b_f!=0]
ggplot(heatmap_exclude, aes(x = decile_asset_b_m, y = decile_asset_b_f, fill = N))+
  geom_tile(color= "white")+
  scale_fill_gradient(low="white",high="blue")+
  theme_minimal()


cor(sample_marriage_panel_asset_outlier$aver_asset_b_m, sample_marriage_panel_asset_outlier$aver_asset_b_f, use="complete.obs")
lm_mating = lm(aver_asset_b_f ~ aver_asset_b_m,data = sample_marriage_panel_asset_outlier)
summary(lm_mating)
ggplot(sample_marriage_panel_asset_outlier, aes(x = aver_asset_b_m, y = aver_asset_b_f))+
  geom_point(alpha=0.6)+
  geom_smooth(method = lm,color="blue")+
  theme_minimal()




heatmap <- sample_marriage_panel_asset_outlier[,.N,by=.(decile_asset_b_m_p,decile_asset_b_f_p)]
heatmap_exclude = heatmap[decile_asset_b_m_p!=0 & decile_asset_b_f_p!=0]
ggplot(heatmap, aes(x = decile_asset_b_m_p, y = decile_asset_b_f_p, fill = N))+
  geom_tile(color= "white")+
  scale_fill_gradient(low="white",high="blue")+
  theme_minimal()
ggplot(heatmap_exclude, aes(x = decile_asset_b_m_p, y = decile_asset_b_f_p, fill = N))+
  geom_tile(color= "white")+
  scale_fill_gradient(low="white",high="blue")+
  theme_minimal()


cor(sample_marriage_panel_asset_outlier$aver_asset_b_m_p, sample_marriage_panel_asset_outlier$aver_asset_b_f_p, use="complete.obs")
lm_mating = lm(aver_asset_b_f_p ~ aver_asset_b_m_p,data = sample_marriage_panel_asset_outlier)
summary(lm_mating)
ggplot(sample_marriage_panel_asset_outlier, aes(x = aver_asset_b_m_p, y = aver_asset_b_f_p))+
  geom_point(alpha=0.6)+
  geom_smooth(method = lm,color="blue")+
  theme_minimal()

nrow(sample_marriage_panel_asset[aver_asset_b_f==0])/nrow(sample_marriage_panel_asset)


sample_marriage_panel_asset[,aver_per_asset_b_f := mean(percentile_asset_b_f),by=.(percentile_asset_b_m)]
heatmap <- sample_marriage_panel_asset[,.N,by=.(aver_per_asset_b_f,percentile_asset_b_m)]
ggplot(heatmap,aes(x=percentile_asset_b_m,y=aver_per_asset_b_f))+
  geom_point()+
  theme_minimal()
lm_mating = lm(aver_per_asset_b_f ~ percentile_asset_b_m,data = heatmap)
summary(lm_mating)

sample_marriage_panel_asset[,aver_per_asset_b_f_p := mean(percentile_asset_b_f_p),by=.(percentile_asset_b_m_p)]
heatmap <- sample_marriage_panel_asset[,.N,by=.(aver_per_asset_b_f_p,percentile_asset_b_m_p)]
ggplot(heatmap,aes(x=percentile_asset_b_m_p,y=aver_per_asset_b_f_p))+
  geom_point()+
  theme_minimal()
lm_mating = lm(aver_per_asset_b_f_p ~ percentile_asset_b_m_p,data = heatmap)
summary(lm_mating)

sample_marriage_panel_asset[,aver_per_asset_b_m := mean(percentile_asset_b_m),by=.(percentile_asset_b_m_p)]
heatmap <- sample_marriage_panel_asset[,.N,by=.(aver_per_asset_b_m,percentile_asset_b_m_p)]
ggplot(heatmap,aes(x=percentile_asset_b_m_p,y=aver_per_asset_b_m))+
  geom_point()+
  theme_minimal()

sample_marriage_panel_asset[,aver_per_asset_b_f := mean(percentile_asset_b_f),by=.(percentile_asset_b_f_p)]
heatmap <- sample_marriage_panel_asset[,.N,by=.(aver_per_asset_b_f,percentile_asset_b_f_p)]
ggplot(heatmap,aes(x=percentile_asset_b_f_p,y=aver_per_asset_b_f))+
  geom_point()+
  theme_minimal()

ggplot()+
  geom_density(data=sample_marriage_panel_asset,aes(x=m_asset_aver_b),fill="red",alpha=0.5)+
  geom_density(data=sample_marriage_panel_asset,aes(x=f_asset_aver_b),fill="blue",alpha=0.5)


ggplot(sample_marriage_panel_asset,aes(x = end_year, y = N , color = end_reason,group=end_reason )) + geom_line() +xlim(90,110)+theme_minimal()


sample_marriage_decile = setDT(read.csv("/Users/bingruxie/Desktop/research project/thesis/202409_2攜出/sample_marriage_decile.csv"))
sample_marriage_percentile = setDT(read.csv("/Users/bingruxie/Desktop/research project/thesis/202409_2攜出/sample_marriage_percentile.csv"))
sample_marriage_parent_decile = setDT(read.csv("/Users/bingruxie/Desktop/research project/thesis/202409_2攜出/sample_marriage_parent_decile.csv"))
sample_marriage_parent_percentile = setDT(read.csv("/Users/bingruxie/Desktop/research project/thesis/202409_2攜出/sample_marriage_parent_percentile.csv"))

stat_marriage_percentile <- sample_marriage_percentile[, .(
  Weighted_Female_Percentile = sum(percentile_asset_b_f * N) / sum(N),
  Weighted_Female_Avg_Salary = sum(w_aver_wealth * N) / sum(N)
  ), by = percentile_asset_b_m]

# 計算 Weighted_Female_Percentile 和 Male_Percentile 之間的斜率
slope_percentile <- lm(Weighted_Female_Percentile ~ percentile_asset_b_m, data = stat_marriage_percentile)$coefficients[2]

ggplot(stat_marriage_percentile, aes(x = percentile_asset_b_m)) +
  geom_point(aes(y = Weighted_Female_Avg_Salary), size = 2) +
  labs(x = "Male Percentile", y = "Female Avg Asset") +
  ggtitle("Relationship between Male Percentile and Female Average Asset") +
  theme_minimal()

ggplot(stat_marriage_percentile, aes(x = percentile_asset_b_m)) +
  geom_point(aes(y = Weighted_Female_Percentile), size = 2) +
  labs(x = "Male Percentile", y = "Weighted Female Percentile") +
  theme_minimal() +
  ggtitle("Relationship between Male Percentile and Female Percentile") +
  annotate("text", x = 20, y = max(stat_marriage_percentile$Weighted_Female_Percentile), 
           label = paste("Slope =", round(slope_percentile, 3)), hjust = 1)



stat_marriage_parent_percentile <- sample_marriage_parent_percentile[, .(
  Weighted_Female_Percentile = sum(percentile_asset_b_f_p * N) / sum(N),
  Weighted_Female_Avg_Salary = sum(w_aver_wealth * N) / sum(N)
), by = percentile_asset_b_m_p]

# 計算 Weighted_Female_Percentile 和 Male_Percentile 之間的斜率
slope_percentile <- lm(Weighted_Female_Percentile ~ percentile_asset_b_m_p, data = stat_marriage_parent_percentile)$coefficients[2]

ggplot(stat_marriage_parent_percentile, aes(x = percentile_asset_b_m_p)) +
  geom_point(aes(y = Weighted_Female_Avg_Salary), size = 2) +
  labs(x = "Male Parent Percentile", y = "Female Parent Avg Asset") +
  ggtitle("Relationship between Male Parent Percentile and Female Parent Average Asset") +
  theme_minimal()

ggplot(stat_marriage_parent_percentile, aes(x = percentile_asset_b_m_p)) +
  geom_point(aes(y = Weighted_Female_Percentile), size = 2) +
  labs(x = "Male Percentile", y = "Weighted Female Percentile") +
  theme_minimal() +
  ggtitle("Relationship between Male Parent Percentile and Female Parent Percentile") +
  annotate("text", x = 20, y = max(stat_marriage_parent_percentile$Weighted_Female_Percentile), 
           label = paste("Slope =", round(slope_percentile, 3)), hjust = 1)


end_reason = setDT(read.csv("/Users/bingruxie/Desktop/research project/thesis/202409_2攜出/end_reason_year.csv"))
marriage_year = setDT(read.csv("/Users/bingruxie/Desktop/research project/thesis/202409_2攜出/marriage_year.csv"))

ggplot(marriage_year, aes(x = Var1, y = Freq)) +
  geom_bar(stat = "identity", fill = "gray", color = "black") +
  labs(x = "Year", y = "Marriage Number", title = "Marriage Number per Year") +
  theme_minimal()

end_reason = end_reason[,end_reason := as.factor(end_reason)][end_year>=90]

ggplot(end_reason, aes(x = end_year, y = N, color = end_reason, group = end_reason)) +
  geom_line() +
  geom_point() +
  labs(x = "End Year", y = "Number of Marriages Ended", title = "Marriage End Reasons by Year") +
  scale_color_discrete(name = "End Reason") +
  theme_minimal()


