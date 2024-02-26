rm(list=ls())
source("Labeling_code.R")

library(dplyr)
library(data.table)
library(gridExtra)
library(ggplot2)
library(stringr)

NASDAQ_stocks <- fread("../data/NASDAQ100.csv") %>% data.frame()

#labeling을 위해 각 주식의 수정 종가만 추출
NASDAQ_adj <- NASDAQ_stocks %>% select(Date, contains("Adj.Close"))

#이름을 변경한다음 후에 식별을 위해 이름에 .을 추가
names(NASDAQ_adj) <- c("Date", paste0(substr(names(NASDAQ_adj)[-1],1,str_length(names(NASDAQ_adj)[-1])-10),"."))

NASDAQ_labeling <- NASDAQ_adj %>%
  select(-Date) %>%
  lapply(function(x)data.frame(UD = UpDown(x, N=5),
                               NV5 = Nday_VDP(x, u_sig=0.2, l_sig=0.2),
                               NV10 = Nday_VDP(x, u_sig=0.2, l_sig=0.2, N=10),
                               NV20 = Nday_VDP(x, u_sig=0.2, l_sig=0.2, N=20),
                               NB5 = Nday_Barrier(x, u_sig=0.2, l_sig=0.2, N=5),
                               NB10 = Nday_Barrier(x, u_sig=0.2, l_sig=0.2, N=10),
                               NB20 = Nday_Barrier(x, u_sig=0.2, l_sig=0.2, N=20),
                               TA5 = Trade_action(x, window=5),
                               TA11 = Trade_action(x, window=11),
                               TA19 = Trade_action(x, window=21))) %>%
  do.call(cbind, .) %>%
  cbind(Date=NASDAQ_adj$Date, .)

write.csv(NASDAQ_labeling ,"../data/Stock_Labeling/NASDAQ_labeling.csv", row.names=F)

# NASDAQ_merge <- merge(NASDAQ_adj, NASDAQ_labeling, by="Date")

#UpDown labeling
# NASDAQ_UD <- data.frame()
# for(symbol in names(NASDAQ_adj)[-1]){
#   row <- NASDAQ_merge %>% 
#     select(Date, symbol, paste0(symbol,".UD")) %>% 
#     rename(label = names(.)[3]) %>% 
#     labeling_metrics(symbol=substr(symbol,1,str_length(symbol)-1),
#                      merge_data = NASDAQ_merge %>% select(Date, symbol))
#   NASDAQ_UD <- rbind(NASDAQ_UD, row)
# }
# 
# #Nday_VDP(N=5) labeling
# NASDAQ_NV5 <- data.frame()
# for(symbol in names(NASDAQ_adj)[-1]){
#   row <- NASDAQ_merge %>%
#   select(Date, symbol, paste0(symbol,".NV5")) %>% 
#   rename(label = names(.)[3]) %>% 
#   labeling_metrics(symbol=substr(symbol,1,str_length(symbol)-1),
#                    merge_data = NASDAQ_merge %>% select(Date, symbol))
#   NASDAQ_NV5 <- rbind(NASDAQ_NV5, row)
# }
# 
# #Nday_VDP(N=10) labeling
# NASDAQ_NV10 <- data.frame()
# for(symbol in names(NASDAQ_adj)[-1]){
#   row <- NASDAQ_merge %>% 
#     select(Date, symbol, paste0(symbol,".NV10")) %>% 
#     rename(label = names(.)[3]) %>% 
#     labeling_metrics(symbol=substr(symbol,1,str_length(symbol)-1),
#                      merge_data = NASDAQ_merge %>% select(Date, symbol))
#   NASDAQ_NV10 <- rbind(NASDAQ_NV10, row)
# }
# 
# #Nday_VDP(N=20) labeling
# NASDAQ_NV20 <- data.frame()
# for(symbol in names(NASDAQ_adj)[-1]){
#   row <- NASDAQ_merge %>% 
#     select(Date, symbol, paste0(symbol,".NV20")) %>% 
#     rename(label = names(.)[3]) %>% 
#     labeling_metrics(symbol=substr(symbol,1,str_length(symbol)-1),
#                      merge_data = NASDAQ_merge %>% select(Date, symbol))
#   NASDAQ_NV20 <- rbind(NASDAQ_NV20, row)
# }
# 
# #file save
# write.csv(NASDAQ_UD, "./Labeling_result/NASDAQ_UD.csv", row.names=F)
# write.csv(NASDAQ_NV5, "./Labeling_result/NASDAQ_NV5.csv", row.names=F)
# write.csv(NASDAQ_NV10, "./Labeling_result/NASDAQ_NV10.csv", row.names=F)
# write.csv(NASDAQ_NV20, "./Labeling_result/NASDAQ_NV20.csv", row.names=F)

# TEST <- NASDAQ_stocks %>% select(Date, AAPL.Adj.Close) %>% 
#   filter(row.names(.) %>% as.numeric()<=100)
# 
# TEST$AAPL.Adj.Close %>% UpDown(logging=T)
# TEST$AAPL.Adj.Close %>% Nday_VDP(logging=T)
# 
# TEST <- NASDAQ_labeling %>% select(contains("AAPL"))

# UD <- labeling(TEST, fun="UD") %>% Plotting_buy_sell
# NV5 <- labeling(TEST, fun="NV", N=5) %>% Plotting_buy_sell
# NV10 <- labeling(TEST, fun="NV", N=10) %>% Plotting_buy_sell
# NV20 <- labeling(TEST, fun="NV", N=20) %>% Plotting_buy_sell
# 
# grid.arrange(UD, NV5, NV10, NV20, nrow=4, ncol=1)
