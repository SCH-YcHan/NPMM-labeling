rm(list=ls())

install.packages("catboost")
library(catboost)
library(foreach)
library(doParallel)
library(caret)
source("Labeling_code.R")

labeling_file <- read.csv("../data/Stock_Labeling/NASDAQ_labeling.csv")

NASDAQ_names <- list.files("../data/Stock_TI")

#실험에서 제외할 주식
rm_stock <- c("KDP_TI.csv", "KLAC_TI.csv", "MDLZ_TI.csv", "PLUG_TI.csv")
NASDAQ_names <- setdiff(NASDAQ_names, rm_stock)

#코어 개수 가져오기
numCo <- parallel::detectCores()-1

#클러스터 초기화
myCluster <- parallel::makeCluster(numCo)
doParallel::registerDoParallel(myCluster)

pac <- c("stringr", "dplyr", "lubridate")

#NASDAQ stock + Label
NASDAQ <- foreach::foreach(index=NASDAQ_names, .combine=rbind, .packages=pac) %dopar% {
  name <<- index
  stock_ti <- read.csv(paste0("../data/Stock_TI/", name))
  Symbol <- str_split(name, "_")[[1]][1]
  label <- labeling_file %>% 
    select(Date, contains(paste0(Symbol,".")))
  names(label) <- c("Date", "UD", "NV5", "NV10", "NV20", "NB5", "NB10", "NB20", "TA5", "TA11", "TA21")
  stock_ti %>%
    na.omit() %>% 
    cbind(Symbol=Symbol,.) %>% 
    merge(label, by="Date") %>% 
    filter(as.Date(Date) < "2021-01-01")
}

if (!file.exists("../data/train_instance")){
  dir.create("../data/train_instance")
}

if (!file.exists("../data/test_prediction")){
  dir.create("../data/test_prediction")
}

if (!file.exists("../data/ML_result")){
  dir.create("../data/ML_result")
}

#병렬처리 + 자동화
Parallel_processing <- function(Stock, Symb, Comb, Pac, Ex, Method, Period,
                                Set_Labels, Dep_Label, Label_N, Use_Model, Start_Test){
  result <- foreach::foreach(index=Symb, .combine=Comb, .packages=pac, .export=Ex) %dopar% {
    symb <<- index
    
      Stock_data <- Stock %>% 
        filter(Symbol==symb) %>% 
        select(-Symbol, -Adj_Close, -Open) %>% 
        data_split(method=Method, period=Period)
      
      label <- c()
      train_N <- data.frame()
      
      for(case in Stock_data %>% select(contains("case")) %>% names){
        train_ins <- Stock_data %>% 
          filter(get(case)=="Train") %>% 
          .[1:(nrow(.)-Label_N),] %>% 
          select(-setdiff(Set_Labels, Dep_Label), -Date) %>% 
          select(!contains("case")) %>% 
          instance_selection(Dep_Label=Dep_Label, dup=F)
        
        train_N <- rbind(train_N, data.frame(Train_N_before = train_ins$Train_N_before[1],
                                             Train_N_after = train_ins$Train_N_after[1]))
        
        train_set <- train_ins %>% 
          select(-Train_N_before, -Train_N_after)
        
        test_set <- Stock_data %>% 
          filter(get(case)=="Test") %>% 
          select(-setdiff(Set_Labels, Dep_Label), -Date) %>% 
          select(!contains("case"))
        
        if(Use_Model=="NB"){
          nb <- naiveBayes(as.formula(paste0(Dep_Label, "~.")), data=train_set)
          test_pre <- predict(nb, test_set, type="raw")[,1]
        } else if(Use_Model=="ADA"){
          train_set[Dep_Label] <- as.factor(train_set[Dep_Label] %>% unlist() %>% as.vector())
          test_set[Dep_Label] <- as.factor(test_set[Dep_Label] %>% unlist() %>% as.vector())
          
          set.seed(20207188)
          ada <- boosting(as.formula(paste0(Dep_Label, "~.")), data=train_set)
          test_pre <- predict(ada, test_set)$prob[,2]
        } else if(Use_Model=="GBM"){
          set.seed(20207188)
          train_set[Dep_Label] <- train_set[Dep_Label]-1
          gbm <- gbm(as.formula(paste0(Dep_Label, "~.")),
                     data = train_set,
                     verbose = F)
          test_pre <- predict(gbm, test_set, type="response")
        } else if(Use_Model=="XGB"){
          set.seed(20207188)
          xgb <- xgboost(data = train_set %>% select(-Dep_Label) %>% data.matrix(),
                         label = train_set %>% select(Dep_Label) %>% .[,1]-1,
                         nrounds = 100,
                         objective = "binary:logistic",
                         verbose = F)
          test_pre <- predict(xgb, test_set %>% select(-Dep_Label) %>% data.matrix())
        } else if(Use_Model=="LGBM"){
          train_lgb <- lgb.Dataset(data = train_set %>% select(-Dep_Label) %>% as.matrix(),
                                   label = train_set %>% select(Dep_Label) %>% .[,1]-1)
          
          set.seed(20207188)
          lgbm <- lgb.train(data = train_lgb,
                            obj = "binary",
                            nrounds = 100,
                            verbose = 0)
          test_pre <- predict(lgbm, test_set %>% select(-Dep_Label) %>% as.matrix())
        } else if(Use_Model=="CAT"){
          train_pool <- catboost.load_pool(data = train_set %>% select(-Dep_Label),
                                           label = train_set %>% select(Dep_Label) %>% .[,1]-1)
          test_pool <- catboost.load_pool(data = test_set %>% select(-Dep_Label))
          
          params <- list(loss_function = "Logloss",
                         eval_metric = "AUC",
                         random_seed = 1234)
          
          catb <- catboost.train(train_pool, params = params)
          test_pre <- catboost.predict(catb, test_pool, prediction_type = "Probability")
        } else{
          print("Error : Use_Model = 'ADA' or 'GBM' or 'XGB' or 'LGBM' or 'CAT'")
        }
        label <- c(label, test_pre)
      }

      write.csv(train_N, paste0("../data/train_instance/",symb,Use_Model,Dep_Label,Method,Period,".csv"), row.names=F)
      write.csv(data.frame(pred=label), paste0("../data/test_prediction/",symb,Use_Model,Dep_Label,Method,Period,".csv"), row.names=F)
      
      label <- ifelse(label>0.75, 2, ifelse(label<0.25, 1, 0))
      
      train_N <- train_N %>% colMeans() %>% round() %>% t() %>% data.frame()
      
      test_merge <- Stock %>%
        filter(Symbol==symb) %>%
        filter(as.Date(Date) > Start_Test) %>% 
        select(Date, Adj_Close, Open, -Symbol) %>% 
        mutate(Open2 = lead(Open,1)) %>% 
        mutate(Open = ifelse(is.na(Open2), Open, Open2)) %>% 
        select(-Open2)
      
      No_Trade <- test_merge$Open[nrow(test_merge)]-test_merge$Open[1]
      
      test_merge %>%
        cbind(label = label) %>%
        labeling_metrics(symbol = symb,
                         merge_data = test_merge) %>% 
        cbind(No_Trade = No_Trade) %>% 
        cbind(train_N) %>% 
        .[c(1,11,12,2:10)]
  }
  write.csv(result, paste0("../data/ML_result/",Use_Model,Dep_Label,Method,Period,".csv"), row.names=F)
}

Symbols <- str_split(NASDAQ_names, "_", simplify=T)[,1]
pac <- c("dplyr", "e1071", "adabag", "xgboost", "catboost", "gbm", "lightgbm")
label_names <- c("UD", "NV5", "NV10", "NV20", "NB5", "NB10", "NB20", "TA5", "TA11", "TA21")
export <- c("data_split", "labeling_metrics", "instance_selection")

exg <- expand.grid(P = c("3M"), UM = c("XGB"))

#Testing
for(idx in 1:nrow(exg)){
  #UpDown_TsCV_M
  Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
                      Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "UD",
                      Label_N = 1, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
  
  # #5VDP_TsCV_M
  # Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
  #                     Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "NV5",
  #                     Label_N = 5, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
  # 
  # #10VDP_TsCV_M
  # Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
  #                     Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "NV10",
  #                     Label_N = 10, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
  # 
  # #20VDP_TsCV_M
  # Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
  #                     Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "NV20",
  #                     Label_N = 20, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
  
  # #5Barrier_TsCV_M
  # Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
  #                     Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "NB5",
  #                     Label_N = 5, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
  # 
  # #10Barrier_TsCV_M
  # Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
  #                     Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "NB10",
  #                     Label_N = 10, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
  # 
  # #20Barrier_TsCV_M
  # Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
  #                     Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "NB20",
  #                     Label_N = 20, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
  # 
  # #5TA_TsCV_M
  # Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
  #                     Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "TA5",
  #                     Label_N = 2, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
  # 
  # #11TA_TsCV_M
  # Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
  #                     Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "TA11",
  #                     Label_N = 5, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
  # 
  # #21TA_TsCV_M
  # Parallel_processing(Stock = NASDAQ, Symb = Symbols, Comb = rbind, Pac = pac, Ex = export,
  #                     Method = "TsCV", Period = exg$P[idx], Set_Labels = label_names, Dep_Label = "TA21",
  #                     Label_N = 10, Use_Model = exg$UM[idx], Start_Test = "2018-01-01")
}


