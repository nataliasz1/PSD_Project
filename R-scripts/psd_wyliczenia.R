X1 = read.csv("probki.csv",header=TRUE, sep=";")

cat("V;Mean;Median;Quantile_10;TopMinMean;MB_1;MB_2",file="pomiary_aktywa.csv", sep="\n")

T<-1000000
Mean<-replicate(6,0)
Median<-replicate(6,0)
Quantile_10<-replicate(6,0)
TopMinMean<-replicate(6,0)
MB_1<-replicate(6,0)
MB_2<-replicate(6,0)

for (column in 1:6){
  Mean[column]<- mean(X1[,column])
  Median[column] <- median(X1[,column])
  Quantile_10[column] <-quantile(X1[,column], probs=0.1)
  TopMinMean[column]<- mean(head(sort(X1[,column]),0.1*T))
  MB_1[column]<- Mean[column] - sum(abs(rep(Mean[column], T) - X1[,column]))/2/T
  temp_sum<-0
  GM<-GiniMd(X1[,column])*(T-1)
  MB_2[column]<-Mean[column] - GM/2/T
  cat(c(column,Mean[column],Median[column],Quantile_10[column]
        ,TopMinMean[column],MB_1[column], MB_2[column])
      ,file = "pomiary_aktywa.csv", sep=";", append=TRUE)
  cat("\n" ,file = "pomiary_aktywa.csv", append=TRUE)
}

# X1 = read.csv("probki.csv",header=TRUE, sep=";")
# 
# cat("V;Mean;Median;Quantile_10;TopMinMean;MB_1;MB_2",file="pomiary_aktywa_2.csv", sep="\n")
# 
# T<-1000000
# Mean<-replicate(6,0)
# Median<-replicate(6,0)
# Quantile_10<-replicate(6,0)
# TopMinMean<-replicate(6,0)
# MB_1<-replicate(6,0)
# MB_2<-replicate(6,0)
# 
# for (column in 1:6){
#   Mean[column]<- mean(X1[,column])
#   Median[column] <- median(X1[,column])
#   Quantile_10[column] <-quantile(X1[,column], probs=0.1)
#   TopMinMean[column]<- mean(head(sort(X1[,column]),0.1*T))
#   MB_1[column]<- Mean[column] - sum(abs(rep(Mean[column], T) - X1[,column]))/2/T
#   temp_sum<-0
#   for (t1 in 1:T){
#       temp_sum<-temp_sum+sum(abs(rep(X1[t1,column],T)-X1[,column]))
#   }
#   MB_2[column]<-Mean[column] - temp_sum/2/T/T
#   cat(c(column,Mean[column],Median[column],Quantile_10[column]
#         ,TopMinMean[column],MB_1[column], MB_2[column])
#       ,file = "pomiary_aktywa_2.csv", sep=";", append=TRUE)
#   cat("\n" ,file = "pomiary_aktywa_2.csv", append=TRUE)
# }

X1 = read.csv("probki.csv",header=TRUE, sep=";")

cat("Mean;Median;Quantile_10;TopMinMean;MB_1;MB_2",file="pomiary_portfel.csv", sep="\n")

T<-1000000
Mean<-0
Median<-0
Quantile_10<-0
TopMinMean<-0
MB_1<-0
MB_2<-0
wallet_shares <-matrix(c(0.2,0.2,0.2,0.15,0.15,0.1),ncol=1)
wallet_values= as.matrix(X1)%*%wallet_shares

Mean<- mean(wallet_values)
Median <- median(wallet_values)
Quantile_10 <-quantile(wallet_values, probs=0.1)
TopMinMean<- mean(head(sort(wallet_values),0.1*T))
MB_1<- Mean - sum(abs(rep(Mean, T) -wallet_values))/2/T
temp_sum<-0
GM<-GiniMd(wallet_values)*(T-1)
MB_2<-Mean - GM/2/T
cat(c(Mean,Median,Quantile_10
         ,TopMinMean,MB_1, MB_2)
       ,file = "pomiary_portfel.csv", sep=";", append=TRUE)
 cat("\n" ,file = "pomiary_portfel.csv", append=TRUE)

