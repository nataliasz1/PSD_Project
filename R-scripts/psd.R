df=4
mu = c(0.004, 0.003, 0.003, 0.001, 0.002, 0.002)
sigma = matrix(c(1,1,0,1,1,1,
                 1,36,-1,-1,-3,-5,
                 0,-1,4,2,2,0,
                 1,-1,2,49,-5,2,
                 1,-3,2,-5,16,-2,
                 1,-5,0,-2,-2,9),6,6)
sigma[6,4]=-2
print(sigma)
lower = c(-0.1,-0.1,-0.1,-0.1,-0.1,-0.1)
upper = c(0.1,0.1,0.1,0.1,0.1,0.1)
X1 <- rtmvt(n=1000000, mu, sigma, df, lower, upper, algorithm = "gibbs")
write.table(X1,file="probki.csv",sep=";",row.names=F, col.names=T)

