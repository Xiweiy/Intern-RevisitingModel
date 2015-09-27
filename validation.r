library(dplyr)
validation = read.table("validation.txt", header = T)
keys=read.csv(file="POWERKEYS.txt", header = FALSE, sep = "|", stringsAsFactors = FALSE, strip.white = TRUE);
colnames(keys) = c('SORTKEY', 'SOAR_NM', 'CLS_DS')
n=length(unique(validation$SORTKEY))

nmbr = 0
top_decile_trs = 0


##Plotting
pdf(file="validation.pdf");
par(mfrow=c(3,1),mar=c(1,2,1,1)+1.5)
end = 0
for (i in 0:(n-1)){
  start = end+1 ; end = start + 9
  onekey = validation[start:end,]
  onekey$oneweekprop = onekey$ONE_WEEK_NMBR*100/sum(onekey$ONE_WEEK_NMBR)
  onekey$twoweekprop = onekey$TWO_WEEK_NMBR*100/sum(onekey$TWO_WEEK_NMBR)

  baseline.prob = round(sum(onekey$ONE_WEEK_NMBR)/sum(onekey$DECILE_TOTAL),digits=3)
  pred.increase = round((onekey$ONE_WEEK_NMBR[1]/onekey$DECILE_TOTAL[1])/baseline.prob, digits=2)
  total.nx.wk.purchase = sum(onekey$ONE_WEEK_TRS)

  sortkey = unique(onekey$SORTKEY)
  
  ##Some key got less transactions and missing deciles -- less than 10 rows
  if(length(sortkey)>1| sum(onekey$TWO_WEEK_TRS)< 100){
    print(c("Sample Too Small",as.character(sortkey)))
    wrongindices = which(validation$SORTKEY == sortkey[1])
    end = max(wrongindices)
  }
  else
  {
  soarnm = keys$SOAR_NM[keys$SORTKEY==sortkey]
  classds = keys$CLS_DS[keys$SORTKEY==sortkey]
  nmbr = nmbr + onekey$ONE_WEEK_NMBR[1]
  top_decile_trs = top_decile_trs + onekey$ONE_WEEK_TRS[1]
    

  prob.table = rbind(onekey$oneweekprop,onekey$twoweekprop)
  colnames(prob.table) = as.character(1:10)
  barplot(prob.table, beside=T,
	legend = c("ONE_WEEK_TRS","TWO_WEEK_TRS"), xlab="Deciles",
	ylim=c(0,55), main=paste(soarnm,classds,"\n", sortkey))
  text(x=10,y=45,paste("Overall Purchase Prob.: ", baseline.prob))
  text(x=10,y=40,paste("Increase in Purchase Prob. in 1st Decile: ", pred.increase))
  text(x=10,y=50,paste("Total NX WK Purchase: ", total.nx.wk.purchase))
  mtext(text= "Percent Future Purchase in each Deciles",side=2, line=3, adj=1 ,padj=1, cex=1)
  }
}
dev.off()

##Only Generating Statistics 
groupby = group_by(validation,SORTKEY)
summarization = summarize(groupby, Next.Week.Shopper = sum(ONE_WEEK_NMBR),
                First.Decile.Shopper=first(ONE_WEEK_NMBR),
                Total.Revisting.Member =sum(DECILE_TOTAL),
                Purchase.Prob = sum(ONE_WEEK_NMBR)/sum(DECILE_TOTAL),
                pred.increase = (first(ONE_WEEK_NMBR)/first(DECILE_TOTAL))/Purchase.Prob)
with_ds = inner_join(keys,summarization, by='SORTKEY')
write.csv(with_ds, 'boost.csv', col.names=T, row.names=F)
