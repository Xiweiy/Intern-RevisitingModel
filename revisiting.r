library(dplyr)
data = read.table(file="powerkeytrs.txt",header=T);
keys=read.csv(file="POWERKEYS.txt", header = FALSE, sep = "|", stringsAsFactors = FALSE, strip.white = TRUE);
colnames(keys) = c('SORTKEY', 'SOAR_NM', 'CLS_DS')
sortkeys=as.character(unique(data$sortkey));  

for (i in sortkeys){
	print(i)
	##Select the data associated with each key
	onekey = data[data$sortkey == i, ]
	onekey$day_int[onekey$day_int > 364]=364; 
	dayint = onekey$day_int

	##Find BU & CLASS DESCRIPTI associated with a specific key
	soarnm= gsub("/", '.', keys$SOAR_NM[keys$SORTKEY==i])
	classds = gsub("/", '.', keys$CLS_DS[keys$SORTKEY==i])
	print(c(soarnm,classds))

	###########################################################################################
	###################################Plot Histogram for Each Key ############################
	###########################################################################################
	## Calculate culmulative density function and the grid points & 50% in-pattern/out-of-pattern threshold.
#	brks=seq(from=0,to=52*7,by=7); # 52 weeks.
#	fn=ecdf(dayint);
#	y=100.0*sapply(brks,fn);
#	RI_Threshold=50;
#	xRI_Threshold=approx(y,brks,RI_Threshold)$y;

	## Construct the histogram & curve & labels
#	pdf(file=paste(soarnm,classds,".pdf",sep=''));
#	h=hist(dayint,breaks=brks,
#  		,main=paste("Distribution of Members' Revisiting Frequency","\n", soarnm, classds," ", "KEY: ",i)
#		,xlab="Revisiting Interval (days)",ylab="Counts");
#	par(new=TRUE, mar = c(5,4,4,6) + 0.1);
#	plot(brks,y,type="l",col="red",xaxt="n",yaxt="n",xlab="",ylab="",ylim=c(0,100));
#	lines(xRI_Threshold,RI_Threshold,type="p",col="red");
#	text(xRI_Threshold+10,RI_Threshold-3,paste("RI=",toString(as.integer(xRI_Threshold)),sep=""));
#	lines(c(min(h$breaks),max(h$breaks)),c(0,100),col="blue");
#		axis(4);
#		mtext("Accumulative Probability (%)",side=4,line=3,adj=1,padj=1);
#		grid();
#	dev.off()

	############################END OF PLOTTING ##########################################


	#Identify the mode of each distribution
	epdf = density(dayint)
	x = epdf$x; y = epdf$y
#	mode = data.frame(i,soarnm,classds,mode=x[y==max(y)])
#	write.table(mode,file ="keys_modes.txt",append=T, sep=" ",col.names=F,row.names=F)


	########################################################################################
	########For each member, find the latest purchase, and predict purchase prob.###########
	#########################################################################################
	lastpurchase = onekey %>% group_by(lyl_id_no, sortkey) %>% summarize(daySince= min(daySince))
	##Approx FUN calculate the prob of each member, which is the score to give ranks--> Group into 10 deciles(rank 0-9)
	lastpurchase$prob = sapply(lastpurchase$daySince,FUN=function(daysince){approx(x,y,daysince)$y})  
	deciles = lastpurchase[order(lastpurchase$prob, decreasing =T),]
	deciles$rank = ceiling(1: nrow(deciles)*10/nrow(deciles))

	##Output the deciles.csv file for fastload
	write.table(deciles, "deciles.csv", append = T, sep=",", row.names =F, col.names =F)
}




