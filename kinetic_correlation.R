kinetic_correlation=function(feature1,feature2)
{
if(length(feature1)!=length(feature2))
{
  print("Features must have same length")
}
else
{
  freq1=table(feature1)
  prob1=freq1/length(feature1)
  energy1=0
  for( i in c(1:length(prob1)) )
  {
    energy1=energy1+(prob1[[i]]^2)
  }
  freq2=table(feature2)
  prob2=freq2/length(feature2)
  energy2=0
  for( i in c(1:length(prob2)) )
  {
    energy2=energy2+(prob2[[i]]^2)
  }
  #class_prob1=merge(as.data.frame(feature1),as.data.frame(prob1),by="feature1")
  #class_prob2=merge(as.data.frame(feature2),as.data.frame(prob2),by="feature2")
  #c=as.numeric(class_prob1$Freq%*%class_prob2$Freq)
  c=prob1%*%prob2
  energy=c/sqrt(energy1*energy2)
  return(as.numeric(energy))
  
}
}
