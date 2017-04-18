V = LOAD '/home/edureka/SAIWS/Dataset/PIG/Visits.csv' USING PigStorage(',') 
	AS (user:chararray,url:chararray,timestamp:chararray);
P = LOAD '/home/edureka/SAIWS/Dataset/PIG/Pages.csv' USING PigStorage(',') AS (url:chararray,prnk:float);                         
J = JOIN V by url,P by url;                                                                       
F = Filter J by P::prnk > 0.5; 
STORE F INTO '/home/edureka/SAIWS/Dataset/PIG/TEMPSTORAGE' ; 
