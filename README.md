# mlpp_etlassign1
Code for mlpp class assignment 1. 

This assignment collects data from the state of Hawaii about energy sources to heat homes 
and demographic data at the block track census level. As the US continues to shift towards
more renewable energy sources, it could be interesting to examine how the number of solar-
heated homes compares to the number of homes heated by conventional fuels, like gas. It would 
also be interesting to see if these numbers are correlated with personal income. Hawaii 
offers an interesting case study because it is an island, and therefore may have a different
energy portfolio than states on the mainland. 

I chose to use the 5 year census estimates centered on the year 2019 because 2019 was the 
most recent year of data collection and a 5-year estimate will lessen the chance of errors driven
by random variation in the data. Since census blocks are the smallest reported geographical area, 
they are more likely to experience extreme (but random) fluxuations from year to year. Using a 
5-year estimate smooths these random fluxuations. 

I transformed my data by breaking up the "NAME" field into discrete columns and deleting redundant 
columns. I kept the columns with the state and county name to make the data more easy to reference. 
I also changed the string API data into numeric columns where appropriate. Lastly, I replaced the 
placeholder NaN value of -666666666 with an actual NaN value to allow the data to be summarized more 
easily. 

To upload my data to the PostgreSQL database, I used an Ohio extension designed to work with pandas
dataframes, pd_copy_to. In my engine, I used a connection string formatted as 
'dialect+driver://username:password@host:port/database' and changed the search path to upload the 
table to the acs schema instead of the public schema. 
