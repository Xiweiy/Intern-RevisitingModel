revisiting.sh - The overall script, will call the 'revisiting.r' and 'validation.r'
		step 0-3   Create the table of powerful sortkeys that with >5% revisiting members and 500k transactions in the past year.
		step 4-6   Output all the transaction records related to the powerful sortkeys in the last year as 'powerkeytrs.txt', as well as powerful sortkey info as 'POWERKEYS.txt'
		step 7     call R script 'revisiting.r' to generate the plots, and predict the probability of purchase intent. R output a file 'deciles.csv' with each member's purchase prob. and the decile determined by the purchase prob.
		step 8     fastload the 'deciles.csv' onto Teradata
		step 9-10  find out the actual purchase activity of the members within each decile in the next week. And output the # transactions, # revisiting members of each decile in each sortkey
		step 11    call R script 'validation.r' to generate the validation plots for each key

revisiting.r - The R script used in step 7 of revisiting.sh
		Aim to (1) Create the histograms of # of revisiting members versus day_interval btw two consecutive purchases of same person. (This section is currently commentted off) (2) Group by lyl_id_no and sortkey, predict each member's purchase probability based on their last purchase as well as the fitted curve of the histogram. (3) Output the result in 'deciles.csv'

validation.r - The R script used in step 11 of revisiting.sh
		Read in the # one week revisiting members, # one week transactions, # two week revisiting members, # two week transactions. Calculate the percentage of transaction in each decile, the overall purchase prob, and the # of times first decile purchase prob. is higer than overall purchase prob.
		Generate the validation plots for each sortkey. Also output a 'boost.csv' to store the resulting statistics associated with each sortkey.

