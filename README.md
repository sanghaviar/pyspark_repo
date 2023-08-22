1. Convert the Issue Date with the timestamp format. 

Example:  

Input: 1648770933000 -> Output: 2022-03-31T23:55:33.000+0000 

Input: 1648770999000 -> Output: 2022-03-31T23:56:39.000+0000 

Input: 1648770948000 -> Output: 2022-03-31T23:55:48.000+0000 

2. Convert timestamp to date type    

Example: Input: 2022-03-31T23:55:33.000+0000 -> Output: 2022-03-31 

Remove the starting extra space in Brand column for LG and Voltas fields 
Replace null values with empty values in Country column 



3. Change the camel case columns to snake case  

Example: SourceId: source_id, TransactionNumber: transaction_number 

Add another column as start_time_ms and convert the values of StartTime to milliseconds. 

Example:  

Input: 2021-12-27T08:20:29.842+0000 -> Output: 1640593229842 

Input: 2021-12-27T08:21:14.645+0000 -> Output: 1640593274645 

Input: 2021-12-27T08:22:42.445+0000 -> Output: 1640593362445 

Input: 2021-12-27T08:22:43.183+0000 -> Output: 1640593363183 


4.Combine both the tables based on the Product Number  

and get all the fields in return. 
And get the country as EN
