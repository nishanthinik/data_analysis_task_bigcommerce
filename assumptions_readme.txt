The order_items file doens't have the informations for all the orders and it is not clear enough to use for any analysis. Hence cannot do any further analysis on Product level too
The order data - the KPI definitions are required to do any further analysis. hence taken few assumptions
1. Exchange Rate for EUR to USD as 1.16(as of today from web)
2. All the amounts are in respective currency in currency column
3. total_inc_tax taken as total sales amount with tax paid
4. Shipping cost beared by the business hence a cost
5. Revenue = total_inc_tax - shipping_cost- discount_amount (assummed no additional  cost incurred)

Since I don't have any further context on this data insights, taken above assumptions and did the analysis accordingly


No specific storage is used. Used inbuilt databricks storage and created volumes according to Medallion structure

filestorage - consider as source(where file arrives daily)
raw_data - to append every new oncoming data as it is
stage_date - to clean and merge the data using the PKL at entity level
processed_data - transofrmed data with more info for further analysis

Assumed all the info are general data and no security required.