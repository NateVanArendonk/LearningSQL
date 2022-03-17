require 'pg'
require_relative('river.rb')
require_relative('connect_psql')
require_relative('write_psql_message_join')
require_relative('write_psql_message_average')

# All necessary inputs 
gcm_list = ['CanESM2', 'CCSM4', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'GFDL-ESM2M','HadGEM2-CC', 'IPSL-CM5A-MR', 'inmcm4', 'MIROC5']
river_list = ['AUB', 'CED', 'CS2', 'DUNDU', 'ELWPO', 'LAG', 'NFSTI', 'NOOFE', 'PUY', 'SKAMO', 'SKOPO', 'SNOMO']
hydro_list = ['PRMS_P1', 'VIC_P1', 'VIC_P2', 'VIC_P3']
downscale_list = ['BCSD', 'MACA']
rcp_list = ['RCP45', 'RCP85']

# Connect to PSQL database
db_connection = ConnectPSQL.new(
	:host => 'localhost', 
	:port => 5432,
	:dbname => 'discharge',
	:user => 'postgres',
	:password => 'password'
)
db_connection.connect

hydro_list.each do |hydro| # Loop through hydro models
	rcp_list.each do |rcp| # Loop through rcp scenarios 
		river_list.each do |riv|
			# New instance of River class 
			river = River.new(riv, rcp, hydro, 'BCSD', gcm_list) 

			# Delete temp table to start 
			query_delete = WritePSQLMessageAverage.new(
				:connection => db_connection,
				:river => river
			)
			query_delete.write_drop_temp_table
			db_connection.execute_query(query_delete.message)

			# Generate instance of SQL query for JOIN 
			query_join = WritePSQLMessageJoin.new(
				:connection => db_connection,
				:river => river
			)
			query_join.write_gcm_join
			# Execute SQL Query 
			db_connection.execute_query(query_join.message)

			# Generate instance of SQL query for AVG
			query_avg = WritePSQLMessageAverage.new(
				:connection => db_connection,
				:river => river
			)
			# Ensure the table we'll be making is deleted if it exists 
			db_connection.execute_query(query_avg.write_delete_new_table)
			query_avg.write_average_discharges
			# Execute SQL Query 
			db_connection.execute_query(query_avg.message)

			# Output success message to terminal 
			puts query_avg.write_success_average_message
		end
	end
end
# Close connection
db_connection.close
