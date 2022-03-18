require 'pg'
require_relative('river.rb')
require_relative('connect_psql')
require_relative('write_psql_message_join_hydro_models')
require_relative('write_psql_message_average_hydro_models')

# All necessary inputs 
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

rcp_list.each do |rcp| # Loop through rcp scenarios
	river_list.each do |riv|

		# New instance of River class 
		river = River.new(riv, rcp, 'VIC_P1', 'BCSD', '') # Dummy class 

		# Delete temp table to start 
		query_delete = WritePSQLMessageAverageHydroModels.new(
			:connection => db_connection,
			:river => river,
		)
		query_delete.write_drop_temp_table
		db_connection.execute_query(query_delete.message)

		# Get List of the model to average together 
		river_db_names = []
		hydro_list.each do |hydro|
			river = River.new(riv, rcp, hydro, 'BCSD') 
			river_db_names.append("#{river.name.downcase}_#{river.rcp.downcase}_#{river.downscale.downcase}_#{river.hydro.downcase}_mean")
		end

		query_join = WritePSQLMessageJoinHydroModels.new(
			:connection => db_connection,
			:db => river_db_names
		)
		# Execute SQL Query 
		query_join.write_gcm_join
		db_connection.execute_query(query_join.message)

		# Generate instance of SQL query for AVG
		query_avg = WritePSQLMessageAverageHydroModels.new(
			:connection => db_connection,
			:river => river,
			:db => river_db_names
		)

		# # Ensure the table we'll be making is deleted if it exists 
		db_connection.execute_query(query_avg.write_delete_new_table)
		# Execute SQL Query 
		query_avg.write_average_discharges
		db_connection.execute_query(query_avg.message)

		# Output success message to terminal 
		puts query_avg.write_success_average_message
	end
end
# # Close connection
# db_connection.close
