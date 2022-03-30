require 'pg'
require_relative('river.rb')
require_relative('connect_psql')
require_relative('write_psql_message_to_csv')

# All necessary inputs 
river_list = ['AUB', 'CED', 'CS2', 'DUNDU', 'ELWPO', 'LAG', 'NFSTI', 'NOOFE', 'PUY', 'SKAMO', 'SKOPO', 'SNOMO']
rcp_list = ['RCP45', 'RCP85']
out_fol = 'C:\usgs_vanarendonk\datasets\fluvial\uw_hydro_crcc_data\postgresql_processed_csv'.gsub(/\\/,'/')


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
        river = River.new(riv, rcp, 'VIC_P1', 'BCSD', []) # dummy class for now
        
        # Delete temp table to start 
        query_to_csv = WritePSQLMessageToCSV.new(
            :connection => db_connection,
            :river => river
        )
        
        # Name of database to grab 
        db_name = "#{riv.downcase}_#{rcp.downcase}_bcsd_hydro_mean"
        out_file = "#{out_fol}/#{db_name}.csv"
        csv_message = query_to_csv.write_to_csv(db_name, out_file)
        db_connection.execute_query(csv_message)
        puts query_to_csv.write_success_to_csv_message(db_name)


    end
end



# COPY persons TO 'C:\tmp\persons_db.csv' DELIMITER ',' CSV HEADER;
