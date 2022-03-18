require 'pg'
require_relative('river.rb')
require_relative('connect_psql')
require_relative('date_query')
require_relative('sql_table_river')

# All necessary inputs 
river_list = ['AUB', 'CED', 'CS2', 'DUNDU', 'ELWPO', 'LAG', 'NFSTI', 'NOOFE', 'PUY', 'SKAMO', 'SKOPO', 'SNOMO']
downscale_list = ['BCSD']#, 'MACA']
rcp_list = ['RCP45', 'RCP85']
time_list = [{start: '1950-01-01', stop: '2006-01-01',time_frame: 'historic'}, {start: '2030-01-01', stop: '2059-01-01',time_frame: 'future_1'},{start: '2070-01-01', stop: '2099-01-01',time_frame: 'future_2'}]
# time_list = [{start: '2070-01-01', stop: '2099-01-01',time_frame: 'future_2'}]


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
		river = River.new(riv, rcp, '', 'BCSD', '') 
        column_headers = ['date','sf0','sf1','sf2','sf3','q_avg'] # Kinda useless mostly metadata for me 
        sql_table = SqlTableRiver.new(river,column_headers) # Create table object 
        time_list.each do |time| # Loop through the times of interest
            my_query = DateQuery.new(time,sql_table,'') # Make a new query object
            my_query.create_date_bracketing_query('date','q_avg') # generate the query text
            del_query = my_query.write_delete_new_table # generate the query text to delete the table if it exists
            
            db_connection.execute_query(del_query) # Execute the delete 
            sleep 0.2
            db_connection.execute_query(my_query.message) # Execute the query 
            sleep 0.2
            puts my_query.write_success_message # Write success message 
        end
    end
end
db_connection.close



# Example Query 
# SELECT q_avg FROM aub_rcp45_bcsd_hydro_mean t
# WHERE t.date <= '1970-01-05' 
# AND t.date >= '1970-01-01';


