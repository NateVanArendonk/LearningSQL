require 'csv'
require 'pg'
require 'faker'


# Data folder & CSV files (UWHYDRO)
data_folder = 'F:\data\fluvial\nooksack\uw_hydro\historic'.gsub(/\\/,'/')
time_frame = 'historic'
files = Dir[data_folder + '/*.csv']
file = files[0]

# Get name of model
model_name = file.gsub('-NOOFE-streamflow-1.0.csv', '').gsub(data_folder + '/', '').gsub('historical_livneh_','')

# Note header for each file extends from idx 0..38
content = CSV.readlines(file)[39..]

# Add row ID to conent array
row_id = [*1..content.length]
content.each_with_index do |row, idx|
	content[idx] = [row_id[idx]] + row
end

# Add model and time frame to each row
content.each do |line|
	line.push(model_name, time_frame)
end

# Write data to new CSV file
header = 'id,date,streamflow,model,time_frame'
CSV.open('test_file.csv', 'w', write_headers: true, headers: header) do |csv|
	content.each do |line|
		csv << line
	end
end

# Connect to database
conn = PG.connect(
	:host => "localhost",
	:port => 5432, 
	:dbname => "discharge", 
	:user => "postgres", 
	:password => "password"
	)

# Create table
conn.exec("DROP TABLE IF EXISTS nook_test")
conn.exec("CREATE TABLE IF NOT EXISTS nook_test (id INTEGER, date DATE, streamflow FLOAT(3), model VARCHAR(255), time_frame VARCHAR(255))")

# Insert data into database
conn.exec("COPY nook_test FROM 'F:/data/fluvial/analysis_ruby/test_file.csv' DELIMITER ',' CSV HEADER")
