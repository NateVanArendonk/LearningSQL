require 'csv'
require 'pg'
require 'faker'

def replace_hyphens(string)
	string.gsub('-', '_')
end


# Models for each river 
models = ['CanESM2', 'CCSM4', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'GFDL-ESM2M','HadGEM2-CC', 'IPSL-CM5A-MR', 'inmcm4', 'MIROC5']
# RCP Scenarios
rcp = ['RCP45', 'RCP85']
# Hydrology models 
hydro_model = ['PRMS_P1', 'VIC_P1', 'VIC_P2', 'VIC_P3']
# Statistical downscaling method used 
downscale = ['BCSD', 'MACA']
# Rivers to analyze 
rivers = ['AUB', 'CED', 'CS2', 'DUNDU', 'ELWPO', 'LAG', 'NFSTI', 'NOOFE', 'PUY', 'SKAMO', 'SKOPO', 'SNOMO']
# rivers = ['ELWPO', 'LAG', 'NFSTI', 'NOOFE', 'PUY', 'SKAMO', 'SKOPO', 'SNOMO']
# Bias corrected or not 
bias_corrected = ['biascorrected_streamflow', 'streamflow', 'biascorrected_streamflow', 'streamflow', 'streamflow', 'biascorrected_streamflow','streamflow', 'streamflow', 'streamflow', 'streamflow', 'streamflow', 'streamflow']

# # For Debugging purposes 
# models = models[0..1]
# rcp = rcp[0..1]
# hydro_model = hydro_model[0..1]
# downscale = downscale[0..1]
# rivers = rivers[0..1]
# bias_corrected = bias_corrected[0..1]

# Total files 
total_files = models.length * rcp.length * hydro_model.length * downscale.length * rivers.length

# Data folder (UWHYDRO)
data_folder = 'E:\F_Drive_Data\fluvial\uw_hydro_crcc_data'.gsub(/\\/,'/')
temp_folder = 'F:\analysis\fluvial\uw_hydro_forecast\ruby_files'.gsub(/\\/,'/')
# C:\usgs_vanarendonk\datasets\fluvial\uw_hydro_crcc_data\AUB\biascorrected_streamflow
# Make a counter for the bias correction folder 
count = 0
file_counter = 1;
# Loop through each river 
rivers.each do |river|
	bias_correct_folder = bias_corrected[count]
		# Loop through each global climate model
		models.each do |model|
			# Loop through each rcp scenario
			rcp.each do |rcp_scenario|
				# Loop through each hydrology model
				hydro_model.each do |hydro_scenario|
					# Loop through each downscaling method
					downscale.each do |downscaling|
						starting = Process.clock_gettime(Process::CLOCK_MONOTONIC)
						# Get name of file to be read
						file_name = "#{model}_#{rcp_scenario}_#{downscaling}_#{hydro_scenario}-#{river}-#{bias_correct_folder}-1.0.csv"
						# Make full path to file
						full_file_path = "#{data_folder}/#{river}/#{bias_correct_folder}/#{file_name}"

							# Read in data to array: Note header for each file extends from idx 0..38
							content = CSV.readlines(full_file_path)[39..] 

							# Add row ID to conent array (PRIMARY KEY)
							row_id = [*1..content.length]
							content.each_with_index do |row, idx|
								content[idx] = [row_id[idx]] + row
							end

							# Add model name to each row 
							content.each do |line|
								line.push(model, rcp_scenario, downscaling, hydro_scenario)
							end

							# Write data to new CSV file
							header = 'id,date,streamflow,model,rcp_scenario,downscaling,hydro_model'
							CSV.open('temp.csv', 'w', write_headers: true, headers: header) do |csv|
								content.each do |line|
									csv << line
								end
							end

							# Connect to PSQL database
							conn = PG.connect(
								:host => "localhost",
								:port => 5432, 
								:dbname => "discharge", 
								:user => "postgres", 
								:password => "password"
								)

							# Create table
							table_name = "#{river}_#{model}_#{rcp_scenario}_#{downscaling}_#{hydro_scenario}"
							table_name = replace_hyphens(table_name)
							drop_phrase = "DROP TABLE IF EXISTS #{table_name}"
							conn.exec(drop_phrase)
							create_phrase = "CREATE TABLE IF NOT EXISTS #{table_name} (id INTEGER PRIMARY KEY, date DATE, streamflow FLOAT(3), model VARCHAR(255), rcp_scenario VARCHAR(255), downscaling VARCHAR(255), hydro_model VARCHAR(255))"
							conn.exec(create_phrase)

							# Insert data into database
							insert_phrase = "COPY #{table_name} FROM '#{temp_folder}/temp.csv' DELIMITER ',' CSV HEADER"
							conn.exec(insert_phrase)

							# Close connection
							conn.close

							# Delete temp file 
							File.delete('temp.csv')

							# Increment file counter
							ending = Process.clock_gettime(Process::CLOCK_MONOTONIC)
							elapsed = ending - starting
							count_phrase =  "Done with #{file_counter} of #{total_files} files. Completed in #{elapsed} seconds."
							puts count_phrase
							file_counter += 1
					end
				end
			end
		end
	count += 1
end