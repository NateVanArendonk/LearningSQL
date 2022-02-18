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

# Total files 
total_files = models.length * rcp.length * hydro_model.length * downscale.length * rivers.length

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
						fix_phrase = "UPDATE #{table_name} SET streamflow = NULL WHERE streamflow = -9999"
						conn.exec(fix_phrase)

						# Close connection
						conn.close
						
						# Increment file counter
						count_phrase =  "Done with #{file_counter} of #{total_files} files."
						puts count_phrase
						file_counter += 1
					end
				end
			end
		end
	count += 1
end