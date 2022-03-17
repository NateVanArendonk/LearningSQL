#####################################################################################################################################
# NOTE THIS CODE IS DEPRECIATED AND KEPT FOR RECORD KEEPING. PLEASE SEE 'Compute_inter_model_average_discharge' FOR UPDATED VERSION #
#####################################################################################################################################

require 'pg'

class River 
	
	attr_accessor :name, :rcp, :hydro, :downscale, :gcm

	def initialize(name, rcp, hydro, downscale, gcm)
		@name = name
		@rcp = rcp
		@hydro = hydro
		@downscale = downscale
		@gcm = gcm
	end

	def db_table_name
		"#{name.downcase}_#{gcm.downcase}_#{rcp.downcase}_#{downscale.downcase}_#{hydro.downcase}"
	end
end

class ConnectPSQL
	
	attr_accessor :connection_status

	def initialize(args)
		@host = args.fetch(:host)
		@port = args.fetch(:port)
		@dbname = args.fetch(:dbname)
		@user = args.fetch(:user)
		@password = args.fetch(:password)
		@connection_status = args.fetch(:connection_status, 'closed')
	end

	def connect
		# Connect to PSQL database
		@connection_status = PG.connect(
			:host => @host,
			:port => @port, 
			:dbname => @dbname, 
			:user => @user,
			:password => @password
			)
	end

	def execute_query(query)
		@connection_status.exec(query)
	end

	def close
		@connection_status.close
	end
end

class WritePSQLMessage
	attr_accessor :message, :conection, :river

	def initialize(args)
		@connection = args.fetch(:connection)
		@message = args.fetch(:message,'')
		@river = args.fetch(:river)
	end

	# Create name name for table of averaged river discharges 
	def new_table_name
		"#{@river.name.downcase}_#{@river.rcp.downcase}_#{@river.downscale.downcase}_#{@river.hydro.downcase}_mean"		
	end

	# Write SQL query header/SELECT statement to instance of message for join 
  def write_psql_head_message_join(header)
    @message = "CREATE TABLE temp AS\nSELECT\n#{header}\n"
  end	

	# Wriet SQL query header/SELECT statement to instance of message for computing AVG
	def write_psql_head_message_average
		@message = "CREATE TABLE #{new_table_name} AS\n" 
	end

	# Write join of all tables into single table of just river data by date with single date column 
	def write_gcm_join(gcm_list)
		hash = generate_hash_for_gcm_join_alias(gcm_list) # Generate a hash of unique letters and numbers for the GCMs
  	message_1, table_hash = write_alias_message(hash) # Generate the message and a hash of the table names
  	hash_length = table_hash.length

		# Write the header 
		write_psql_head_message_join(message_1)

		# Write the body of the query (INNER JOIN)
		@message += write_inner_join_message(table_hash)
		@message += ";"
	end
	
	# Write row-wise averaging of the rivers in the temp table 
	def write_average_discharges(gcm_list)
		@message << 'SELECT *,\n'
		@message << '       (SELECT AVG(Col)\n'
		gcm_list.each_with_index do |gcm, ii|
			if ii < 1
				@message << "        FROM   (VALIES(sf#{ii}),\n"
			elsif ii >= 1 && ii < gcm_list.length
				@message << "                      (sf#{ii}),\n"
			else
				@message << "                      (sf#{ii})) V(Col)) AS q_avg\n"
			end
		end
		@message << "FROM temp;"
	end

	def write_drop_temp_table
		@message = 'DROP TABLE temp;'
	end

	# Write the inner join SQL Query text 
  def write_inner_join_message(hash)
    counter = 1
    message = []
    key_1 = hash.keys.first
    hash.each do |k, v|
      if counter == 1
        message << "FROM #{v} #{k}"
      else
        message << "INNER JOIN #{v} #{k}\nON #{key_1}.date = #{k}.date"
      end
      counter += 1
    end
    message.join("\n")
  end

	# Write the alias text 
  def write_alias_message(hash)
    message = []
    table_hash = {}
    hash.each_with_index do |(k, v), ii|
			if ii < 1
				@message << "#{k}.date as date,#{k}.streamflow as sf#{ii},"
			elsif ii >=1 && ii < hash.length
				@message << "#{k}.streamflow as sf#{ii},"
			else 
				@message << "#{k}.streamflow as sf#{ii}"
			end
      @river.gcm = replace_hyphens(v)
      table_hash[k] = @river.db_table_name
    end
    return @message, table_hash
  end

	# Create a hash relating the GCMS to a specific random alphanumeric alias for SQL query 
	def generate_hash_for_gcm_join_alias(gcm_list)
		hash = {}
		alphabet = ('a'..'z').to_a 
		numbers = ('1'..'9').to_a
		arr = []
		
		# Make unique array of letters and numbers 
		alphabet.each do |letter|
			numbers.each do |num|
				arr << letter + num
			end
		end
		arr = arr.shuffle

		gcm_list.each do |gcm|
			key = arr.shift
			hash[key] = gcm
		end
		hash
	end	

	# Helper method
	def replace_hyphens(string)
		string.gsub('-', '_')
	end
end


# Example River object
gcm_list = ['CanESM2', 'CCSM4', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'GFDL-ESM2M','HadGEM2-CC', 'IPSL-CM5A-MR', 'inmcm4', 'MIROC5']
river = River.new('AUB', 'RCP85', 'VIC_P1', 'BCSD', 'CanESM2') 

# Connect to PSQL database
db_connection = ConnectPSQL.new(
	:host => 'localhost', 
	:port => 5432,
	:dbname => 'discharge',
	:user => 'postgres',
	:password => 'password'
)
db_connection.connect

# Generate SQL query for join 
query_join = WritePSQLMessage.new(
	:connection => db_connection,
	:river => river
)

# Genearte hash for join SQL query 
query_join.generate_hash_for_gcm_join_alias(gcm_list)

# Write SQL join query
query_join.write_gcm_join(gcm_list)

# Execute SQL join query
# results = db_connection.execute_query(query.message)

# Generate SQL query for averaging discharge data 
query_avg = WritePSQLMessage.new(
	:connection => db_connection, 
	:river => river
)

# Close connection
db_connection.close

# puts results.to_a


# SELECT 
# i9.date,i9.streamflow,y2.date,y2.streamflow,c7.date,c7.streamflow,t9.date,t9.streamflow,v5.date,v5.streamflow,o6.date,o6.streamflow,f7.date,f7.streamflow,u8.date,u8.streamflow,k8.date,k8.streamflow
# FROM aub_canesm2_rcp85_bcsd_vic_p1 i9
# INNER JOIN aub_ccsm4_rcp85_bcsd_vic_p1 y2
# ON i9.date = y2.date
# INNER JOIN aub_cnrm_cm5_rcp85_bcsd_vic_p1 c7
# ON i9.date = c7.date
# INNER JOIN aub_csiro_mk3_6_0_rcp85_bcsd_vic_p1 t9
# on i9.date = t9.date
# INNER JOIN aub_gfdl_esm2m_rcp85_bcsd_vic_p1 v5
# ON i9.date = v5.date
# INNER JOIN aub_hadgem2_cc_rcp85_bcsd_vic_p1 o6
# ON i9.date = o6.date
# INNER JOIN aub_ipsl_cm5a_mr_rcp85_bcsd_vic_p1 f7
# on i9.date = f7.date
# INNER JOIN aub_inmcm4_rcp85_bcsd_vic_p1 u8
# ON i9.date = u8.date
# INNER JOIN aub_miroc5_rcp85_bcsd_vic_p1 k8
# ON i9.date = k8.date;




# Here is an example of how to merge all the columns into a new table 
# CREATE TABLE t4 AS
# SELECT 
# l.date as date,l.streamflow as sfl,r.streamflow as sfr,m.streamflow as sfm
# FROM t1 l
# INNER JOIN t2 r
# ON l.date = r.date
# INNER JOIN t3 m
# ON l.date = m.date;