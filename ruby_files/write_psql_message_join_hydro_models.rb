class WritePSQLMessageJoinHydroModels
	attr_accessor :message, :conection, :river

	def initialize(args)
		@connection = args.fetch(:connection)
		@message = args.fetch(:message,'')
		@db = args.fetch(:db)
	end

	# Create a hash relating the GCMS to a specific random alphanumeric alias for SQL query 
	def generate_hash_for_gcm_join_alias
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

		@db.each do |db_name|
			key = arr.shift
			hash[key] = db_name
		end
		hash
	end	

	# Write the alias text for SQL Query 
	def write_alias_message(hash)
		message = []
		table_hash = {}
		hash.each_with_index do |(k, v), ii|
				if ii < 1
					@message << "#{k}.date as date,#{k}.q_avg as sf#{ii},"
				elsif ii >=1 && ii < hash.length-1
					@message << "#{k}.q_avg as sf#{ii},"
				else 
					@message << "#{k}.q_avg as sf#{ii}"
				end
		# table_hash[k] = "#{@river.name.downcase}_#{replace_hyphens(v)}_#{@river.rcp.downcase}_#{@river.downscale.downcase}_#{@river.hydro.downcase}"
		end
		# return @message, table_hash
		return @message
	end

		# Write SQL query header/SELECT statement to instance of message for join 
	def write_psql_head_message_join(header)
		@message = "CREATE TABLE temp AS\nSELECT\n#{header}\n"
	end	

	# Write the inner join text for SQL Query 
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

	# Write join of all tables into single table of just river data by date with single date column 
	def write_gcm_join
		hash = generate_hash_for_gcm_join_alias # Generate a hash of unique letters and numbers for the GCMs
		message_1 = write_alias_message(hash) # Generate the message and a hash of the table names
		hash_length = hash.length

		# Write the header 
		write_psql_head_message_join(message_1)

		# Write the body of the query (INNER JOIN)
		@message += write_inner_join_message(hash)
		@message += ";"
	end

	# Helper method 
	def replace_hyphens(string)
		string.gsub('-', '_')
	end
end


# Here is an example of how to merge all the columns into a new table 
# CREATE TABLE t4 AS
# SELECT 
# l.date as date,l.streamflow as sfl,r.streamflow as sfr,m.streamflow as sfm
# FROM t1 l
# INNER JOIN t2 r
# ON l.date = r.date
# INNER JOIN t3 m
# ON l.date = m.date;