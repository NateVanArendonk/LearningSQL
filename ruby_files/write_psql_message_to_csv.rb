class WritePSQLMessageToCSV
	attr_accessor :message, :conection, :river

	def initialize(args)
		@connection = args.fetch(:connection)
		@message = args.fetch(:message,'')
		@river = args.fetch(:river)
	end


	# Wriet SQL query header/SELECT statement to instance of message for computing AVG
	def write_psql_head_message_average
		@message = "CREATE TABLE #{new_table_name} AS\n" 
	end

	# Write row-wise averaging of the rivers in the temp table 
	def write_to_csv(table, out_file)
        @message << "COPY #{table} TO '#{out_file}' DELIMITER ',' CSV HEADER;"
	end

	def write_success_to_csv_message(table)
		"Successfully added table #{table}"
	end
end


# COPY persons TO 'C:\tmp\persons_db.csv' DELIMITER ',' CSV HEADER;
