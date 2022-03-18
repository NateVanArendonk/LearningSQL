class DateQuery

    attr_accessor :time_range, :message, :table

    def initialize(time_range, table=nil, message = '')
        @time_range = time_range
        @table = table
        @message = message
    end

    def create_date_bracketing_query(date_name='date', column_want='q_avg')
        table_name = @table.table_name
        new_table_name = create_date_specific_table_name(@time_range[:time_frame])

        @message << "CREATE TABLE #{new_table_name} AS\n"
        @message << "SELECT #{date_name}, #{column_want} FROM #{table_name} t\n"
        @message << "WHERE t.date <= '#{@time_range[:stop]}'\nAND t.date >= '#{@time_range[:start]}';"
    end

    def create_date_specific_table_name(time_period = '')
        "#{@table.river.name.downcase}_#{@table.river.rcp.downcase}_#{@table.river.downscale.downcase}_#{time_period}"
    end

    def write_delete_new_table
		"DROP TABLE IF EXISTS #{create_date_specific_table_name(@time_range[:time_frame])}"
	end

    def write_success_message
        "Successfully added #{create_date_specific_table_name(@time_range[:time_frame])}\n"
    end
end