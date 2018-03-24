require_relative "connection"

module PgSlice
  class Fill
    attr_reader :table, :options, :conn, :batch_size

    def initialize(table, options = {})
      @table = table
      @options = options
      @batch_size = options[:batch_size] || 1000
      @conn = Connection.new(options[:url])
    end

    def run
      Signal.trap("INT") { |signo| @aborted = true; puts "\n\n!!! Got signal INT. Aborting ..." }

      abort "Table not found: #{source_table}" unless conn.table_exists?(source_table)
      abort "Table not found: #{dest_table}" unless conn.table_exists?(dest_table)

      settings = settings_from_trigger
      @trigger_created_at = Time.at(settings["created_at"].to_i).utc

      if options[:batch_by]
        batch_by_generic_column
      else
        batch_by_numeric_primary_key
      end
    end

    private

    def source_table
      @source_table ||= options[:source_table] || (options[:swapped] ? retired_name(table) : table)
    end

    def dest_table
      @dest_table ||= options[:dest_table] || (options[:swapped] ? table : intermediate_table)
    end

    def batch_by_generic_column
      column = options[:batch_by]

      starting_val = get_starting_val(column)
      unless starting_val
        abort "All data have been copied?"
      end

      ending_val = get_ending_val(column)
      if ending_val
        query = "SELECT COUNT(*) AS cnt FROM #{conn.quote_ident(source_table)} WHERE created_at = '#{ending_val}'"
        rows = conn.execute(query)
        if rows[0]["cnt"].to_i > 1
          abort "More than one rows with #{column} = #{ending_val}"
        end
      end

      fields = conn.columns(source_table).map { |c| conn.quote_ident(c) }.join(", ")

      batch_count = 1
      loop do
        where = "#{conn.quote_ident(column)} >= '#{starting_val}'"

        batch_ending_val = get_batch_ending_val(column, starting_val)
        if ending_val && batch_ending_val > ending_val
          batch_ending_val = ending_val
        end
        if batch_ending_val
          where << " AND #{conn.quote_ident(column)} < '#{batch_ending_val}'"
        end

        if options[:where]
          where << " AND #{options[:where]}"
        end

        query = <<-SQL
          /* batch ##{batch_count} at #{Time.now} */
          INSERT INTO #{conn.quote_ident(dest_table)} (#{fields})
            SELECT #{fields} FROM #{conn.quote_ident(source_table)}
            WHERE #{where}
        SQL

        conn.run_query(query)

        if batch_ending_val
          starting_val = batch_ending_val
        else
          puts "All done!"
          exit(0)
        end

        if options[:sleep] && starting_id <= max_source_id
          sleep(options[:sleep])
        end

        batch_count += 1

        if @aborted
          puts "Task is cancelled."
          exit(0)
        end
      end
    end

    def batch_by_numeric_primary_key
      period, field, cast, _, declarative = settings_from_trigger(table, dest_table)

      if period
        name_format = self.name_format(period)

        existing_tables = existing_partitions(table)
        if existing_tables.any?
          starting_time = DateTime.strptime(existing_tables.first.split("_").last, name_format)
          ending_time = advance_date(DateTime.strptime(existing_tables.last.split("_").last, name_format), period, 1)
        end
      end

      schema_table = period && declarative ? existing_tables.last : table
      primary_key = self.primary_key(schema_table)
      abort "No primary key" unless primary_key
      max_source_id = max_id(source_table, primary_key)

      max_dest_id =
        if options[:start]
          options[:start]
        elsif options[:swapped]
          max_id(dest_table, primary_key, where: options[:where], below: max_source_id)
        else
          max_id(dest_table, primary_key, where: options[:where])
        end

      if max_dest_id == 0 && !options[:swapped]
        min_source_id = min_id(source_table, primary_key, field, cast, starting_time, options[:where])
        max_dest_id = min_source_id - 1 if min_source_id
      end

      starting_id = max_dest_id
      fields = columns(source_table).map { |c| conn.quote_ident(c) }.join(", ")
      batch_size = options[:batch_size]

      i = 1
      batch_count = ((max_source_id - starting_id) / batch_size.to_f).ceil

      if batch_count == 0
        log_sql "/* nothing to fill */"
      end

      while starting_id < max_source_id
        where = "#{conn.quote_ident(primary_key)} > #{starting_id} AND #{conn.quote_ident(primary_key)} <= #{starting_id + batch_size}"
        if starting_time
          where << " AND #{conn.quote_ident(field)} >= #{sql_date(starting_time, cast)} AND #{conn.quote_ident(field)} < #{sql_date(ending_time, cast)}"
        end
        if options[:where]
          where << " AND #{options[:where]}"
        end

        query = <<-SQL
/* #{i} of #{batch_count} */
INSERT INTO #{conn.quote_ident(dest_table)} (#{fields})
    SELECT #{fields} FROM #{conn.quote_ident(source_table)}
    WHERE #{where}
        SQL

        run_query(query)

        starting_id += batch_size
        i += 1

        if options[:sleep] && starting_id <= max_source_id
          sleep(options[:sleep])
        end
      end
    end

    def intermediate_table
      "#{table}_intermediate"
    end

    def max_id(table, primary_key, below: nil, where: nil)
      query = "SELECT MAX(#{conn.quote_ident(primary_key)}) FROM #{conn.quote_ident(table)}"
      conditions = []
      conditions << "#{conn.quote_ident(primary_key)} <= #{below}" if below
      conditions << where if where
      query << " WHERE #{conditions.join(" AND ")}" if conditions.any?
      execute(query)[0]["max"].to_i
    end

    def min_id(table, primary_key, column, cast, starting_time, where)
      query = "SELECT MIN(#{conn.quote_ident(primary_key)}) FROM #{conn.quote_ident(table)}"
      conditions = []
      conditions << "#{conn.quote_ident(column)} >= #{sql_date(starting_time, cast)}" if starting_time
      conditions << where if where
      query << " WHERE #{conditions.join(" AND ")}" if conditions.any?
      (execute(query)[0]["min"] || 1).to_i
    end

    def get_batch_ending_val(column, starting_val)
      query = <<-SQL
        SELECT #{conn.quote_ident(column)} val FROM #{conn.quote_ident(source_table)}
          WHERE #{conn.quote_ident(column)} >= '#{starting_val}'
          ORDER BY #{conn.quote_ident(column)} OFFSET #{batch_size} LIMIT 1
      SQL
      rows = conn.execute(query)
      return if rows.empty?

      rows[0]["val"]
    end

    def get_next_val(column, starting_val)
      query = <<-SQL
        SELECT #{conn.quote_ident(column)} val FROM #{conn.quote_ident(source_table)}
          WHERE #{conn.quote_ident(column)} > '#{starting_val}'
          ORDER BY #{conn.quote_ident(column)} OFFSET 1 LIMIT 1
      SQL
      rows = conn.execute(query)
      return if rows.empty?

      rows[0]["val"]
    end

    def get_starting_val(column)
      timestamp = @trigger_created_at.strftime("%Y-%m-%d %H:%M:%S")
      timestamp = '2018-03-23 11:59:56.860362' # time difference between server and work station
      query = "SELECT MAX(#{conn.quote_ident(column)}) AS val FROM #{conn.quote_ident(dest_table)} WHERE created_at < '#{timestamp}'"
      rows = conn.execute(query)
      return default_starting_val(table, column) if rows.empty? || rows[0]["val"].nil?

      get_next_val(column, rows[0]["val"])
    end

    def get_ending_val(column)
      timestamp = @trigger_created_at.strftime("%Y-%m-%d %H:%M:%S")
      query = "SELECT MIN(#{conn.quote_ident(column)}) AS val FROM #{conn.quote_ident(dest_table)} WHERE created_at > '#{timestamp}'"
      rows = conn.execute(query)
      return if rows.empty?

      rows[0]["val"]
    end

    def default_starting_val(table, column)
      column_type = conn.column_type(table, column)

      case column_type
      when "timestamp without time zone", "timestamp with time zone"
        "1900-01-01 00:00:00"
      when "date"
        "1900-01-01"
      when "text"
        ""
      else
        abort "Unknow column type #{column_type} of #{column}"
      end
    end

    def trigger_name
      "#{intermediate_table}_insert_trigger"
    end

    def settings_from_trigger
      comment = conn.fetch_trigger(trigger_name, intermediate_table)
      if comment
        comment["comment"].split(",").inject({}) { |memo, v| pair = v.split(":"); memo[pair.first] = pair.last; memo}
      end
    end
  end
end
