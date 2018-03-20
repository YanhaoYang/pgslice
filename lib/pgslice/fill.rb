require_relative "connection"

module PgSlice
  class Fill
    attr_reader :table, :options, :conn

    def initialize(table, options = {})
      @table = table
      @options = options
      @conn = Connection.new(options[:url])
    end

    def run
      source_table = options[:source_table]
      dest_table = options[:dest_table]

      if options[:swapped]
        source_table ||= retired_name(table)
        dest_table ||= table
      else
        source_table ||= table
        dest_table ||= intermediate_name(table)
      end

      abort "Table not found: #{source_table}" unless conn.table_exists?(source_table)
      abort "Table not found: #{dest_table}" unless conn.table_exists?(dest_table)

      period, field, cast, needs_comment, declarative = settings_from_trigger(table, dest_table)

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
      fields = columns(source_table).map { |c| quote_ident(c) }.join(", ")
      batch_size = options[:batch_size]

      i = 1
      batch_count = ((max_source_id - starting_id) / batch_size.to_f).ceil

      if batch_count == 0
        log_sql "/* nothing to fill */"
      end

      while starting_id < max_source_id
        where = "#{quote_ident(primary_key)} > #{starting_id} AND #{quote_ident(primary_key)} <= #{starting_id + batch_size}"
        if starting_time
          where << " AND #{quote_ident(field)} >= #{sql_date(starting_time, cast)} AND #{quote_ident(field)} < #{sql_date(ending_time, cast)}"
        end
        if options[:where]
          where << " AND #{options[:where]}"
        end

        query = <<-SQL
/* #{i} of #{batch_count} */
INSERT INTO #{quote_ident(dest_table)} (#{fields})
    SELECT #{fields} FROM #{quote_ident(source_table)}
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

    private

    def intermediate_name(table)
      "#{table}_intermediate"
    end
  end
end
