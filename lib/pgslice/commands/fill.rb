require_relative "base"

module PgSlice
  module Commands
    class Fill < Base
      def run
        Signal.trap("INT") { |signo| @aborted = true; puts "\n\n!!! Got signal INT. Aborting ..." }

        abort "Table not found: #{strategy.table}" unless conn.table_exists?(strategy.table)
        abort "Table not found: #{dest_table}" unless conn.table_exists?(dest_table)

        batch_by_generic_column
      end

      private

      def source_table
        strategy.table
      end

      def dest_table
        strategy.intermediate_table
      end

      def batch_by_generic_column
        column = strategy.batch_key

        starting_val = options[:starting_val] || get_starting_val(column)
        fields = conn.columns(strategy.table).map { |c| conn.quote_ident(c) }.join(", ")
        batch_count = 1
        loop do
          where = "#{conn.quote_ident(column)} >= '#{starting_val}'"

          batch_ending_val = get_batch_ending_val(column, starting_val)
          if batch_ending_val
            where << " AND #{conn.quote_ident(column)} < '#{batch_ending_val}'"
          end

          if options[:where]
            where << " AND #{options[:where]}"
          end

          query = <<-SQL
            /* batch ##{batch_count} * #{strategy.batch_size} at #{Time.now} */
            INSERT INTO #{conn.quote_ident(dest_table)} (#{fields})
              SELECT #{fields} FROM #{conn.quote_ident(strategy.table)}
              WHERE #{where} AND #{where_to_skip}
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

      def get_batch_ending_val(column, starting_val)
        where = "#{conn.quote_ident(column)} >= '#{starting_val}'"
        if options[:where]
          where << " AND #{options[:where]}"
        end
        query = <<-SQL
          SELECT #{conn.quote_ident(column)} val FROM #{conn.quote_ident(strategy.table)}
            WHERE #{where}
            ORDER BY #{conn.quote_ident(column)} OFFSET #{strategy.batch_size} LIMIT 1
        SQL
        rows = conn.execute(query)
        return if rows.empty?

        rows[0]["val"]
      end

      def get_next_val(column, starting_val)
        query = <<-SQL
          SELECT #{conn.quote_ident(column)} val FROM #{conn.quote_ident(strategy.table)}
            WHERE #{conn.quote_ident(column)} > '#{starting_val}'
            ORDER BY #{conn.quote_ident(column)} OFFSET 1 LIMIT 1
        SQL
        rows = conn.execute(query)
        return if rows.empty?

        rows[0]["val"]
      end

      def get_starting_val(column)
        query = "SELECT MAX(#{conn.quote_ident(column)}) AS val FROM #{conn.quote_ident(dest_table)} WHERE #{where_to_skip}"
        rows = conn.execute(query)
        return default_starting_val(table, column) if rows.empty? || rows[0]["val"].nil?

        get_next_val(column, rows[0]["val"])
      end

      def where_to_skip
        if strategy.batch_skipes_enabled?
          "#{conn.quote_ident(strategy.batch_key)} NOT IN (SELECT #{conn.quote_ident(strategy.batch_key)}" +
            " FROM #{conn.quote_ident(strategy.batch_skipes_table)})"
        end
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
    end
  end
end
