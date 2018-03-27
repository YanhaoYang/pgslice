require_relative "base"

module PgSlice
  module Commands
    class Prep < Base
      def run
        abort "Table not found: #{strategy.table}" unless conn.table_exists?(strategy.table)

        if conn.table_exists?(strategy.intermediate_table)
          abort "Table already exists: #{strategy.intermediate_table}"
        end

        unless conn.columns(strategy.table).include?(strategy.partition_key)
          abort "Column not found: #{strategy.partition_key}"
        end

        queries = []

        strategy.migrations[:prep].each do |m|
          queries.concat(m.up)
        end

        conn.run_queries(queries)
      end

      private

    end
  end
end
