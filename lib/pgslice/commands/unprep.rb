require_relative "base"

module PgSlice
  module Commands
    class Unprep < Base
      def run
        abort "Table not found: #{strategy.intermediate_table}" unless conn.table_exists?(strategy.intermediate_table)

        queries = []

        strategy.migrations[:prep].each do |m|
          queries.concat(m.down)
        end

        strategy.migrations[:sync].each do |m|
          queries.concat(m.down)
        end

        conn.run_queries(queries)
      end
    end
  end
end
