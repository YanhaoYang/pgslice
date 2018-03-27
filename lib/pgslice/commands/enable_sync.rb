require_relative "base"

module PgSlice
  module Commands
    class EnableSync < Base
      def run
        abort "Table not found: #{strategy.table}" unless conn.table_exists?(strategy.table)

        queries = []

        strategy.migrations[:sync].each do |m|
          queries.concat(m.up)
        end

        conn.run_queries(queries)
      end
    end
  end
end
