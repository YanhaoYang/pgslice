require_relative "connection"

module PgSlice
  class Unprep
    attr_reader :table, :conn

    def initialize(table, options = {})
      @table = table
      @conn = Connection.new(options[:url])
    end

    def run
      abort "Table not found: #{intermediate_table}" unless conn.table_exists?(intermediate_table)

      queries = [
        "DROP TABLE #{conn.quote_ident(intermediate_table)} CASCADE;",
        "DROP FUNCTION IF EXISTS #{conn.quote_ident(trigger_name)}();"
      ]
      conn.run_queries(queries)
    end

    private

    def intermediate_table
      "#{table}_intermediate"
    end

    def trigger_name
      "#{table}_insert_trigger"
    end
  end
end
