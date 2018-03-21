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
        "DROP FUNCTION IF EXISTS #{conn.quote_ident(trigger_name)}();",
        "DROP TRIGGER IF EXISTS #{conn.quote_ident(source_table_insert_trigger)} ON #{conn.quote_ident(table)};",
        "DROP TRIGGER IF EXISTS #{conn.quote_ident(source_table_update_trigger)} ON #{conn.quote_ident(table)};",
        "DROP TRIGGER IF EXISTS #{conn.quote_ident(source_table_delete_trigger)} ON #{conn.quote_ident(table)};",
        "DROP FUNCTION IF EXISTS #{conn.quote_ident(source_table_insert_trigger)}();",
        "DROP FUNCTION IF EXISTS #{conn.quote_ident(source_table_update_trigger)}();",
        "DROP FUNCTION IF EXISTS #{conn.quote_ident(source_table_delete_trigger)}();",
      ]
      conn.run_queries(queries)
    end

    private

    def intermediate_table
      "#{table}_intermediate"
    end

    def trigger_name
      "#{intermediate_table}_insert_trigger"
    end

    def source_table_insert_trigger
      "#{table}_insert_trigger_for_pgslice"
    end

    def source_table_update_trigger
      "#{table}_update_trigger_for_pgslice"
    end

    def source_table_delete_trigger
      "#{table}_delete_trigger_for_pgslice"
    end
  end
end
