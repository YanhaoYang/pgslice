require_relative "connection"

module PgSlice
  class Swap
    attr_reader :table, :options, :conn

    def initialize(table, options = {})
      @table = table
      @options = options
      @conn = Connection.new(options[:url])
    end

    def run
      abort "Table not found: #{table}" unless conn.table_exists?(table)
      abort "Table not found: #{intermediate_table}" unless conn.table_exists?(intermediate_table)
      abort "Table already exists: #{retired_table}" if conn.table_exists?(retired_table)

      queries = [
        "ALTER TABLE #{conn.quote_ident(table)} RENAME TO #{conn.quote_ident(retired_table)};",
        "ALTER TABLE #{conn.quote_ident(intermediate_table)} RENAME TO #{conn.quote_ident(table)};"
      ]

      sequences(table).each do |sequence|
        queries << "ALTER SEQUENCE #{conn.quote_ident(sequence["sequence_name"])} OWNED BY #{conn.quote_ident(table)}.#{conn.quote_ident(sequence["related_column"])};"
      end

      queries.unshift("SET LOCAL lock_timeout = '#{options[:lock_timeout]}';") if conn.server_version_num >= 90300

      conn.run_queries(queries)
    end

    private

    def intermediate_table
      "#{table}_intermediate"
    end

    def retired_table
      "#{table}_retired"
    end

    def sequences(table)
      query = <<-SQL
        SELECT
          a.attname as related_column,
          s.relname as sequence_name
        FROM pg_class s
          JOIN pg_depend d ON d.objid = s.oid
          JOIN pg_class t ON d.objid = s.oid AND d.refobjid = t.oid
          JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum)
          JOIN pg_namespace n ON n.oid = s.relnamespace
        WHERE s.relkind = 'S'
          AND n.nspname = $1
          AND t.relname = $2
      SQL
      conn.execute(query, [conn.schema, table])
    end
  end
end
