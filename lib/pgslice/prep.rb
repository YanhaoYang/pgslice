require_relative "connection"

module PgSlice
  class Prep
    attr_reader :table, :column, :period, :options, :conn

    SQL_FORMAT = {
      day: "YYYYMMDD",
      month: "YYYYMM"
    }

    def initialize(table, column, period, options = {})
      @table = table
      @column = column
      @period = period
      @options = options
      @conn = Connection.new(options[:url])
    end

    def run

      abort "Table not found: #{table}" unless conn.table_exists?(table)
      abort "Table already exists: #{intermediate_table}" if conn.table_exists?(intermediate_table)

      unless options[:no_partition]
        abort "Column not found: #{column}" unless conn.columns(table).include?(column)
        abort "Invalid period: #{period}" unless SQL_FORMAT[period.to_sym]
      end

      queries = []

      declarative = conn.server_version_num >= 100000 && !options[:trigger_based]

      if declarative && !options[:no_partition]
        queries << <<-SQL
CREATE TABLE #{conn.quote_ident(intermediate_table)} (LIKE #{conn.quote_ident(table)} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS) PARTITION BY RANGE (#{conn.quote_ident(column)});
        SQL

        # add comment
        cast = column_cast(table, column)
        queries << <<-SQL
COMMENT ON TABLE #{conn.quote_ident(intermediate_table)} is 'column:#{column},period:#{period},cast:#{cast}';
        SQL
      else
        queries << <<-SQL
CREATE TABLE #{conn.quote_ident(intermediate_table)} (LIKE #{conn.quote_ident(table)} INCLUDING ALL);
        SQL

        conn.foreign_keys(table).each do |fk_def|
          queries << "ALTER TABLE #{conn.quote_ident(intermediate_table)} ADD #{fk_def};"
        end
      end

      if !options[:no_partition] && !declarative
        sql_format = SQL_FORMAT[period.to_sym]
        queries << <<-SQL
CREATE FUNCTION #{conn.quote_ident(trigger_name)}()
    RETURNS trigger AS $$
    BEGIN
        RAISE EXCEPTION 'Create partitions first.';
    END;
    $$ LANGUAGE plpgsql;
        SQL

        queries << <<-SQL
CREATE TRIGGER #{conn.quote_ident(trigger_name)}
    BEFORE INSERT ON #{conn.quote_ident(intermediate_table)}
    FOR EACH ROW EXECUTE PROCEDURE #{conn.quote_ident(trigger_name)}();
        SQL

        cast = conn.column_cast(table, column)
        queries << <<-SQL
COMMENT ON TRIGGER #{conn.quote_ident(trigger_name)} ON #{conn.quote_ident(intermediate_table)} is 'column:#{column},period:#{period},cast:#{cast}';
        SQL
      end

      conn.run_queries(queries)
    end

    private

    def trigger_name
      "#{table}_insert_trigger"
    end
  end
end
