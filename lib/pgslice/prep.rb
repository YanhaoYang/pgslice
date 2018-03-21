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

      queries.concat(source_table_insert_trigger)
      queries.concat(source_table_update_trigger)
      queries.concat(source_table_delete_trigger)

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
      name = "#{table}_insert_trigger_for_pgslice"
      queries = []
      queries << <<-SQL
          CREATE FUNCTION #{conn.quote_ident(name)}()
              RETURNS trigger AS $$
              BEGIN
                  RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
      SQL

      queries << <<-SQL
          CREATE TRIGGER #{conn.quote_ident(name)}
              AFTER INSERT ON #{conn.quote_ident(table)}
              FOR EACH ROW EXECUTE PROCEDURE #{conn.quote_ident(name)}();
      SQL
      queries
    end

    def source_table_update_trigger
      name = "#{table}_update_trigger_for_pgslice"
      queries = []
      queries << <<-SQL
          CREATE FUNCTION #{conn.quote_ident(name)}()
              RETURNS trigger AS $$
              BEGIN
                  RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
      SQL

      queries << <<-SQL
          CREATE TRIGGER #{conn.quote_ident(name)}
              AFTER UPDATE ON #{conn.quote_ident(table)}
              FOR EACH ROW EXECUTE PROCEDURE #{conn.quote_ident(name)}();
      SQL
      queries
    end

    def source_table_delete_trigger
      name = "#{table}_delete_trigger_for_pgslice"
      queries = []
      queries << <<-SQL
          CREATE FUNCTION #{conn.quote_ident(name)}()
              RETURNS trigger AS $$
              BEGIN
                  RETURN OLD;
              END;
              $$ LANGUAGE plpgsql;
      SQL

      queries << <<-SQL
          CREATE TRIGGER #{conn.quote_ident(name)}
              AFTER DELETE ON #{conn.quote_ident(table)}
              FOR EACH ROW EXECUTE PROCEDURE #{conn.quote_ident(name)}();
      SQL
      queries
    end
  end
end
