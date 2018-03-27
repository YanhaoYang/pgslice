require_relative "connection"
require_relative "strategies"

module PgSlice
  class Prep
    attr_reader :options, :conn, :strategy

    def initialize(options = {})
      @options = options
      @conn = Connection.new(options[:url])
      @strategy = load_strategy(options[:strategy]).new(@conn, options)
    end

    def run
      abort "Table not found: #{strategy.table}" unless conn.table_exists?(strategy.table)

      if conn.table_exists?(strategy.intermediate_table)
        abort "Table already exists: #{strategy.intermediate_table}"
      end

      unless conn.columns(strategy.table).include?(strategy.partition_key)
        abort "Column not found: #{strategy.partition_key}"
      end

      queries = strategy.sqls_for_intermediate_table

      queries << <<-SQL
          CREATE TABLE #{conn.quote_ident(strategy.batch_skipes_table)} (
              #{strategy.batch_key} #{strategy.type_of_batch_key}
          );
      SQL

      queries << <<-SQL
          CREATE INDEX index_#{strategy.batch_key}_on_#{strategy.batch_skipes_table}
              ON #{@conn.quote_ident(strategy.batch_skipes_table)} USING btree (#{strategy.batch_key});
      SQL

      queries.concat(source_table_insert_trigger)
      queries.concat(source_table_update_trigger)
      queries.concat(source_table_delete_trigger)

      conn.run_queries(queries)
    end

    private

    def load_strategy(name_and_path)
      name, path = name_and_path.split('@')
      load path if path
      Module.const_get name
    end

    def source_table_insert_trigger
      name = strategy.sync_trigger_names["insert"]
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
              AFTER INSERT ON #{conn.quote_ident(strategy.table)}
              FOR EACH ROW EXECUTE PROCEDURE #{conn.quote_ident(name)}();
      SQL
      queries
    end

    def source_table_update_trigger
      name = strategy.sync_trigger_names["update"]
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
              AFTER UPDATE ON #{conn.quote_ident(strategy.table)}
              FOR EACH ROW EXECUTE PROCEDURE #{conn.quote_ident(name)}();
      SQL
      queries
    end

    def source_table_delete_trigger
      name = strategy.sync_trigger_names["delete"]
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
              AFTER DELETE ON #{conn.quote_ident(strategy.table)}
              FOR EACH ROW EXECUTE PROCEDURE #{conn.quote_ident(name)}();
      SQL
      queries
    end
  end
end
