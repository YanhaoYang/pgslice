module PgSlice
  module Strategies
    class NonDeclarative
      def initialize(conn, options)
        @conn = conn
        @options = options
      end

      def table
        @options[:table]
      end

      def intermediate_table
        "#{table}_intermediate"
      end

      def partition_trigger_name
        "#{intermediate_table}_partition_trigger"
      end

      def sync_trigger_names
        {
          "insert" => "#{table}_insert_trigger_for_pgslice",
          "update" => "#{table}_update_trigger_for_pgslice",
          "delete" => "#{table}_delete_trigger_for_pgslice",
        }
      end

      def retired_table
        "#{table}_retired"
      end

      def partition_key
        "id"
      end

      def partitions
        raise "Please define partitions in an inherited class"
      end

      def batch_key
        "id"
      end

      def type_of_batch_key
        "text"
      end

      def batch_skipes_table
        "#{table}_batch_skips"
      end

      def batch_size
        options[:batch_size]
      end

      def lock_timeout
        "30s"
      end

      def sqls_for_intermediate_table
        queries = []
        queries << <<-SQL
            CREATE TABLE #{@conn.quote_ident(intermediate_table)} (LIKE #{@conn.quote_ident(table)} INCLUDING ALL);
        SQL

        @conn.foreign_keys(table).each do |fk_def|
          queries << "ALTER TABLE #{@conn.quote_ident(intermediate_table)} ADD #{fk_def};"
        end

        queries << <<-SQL
          CREATE FUNCTION #{@conn.quote_ident(partition_trigger_name)}()
              RETURNS trigger AS $$
              BEGIN
                  RAISE EXCEPTION 'Create partitions first.';
              END;
              $$ LANGUAGE plpgsql;
        SQL

        queries << <<-SQL
          CREATE TRIGGER #{@conn.quote_ident(partition_trigger_name)}
              BEFORE INSERT ON #{@conn.quote_ident(intermediate_table)}
              FOR EACH ROW EXECUTE PROCEDURE #{@conn.quote_ident(partition_trigger_name)}();
        SQL

        queries
      end
    end
  end
end
