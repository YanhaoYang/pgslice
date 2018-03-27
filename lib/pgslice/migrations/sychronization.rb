require "forwardable"

module PgSlice
  module Migrations
    class Sychronization
      extend Forwardable

      def initialize(strategy)
        @strategy = strategy
      end

      def_delegators :@strategy, :conn, :table, :intermediate_table, :sync_trigger_names

      def up
        insert_trigger + update_trigger + delete_trigger
      end

      def down
        [
          "DROP TRIGGER IF EXISTS #{conn.quote_ident(sync_trigger_names[:insert])} ON #{conn.quote_ident(table)};",
          "DROP TRIGGER IF EXISTS #{conn.quote_ident(sync_trigger_names[:update])} ON #{conn.quote_ident(table)};",
          "DROP TRIGGER IF EXISTS #{conn.quote_ident(sync_trigger_names[:delete])} ON #{conn.quote_ident(table)};",
          "DROP FUNCTION IF EXISTS #{conn.quote_ident(sync_trigger_names[:insert])}();",
          "DROP FUNCTION IF EXISTS #{conn.quote_ident(sync_trigger_names[:update])}();",
          "DROP FUNCTION IF EXISTS #{conn.quote_ident(sync_trigger_names[:delete])}();",
        ]
      end

      private

      def insert_trigger
        name = sync_trigger_names[:insert]
        queries = []
        queries << <<-SQL
          CREATE OR REPLACE FUNCTION #{conn.quote_ident(name)}()
              RETURNS trigger AS $$
              BEGIN
                  INSERT INTO #{conn.quote_ident(intermediate_table)} VALUES (NEW.*);
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

      def update_trigger
        name = sync_trigger_names[:update]
        queries = []
        queries << <<-SQL
          CREATE OR REPLACE FUNCTION #{conn.quote_ident(name)}()
              RETURNS trigger AS $$
              DECLARE
                  tbl  text := quote_ident('#{intermediate_table}');
                  cols text;
                  vals text;
              BEGIN
                  SELECT INTO cols, vals
                         string_agg(quote_ident(attname), ', ')
                        ,string_agg('x.' || quote_ident(attname), ', ')
                  FROM   pg_attribute
                  WHERE  attrelid = tbl::regclass
                  AND    NOT attisdropped   -- no dropped (dead) columns
                  AND    attnum > 0;        -- no system columns

                  EXECUTE format('
                  UPDATE %s t
                  SET   (%s) = (%s)
                  FROM  (SELECT ($1).*) x
                  WHERE  t.id = ($2).id'
                  , tbl, cols, vals) -- assuming unique "id" in every table
                  USING NEW, OLD;
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

      def delete_trigger
        name = sync_trigger_names[:delete]
        queries = []
        queries << <<-SQL
          CREATE OR REPLACE FUNCTION #{conn.quote_ident(name)}()
              RETURNS trigger AS $$
              BEGIN
                  DELETE FROM #{conn.quote_ident(intermediate_table)}
                      WHERE id = OLD.id;
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
end
