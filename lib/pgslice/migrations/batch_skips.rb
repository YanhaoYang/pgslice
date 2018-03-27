require "forwardable"

module PgSlice
  module Migrations
    class BatchSkips
      extend Forwardable

      def initialize(strategy)
        @strategy = strategy
      end

      def_delegators :@strategy, :conn, :table, :batch_key, :batch_skipes_table, :batch_skipes_trigger

      def up
        queries = []

        return queries if batch_skipes_table.nil? or batch_skipes_trigger.nil?

        column_type = conn.column_type(table, batch_key)
        queries << <<-SQL
          CREATE TABLE #{conn.quote_ident(batch_skipes_table)} (
              #{batch_key} #{column_type}
          );
        SQL

        queries << <<-SQL
          CREATE INDEX index_#{batch_key}_on_#{batch_skipes_table}
              ON #{conn.quote_ident(batch_skipes_table)} USING btree (#{batch_key});
        SQL

        queries << <<-SQL
          CREATE FUNCTION #{conn.quote_ident(batch_skipes_trigger)}()
              RETURNS trigger AS $$
              BEGIN
                  INSERT INTO #{conn.quote_ident(batch_skipes_table)} VALUES (NEW.#{conn.quote_ident(batch_key)});
                  RETURN NEW;
              END;
              $$ LANGUAGE plpgsql;
        SQL

        queries << <<-SQL
          CREATE TRIGGER #{conn.quote_ident(batch_skipes_trigger)}
              AFTER INSERT ON #{conn.quote_ident(table)}
              FOR EACH ROW EXECUTE PROCEDURE #{conn.quote_ident(batch_skipes_trigger)}();
        SQL

        queries
      end

      def down
        return [] unless conn.table_exists?(batch_skipes_table)

        [
          "DROP TABLE #{conn.quote_ident(batch_skipes_table)} CASCADE;",
          "DROP TRIGGER IF EXISTS #{conn.quote_ident(batch_skipes_trigger)} ON #{conn.quote_ident(table)};",
          "DROP FUNCTION IF EXISTS #{conn.quote_ident(batch_skipes_trigger)}();",
        ]
      end
    end
  end
end
