require "forwardable"

module PgSlice
  module Migrations
    class NonDeclarative
      extend Forwardable

      def initialize(strategy)
        @strategy = strategy
      end

      def_delegators :@strategy, :conn, :table, :intermediate_table, :partition_trigger_name

      def up
        queries = []
        queries << <<-SQL
          CREATE TABLE #{conn.quote_ident(intermediate_table)} (LIKE #{conn.quote_ident(table)} INCLUDING ALL);
        SQL

        conn.foreign_keys(table).each do |fk_def|
          queries << "ALTER TABLE #{conn.quote_ident(intermediate_table)} ADD #{fk_def};"
        end

        queries << <<-SQL
          CREATE FUNCTION #{conn.quote_ident(partition_trigger_name)}()
              RETURNS trigger AS $$
              BEGIN
                  RAISE EXCEPTION 'Create partitions first.';
              END;
              $$ LANGUAGE plpgsql;
        SQL

        queries << <<-SQL
          CREATE TRIGGER #{conn.quote_ident(partition_trigger_name)}
              BEFORE INSERT ON #{conn.quote_ident(intermediate_table)}
              FOR EACH ROW EXECUTE PROCEDURE #{conn.quote_ident(partition_trigger_name)}();
        SQL

        queries
      end

      def down
        [
          "DROP TABLE #{conn.quote_ident(intermediate_table)} CASCADE;",
          "DROP FUNCTION IF EXISTS #{conn.quote_ident(partition_trigger_name)}();",
        ]
      end
    end
  end
end
