require_relative "../migrations/non_declarative"
require_relative "../migrations/batch_skips"
require_relative "../migrations/sychronization"

module PgSlice
  module Strategies
    class NonDeclarative
      attr_accessor :conn

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
          insert: "#{table}_insert_trigger_for_pgslice",
          update: "#{table}_update_trigger_for_pgslice",
          delete: "#{table}_delete_trigger_for_pgslice",
        }.freeze
      end

      def retired_table
        "#{table}_retired"
      end

      def partition_key
        "id"
      end

      def quoted_partition_key
        @quoted_partition_key ||= @conn.quote_ident(partition_key)
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

      def batch_skipes_trigger
        "#{table}_batch_skips_trigger"
      end

      def batch_skipes_enabled?
        true
      end

      def batch_size
        @options[:batch_size] || 1000
      end

      def lock_timeout
        "30s"
      end

      def partition_tables
        Enumerator.new do |y|
          partitions.each do |k, v|
            partition_name = "#{table}_#{k}"
            next if @conn.table_exists?(partition_name)

            sql = <<-SQL
              CREATE TABLE #{@conn.quote_ident(partition_name)}
                  (CHECK (#{quoted_partition_key} >= '#{v.first}' AND #{quoted_partition_key} < '#{v.last}'))
                  INHERITS (#{@conn.quote_ident(intermediate_table)});
            SQL

            y << [partition_name, sql]
          end
        end
      end

      def partition_trigger_def
        trigger_defs = []
        partitions.each do |k, v|
          partition_name = "#{table}_#{k}"

          trigger_defs << <<-SQL
              (NEW.#{quoted_partition_key} >= '#{v.first}' AND NEW.#{quoted_partition_key} < '#{v.last}') THEN
                  INSERT INTO #{@conn.quote_ident(partition_name)} VALUES (NEW.*);
          SQL
        end

        <<-SQL
          CREATE OR REPLACE FUNCTION #{@conn.quote_ident(partition_trigger_name)}()
              RETURNS trigger AS $$
              BEGIN
                  IF #{trigger_defs.join("\n        ELSIF ")}
                  ELSE
                      RAISE EXCEPTION 'Date out of range? Should never reach here!';
                  END IF;
                  RETURN NULL;
              END;
              $$ LANGUAGE plpgsql;
        SQL
      end

      def migrations
        @migrations ||=
          {
            prep: [ Migrations::NonDeclarative.new(self) ],
            sync: [ Migrations::Sychronization.new(self), Migrations::BatchSkips.new(self) ],
          }
      end
    end
  end
end
