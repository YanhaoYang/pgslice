module PgSlice
  module Strategies
    class DeclarativeByRange
      def initialize(db, options)
        @db = db
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

      def partition_by
        "id"
      end

      def partitions
        parts = { "0" => "#{quoted_partition_by} < '1'"}
        ('1'..'9').each_cons(2) do |a, b|
          parts[a] = "#{quoted_partition_by} >= '#{a}' AND #{quoted_partition_by} < '#{b}'"
        end
        parts['9'] = "#{quoted_partition_by} >= '9' AND #{quoted_partition_by} < 'a'"
        ('a'..'e').each_cons(2) do |a, b|
          parts[a] = "#{quoted_partition_by} >= '#{a}' AND #{quoted_partition_by} < '#{b}'"
        end
        parts['f'] = "#{quoted_partition_by} >= 'f'"
        parts
      end

      def batch_by
        "id"
      end

      def type_of_batch_by
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
          CREATE TABLE #{conn.quote_ident(intermediate_table)} (LIKE #{conn.quote_ident(table)} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS) PARTITION BY RANGE (#{conn.quote_ident(column)});
        SQL
        queries
      end

      private

      def quoted_partition_by
        @quoted_partition_by = @db.quote_ident(partition_by)
      end
    end
  end
end
