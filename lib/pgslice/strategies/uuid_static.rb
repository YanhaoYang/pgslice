module PgSlice
  module Strategies
    class UuidStatic < NonDeclarative
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

      private

      def quoted_partition_by
        @quoted_partition_by = @db.quote_ident(partition_by)
      end
    end
  end
end
