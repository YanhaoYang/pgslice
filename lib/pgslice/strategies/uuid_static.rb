module PgSlice
  module Strategies
    class UuidStatic < NonDeclarative
      def partitions
        parts = {}
        (('0'..'9').to_a + ('a'..'g').to_a).each_cons(2) do |a, b|
          parts[a] = [a, b]
        end
        parts
      end

      private

      def quoted_partition_by
        @quoted_partition_by = @db.quote_ident(partition_by)
      end
    end
  end
end
