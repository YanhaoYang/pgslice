require_relative "connection"

module PgSlice
  class Analyze
       attr_reader :table, :options, :conn

    def initialize(table, options = {})
      @table = table
      @options = options
      @conn = Connection.new(options[:url])
    end

    def run
      parent_table = options[:swapped] ? table : intermediate_table

      existing_tables = existing_partitions(table)
      analyze_list = existing_tables + [parent_table]
      run_queries_without_transaction analyze_list.map { |t| "ANALYZE VERBOSE #{conn.quote_ident(t)};" }
    end

    private

    def intermediate_table
      "#{table}_intermediate"
    end

    def existing_partitions(table)
      conn.existing_tables(like: "#{table}_%").select { |t| /\A#{Regexp.escape("#{table}_")}\d{6,8}\z/.match(t) }
    end

    def run_queries_without_transaction(queries)
      queries.each do |query|
        conn.run_query(query)
      end
    end
  end
end
