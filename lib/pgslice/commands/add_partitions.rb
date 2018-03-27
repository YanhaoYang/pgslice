require_relative "base"

module PgSlice
  module Commands
    class AddPartitions < Base
      def run
        abort "Table not found: #{table}" unless conn.table_exists?(strategy.table)

        queries = []

        index_sql = <<-SQL
            SELECT pg_get_indexdef(indexrelid) FROM pg_index
                WHERE indrelid = #{conn.regclass(conn.schema, strategy.table)} AND indisprimary = 'f'
        SQL
        index_defs = conn.execute(index_sql).map { |r| r["pg_get_indexdef"] }
        fk_defs = conn.foreign_keys(strategy.table)
        primary_key = conn.primary_key(strategy.table)

        strategy.partition_tables.each do |partition_name, table_def|
          queries << table_def

          queries << "ALTER TABLE #{conn.quote_ident(partition_name)} ADD PRIMARY KEY (#{conn.quote_ident(primary_key)});" if primary_key

          index_defs.each do |index_def|
            queries << index_def.sub(/ ON \S+ USING /, " ON #{conn.quote_ident(partition_name)} USING ").sub(/ INDEX .+ ON /, " INDEX ON ") + ";"
          end

          fk_defs.each do |fk_def|
            queries << "ALTER TABLE #{conn.quote_ident(partition_name)} ADD #{fk_def};"
          end
        end

        queries << strategy.partition_trigger_def

        conn.run_queries(queries) if queries.any?
      end
    end
  end
end
