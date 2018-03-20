require "pg"
require "cgi"

module PgSlice
  class Connection
    attr_reader :schema, :dry_run

    def initialize(url, dry_run = false)
      @dry_run = dry_run
      @url = url || ENV["PGSLICE_URL"]
      abort "Set PGSLICE_URL or use the --url option" unless @url
      connect
    end

    def existing_tables(like:)
      query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename LIKE $2"
      execute(query, [schema, like]).map { |r| r["tablename"] }.sort
    end

    def table_exists?(table)
      existing_tables(like: table).any?
    end

    def columns(table)
      execute("SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2", [schema, table]).map{ |r| r["column_name"] }
    end

    def foreign_keys(table)
      execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = #{regclass(schema, table)} AND contype ='f'").map { |r| r["pg_get_constraintdef"] }
    end

    def primary_key(table)
      query = <<-SQL
        SELECT
          pg_attribute.attname,
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
        FROM
          pg_index, pg_class, pg_attribute, pg_namespace
        WHERE
          relname = $2 AND
          indrelid = pg_class.oid AND
          nspname = $1 AND
          pg_class.relnamespace = pg_namespace.oid AND
          pg_attribute.attrelid = pg_class.oid AND
          pg_attribute.attnum = any(pg_index.indkey) AND
          indisprimary
      SQL
      row = execute(query, [schema, table])[0]
      row && row["attname"]
    end

    def regclass(schema, table)
      "'#{quote_ident(schema)}.#{quote_ident(table)}'::regclass"
    end

    def column_cast(table, column)
      data_type = execute("SELECT data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3", [schema, table, column])[0]["data_type"]
      data_type == "timestamp with time zone" ? "timestamptz" : "date"
    end

    def server_version_num
      execute("SHOW server_version_num")[0]["server_version_num"].to_i
    end

    def quote_ident(value)
      PG::Connection.quote_ident(value)
    end

    def execute(query, params = [])
      @connection.exec_params(query, params).to_a
    end

    def run_queries(queries)
      @connection.transaction do
        execute("SET LOCAL client_min_messages TO warning") unless dry_run
        log_sql "BEGIN;"
        log_sql
        run_queries_without_transaction(queries)
        log_sql "COMMIT;"
      end
    end

    def run_query(query)
      log_sql query
      unless dry_run
        begin
          execute(query)
        rescue PG::ServerError => e
          abort("#{e.class.name}: #{e.message}")
        end
      end
      log_sql
    end

    def run_queries_without_transaction(queries)
      queries.each do |query|
        run_query(query)
      end
    end

    def log_sql(message = nil)
      $stdout.puts message
    end

    def sql_date(time, cast, add_cast = true)
      if cast == "timestamptz"
        fmt = "%Y-%m-%d %H:%M:%S UTC"
      else
        fmt = "%Y-%m-%d"
      end
      str = "'#{time.strftime(fmt)}'"
      add_cast ? "#{str}::#{cast}" : str
    end

    def fetch_trigger(trigger_name, table)
      execute("SELECT obj_description(oid, 'pg_trigger') AS comment FROM pg_trigger WHERE tgname = $1 AND tgrelid = #{regclass(schema, table)}", [trigger_name])[0]
    end

    private

    def connect
      @connection ||= begin
        uri = URI.parse(@url)
        uri_parser = URI::Parser.new
        config = {
          host: uri.host,
          port: uri.port,
          dbname: uri.path.sub(/\A\//, ""),
          user: uri.user,
          password: uri.password,
          connect_timeout: 1
        }.reject { |_, value| value.to_s.empty? }
        config.map { |key, value| config[key] = uri_parser.unescape(value) if value.is_a?(String) }
        @schema = CGI.parse(uri.query.to_s)["schema"][0] || "public"
        PG::Connection.new(config)
      end
    rescue PG::ConnectionBad => e
      abort e.message
    end
  end
end
