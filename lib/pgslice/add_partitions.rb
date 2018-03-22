require_relative "connection"

module PgSlice
  class AddPartitions
    attr_reader :original_table, :table, :options, :conn

    def initialize(table, options = {})
      @original_table = table
      @table = options[:intermediate] ? intermediate_table : original_table
      @options = options
      @conn = Connection.new(options[:url])
    end

    def run
      abort "Table not found: #{table}" unless conn.table_exists?(table)

      values = options[:series].split(',').map(&:strip)

      field, cast, needs_comment, declarative = settings_from_trigger(original_table, table)

      queries = []

      schema_table =
        if !declarative
          table
        elsif options[:intermediate]
          original_table
        else
          "#{original_table}_#{today.strftime(name_format(period))}"
        end

      index_defs = conn.execute("SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = #{conn.regclass(conn.schema, schema_table)} AND indisprimary = 'f'").map { |r| r["pg_get_indexdef"] }
      fk_defs = conn.foreign_keys(schema_table)
      primary_key = conn.primary_key(schema_table)

      added_partitions = []
      trigger_defs = []
      ([nil] + values + [nil]).each_cons(2) do |v1, v2|
        if v1.nil?
          partition_name = "#{original_table}_lt"
          check = "(CHECK (#{conn.quote_ident(field)} < '#{v2}'))"
          trigger_defs << <<-SQL
            (NEW.#{conn.quote_ident(field)} < '#{v2}') THEN
                INSERT INTO #{conn.quote_ident(partition_name)} VALUES (NEW.*);
          SQL
        elsif v2.nil?
          partition_name = "#{original_table}_#{v1}"
          check = "(CHECK (#{conn.quote_ident(field)} > '#{v1}'))"
          trigger_defs << <<-SQL
            (NEW.#{conn.quote_ident(field)} >= '#{v1}') THEN
                INSERT INTO #{conn.quote_ident(partition_name)} VALUES (NEW.*);
          SQL
        else
          partition_name = "#{original_table}_#{v1}"
          check = "(CHECK (#{conn.quote_ident(field)} >= '#{v1}' AND #{conn.quote_ident(field)} < '#{v2}'))"
          trigger_defs << <<-SQL
            (NEW.#{conn.quote_ident(field)} >= '#{v1}' AND NEW.#{conn.quote_ident(field)} < '#{v2}') THEN
                INSERT INTO #{conn.quote_ident(partition_name)} VALUES (NEW.*);
          SQL
        end

        next if conn.table_exists?(partition_name)
        added_partitions << partition_name

        if declarative
          queries << <<-SQL
            CREATE TABLE #{conn.quote_ident(partition_name)} PARTITION OF #{conn.quote_ident(table)}
              FOR VALUES FROM (#{conn.sql_date(day, cast, false)})
              TO (#{conn.sql_date(advance_date(day, period, 1), cast, false)});
          SQL
        else
          queries << <<-SQL
            CREATE TABLE #{conn.quote_ident(partition_name)}
                #{check}
                INHERITS (#{conn.quote_ident(table)});
          SQL
        end

        queries << "ALTER TABLE #{conn.quote_ident(partition_name)} ADD PRIMARY KEY (#{conn.quote_ident(primary_key)});" if primary_key

        index_defs.each do |index_def|
          queries << index_def.sub(/ ON \S+ USING /, " ON #{conn.quote_ident(partition_name)} USING ").sub(/ INDEX .+ ON /, " INDEX ON ") + ";"
        end

        fk_defs.each do |fk_def|
          queries << "ALTER TABLE #{conn.quote_ident(partition_name)} ADD #{fk_def};"
        end
      end

      unless declarative
        if trigger_defs.any?
          queries << <<-SQL
            CREATE OR REPLACE FUNCTION #{conn.quote_ident(trigger_name)}()
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
      end

      queries.concat(source_table_insert_trigger)
      queries.concat(source_table_update_trigger)
      queries.concat(source_table_delete_trigger)

      conn.run_queries(queries) if queries.any?
    end

    private

    def intermediate_table
      "#{original_table}_intermediate"
    end

    def trigger_name
      "#{table}_insert_trigger"
    end

    def settings_from_trigger(original_table, table)
      needs_comment = false
      trigger_comment = conn.fetch_trigger(trigger_name, table)
      comment = trigger_comment || fetch_comment(table)
      if comment
        field, cast = comment["comment"].split(",").map { |v| v.split(":").last } rescue [nil, nil]
      end

      [field, cast, needs_comment, !trigger_comment]
    end

    def round_date(date, period)
      date = date.to_date
      case period.to_sym
      when :day
        date
      else
        Date.new(date.year, date.month)
      end
    end

    def advance_date(date, period, count = 1)
      date = date.to_date
      case period.to_sym
      when :day
        date.next_day(count)
      else
        date.next_month(count)
      end
    end

    def date_format(period)
      case period.to_sym
      when :day
        "YYYYMMDD"
      else
        "YYYYMM"
      end
    end

    def name_format(period)
      case period.to_sym
      when :day
        "%Y%m%d"
      else
        "%Y%m"
      end
    end

    def existing_partitions(table)
      conn.existing_tables(like: "#{table}_%").select { |t| /\A#{Regexp.escape("#{table}_")}\d{6,8}\z/.match(t) }
    end

    def source_table_insert_trigger
      name = "#{original_table}_insert_trigger_for_pgslice"
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
      queries
    end

    def source_table_update_trigger
      name = "#{original_table}_update_trigger_for_pgslice"
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
      queries
    end

    def source_table_delete_trigger
      name = "#{original_table}_delete_trigger_for_pgslice"
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
      queries
    end
  end
end
