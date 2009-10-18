# Rake task for migrating MySQL database to PostgreSQL. Migrates both the
# schema and data.
#
# Requires RAILS_ENV+'_mysql' and RAILS_ENV environments to be setup (eg.
# 'development_mysql' and 'development').
#
# To ensure that the data migration is performed as fast as possible (critical
# for large, production datasets), we avoid using ActiveRecord for
# reading and writing data. A number of additional optimizations are also
# employed: disabling of indexes, triggers, use of prepared statements, paging,
# and transactions. On a dataset with 244,214 records (98 MB MySQL dumpfile),
# we acheived a 11x speed up over a simple ActiveRecord based implementation.
#
# Usage:
#
#     rake db:migrate:mysql_to_postgres
#
# This migrates the database from the RAILS_ENV+'_mysql' to RAILS_ENV (eg. from
# 'development_mysql' to 'development').
#
# You can also turn off optimizations like the disabling of triggers and indexes
# by passing 'drop_triggers=false' and 'drop_indexes=false', eg.
#
#     rake db:migrate:mysql_to_postgres drop_triggers=false drop_indexes=false
#
# @author James Tan (jamestyj)

namespace :db do
  namespace :migrate do

    desc 'Migrates database from MySQL to Postgres.'
    task :mysql_to_postgres => :environment do
      require 'mysql'

      # Get command line options
      OPTS = OpenStruct.new({
        :source_env => RAILS_ENV + '_mysql',
        :target_env => RAILS_ENV,

        :drop_triggers => ((ENV['drop_triggers'].downcase == 'true') rescue true),
        :drop_indexes  => ((ENV['drop_indexes' ].downcase == 'true') rescue true),

        :no_schema   => ((ENV['no_schema'  ].downcase == 'true') rescue false),
        :no_migrate  => ((ENV['no_migrate' ].downcase == 'true') rescue false),
        :no_commit   => ((ENV['no_commit'  ].downcase == 'true') rescue false),

        :tables      => (ENV['tables'     ].split(',').map(&:strip) rescue []),
        :skip_tables => (ENV['skip_tables'].split(',').map(&:strip) rescue []),

        :no_progress => ((ENV['no_progress'].downcase == 'true') rescue true),
      })

      class SourceModelClass < ActiveRecord::Base; end
      class TargetModelClass < ActiveRecord::Base; end

      SourceModelClass.establish_connection(OPTS.source_env)
      TargetModelClass.establish_connection(OPTS.target_env)

      PAGE_SIZE = 10000

      check_db_type
      source_conn, target_conn = get_db_connections

      migrate_schema(target_conn)

      ensure_tables_and_fields_match
      tables = get_tables

      delete_data(tables, target_conn)

      # Don't include preparations in total time.
      start_time = Time.now

      indexes = remove_indexes(tables, target_conn)
      total_count, total_error_count = migrate_data(tables, source_conn, target_conn)

      add_indexes(indexes, target_conn)

      total_time = Time.now - start_time
      log("Total: #{delimit(total_count)} records, #{delimit(total_error_count)} errors " +
          "(took #{format_time(total_time)}, " +
          "#{delimit((total_count/total_time).to_i)} rec/s)\n")

      validate_migration(tables)
    end

    # Helper function for timing a block of code. Also runs the given command,
    # if any, and terminates execution if that command fails.
    def run(message, command = nil)
      if command.nil? and not block_given?
        log "** #{message}\n"
        return
      end

      start = Time.now
      log "** #{message}... "

      unless command.blank?
        output = `#{command}`
        if $? != 0
          puts output
          exit 1
        end
      end

      yield if block_given?

      puts "done (took #{format_time(Time.now - start)})"
    end

    # Migrates the schema. First perform a migration on the source database,
    # with the 'db_type=postgres'. This performs any outstanding migrations on
    # the source database first, and also recreates db/schema.rb with the
    # correct trigger type (as specified with db_type). Then load (re)load the
    # schema in the target database.
    def migrate_schema(target_conn)
      if OPTS.no_schema
        run "Skipped schema migrations"
        return
      end

      if OPTS.no_migrate
        run "Dumping schema from source environment '#{OPTS.source_env}'",
            "RAILS_ENV=#{OPTS.source_env} rake db:schema:dump db_type=postgres 2>&1"
      else
        run "Migrating source environment '#{OPTS.source_env}'",
            "RAILS_ENV=#{OPTS.source_env} rake db:migrate db_type=postgres 2>&1"
      end

      run "Loading schema into target environment '#{OPTS.target_env}'",
          "rake db:schema:reload 2>&1"

      # Create 'schema_migrations' table.
      target_conn.exec(
        'DROP TABLE IF EXISTS "schema_migrations";' +
        'CREATE TABLE "schema_migrations" (version varchar(255));')
    end

    # Deletes all data from target tables. Only need to do this if we didn't
    # reload the schema.
    def delete_data(tables, target_conn)
      return unless OPTS.no_schema
      run 'Deleting all data from target tables' do
        target_conn.exec('BEGIN;')
        tables.each do |table_name|
          target_conn.exec %(TRUNCATE "#{table_name}";)
        end
        target_conn.exec('COMMIT;')
      end
    end

    # Ensure that the database types of the source and target environments are
    # the expected types.
    def check_db_type
      if SourceModelClass.connection.adapter_name != 'MySQL'
        puts "Error: Source database is not MySQL!"
        exit 1
      end
      if TargetModelClass.connection.adapter_name != 'PostgreSQL'
        puts "Error: Target database is not PostgreSQL!"
        exit 1
      end
    end

    # Drop all indexes before migrating the data. It is faster to create/update
    # indexes in a batch rather than per insert.
    def remove_indexes(tables, target_conn)
      if not OPTS.drop_indexes
        run "Skipped drop indexes"
        return
      end
      indexes = []
      run 'Dropping indexes' do
        target_conn.exec('BEGIN;')
        tables.each do |table_name|
          table_indexes = TargetModelClass.connection.indexes(table_name)
          table_indexes.each do |index|
            target_conn.exec %(DROP INDEX "#{index.name}";)
          end
          indexes += table_indexes
        end
        target_conn.exec('COMMIT;')
      end
      indexes
    end

    # Re-create the indexes we've dropped earlier.
    def add_indexes(indexes, target_conn)
      if not OPTS.drop_indexes or OPTS.no_commit
        run "Skipped add indexes"
        return
      end
      run 'Re-adding indexes' do
        target_conn.exec('BEGIN;')
        indexes.each do |index|
          type = index.unique ? 'UNIQUE ' : ''
          columns = index.columns.map{|c| %("#{c}") }.join(', ')
          target_conn.exec %(CREATE #{type}INDEX "#{index.name}" ON "#{index.table}" (#{columns});)
        end
        target_conn.exec('COMMIT;')
      end
    end

    # Perform the actual data migration.
    def migrate_data(tables, source_conn, target_conn)
      total_count = total_error_count = 0
      tables_size = tables.size
      utf8_ic  = Iconv.new('UTF-8//IGNORE', 'UTF-8')
      ascii_ic = Iconv.new('US-ASCII//TRANSLIT//IGNORE', 'UTF-8')

      tables.each_with_index do |table_name, table_idx|
        log "#{sprintf('%02d/%02d', table_idx+1, tables_size)} Table \"#{table_name}\"..."
        table_start_time = Time.now

        # Create prepared statements, which reduces database overheads of parsing
        # and optimizing the same query over and over.
        select_sql, insert_sql = get_prepared_sql(table_name)
        select_stmt = source_conn.prepare(select_sql)
        insert_stmt_name = "insert_#{table_name}"
        target_conn.prepare(insert_stmt_name, insert_sql)

        if OPTS.drop_triggers
          # Disabling triggers (thus constraint checks) speeds things up.
          target_conn.exec %(ALTER TABLE "#{table_name}" DISABLE TRIGGER ALL;)
        end

        # Perform online migration (insert directly from source to target database)
        # with paging to ensure that things fit into RAM.
        offset = count = 0
        error_ids = []; utf8_ids  = []; ascii_ids = []
        has_error = false
        while select_stmt.execute(offset).num_rows > 0
          # Transactions (BEGIN/COMMIT) make inserts much faster.
          target_conn.exec('BEGIN;')
          uncommited_rows = []

          while row = select_stmt.fetch do
            # Massage data to be Postgres friendly.
            row.map! do |val|
              if val.class == Mysql::Time
                if val.to_s == '0000-00-00 00:00:00'
                  # Special case for empty timestamp and non-nullable field
                  '2009-01-01 13:00:00'
                else
                  # Strip leading '0000-00-00 ' for Time fields.
                  val.to_s.split('0000-00-00 ', 2).last
                end
              else
                val
              end
            end

            # Perform insertion.
            begin
              target_conn.exec_prepared(insert_stmt_name, row)
              uncommited_rows << row
            rescue Exception => ex
              # The failed statement taints the earlier uncommited inserts. So
              # rollback and re-commit them.
              target_conn.exec('ROLLBACK; BEGIN;')
              uncommited_rows.each do |urow|
                begin
                  target_conn.exec_prepared(insert_stmt_name, urow)
                rescue Exception => ex
                  puts '' unless has_error
                  puts "  Failed to insert uncommitted record ##{urow[0]}"
                  error_ids << urow[0].to_i
                  has_error = true
                end
              end
              target_conn.exec('COMMIT;')
              uncommited_rows = []

              # Try the failed insert again with supposedly UTF8 safe strings.
              # See http://po-ru.com/diary/fixing-invalid-utf-8-in-ruby-revisited/
              row.map! do |val|
                (val.class == String) ? utf8_ic.iconv(val + '  ')[0..-2] : val
              end
              begin
                target_conn.exec_prepared(insert_stmt_name, row)
                utf8_ids << row[0].to_i
              rescue Exception => ex
                # Try again with US-ASCII to UTF8 re-encoding
                row.map! do |val|
                  (val.class == String) ? ascii_ic.iconv(val + '  ')[0..-2] : val
                end
                begin
                  target_conn.exec_prepared(insert_stmt_name, row)
                  ascii_ids << row[0].to_i
                rescue Exception => ex
                  error_ids << row[0].to_i
                end
              end

              # Restart transaction block
              target_conn.exec('BEGIN;')
            end
          end

          if OPTS.no_commit
            target_conn.exec('ROLLBACK;')
          else
            target_conn.exec('COMMIT;')
          end

          unless OPTS.no_progress
            print '.'; STDOUT.flush
          end

          count  += select_stmt.num_rows
          offset += PAGE_SIZE
        end

        # Reset the primary key sequence
        TargetModelClass.connection.reset_pk_sequence! table_name

        if OPTS.drop_triggers
          # Enable the earlier disabled triggers
          target_conn.exec %(ALTER TABLE "#{table_name}" ENABLE TRIGGER ALL;)
        end

        elapsed_time = Time.now - table_start_time
        total_count += count
        total_error_count += error_ids.size
        puts '' if not error_ids.empty? or not utf8_ids.empty? or not ascii_ids.empty?
        puts "  Error IDs: #{error_ids.inspect} (#{error_ids.size} records)" unless error_ids.empty?
#       puts "  UTF8 IDs : #{utf8_ids.inspect} (#{utf8_ids.size} records)"   unless utf8_ids.empty?
#       puts "  ASCII IDs: #{ascii_ids.inspect} (#{ascii_ids.size} records)" unless ascii_ids.empty?
        print "  #{delimit(count)} records, #{error_ids.size} errors (took #{format_time(elapsed_time)}, " +
              "#{delimit((count/elapsed_time).to_i)} rec/s)\n"
      end
      return total_count, total_error_count
    end

    # Check that the data has been migrated correctly.
    def validate_migration(tables)
      if OPTS.no_commit
        run "Skipped row count check"
        return
      end
      has_error = false
      run 'Checking that all row counts match' do
        tables.each do |table_name|
          SourceModelClass.set_table_name(table_name)
          TargetModelClass.set_table_name(table_name)
          source_count = SourceModelClass.count
          target_count = TargetModelClass.count
          if source_count != target_count
            puts '' unless has_error
            puts "  Table '#{table_name}' row counts mismatch: #{source_count} vs #{target_count}" +
                 " (by #{(source_count - target_count).abs} records)"
            has_error = true
          end
        end 
      end
    end

    # Returns the tables for migration.
    def get_tables
      return OPTS.tables unless OPTS.tables.empty?
      skip_tables = OPTS.skip_tables
      (TargetModelClass.connection.tables - skip_tables).sort
    end

    # Returns the source (MySQL) and target (Postgres) database connections.
    def get_db_connections
      db_info = YAML.load_file('config/database.yml')

      source_db_info = db_info[OPTS.source_env]
      source_conn = Mysql.new(source_db_info['host'],
                              source_db_info['username'],
                              source_db_info['password'],
                              source_db_info['database'])

      target_db_info = db_info[OPTS.target_env]
      target_conn = PGconn.open({
        :host     => target_db_info['host'],
        :user     => target_db_info['username'],
        :password => target_db_info['password'],
        :dbname   => target_db_info['database']
      })

      return source_conn, target_conn
    end

    # Returns the SQL for the SELECT (MySQL) and INSERT (Postgres) prepared
    # statements.
    def get_prepared_sql(table_name)
      fields_array = []
      values_array = []

      TargetModelClass.set_table_name(table_name)
      TargetModelClass.reset_column_information
      TargetModelClass.columns.each_with_index do |column, column_idx|
        fields_array << column.name
        values_array << %($#{column_idx+1}::#{column.sql_type.split('(', 2)[0]})
      end

      fields_mysql    = fields_array.map{|x| %(`#{x}`) }.join(', ')
      fields_postgres = fields_array.map{|x| %("#{x}") }.join(', ')
      values = values_array.join(', ')

      return %[SELECT #{fields_mysql} FROM `#{table_name}` LIMIT #{PAGE_SIZE} OFFSET ?;],
             %[INSERT INTO "#{table_name}" (#{fields_postgres}) VALUES (#{values});]
    end

    # Check that the source and target database schemas match (every table and
    # field).
    def ensure_tables_and_fields_match
      tables = SourceModelClass.connection.tables.sort
      run "Checking schema (#{tables.size} tables)" do
        target_tables = TargetModelClass.connection.tables.sort
        if not (diff = tables - target_tables).empty? or
           not (diff = target_tables - tables).empty?
          puts "\nError: Tables do not match: #{diff.join(', ')}"
          exit 1
        end
        tables.each do |table_name|
          SourceModelClass.set_table_name(table_name)
          TargetModelClass.set_table_name(table_name)
          SourceModelClass.reset_column_information
          TargetModelClass.reset_column_information
          source_columns = SourceModelClass.column_names.sort
          target_columns = TargetModelClass.column_names.sort
          if not (diff = source_columns - target_columns).empty? or 
             not (diff = target_columns - source_columns).empty?
            puts "\nError: Table '#{table_name}' has mis-matched field(s): #{diff.join(', ')}"
            exit 1
          end
        end
      end
    end

    # Print to STDOUT with time stamp.
    def log(message)
      print "#{Time.now.strftime("%Y-%m-%d %I:%M:%S")} #{message}"; STDOUT.flush
    end

    # Format elapsed time in seconds.
    def format_time(secs)
      return "#{secs}s" if secs < 60
      mins = (secs / 60).to_i
      secs = secs % 60
      return "#{mins}m #{secs}s" if mins < 60
      hrs  = (mins / 60).to_i
      mins = mins % 60
      "#{hrs}h #{mins}m #{secs}s" if mins < 60
    end

    # Returns a thousands separated string.
    def delimit(number)
      number.to_s.gsub(/(\d)(?=(\d\d\d)+(?!\d))/, "\\1,")
    end

  end
end
