require "date"
require "fileutils"
require "timeout"
require "debug"
require "open3"

class PostgresMaintenanceService
  S3_HOST = ENV.fetch("S3_HOST").freeze
  S3_REGION = ENV.fetch("S3_REGION").freeze
  S3_ACCESS_KEY = ENV.fetch("S3_ACCESS_KEY").freeze
  S3_SECRET_KEY = ENV.fetch("S3_SECRET_KEY").freeze
  S3_BUCKET_NAME = ENV.fetch("S3_BUCKET_NAME").freeze
  S3_HOST_BUCKET = ENV.fetch("S3_HOST_BUCKET").freeze

  # No / in the beginning or in the end allowed for:
  S3_WALS_DIR_KEY = ENV.fetch("S3_WALS_DIR_KEY", "backups/wals").freeze
  S3_PG_BASEBACKUP_DIR_KEY = ENV.fetch("S3_PG_BASEBACKUP_DIR_KEY", "backups/basebackups").freeze

  POSTGRES_HOST = ENV.fetch("POSTGRES_HOST").freeze
  POSTGRES_USER = ENV.fetch("POSTGRES_USER").freeze
  POSTGRES_PORT = ENV.fetch("POSTGRES_PORT", "5432").freeze
  POSTGRES_PASSWORD = ENV.fetch("POSTGRES_PASSWORD").freeze
  POSTGRES_CONF_MAX_CONNECTIONS = ENV.fetch("POSTGRES_CONF_MAX_CONNECTIONS", 200).freeze # Important for this setting to match what is set in the original database
  POSTGRES_RESTORE_COMMAND = "s3cmd get s3://#{S3_BUCKET_NAME}/#{S3_WALS_DIR_KEY}/%f %p --access_key=#{S3_ACCESS_KEY} --secret_key=#{S3_SECRET_KEY} --host=#{S3_HOST} --host-bucket=#{S3_HOST_BUCKET} --region=#{S3_REGION}".freeze

  # No trailing slash allowed for:
  LOCAL_WALS_DIR_PATH = ENV.fetch("LOCAL_WALS_DIR_PATH", "/wals").freeze
  LOCAL_PGDATA_DIR_PATH = ENV.fetch("LOCAL_PGDATA_DIR_PATH", "/pgdata").freeze
  LOCAL_LATEST_BACKUP_DIR_PATH = ENV.fetch("LOCAL_LATEST_BACKUP_DIR_PATH", "/latest_pg_basebackup").freeze
  LOCAL_OLDEST_BACKUP_DIR_PATH = ENV.fetch("LOCAL_OLDEST_BACKUP_DIR_PATH", "/oldest_pg_basebackup").freeze
  LOCAL_PG_BASEBACKUP_DIR_PATH = ENV.fetch("LOCAL_PG_BASEBACKUP_DIR_PATH", "/pg_basebackup").freeze
  LOCAL_POSTGRES_LOG_FILE_PATH = ENV.fetch("LOCAL_POSTGRES_LOG_FILE_PATH", "/postgresql.log").freeze

  WAITING_STEP_SEC = ENV.fetch("WAITING_STEP_SEC", 5).freeze
  MAX_WAITING_TIME_SEC = ENV.fetch("MAX_WAITING_TIME_SEC", 3000).freeze
  KEEP_PG_BASEBACKUPS_NUMBER = ENV.fetch("KEEP_PG_BASEBACKUPS_NUMBER", 5).freeze
  MIN_CLEANUP_DAYS = ENV.fetch("MIN_CLEANUP_DAYS", 5).to_i

  CHECK_QUERY = ENV.fetch("CHECK_QUERY").freeze
  CHECK_DATABASE = ENV.fetch("CHECK_DATABASE").freeze

  def initialize(command)
    validate_envs!(command)
  end

  def pg_basebackup
    messages = []
    puts "Checking if backup was taken already today"

    stdout, stderr, status = Open3.capture3("s3cmd ls s3://#{S3_BUCKET_NAME}/#{S3_PG_BASEBACKUP_DIR_KEY}/*")
    if status.success?
      puts "Successfully fetched basebackups list"
      base_backups = stdout.split("\n").map { |line| line.split("/").last }
    else
      messages << stderr
      return [messages, "FAILURE"]
    end

    if base_backups.include?(Date.today.to_s)
      puts "Backup was already taken today. Skipping"
      messages << "Backup was already taken today. Skipping"
      return [messages, "SUCCESS"]
    end

    puts "Starting pg_basebackup"

    _stdout, stderr, status = Open3.capture3("pg_basebackup -h #{POSTGRES_HOST} -p #{POSTGRES_PORT} -U #{POSTGRES_USER} -D #{LOCAL_PG_BASEBACKUP_DIR_PATH} --progress -z -Ft")
    if status.success?
      puts "Successfully processed backup"
      messages << "Successfully processed backup"
    else
      messages << stderr
      return [messages, "FAILURE"]
    end

    puts "Uploading basebackup to S3 "

    _stdout, stderr, status = Open3.capture3(%(s3cmd put --recursive #{LOCAL_PG_BASEBACKUP_DIR_PATH}/* "s3://#{S3_BUCKET_NAME}/#{S3_PG_BASEBACKUP_DIR_KEY}/#{Date.today}/" --no-check-certificate))
    if status.success?
      puts "Successfully uploaded new basebackup"
      messages << "Successfully uploaded new basebackup"
    else
      messages << stderr
      return [messages, "FAILURE"]
    end

    puts "Removing basebackup from local"
    FileUtils.rm_rf(LOCAL_PG_BASEBACKUP_DIR_PATH)

    [messages, "SUCCESS"]
  end

  def wal_cleanup
    unless old_wals_present?
      puts "There are no WAL files created more than #{MIN_CLEANUP_DAYS} days. Skipping"
      return
    end

    if !Dir.exist?(LOCAL_WALS_DIR_PATH) || Dir.empty?(LOCAL_WALS_DIR_PATH)
      puts "WAL archive not found locally, downloading from S3"
      `s3cmd sync s3://#{S3_BUCKET_NAME}/#{S3_WALS_DIR_KEY}/ #{LOCAL_WALS_DIR_PATH}/`
    end

    oldest_backup_wal_file_name = File.basename(Dir.glob("#{LOCAL_WALS_DIR_PATH}/*.backup").min_by { |path| File.mtime(path) })
    puts "Oldest backup found: #{oldest_backup_wal_file_name}"
    wals_to_delete = `pg_archivecleanup -n #{LOCAL_WALS_DIR_PATH} #{oldest_backup_wal_file_name}`
    wals_to_delete = wals_to_delete.split("\n").map { |l| l.split("/").last }.map { |wal_file_name| "s3://#{S3_BUCKET_NAME}/#{S3_WALS_DIR_KEY}/#{wal_file_name}" }
    puts "Deleting #{wals_to_delete.size} WAL files from S3"
    `s3cmd del #{wals_to_delete.join(" ")}`
  end

  def pg_basebackup_cleanup
    unless old_wals_present?
      puts "There are no WAL files created more than #{MIN_CLEANUP_DAYS} days. Skipping"
      return
    end

    base_backups = `s3cmd ls s3://#{S3_BUCKET_NAME}/#{S3_PG_BASEBACKUP_DIR_KEY}/*`.split("\n").map { |line| line.split("/").last }
    puts "Found #{base_backups.size} backups on S3"
    return if base_backups.size <= KEEP_PG_BASEBACKUPS_NUMBER

    KEEP_PG_BASEBACKUPS_NUMBER.times { base_backups.pop }
    puts "Going to delete next backups: #{base_backups.join(", ")}"

    if !Dir.exist?(LOCAL_WALS_DIR_PATH) || Dir.empty?(LOCAL_WALS_DIR_PATH)
      puts "WAL archive not found locally, downloading from S3"
      `s3cmd sync s3://#{S3_BUCKET_NAME}/#{S3_WALS_DIR_KEY}/ #{LOCAL_WALS_DIR_PATH}/`
    end

    wals_backups_to_delete = Dir.glob("#{LOCAL_WALS_DIR_PATH}/*.backup")
      .select { |path| base_backups.include?(File.mtime(path).to_date.to_s) }
      .map { |wal_backup_path| File.basename(wal_backup_path) }
      .map { |wal_file_name| "s3://#{S3_BUCKET_NAME}/#{S3_WALS_DIR_KEY}/#{wal_file_name}" }

    if wals_backups_to_delete.size.positive?
      puts "Deleting WAL .backup files: #{wals_backups_to_delete.join(", ")}".to_s
      `s3cmd del #{wals_backups_to_delete.join(" ")}`
    else
      puts "WAL .backup files were not found for basebackups, skipping"
    end
  end

  def restore
    prepare_restore

    puts "Starting postgres in foreground"
    `su postgres -c "/usr/lib/postgresql/14/bin/postgres -D #{LOCAL_PGDATA_DIR_PATH}"`
  end

  def restore_and_check
    prepare_restore

    puts "Starting postgres in background"
    `su postgres -c "/usr/lib/postgresql/14/bin/postgres -D #{LOCAL_PGDATA_DIR_PATH} > #{LOCAL_POSTGRES_LOG_FILE_PATH} 2>&1 &"`

    puts "Check procedure started. It will try to wait until postgres is fully operational (may take a while). Max waiting time is: #{MAX_WAITING_TIME_SEC} seconds"
    check
  end

  private

  def old_wals_present?
    all_wals_ls = `s3cmd ls s3://recario-space/backups/wals/*`.split("\n")
    oldest_wal = all_wals_ls.map { |l| l.split(" ").first }.min
    oldest_wal_created_days_ago = (Date.today - Date.parse(oldest_wal)).to_i
    oldest_wal_created_days_ago >= MIN_CLEANUP_DAYS
  end

  def prepare_restore
    if !Dir.exist?(LOCAL_PGDATA_DIR_PATH) || Dir.empty?(LOCAL_PGDATA_DIR_PATH)
      base_backups = `s3cmd ls s3://#{S3_BUCKET_NAME}/#{S3_PG_BASEBACKUP_DIR_KEY}/*`.split("\n").map { |line| line.split("/").last }
      latest_backup = base_backups.map { |backup_date_from_file_path| Date.parse(backup_date_from_file_path).to_s }.max
      ractors = []

      puts "Found next backups: #{base_backups.join(", ")}, latest is #{latest_backup}"

      FileUtils.mkdir_p(LOCAL_LATEST_BACKUP_DIR_PATH)
      FileUtils.mkdir_p(LOCAL_PGDATA_DIR_PATH)
      FileUtils.mkdir_p("#{LOCAL_PGDATA_DIR_PATH}/pg_wal")

      ractors << Ractor.new do
        r_latest_backup = receive
        puts "Downloading base.tar.gz"
        `s3cmd get s3://#{S3_BUCKET_NAME}/#{S3_PG_BASEBACKUP_DIR_KEY}/#{r_latest_backup}/base.tar.gz #{LOCAL_LATEST_BACKUP_DIR_PATH}/base.tar.gz`
        puts "Finished downloading base.tar.gz"
        puts "Extracting base.tar.gz"
        `tar -xvf #{LOCAL_LATEST_BACKUP_DIR_PATH}/base.tar.gz -C #{LOCAL_PGDATA_DIR_PATH}`
        puts "Finished extracting base.tar.gz"
        true
      end

      ractors << Ractor.new do
        r_latest_backup = receive
        puts "Downloading pg_wal.tar.gz"
        `s3cmd get s3://#{S3_BUCKET_NAME}/#{S3_PG_BASEBACKUP_DIR_KEY}/#{r_latest_backup}/pg_wal.tar.gz #{LOCAL_LATEST_BACKUP_DIR_PATH}/pg_wal.tar.gz`
        puts "Finished downloading pg_wal.tar.gz"
        puts "Extracting pg_wal.tar.gz"
        `tar -xvf #{LOCAL_LATEST_BACKUP_DIR_PATH}/pg_wal.tar.gz -C #{LOCAL_PGDATA_DIR_PATH}/pg_wal`
        puts "Finished extracting pg_wal.tar.gz"
        true
      end

      ractors.each { |r| r.send(latest_backup) }

      ractors.map(&:take)
    else
      puts "Postgres data directory #{LOCAL_PGDATA_DIR_PATH} is not empty, using it to restore"
    end

    puts "Creating postgresql.conf"
    File.open("#{LOCAL_PGDATA_DIR_PATH}/postgresql.conf", "w") do |f|
      f.puts("max_connections = #{POSTGRES_CONF_MAX_CONNECTIONS}")
      f.puts("restore_command='#{POSTGRES_RESTORE_COMMAND}'")
    end

    puts "Creating pg_hba.conf"
    File.open("#{LOCAL_PGDATA_DIR_PATH}/pg_hba.conf", "w") do |f|
      f.puts("host all all 127.0.0.1/32 trust")
      f.puts("host all all 0.0.0.0/0 md5")
    end

    puts "Configuring PGDATA and recovery.signal"
    FileUtils.touch("#{LOCAL_PGDATA_DIR_PATH}/recovery.signal")
    FileUtils.chown "postgres", "postgres", LOCAL_PGDATA_DIR_PATH
    FileUtils.chmod 0o700, LOCAL_PGDATA_DIR_PATH

    puts "Configuring PostgreSQL log file"
    FileUtils.touch(LOCAL_POSTGRES_LOG_FILE_PATH)
    FileUtils.chmod 0o700, LOCAL_POSTGRES_LOG_FILE_PATH
    FileUtils.chown "postgres", "postgres", LOCAL_POSTGRES_LOG_FILE_PATH
  end

  def check
    waiting_time = 0

    Timeout.timeout(MAX_WAITING_TIME_SEC) do
      loop do
        if postgres_running?
          puts "Postgres started in #{waiting_time} seconds"
          users_count = `psql -U #{POSTGRES_USER} -h localhost -d #{CHECK_DATABASE} -t -c "#{CHECK_QUERY};"`.strip.to_i

          if users_count > 0
            puts "There are #{users_count} Users records in this backup. Success."
            exit(0)
          else
            puts "There are #{users_count} Users. Error"
            exit(1)
          end
        else
          puts "Waiting for Postgres to start..."
          sleep(WAITING_STEP_SEC)
          waiting_time += WAITING_STEP_SEC
          redo
        end
      end
    end
  rescue Timeout::Error
    puts "Postgres hasn't started in #{MAX_WAITING_TIME_SEC} seconds"
    exit(1)
  end

  def postgres_running?
    output = `pg_isready 2>&1` # Capture both stdout and stderr

    if $?.success?
      puts "PostgreSQL is running"
      true
    else
      puts "PostgreSQL is not running: #{output}"
      false
    end
  end

  def validate_envs!(command)
    # TODO: Implement
  end
end
