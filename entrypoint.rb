#!/usr/local/bin/ruby

require "./postgres_maintenance_service"

raise "Single command expected as input" if ARGV.size > 1
raise "Missing S3 configuration" unless %w[S3_ACCESS_KEY S3_REGION S3_HOST_BUCKET S3_HOST_BUCKET S3_SECRET_KEY].all? { |s3_var_name| ENV[s3_var_name].to_s.length.positive? }
raise "Missing PostgreSQL configuration" unless %w[POSTGRES_HOST POSTGRES_PORT POSTGRES_USER POSTGRES_PASSWORD].all? { |pg_var_name| ENV[pg_var_name].to_s.length.positive? }

puts "Initialize s3cmd config"
`envsubst < ./s3cfg.template > ~/.s3cfg`

puts "Initialize ~/.pgpass"
`echo "#{ENV["POSTGRES_HOST"]}:#{ENV["POSTGRES_PORT"]}:*:#{ENV["POSTGRES_USER"]}:#{ENV["POSTGRES_PASSWORD"]}" > ~/.pgpass`
`chmod 600 ~/.pgpass`

command = ARGV[0]

service = PostgresMaintenanceService.new(command)

case command
when "pg_basebackup"
  service.pg_basebackup
when "restore"
  service.restore
when "restore_and_check"
  service.restore_and_check
when "wal_cleanup"
  service.wal_cleanup
when "pg_basebackup_cleanup"
  service.pg_basebackup_cleanup
else
  puts "Available commands: pg_basebackup, restore, restore_and_check, wal_cleanup, pg_basebackup_cleanup"
end
