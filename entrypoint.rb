#!/usr/local/bin/ruby

require "./postgres_maintenance_service"
require "net/smtp"

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

def notify(messages, subject)
  if %w[SMTP_HOST SMTP_PORT SMTP_USER SMTP_PASSWORD SMTP_FROM SMTP_TO].any? { |smtp_var_name| ENV[smtp_var_name].to_s.length.zero? }
    puts "Missing SMTP settings, no email will be sent."
    return [messages, subject]
  end

  body = <<~MESSAGE_END
    From: PostgreSQL Maintenance <#{ENV["SMTP_FROM"]}>
    To: Admin <#{ENV["SMTP_TO"]}>
    Subject: #{subject}

    #{messages.join("\n")}
  MESSAGE_END

  Net::SMTP.start(ENV["SMTP_HOST"], ENV["SMTP_PORT"], "localhost", ENV["SMTP_USER"], ENV["SMTP_PASSWORD"], :login) do |smtp|
    smtp.send_message(body, ENV["SMTP_FROM"], ENV["SMTP_TO"])
  end
end

case command
when "pg_basebackup"
  notify(*service.pg_basebackup)
when "restore"
  service.restore
when "restore_and_check"
  notify(*service.restore_and_check)
when "wal_cleanup"
  notify(*service.wal_cleanup)
when "pg_basebackup_cleanup"
  notify(*service.pg_basebackup_cleanup)
else
  puts "Available commands: pg_basebackup, restore, restore_and_check, wal_cleanup, pg_basebackup_cleanup"
end
