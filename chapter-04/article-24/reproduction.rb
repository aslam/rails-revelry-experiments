# frozen_string_literal: true

# Reproduces the transaction/queue handoff described in RailsRevelry Article 24.
#
#   ruby chapter-04/article-24/reproduction.rb separate
#   ruby chapter-04/article-24/reproduction.rb same

require "bundler/inline"

gemfile do
  source "https://rubygems.org"

  gem "rails", "8.1.3", require: false
  gem "solid_queue", "1.4.0", require: false
  gem "sqlite3", "2.9.5", require: false
end

require "tmpdir"
require "fileutils"
require "rails"
require "active_record/railtie"
require "active_job/railtie"
require "solid_queue"

topology = ARGV.fetch(0, "separate")
abort "usage: #{$PROGRAM_NAME} separate|same" unless %w[separate same].include?(topology)

temporary_directory = Dir.mktmpdir("railsrevelry-article-24-")
at_exit { FileUtils.remove_entry(temporary_directory) if File.directory?(temporary_directory) }
primary_database = File.join(temporary_directory, "primary.sqlite3")
queue_database = topology == "same" ? primary_database : File.join(temporary_directory, "queue.sqlite3")

database_configurations = {
  "test" => {
    "primary" => { "adapter" => "sqlite3", "database" => primary_database },
    "queue" => { "adapter" => "sqlite3", "database" => queue_database }
  }
}

ENV["RAILS_ENV"] = "test"
ENV["DATABASE_URL"] = "sqlite3:#{primary_database}"
ActiveRecord::Base.configurations = database_configurations

class Article24Application < Rails::Application
  config.eager_load = false
  config.secret_key_base = "article-24-reproduction"
  config.logger = ActiveSupport::Logger.new(nil)
  config.active_job.queue_adapter = :solid_queue
end

if topology == "separate"
  Article24Application.config.solid_queue.connects_to = { database: { writing: :queue } }
end

Article24Application.initialize!
ActiveRecord::Base.configurations = database_configurations

class ApplicationRecord < ActiveRecord::Base
  primary_abstract_class
end

ApplicationRecord.establish_connection(:primary)

ApplicationRecord.connection.create_table(:orders) do |table|
  table.string :state, null: false
  table.timestamps null: false
end

queue_schema = File.expand_path(
  "lib/generators/solid_queue/install/templates/db/queue_schema.rb",
  Gem.loaded_specs.fetch("solid_queue").full_gem_path
)

if topology == "same"
  load queue_schema
else
  ActiveRecord::Base.establish_connection(:queue)
  load queue_schema
  ActiveRecord::Base.establish_connection(:primary)
end

class Order < ApplicationRecord
end

class FulfillOrderJob < ActiveJob::Base
  self.enqueue_after_transaction_commit = true

  def perform(order_id)
  end
end

class ImmediateFulfillOrderJob < ActiveJob::Base
  self.enqueue_after_transaction_commit = false

  def perform(order_id)
  end
end

events = []
timeline = []
ActiveSupport::Notifications.subscribe("transaction.active_record") do |event|
  connection = event.payload.fetch(:connection)
  timeline << {
    event: event.name,
    database: connection.pool.db_config.name,
    outcome: event.payload.fetch(:outcome)
  }
end

ActiveSupport::Notifications.subscribe("enqueue.active_job") do |event|
  job = event.payload.fetch(:job)
  error = event.payload[:exception_object] || job.enqueue_error
  enqueue_event = {
    event: event.name,
    successful: job.successfully_enqueued?,
    provider_job_id: job.provider_job_id,
    error: error&.class&.name
  }
  events << enqueue_event
  timeline << enqueue_event
end

puts "topology=#{topology}"

job = nil
ApplicationRecord.transaction do
  order = Order.create!(state: "accepted")
  job = FulfillOrderJob.perform_later(order.id)

  puts "inside_transaction order_count=#{Order.count} " \
       "queue_job_count=#{SolidQueue::Job.count} " \
       "successful=#{job.successfully_enqueued?} " \
       "provider_job_id=#{job.provider_job_id.inspect} " \
       "enqueue_events=#{events.size}"
end

puts "after_commit order_count=#{Order.count} " \
     "queue_job_count=#{SolidQueue::Job.count} " \
     "successful=#{job.successfully_enqueued?} " \
     "provider_job_id=#{job.provider_job_id.inspect} " \
     "enqueue_events=#{events.inspect} " \
     "timeline=#{timeline.inspect}"

order_count_before_rollback = Order.count
queue_count_before_rollback = SolidQueue::Job.count
ApplicationRecord.transaction do
  order = Order.create!(state: "will_rollback")
  immediate_job = ImmediateFulfillOrderJob.perform_later(order.id)

  puts "before_immediate_rollback order_id=#{order.id} " \
       "successful=#{immediate_job.successfully_enqueued?} " \
       "provider_job_id=#{immediate_job.provider_job_id.inspect}"

  raise ActiveRecord::Rollback
end

puts "after_immediate_rollback order_delta=#{Order.count - order_count_before_rollback} " \
     "queue_job_delta=#{SolidQueue::Job.count - queue_count_before_rollback}"

order_count_before_rollback = Order.count
queue_count_before_rollback = SolidQueue::Job.count
ApplicationRecord.transaction do
  order = Order.create!(state: "will_rollback")
  deferred_job = FulfillOrderJob.perform_later(order.id)

  puts "before_deferred_rollback order_id=#{order.id} " \
       "successful=#{deferred_job.successfully_enqueued?} " \
       "provider_job_id=#{deferred_job.provider_job_id.inspect}"

  raise ActiveRecord::Rollback
end

puts "after_deferred_rollback order_delta=#{Order.count - order_count_before_rollback} " \
     "queue_job_delta=#{SolidQueue::Job.count - queue_count_before_rollback}"

if topology == "separate"
  SolidQueue::Record.connection.drop_table(:solid_queue_jobs)
  timeline.clear

  failed_job = nil
  begin
    ApplicationRecord.transaction do
      order = Order.create!(state: "accepted")
      failed_job = FulfillOrderJob.perform_later(order.id)

      puts "before_failed_enqueue order_id=#{order.id} " \
           "successful=#{failed_job.successfully_enqueued?} " \
           "provider_job_id=#{failed_job.provider_job_id.inspect}"
    end
  rescue SolidQueue::Job::EnqueueError => error
    puts "enqueue_failure error=#{error.class} cause=#{error.message.lines.first.strip.inspect}"
  end

  puts "after_failed_enqueue order_count=#{Order.count} " \
       "successful=#{failed_job.successfully_enqueued?} " \
       "provider_job_id=#{failed_job.provider_job_id.inspect} " \
       "enqueue_error=#{failed_job.enqueue_error.inspect} " \
       "timeline=#{timeline.inspect}"
end
