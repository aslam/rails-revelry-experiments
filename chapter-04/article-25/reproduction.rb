# frozen_string_literal: true

# ruby planning/article-25-reproduction.rb
# Each producer and consumer boots in a separate Ruby process.
require "json"
require "tmpdir"
require "open3"
require "rbconfig"

if ARGV.empty?
  Dir.mktmpdir("railsrevelry-article-25-") do |directory|
    run = lambda do |*arguments|
      output, status = Open3.capture2e(RbConfig.ruby, __FILE__, *arguments)
      abort output unless status.success?
      puts output
    end
    old_payload = File.join(directory, "old.json")
    new_payload = File.join(directory, "new.json")
    run.call("produce", "old", old_payload)
    %w[old missing shim required compatible].each { |release| run.call("consume", release, old_payload) }
    run.call("produce", "compatible", new_payload)
    run.call("consume", "reverse", new_payload)
    database = File.join(directory, "queue.sqlite3")
    run.call("queue_produce", "old", database)
    run.call("queue_consume", "missing", database)
  end
  exit
end

operation, release, path = ARGV
gem "rails", "8.1.3"
require "active_job"
ActiveJob::Base.logger = Logger.new(nil)

if operation.start_with?("queue_")
  gem "solid_queue", "1.4.0"
  require "rails"
  require "active_record/railtie"
  require "active_job/railtie"
  require "solid_queue"
  ENV["RAILS_ENV"] = "test"
  ENV["DATABASE_URL"] = "sqlite3:#{path}"
  class Article25Application < Rails::Application
    config.eager_load = false
    config.secret_key_base = "article-25-reproduction"
    config.logger = ActiveSupport::Logger.new(nil)
    config.active_job.queue_adapter = :solid_queue
  end
  Article25Application.initialize!
  ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: path)
  if operation == "queue_produce"
    ActiveRecord::Schema.verbose = false
    load File.join(Gem.loaded_specs.fetch("solid_queue").full_gem_path,
      "lib/generators/solid_queue/install/templates/db/queue_schema.rb")
  end
end

class ApplicationJob < ActiveJob::Base
  class_attribute :handler_reached, default: false
  rescue_from(StandardError) do |error|
    ApplicationJob.handler_reached = true
    raise error
  end
end

unless %w[missing shim].include?(release)
  class FulfillOrderJob < ApplicationJob
    def perform(order_id)
      "fulfilled=#{order_id} warehouse=default"
    end
  end
end

case release
when "required"
  class FulfillOrderJob
    def perform(order_id, warehouse)
      "fulfilled=#{order_id} warehouse=#{warehouse}"
    end
  end
when "compatible"
  class FulfillOrderJob
    def perform(order_id, warehouse = "default")
      "fulfilled=#{order_id} warehouse=#{warehouse}"
    end
  end
when "missing", "shim"
  class DispatchOrderJob < ApplicationJob
    def perform(order_id)
      "fulfilled=#{order_id} warehouse=default"
    end
  end
end

if release == "shim"
  class FulfillOrderJob < DispatchOrderJob
  end
end

case operation
when "produce"
  arguments = release == "old" ? [42] : [42, "west"]
  File.write(path, JSON.generate(FulfillOrderJob.new(*arguments).serialize))
  puts "producer=#{release} arguments=#{arguments.inspect}"
when "queue_produce"
  job = FulfillOrderJob.perform_later(42)
  raise "enqueue failed" unless job.successfully_enqueued? && SolidQueue::ReadyExecution.count == 1
  puts "queue_enqueued class=#{job.class} stored=#{SolidQueue::Job.count}"
else
  result = error = nil
  begin
    if operation == "queue_consume"
      process = SolidQueue::Process.register(kind: "Worker", name: "article-25", pid: Process.pid)
      claimed = SolidQueue::ReadyExecution.claim(["*"], 1, process.id).fetch(0)
      result = claimed.perform
    else
      result = ActiveJob::Base.execute(JSON.parse(File.read(path)))
    end
  rescue StandardError => caught
    error = caught
  end

  expected_error = case release
  when "missing" then ActiveJob::UnknownJobClassError
  when "required", "reverse" then ArgumentError
  end
  raise "unexpected result: #{result.inspect}, #{error.inspect}" unless error&.class == expected_error
  expected_handler = %w[required reverse].include?(release)
  raise "wrong handler path" unless ApplicationJob.handler_reached == expected_handler
  raise "wrong fulfillment" if expected_error.nil? && result != "fulfilled=42 warehouse=default"
  puts "consumer=#{release} result=#{result.inspect} error=#{error&.class} handler=#{ApplicationJob.handler_reached}"

  if operation == "queue_consume"
    failure = SolidQueue::FailedExecution.sole
    raise "wrong persisted error" unless failure.exception_class == expected_error.name
    raise "job not retained" unless failure.job.class_name == "FulfillOrderJob"
    raise "claim not removed" unless SolidQueue::ClaimedExecution.count.zero?
    puts "queue_failed=#{SolidQueue::FailedExecution.count} class=#{failure.job.class_name} error=#{failure.exception_class}"
  end
end
