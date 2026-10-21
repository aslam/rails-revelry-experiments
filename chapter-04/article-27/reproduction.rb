# frozen_string_literal: true

# Article: https://railsrevelry.substack.com/p/why-a-background-job-can-run-more-than-once
# Run: ruby chapter-04/article-27/reproduction.rb
# Uses real Solid Queue claims and process death; invokes pruning explicitly.
gem "rails", "8.1.3"
gem "solid_queue", "1.4.0"
gem "sqlite3", "2.9.5"
# Avoid the local Ruby installation's default/installed digest activation conflict.
gem "digest", "3.1.0"
# Rails 8.1.3 passes the JSON options as a positional hash.
gem "json", "2.19.9"
require "tmpdir"
require "open3"
require "rbconfig"
require "timeout"
require "rails"
require "active_record/railtie"
require "active_job/railtie"
require "solid_queue"

child = ARGV.first == "worker"
directory = child ? ARGV.fetch(1) : Dir.mktmpdir("railsrevelry-article-27-")
at_exit { FileUtils.remove_entry(directory) } unless child
ENV["RAILS_ENV"] = "test"
ENV["DATABASE_URL"] = "sqlite3:#{directory}/application.sqlite3"

class Article27Application < Rails::Application
  config.eager_load = false
  config.secret_key_base = "article-27-reproduction"
  config.logger = ActiveSupport::Logger.new(nil)
  config.active_job.queue_adapter = :solid_queue
end
Article27Application.initialize!
ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: "#{directory}/application.sqlite3")
SolidQueue::Record.establish_connection(adapter: "sqlite3", database: "#{directory}/queue.sqlite3")
class ReceiverRecord < ActiveRecord::Base
  self.abstract_class = true
end
ReceiverRecord.establish_connection(adapter: "sqlite3", database: "#{directory}/receiver.sqlite3")
class Delivery < ActiveRecord::Base; end
class Effect < ReceiverRecord; end

unless child
  ActiveRecord::Schema.verbose = false
  ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: "#{directory}/queue.sqlite3")
  load File.join(Gem.loaded_specs.fetch("solid_queue").full_gem_path,
    "lib/generators/solid_queue/install/templates/db/queue_schema.rb")
  ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: "#{directory}/application.sqlite3")
  Delivery.connection.create_table(:deliveries) do |t|
    t.string :operation_key, null: false
    t.boolean :deduplicate, null: false, default: false
    t.integer :receipt_id
  end
  Effect.connection.create_table(:effects) do |t|
    t.string :operation_key, null: false
    t.string :deduplication_key
  end
  Effect.connection.add_index(:effects, :deduplication_key, unique: true)
end

class DeliverWebhookJob < ActiveJob::Base
  def perform(delivery_id)
    delivery = Delivery.find(delivery_id)
    return if delivery.receipt_id.present?

    # shortcut: a committed receiver row models the effect; use HTTP for transport tests.
    effect = if delivery.deduplicate?
      Effect.create_or_find_by!(deduplication_key: delivery.operation_key) do |record|
        record.operation_key = delivery.operation_key
      end
    else
      Effect.create!(operation_key: delivery.operation_key)
    end
    if ARGV.first == "worker"
      STDOUT.puts "receiver_committed"
      STDOUT.flush
      STDIN.read(1) # The parent kills this process while its receipt is still unwritten.
    end
    delivery.update!(receipt_id: effect.id)
  end
end

def assert(condition, message)
  raise message unless condition
end

def register_process
  SolidQueue::Process.register(kind: "Worker", name: SecureRandom.uuid,
    pid: Process.pid, hostname: "article-27")
end

def run_ready
  process = register_process
  claims = SolidQueue::ReadyExecution.claim(["default"], 1, process.id)
  assert(claims.size == 1, "expected one ready execution")
  claims.first.perform
ensure
  process&.deregister
end

if child
  process = register_process
  SolidQueue::ReadyExecution.claim(["default"], 1, process.id).fetch(0).perform
  abort "worker should have been killed at barrier"
end

puts "Ruby #{RUBY_VERSION}; Rails #{Rails.version}; Solid Queue #{Gem.loaded_specs.fetch('solid_queue').version}; SQLite #{SQLite3::SQLITE_VERSION}"
normal = Delivery.create!(operation_key: "normal")
normal_job = DeliverWebhookJob.perform_later(normal.id)
run_ready
assert(normal.reload.receipt_id.present?, "normal receipt missing")
assert(SolidQueue::Job.find(normal_job.provider_job_id).finished?, "normal job unfinished")
assert(Effect.where(operation_key: "normal").count == 1, "normal effect missing")
DeliverWebhookJob.perform_now(normal.id)
assert(Effect.where(operation_key: "normal").count == 1, "receipt guard did not skip delivery")
puts "normal: effects=1 receipt=true finished=true"

[false, true].each do |deduplicate|
  key = deduplicate ? "protected" : "unprotected"
  delivery = Delivery.create!(operation_key: key, deduplicate: deduplicate)
  active_job = DeliverWebhookJob.perform_later(delivery.id)
  queue_job = SolidQueue::Job.find(active_job.provider_job_id)
  Open3.popen2e(RbConfig.ruby, __FILE__, "worker", directory) do |input, output, waiter|
    begin
      Timeout.timeout(20) do
        loop do
          line = output.gets
          raise "worker exited before barrier" unless line
          break if line.strip == "receiver_committed"
        end
      end
      Process.kill("KILL", waiter.pid)
      status = waiter.value
      assert(status.signaled? && status.termsig == Signal.list.fetch("KILL"), "worker was not killed")
    ensure
      if waiter.alive?
        Process.kill("KILL", waiter.pid)
        waiter.value
      end
      input.close
    end
  end
  assert(delivery.reload.receipt_id.nil?, "receipt unexpectedly persisted")
  assert(Effect.where(operation_key: key).count == 1, "first effect missing")
  claim = SolidQueue::ClaimedExecution.find_by!(job_id: queue_job.id)
  process = claim.process
  process.update!(last_heartbeat_at: 1.hour.ago)
  # shortcut: invoke real pruning directly; run a supervisor to test heartbeat timing.
  SolidQueue::Process.prune
  failure = SolidQueue::FailedExecution.find_by!(job_id: queue_job.id)
  assert(failure.exception_class == "SolidQueue::Processes::ProcessPrunedError", "wrong recovery error")
  assert(!SolidQueue::ReadyExecution.exists?(job_id: queue_job.id), "unexpected automatic retry")
  assert(!SolidQueue::ClaimedExecution.exists?(job_id: queue_job.id), "claim remained after pruning")
  puts "#{key} after prune: effects=1 receipt=false failed=true ready=false"

  first_effect_id = Effect.find_by!(operation_key: key).id
  failure.retry
  run_ready
  effects = Effect.where(operation_key: key).count
  assert(effects == (deduplicate ? 1 : 2), "wrong effect count after retry")
  assert(delivery.reload.receipt_id.present?, "retry receipt missing")
  assert(queue_job.reload.finished?, "retried job unfinished")
  assert(queue_job.active_job_id == active_job.job_id, "manual retry changed job identity")
  assert(delivery.receipt_id == first_effect_id, "original result not recovered") if deduplicate
  puts "#{key} after manual retry: effects=#{effects} receipt=true finished=true"

  next unless deduplicate
  delivery.update!(receipt_id: nil)
  assert(delivery.reload.receipt_id.nil?, "new-job recovery must start without a receipt")
  second_job = DeliverWebhookJob.perform_later(delivery.id)
  assert(second_job.job_id != active_job.job_id, "new enqueue reused job identity")
  run_ready
  assert(Effect.where(operation_key: key).count == 1, "new job repeated protected effect")
  assert(delivery.reload.receipt_id == first_effect_id, "new job did not recover original receipt")
  puts "protected new job: different_job_id=true effects=1 same_receipt=true"
end
