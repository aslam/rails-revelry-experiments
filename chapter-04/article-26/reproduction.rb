# frozen_string_literal: true

# Run: ruby chapter-04/article-26/reproduction.rb
# Test adapter only: no network, database, worker, or real scheduling delay.
gem "activejob", "8.1.3"
require "active_job"
require "json"
require "stringio"

ActiveJob::Base.logger = Logger.new(nil)
ActiveJob::Base.queue_adapter = :test

class ReceiptWriteFailed < StandardError; end

class RetryProbeJob < ActiveJob::Base
  class_attribute :deliveries, default: []
  class_attribute :starts, default: []
  retry_on ReceiptWriteFailed, wait: 10.seconds, attempts: 3, jitter: 0

  def perform(always_fail)
    self.class.starts << [job_id, executions, @sent]
    self.class.deliveries << "webhook accepted"
    @sent = true
    raise ReceiptWriteFailed if always_fail || executions == 1
  end
end


def check(condition, message)
  raise message unless condition
end

# Round-trip only Active Job's serialized fields; omit test-adapter extras.
def next_payload
  raw = ActiveJob::Base.queue_adapter.enqueued_jobs.shift
  check(raw, "expected an enqueued attempt")
  JSON.parse(JSON.generate(raw.select { |key, _| key.is_a?(String) }))
end

adapter = ActiveJob::Base.queue_adapter
job = RetryProbeJob.perform_later(false)
ActiveJob::Base.execute(next_payload)
retry_data = next_payload
check(retry_data.fetch("job_id") == job.job_id, "job ID changed")
check(retry_data.fetch("executions") == 1, "execution count missing")
check(retry_data.fetch("exception_executions").values == [1], "handler count missing")
delay = Time.iso8601(retry_data.fetch("scheduled_at")) - Time.iso8601(retry_data.fetch("enqueued_at"))
check((delay - 10).abs < 0.5, "wrong retry delay")
ActiveJob::Base.execute(retry_data) # Deliberately execute without waiting.
check(RetryProbeJob.deliveries.size == 2, "first side effect did not repeat")
check(RetryProbeJob.starts.map { |row| row[1] } == [1, 2], "wrong attempts")
check(RetryProbeJob.starts.all? { |row| row[2].nil? }, "instance state survived")
check(adapter.enqueued_jobs.empty?, "successful attempt scheduled a retry")
puts "recovery: attempts=1,2 deliveries=2 same_job_id=true instance_state_restored=false"

RetryProbeJob.deliveries = []
RetryProbeJob.starts = []
RetryProbeJob.perform_later(true)
2.times { ActiveJob::Base.execute(next_payload) }
error = nil
begin
  ActiveJob::Base.execute(next_payload)
rescue ReceiptWriteFailed => caught
  error = caught
end
check(error, "exhaustion did not raise")
check(RetryProbeJob.starts.map { |row| row[1] } == [1, 2, 3], "attempts excludes original")
check(adapter.enqueued_jobs.empty?, "exhaustion scheduled a fourth attempt")
puts "exhaustion: attempts=1,2,3 deliveries=3 error=ReceiptWriteFailed fourth_attempt=false"

class TerminalProbeJob < ActiveJob::Base
  class_attribute :terminal_calls, default: 0
  retry_on ReceiptWriteFailed, attempts: 1 do |_job, _error|
    self.terminal_calls += 1
  end

  def perform
    raise ReceiptWriteFailed
  end
end

TerminalProbeJob.perform_later
ActiveJob::Base.execute(next_payload)
check(TerminalProbeJob.terminal_calls == 1, "terminal block missing")
check(adapter.enqueued_jobs.empty?, "terminal block scheduled a retry")
puts "terminal_block: called=1 error_escaped=false retry_enqueued=false"

# Initial enqueue succeeds; the adapter rejects the scheduled retry.
class RejectRetryAdapter < ActiveJob::QueueAdapters::TestAdapter
  def enqueue_at(*)
    raise ActiveJob::EnqueueError, "simulated queue write failure"
  end
end

ActiveJob::Base.queue_adapter = RejectRetryAdapter.new
logs = StringIO.new
ActiveJob::Base.logger = Logger.new(logs)
retry_observations = []
stopped = []
retry_subscription = ActiveSupport::Notifications.subscribe("enqueue_retry.active_job") do |event|
  retried = event.payload.fetch(:job)
  retry_observations << [retried.successfully_enqueued?, retried.enqueue_error&.class]
end
stop_subscription = ActiveSupport::Notifications.subscribe("retry_stopped.active_job") do |event|
  stopped << event
end
begin
  RetryProbeJob.perform_later(true)
  ActiveJob::Base.execute(next_payload)
  check(retry_observations == [[false, ActiveJob::EnqueueError]], "missing failed enqueue state")
  check(ActiveJob::Base.queue_adapter.enqueued_jobs.empty?, "failed enqueue stored a retry")
  check(stopped.empty?, "enqueue failure emitted retry_stopped")
  check(logs.string.include?("Retrying RetryProbeJob"), "retry log missing")
  check(logs.string.include?("Failed enqueuing RetryProbeJob"), "enqueue error log missing")
  check(logs.string.include?("Performed RetryProbeJob"), "normal completion log missing")
  puts "enqueue_failure: error_escaped=false retry_enqueued=false retry_stopped=false retry_log=true enqueue_error_log=true"
ensure
  ActiveSupport::Notifications.unsubscribe(retry_subscription)
  ActiveSupport::Notifications.unsubscribe(stop_subscription)
  ActiveJob::Base.logger = Logger.new(nil)
end

# Model an adapter that wraps a queue database error in its own StandardError.
# This checks propagation and Ruby's cause chain, not Solid Queue persistence.
class QueueWriteFailed < StandardError; end
class AdapterEnqueueFailed < StandardError; end

class EscapingRetryAdapter < ActiveJob::QueueAdapters::TestAdapter
  def enqueue_at(*)
    raise QueueWriteFailed, "simulated queue database failure"
  rescue QueueWriteFailed
    raise AdapterEnqueueFailed, "simulated adapter wrapper"
  end
end

ActiveJob::Base.queue_adapter = EscapingRetryAdapter.new
RetryProbeJob.perform_later(true)
escaped = nil
begin
  ActiveJob::Base.execute(next_payload)
rescue AdapterEnqueueFailed => error
  escaped = error
end
check(escaped, "adapter error did not escape")
check(escaped.cause.is_a?(QueueWriteFailed), "queue failure cause missing")
check(escaped.cause.cause.is_a?(ReceiptWriteFailed), "original job failure cause missing")
check(ActiveJob::Base.queue_adapter.enqueued_jobs.empty?, "escaping failure stored a retry")
puts "adapter_failure: escaped=AdapterEnqueueFailed cause=QueueWriteFailed original_cause=ReceiptWriteFailed retry_enqueued=false"
