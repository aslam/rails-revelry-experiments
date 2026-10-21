# RailsRevelry Experiments

Runnable reproductions for [RailsRevelry](https://railsrevelry.substack.com) articles.

## Your Transaction Committed. Did the Job Reach the Queue?

[Read the article](https://railsrevelry.substack.com/p/transaction-committed-job-reach-queue).

Requires Ruby 3.4.7. Dependencies are pinned and installed by Bundler on the first run.

```bash
ruby chapter-04/article-24/reproduction.rb separate
ruby chapter-04/article-24/reproduction.rb same
```

The `separate` run demonstrates deferred enqueueing, both sides of a rollback,
and a Solid Queue insert failure after the application transaction commits.
The `same` run shows an immediate queue insert sharing the application transaction.

## The Job Was Enqueued by Older Code

[Read the article](https://railsrevelry.substack.com/p/job-enqueued-by-older-code):
renaming a Rails job without breaking queued work.

Tested with Ruby 3.4.7, Rails 8.1.3, Solid Queue 1.4.0, and SQLite gem 2.9.5.
Install the dependencies once, then run from the repository root:

```bash
gem install rails:8.1.3 solid_queue:1.4.0 sqlite3:2.9.5
ruby chapter-04/article-25/reproduction.rb
```

Each producer and consumer boots in a separate Ruby process. The checks cover
an old payload after a class rename, a compatibility subclass, required versus
optional arguments, and a newer payload reaching older code. A separate Solid
Queue check verifies that a missing job class produces a retained failed
execution without reaching the job's exception handler.

The script uses temporary files and a temporary SQLite database, cleans them up
after the run, and exits unsuccessfully if an assertion fails.

## What `retry_on` Actually Means

Article publication pending. Tested with Ruby 3.4.7 and Active Job 8.1.3.
Install the dependency once, then run from the repository root:

```bash
gem install activejob:8.1.3
ruby chapter-04/article-26/reproduction.rb
```

Five checks cover repeated side effects, retry exhaustion, a terminal block,
a handled `ActiveJob::EnqueueError`, and an escaping adapter error with its
cause chain. The script also checks serialized metadata and retry log output.

It uses the test adapter and simulated effects: no network requests, database,
worker, or real scheduling delay. It does not test Solid Queue persistence.
The script exits unsuccessfully if an assertion fails.


## Why a Background Job Can Run More Than Once

[Read the article](https://railsrevelry.substack.com/p/why-a-background-job-can-run-more-than-once).

Tested with Ruby 3.4.7, Rails 8.1.3, Solid Queue 1.4.0, SQLite gem 2.9.5,
and SQLite 3.53.2. Requires a Unix-like system (macOS or Linux) with `SIGKILL`.
Install the pinned dependencies once, then run from the repository root:

```bash
gem install rails:8.1.3 solid_queue:1.4.0 sqlite3:2.9.5 digest:3.1.0 json:2.19.9
ruby chapter-04/article-27/reproduction.rb
```

The digest and JSON pins preserve the tested dependency combination; see the
comments in the script for the activation and compatibility issues they avoid.

The script commits a receiver effect in a child process, kills that child before
it saves the application receipt, explicitly prunes its Solid Queue claim, and
manually retries the failed execution. It compares an unprotected operation with
one protected by a stable operation key and a unique database index. A final
check enqueues a new job with a different job ID and recovers the original receipt.

Expected output after the version banner:

```text
normal: effects=1 receipt=true finished=true
unprotected after prune: effects=1 receipt=false failed=true ready=false
unprotected after manual retry: effects=2 receipt=true finished=true
protected after prune: effects=1 receipt=false failed=true ready=false
protected after manual retry: effects=1 receipt=true finished=true
protected new job: different_job_id=true effects=1 same_receipt=true
```

Application, queue, and receiver data live in three temporary SQLite databases,
removed when the parent exits normally or through an assertion failure. Only the
child processes created by this script are killed. An assertion failure exits
unsuccessfully.

The receiver effect is a database row: no HTTP requests are sent. This tests
sequential replay after process death, not concurrent workers, supervisor
shutdown, deploy timing, or automatic retry. The article's deploy path is traced
from tagged source. The receiver model does not implement payload-mismatch
rejection or key expiry and is not a production endpoint.
