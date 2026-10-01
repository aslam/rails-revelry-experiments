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
