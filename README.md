# RailsRevelry Experiments

Runnable reproductions for [RailsRevelry](https://railsrevelry.substack.com) articles.

## Article 24: Your Transaction Committed. Did the Job Reach the Queue?

Requires Ruby 3.4.7. Dependencies are pinned and installed by Bundler on the first run.

```bash
ruby chapter-04/article-24/reproduction.rb separate
ruby chapter-04/article-24/reproduction.rb same
```

The `separate` run demonstrates deferred enqueueing, both sides of a rollback,
and a Solid Queue insert failure after the application transaction commits.
The `same` run shows an immediate queue insert sharing the application transaction.
