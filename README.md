# Prometheus remote storage adapter for Timescale

Since promscale has been deprecated and I didn't need any of its performance optimisations anyway, I decided to fork the old pre-promscale `prometheus-postgresql-adapter` to feed data from my Homelab's Prometheus instance into TimescaleDB.

Roughly speaking, these are the hacks I applied:

 - Move the postgres implementation to pgx, bump dependencies.
 - Drop support for reading (ripped out the promql compatibility layer) and only support storing data in normalised form.
 - Rewrite the SQL queries to no longer depend on the old `pg_prometheus` extension for Postgres.
 - Support reading the password from a file on every new connection attempt (to facilitate connecting to the DB using short-lived access tokens)
 - Tweaked the container image description to be more amenable to crossbuilds (my k8s cluster is multi-architecture).

This is strictly a lab hack. It performs well enough for my needs, but it's probably a terrible idea to put this in production as-is.
