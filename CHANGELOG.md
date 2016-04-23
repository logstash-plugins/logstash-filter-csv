# 2.1.3
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash
# 2.1.2
  - New dependency requirements for logstash-core for the 5.0 release
## 2.1.1
 - Fixed field references, refactors converters, code cleanups

## 2.1.0
 - Added support for not parsing columns without a defined header.
 - Added support for dropping columns that has no value
 - Added support for type conversion within the filter
 - Fix unnecessary source field mutation. Fixes #18
 - Refactored specs to avoid using sample and insist in favor of rspec3
   helper methods.

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully,
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

