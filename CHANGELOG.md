# Changelog

## 1.3.3
  * Adds additional logging and state management during a PK Chunked Bulk Sync

## 1.3.2
  * Fixes a bad variable name

## 1.3.1
  * Uses the correct datetime to string function for chunked bookmarks

## 1.3.0
  * Adds a feature for resuming a PK-Chunked Bulk API job [#22](https://github.com/singer-io/tap-salesforce/pull/22)
  * Fixes an issue where a Salesforce's field data containing NULL bytes would cause an error reading the CSV response [#21](https://github.com/singer-io/tap-salesforce/pull/21)
  * Fixes an issue where the timed `login()` thread could die and never call a new login [#20](https://github.com/singer-io/tap-salesforce/pull/20)

## 1.2.2
  * Fixes a bug with with yield records when the Bulk job is successful [#19](https://github.com/singer-io/tap-salesforce/pull/19)

## 1.2.1
  * Fixes a bug with a missing pk_chunking attribute

## 1.2.0
  * Adds support for Bulk API jobs which time out to be retried with Salesforce's PK Chunking feature enabled

## 1.1.1
  * Allows compound fields to be supported with the exception of "address" types
  * Adds additional unsupported Bulk API Objects

## 1.1.0
  * Support for time_extracted property on Singer messages

## 1.0.0
  * Initial release
