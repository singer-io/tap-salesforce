# Changelog

## 1.2.2
  * Fixes a bug with with yield records when the Bulk job is successful (#19)[https://github.com/singer-io/tap-salesforce/pull/19]

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
