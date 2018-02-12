# Changelog

## 1.4.8
  * Bumps singer-python dependency to help with formatting dates < 1000

## 1.4.7
  * Fixes a bug with datetime conversion during the generation of the SF query string [#40](https://github.com/singer-io/tap-salesforce/pull/40)

## 1.4.6
  * Fixes more bugs with exception handling where the REST API was not capturing the correct error [#39](https://github.com/singer-io/tap-salesforce/pull/39)

## 1.4.5
  * Fixes a schema issue with 'location' fields that come back as JSON objects [#36](https://github.com/singer-io/tap-salesforce/pull/36)
  * Fixes a bug where a `"version"` in the state would not be preserved due to truthiness [#37](https://github.com/singer-io/tap-salesforce/pull/37)
  * Fixes a bug in exception handling where rendering an exception as a string would cause an additional exception [#38](https://github.com/singer-io/tap-salesforce/pull/38)

## 1.4.4
  * Fixes automatic property selection when select-fields-by-default is true [#35](https://github.com/singer-io/tap-salesforce/pull/35)

## 1.4.3
  * Adds the `AttachedContentNote` and `QuoteTemplateRichTextData` objects to the list of query-incompatible Salesforce objects so they are excluded from discovery / catalogs [#34](https://github.com/singer-io/tap-salesforce/pull/34)

## 1.4.2
  * Adds backoff for the `_make_request` function to prevent failures in certain cases [#33](https://github.com/singer-io/tap-salesforce/pull/33)

## 1.4.1
  * Adds detection for certain SF Objects whose parents can be used as the parent during PK Chunking [#32](https://github.com/singer-io/tap-salesforce/pull/32)

## 1.4.0
  * Fixes a logic bug in the build_state function
  * Improves upon streaming bulk results by first writing the file to a tempfile and then consuming it [#31](https://github.com/singer-io/tap-salesforce/pull/31)

## 1.3.9
  * Updates the retrieval of a bulk result set to be downloaded entirely instead of streaming [#30](https://github.com/singer-io/tap-salesforce/pull/30)

## 1.3.8
  * Removes `multipleOf` JSON Schema parameters for latitude / longitude fields that are part of an Address object

## 1.3.7
  * Adds a check to make sure the start_date has time information associated with it
  * Adds more robust parsing for select_fields_by_default

## 1.3.6
  * Fixes a bug with running the tap when provided a Catalog containing streams without a replication key [#27](https://github.com/singer-io/tap-salesforce/pull/27)

## 1.3.5
  * Bumps the dependency singer-python's version to 5.0.4

## 1.3.4
  * Fixes a bug where bookmark state would not get set after resuming a PK Chunked Bulk Sync [#24](https://github.com/singer-io/tap-salesforce/pull/24)

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
