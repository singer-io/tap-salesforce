# Changelog

## 1.4.25
 * Mark `Announcement` as query-incompatible [#64](https://github.com/singer-io/tap-salesforce/pull/64)

## 1.4.24
 * Mark json fields as `unsupported` instead of throwing exception.  If, in the future, we find streams with json fields that have records, we can consider supporting the json field type. [commit](https://github.com/singer-io/tap-salesforce/commit/85e3811b9cb5673e23cab8e7b011d2a3d3064d0f)

## 1.4.23
  * Protect against empty strings for quota config fields [commit](https://github.com/singer-io/tap-salesforce/commit/1133726e20af434d82af8761ba3ad006f49f0b42)

## 1.4.22
  * Filter out *ChangeEvent tables from discovery as neither REST nor BULK can sync them [#62](https://github.com/singer-io/tap-salesforce/pull/62)

## 1.4.21
  * Move the transformer outside of the record write-loop to quiet logging [#61](https://github.com/singer-io/tap-salesforce/pull/61)

## 1.4.20
  * (Bulk Rest API) Sync the second half of the date range After a timeout occurs and the date window is halved [#60](https://github.com/singer-io/tap-salesforce/pull/60)

## 1.4.19
  * (Bulk API) Removes failed jobs that don't exists in Salesforce from state when encountered [#57](https://github.com/singer-io/tap-salesforce/pull/57)
  * (All APIs) Makes `BackgroundOperationResult` sync full table, since it cannot be sorted by `CreatedDate` [#58](https://github.com/singer-io/tap-salesforce/pull/58)
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074 [#59](https://github.com/singer-io/tap-salesforce/pull/59)

## 1.4.18
  * Increases the `field_size_limit` on the CSV reader to enable larger fields coming through without error [#53](https://github.com/singer-io/tap-salesforce/pull/53)

## 1.4.17
  * Adds the suffix "FieldHistory" to those checked for when finding the parent object to fix the `OpportunityFieldHistory` stream [#52](https://github.com/singer-io/tap-salesforce/pull/52)

## 1.4.16
  * Fixes a few bugs with PK chunking including allowing a custom table to be chunked by its parent table [#51](https://github.com/singer-io/tap-salesforce/pull/51)

## 1.4.15
  * Added a correct else condition to fix an error being raised during the PK Chunking query [#50](https://github.com/singer-io/tap-salesforce/pull/50)

## 1.4.14
  * Updated the usage of singer-python's Transformer to reduce its scope [#48](https://github.com/singer-io/tap-salesforce/pull/48)

## 1.4.13
  * Updated the JSON schema generated for Salesforce Date types to use `anyOf` so when a bad date comes through we use the String instead [#47](https://github.com/singer-io/tap-salesforce/pull/47)

## 1.4.12
  * Bug fix for metadata when resuming bulk sync jobs.

## 1.4.11
  * Moved ContentFolderItem to query restricted objects list since the REST API requires specific IDs to query this object.

## 1.4.10
  * Read replication-method, replication-key from metadata instead of Catalog.  Publish key-properties as table-key-properties metadata instead of including on the Catalog.

## 1.4.9
  * Fixes logging output when an HTTP error occurs

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
