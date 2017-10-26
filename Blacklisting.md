# Blacklisted Objects Explanation

Some objects or fields may not be queryable for a variety of reasons, and these reasons are not always apparent. This document exists to provide some visibility to the research that prompted these fields and objects to be excluded from the tap's discovery or sync mode.

[Overall](#overall)

[Bulk API](#bulk-api)

[REST API](#rest-api)

Each section has two parts.
1. A short explanation of what the category means.
2. Example(s) of messages returned by Salesforce when these situations are encountered.

## Overall

### Blacklisted Fields

#### attributes
This field is returned in JSON responses from Salesforce, but is not included in the `describe` endpoint's view of objects and their fields, so it will be removed from `RECORD`s before emitting.

## Bulk API

### Unsupported Fields

This refers to fields that are unsupported by the Bulk API for any reason, such as not being CSV serializable by the API end point (which is required to process records in a streaming manner).

#### Types of Salesforce Errors associated with this category:

```
FeatureNotEnabled : Cannot serialize value for '________' in CSV format
```

### Unsupported Objects

These objects are explicitly not supported by the Salesforce Bulk API, as reported by the API itself.

#### Types of Salesforce Errors associated with this category:

```
Entity '________' is not supported by the Bulk API.
```

### Query Restricted Objects

These are objects which the Salesforce Bulk API endpoint has reported a specific method of querying that requires a special `WHERE` clause, which may be incompatible for bulk replication.

#### Types of Salesforce Errors associated with this category:

```
MALFORMED_QUERY: ________: a filter on a reified column is required [UserId,DurableId]
```

```
MALFORMED_QUERY: Implementation restriction: ________ requires a filter by a single Id, ChildRecordId or ParentContentFolderId using the equals operator
```

```
EXTERNAL_OBJECT_UNSUPPORTED_EXCEPTION: Where clauses should contain ________
```

### Query Incompatible Objects

These are objects which the Salesforce Bulk API endpoint has reported issues with the `queryAll` endpoint, or the concept of *query* in general.

#### Types of Salesforce Errors associated with this category:

```
INVALID_TYPE_FOR_OPERATION: entity type ________ does not support query
```

```
EXTERNAL_OBJECT_UNSUPPORTED_EXCEPTION: This query is not supported on the OutgoingEmail object. (OutgoingEmailRelation)
```

```
EXTERNAL_OBJECT_UNSUPPORTED_EXCEPTION: Getting all ________ is unsupported
```

## Tags Referencing Custom Settings ##

During testing, it was discovered that `\_\_Tag` objects associated with Custom Settings objects are reported as being not supported by the Bulk API. Because of this, affected `\_\_Tag` objects will be removed from those found in discovery mode before emitting `SCHEMA` records.

In practice, this refers to objects that are described by Salesforce with an `Item` relationship field that has a `referenceTo` property for another object that is marked as `customSetting: true`.

#### Types of Salesforce Errors associated with this category:

```
Entity '01AA00000010AAA.Tag' is not supported by the Bulk API.
```
* When querying a `__Tag` field.

## REST API

**TBD** Unsupported objects remain to be identified during testing.



---

Copyright &copy; 2017 Stitch
