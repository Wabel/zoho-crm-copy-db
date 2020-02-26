[![Latest Stable Version](https://poser.pugx.org/wabel/zoho-crm-copy-db/v/stable)](https://packagist.org/packages/wabel/zoho-crm-copy-db)
[![Latest Unstable Version](https://poser.pugx.org/wabel/zoho-crm-copy-db/v/unstable)](https://packagist.org/packages/wabel/zoho-crm-copy-db)
[![License](https://poser.pugx.org/wabel/zoho-crm-copy-db/license)](https://packagist.org/packages/wabel/zoho-crm-copy-db)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/Wabel/zoho-crm-copy-db/badges/quality-score.png?b=2.0)](https://scrutinizer-ci.com/g/Wabel/zoho-crm-copy-db/?branch=2.0)
[![Build Status](https://travis-ci.org/Wabel/zoho-crm-copy-db.svg?branch=2.0)](https://travis-ci.org/Wabel/zoho-crm-copy-db)
[![Coverage Status](https://coveralls.io/repos/Wabel/zoho-crm-copy-db/badge.svg?branch=2.0)](https://coveralls.io/r/Wabel/zoho-crm-copy-db?branch=2.0)

Wabel's Zoho-CRM Database copier
================================

What is this?
-------------

This project is a set of tools to help you copy your Zoho CRM records directly into your database.
The tool will create new tables in your database matching Zoho records. If you are looking to synchronize
data from ZohoCRM with your own tables, you should rather have a look at [ZohoCRM Sync](https://github.com/Wabel/zoho-crm-sync).
It is built on top of the [ZohoCRM ORM](https://github.com/Wabel/zoho-crm-orm).
Before reading further you should get used to working with [ZohoCRM ORM](https://github.com/Wabel/zoho-crm-orm),
so if you do not know this library, [STOP READING NOW and follow this link](https://github.com/Wabel/zoho-crm-orm).

How does it work?
-----------------
This projects provides a `ZohoDatabaseCopier` class, with a simple `fetchFromZoho` method. This method takes a `ZohoDao` in argument.
`ZohoDaos` can be created using the [ZohoCRM ORM](https://github.com/Wabel/zoho-crm-orm).
It also provides a `ZohoDatabasePusher` class with a  `pushToZoho` method to push data to Zoho CRM.

-----------------
<h4>ZohoDatabaseCopier for Module</h4>

Usage:

```php
// $connection is a Doctrine DBAL connection to your database.
$databaseCopier = new ZohoDatabaseCopier($connection);

// $contactZohoDao is the Zoho Dao to the module you want to copy.
$databaseCopier->fetchFromZoho($contactZohoDao);
```

The copy command will create a 'zoho_Contacts' table in your database and copy all data from Zoho.
Table names are prefixed by 'zoho_'.

You can change the prefix using the second (optional) argument of the constructor:

```php
// Generated database table will be prefixed with "my_prefix_"
$databaseCopier = new ZohoDatabaseCopier($connection, "my_prefix_");
```

By default, copy is performed incrementally. If you have touched some of the data in your database and want to copy again 
everything, you can use the second parameter of the `copy` method:
 
```php
// Pass false as second parameter to force copying everything rather than doing an incremental copy.
$databaseCopier->fetchFromZoho($contactZohoDao, false);
```
-----------------
<h4>ZohoDatabaseCopier for Users</h4>

With the same `$databaseCopier` you can fetch the users from zoho.
Usage:

```php
// $connection is a Doctrine DBAL connection to your database.
$databaseCopier = new ZohoDatabaseCopier($connection);


```php
// $userResponse is the Zoho Client Response from zoho-crm-orm package.
$databaseCopier->fetchUserFromZoho($userResponse);
```
-----------------
<h4>ZohoDatabasePusher</h4>

Usage:

```php
// $connection is a Doctrine DBAL connection to your database.
$databaseSync = new ZohoDatabasePusher($connection);

// $contactZohoDao is the Zoho Dao to the module you want to push.
$databaseSync->pushToZoho($contactZohoDao);
```

Requirements
------------

This project requires MySQL 5.7+ to work.

Symfony command
---------------

The project also comes with a Symfony Command that you can use to easily copy tables.

The command's constructor takes in parameter a `ZohoDatabaseCopier` instance, a`ZohoDatabasePusher` instance and a `ZohoClient`. This command
regenerates automatically the Daos in order to pass them in `ZohoDatabaseCopier` instance, a`ZohoDatabasePusher`.

Usage:

```sh
# Command to synchronize data (both ways)
$ console zoho:sync
# Command to only fetch data from Zoho
$ console zoho:sync --fetch-only
# Command to only push data to Zoho
$ console zoho:sync --push-only
```

Listeners
---------

For each `ZohoDatabaseCopier`, you can register one or many listeners. These listeners should implement the 
[`ZohoChangeListener`](blob/2.0/src/ZohoChangeListener.php) interface.

You register those listener by passing an array of listeners to the 3rd parameter of the constructor:

```php
$listener = new MyListener();
$databaseCopier = new ZohoDatabaseCopier($connection, "my_prefix_", [ $listener ]);
```

Versions
--------

### 3.2.3

_26 Feb. 2020_

Change the way update trigger is done. On update, it will check that a row doesn't already exists in `local_insert` first, to avoid wrong value in `local_update` for a new record.

When fetching update from zoho, if some data have been updated and are present in `local_update`, the data are skipped to avoid override. 
 
_20 Feb. 2020_

Add listeners when pushing data to Zoho.

_18 Feb. 2020_

Timezone is now used when getting the records with "If-Modified-Since" and using the `zoho_sync_config` table.

Records are now fetched using the sort and order columns (by priority: Modified_Time, Created_Time).

Improve processing time when fetching data by adding some cache.  
> When fetching data from Zoho, the script was checking for each record in the database if the column exists.  
> Then it would check if the DAO contains the correct setter linked for the column.  
> We added some cache on the columns and the setters fields to avoid checking the information for each records.  
> In local, processing 200 records went from ~26sec to ~1sec with this change :)

_11 Feb. 2020_

This version allow to use a configuration table to store multiple config data.  
The table looks like:

```sql
CREATE TABLE `zoho_sync_config` ( `config_key` VARCHAR(100) NOT NULL , `table_name` VARCHAR(200) NULL DEFAULT NULL , `config_value` VARCHAR(200) NOT NULL ) ENGINE = InnoDB;
```

The first usage of this table, implemented in this version allow to see/override the date that is sent to zoho when fetching records.

This table is optional and the script should work as usual if you don't have it.

The first two `config_key` are:
* `FETCH_RECORDS_MODIFIED_SINCE__DATE`: Date-modified-since, that is used when fetching data
* `FETCH_RECORDS_MODIFIED_SINCE__PAGE`: The page (offset) of the records

The script has been updated to use this new configuration. That is helpful in case of unhandled error to continue on the same page and same date as the last synchronization failed.  
And you are also free to update the value in database, to manually enter the date from when you want your data.

### 3.2.2

Fix a bug by changing the way triggers are created after a table structure has been updated.

Create an option `recreate-triggers` that will force the creation of the triggers for all tables. 

### 3.2

**Bulk**

This version adds an option `fetch-bulk` that will loop over all the modules in order to populate the data in each tables. This option will use the Bulk Read API v2.  
For each modules, the following actions will be executed:

1. Send a request to create a bulk job
2. Every 15 seconds, check the status of the job. If ready, go to step 3
3. Download the CSV results
4. Insert the results into the module's table
5. If more results are expected, goes back to step 1

Maximum 200000 records are fetched per API call. In case of error, the script pass to the next module.  
Logging is enabled.

**Algorithms and error logging**

This version also includes multiple on the structure of the tracking tables and on the way data are pushed to Zoho.  
The tracking table now have a column error, that can indicate what is the error if something went wrong when pushing it.  
The logging is still working and will display the same error.

Plus, many changes in the algorithms have been made to fix some issues:

* If a row with an UID is not found in a particular table, it doesn't go in infinite loop anymore.
* If a field_name (local_update) is not found in a particular table, a correct error message is logged.
* Changed the algorithm when updating a record, in order to send to Zoho all the fields that have been updated. (Previously the algorithm took only the fields in the first 200 records and deleted the other...)
* In case of error after sending the request to Zoho, the message is correctly logged.
* Added a log message when inserting a record but Zoho merged it internally (previously it was still done, but in silent, with no message).


Developer note: This new option has been developed quickly in order to migrate from API v1 to v2 (EOL december 2019). The code is dirty, but everything works.
