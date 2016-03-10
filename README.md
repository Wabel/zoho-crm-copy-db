[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/Wabel/zoho-crm-copy-db/badges/quality-score.png?b=1.0)](https://scrutinizer-ci.com/g/Wabel/zoho-crm-copy-db/?branch=1.0)
[![Build Status](https://travis-ci.org/Wabel/zoho-crm-copy-db.svg?branch=1.0)](https://travis-ci.org/Wabel/zoho-crm-copy-db)
[![Coverage Status](https://coveralls.io/repos/Wabel/zoho-crm-copy-db/badge.svg?branch=1.0)](https://coveralls.io/r/Wabel/zoho-crm-copy-db?branch=1.0)

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

This projects provides a `ZohoDatabaseCopier` class, with a simple `copy` method. This method takes a `ZohoDao` in argument.
`ZohoDaos` can be created using the [ZohoCRM ORM](https://github.com/Wabel/zoho-crm-orm).

Usage:

```php
// $connection is a Doctrine DBAL connection to your database.
$databaseCopier = new ZohoDatabaseCopier($connection);

// $contactZohoDao is the Zoho Dao to the module you want to copy.
$databaseCopier->copy($contactZohoDao);
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
$databaseCopier->copy($contactZohoDao, false);
```


Symfony command
---------------

The project also comes with a Symfony Command that you can use to easily copy tables.

The command's constructor takes in parameter a `ZohoDatabaseCopier` instance and a list of `ZohoDAOs`.

Usage:

```sh
$ console zoho:copy-db
```
