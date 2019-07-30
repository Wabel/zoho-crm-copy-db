<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;
use Wabel\Zoho\CRM\Request\Response;
use zcrmsdk\crm\setup\users\ZCRMUser;

/**
 * This class is in charge of synchronizing one table MODEL with Zoho.
 */
class ZohoDatabaseModelSync
{
    /**
     * @var Connection
     */
    private $connection;

    private $prefix;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var LocalChangesTracker
     */
    private $localChangesTracker;
    /**
     * @var ZohoUserService
     */
    private $zohoUserService;

    /**
     * @var bool $trackingTablesDone
     */
    private $trackingTablesDone;

    /**
     * ZohoDatabaseCopier constructor.
     *
     * @param Connection $connection
     * @param string     $prefix     Prefix for the table name in DB
     */
    public function __construct(Connection $connection, ZohoUserService $zohoUserService, $prefix = 'zoho_', LoggerInterface $logger = null)
    {
        $this->connection = $connection;
        $this->prefix = $prefix;
        if ($logger === null) {
            $this->logger = new NullLogger();
        } else {
            $this->logger = $logger;
        }
        $this->localChangesTracker = new LocalChangesTracker($connection, $this->logger);
        $this->zohoUserService = $zohoUserService;
        $this->trackingTablesDone = false;
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * Synchronizes the DB model with Zoho.
     *
     * @param AbstractZohoDao $dao
     * @param bool            $twoWaysSync
     * @param bool            $skipCreateTrigger
     *
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Schema\SchemaException
     */
    public function synchronizeDbModel(AbstractZohoDao $dao, $twoWaysSync, $skipCreateTrigger = false)
    {
        if ($twoWaysSync === true && !$this->trackingTablesDone) {
            $this->localChangesTracker->createTrackingTables();
            $this->trackingTablesDone = true;
        }

        $tableName = ZohoDatabaseHelper::getTableName($dao, $this->prefix);
        $this->logger->notice('Synchronizing DB Model for '.$tableName.'...');

        $schema = new Schema();
        $table = $schema->createTable($tableName);

        //@Temporary fix to use Mysql5.7 not strict
        $table->addColumn('uid', 'string', ['length' => 36,'notnull'=>false]);
        $table->addColumn('id', 'string', ['length' => 100,'notnull'=>false]);
        $table->addUniqueIndex(['id']);
        $table->setPrimaryKey(['uid']);

        foreach ($dao->getFields() as $field) {
            $columnName = $field->getName();
            //It seems sometime we can have the same field twice in the list of fields from the API.
            if($table->hasColumn($columnName)) {
                continue;
            }

            $length = null;
            $index = false;
            $options = [];
            // Note: full list of types available here: https://www.zoho.com/crm/help/customization/custom-fields.html
            switch ($field->getType()) {
            case 'fileupload':
                $type = 'string';
                $length = $field->getMaxlength() && $field->getMaxlength() > 0?$field->getMaxlength() : 255;
                break;
            case 'lookup':
                $type = 'string';
                $length = $field->getMaxlength() && $field->getMaxlength() > 0?$field->getMaxlength() : 100;
                $index = true;
                break;
            case 'userlookup':
            case 'ownerlookup':
                $type = 'string';
                $index = true;
                $length = $field->getMaxlength() && $field->getMaxlength() > 0?$field->getMaxlength() : 25;
                break;
            case 'formula':
                // Note: a Formula can return any type, but we have no way to know which type it returns...
                $type = 'string';
                $length = $field->getMaxlength() && $field->getMaxlength() > 0?$field->getMaxlength() : 100;
                break;
            case 'datetime':
                $type = 'datetime';
                break;
            case 'date':
                $type = 'date';
                break;
            case 'boolean':
                $type = 'boolean';
                break;
            case 'textarea':
                $type = 'text';
                break;
            case 'bigint':
                $type = 'bigint';
                break;
            case 'phone':
            case 'text':
            case 'url':
            case 'email':
            case 'picklist':
            case 'website':
                $type = 'string';
                $length = $field->getMaxlength() && $field->getMaxlength() > 0?$field->getMaxlength() : 255;
                break;
            case 'multiselectlookup':
            case 'multiuserlookup':
            case 'multiselectpicklist':
                $type = 'text';
                break;
            case 'percent':
                $type = 'integer';
                break;
            case 'double':
                $type = 'float';
                break;
            case 'autonumber':
            case 'integer':
                $type = 'integer';
                $length = $field->getMaxlength() && $field->getMaxlength() > 0?$field->getMaxlength() : 255;
                break;
            case 'currency':
            case 'decimal':
                $type = 'decimal';
                $options['scale'] = 2;
                break;
            case 'consent_lookup':
            case 'profileimage':
            case 'ALARM':
            case 'RRULE':
            case 'event_reminder':
                continue 2;
                    break;
            default:
                throw new \RuntimeException('Unknown type "'.$field->getType().'"');
            }

            if ($length) {
                $options['length'] = $length;
            }

            $options['notnull'] = false;

            $table->addColumn($columnName, $type, $options);

            if ($index) {
                $table->addIndex([$columnName]);
            }
        }

        $dbalTableDiffService = new DbalTableDiffService($this->connection, $this->logger);
        $hasChanges = $dbalTableDiffService->createOrUpdateTable($table);
        if ($hasChanges || !$skipCreateTrigger) {
            $this->localChangesTracker->createUuidInsertTrigger($table);
            if ($twoWaysSync) {
                $this->localChangesTracker->createInsertTrigger($table);
                $this->localChangesTracker->createDeleteTrigger($table);
                $this->localChangesTracker->createUpdateTrigger($table);
            }
        }

    }

    public function synchronizeUserDbModel()
    {
        $tableName = 'users';
        $schema = new Schema();
        $table = $schema->createTable($tableName);

        $table->addColumn('id', 'string', ['length' => 100,'notnull'=>false]);
        $table->setPrimaryKey(['id']);
        foreach (ZCRMUser::$defaultKeys as $field) {
            if ($field === 'id') {
                continue;
            }
            $columnName = $field;
            $length = null;
            $index = false;
            switch ($field) {
            case 'zuid':
                $type = 'string';
                $length = 100;
                $index = true;
                break;
            case 'name':
            case 'email':
                $type = 'string';
                $length = 255;
                $index = true;
                break;
            case 'phone':
            case 'website':
                $type = 'text';
                break;
            default:
                $type = 'string';
                $length = 100;
            }

            $options = [];

            if ($length) {
                $options['length'] = $length;
            }

            $options['notnull'] = false;

            $table->addColumn($columnName, $type, $options);

            if ($index) {
                $table->addIndex([$columnName]);
            }
        }

        $dbalTableDiffService = new DbalTableDiffService($this->connection, $this->logger);
        $dbalTableDiffService->createOrUpdateTable($table);
    }
}
