<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;

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
     * ZohoDatabaseCopier constructor.
     *
     * @param Connection $connection
     * @param string     $prefix     Prefix for the table name in DB
     */
    public function __construct(Connection $connection, $prefix = 'zoho_', LoggerInterface $logger = null)
    {
        $this->connection = $connection;
        $this->prefix = $prefix;
        if ($logger === null) {
            $this->logger = new NullLogger();
        } else {
            $this->logger = $logger;
        }
        $this->localChangesTracker = new LocalChangesTracker($connection, $this->logger);
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
        if ($twoWaysSync === true) {
            $this->localChangesTracker->createTrackingTables();
        }

        $tableName = ZohoDatabaseHelper::getTableName($dao, $this->prefix);
        $this->logger->info('Synchronizing DB Model for '.$tableName);

        $schema = new Schema();
        $table = $schema->createTable($tableName);

        $flatFields = ZohoDatabaseHelper::getFlatFields($dao->getFields());
        $table->addColumn('uid', 'integer', ['autoincrement' => true]);
        $table->addColumn('id', 'string', ['length' => 100]);
        $table->addUniqueIndex(['id']);
        $table->setPrimaryKey(['uid']);

        foreach ($flatFields as $field) {
            $columnName = $field['name'];

            $length = null;
            $index = false;

            // Note: full list of types available here: https://www.zoho.com/crm/help/customization/custom-fields.html
            switch ($field['type']) {
                case 'Lookup ID':
                case 'Lookup':
                    $type = 'string';
                    $length = 100;
                    $index = true;
                    break;
                case 'OwnerLookup':
                    $type = 'string';
                    $index = true;
                    $length = 25;
                    break;
                case 'Formula':
                    // Note: a Formula can return any type, but we have no way to know which type it returns...
                    $type = 'string';
                    $length = 100;
                    break;
                case 'DateTime':
                    $type = 'datetime';
                    break;
                case 'Date':
                    $type = 'date';
                    break;
                case 'DateTime':
                    $type = 'datetime';
                    break;
                case 'Boolean':
                    $type = 'boolean';
                    break;
                case 'TextArea':
                    $type = 'text';
                    break;
                case 'BigInt':
                    $type = 'bigint';
                    break;
                case 'Phone':
                case 'Auto Number':
                case 'Text':
                case 'URL':
                case 'Email':
                case 'Website':
                case 'Pick List':
                case 'Multiselect Pick List':
                    $type = 'string';
                    $length = $field['maxlength'];
                    break;
                case 'Double':
                case 'Percent':
                    $type = 'float';
                    break;
                case 'Integer':
                    $type = 'integer';
                    break;
                case 'Currency':
                case 'Decimal':
                    $type = 'decimal';
                    break;
                default:
                    throw new \RuntimeException('Unknown type "'.$field['type'].'"');
            }

            $options = [];

            if ($length) {
                $options['length'] = $length;
            }

            //$options['notnull'] = $field['req'];
            $options['notnull'] = false;

            $table->addColumn($columnName, $type, $options);

            if ($index) {
                $table->addIndex([$columnName]);
            }
        }

        $dbalTableDiffService = new DbalTableDiffService($this->connection, $this->logger);
        $hasChanges = $dbalTableDiffService->createOrUpdateTable($table);
        if ($twoWaysSync && ($hasChanges || !$skipCreateTrigger)) {
            $this->localChangesTracker->createInsertTrigger($table);
            $this->localChangesTracker->createDeleteTrigger($table);
            $this->localChangesTracker->createUpdateTrigger($table);
        }
    }
}
