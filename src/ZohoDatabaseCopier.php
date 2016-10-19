<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;
use Wabel\Zoho\CRM\Request\Response;

/**
 * This class is in charge of synchronizing one table of your database with Zoho records.
 */
class ZohoDatabaseCopier
{
    /**
     * @var Connection
     */
    private $connection;

    private $prefix;

    /**
     * @var ZohoChangeListener[]
     */
    private $listeners;

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
     * @param Connection           $connection
     * @param string               $prefix     Prefix for the table name in DB
     * @param ZohoChangeListener[] $listeners  The list of listeners called when a record is inserted or updated.
     */
    public function __construct(Connection $connection, $prefix = 'zoho_', array $listeners = [], LoggerInterface $logger = null)
    {
        $this->connection = $connection;
        $this->prefix = $prefix;
        $this->listeners = $listeners;
        if ($logger === null) {
            $this->logger = new NullLogger();
        } else {
            $this->logger = $logger;
        }
        $this->localChangesTracker = new LocalChangesTracker($connection, $this->logger);
    }

    /**
     * @param Response $userResponse
     * 
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Schema\SchemaException
     * @throws \Wabel\Zoho\CRM\Exception\ZohoCRMResponseException
     */
    public function fetchUserFromZoho(Response $userResponse)
    {
        $tableName = 'users';
        $this->logger->notice("Copying FULL data users for '$tableName'");
        $records = $userResponse->getRecords();
        $this->logger->info('Fetched '.count($records).' records');

        $table = $this->connection->getSchemaManager()->createSchema()->getTable($tableName);
        
        $select = $this->connection->prepare('SELECT * FROM '.$tableName.' WHERE id = :id');

        $this->connection->beginTransaction();
        foreach ($records as $record) {
            $data = [];
            $types = [];
            foreach ($table->getColumns() as $column) {
                if (in_array($column->getName(), ['id'])) {
                    continue;
                } else {
                    $data[$column->getName()] = $record[$column->getName()];
                    $types[$column->getName()] = $column->getType()->getName();
                }
            }

            $select->execute(['id' => $record['id']]);
            $result = $select->fetch(\PDO::FETCH_ASSOC);
            if ($result === false) {
                $this->logger->debug("Inserting record with ID '".$record['id']."'.");

                $data['id'] = $record['id'];
                $types['id'] = 'string';

                $this->connection->insert($tableName, $data, $types);

            } else {
                $this->logger->debug("Updating record with ID '".$record['id']."'.");
                $identifier = ['id' => $record['id']];
                $types['id'] = 'string';

                $this->connection->update($tableName, $data, $identifier, $types);
            }
        }
        $this->connection->commit();
    }
    
    /**
     * @param AbstractZohoDao $dao
     * @param bool            $incrementalSync Whether we synchronize only the modified files or everything.
     * @param bool            $twoWaysSync
     *
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Schema\SchemaException
     * @throws \Wabel\Zoho\CRM\Exception\ZohoCRMResponseException
     */
    public function fetchFromZoho(AbstractZohoDao $dao, $incrementalSync = true, $twoWaysSync = true)
    {
        $tableName = ZohoDatabaseHelper::getTableName($dao, $this->prefix);

        if ($incrementalSync) {
            $this->logger->info("Copying incremental data for '$tableName'");
            /////'SHOW COLUMNS FROM '.$tableName.' LIKE `lastActivityTime`');
            // Let's get the last modification date:
            $lastActivityTime = $this->connection->fetchColumn('SELECT MAX(lastActivityTime) FROM '.$tableName);
            if ($lastActivityTime !== null) {
                $lastActivityTime = new \DateTime($lastActivityTime);
                $this->logger->info('Last activity time: '.$lastActivityTime->format('c'));
                // Let's add one second to the last activity time (otherwise, we are fetching again the last record in DB).
                $lastActivityTime->add(new \DateInterval('PT1S'));
            }

            $records = $dao->getRecords(null, null, $lastActivityTime);
            $deletedRecordIds = $dao->getDeletedRecordIds($lastActivityTime);
        } else {
            $this->logger->notice("Copying FULL data for '$tableName'");
            $records = $dao->getRecords();
            $deletedRecordIds = [];
        }
        $this->logger->info('Fetched '.count($records).' records');

        $table = $this->connection->getSchemaManager()->createSchema()->getTable($tableName);

        $flatFields = ZohoDatabaseHelper::getFlatFields($dao->getFields());
        $fieldsByName = [];
        foreach ($flatFields as $field) {
            $fieldsByName[$field['name']] = $field;
        }

        $select = $this->connection->prepare('SELECT * FROM '.$tableName.' WHERE id = :id');

        $this->connection->beginTransaction();

        foreach ($records as $record) {
            $data = [];
            $types = [];
            foreach ($table->getColumns() as $column) {
                if (in_array($column->getName(), ['id', 'uid'])) {
                    continue;
                } else {
                    $field = $fieldsByName[$column->getName()];
                    $getterName = $field['getter'];
                    $data[$column->getName()] = $record->$getterName();
                    $types[$column->getName()] = $column->getType()->getName();
                }
            }

            $select->execute(['id' => $record->getZohoId()]);
            $result = $select->fetch(\PDO::FETCH_ASSOC);
            if ($result === false) {
                $this->logger->debug("Inserting record with ID '".$record->getZohoId()."'.");

                $data['id'] = $record->getZohoId();
                $types['id'] = 'string';

                $this->connection->insert($tableName, $data, $types);

                foreach ($this->listeners as $listener) {
                    $listener->onInsert($data, $dao);
                }
            } else {
                $this->logger->debug("Updating record with ID '".$record->getZohoId()."'.");
                $identifier = ['id' => $record->getZohoId()];
                $types['id'] = 'string';

                $this->connection->update($tableName, $data, $identifier, $types);

                // Let's add the id for the update trigger
                $data['id'] = $record->getZohoId();
                foreach ($this->listeners as $listener) {
                    $listener->onUpdate($data, $result, $dao);
                }
            }
        }
        $sqlStatementUid = 'select uid from '.$this->connection->quoteIdentifier($tableName).' where id = :id';
        foreach ($deletedRecordIds as $id) {
            $uid = $this->connection->fetchColumn($sqlStatementUid, ['id' => $id]);
            $this->connection->delete($tableName, ['id' => $id]);
            if ($twoWaysSync) {
                // TODO: we could detect if there are changes to be updated to the server and try to warn with a log message
                // Also, let's remove the newly created field (because of the trigger) to avoid looping back to Zoho
                $this->connection->delete('local_delete', ['table_name' => $tableName, 'id' => $id]);
                $this->connection->delete('local_update', ['table_name' => $tableName, 'uid' => $uid]);
            }
        }

        $this->connection->commit();
    }
}
