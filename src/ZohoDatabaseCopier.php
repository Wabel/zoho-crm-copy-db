<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;
use Wabel\Zoho\CRM\Request\Response;
use zcrmsdk\crm\crud\ZCRMRecord;
use zcrmsdk\crm\exception\ZCRMException;

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
     * @var ZohoUserService
     */
    private $zohoUserService;

    /**
     * ZohoDatabaseCopier constructor.
     *
     * @param Connection $connection
     * @param string $prefix Prefix for the table name in DB
     * @param ZohoChangeListener[] $listeners The list of listeners called when a record is inserted or updated.
     */
    public function __construct(Connection $connection, ZohoUserService $zohoUserService, $prefix = 'zoho_', array $listeners = [], LoggerInterface $logger = null)
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
        $this->zohoUserService = $zohoUserService;
    }

    /**
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Schema\SchemaException
     * @throws \Wabel\Zoho\CRM\Exception\ZohoCRMResponseException
     */
    public function fetchUserFromZoho()
    {
        $users = $this->zohoUserService->getUsers();
        $tableName = 'users';
        $this->logger->info('Fetched ' . count($users) . ' records');

        $table = $this->connection->getSchemaManager()->createSchema()->getTable($tableName);

        $select = $this->connection->prepare('SELECT * FROM ' . $tableName . ' WHERE id = :id');

        $this->connection->beginTransaction();
        foreach ($users as $user) {
            $data = [];
            $types = [];
            foreach ($table->getColumns() as $column) {
                if ($column->getName() === 'id') {
                    continue;
                } else {
                    $fieldMethod = ZohoDatabaseHelper::getUserMethodNameFromField($column->getName());
                    if (method_exists($user, $fieldMethod)
                        && (!is_array($user->{$fieldMethod}()) && !is_object($user->{$fieldMethod}()))
                    ) {
                        $data[$column->getName()] = $user->{$fieldMethod}();
                    } elseif (method_exists($user, $fieldMethod)
                        && is_array($user->{$fieldMethod}())
                        && array_key_exists('name', $user->{$fieldMethod}())
                        && array_key_exists('id', $user->{$fieldMethod}())
                    ) {
                        $data[$column->getName()] = $user->{$fieldMethod}()['name'];
                    } elseif (method_exists($user, $fieldMethod)
                        && is_object($user->{$fieldMethod}()) && method_exists($user->{$fieldMethod}(), 'getName')
                    ) {
                        $object = $user->{$fieldMethod}();
                        $data[$column->getName()] = $object->getName();
                    } elseif ($column->getName() === 'Currency') {
                        //Todo: Do a pull request about \ZCRMUser::geCurrency() to \ZCRMUser::getCurrency()
                        $data[$column->getName()] = $user->geCurrency();
                    } else {
                        continue;
                    }
                }
            }
            $select->execute(['id' => $user->getId()]);
            $result = $select->fetch(\PDO::FETCH_ASSOC);
            if ($result === false && $data) {
                $this->logger->debug("Inserting record with ID '" . $user->getId() . "'.");

                $data['id'] = $user->getId();
                $types['id'] = 'string';

                $this->connection->insert($tableName, $data, $types);
            } elseif ($data) {
                $this->logger->debug("Updating record with ID '" . $user->getId() . "'.");
                $identifier = ['id' => $user->getId()];
                $types['id'] = 'string';
                $this->connection->update($tableName, $data, $identifier, $types);
            }

        }
        $this->connection->commit();
    }

    /**
     * @param AbstractZohoDao $dao
     * @param bool $incrementalSync Whether we synchronize only the modified files or everything.
     * @param bool $twoWaysSync
     * @param bool $throwErrors
     *
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Schema\SchemaException
     * @throws \Wabel\Zoho\CRM\Exception\ZohoCRMResponseException
     */
    public function fetchFromZoho(AbstractZohoDao $dao, $incrementalSync = true, $twoWaysSync = true, $throwErrors = true)
    {
        $tableName = ZohoDatabaseHelper::getTableName($dao, $this->prefix);

        $totalRecords = 0;
        $totalRecordsDeleted = 0;
        try {
            if ($incrementalSync) {
                // Let's get the last modification date:
                $tableDetail = $this->connection->getSchemaManager()->listTableDetails($tableName);
                $lastActivityTime = null;
                if ($tableDetail->hasColumn('modifiedTime')) {
                    $lastActivityTime = $this->connection->fetchColumn('SELECT MAX(modifiedTime) FROM ' . $tableName);
                }
                if (!$lastActivityTime && $tableDetail->hasColumn('createdTime')) {
                    $lastActivityTime = $this->connection->fetchColumn('SELECT MAX(createdTime) FROM ' . $tableName);
                }

                if ($lastActivityTime !== null) {
                    $lastActivityTime = new \DateTime($lastActivityTime, new \DateTimeZone($dao->getZohoClient()->getTimezone()));
                    // Let's add one second to the last activity time (otherwise, we are fetching again the last record in DB).
                    $lastActivityTime->add(new \DateInterval('PT1S'));
                }

                if ($lastActivityTime) {
                    $this->logger->info('Incremental copy from ' . $lastActivityTime->format(\DateTime::ATOM) . ' started');
                } else {
                    $this->logger->info('Incremental copy started');
                }

                $this->logger->info('Fetching the records to insert/update...');
                $records = $dao->getRecords(null, null, null, $lastActivityTime);
                $totalRecords = count($records);
                $this->logger->debug($totalRecords . ' records fetched.');
                $this->logger->info('Fetching the records to delete...');
                $deletedRecords = $dao->getDeletedRecordIds($lastActivityTime);
                $totalRecordsDeleted = count($deletedRecords);
                $this->logger->debug($totalRecordsDeleted . ' records fetched.');
            } else {
                $this->logger->info('Full copy started');
                $this->logger->info('Fetching the records to insert/update...');
                $records = $dao->getRecords();
                $totalRecords = count($records);
                $this->logger->debug($totalRecords . ' records fetched.');
                $deletedRecords = [];
            }
        } catch (ZCRMException $exception) {
            $this->logger->error('Error when getting records for module ' . $tableName . ': ' . $exception->getMessage(), [
                'exception' => $exception
            ]);
            if ($throwErrors) {
                throw $exception;
            }
            return;
        }
        $this->logger->info('Inserting/updating ' . count($records) . ' records into table ' . $tableName . '...');

        $table = $this->connection->getSchemaManager()->createSchema()->getTable($tableName);

        $select = $this->connection->prepare('SELECT * FROM ' . $tableName . ' WHERE id = :id');

        $this->connection->beginTransaction();

        $recordsModificationCounts = [
            'insert' => 0,
            'update' => 0,
            'delete' => 0,
        ];

        $logOffset = $totalRecords >= 500 ? 100 : 50;
        $processedRecords = 0;
        foreach ($records as $record) {
            if (($processedRecords % $logOffset) === 0) {
                $this->logger->info($processedRecords . '/' . $totalRecords . ' records processed');
            }
            ++$processedRecords;
            $data = [];
            $types = [];
            foreach ($table->getColumns() as $column) {
                if (in_array($column->getName(), ['id', 'uid'])) {
                    continue;
                } else {
                    $field = $dao->getFieldFromFieldName($column->getName());
                    if (!$field) {
                        continue;
                    }
                    $getterName = $field->getGetter();
                    $dataValue = $record->$getterName();
                    $finalFieldData = null;
                    if ($dataValue instanceof ZCRMRecord) {
                        $finalFieldData = $dataValue->getEntityId();
                    } elseif (is_array($dataValue)) {
                        $finalFieldData = implode(';', $dataValue);
                    } else {
                        $finalFieldData = $dataValue;
                    }
                    $data[$column->getName()] = $finalFieldData;
                    $types[$column->getName()] = $column->getType()->getName();
                }
            }

            $select->execute(['id' => $record->getZohoId()]);
            $result = $select->fetch(\PDO::FETCH_ASSOC);
            if ($result === false) {
                $this->logger->debug("Inserting record with ID '" . $record->getZohoId() . "'...");

                $data['id'] = $record->getZohoId();
                $types['id'] = 'string';

                $recordsModificationCounts['insert'] += $this->connection->insert($tableName, $data, $types);

                foreach ($this->listeners as $listener) {
                    $listener->onInsert($data, $dao);
                }
            } else {
                $this->logger->debug("Updating record with ID '" . $record->getZohoId() . "'...");
                $identifier = ['id' => $record->getZohoId()];
                $types['id'] = 'string';

                $recordsModificationCounts['update'] += $this->connection->update($tableName, $data, $identifier, $types);

                // Let's add the id for the update trigger
                $data['id'] = $record->getZohoId();
                foreach ($this->listeners as $listener) {
                    $listener->onUpdate($data, $result, $dao);
                }
            }
        }

        $this->logger->info('Deleting ' . count($deletedRecords) . ' records into table ' . $tableName . '...');
        $sqlStatementUid = 'select uid from ' . $this->connection->quoteIdentifier($tableName) . ' where id = :id';
        $processedRecords = 0;
        foreach ($deletedRecords as $deletedRecord) {
            if (($processedRecords % $logOffset) === 0) {
                $this->logger->info($processedRecords . '/' . $totalRecords . ' records processed');
            }
            ++$processedRecords;
            $this->logger->debug("Deleting record with ID '" . $deletedRecord->getEntityId() . "'...");
            $uid = $this->connection->fetchColumn($sqlStatementUid, ['id' => $deletedRecord->getEntityId()]);
            $recordsModificationCounts['delete'] += $this->connection->delete($tableName, ['id' => $deletedRecord->getEntityId()]);
            if ($twoWaysSync) {
                // TODO: we could detect if there are changes to be updated to the server and try to warn with a log message
                // Also, let's remove the newly created field (because of the trigger) to avoid looping back to Zoho
                $this->connection->delete('local_delete', ['table_name' => $tableName, 'id' => $deletedRecord->getEntityId()]);
                $this->connection->delete('local_update', ['table_name' => $tableName, 'uid' => $uid]);
            }
        }

        $this->logger->notice(sprintf('Copy finished with %d item(s) inserted, %d item(s) updated and %d item(s) deleted.',
            $recordsModificationCounts['insert'],
            $recordsModificationCounts['update'],
            $recordsModificationCounts['delete']
        ));

        $this->connection->commit();
    }
}
