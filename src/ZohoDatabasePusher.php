<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;
use Wabel\Zoho\CRM\Service\EntitiesGeneratorService;
use Wabel\Zoho\CRM\ZohoBeanInterface;
use zcrmsdk\crm\api\response\EntityResponse;

/**
 * Description of ZohoDatabasePusher.
 *
 * @author rbergina
 */
class ZohoDatabasePusher
{
    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var string
     */
    private $prefix;

    /**
     * @var ZohoChangeListener[]
     */
    private $listeners;

    /**
     * @param Connection $connection
     * @param int $apiLimitInsertUpdateDelete
     * @param string $prefix
     * @param LoggerInterface $logger
     * @param ZohoChangeListener[] $listeners
     */
    public function __construct(Connection $connection, $apiLimitInsertUpdateDelete = 100, $prefix = 'zoho_', LoggerInterface $logger = null, array $listeners = [])
    {
        $this->connection = $connection;
        $this->prefix = $prefix;
        if ($logger === null) {
            $this->logger = new NullLogger();
        } else {
            $this->logger = $logger;
        }
        $this->apiLimitInsertUpdateDelete = $apiLimitInsertUpdateDelete;
        if ($apiLimitInsertUpdateDelete === null) {
            $this->apiLimitInsertUpdateDelete = 100;
        }
        $this->listeners = $listeners;
    }

    /**
     * @var int
     */
    private $apiLimitInsertUpdateDelete;

    /**
     * @param AbstractZohoDao $zohoDao
     * @param bool $update
     * @return int
     */
    private function countElementInTable(AbstractZohoDao $zohoDao, $update = false)
    {
        $tableName = ZohoDatabaseHelper::getTableName($zohoDao, $this->prefix);
        if ($update) {
            return (int)$this->connection->executeQuery('SELECT COUNT(DISTINCT uid) AS nb FROM `local_update` WHERE table_name LIKE :tableName AND error IS NULL', ['tableName' => $tableName])->fetchColumn();
        }
        return (int)$this->connection->executeQuery('SELECT COUNT(uid) AS nb FROM `local_insert` WHERE table_name LIKE :tableName AND error IS NULL', ['tableName' => $tableName])->fetchColumn();
    }

    /**
     * Insert or Update rows.
     *
     * @param AbstractZohoDao $zohoDao
     */
    public function pushDataToZoho(AbstractZohoDao $zohoDao, $update = false)
    {
        $localTable = $update ? 'local_update' : 'local_insert';
        $tableName = ZohoDatabaseHelper::getTableName($zohoDao, $this->prefix);
        $countToPush = $this->countElementInTable($zohoDao, $update);
        $this->logger->notice($countToPush . ' records to ' . ($update ? 'update' : 'insert') . ' into Zoho for module ' . $zohoDao->getPluralModuleName());
        if ($countToPush) {
            do {
                $rowsDeleted = [];
                $zohoBeans = [];
                $localRecords = [];
                //@see https://www.zoho.com/crm/help/api/v2/#ra-update-records
                //To optimize your API usage, get maximum 200 records with each request and insert, update or delete maximum 100 records with each request.

                if ($update) {
                    $recordsToUpdateQuery = $this->connection->createQueryBuilder();
                    $recordsToUpdateQuery
                        ->select('DISTINCT table_name, uid')
                        ->from($localTable)
                        ->where('error IS NULL')
                        ->andWhere('table_name = :table_name')
                        ->setMaxResults($this->apiLimitInsertUpdateDelete)
                        ->setParameters([
                            'table_name' => $tableName
                        ]);
                    $recordsToUpdateResults = $recordsToUpdateQuery->execute()->fetchAll();
                    $this->logger->info(sprintf('Processing %s records to update...', count($recordsToUpdateResults)));
                    foreach ($recordsToUpdateResults as $result) {
                        $recordQuery = $this->connection->createQueryBuilder();
                        $recordQuery
                            ->select('*')
                            ->from($tableName)
                            ->where('uid = :uid')
                            ->setParameters([
                                'uid' => $result['uid']
                            ]);
                        $record = $recordQuery->execute()->fetch();

                        if (!$record) {
                            $errorMessage = sprintf('Impossible to find row with uid %s in the table %s', $result['uid'], $tableName);
                            $this->logger->warning($errorMessage);
                            $this->connection->update($localTable, [
                                'error' => $errorMessage,
                                'errorTime' => date('Y-m-d H:i:s'),
                            ], [
                                'uid' => $result['uid'],
                                'table_name' => $tableName
                            ]);
                            continue;
                        }

                        if (isset($zohoBeans[$record['uid']])) {
                            $zohoBean = $zohoBeans[$record['uid']];
                        } else {
                            $zohoBean = $zohoDao->create();
                        }

                        if ($record['id'] && !$zohoBean->getZohoId()) {
                            $zohoBean->setZohoId($record['id']);
                        }

                        $fieldsUpdatedQuery = $this->connection->createQueryBuilder();
                        $fieldsUpdatedQuery
                            ->select('field_name')
                            ->from($localTable)
                            ->where('uid = :uid')
                            ->andWhere('table_name = :table_name')
                            ->andWhere('error IS NULL')
                            ->setParameters([
                                'uid' => $result['uid'],
                                'table_name' => $tableName,
                            ]);
                        $fieldsUpdatedResults = $fieldsUpdatedQuery->execute()->fetchAll();

                        foreach ($fieldsUpdatedResults as $fieldResults) {
                            $columnName = $fieldResults['field_name'];
                            if (array_key_exists($columnName, $record)) {
                                $this->updateDataZohoBean($zohoDao, $zohoBean, $columnName, $record[$columnName]);
                            } else {
                                $errorMessage = sprintf('Impossible to find the column %s for row with uid %s in the table %s', $columnName, $result['uid'], $tableName);
                                $this->logger->warning($errorMessage);
                                $this->connection->update($localTable, [
                                    'error' => $errorMessage,
                                    'errorTime' => date('Y-m-d H:i:s'),
                                ], [
                                    'uid' => $result['uid'],
                                    'table_name' => $tableName,
                                    'field_name' => $columnName
                                ]);
                                continue;
                            }
                        }

                        $this->logger->debug(sprintf('Updated row %s (id: \'%s\') from table %s added in queue to be pushed.', $record['uid'], $record['id'], $tableName));
                        $zohoBeans[$record['uid']] = $zohoBean;
                        $localRecords[$record['uid']] = $record;
                        $rowsDeleted[] = $record['uid'];
                    }
                } else {
                    $recordsToInsertQuery = $this->connection->createQueryBuilder();
                    $recordsToInsertQuery
                        ->select('DISTINCT table_name, uid')
                        ->from($localTable)
                        ->where('error IS NULL')
                        ->andWhere('table_name = :table_name')
                        ->setMaxResults($this->apiLimitInsertUpdateDelete)
                        ->setParameters([
                            'table_name' => $tableName
                        ]);
                    $recordsToInsertResults = $recordsToInsertQuery->execute()->fetchAll();
                    $this->logger->info(sprintf('Processing %s records to insert...', count($recordsToInsertResults)));
                    foreach ($recordsToInsertResults as $result) {
                        $recordQuery = $this->connection->createQueryBuilder();
                        $recordQuery
                            ->select('*')
                            ->from($tableName)
                            ->where('uid = :uid')
                            ->setParameters([
                                'uid' => $result['uid']
                            ]);
                        $record = $recordQuery->execute()->fetch();

                        if (!$record) {
                            $errorMessage = sprintf('Impossible to find row with uid %s in the table %s', $result['uid'], $tableName);
                            $this->logger->warning($errorMessage);
                            $this->connection->update($localTable, [
                                'error' => $errorMessage,
                                'errorTime' => date('Y-m-d H:i:s'),
                            ], [
                                'uid' => $result['uid'],
                                'table_name' => $tableName
                            ]);
                            continue;
                        }

                        if (isset($zohoBeans[$record['uid']])) {
                            $zohoBean = $zohoBeans[$record['uid']];
                        } else {
                            $zohoBean = $zohoDao->create();
                        }

                        $this->logger->debug(sprintf('New row with uid %s from table %s added in queue to be pushed.', $record['uid'], $tableName));
                        $this->insertDataZohoBean($zohoDao, $zohoBean, $record);
                        $zohoBeans[$record['uid']] = $zohoBean;
                        $localRecords[$record['uid']] = $record;
                        $rowsDeleted[] = $record['uid'];
                    }
                }
                if (count($zohoBeans)) {
                    $this->sendDataToZohoCleanLocal($zohoDao, $zohoBeans, $rowsDeleted, $update, $localRecords);
                }
                $countToPush = $this->countElementInTable($zohoDao, $update);
            } while ($countToPush > 0);
        }
    }

    /**
     * @param AbstractZohoDao $zohoDao
     * @param ZohoBeanInterface[] $zohoBeans
     * @param string[] $rowsDeleted
     * @param bool $update
     * @param mixed[] $localRecords
     */
    private function sendDataToZohoCleanLocal(AbstractZohoDao $zohoDao, array $zohoBeans, $rowsDeleted, $update = false, array $localRecords = [])
    {
        $local_table = $update ? 'local_update' : 'local_insert';
        $tableName = ZohoDatabaseHelper::getTableName($zohoDao, $this->prefix);
        $entityResponses = $zohoDao->save($zohoBeans);
        $responseKey = 0;
        foreach ($zohoBeans as $uid => $zohoBean) {
            $response = $entityResponses[$responseKey]->getResponseJSON();
            if (strtolower($response['code']) === 'success') {
                if ($update) {
                    $this->logger->debug(sprintf('Updated successfully the record with uid %s (id \'%s\') from the table %s', $uid, $zohoBean->getZohoId(), $tableName));
                    $this->connection->executeQuery(
                        'DELETE FROM local_update WHERE uid LIKE :uid AND table_name = :table_name AND error IS NULL',
                        [
                            'uid' => $uid,
                            'table_name' => $tableName
                        ]
                    );
                } else {
                    $countResult = (int)$this->connection->fetchColumn('select count(id) from ' . $tableName . ' where id = :id', ['id' => $zohoBean->getZohoId()]);
                    //If the sent data were duplicates Zoho can merged so we need to check if the Zoho ID already exist.
                    if ($countResult === 0) {
                        // ID not exist we can update the new row with the Zoho ID
                        $this->connection->beginTransaction();
                        $this->connection->update($tableName, ['id' => $zohoBean->getZohoId()], ['uid' => $uid]);
                        $this->connection->delete('local_insert', ['table_name' => $tableName, 'uid' => $uid]);
                        $this->connection->commit();
                        $this->logger->debug(sprintf('Inserted successfully the record with uid %s (id \'%s\') from the table %s', $uid, $zohoBean->getZohoId(), $tableName));
                    } else {
                        //ID already exist we need to delete the duplicate row.
                        $this->connection->beginTransaction();
                        $this->connection->delete($tableName, ['uid' => $uid]);
                        $this->connection->delete('local_insert', ['table_name' => $tableName, 'uid' => $uid]);
                        $this->connection->commit();
                        $this->logger->warning(sprintf('Duplicate record found when inserting record with uid %s from the table %s. ID updated: %s. UID deleted: %s', $uid, $tableName, $zohoBean->getZohoId(), $uid));
                    }
                    $record = $localRecords[$uid];
                    $record['id'] = $zohoBean->getZohoId();

                    foreach ($this->listeners as $listener) {
                        $listener->onInsert($record, $zohoDao);
                    }
                }
            } else {
                $errorMessage = sprintf('An error occurred when %s record with uid %s from table %s into Zoho: %s', ($update ? 'updating' : 'inserting'), $uid, $tableName, json_encode($response));
                $this->logger->error($errorMessage);
                $this->connection->update($local_table, [
                    'error' => $errorMessage,
                    'errorTime' => date('Y-m-d H:i:s')
                ], [
                    'uid' => $uid,
                    'table_name' => $tableName
                ]);
            }
            $responseKey++;
        }
    }

    private function endsWith($haystack, $needle)
    {
        return substr_compare($haystack, $needle, -strlen($needle)) === 0;
    }

    /**
     * Insert data to bean in order to insert zoho records.
     *
     * @param AbstractZohoDao $dao
     * @param ZohoBeanInterface $zohoBean
     * @param array $row
     */
    private function insertDataZohoBean(AbstractZohoDao $dao, ZohoBeanInterface $zohoBean, array $row)
    {
        foreach ($row as $columnName => $columnValue) {
            $fieldMethod = $dao->getFieldFromFieldName($columnName);
            if (!in_array($columnName, EntitiesGeneratorService::$defaultDateFields) && $fieldMethod
                && (!in_array($columnName, ['id', 'uid'])) && !is_null($columnValue)
            ) {
                // Changing only Name doesn't work properly on Zoho
                if ($this->endsWith($columnName, '_OwnerName') || $this->endsWith($columnName, '_Name')) {
                    continue;
                }
                $type = $fieldMethod->getType();
                $value = $this->formatValueToBeans($type, $columnValue);
                $setterMethod = $fieldMethod->getSetter();
                $zohoBean->{$setterMethod}($value);
            }
        }
    }

    /**
     * Insert data to bean in order to update zoho records.
     *
     * @param ZohoBeanInterface $zohoBean
     * @param array $fieldsMatching
     * @param type $columnName
     * @param type $valueDb
     */
    private function updateDataZohoBean(AbstractZohoDao $dao, ZohoBeanInterface $zohoBean, $columnName, $valueDb)
    {
        $fieldMethod = $dao->getFieldFromFieldName($columnName);
        if (!in_array($columnName, EntitiesGeneratorService::$defaultDateFields) && $fieldMethod
            && !in_array($columnName, ['id', 'uid'])
        ) {
            // Changing only Name doesn't work properly on Zoho
            if ($this->endsWith($columnName, '_OwnerName') || $this->endsWith($columnName, '_Name')) {
                return;
            }
            $type = $fieldMethod->getType();
            $value = is_null($valueDb) ? $valueDb : $this->formatValueToBeans($type, $valueDb);
            $setterMethod = $fieldMethod->getSetter();
            $zohoBean->{$setterMethod}($value);
        }
    }

    /**
     * Change the value to the good format.
     *
     * @param string $type
     * @param mixed $value
     *
     * @return mixed
     */
    private function formatValueToBeans($type, $value)
    {
        switch ($type) {
            case 'date':
                $value = \DateTime::createFromFormat('Y-m-d', $value) ?: null;
                break;
            case 'datetime':
                $value = \DateTime::createFromFormat('Y-m-d H:i:s', $value) ?: null;
                break;
            case 'boolean' :
                $value = (bool)$value;
                break;
            case 'percent' :
                $value = (int)$value;
                break;
            case 'double' :
                $value = (float)$value;
                break;
            case 'multiselectlookup':
            case 'multiuserlookup':
            case 'multiselectpicklist':
                $value = explode(';', $value);
                break;
        }

        return $value;
    }

    /**
     * Run deleted rows to Zoho : local_delete.
     *
     * @param AbstractZohoDao $zohoDao
     */
    public function pushDeletedRows(AbstractZohoDao $zohoDao)
    {
        $localTable = 'local_delete';
        $tableName = ZohoDatabaseHelper::getTableName($zohoDao, $this->prefix);
        $statement = $this->connection->createQueryBuilder();
        $statement->select('l.id')
            ->from($localTable, 'l')
            ->where('l.table_name=:table_name')
            ->setParameters(
                [
                    'table_name' => $tableName,
                ]
            );
        $results = $statement->execute();
        while ($row = $results->fetch()) {
            $zohoDao->delete($row['id']);
            $this->connection->delete($localTable, ['table_name' => $tableName, 'id' => $row['id']]);
        }
    }

    /**
     * Run inserted rows to Zoho : local_insert.
     *
     * @param AbstractZohoDao $zohoDao
     */
    public function pushInsertedRows(AbstractZohoDao $zohoDao)
    {
        $this->pushDataToZoho($zohoDao);
    }

    /**
     * Run updated rows to Zoho : local_update.
     *
     * @param AbstractZohoDao $zohoDao
     */
    public function pushUpdatedRows(AbstractZohoDao $zohoDao)
    {
        $this->pushDataToZoho($zohoDao, true);
    }

    /**
     * Push data from db to Zoho.
     *
     * @param AbstractZohoDao $zohoDao
     */
    public function pushToZoho(AbstractZohoDao $zohoDao)
    {
        $this->logger->info(sprintf('Pushing inserted rows for module %s into Zoho...', $zohoDao->getPluralModuleName()));
        $this->pushInsertedRows($zohoDao);
        $this->logger->info(sprintf('Pushing updated rows for module %s into Zoho...', $zohoDao->getPluralModuleName()));
        $this->pushUpdatedRows($zohoDao);
        $this->logger->info(sprintf('Pushing deleted rows for module %s into Zoho...', $zohoDao->getPluralModuleName()));
        $this->pushDeletedRows($zohoDao);
    }
}
