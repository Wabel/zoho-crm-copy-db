<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;
use Wabel\Zoho\CRM\ZohoClient;
use zcrmsdk\crm\crud\ZCRMRecord;
use zcrmsdk\crm\exception\ZCRMException;
use ZipArchive;

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
        $this->logger->info('Fetched ' . count($users) . ' records for table ' . $tableName);

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
                $this->logger->debug(sprintf('Inserting record with ID \'%s\' in table %s...', $user->getId(), $tableName));

                $data['id'] = $user->getId();
                $types['id'] = 'string';

                $this->connection->insert($tableName, $data, $types);
            } elseif ($data) {
                $this->logger->debug(sprintf('Updating record with ID \'%s\' in table %s...', $user->getId(), $tableName));
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
     * @param string $modifiedSince
     *
     */
    public function fetchFromZoho(AbstractZohoDao $dao, $incrementalSync = true, $twoWaysSync = true, $throwErrors = true, $modifiedSince = null): void
    {
        $tableName = ZohoDatabaseHelper::getTableName($dao, $this->prefix);
        $tableDetail = $this->connection->getSchemaManager()->listTableDetails($tableName);
        $tableHasColumnModifiedTime = $tableDetail->hasColumn('modifiedTime');
        $tableHasColumnCreatedTime = $tableDetail->hasColumn('createdTime');

        $table = $this->connection->getSchemaManager()->createSchema()->getTable($tableName);

        // To speed up process we cache the table columns/type here
        $tableColumns = [];
        foreach ($table->getColumns() as $column) {
            $tableColumns[$column->getName()] = $column->getType()->getName();
        }

        $cachedFieldForColumnName = [];

        $zohoSyncConfigTableExists = $this->connection->getSchemaManager()->tablesExist(['zoho_sync_config']);
        $localUpdateTableExists = $this->connection->getSchemaManager()->tablesExist(['local_update']);

        $currentDateTime = new \DateTime();

        // First step, we determine the oldest date to get records from
        $dateModifiedSince = null;
        if ($incrementalSync) {
            // For incremental copy, we search the best date in config table or according the the latest value in the table
            $this->logger->info('Incremental synchronization started');
            $findDateByModifiedTime = false;

            // We check if the synchronization date exists in config table
            if ($zohoSyncConfigTableExists && $modifiedSince === null) {
                $lastDateInConfig = $this->connection->fetchColumn('SELECT config_value FROM zoho_sync_config WHERE config_key = ? AND table_name = ?', [
                    'FETCH_RECORDS_MODIFIED_SINCE__DATE',
                    $tableName
                ]);
                if ($lastDateInConfig !== false) {
                    $lastPageInConfig = $this->connection->fetchColumn('SELECT config_value FROM zoho_sync_config WHERE config_key = ? AND table_name = ?', [
                        'FETCH_RECORDS_MODIFIED_SINCE__PAGE',
                        $tableName
                    ]);
                    if ($lastPageInConfig === false) {
                        $lastPageInConfig = '1';
                    }
                    $dateModifiedSince = new \DateTime($lastDateInConfig, new \DateTimeZone($dao->getZohoClient()->getTimezone()));
                    $recordsPage = (int)$lastPageInConfig;
                } else {
                    $findDateByModifiedTime = true;
                }
            } else {
                $findDateByModifiedTime = true;
            }

            // If no synchronization date found, we search according to the latest value from 'modified/created time' from the table
            if ($findDateByModifiedTime) {
                if ($modifiedSince) {
                    $dateModifiedSince = new \DateTime($modifiedSince);
                } else {
                    $latestDateValue = null;
                    if ($tableHasColumnModifiedTime) {
                        $latestDateValue = $this->connection->fetchColumn('SELECT MAX(modifiedTime) FROM ' . $tableName);
                    }
                    if (!$latestDateValue && $tableHasColumnCreatedTime) {
                        $latestDateValue = $this->connection->fetchColumn('SELECT MAX(createdTime) FROM ' . $tableName);
                    }

                    if ($latestDateValue !== null) {
                        $dateModifiedSince = new \DateTime($latestDateValue, new \DateTimeZone($dao->getZohoClient()->getTimezone()));
                        // Let's add one second to the last activity time (otherwise, we are fetching again the last record in DB).
                        $dateModifiedSince->add(new \DateInterval('PT1S'));
                    }
                }
            }
        } else {
            $this->logger->info('Full synchronization started');
            // In case of incremental sync, we first check if a specific date exists in input
            // Otherwise we start to get records since the beginning
            if ($modifiedSince !== null) {
                $dateModifiedSince = new \DateTime($modifiedSince);
            }
        }

        if ($dateModifiedSince) {
            $this->logger->notice(sprintf('Synchronization started from %s for module %s', $dateModifiedSince->format('c'), $dao->getPluralModuleName()));
        } else {
            $this->logger->notice(sprintf('Synchronization started from the beginning of time for module %s', $dao->getPluralModuleName()));
        }

        $sortColumn = null;
        $sortOrder = null;

        if ($tableHasColumnModifiedTime) {
            $sortColumn = 'Modified_Time';
            $sortOrder = 'asc';
        } else if ($tableHasColumnCreatedTime) {
            $sortColumn = 'Created_Time';
            $sortOrder = 'asc';
        }

        $stopAndhasMoreResults = true;
        $recordsPaginationLastTime = null;

        $recordsModificationCounts = [
            'insert' => 0,
            'update' => 0,
            'delete' => 0,
        ];

        $recordsPage = 1;

        // Second step, we loop on every records to update
        while ($stopAndhasMoreResults) {
            // Fetch of the records, 200 per 200, to avoid memory loss
            try {
                $records = $dao->getRecords(null, $sortColumn, $sortOrder, $dateModifiedSince, $recordsPage, 200, $stopAndhasMoreResults);
            } catch (ZCRMException $exception) {
                $this->logger->error('Error when getting updated records for module ' . $dao->getPluralModuleName() . ' and page ' . $recordsPage . ': ' . $exception->getMessage(), [
                    'exception' => $exception
                ]);
                if ($throwErrors) {
                    throw $exception;
                }
                return;
            }
            if ($stopAndhasMoreResults) {
                if ($zohoSyncConfigTableExists) {
                    $lastDate = $dateModifiedSince ? $dateModifiedSince->format('Y-m-d H:i:s') : $currentDateTime->format('Y-m-d H:i:s');
                    $this->upsertZohoConfig('FETCH_RECORDS_MODIFIED_SINCE__DATE', $tableName, $lastDate);
                    $this->upsertZohoConfig('FETCH_RECORDS_MODIFIED_SINCE__PAGE', $tableName, (string)$recordsPage);
                }
                $recordsPage++;
            } else {
                if ($zohoSyncConfigTableExists) {
                    $latestDateToSave = $currentDateTime->format('Y-m-d H:i:s');
                    if ($tableHasColumnModifiedTime) {
                        $latestDateToSave = $this->connection->fetchColumn('SELECT MAX(modifiedTime) FROM ' . $tableName);
                    }
                    if (!$latestDateToSave && $tableHasColumnCreatedTime) {
                        $latestDateToSave = $this->connection->fetchColumn('SELECT MAX(createdTime) FROM ' . $tableName);
                    }
                    if (!$latestDateToSave) {
                        $latestDateToSave = $currentDateTime->format('Y-m-d H:i:s');
                    }
                    $this->upsertZohoConfig('FETCH_RECORDS_MODIFIED_SINCE__DATE', $tableName, $latestDateToSave);
                    $this->upsertZohoConfig('FETCH_RECORDS_MODIFIED_SINCE__PAGE', $tableName, '1');
                }
            }
            $totalRecords = count($records);
            $this->logger->debug($totalRecords . ' records fetched.');

            // Save of the records in db
            $this->logger->info(sprintf('Inserting/updating %s records into table %s...', $totalRecords, $tableName));
            $select = $this->connection->prepare('SELECT * FROM ' . $tableName . ' WHERE id = :id');
            $this->connection->beginTransaction();
            $logOffset = (int)ceil($totalRecords / 4);
            $processedRecords = 0;
            $dataInsertedInDb = 0;
            $dataUpdatedInDb = 0;
            if ($totalRecords) {
                foreach ($records as $record) {
                    if ($processedRecords !== 0 && ($processedRecords % $logOffset) === 0) {
                        $this->logger->info(sprintf('%d/%s records processed for module %s', $processedRecords, $totalRecords, $dao->getPluralModuleName()));
                    }
                    ++$processedRecords;

                    // Get column type dans data
                    $data = [];
                    $types = [];
                    foreach ($tableColumns as $columnName => $columntype) {
                        if (in_array($columnName, ['id', 'uid'])) {
                            continue;
                        }

                        $field = null;
                        if (isset($cachedFieldForColumnName[$columnName]) && $cachedFieldForColumnName[$columnName] !== null) {
                            $field = $cachedFieldForColumnName[$columnName];
                        }
                        if (!$field) {
                            $field = $dao->getFieldFromFieldName($columnName);
                            if ($field) {
                                $cachedFieldForColumnName[$columnName] = $field;
                            }
                        }
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
                        $data[$columnName] = $finalFieldData;
                        $types[$columnName] = $columntype;
                    }

                    // Start to insert/update
                    $select->execute(['id' => $record->getZohoId()]);
                    $result = $select->fetch(\PDO::FETCH_ASSOC);
                    if ($result === false) {
                        $this->logger->debug(sprintf('Inserting record with ID \'%s\' in table %s...', $record->getZohoId(), $tableName));

                        $data['id'] = $record->getZohoId();
                        $types['id'] = 'string';

                        $dataInsertedInDb += $this->connection->insert($tableName, $data, $types);

                        foreach ($this->listeners as $listener) {
                            $listener->onInsert($data, $dao);
                        }
                    } else {
                        $this->logger->debug(sprintf('Updating record with ID \'%s\' in table %s...', $record->getZohoId(), $tableName));
                        $identifier = ['id' => $record->getZohoId()];
                        $types['id'] = 'string';

                        // If there is some columns updated in local_update, we skip them to avoid Zoho values to overide them
                        if ($localUpdateTableExists && $result['uid']) {
                            $selectRecordInLocalUpdate = $this->connection->prepare('SELECT field_name FROM local_update WHERE table_name = "' . $tableName . '" AND uid = :uid');
                            $selectRecordInLocalUpdate->execute(['uid' => $result['uid']]);
                            $resultsInLocalUpdate = $selectRecordInLocalUpdate->fetchAll(\PDO::FETCH_ASSOC);
                            $fieldsUpdated = [];
                            if (count($resultsInLocalUpdate)) {
                                foreach ($resultsInLocalUpdate as $resultUpdated) {
                                    $fieldsUpdated[] = $resultUpdated['field_name'];
                                }
                                $fieldsUpdated = array_unique($fieldsUpdated);
                            }

                            if (count($fieldsUpdated)) {
                                foreach ($fieldsUpdated as $field) {
                                    if (isset($data[$field])) {
                                        unset($data[$field], $types[$field]);
                                    }
                                }
                            }
                        }

                        $dataUpdatedInDb += $this->connection->update($tableName, $data, $identifier, $types);

                        // Let's add the id for the update trigger
                        $data['id'] = $record->getZohoId();
                        foreach ($this->listeners as $listener) {
                            $listener->onUpdate($data, $result, $dao);
                        }
                    }
                }
            }
            $recordsModificationCounts['insert'] += $dataInsertedInDb;
            $recordsModificationCounts['update'] += $dataUpdatedInDb;

            if ($totalRecords === 0) {
                $stopAndhasMoreResults = false;
            }

            $this->connection->commit();
        }

        // Third step, we loop on every records to delete
        $stopAndhasMoreResults = true;
        $recordsDeletedPage = 1;
        while ($stopAndhasMoreResults) {
            // Fetch of the records, 200 per 200, to avoid memory loss
            try {
                $deletedRecords = $dao->getDeletedRecordIds($dateModifiedSince, $recordsDeletedPage, 200, $stopAndhasMoreResults);
            } catch (ZCRMException $exception) {
                $this->logger->error('Error when getting deleted records for module ' . $dao->getPluralModuleName() . ' and page ' . $recordsDeletedPage . ': ' . $exception->getMessage(), [
                    'exception' => $exception
                ]);
                if ($throwErrors) {
                    throw $exception;
                }
                return;
            }
            if ($stopAndhasMoreResults) {
                ++$recordsDeletedPage;
            }
            $totalRecordsDeleted = count($deletedRecords);
            $this->logger->debug($totalRecordsDeleted . ' records fetched.');

            $this->logger->info(sprintf('Deleting %d records from table %s...', $totalRecordsDeleted, $tableName));
            $this->connection->beginTransaction();
            $sqlStatementUid = 'select uid from ' . $this->connection->quoteIdentifier($tableName) . ' where id = :id';
            $processedRecords = 0;
            $logOffset = (int)ceil($totalRecordsDeleted / 4);
            $dataDeletedInDb = 0;
            if ($totalRecordsDeleted) {
                foreach ($deletedRecords as $deletedRecord) {
                    if ($processedRecords !== 0 && ($processedRecords % $logOffset) === 0) {
                        $this->logger->info(sprintf('%d/%d records processed for module %s', $processedRecords, $totalRecordsDeleted, $dao->getPluralModuleName()));
                    }
                    ++$processedRecords;
                    $this->logger->debug(sprintf('Deleting record with ID \'%s\' in table %s...', $deletedRecord->getEntityId(), $tableName));
                    $uid = $this->connection->fetchColumn($sqlStatementUid, ['id' => $deletedRecord->getEntityId()]);
                    $dataDeletedInDb += $this->connection->delete($tableName, ['id' => $deletedRecord->getEntityId()]);
                    if ($twoWaysSync) {
                        // Also, let's remove the newly created field (because of the trigger) to avoid looping back to Zoho
                        $this->connection->delete('local_delete', ['table_name' => $tableName, 'id' => $deletedRecord->getEntityId()]);
                        $this->connection->delete('local_update', ['table_name' => $tableName, 'uid' => $uid]);
                    }
                }
            }
            $recordsModificationCounts['delete'] += $dataDeletedInDb;

            if (!$totalRecordsDeleted === 0) {
                $stopAndhasMoreResults = false;
            }

            $this->connection->commit();
        }

        // Last step, we save the current status of the synchronization
        $this->logger->notice(sprintf('Synchronization finished with %d item(s) inserted, %d item(s) updated and %d item(s) deleted for module %s.',
            $recordsModificationCounts['insert'],
            $recordsModificationCounts['update'],
            $recordsModificationCounts['delete'],
            $dao->getPluralModuleName()
        ));
    }

    public function fetchFromZohoInBulk(AbstractZohoDao $dao)
    {
        /*
         * This method is really dirty, and do not use the php sdk because late development for the zoho v1 EOL in december.
         * Should be re-written to make it clean.
         */
        // Doc: https://www.zoho.com/crm/developer/docs/api/bulk-read/create-job.html

        $tableName = ZohoDatabaseHelper::getTableName($dao, $this->prefix);
        $table = $this->connection->getSchemaManager()->createSchema()->getTable($tableName);
        $apiModuleName = $dao->getPluralModuleName();

        $this->logger->notice('Starting bulk fetch for module ' . $apiModuleName . '...');

        $zohoClient = new ZohoClient([
            'client_id' => ZOHO_CRM_CLIENT_ID,
            'client_secret' => ZOHO_CRM_CLIENT_SECRET,
            'redirect_uri' => ZOHO_CRM_CLIENT_REDIRECT_URI,
            'currentUserEmail' => ZOHO_CRM_CLIENT_CURRENT_USER_EMAIL,
            'applicationLogFilePath' => ZOHO_CRM_CLIENT_APPLICATION_LOGFILEPATH,
            'persistence_handler_class' => ZOHO_CRM_CLIENT_PERSISTENCE_HANDLER_CLASS,
            'token_persistence_path' => ZOHO_CRM_CLIENT_PERSITENCE_PATH,
            'sandbox' => ZOHO_CRM_SANDBOX
        ], 'Europe/Paris');

        $client = new \GuzzleHttp\Client();
        $page = 1;
        while (true) {
            $oauthToken = $zohoClient->getZohoOAuthClient()->getAccessToken(ZOHO_CRM_CLIENT_CURRENT_USER_EMAIL);

            // Step 1: Create a bulk read job
            $this->logger->info('Creating read job for module ' . $apiModuleName . ' and page ' . $page . '...');
            $response = $client->request('POST', 'https://' . (ZOHO_CRM_SANDBOX === 'true' ? 'sandbox' : 'www') . '.zohoapis.com/crm/bulk/v2/read', [
                'http_errors' => false,
                'headers' => [
                    'Authorization' => 'Zoho-oauthtoken ' . $oauthToken
                ],
                'json' => [
                    'query' => [
                        'module' => $apiModuleName,
                        'page' => $page
                    ]
                ]
            ]);
            $jobId = null;
            if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
                $resultStr = $response->getBody()->getContents();
                $json = json_decode($resultStr, true);

                $jobId = $json['data'][0]['details']['id'];

                // We don't care about the job status right now, it will be checked later
            } else {
                $this->logger->error('Cannot create bulk read query for module ' . $apiModuleName . ': status: ' . $response->getStatusCode() . '. Status: ' . $response->getBody()->getContents());
                break;
            }

            if ($jobId === null) {
                $this->logger->error('JobID cannot be null. json:' . $resultStr);
                break;
            }

            // Step 2: Check job status
            $jobDetails = null;
            while (true) {
                $this->logger->info('Checking job ' . $jobId . ' status for module ' . $apiModuleName . ' and page ' . $page . '...');
                $response = $client->request('GET', 'https://' . (ZOHO_CRM_SANDBOX === 'true' ? 'sandbox' : 'www') . '.zohoapis.com/crm/bulk/v2/read/' . $jobId, [
                    'http_errors' => false,
                    'headers' => [
                        'Authorization' => 'Zoho-oauthtoken ' . $oauthToken
                    ]
                ]);
                if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
                    $resultStr = $response->getBody()->getContents();
                    $json = json_decode($resultStr, true);

                    if (isset($json['data'][0]['state'])) {
                        $status = $json['data'][0]['state'];
                        if ($status === 'ADDED' || $status === 'QUEUED') {
                            $this->logger->info('Job still waiting for process');
                        } else if ($status === 'IN PROGRESS') {
                            $this->logger->info('Job in progress');
                        } else if ($status === 'COMPLETED') {
                            $this->logger->info('Job completed');
                            $jobDetails = $json;
                            break;
                        } else {
                            $this->logger->info('Unsupported job status: ' . $resultStr);
                            break;
                        }
                    } else {
                        $this->logger->error('Unsupported response: ' . $resultStr);
                        break;
                    }
                } else {
                    $this->logger->error('Cannot get bulk job status query for module ' . $apiModuleName . ': status: ' . $response->getStatusCode() . '. Status: ' . $response->getBody()->getContents());
                    break;
                }
                sleep(15);
            }

            // Step 3: Download the result
            if ($jobDetails === null) {
                $this->logger->error('JobDetails cannot be empty. json:' . $resultStr);
                break;
            }
            $this->logger->debug(json_encode($jobDetails));
            $this->logger->info('Downloading zip file for module ' . $apiModuleName . ' and page ' . $page . '...');
            $jobZipFile = '/tmp/job_' . $dao->getZCRMModule()->getAPIName() . '_' . $jobDetails['data'][0]['id'] . '.zip';
            $jobCsvPath = '/tmp/job_extract';
            $jobCsvFile = '/tmp/job_extract/' . $jobDetails['data'][0]['id'] . '.csv';
            $canProcessCsv = false;

            $response = $client->request('GET', 'https://' . (ZOHO_CRM_SANDBOX === 'true' ? 'sandbox' : 'www') . '.zohoapis.com/crm/bulk/v2/read/' . $jobId . '/result', [
                'http_errors' => false,
                'headers' => [
                    'Authorization' => 'Zoho-oauthtoken ' . $oauthToken
                ],
                'sink' => $jobZipFile
            ]);
            if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
                $this->logger->info('Extracting ' . $jobZipFile . ' file for module ' . $apiModuleName . ' and page ' . $page . '...');
                $zip = new ZipArchive();
                $res = $zip->open($jobZipFile);
                if ($res === TRUE) {
                    $zip->extractTo($jobCsvPath);
                    $zip->close();
                    $this->logger->info('File extracted in ' . $jobCsvFile);
                    $canProcessCsv = true;
                } else {
                    switch ($res) {
                        case ZipArchive::ER_EXISTS:
                            $zipErrorMessage = 'File already exists.';
                            break;
                        case ZipArchive::ER_INCONS:
                            $zipErrorMessage = 'Zip archive inconsistent.';
                            break;
                        case ZipArchive::ER_MEMORY:
                            $zipErrorMessage = 'Malloc failure.';
                            break;
                        case ZipArchive::ER_NOENT:
                            $zipErrorMessage = 'No such file.';
                            break;
                        case ZipArchive::ER_NOZIP:
                            $zipErrorMessage = 'Not a zip archive.';
                            break;
                        case ZipArchive::ER_OPEN:
                            $zipErrorMessage = "Can't open file.";
                            break;
                        case ZipArchive::ER_READ:
                            $zipErrorMessage = 'Read error.';
                            break;
                        case ZipArchive::ER_SEEK:
                            $zipErrorMessage = 'Seek error.';
                            break;
                        default:
                            $zipErrorMessage = "Unknow (Code $res)";
                            break;
                    }
                    $this->logger->error('Error when extracting zip file: ' . $zipErrorMessage);
                    break;
                }
            } else {
                $this->logger->error('Cannot download results for module ' . $apiModuleName . ': status: ' . $response->getStatusCode() . '. Status: ' . $response->getBody()->getContents());
                break;
            }

            // Step 4: Save data
            if (!$canProcessCsv) {
                $this->logger->error('Cannot process CSV');
                break;
            }

            $this->logger->info('Building list of users...');
            $usersQuery = $this->connection->executeQuery('SELECT id, full_name FROM users');
            $usersResults = $usersQuery->fetchAll();
            $users = [];
            foreach ($usersResults as $user) {
                $users[$user['id']] = $user['full_name'];
            }

            $this->logger->info('Saving records to db...');
            $nbRecords = $jobDetails['data'][0]['result']['count'];
            $whenToLog = ceil($nbRecords / 100);
            $this->logger->info($nbRecords . ' records to save');
            $nbSaved = 0;
            $handle = fopen($jobCsvFile, 'r');
            $fields = [];
            if ($handle) {
                while (($row = fgetcsv($handle)) !== false) {
                    if (empty($fields)) {
                        $fields = $row;
                        continue;
                    }
                    $recordDataToInsert = [];
                    foreach ($row as $k => $value) {
                        $columnName = $fields[$k];
                        $decodedColumnName = str_replace('_', '', $columnName);
                        if ($table->hasColumn($decodedColumnName)) {
                            $recordDataToInsert[$decodedColumnName] = $value === '' ? null : $value;
                        } else {
                            if ($columnName === 'Owner' || $columnName === 'Created_By' || $columnName === 'Modified_By') {
                                $recordDataToInsert[$decodedColumnName . '_OwnerID'] = $value === '' ? null : $value;
                                $recordDataToInsert[$decodedColumnName . '_OwnerName'] = $users[$value] ?? null;
                            } else if ($table->hasColumn($decodedColumnName . '_ID')) {
                                $recordDataToInsert[$decodedColumnName . '_ID'] = $value === '' ? null : $value;
                            }
                        }
                    }
                    $this->connection->insert($tableName, $recordDataToInsert);
                    ++$nbSaved;
                    if (($nbSaved % $whenToLog) === 0) {
                        $this->logger->info($nbSaved . '/' . $nbRecords . ' records processed');
                    }
                }
                $this->logger->info($nbSaved . ' records saved for module ' . $apiModuleName . ' and page ' . $page);
                fclose($handle);
            }

            // Step 5: Check if there is more results
            $hasMoreRecords = $jobDetails['data'][0]['result']['more_records'];
            if (!$hasMoreRecords) {
                $this->logger->info('No more records for the module ' . $apiModuleName);
                break;
            }
            $this->logger->info('More records to fetch for the module ' . $apiModuleName);
            ++$page;
        }
    }

    private function upsertZohoConfig(string $configKey, string $tableName, string $configValue)
    {
        $configExists = $this->connection->fetchColumn('SELECT config_value FROM zoho_sync_config WHERE config_key = ? AND table_name = ?', [
            $configKey,
            $tableName
        ]);
        if ($configExists === false) {
            $this->connection->insert('zoho_sync_config', [
                'config_key' => $configKey,
                'table_name' => $tableName,
                'config_value' => $configValue
            ]);
        } else {
            $this->connection->update('zoho_sync_config', [
                'config_value' => $configValue
            ], [
                'config_key' => $configKey,
                'table_name' => $tableName,
            ]);
        }
    }
}
