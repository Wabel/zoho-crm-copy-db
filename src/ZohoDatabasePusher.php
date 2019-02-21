<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;
use Wabel\Zoho\CRM\Service\EntitiesGeneratorService;
use Wabel\Zoho\CRM\ZohoBeanInterface;

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
     * @param Connection $connection
     * @param int $apiLimitInsertUpdateDelete
     * @param string $prefix
     * @param LoggerInterface $logger
     */
    public function __construct(Connection $connection, $apiLimitInsertUpdateDelete = 100, $prefix = 'zoho_', LoggerInterface $logger = null)
    {
        $this->connection = $connection;
        $this->prefix = $prefix;
        if ($logger === null) {
            $this->logger = new NullLogger();
        } else {
            $this->logger = $logger;
        }
        $this->apiLimitInsertUpdateDelete = $apiLimitInsertUpdateDelete;
        if($apiLimitInsertUpdateDelete === null){
            $this->apiLimitInsertUpdateDelete = 100;
        }
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
        $localTable = $update ? 'local_update' : 'local_insert';
        $tableName = ZohoDatabaseHelper::getTableName($zohoDao, $this->prefix);
        return $this->connection->executeQuery('select uid from '.$localTable.' where table_name like :tableName',
            ['tableName' => $tableName])->rowCount();
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
        do{
            $rowsDeleted = [];
            //@see https://www.zoho.com/crm/help/api/v2/#ra-update-records
            //To optimize your API usage, get maximum 200 records with each request and insert, update or delete maximum 100 records with each request.
            $statementLimiter = $this->connection->createQueryBuilder();
            $statementLimiter->select('DISTINCT table_name,uid')
                ->from($localTable)->setMaxResults($this->apiLimitInsertUpdateDelete);
            $statement = $this->connection->createQueryBuilder();
            $statement->select('zcrm.*');
            if ($update) {
                $statement->addSelect('l.field_name as updated_fieldname');
            }
            $statement->from($localTable, 'l')
                ->join('l','('.$statementLimiter->getSQL().')','ll','ll.table_name = l.table_name and  ll.uid = l.uid')
                ->join('l', $tableName, 'zcrm', 'zcrm.uid = l.uid')
                ->where('l.table_name=:table_name')
                ->setParameters([
                    'table_name' => $tableName,
                ])
            ;
            $results = $statement->execute();
            /* @var $zohoBeans ZohoBeanInterface[] */
            $zohoBeans = array();
            while ($row = $results->fetch()) {
//                $beanClassName = $zohoDao->getBeanClassName();
                /* @var $zohoBean ZohoBeanInterface */
                if (isset($zohoBeans[$row['uid']])) {
                    $zohoBean = $zohoBeans[$row['uid']];
                } else {
//                    $zohoBean = new $beanClassName();
                    $zohoBean = $zohoDao->create();
                }

                if (!$update) {
                    $this->insertDataZohoBean($zohoDao, $zohoBean, $row);
                    $zohoBeans[$row['uid']] = $zohoBean;
                    $rowsDeleted[] = $row['uid'];
                }
                if ($update && isset($row['updated_fieldname'])) {
                    $columnName = $row['updated_fieldname'];
                    $zohoBean->getZohoId() ?: $zohoBean->setZohoId($row['id']);
                    $this->updateDataZohoBean($zohoDao, $zohoBean, $columnName, $row[$columnName]);
                    $zohoBeans[$row['uid']] = $zohoBean;
                    $rowsDeleted[] = $row['uid'];
                }
            }
            if($zohoBeans){
                $this->sendDataToZohoCleanLocal($zohoDao,$zohoBeans,$rowsDeleted,$update);
            }
            $countToPush = $this->countElementInTable($zohoDao,$update);
        } while($countToPush > 0);
    }

    /**
     * @param AbstractZohoDao $zohoDao
     * @param ZohoBeanInterface[] $zohoBeans
     * @param string[] $rowsDeleted
     * @param bool $update
     */
    private function sendDataToZohoCleanLocal(AbstractZohoDao $zohoDao, array $zohoBeans,$rowsDeleted, $update = false)
    {
        $tableName = ZohoDatabaseHelper::getTableName($zohoDao, $this->prefix);
        $zohoDao->save($zohoBeans);
        if (!$update) {
            foreach ($zohoBeans as $uid => $zohoBean) {
                $countResult = (int) $this->connection->fetchColumn('select count(id) from '.$tableName.' where id = :id', ['id'=>$zohoBean->getZohoId()]);
                //If the sent data were duplicates Zoho can merged so we need to check if the Zoho ID already exist.
                if ($countResult === 0) {
                    // ID not exist we can update the new row with the Zoho ID
                    $this->connection->beginTransaction();
                    $this->connection->update($tableName, ['id' => $zohoBean->getZohoId()], ['uid' => $uid]);
                    $this->connection->delete('local_insert', ['table_name'=>$tableName, 'uid' => $uid ]);
                    $this->connection->commit();
                } else {
                    //ID already exist we need to delete the duplicate row.
                    $this->connection->beginTransaction();
                    $this->connection->delete($tableName, ['uid' => $uid ]);
                    $this->connection->delete('local_insert', ['table_name'=>$tableName, 'uid' => $uid ]);
                    $this->connection->commit();
                }
            }
        } else {
            $this->connection->executeUpdate('delete from local_update where uid in ( :rowsDeleted)',
                [
                    'rowsDeleted' => $rowsDeleted,
                ],
                [
                    'rowsDeleted' => Connection::PARAM_INT_ARRAY,
                ]
            );
        }
    }

    /**
     * Insert data to bean in order to insert zoho records.
     * @param AbstractZohoDao $dao
     * @param ZohoBeanInterface $zohoBean
     * @param array $row
     */
    private function insertDataZohoBean(AbstractZohoDao $dao, ZohoBeanInterface $zohoBean, array $row)
    {
        foreach ($row as $columnName => $columnValue) {
            $fieldMethod = $dao->getFieldFromFieldName($columnName);
            if(!in_array($columnName, EntitiesGeneratorService::$defaultDateFields) && $fieldMethod
                && (!in_array($columnName, ['id', 'uid'])) && !is_null($columnValue)) {
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
     * @param array             $fieldsMatching
     * @param type              $columnName
     * @param type              $valueDb
     */
    private function updateDataZohoBean(AbstractZohoDao $dao, ZohoBeanInterface $zohoBean, $columnName, $valueDb)
    {
        $fieldMethod = $dao->getFieldFromFieldName($columnName);
        if (!in_array($columnName, EntitiesGeneratorService::$defaultDateFields) && $fieldMethod
            && !in_array($columnName, ['id', 'uid'])) {
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
     * @param mixed  $value
     *
     * @return mixed
     */
    private function formatValueToBeans($type, $value)
    {
        switch ($type) {
            case 'date':
                $value = \DateTime::createFromFormat('Y-m-d', $value)?:null;
                break;
            case 'datetime':
                $value = \DateTime::createFromFormat('Y-m-d H:i:s', $value)?:null;
                break;
            case 'boolean' :
                $value = (bool) $value;
                break;
            case 'percent' :
                $value = (int) $value;
                break;
            case 'double' :
                $value = number_format($value, 2);
                break;
            case 'multiselectlookup':
            case 'multiuserlookup':
            case 'multiselectpicklist':
                $value = explode(';',$value);
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
        ->setParameters([
            'table_name' => $tableName,
        ]);
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
        return $this->pushDataToZoho($zohoDao);
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
        $this->logger->info(' > Insert new rows using {class_name}', ['class_name' => get_class($zohoDao)]);
        $this->pushInsertedRows($zohoDao);
        $this->logger->info(' > Update rows using {class_name}', ['class_name' => get_class($zohoDao)]);
        $this->pushUpdatedRows($zohoDao);
        $this->logger->info(' > Delete rows using  {class_name}', ['class_name' => get_class($zohoDao)]);
        $this->pushDeletedRows($zohoDao);
    }
}
