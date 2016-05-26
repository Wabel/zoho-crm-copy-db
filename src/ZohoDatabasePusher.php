<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;
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
     * @param Connection      $connection
     * @param string          $prefix
     * @param LoggerInterface $logger
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
    }

    /**
     * @param AbstractZohoDao $zohoDao
     *
     * @return array
     */
    private function findMethodValues(AbstractZohoDao $zohoDao)
    {
        $fieldsMatching = array();
        foreach ($zohoDao->getFields() as $fieldsDescriptor) {
            foreach (array_values($fieldsDescriptor) as $fieldDescriptor) {
                $fieldsMatching[$fieldDescriptor['name']] = [
                    'setter' => $fieldDescriptor['setter'],
                    'type' => $fieldDescriptor['type'],
                ];
            }
        }

        return $fieldsMatching;
    }

    /**
     * Insert or Update rows.
     *
     * @param AbstractZohoDao $zohoDao
     */
    public function pushDataToZoho(AbstractZohoDao $zohoDao, $update = false)
    {
        $localTable = $update ? 'local_update' : 'local_insert';
        $fieldsMatching = $this->findMethodValues($zohoDao);
        $tableName = ZohoDatabaseHelper::getTableName($zohoDao, $this->prefix);
        $rowsDeleted = [];
        $statement = $this->connection->createQueryBuilder();
        $statement->select('zcrm.*');
        if ($update) {
            $statement->addSelect('l.field_name as updated_fieldname');
        }
        $statement->from($localTable, 'l')
        ->join('l', $tableName, 'zcrm', 'zcrm.uid = l.uid')
        ->where('l.table_name=:table_name')
        ->setParameters([
            'table_name' => $tableName,
        ]);
        $results = $statement->execute();
        /* @var $zohoBeans ZohoBeanInterface[] */
        $zohoBeans = array();
        while ($row = $results->fetch()) {
            $beanClassName = $zohoDao->getBeanClassName();
            /* @var $zohoBean ZohoBeanInterface */
            if (isset($zohoBeans[$row['uid']])) {
                $zohoBean = $zohoBeans[$row['uid']];
            } else {
                $zohoBean = new $beanClassName();
            }

            if (!$update) {
                $this->insertDataZohoBean($zohoBean, $fieldsMatching, $row);
                $zohoBeans[$row['uid']] = $zohoBean;
                $rowsDeleted[] = $row['uid'];
            }
            if ($update && isset($row['updated_fieldname'])) {
                $columnName = $row['updated_fieldname'];
                $zohoBean->getZohoId() ?: $zohoBean->setZohoId($row['id']);
                $this->updateDataZohoBean($zohoBean, $fieldsMatching, $columnName, $row[$columnName]);
                $zohoBeans[$row['uid']] = $zohoBean;
                $rowsDeleted[] = $row['uid'];
            }
        }
        $zohoDao->save($zohoBeans);
        if (!$update) {
            foreach ($zohoBeans as $uid => $zohoBean) {
                $this->connection->update($tableName, ['id' => $zohoBean->getZohoId()], ['uid' => $uid]);
            }
        }
        $this->connection->executeUpdate('delete from '.$localTable.' where uid in ( :rowsDeleted)',
            [
            'rowsDeleted' => $rowsDeleted,
            ],
            [
            'rowsDeleted' => Connection::PARAM_INT_ARRAY,
            ]
        );
    }

    /**
     * Insert data to bean in order to insert zoho records.
     *
     * @param ZohoBeanInterface $zohoBean
     * @param array             $fieldsMatching
     * @param array             $row
     */
    private function insertDataZohoBean(ZohoBeanInterface $zohoBean, array $fieldsMatching, array $row)
    {
        foreach ($row as $columnName => $columnValue) {
            if ((!in_array($columnName, ['id', 'uid']) || isset($fieldsMatching[$columnName])) && !is_null($columnValue)) {
                $type = $fieldsMatching[$columnName]['type'];
                $value = $this->formatValueToBeans($type, $columnValue);
                $zohoBean->{$fieldsMatching[$columnName]['setter']}($value);
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
    private function updateDataZohoBean(ZohoBeanInterface $zohoBean, array $fieldsMatching, $columnName, $valueDb)
    {
        if (!in_array($columnName, ['id', 'uid']) || (isset($fieldsMatching[$columnName]))) {
            $type = $fieldsMatching[$columnName]['type'];
            $value = is_null($valueDb) ? $valueDb : $this->formatValueToBeans($type, $valueDb);
            $zohoBean->{$fieldsMatching[$columnName]['setter']}($value);
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
            case 'Date' :
                $value = \DateTime::createFromFormat('Y-m-d', $value);
                break;
            case 'DateTime':
                $value = \DateTime::createFromFormat('Y-m-d H:i:s', $value);
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

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }
}
