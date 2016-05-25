<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;
use Wabel\Zoho\CRM\ZohoBeanInterface;
use Wabel\Zoho\CRM\Exception\ZohoCRMException;

/**
 * Description of ZohoDatabaseSyncZoho
 *
 * @author rbergina
 */
class ZohoDatabaseSyncZoho
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
     *
     * @var string
     */
    private $prefix;

    /**
     * @param Connection $connection
     * @param string $prefix
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
     *
     * @param AbstractZohoDao $zohoDao
     * @return array
     */
    private function findMethodValues(AbstractZohoDao $zohoDao){
        $fieldsMatching = array();
        foreach ($zohoDao->getFields() as $fieldsDescriptor) {
            foreach (array_values($fieldsDescriptor) as $fieldDescriptor) {
                $fieldsMatching[$fieldDescriptor['name']] = [
                    'setter' => $fieldDescriptor['setter'],
                    'type' => $fieldDescriptor['type']
                ];
            }

        }
        return $fieldsMatching;
    }

    /**
     * Insert or Update rows.
     * @param AbstractZohoDao $zohoDao
     * @param string $localTable
     */
    public function pushDataToZoho(AbstractZohoDao $zohoDao, $localTable, $update = false){

            $fieldsMatching = $this->findMethodValues($zohoDao);
            $tableName = ZohoDatabaseHelper::getTableName($zohoDao,$this->prefix);
            $rowsDeleted = [];
            $statement = $this->connection->createQueryBuilder();
            $statement->select('zcrm.*');
            if($update){
                $statement->addSelect('l.field_name as updated_fieldname');
            }
            $statement->from($localTable, 'l')
            ->join('l', $tableName, 'zcrm', 'zcrm.uid = l.uid')
            ->where('l.table_name=:table_name')
            ->setParameters([
                'table_name' => $tableName
            ]);
            $results = $statement->execute();
            /* @var $zohoBeans ZohoBeanInterface[] */
            $zohoBeans = array();
            while ($row = $results->fetch()) {
                $beanClassName = $zohoDao->getBeanClassName();
                /* @var $zohoBean ZohoBeanInterface */
                if(isset($zohoBeans[$row['uid']])){
                    $zohoBean = $zohoBeans[$row['uid']];
                }else{
                    $zohoBean = new $beanClassName();
                }
                if(!$update){
                    foreach ($row as $columnName => $columnValue) {
                        if (!in_array($columnName,['id','uid']) || isset($fieldsMatching[$columnName])) {
                            $value = $this->formatValueToBeans($zohoDao->getModule(), $fieldsMatching, $columnName, $columnValue, null, $row['uid']);
                           if($columnValue){
                               $zohoBean->{$fieldsMatching[$columnName]['setter']}($value);
                           }
                        }
                    }
                    $zohoBeans[$row['uid']] =  $zohoBean;
                    $rowsDeleted[] = $row['uid'];
                } else{
                    $columnName = $row['updated_fieldname'];
                    $zohoBean->setZohoId($row['id']);
                    if (!in_array($columnName,['id','uid']) || isset($fieldsMatching[$columnName])) {
                        $value = $this->formatValueToBeans($zohoDao->getModule(), $fieldsMatching, $columnName, $row[$columnName], $row['id']);
                        $zohoBean->{$fieldsMatching[$columnName]['setter']}($value);
                        $zohoBeans[$row['uid']] = $zohoBean;
                        $rowsDeleted[] = $row['uid'];
                    }
                }
            }
            $zohoDao->save($zohoBeans);
            if(!$update){
                foreach ($zohoBeans as $uid => $zohoBean) {
                    $this->connection->update($tableName, [ 'id'=>$zohoBean->getZohoId(),'lastActivityTime'=> date("Y-m-d H:i:s") ], ['uid'=>$uid ]);
                }
            }
            $statementDelete = $this->connection->prepare('delete from '.$localTable.' where uid in ( :rowsDeleted)');
            $statementDelete->execute([
                'rowsDeleted' => implode(',', $rowsDeleted)
            ]);
    }

    /**
     * Change the value to the good format.
     * @param string $moduleName
     * @param array $fieldsMatching
     * @param string $columnName
     * @param mixed $value
     * @param int $id
     * @return mixed
     * @throws ZohoCRMException
     */
    private function formatValueToBeans($moduleName, $fieldsMatching,$columnName,$value,$id=null, $uid = null)
    {
        $idrecord = $id?$id.'[ZOHO]':$uid.'[UID]';
        if(isset($fieldsMatching[$columnName]) && $value){
            switch ($fieldsMatching[$columnName]['type']) {
                case 'Date':
                    if ($dateObj = \DateTime::createFromFormat('M/d/Y', $value)) {
                        $value = $dateObj;
                    } elseif ($dateObj = \DateTime::createFromFormat('Y-m-d', $value)) {
                        $value = $dateObj;
                    } else {
                        throw new ZohoCRMException('Unable to convert the Date field "'.$columnName."\" into a DateTime PHP object from the the record $idrecord of the module ".$moduleName.'.');
                    }
                    break;
                case 'DateTime':
                    $value = \DateTime::createFromFormat('Y-m-d H:i:s', $value);
                    break;
                default:
                    break;
            }
        }
        return $value;
    }

    /**
     * Find the row to delete and remove to Zoho.
     * @param AbstractZohoDao $zohoDao
     * @param string $localTable
     */
    public function deleteDataToZoho(AbstractZohoDao $zohoDao, $localTable){
        $tableName = ZohoDatabaseHelper::getTableName($zohoDao,$this->prefix);
        $statement = $this->connection->createQueryBuilder();
        $statement->select('l.id')
        ->from($localTable, 'l')
        ->where('l.table_name=:table_name')
        ->setParameters([
            'table_name' => $tableName
        ]);
        $results = $statement->execute();
        while ($row = $results->fetch()) {
            $zohoDao->delete($row['id']);
            $this->connection->delete($localTable, ['table_name' => $tableName,'id' => $row['id']]);
        }
}
    
    /**
     * Run inserted rows to Zoho : local_insert.
     * @param AbstractZohoDao $zohoDao
     */
    public function pushInsertedRows(AbstractZohoDao $zohoDao){
        return $this->pushDataToZoho($zohoDao, 'local_insert');
    }

    /**
     * Run updated rows to Zoho : local_update.
     * @param AbstractZohoDao $zohoDao
     */
    public function pushUpdatedRows(AbstractZohoDao $zohoDao){
        $this->pushDataToZoho($zohoDao, 'local_update', true);
    }

    /**
     * Run deleted rows to Zoho : local_delete.
     * @param AbstractZohoDao $zohoDao
     */
    public function pushDeletedRows(AbstractZohoDao $zohoDao){
        $this->deleteDataToZoho($zohoDao, 'local_delete');
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }


}