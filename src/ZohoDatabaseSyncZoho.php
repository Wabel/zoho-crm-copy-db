<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Wabel\Zoho\CRM\AbstractZohoDao;
use Wabel\Zoho\CRM\ZohoBeanInterface;
use Wabel\Zoho\CRM\Exception\ZohoCRMException;
use Wabel\Zoho\CRM\Exception\ZohoCRMResponseException;
use function Stringy\create as s;

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
                    'setter' => $fieldDescriptor['setter']
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
            $tableName = $this->getTableName($zohoDao);
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
                $zohoBean = new $beanClassName();
                if(!$update){
                    foreach ($row as $columnName => $columnValue) {
                        if (in_array($columnName,['id','uid'])) {
                            continue;
                        }else{
                           if($columnValue){
                               $zohoBean->{$fieldsMatching[$columnName]['setter']}($columnValue);
                           }
                        }

                    }
                    $zohoBeans[$row['uid']] =  $zohoBean;
                    $rowsDeleted[] = $row['uid'];
                } else{
                    $columnName = $row['updated_fieldname'];
                    $zohoBean->setZohoId($row['id']);
                    if (in_array($columnName,['uid'])) {
                        continue;
                    }else{
                        $zohoBean->{$fieldsMatching[$columnName]['setter']}($row[$columnName]);
                        $zohoBeans[] = $zohoBean;
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
     * Find the row to delete and remove to Zoho.
     * @param AbstractZohoDao $zohoDao
     * @param string $localTable
     */
    public function deleteDataToZoho(AbstractZohoDao $zohoDao, $localTable){
        $tableName = $this->getTableName($zohoDao);
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
     * Computes the name of the table based on the DAO plural module name.
     *
     * @param AbstractZohoDao $dao
     *
     * @return string
     */
    private function getTableName(AbstractZohoDao $dao)
    {
        $tableName = $this->prefix.$dao->getPluralModuleName();
        $tableName = s($tableName)->upperCamelize()->underscored();

        return (string) $tableName;
    }


    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }


}