<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Psr\Log\LoggerInterface;
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
     * @param \Wabel\Zoho\CRM\AbstractZohoDao[] $zohoDaos           The list of Zoho DAOs to copy
     * @param Connection $connection
     *
     */
    public function __construct(Connection $connection, LoggerInterface $logger)
    {
        $this->connection = $connection;
        $this->logger = $logger;
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
     *
     * @param AbstractZohoDao $zohoDao
     * @param string $localTable
     */
    public function pushDataToZoho(AbstractZohoDao $zohoDao, $localTable){

            $fieldsMatching = $this->findMethodValues($zohoDao);
            $tableName = $this->getTableName($zohoDao);
            $statement = $this->connection->createQueryBuilder();
            $statement->select('zcrm.*')
            ->from($localTable, 'l')
            ->join('l', $tableName, 'zcrm', 'zcrm.id = l.id')
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
                foreach ($row as $columnName => $columnValue) {
                    $zohoBean->{$fieldsMatching[$columnName]}($columnValue);
                }
                $zohoBeans[] =  $zohoBean;
            }
            try{
              $zohoDao->save($zohoBeans);  
            } catch (ZohoCRMException $ex) {
                $this->logger->error($ex->getMessage());
            }
    }

    /**
     *
     * @param AbstractZohoDao $zohoDao
     * @param string $localTable
     */
    public function deleteDataToZoho(AbstractZohoDao $zohoDao, $localTable){
        $tableName = $this->getTableName($zohoDao);
        $statement = $this->connection->createQueryBuilder();
        $statement->select('zcrm.id')
        ->from($localTable, 'l')
        ->join('l', $tableName, 'zcrm', 'zcrm.id = l.id')
        ->where('l.table_name=:table_name')
        ->setParameters([
            'table_name' => $tableName
        ]);
        $results = $statement->execute();
        while ($row = $results->fetch()) {
            try{
                $zohoDao->delete($row['id']);
            } catch (ZohoCRMResponseException $ex) {
                $this->logger->error($ex->getMessage());
            }

        }
}
    
    /**
     *
     * @param AbstractZohoDao $zohoDao
     */
    public function pushInsertedRows(AbstractZohoDao $zohoDao){
        $this->pushDataToZoho($zohoDao, 'local_insert');
    }

    /**
     *
     * @param AbstractZohoDao $zohoDao
     */
    public function pushUpdatedRows(AbstractZohoDao $zohoDao){
        $this->pushDataToZoho($zohoDao, 'local_update');
    }

    /**
     * 
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