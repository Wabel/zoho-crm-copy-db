<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Psr\Log\NullLogger;
use TestNamespace\Contact;
use TestNamespace\ContactZohoDao;
use Wabel\Zoho\CRM\Service\EntitiesGeneratorService;
use Wabel\Zoho\CRM\ZohoClient;

class ZohoDatabaseSyncZohoTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Connection
     */
    protected $dbConnection;


    protected function setUp()
    {
        $config = new \Doctrine\DBAL\Configuration();
        $connectionParams = array(
            'user' => $GLOBALS['db_username'],
            'password' => $GLOBALS['db_password'],
            'host' => $GLOBALS['db_host'],
            'port' => $GLOBALS['db_port'],
            'driver' => $GLOBALS['db_driver'],
            'dbname' => $GLOBALS['db_name'],
        );
        $this->dbConnection = DriverManager::getConnection($connectionParams, $config);
    }

    public function getZohoClient()
    {
        return new ZohoClient($GLOBALS['auth_token']);
    }

    /**
     * @depends Wabel\Zoho\CRM\Copy\ZohoDatabaseCopierTest::testSync
     */
    public function testSync()
    {
        $contactZohoDao = new ContactZohoDao($this->getZohoClient());
        $zohoZync= new ZohoDatabaseSyncZoho($this->dbConnection);
        $tableName = 'zoho_contacts';
        $this->assertTrue($this->dbConnection->getSchemaManager()->tablesExist($tableName));
        $data = [
            'firstName' => 'TestZohoSync',
            'lastName' => uniqid('Test'),
        ];
        $data['email'] = $data['lastName'].'@test.com';
        $this->dbConnection->insert($tableName, $data);
        $statement = $this->dbConnection->createQueryBuilder();
        $statement->select('zcrm.*')
        ->from('local_insert', 'l')
        ->join('l', $tableName, 'zcrm', 'zcrm.uid = l.uid')
        ->where('l.table_name=:table_name')
        ->setParameters([
            'table_name' => $tableName
        ]);
        $result = $statement->execute()->fetchAll();
        $this->assertNotFalse($result);
        $zohoZync->pushInsertedRows($contactZohoDao);
        $resultContact = $this->dbConnection->fetchAssoc('SELECT * FROM '.$tableName.' WHERE uid = :uid', ['uid' => $result[0]['uid']]);
        $this->assertNotFalse($resultContact);
        $this->assertNotNull($resultContact['id']);

    }

    protected function tearDown()
    {
    }
}
