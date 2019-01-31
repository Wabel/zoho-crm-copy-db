<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use TestNamespace\ContactZohoDao;
use Wabel\Zoho\CRM\ZohoClient;
use Wabel\Zoho\CRM\Exception\ZohoCRMResponseException;

class ZohoDatabasePusherTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Connection
     */
    protected $dbConnection;

    /**
     * @var ZohoClient
     */
    private $zohoClient;

    protected function setUp()
    {
        $this->zohoClient  = new ZohoClient(
            [
                'client_id' => getenv('client_id'),
                'client_secret' => getenv('client_secret'),
                'redirect_uri' => getenv('redirect_uri'),
                'currentUserEmail' => getenv('currentUserEmail'),
                'applicationLogFilePath' => getenv('applicationLogFilePath'),
                'persistence_handler_class' => getenv('persistence_handler_class'),
                'token_persistence_path' => getenv('token_persistence_path'),
            ]
        );
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

    /**
     * @depends Wabel\Zoho\CRM\Copy\ZohoDatabaseCopierTest::testFetch
     */
    public function testSync()
    {
        $contactZohoDao = new ContactZohoDao($this->zohoClient);
        $zohoZync = new ZohoDatabasePusher($this->dbConnection);
        $tableName = 'zoho_contacts';
        $this->assertTrue($this->dbConnection->getSchemaManager()->tablesExist($tableName));
        // Test insert
        $data = [
            'firstName' => $this->randName(),
            'lastName' => $this->randName(),
            'dateOfBirth' => date('Y-m-d'),
        ];
        $data['email'] = $data['lastName'].'@test.com';
        $this->dbConnection->insert($tableName, $data);
        $statementTestInsert = $this->dbConnection->createQueryBuilder();
        $statementTestInsert->select('zcrm.*')
        ->from('local_insert', 'l')
        ->join('l', $tableName, 'zcrm', 'zcrm.uid = l.uid')
        ->where('l.table_name=:table_name')
        ->setParameters([
            'table_name' => $tableName,
        ]);
        $resultInsertion = $statementTestInsert->execute()->fetchAll();
        $this->assertNotFalse($resultInsertion);
        $zohoZync->pushInsertedRows($contactZohoDao);
        $resultContactInserted = $this->dbConnection->fetchAssoc('SELECT * FROM '.$tableName.' WHERE uid = :uid', ['uid' => $resultInsertion[0]['uid']]);
        $this->assertNotFalse($resultContactInserted);
        $this->assertNotNull($resultContactInserted['id']);

        //Test update
        $dataUpdate = [
            'firstName' => 'TestZohoSyncUpdated',
            'lastName' => uniqid('TestUpdated'),
        ];
        $this->dbConnection->update($tableName, $dataUpdate, ['id' => $resultContactInserted['id']]);
        $statementTestUpdate = $this->dbConnection->createQueryBuilder();
        $statementTestUpdate->select('l.field_name as updated_fieldname')
        ->from('local_update', 'l')
        ->join('l', $tableName, 'zcrm', 'zcrm.uid = l.uid')
        ->where('l.table_name=:table_name')
        ->andWhere('l.uid = :uid')
        ->setParameters([
            'table_name' => $tableName,
            'uid' => $resultInsertion[0]['uid'],
        ]);
        $resultUpdate = $statementTestUpdate->execute()->fetchAll();
        $this->assertNotFalse($resultUpdate);
        $this->assertCount(2, $resultUpdate);
        $zohoZync->pushUpdatedRows($contactZohoDao);
        sleep(60);
        $contactZohoUpdate = $contactZohoDao->getById($resultContactInserted['id']);
        $this->assertEquals($dataUpdate['firstName'], $contactZohoUpdate->getFirstName());
        $this->assertEquals($dataUpdate['lastName'], $contactZohoUpdate->getLastName());

        // Delete
        $this->dbConnection->delete($tableName, ['uid' => $resultInsertion[0]['uid']]);
        $statementTestDelete = $this->dbConnection->createQueryBuilder();
        $statementTestDelete->select('l.*')
        ->from('local_delete', 'l')
        ->where('l.table_name=:table_name')
        ->andWhere('l.id = :id')
        ->setParameters([
            'table_name' => $tableName,
            'id' => $resultContactInserted['id'],
        ]);
        $resultDelete = $statementTestDelete->execute()->fetchAll();
        $this->assertNotFalse($resultDelete);
        $zohoZync->pushDeletedRows($contactZohoDao);
        try {
            $recordSearch = $contactZohoDao->getById($resultContactInserted['id']);
            // It returns an empty array when it found nothing.
            $isRecordSearch = (empty($recordSearch)) ? false : true;
        } catch (ZohoCRMResponseException $ex) {
            $isRecordSearch = null;
        }
        $this->assertFalse($isRecordSearch);
    }

    private function randName()
    {
        $characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';
        $result = '';
        for ($i = 0; $i < 10; $i++) {
            $result .= $characters[mt_rand(0, 51)];
        }
        return $result;
    }

    protected function tearDown()
    {
    }
}
