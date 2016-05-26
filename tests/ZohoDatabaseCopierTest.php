<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use TestNamespace\Contact;
use TestNamespace\ContactZohoDao;
use Wabel\Zoho\CRM\ZohoClient;

class ZohoDatabaseCopierTest extends \PHPUnit_Framework_TestCase
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
     * @depends Wabel\Zoho\CRM\Copy\ZohoDatabaseModelSyncTest::testModelSync
     */
    public function testFetch()
    {
        $listener = new TestListener();

        $contactZohoDao = new ContactZohoDao($this->getZohoClient());

        // Let's add a single user:
        $testContact = new Contact();
        $testContact->setLastName(uniqid('Test'));
        $testContact->setFirstName('TestZohoCopier');
        $testContact->setEmail($testContact->getLastName().'@test.com');
        $contactZohoDao->save($testContact);

        $databaseCopier = new ZohoDatabaseCopier($this->dbConnection, 'zoho_', [$listener]);

        $databaseCopier->fetchFromZoho($contactZohoDao);

        $this->assertTrue($listener->isInsertCalled());

        $this->assertTrue($this->dbConnection->getSchemaManager()->tablesExist('zoho_contacts'));
        $result = $this->dbConnection->fetchAssoc('SELECT * FROM zoho_contacts WHERE id = :id', ['id' => $testContact->getZohoId()]);
        $this->assertNotFalse($result);
        $this->assertEquals($testContact->getLastName(), $result['lastName']);

        // Now, let's trigger an update:
        $testContact->setLastName(uniqid('Test2'));
        $contactZohoDao->save($testContact);

        $databaseCopier->fetchFromZoho($contactZohoDao);
        $result = $this->dbConnection->fetchAssoc('SELECT * FROM zoho_contacts WHERE id = :id', ['id' => $testContact->getZohoId()]);
        $this->assertNotFalse($result);
        $this->assertEquals($testContact->getLastName(), $result['lastName']);

        $this->assertTrue($listener->isUpdateCalled());

        $contactZohoDao->delete($testContact->getZohoId());
    }

    protected function tearDown()
    {
    }
}
