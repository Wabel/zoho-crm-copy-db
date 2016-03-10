<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Psr\Log\NullLogger;
use TestNamespace\Contact;
use TestNamespace\ContactZohoDao;
use Wabel\Zoho\CRM\Service\EntitiesGeneratorService;
use Wabel\Zoho\CRM\ZohoClient;

class ZohoDatabaseCopierTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Connection
     */
    protected $dbConnection;

    public static function setUpBeforeClass()
    {
        $config = new \Doctrine\DBAL\Configuration();
        $connectionParams = array(
            'user' => $GLOBALS['db_username'],
            'password' => $GLOBALS['db_password'],
            'host' => $GLOBALS['db_host'],
            'port' => $GLOBALS['db_port'],
            'driver' => $GLOBALS['db_driver'],
        );
        $adminConn = DriverManager::getConnection($connectionParams, $config);
        $adminConn->getSchemaManager()->dropAndCreateDatabase($GLOBALS['db_name']);
    }

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

    public function getEntitiesGeneratorService()
    {
        return new EntitiesGeneratorService($this->getZohoClient(), new NullLogger());
    }

    public function testSync()
    {
        $listener = new TestListener();

        $generator = $this->getEntitiesGeneratorService();
        $generator->generateModule('Contacts', 'Contacts', 'Contact', __DIR__.'/generated/', 'TestNamespace');

        require __DIR__.'/generated/Contact.php';
        require __DIR__.'/generated/ContactZohoDao.php';

        $contactZohoDao = new ContactZohoDao($this->getZohoClient());

        // Let's add a single user:
        $testContact = new Contact();
        $testContact->setLastName(uniqid('Test'));
        $testContact->setFirstName('TestZohoCopier');
        $testContact->setEmail($testContact->getLastName().'@test.com');
        $contactZohoDao->save($testContact);

        $databaseCopier = new ZohoDatabaseCopier($this->dbConnection, 'zoho_', [$listener]);

        $databaseCopier->copy($contactZohoDao);

        $this->assertTrue($listener->isInsertCalled());

        $this->assertTrue($this->dbConnection->getSchemaManager()->tablesExist('zoho_contacts'));
        $result = $this->dbConnection->fetchAssoc('SELECT * FROM zoho_contacts WHERE id = :id', ['id' => $testContact->getZohoId()]);
        $this->assertNotFalse($result);
        $this->assertEquals($testContact->getLastName(), $result['lastName']);

        // Now, let's trigger an update:
        $testContact->setLastName(uniqid('Test2'));
        $contactZohoDao->save($testContact);

        $databaseCopier->copy($contactZohoDao);
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
