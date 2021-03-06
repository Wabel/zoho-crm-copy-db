<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\TestCase;
use TestNamespace\Contact;
use TestNamespace\ContactZohoDao;
use Wabel\Zoho\CRM\ZohoClient;

class ZohoDatabaseCopierTest extends TestCase
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
            ],
            getenv('timeZone')
        );

        $config = new \Doctrine\DBAL\Configuration();
        $connectionParams = array(
            'user' => getenv('db_username'),
            'password' => getenv('db_password'),
            'host' => getenv('db_host'),
            'port' => getenv('db_port'),
            'driver' => getenv('db_driver'),
            'dbname' => getenv('db_name'),
        );
        $this->dbConnection = DriverManager::getConnection($connectionParams, $config);
    }

    public function getZohoUserService()
    {
        return new ZohoUserService($this->zohoClient);
    }

    /**
     * @depends Wabel\Zoho\CRM\Copy\ZohoDatabaseModelSyncTest::testModelSync
     */
    public function testFetch()
    {
        if(!class_exists('Wabel\Zoho\CRM\Copy\TestListener')) {
            include_once 'TestListener.php';
        }
        $listener = new TestListener();

        $contactZohoDao = new ContactZohoDao($this->zohoClient);

        // Let's add a single user:
        $testContact = new Contact();
        $testContact->setLastName(uniqid('Test'));
        $testContact->setFirstName('TestZohoCopier');
        $testContact->setEmail($testContact->getLastName().'@test.com');
        $contactZohoDao->save($testContact);

        $databaseCopier = new ZohoDatabaseCopier($this->dbConnection, $this->getZohoUserService(), 'zoho_', [$listener]);

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
