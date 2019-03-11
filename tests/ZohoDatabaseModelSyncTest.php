<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use TestNamespace\Contact;
use TestNamespace\ContactZohoDao;
use Wabel\Zoho\CRM\Service\EntitiesGeneratorService;
use Wabel\Zoho\CRM\ZohoClient;

class ZohoDatabaseModelSyncTest extends TestCase
{
    /**
     * @var Connection
     */
    protected $dbConnection;

    public static function setUpBeforeClass()
    {
        $config = new \Doctrine\DBAL\Configuration();
        $connectionParams = array(
            'user' => getenv('db_username'),
            'password' => getenv('db_password'),
            'host' => getenv('db_host'),
            'port' => getenv('db_port'),
            'driver' => getenv('db_driver'),
            'dbname' => getenv('db_name'),
        );
        $adminConn = DriverManager::getConnection($connectionParams, $config);
        $adminConn->getSchemaManager()->dropAndCreateDatabase(getenv('db_name'));
    }

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

    public function getEntitiesGeneratorService()
    {
        return new EntitiesGeneratorService($this->zohoClient, new NullLogger());
    }

    public function testModelSync()
    {
        $generator = $this->getEntitiesGeneratorService();
        $generator->generateModule('Contacts', 'Contacts', 'Contact', __DIR__.'/generated/', 'TestNamespace');

        include __DIR__.'/generated/Contact.php';
        include __DIR__.'/generated/ContactZohoDao.php';

        $contactZohoDao = new ContactZohoDao($this->zohoClient);

        $databaseModelSync = new ZohoDatabaseModelSync($this->dbConnection, $this->getZohoUserService(), 'zoho_');

        $databaseModelSync->synchronizeDbModel($contactZohoDao, true);

        $this->assertTrue($this->dbConnection->getSchemaManager()->tablesExist('zoho_contacts'));
    }

    protected function tearDown()
    {
    }
}
