<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Psr\Log\NullLogger;
use TestNamespace\Contact;
use TestNamespace\ContactZohoDao;
use Wabel\Zoho\CRM\Service\EntitiesGeneratorService;
use Wabel\Zoho\CRM\ZohoClient;

class ZohoDatabaseModelSyncTest extends \PHPUnit_Framework_TestCase
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

        require __DIR__.'/generated/Contact.php';
        require __DIR__.'/generated/ContactZohoDao.php';

        $contactZohoDao = new ContactZohoDao($this->zohoClient);

        $databaseModelSync = new ZohoDatabaseModelSync($this->dbConnection, $this->getZohoUserService(), 'zoho_');

        $databaseModelSync->synchronizeDbModel($contactZohoDao, true);

        $this->assertTrue($this->dbConnection->getSchemaManager()->tablesExist('zoho_contacts'));
    }

    protected function tearDown()
    {
    }
}
