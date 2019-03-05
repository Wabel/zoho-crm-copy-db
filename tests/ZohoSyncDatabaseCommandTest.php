<?php

namespace Wabel\Zoho\CRM\Copy;

use Symfony\Component\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;
use Wabel\Zoho\CRM\AbstractZohoDao;
use Wabel\Zoho\CRM\ZohoClient;
use Prophecy\Argument;
use Wabel\Zoho\CRM\Service\EntitiesGeneratorService;
use Psr\Log\NullLogger;

class ZohoSyncDatabaseCommandTest extends \PHPUnit_Framework_TestCase
{
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
    }

    public function getEntitiesGeneratorService()
    {
        return new EntitiesGeneratorService($this->zohoClient, new NullLogger());
    }

    /**
     * Create Module and load them to use after.
     * @param EntitiesGeneratorService $generator
     */
    private function loadFilesZohoDaos(EntitiesGeneratorService $generator)
    {
        // Create Module and load them to use after.
        $generator->generateAll(__DIR__.'/generated/', 'TestNamespace');
        foreach (scandir(__DIR__.'/generated/') as $filename) {
            $path = __DIR__.'/generated/'.$filename;
            if (is_file($path)) {
                require_once $path;
            }
        }
    }

    /**
     * @depends Wabel\Zoho\CRM\Copy\ZohoDatabaseModelSyncTest::testModelSync
     */
    public function testExecute()
    {
        $generator = $this->getEntitiesGeneratorService();
        $this->loadFilesZohoDaos($generator);

        $syncModel = $this->prophesize(ZohoDatabaseModelSync::class);
        $syncModel->synchronizeDbModel(Argument::type(AbstractZohoDao::class), true, false)->shouldBeCalled();
        $syncModel->synchronizeUserDbModel()->shouldBeCalled();

        $dbCopier = $this->prophesize(ZohoDatabaseCopier::class);
        $dbCopier->fetchFromZoho(Argument::type(AbstractZohoDao::class), true, true)->shouldBeCalled();
        $dbCopier->fetchUserFromZoho()->shouldBeCalled();

        $pusher = $this->prophesize(ZohoDatabasePusher::class);
        $pusher->pushToZoho(Argument::type(AbstractZohoDao::class))->shouldBeCalled();

        $logger = new \Mouf\Utils\Log\Psr\MultiLogger();
        $application = new Application();
        $application->add(new ZohoSyncDatabaseCommand($syncModel->reveal(), $dbCopier->reveal(), $pusher->reveal(),
            $generator, $this->zohoClient, __DIR__.'/generated/', 'TestNamespace', $logger));
        
        $command = $application->find('zoho:sync');
        $commandTester = new CommandTester($command);
        $commandTester->execute(array('command' => $command->getName()));
    }

    /**
     * @depends Wabel\Zoho\CRM\Copy\ZohoDatabaseModelSyncTest::testModelSync
     */
    public function testException()
    {
        $generator = $this->getEntitiesGeneratorService();
        $this->loadFilesZohoDaos($generator);
        $syncModel = $this->prophesize(ZohoDatabaseModelSync::class);
        $dbCopier = $this->prophesize(ZohoDatabaseCopier::class);
        $pusher = $this->prophesize(ZohoDatabasePusher::class);

        $logger = new \Mouf\Utils\Log\Psr\MultiLogger();
        $application = new Application();
        $application->add(new ZohoSyncDatabaseCommand($syncModel->reveal(), $dbCopier->reveal(), $pusher->reveal(),
            $generator, $this->zohoClient, __DIR__.'/generated/', 'TestNamespace', $logger));

        $command = $application->find('zoho:sync');
        $commandTester = new CommandTester($command);
        $commandTester->execute(array('command' => $command->getName(), '--fetch-only' => true, '--push-only' => true));

        $this->assertRegExp('/Options fetch-only and push-only are mutually exclusive/', $commandTester->getDisplay());
    }
}
