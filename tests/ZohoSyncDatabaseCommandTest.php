<?php

namespace Wabel\Zoho\CRM\Copy;

use Symfony\Component\Console\Application;
use Symfony\Component\Console\Tester\CommandTester;
use TestNamespace\ContactZohoDao;
use Wabel\Zoho\CRM\ZohoClient;
use Prophecy\Argument;
use Symfony\Component\Console\Logger\ConsoleLogger;

class ZohoSyncDatabaseCommandTest extends \PHPUnit_Framework_TestCase
{
    private function getZohoClient()
    {
        return new ZohoClient($GLOBALS['auth_token']);
    }

    /**
     * @depends Wabel\Zoho\CRM\Copy\ZohoDatabaseModelSyncTest::testModelSync
     */
    public function testExecute()
    {
        $contactZohoDao = new ContactZohoDao($this->getZohoClient());

        $syncModel = $this->prophesize(ZohoDatabaseModelSync::class);
        $syncModel->setLogger(Argument::type(ConsoleLogger::class))->shouldBeCalled();
        $syncModel->synchronizeDbModel(Argument::type(ContactZohoDao::class), true, false)->shouldBeCalled();

        $dbCopier = $this->prophesize(ZohoDatabaseCopier::class);
        $dbCopier->setLogger(Argument::type(ConsoleLogger::class))->shouldBeCalled();
        $dbCopier->fetchFromZoho(Argument::type(ContactZohoDao::class), true, true)->shouldBeCalled();

        $pusher = $this->prophesize(ZohoDatabasePusher::class);
        $pusher->setLogger(Argument::type(ConsoleLogger::class))->shouldBeCalled();
        $pusher->pushToZoho(Argument::type(ContactZohoDao::class))->shouldBeCalled();

        $application = new Application();
        $application->add(new ZohoSyncDatabaseCommand($syncModel->reveal(), $dbCopier->reveal(), $pusher->reveal(), [
            $contactZohoDao,
        ]));

        $command = $application->find('zoho:sync');
        $commandTester = new CommandTester($command);
        $commandTester->execute(array('command' => $command->getName()));
    }

    /**
     * @depends Wabel\Zoho\CRM\Copy\ZohoDatabaseModelSyncTest::testModelSync
     */
    public function testException()
    {
        $contactZohoDao = new ContactZohoDao($this->getZohoClient());

        $syncModel = $this->prophesize(ZohoDatabaseModelSync::class);
        $dbCopier = $this->prophesize(ZohoDatabaseCopier::class);
        $pusher = $this->prophesize(ZohoDatabasePusher::class);

        $application = new Application();
        $application->add(new ZohoSyncDatabaseCommand($syncModel->reveal(), $dbCopier->reveal(), $pusher->reveal(), [
            $contactZohoDao,
        ]));

        $command = $application->find('zoho:sync');
        $commandTester = new CommandTester($command);
        $commandTester->execute(array('command' => $command->getName(), '--fetch-only' => true, '--push-only' => true));

        $this->assertRegExp('/Options fetch-only and push-only are mutually exclusive/', $commandTester->getDisplay());
    }
}
