<?php

namespace Wabel\Zoho\CRM\Copy;

use Mouf\Utils\Common\Lock;
use Mouf\Utils\Common\LockException;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Logger\ConsoleLogger;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Input\InputOption;

use Wabel\Zoho\CRM\AbstractZohoDao;

class ZohoSyncDatabaseCommand extends Command
{
    /**
     * The list of Zoho DAOs to copy.
     *
     * @var AbstractZohoDao[]
     */
    private $zohoDaos;

    /**
     * @var ZohoDatabaseSyncZoho
     */
    private $zohoDatabaseSync;

    /**
     * @var Lock
     */
    private $lock;

    /**
     * @param ZohoDatabaseCopier                $zohoDatabaseCopier
     * @param \Wabel\Zoho\CRM\AbstractZohoDao[] $zohoDaos           The list of Zoho DAOs to copy
     * @param Lock                              $lock               A lock that can be used to avoid running the same command twice at the same time
     */
    public function __construct(ZohoDatabaseSyncZoho $zohoDatabaseSync, array $zohoDaos, Lock $lock = null)
    {
        parent::__construct();
        $this->zohoDatabaseSync = $zohoDatabaseSync;
        $this->zohoDaos = $zohoDaos;
        $this->lock = $lock;
    }

    protected function configure()
    {
        $this
            ->setName('zoho:sync-db')
            ->setDescription('Synchronize Zoho CRM from the Zoho database');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        try {
            if ($this->lock) {
                $this->lock->acquireLock();
            }
            $twoWaysSync = true;
            $this->zohoDatabaseSync->setLogger(new ConsoleLogger($output));

            $output->writeln('Starting synchronize Zoho data into Zoho CRM.');
            foreach ($this->zohoDaos as $zohoDao) {
//                $output->writeln(sprintf('Synchronizing data using <info>%s</info>', get_class($zohoDao)));
                $output->writeln(sprintf(' > Insert new rows using <info>%s</info>', get_class($zohoDao)));
                $this->zohoDatabaseSync->pushInsertedRows($zohoDao);
                $output->writeln(sprintf(' > Update rows using <info>%s</info>', get_class($zohoDao)));
                $this->zohoDatabaseSync->pushUpdatedRows($zohoDao);
                $output->writeln(sprintf(' > Delete rows using <info>%s</info>', get_class($zohoDao)));
                $this->zohoDatabaseSync->deleteDataToZoho($zohoDao);
            }
            $output->writeln('Zoho data successfully synchronized.');
            if ($this->lock) {
                $this->lock->releaseLock();
            }
        } catch (LockException $e) {
            $output->writeln('<error>Could not start zoho:sync-db command. Another zoho:sync-db command is already running.</error>');
        }
    }
}
