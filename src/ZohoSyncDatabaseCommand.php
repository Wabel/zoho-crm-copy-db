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
     * @var ZohoDatabaseModelSync
     */
    private $zohoDatabaseModelSync;

    /**
     * @var ZohoDatabaseCopier
     */
    private $zohoDatabaseCopier;

    /**
     * @var ZohoDatabasePusher
     */
    private $zohoDatabaseSync;

    /**
     * @var Lock
     */
    private $lock;

    /**
     * @param ZohoDatabaseModelSync $zohoDatabaseModelSync
     * @param ZohoDatabaseCopier    $zohoDatabaseCopier
     * @param ZohoDatabasePusher    $zohoDatabaseSync
     * @param array                 $zohoDaos              The list of Zoho DAOs to copy
     * @param Lock                  $lock                  A lock that can be used to avoid running the same command (copy) twice at the same time
     */
    public function __construct(ZohoDatabaseModelSync $zohoDatabaseModelSync, ZohoDatabaseCopier $zohoDatabaseCopier, ZohoDatabasePusher $zohoDatabaseSync, array $zohoDaos, Lock $lock = null)
    {
        parent::__construct();
        $this->zohoDatabaseModelSync = $zohoDatabaseModelSync;
        $this->zohoDatabaseCopier = $zohoDatabaseCopier;
        $this->zohoDatabaseSync = $zohoDatabaseSync;
        $this->zohoDaos = $zohoDaos;
        $this->lock = $lock;
    }

    protected function configure()
    {
        $this
            ->setName('zoho:sync')
            ->setDescription('Synchronize the Zoho CRM data in a local database.')
            ->addOption('reset', 'r', InputOption::VALUE_NONE, 'Get a fresh copy of Zoho (rather than doing incremental copy)')
            ->addOption('skip-trigger', 's', InputOption::VALUE_NONE, 'Do not create or update the trigger')
            ->addOption('fetch-only', 'f', InputOption::VALUE_NONE, 'Fetch only the Zoho data in local database')
            ->addOption('push-only', 'p', InputOption::VALUE_NONE, 'Push only the local data to Zoho');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        try {
            if ($this->lock) {
                $this->lock->acquireLock();
            }
            if ($input->getOption('fetch-only') && $input->getOption('push-only')) {
                $output->writeln('<error>Options fetch-only and push-only are mutually exclusive.</error>');
            }

            $this->syncModel($input, $output);

            if (!$input->getOption('push-only')) {
                $this->fetchDb($input, $output);
            }
            if (!$input->getOption('fetch-only')) {
                $this->pushDb($output);
            }
            if ($this->lock) {
                $this->lock->releaseLock();
            }
        } catch (LockException $e) {
            $output->writeln('<error>Could not start zoho:copy-db copy command. Another zoho:copy-db copy command is already running.</error>');
        }
    }

    /**
     * Sychronizes the model of the database with Zoho records.
     *
     * @param OutputInterface $output
     */
    private function syncModel(InputInterface $input, OutputInterface $output)
    {
        $this->zohoDatabaseModelSync->setLogger(new ConsoleLogger($output));

        $twoWaysSync = !$input->getOption('fetch-only');
        $skipCreateTrigger = $input->getOption('skip-trigger');

        $output->writeln('Starting synchronize Zoho data into Zoho CRM.');
        foreach ($this->zohoDaos as $zohoDao) {
            $this->zohoDatabaseModelSync->synchronizeDbModel($zohoDao, $twoWaysSync, $skipCreateTrigger);
        }
        $output->writeln('Zoho data successfully synchronized.');
    }

    /**
     * Run the fetch Db command.
     *
     * @param InputInterface  $input
     * @param OutputInterface $output
     */
    private function fetchDb(InputInterface $input, OutputInterface $output)
    {
        if ($input->getOption('reset')) {
            $incremental = false;
        } else {
            $incremental = true;
        }

        $twoWaysSync = !$input->getOption('fetch-only');
        $this->zohoDatabaseCopier->setLogger(new ConsoleLogger($output));

        $output->writeln('Starting copying Zoho data into local database.');
        foreach ($this->zohoDaos as $zohoDao) {
            $output->writeln(sprintf('Copying data using <info>%s</info>', get_class($zohoDao)));
            $this->zohoDatabaseCopier->fetchFromZoho($zohoDao, $incremental, $twoWaysSync);
        }
        $output->writeln('Zoho data successfully copied.');
    }

    /**
     * Run the push Db command.
     *
     * @param OutputInterface $output
     */
    private function pushDb(OutputInterface $output)
    {
        $this->zohoDatabaseSync->setLogger(new ConsoleLogger($output));

        $output->writeln('Starting synchronize Zoho data into Zoho CRM.');
        foreach ($this->zohoDaos as $zohoDao) {
            $this->zohoDatabaseSync->pushToZoho($zohoDao);
        }
        $output->writeln('Zoho data successfully synchronized.');
    }
}
