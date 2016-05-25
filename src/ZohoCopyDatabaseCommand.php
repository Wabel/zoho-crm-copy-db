<?php

namespace Wabel\Zoho\CRM\Copy;

use Mouf\Utils\Common\Lock;
use Mouf\Utils\Common\LockException;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Logger\ConsoleLogger;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Exception\InvalidArgumentException;
use Wabel\Zoho\CRM\AbstractZohoDao;

class ZohoCopyDatabaseCommand extends Command
{
    /**
     * The list of Zoho DAOs to copy.
     *
     * @var AbstractZohoDao[]
     */
    private $zohoDaos;

    /**
     * @var ZohoDatabaseCopier
     */
    private $zohoDatabaseCopier;

    /**
     * @var ZohoDatabaseSyncZoho
     */
    private $zohoDatabaseSync;

    /**
     * @var Lock
     */
    private $lockCopy;
    
    /**
     * @var Lock
     */
    private $lockSync;

    /**
     * 
     * @param \Wabel\Zoho\CRM\Copy\ZohoDatabaseCopier $zohoDatabaseCopier
     * @param \Wabel\Zoho\CRM\Copy\ZohoDatabaseSyncZoho $zohoDatabaseSync
     * @param array $zohoDaos The list of Zoho DAOs to copy
     * @param Lock $lockCopy A lock that can be used to avoid running the same command (copy) twice at the same time
     * @param Lock $lockSync A lock that can be used to avoid running the same command (sync) twice at the same time
     */
    public function __construct(ZohoDatabaseCopier $zohoDatabaseCopier, ZohoDatabaseSyncZoho $zohoDatabaseSync, array $zohoDaos, Lock $lockCopy = null, Lock $lockSync = null)
    {
        parent::__construct();
        $this->zohoDatabaseCopier = $zohoDatabaseCopier;
        $this->zohoDatabaseSync = $zohoDatabaseSync;
        $this->zohoDaos = $zohoDaos;
        $this->lockCopy = $lockCopy;
        $this->lockSync = $lockSync;
    }

    protected function configure()
    {
        $this
            ->setName('zoho:copy-db')
            ->setDescription('Copies the Zoho database in local DB tables and synchronize Zoho CRM from the Zoho database.')
            ->addArgument("action",  InputArgument::REQUIRED, "Specify 'copy' or 'sync'")
            ->addOption("reset", "r", InputOption::VALUE_NONE, 'Get a fresh copy of Zoho (rather than doing incremental copy)')
            ->addOption("trigger", "t", InputOption::VALUE_NONE, 'Create or update the triggers');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $action = $input->getArgument('action');
        switch ($action) {
            case 'copy':
                $this->copyDb($input, $output);
                break;
            case 'sync':
                $this->syncDb($output);
                break;
            default:
                throw new InvalidArgumentException('Named argument not found.');
        }
    }

    /**
     * Run the copy Db command.
     * @param InputInterface $input
     * @param OutputInterface $output
     */
    private function copyDb(InputInterface $input, OutputInterface $output){
        try {
            if ($this->lockCopy) {
                $this->lockCopy->acquireLock();
            }

            if ($input->getOption('reset')) {
                $incremental = false;
            } else {
                $incremental = true;
            }

            $forceCreateTrigger = false;

            if($input->getOption('trigger')){
                $forceCreateTrigger = true;
            }
            $twoWaysSync = true;
            $this->zohoDatabaseCopier->setLogger(new ConsoleLogger($output));

            $output->writeln('Starting copying Zoho data into local database.');
            foreach ($this->zohoDaos as $zohoDao) {
                $output->writeln(sprintf('Copying data using <info>%s</info>', get_class($zohoDao)));
                $this->zohoDatabaseCopier->copy($zohoDao, $incremental, $twoWaysSync, $forceCreateTrigger);
            }
            $output->writeln('Zoho data successfully copied.');
            if ($this->lockCopy) {
                $this->lockCopy->releaseLock();
            }
        } catch (LockException $e) {
            $output->writeln('<error>Could not start zoho:copy-db copy command. Another zoho:copy-db copy command is already running.</error>');
        }
    }

    /**
     * Run he sync Db command.
     * @param InputInterface $input
     * @param OutputInterface $output
     */
    private function syncDb(OutputInterface $output){
        try {
            if ($this->lockSync) {
                $this->lockSync->acquireLock();
            }
            $this->zohoDatabaseSync->setLogger(new ConsoleLogger($output));

            $output->writeln('Starting synchronize Zoho data into Zoho CRM.');
            foreach ($this->zohoDaos as $zohoDao) {
                $output->writeln(sprintf(' > Insert new rows using <info>%s</info>', get_class($zohoDao)));
                $this->zohoDatabaseSync->pushInsertedRows($zohoDao);
                $output->writeln(sprintf(' > Update rows using <info>%s</info>', get_class($zohoDao)));
                $this->zohoDatabaseSync->pushUpdatedRows($zohoDao);
                $output->writeln(sprintf(' > Delete rows using <info>%s</info>', get_class($zohoDao)));
                $this->zohoDatabaseSync->pushDeletedRows($zohoDao);
            }
            $output->writeln('Zoho data successfully synchronized.');
            if ($this->lockSync) {
                $this->lockSync->releaseLock();
            }
        } catch (LockException $e) {
            $output->writeln('<error>Could not start zoho:sync-db sync command. Another zoho:sync-db sync command is already running.</error>');
        }
    }
}
