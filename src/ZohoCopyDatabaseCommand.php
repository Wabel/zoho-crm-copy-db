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
     * @var Lock
     */
    private $lock;

    /**
     * @param ZohoDatabaseCopier                $zohoDatabaseCopier
     * @param \Wabel\Zoho\CRM\AbstractZohoDao[] $zohoDaos           The list of Zoho DAOs to copy
     * @param Lock                              $lock               A lock that can be used to avoid running the same command twice at the same time
     */
    public function __construct(ZohoDatabaseCopier $zohoDatabaseCopier, array $zohoDaos, Lock $lock = null)
    {
        parent::__construct();
        $this->zohoDatabaseCopier = $zohoDatabaseCopier;
        $this->zohoDaos = $zohoDaos;
        $this->lock = $lock;
    }

    protected function configure()
    {
        $this
            ->setName('zoho:copy-db')
            ->setDescription('Copies the Zoho database in local DB tables')
            ->addOption("reset", "r", InputOption::VALUE_NONE, 'Get a fresh copy of Zoho (rather than doing incremental copy)')
            ->addOption("trigger", "t", InputOption::VALUE_NONE, 'Create or update the triggers');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        try {
            if ($this->lock) {
                $this->lock->acquireLock();
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
            if ($this->lock) {
                $this->lock->releaseLock();
            }
        } catch (LockException $e) {
            $output->writeln('<error>Could not start zoho:copy-db command. Another zoho:copy-db command is already running.</error>');
        }
    }
}
