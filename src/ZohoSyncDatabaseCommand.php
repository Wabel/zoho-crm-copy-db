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
use Wabel\Zoho\CRM\Service\EntitiesGeneratorService;
use Wabel\Zoho\CRM\ZohoClient;
use Logger\Formatters\DateTimeFormatter;
use Mouf\Utils\Log\Psr\MultiLogger;
use Wabel\Zoho\CRM\Request\Response;
use zcrmsdk\crm\utility\ZCRMConfigUtil;

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
     *
     * @var MultiLogger
     */
    private $logger;

    /**
     * @var Lock
     */
    private $lock;

    /**
     * The Zoho Dao and Beans generator
     *
     * @var EntitiesGeneratorService
     */
    private $zohoEntitiesGenerator;

    /**
     *
     * @var ZohoClient
     */
    private $zohoClient;

    private $pathZohoDaos;

    private $namespaceZohoDaos;

    /**
     *
     * @var Response
     */
    private $usersResponse;

    /**
     * @var string[]
     */
    private $excludedZohoDao;


    /**
     * @param ZohoDatabaseModelSync    $zohoDatabaseModelSync
     * @param ZohoDatabaseCopier       $zohoDatabaseCopier
     * @param ZohoDatabasePusher       $zohoDatabaseSync
     * @param EntitiesGeneratorService $zohoEntitiesGenerator The Zoho Dao and Beans generator
     * @param ZohoClient               $zohoClient
     * @param string                   $pathZohoDaos          Tht path where we need to generate the Daos.
     * @param string                   $namespaceZohoDaos     Daos namespace
     * @param MultiLogger              $logger
     * @param Lock                     $lock                  A lock that can be used to avoid running the same command (copy) twice at the same time
     * @param string[]                 $excludedZohoDao       To exclude Dao and or solve Dao which can create ZohoResponse Error
     */
    public function __construct(ZohoDatabaseModelSync $zohoDatabaseModelSync, ZohoDatabaseCopier $zohoDatabaseCopier, ZohoDatabasePusher $zohoDatabaseSync,
        EntitiesGeneratorService $zohoEntitiesGenerator, ZohoClient $zohoClient,
        $pathZohoDaos, $namespaceZohoDaos, MultiLogger $logger, Lock $lock = null, $excludedZohoDao = []
    ) {
        parent::__construct();
        $this->zohoDatabaseModelSync = $zohoDatabaseModelSync;
        $this->zohoDatabaseCopier = $zohoDatabaseCopier;
        $this->zohoDatabaseSync = $zohoDatabaseSync;
        $this->zohoEntitiesGenerator =  $zohoEntitiesGenerator;
        $this->zohoClient = $zohoClient;
        $this->pathZohoDaos = $pathZohoDaos;
        $this->namespaceZohoDaos = $namespaceZohoDaos;
        $this->logger = $logger;
        $this->lock = $lock;
        $this->excludedZohoDao = $excludedZohoDao;
    }

    protected function configure()
    {
        $this
            ->setName('zoho:sync')
            ->setDescription('Synchronize the Zoho CRM data in a local database.')
            ->addOption('reset', 'r', InputOption::VALUE_NONE, 'Get a fresh copy of Zoho (rather than doing incremental copy)')
            ->addOption('skip-trigger', 's', InputOption::VALUE_NONE, 'Do not create or update the trigger')
            ->addOption('fetch-only', 'f', InputOption::VALUE_NONE, 'Fetch only the Zoho data in local database')
            ->addOption('push-only', 'p', InputOption::VALUE_NONE, 'Push only the local data to Zoho')
            ->addOption('limit', 'l', InputOption::VALUE_NONE, 'use defined memory limit or unlimited memory limit')
            ->addOption('log-path', null, InputOption::VALUE_OPTIONAL, 'Set the path of logs file')
            ->addOption('clear-logs', null, InputOption::VALUE_NONE, 'Clear logs file at startup')
            ->addOption('dump-logs', null, InputOption::VALUE_NONE, 'Dump logs into console when command finishes');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->logger->addLogger(new DateTimeFormatter(new ConsoleLogger($output)));
        try {
            if ($this->lock) {
                $this->lock->acquireLock();
            }

            // TODO: find a better way when zohocrm/php-sdk:src/crm/utility/Logger.php will allow to get the filename, delete, etc.
            if ($input->getOption('log-path') && $input->getOption('clear-logs')) {
                $this->logger->info('Clearing logs...');
                $path = $input->getOption('log-path');
                $logFile = $path . '/ZCRMClientLibrary.log';
                if (file_exists($logFile)) {
                    if (is_writable($logFile)) {
                        if (file_put_contents($logFile, '') === false) {
                            $this->logger->error(sprintf('Error when clearing log file in %s', $logFile));
                        }
                    } else {
                        $this->logger->warning(sprintf('Cannot write into log file in %s', $logFile));
                    }
                } else {
                    $this->logger->warning(sprintf('Cannot find log file in %s', $logFile));
                }
            }

            if(!$input->getOption('limit')) {
                ini_set('memory_limit', '-1');
            }

            if ($input->getOption('fetch-only') && $input->getOption('push-only')) {
                $this->logger->error('Options fetch-only and push-only are mutually exclusive.');
            }

            $this->syncUserModel();

            $this->regenerateZohoDao();

            $this->syncModel($input);

            if (!$input->getOption('push-only')) {
                $this->fetchUserDb();
                $this->fetchDb($input);
            }
            if (!$input->getOption('fetch-only')) {
                $this->pushDb();
            }

            if ($input->getOption('log-path') && $input->getOption('dump-logs')) {
                $this->logger->info('Dumping logs...');
                $path = $input->getOption('log-path');
                $logFile = $path . '/ZCRMClientLibrary.log';
                if (file_exists($logFile)) {
                    if (is_readable($logFile)) {
                        $this->logger->info(file_get_contents($logFile));
                    } else {
                        $this->logger->warning(sprintf('Cannot read into log file in %s', $logFile));
                    }
                } else {
                    $this->logger->warning(sprintf('Cannot find log file in %s', $logFile));
                }
            }

            if ($this->lock) {
                $this->lock->releaseLock();
            }
        } catch (LockException $e) {
            $this->logger->error('Could not start zoho:copy-db copy command. Another zoho:copy-db copy command is already running.');
        }
    }

    /**
     * Sychronizes the model of the database with Zoho records.
     *
     * @param InputInterface $input
     */
    private function syncModel(InputInterface $input)
    {
        $twoWaysSync = !$input->getOption('fetch-only');
        $skipCreateTrigger = $input->getOption('skip-trigger');

        $this->logger->info('Starting to synchronize Zoho data into Zoho CRM.');
        foreach ($this->zohoDaos as $zohoDao) {
            $this->zohoDatabaseModelSync->synchronizeDbModel($zohoDao, $twoWaysSync, $skipCreateTrigger);
        }
        $this->logger->info('Zoho data successfully synchronized.');
    }

    /**
     * Sychronizes the model of the database with Zoho Users records.
     */
    private function syncUserModel()
    {
        $this->logger->info('Starting to synchronize Zoho users model.');
        $this->zohoDatabaseModelSync->synchronizeUserDbModel();
        $this->logger->info('Zoho users model successfully synchronized.');
    }

    /**
     * Regerate Zoho Daos
     */
    private function regenerateZohoDao()
    {
        $this->logger->info('Start to generate all the zoho daos.');
        $zohoModules = $this->zohoEntitiesGenerator->generateAll($this->pathZohoDaos, $this->namespaceZohoDaos);
        foreach ($zohoModules as $daoFullClassName) {
            /* @var $zohoDao AbstractZohoDao */
            $zohoDao = new $daoFullClassName($this->zohoClient);
            //To have more module which is use time of modification (createdTime or lastActivityTime).
            //use an array of Excluded Dao by full namespace
            if ($this->excludedZohoDao && in_array(get_class($zohoDao), $this->excludedZohoDao)) {
                continue;
            }
            $this->zohoDaos [] = $zohoDao;
            $this->logger->info(sprintf('%s has been created', get_class($zohoDao)));
        }
        $this->logger->info('Finished to create all the zoho daos.');
    }

    /**
     * Run the fetch User Db command.
     */
    private function fetchUserDb()
    {
        $this->logger->info('Start to copy Zoho users data into local database.');
        $this->zohoDatabaseCopier->fetchUserFromZoho();
        $this->logger->info('Zoho users data successfully copied.');
    }
    
    
    /**
     * Run the fetch Db command.
     *
     * @param InputInterface  $input
     */
    private function fetchDb(InputInterface $input)
    {
        if ($input->getOption('reset')) {
            $incremental = false;
        } else {
            $incremental = true;
        }

        $twoWaysSync = !$input->getOption('fetch-only');

        $this->logger->info('Start to copy Zoho data into local database.');
        foreach ($this->zohoDaos as $zohoDao) {
            $this->logger->info(sprintf('Copying data using %s', get_class($zohoDao)));
                $this->zohoDatabaseCopier->fetchFromZoho($zohoDao, $incremental, $twoWaysSync);
        }
        $this->logger->info('Zoho data successfully copied.');
    }

    /**
     * Run the push Db command.
     */
    private function pushDb()
    {
        $this->logger->info('Start to synchronize Zoho data into Zoho CRM.');
        foreach ($this->zohoDaos as $zohoDao) {
            if($zohoDao->getFieldFromFieldName('createdTime')) {
                $this->zohoDatabaseSync->pushToZoho($zohoDao);
            }
        }
        $this->logger->info('Zoho data successfully synchronized.');
    }
}
