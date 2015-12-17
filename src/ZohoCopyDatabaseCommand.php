<?php


namespace Wabel\Zoho\CRM\Copy;


use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Wabel\Zoho\CRM\AbstractZohoDao;

class ZohoCopyDatabaseCommand extends Command
{
    /**
     * The list of Zoho DAOs to copy
     *
     * @var AbstractZohoDao[]
     */
    private $zohoDaos;

    /**
     * @var ZohoDatabaseCopier
     */
    private $zohoDatabaseCopier;

    /**
     * @param ZohoDatabaseCopier $zohoDatabaseCopier
     * @param \Wabel\Zoho\CRM\AbstractZohoDao[] $zohoDaos The list of Zoho DAOs to copy
     */
    public function __construct(ZohoDatabaseCopier $zohoDatabaseCopier, array $zohoDaos)
    {
        parent::__construct();
        $this->zohoDatabaseCopier = $zohoDatabaseCopier;
        $this->zohoDaos = $zohoDaos;
    }

    protected function configure()
    {
        $this
            ->setName('zoho:copy-db')
            ->setDescription('Copies the Zoho database in local DB tables');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $output->writeln("Starting copying Zoho data into local database.");
        foreach ($this->zohoDaos as $zohoDao) {
            $output->writeln(sprintf("Copying data using <info>%s</info>", get_class($zohoDao)));
            $this->zohoDatabaseCopier->copy($zohoDao);
        }
        $output->writeln("Zoho data successfully copied.");
    }
}
