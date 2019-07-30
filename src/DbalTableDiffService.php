<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\SchemaDiff;
use Doctrine\DBAL\Schema\Table;
use Psr\Log\LoggerInterface;

class DbalTableDiffService
{
    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param Connection $connection
     */
    public function __construct(Connection $connection, LoggerInterface $logger)
    {
        $this->connection = $connection;
        $this->logger = $logger;
    }

    /**
     * @param Table $table
     *
     * @return bool Returns true if changes where applied. False otherwise.
     */
    public function createOrUpdateTable(Table $table)
    {
        $tableName = $table->getName();
        $dbSchema = $this->connection->getSchemaManager()->createSchema();
        if ($this->connection->getSchemaManager()->tablesExist($tableName)) {
            $dbTable = $dbSchema->getTable($tableName);

            $comparator = new Comparator();
            $tableDiff = $comparator->diffTable($dbTable, $table);

            if ($tableDiff !== false) {
                $this->logger->notice('Changes detected in table structure for '.$tableName.'. Applying patch.');
                $diff = new SchemaDiff();
                $diff->fromSchema = $dbSchema;
                $diff->changedTables[$tableName] = $tableDiff;
                $statements = $diff->toSaveSql($this->connection->getDatabasePlatform());
                foreach ($statements as $sql) {
                    $this->connection->exec($sql);
                }
                return true;
            }
            $this->logger->info('No changes detected in table structure for '.$tableName);
            return false;
        }

        $this->logger->notice("Creating new table '$tableName'.");
        $diff = new SchemaDiff();
        $diff->fromSchema = $dbSchema;
        $diff->newTables[$tableName] = $table;
        $statements = $diff->toSaveSql($this->connection->getDatabasePlatform());
        foreach ($statements as $sql) {
            $this->connection->exec($sql);
        }

        return true;
    }
}
