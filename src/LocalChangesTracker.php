<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Table;
use Psr\Log\LoggerInterface;

/**
 * This class is in charge of tracking local files.
 * To do so, it can add a set of triggers that observe and track changes in tables.
 */
class LocalChangesTracker
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

    public function createTrackingTables()
    {
        $schema = new \Doctrine\DBAL\Schema\Schema();

        $localUpdate = $schema->createTable('local_update');
        $localUpdate->addColumn('table_name', 'string', ['length' => 100]);
        $localUpdate->addColumn('uid', 'string', ['length' => 36]);
        $localUpdate->addColumn('field_name', 'string', ['length' => 100]);
        $localUpdate->addColumn('error', 'text', ['notnull' => false]);
        $localUpdate->addColumn('errorTime', 'datetime', ['notnull' => false]);
        $localUpdate->setPrimaryKey(array('table_name', 'uid', 'field_name'));

        $localInsert = $schema->createTable('local_insert');
        $localInsert->addColumn('table_name', 'string', ['length' => 100]);
        $localInsert->addColumn('uid', 'string', ['length' => 36]);
        $localInsert->addColumn('error', 'text', ['notnull' => false]);
        $localInsert->addColumn('errorTime', 'datetime', ['notnull' => false]);
        $localInsert->setPrimaryKey(array('table_name', 'uid'));

        $localDelete = $schema->createTable('local_delete');
        $localDelete->addColumn('table_name', 'string', ['length' => 100]);
        $localDelete->addColumn('uid', 'string', ['length' => 36]);
        $localDelete->addColumn('id', 'string', ['length' => 100, 'notnull' => false]);
        $localDelete->addColumn('error', 'text', ['notnull' => false]);
        $localDelete->addColumn('errorTime', 'datetime', ['notnull' => false]);
        $localDelete->setPrimaryKey(array('table_name', 'uid'));
        $localDelete->addUniqueIndex(['id', 'table_name']);

        $dbalTableDiffService = new DbalTableDiffService($this->connection, $this->logger);
        $dbalTableDiffService->createOrUpdateTable($localUpdate);
        $dbalTableDiffService->createOrUpdateTable($localInsert);
        $dbalTableDiffService->createOrUpdateTable($localDelete);
    }

    public function hasTriggersInsertUpdateDelete(Table $table)
    {
        $triggerInsertName = sprintf('TRG_%s_ONINSERT', $table->getName());
        $triggerUpdateName = sprintf('TRG_%s_ONUPDATE', $table->getName());
        $triggerDeleteName = sprintf('TRG_%s_ONDELETE', $table->getName());
        $nbTriggers = 0;
        $triggers = $this->connection->fetchAll("SHOW TRIGGERS LIKE '{$table->getName()}'");
        foreach ($triggers as $trigger) {
            if (in_array($trigger['Trigger'], [$triggerInsertName, $triggerUpdateName, $triggerDeleteName], true)) {
                $nbTriggers++;
            }
        }
        return $nbTriggers === 3;
    }

    public function hasTriggerInsertUuid(Table $table)
    {
        $triggerInsertName = sprintf('TRG_%s_SETUUIDBEFOREINSERT', $table->getName());
        $triggers = $this->connection->fetchAll("SHOW TRIGGERS LIKE '{$table->getName()}'");
        foreach ($triggers as $trigger) {
            if ($trigger['Trigger'] === $triggerInsertName) {
                return true;
            }
        }
        return false;
    }

    public function createUuidInsertTrigger(Table $table)
    {
        $triggerName = sprintf('TRG_%s_SETUUIDBEFOREINSERT', $table->getName());
        $this->logger->info('Creating ' . $triggerName . ' trigger for table ' . $table->getName() . '...');

        //Fix - temporary MySQL 5.7 strict mode
        $sql = sprintf(
            '
            DROP TRIGGER IF EXISTS %s;
            
            CREATE TRIGGER %s BEFORE INSERT ON `%s` 
            FOR EACH ROW
            IF new.uid IS NULL
              THEN
              	SET @uuidmy = uuid();
                SET new.uid = LOWER(CONCAT(
                SUBSTR(@uuidmy, 1, 8), \'-\',
                SUBSTR(@uuidmy, 10, 4), \'-\',
                SUBSTR(@uuidmy, 15, 4), \'-\',
                SUBSTR(@uuidmy, 20, 4), \'-\',
                SUBSTR(@uuidmy, 25)
              ));
              END IF;
            ', $triggerName, $triggerName, $table->getName()
        );

        $this->connection->exec($sql);
    }

    public function createInsertTrigger(Table $table)
    {
        $triggerName = sprintf('TRG_%s_ONINSERT', $table->getName());
        $this->logger->info('Creating ' . $triggerName . ' trigger for table ' . $table->getName() . '...');

        $tableNameQuoted = $this->connection->quote($table->getName());

        $sql = sprintf(
            '
            DROP TRIGGER IF EXISTS %s;
            
            CREATE TRIGGER %s AFTER INSERT ON `%s` 
            FOR EACH ROW
            BEGIN
              IF (NEW.id IS NULL AND NEW.createdTime IS NULL ) THEN
                INSERT INTO local_insert (table_name, uid) VALUES (%s, NEW.uid);
                DELETE FROM local_delete WHERE table_name = %s AND uid = NEW.uid;
                DELETE FROM local_update WHERE table_name = %s AND uid = NEW.uid;
              END IF;
            END;
            
            ', $triggerName, $triggerName, $table->getName(), $tableNameQuoted, $tableNameQuoted, $tableNameQuoted
        );

        $this->connection->exec($sql);
    }

    public function createDeleteTrigger(Table $table)
    {
        $triggerName = sprintf('TRG_%s_ONDELETE', $table->getName());
        $this->logger->info('Creating ' . $triggerName . ' trigger for table ' . $table->getName() . '...');

        $tableNameQuoted = $this->connection->quote($table->getName());

        $sql = sprintf(
            '
            DROP TRIGGER IF EXISTS %s;
            
            CREATE TRIGGER %s BEFORE DELETE ON `%s` 
            FOR EACH ROW
            BEGIN
              IF (OLD.id IS NOT NULL) THEN
                INSERT INTO local_delete (table_name, uid, id) VALUES (%s, OLD.uid, OLD.id);
              END IF;
              DELETE FROM local_insert WHERE table_name = %s AND uid = OLD.uid;
              DELETE FROM local_update WHERE table_name = %s AND uid = OLD.uid;
            END;
            
            ', $triggerName, $triggerName, $table->getName(), $tableNameQuoted, $tableNameQuoted, $tableNameQuoted
        );

        $this->connection->exec($sql);
    }

    public function createUpdateTrigger(Table $table)
    {
        $triggerName = sprintf('TRG_%s_ONUPDATE', $table->getName());
        $this->logger->info('Creating ' . $triggerName . ' trigger for table ' . $table->getName() . '...');

        $innerCode = '';

        $tableNameQuoted = $this->connection->quote($table->getName());

        foreach ($table->getColumns() as $column) {
            if (in_array($column->getName(), ['id', 'uid'])) {
                continue;
            }
            $columnName = $this->connection->quoteIdentifier($column->getName());
            $innerCode .= sprintf(
                '
                IF NOT(NEW.%s <=> OLD.%s) THEN
                  REPLACE INTO local_update (table_name, uid, field_name) VALUES (%s, NEW.uid, %s);
                END IF;
            ', $columnName, $columnName, $tableNameQuoted, $this->connection->quote($column->getName())
            );
        }

        $sql = sprintf(
            '
            DROP TRIGGER IF EXISTS %s;
            
            CREATE TRIGGER %s AFTER UPDATE ON `%s` 
            FOR EACH ROW
            BEGIN
              IF (NEW.modifiedTime <=> OLD.modifiedTime) THEN
            %s
              END IF;
            END;
            
            ', $triggerName, $triggerName, $table->getName(), $innerCode
        );

        $this->connection->exec($sql);
    }
}
