<?php
namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaDiff;
use Wabel\Zoho\CRM\AbstractZohoDao;

/**
 * This class is in charge of synchronizing one table of your database with Zoho records.
 */
class ZohoDatabaseCopier
{

    /**
     * @var Connection
     */
    private $connection;

    private $prefix;

    /**
     * ZohoDatabaseCopier constructor.
     * @param Connection $connection
     */
    public function __construct(Connection $connection, $prefix = "zoho_")
    {
        $this->connection = $connection;
        $this->prefix = $prefix;
    }


    public function copy(AbstractZohoDao $dao)
    {
        $this->synchronizeDbModel($dao);
    }


    /**
     * Synchronizes the DB model with Zoho.
     * @param AbstractZohoDao $dao
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Schema\SchemaException
     */
    private function synchronizeDbModel(AbstractZohoDao $dao) {
        $tableName = $this->prefix.$dao->getModule();

        $schema = new Schema();
        $table = $schema->createTable($tableName);

        $flatFields = $this->getFlatFields($dao->getFields());

        foreach ($flatFields as $field) {
            $columnName = $field['name'];

            $length = null;
            $index = false;
            $pk = false;

            switch ($field['type']) {
                case 'Lookup ID':
                    $pk = true;
                case 'Lookup':
                    $type = "string";
                    $length = 100;
                    $index = true;
                    break;
                case 'OwnerLookup':
                    $type = "string";
                    $index = true;
                    $length = 25;
                    break;
                case 'DateTime':
                    $type = "datetime";
                    break;
                case 'Date':
                    $type = "date";
                    break;
                case 'DateTime':
                    $type = "datetime";
                    break;
                case 'Boolean':
                    $type = "boolean";
                    break;
                case 'TextArea':
                    $type = "text";
                    break;
                case 'Phone':
                case 'Text':
                case 'Email':
                case 'Pick List':
                    $type = "string";
                    $length = $field['maxlength'];
                    break;
                default:
                    throw new \RuntimeException('Unknown type "'.$field['type'].'"');
            }

            $options = [];

            if ($length) {
                $options['length'] = $length;
            }

            $table->addColumn($columnName, $type, $options);

            if ($pk) {
                $table->setPrimaryKey([ $columnName ]);
            } elseif ($index) {
                $table->addIndex([ $columnName ]);
            }
        }

        $dbSchema = $this->connection->getSchemaManager()->createSchema();
        if ($this->connection->getSchemaManager()->tablesExist($tableName)) {
            $dbTable = $dbSchema->getTable($tableName);

            $comparator = new \Doctrine\DBAL\Schema\Comparator();
            $tableDiff = $comparator->diffTable($dbTable, $table);

            if ($tableDiff !== false) {
                $diff = new SchemaDiff();
                $diff->fromSchema = $dbSchema;
                $diff->changedTables[$tableName] = $tableDiff;
                $statements = $diff->toSaveSql($this->connection->getDatabasePlatform());
                foreach ($statements as $sql) {
                    $this->connection->exec($sql);
                }
            }
        } else {
            $diff = new SchemaDiff();
            $diff->fromSchema = $dbSchema;
            $diff->newTables[$tableName] = $table;
            $statements = $diff->toSaveSql($this->connection->getDatabasePlatform());
            foreach ($statements as $sql) {
                $this->connection->exec($sql);
            }
        }

    }

    private function copyData(AbstractZohoDao $dao) {
        $records = $dao->getRecords();

    }

    private function getFlatFields(array $fields)
    {
        $flatFields = [];
        foreach ($fields as $cat) {
            $flatFields = array_merge($flatFields, $cat);
        }
        return $flatFields;
    }

}
