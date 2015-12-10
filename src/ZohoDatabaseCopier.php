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
        $this->copyData($dao);
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

        $table->addColumn("id", "string", ['length'=>100]);
        $table->setPrimaryKey(['id']);

        foreach ($flatFields as $field) {
            $columnName = $field['name'];

            $length = null;
            $index = false;

            switch ($field['type']) {
                case 'Lookup ID':
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

            //$options['notnull'] = $field['req'];
            $options['notnull'] = false;

            $table->addColumn($columnName, $type, $options);

            if ($index) {
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
        $tableName = $this->prefix.$dao->getModule();

        $table = $this->connection->getSchemaManager()->createSchema()->getTable($tableName);


        $flatFields = $this->getFlatFields($dao->getFields());
        $fieldsByName = [];
        foreach ($flatFields as $field) {
            $fieldsByName[$field['name']] = $field;
        }

        foreach ($records as $record) {
            $data = [];
            $types = [];
            foreach ($table->getColumns() as $column) {
                if ($column->getName() === 'id') {
                    $data['id'] = $record->getZohoId();
                    $types['id'] = 'string';
                } else {
                    $field = $fieldsByName[$column->getName()];
                    $getterName = $field['getter'];
                    $data[$column->getName()] = $record->$getterName();
                    $types[$column->getName()] = $column->getType()->getName();
                }
            }
            $this->connection->insert($tableName, $data, $types);
        }
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
