<?php

namespace Wabel\Zoho\CRM\Copy;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaDiff;
use Wabel\Zoho\CRM\AbstractZohoDao;
use function Stringy\create as s;

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
     * @var ZohoChangeListener[]
     */
    private $listeners;

    /**
     * ZohoDatabaseCopier constructor.
     *
     * @param Connection $connection
     * @param string $prefix Prefix for the table name in DB
     * @param ZohoChangeListener[] $listeners The list of listeners called when a record is inserted or updated.
     */
    public function __construct(Connection $connection, $prefix = 'zoho_', array $listeners = [])
    {
        $this->connection = $connection;
        $this->prefix = $prefix;
        $this->listeners = $listeners;
    }

    /**
     * @param AbstractZohoDao $dao
     * @param bool            $incrementalSync Whether we synchronize only the modified files or everything.
     */
    public function copy(AbstractZohoDao $dao, $incrementalSync = true)
    {
        $this->synchronizeDbModel($dao);
        $this->copyData($dao, $incrementalSync);
    }

    /**
     * Synchronizes the DB model with Zoho.
     *
     * @param AbstractZohoDao $dao
     *
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Schema\SchemaException
     */
    private function synchronizeDbModel(AbstractZohoDao $dao)
    {
        $tableName = $this->getTableName($dao);

        $schema = new Schema();
        $table = $schema->createTable($tableName);

        $flatFields = $this->getFlatFields($dao->getFields());

        $table->addColumn('id', 'string', ['length' => 100]);
        $table->setPrimaryKey(['id']);

        foreach ($flatFields as $field) {
            $columnName = $field['name'];

            $length = null;
            $index = false;

            // Note: full list of types available here: https://www.zoho.com/crm/help/customization/custom-fields.html
            switch ($field['type']) {
                case 'Lookup ID':
                case 'Lookup':
                    $type = 'string';
                    $length = 100;
                    $index = true;
                    break;
                case 'OwnerLookup':
                    $type = 'string';
                    $index = true;
                    $length = 25;
                    break;
                case 'Formula':
                    // Note: a Formula can return any type, but we have no way to know which type it returns...
                    $type = 'string';
                    $length = 100;
                    break;
                case 'DateTime':
                    $type = 'datetime';
                    break;
                case 'Date':
                    $type = 'date';
                    break;
                case 'DateTime':
                    $type = 'datetime';
                    break;
                case 'Boolean':
                    $type = 'boolean';
                    break;
                case 'TextArea':
                    $type = 'text';
                    break;
                case 'BigInt':
                    $type = 'bigint';
                    break;
                case 'Phone':
                case 'Auto Number':
                case 'Text':
                case 'URL':
                case 'Email':
                case 'Website':
                case 'Pick List':
                case 'Multiselect Pick List':
                    $type = 'string';
                    $length = $field['maxlength'];
                    break;
                case 'Double':
                case 'Percent':
                    $type = 'float';
                    break;
                case 'Integer':
                    $type = 'integer';
                    break;
                case 'Currency':
                case 'Decimal':
                    $type = 'decimal';
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
                $table->addIndex([$columnName]);
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

    /**
     * @param AbstractZohoDao $dao
     * @param bool            $incrementalSync Whether we synchronize only the modified files or everything.
     *
     * @throws \Doctrine\DBAL\DBALException
     * @throws \Doctrine\DBAL\Schema\SchemaException
     * @throws \Wabel\Zoho\CRM\Exception\ZohoCRMResponseException
     */
    private function copyData(AbstractZohoDao $dao, $incrementalSync = true)
    {
        $tableName = $this->getTableName($dao);

        if ($incrementalSync) {
            // Let's get the last modification date:
            $lastActivityTime = $this->connection->fetchColumn('SELECT MAX(lastActivityTime) FROM '.$tableName);
            if ($lastActivityTime !== null) {
                $lastActivityTime = new \DateTime($lastActivityTime);
            }
            $records = $dao->getRecords(null, null, $lastActivityTime);
        } else {
            $records = $dao->getRecords();
        }

        $table = $this->connection->getSchemaManager()->createSchema()->getTable($tableName);

        $flatFields = $this->getFlatFields($dao->getFields());
        $fieldsByName = [];
        foreach ($flatFields as $field) {
            $fieldsByName[$field['name']] = $field;
        }

        $select = $this->connection->prepare('SELECT * FROM '.$tableName.' WHERE id = :id');

        $this->connection->beginTransaction();

        foreach ($records as $record) {
            $data = [];
            $types = [];
            foreach ($table->getColumns() as $column) {
                if ($column->getName() === 'id') {
                    continue;
                } else {
                    $field = $fieldsByName[$column->getName()];
                    $getterName = $field['getter'];
                    $data[$column->getName()] = $record->$getterName();
                    $types[$column->getName()] = $column->getType()->getName();
                }
            }

            $select->execute(['id' => $record->getZohoId()]);
            $result = $select->fetch(\PDO::FETCH_ASSOC);
            if ($result === false) {
                $data['id'] = $record->getZohoId();
                $types['id'] = 'string';

                $this->connection->insert($tableName, $data, $types);

                foreach ($this->listeners as $listener) {
                    $listener->onInsert($data, $dao);
                }
            } else {
                $identifier = ['id' => $record->getZohoId()];
                $types['id'] = 'string';

                $this->connection->update($tableName, $data, $identifier, $types);

                // Let's add the id for the update trigger
                $data['id'] = $record->getZohoId();
                foreach ($this->listeners as $listener) {
                    $listener->onUpdate($data, $result, $dao);
                }
            }
        }

        $this->connection->commit();
    }

    private function getFlatFields(array $fields)
    {
        $flatFields = [];
        foreach ($fields as $cat) {
            $flatFields = array_merge($flatFields, $cat);
        }

        return $flatFields;
    }

    /**
     * Computes the name of the table based on the DAO plural module name.
     *
     * @param AbstractZohoDao $dao
     *
     * @return string
     */
    private function getTableName(AbstractZohoDao $dao)
    {
        $tableName = $this->prefix.$dao->getPluralModuleName();
        $tableName = s($tableName)->upperCamelize()->underscored();

        return (string) $tableName;
    }
}
