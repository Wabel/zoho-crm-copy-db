<?php
namespace Wabel\Zoho\CRM\Copy\Log;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Logging\SQLLogger;
use Doctrine\DBAL\SQLParserUtils;
use function PHPSTORM_META\type;
use Psr\Log\LoggerInterface;

class ZohoSyncSQLLogger implements SQLLogger
{

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var Connection
     */
    private $connection;
    /**
     * @var bool
     */
    private $authorizedSQLWithoutParams;
    /**
     * @var bool
     */
    private $authorizedSQLWithParams;

    /**
     * ZohoSyncSQLLogger constructor.
     * @param LoggerInterface $logger
     * @param Connection $connection
     * @param bool $authorizedSQLWithoutParams
     * @param bool $authorizedSQLWithParams
     */
    public function __construct(LoggerInterface $logger, Connection $connection, $authorizedSQLWithoutParams = false, $authorizedSQLWithParams = false)
    {
        $this->logger = $logger;
        $this->connection = $connection;
        $this->authorizedSQLWithoutParams = $authorizedSQLWithoutParams;
        $this->authorizedSQLWithParams = $authorizedSQLWithParams;
    }

    /**
     * Logs a SQL statement somewhere.
     *
     * @param string $sql The SQL to be executed.
     * @param array|null $params The SQL parameters.
     * @param array|null $types The SQL parameter types.
     *
     * @return void
     */
    public function startQuery($sql, array $params = null, array $types = null)
    {
        $query = null;
        if ($params && $this->authorizedSQLWithParams) {
            $this->logger->debug($sql.' -- Params : {params} - Types : {types}', ['params' => json_encode($params), 'types' => json_encode($types)]);
        } elseif($this->authorizedSQLWithoutParams) {
            $this->logger->debug($sql);
        }
    }

    /**
     * Marks the last started query as stopped. This can be used for timing of queries.
     *
     * @return void
     */
    public function stopQuery()
    {
    }
}