<?php

namespace Wabel\Zoho\CRM\Copy;

use function Stringy\create as s;
use Wabel\Zoho\CRM\AbstractZohoDao;

/**
 *  ZohoDatabaseHelper : Helper class
 * 
 */
class ZohoDatabaseHelper
{
    
    /**
     * Computes the name of the table based on the DAO plural module name.
     *
     * @param AbstractZohoDao $dao
     *
     * @return string
     */
    public static function getTableName(AbstractZohoDao $dao, $prefix)
    {
        $tableName = $prefix.$dao->getPluralModuleName();
        $tableName = s($tableName)->upperCamelize()->underscored();

        return (string) $tableName;
    }
}