<?php
namespace Wabel\Zoho\CRM\Copy;

use Wabel\Zoho\CRM\AbstractZohoDao;

/**
 * Interface used to catch changes in Zoho.
 */
interface ZohoChangeListener
{
    /**
     * Function call triggered when a new field has been inserted.
     *
     * @param array $data
     * @param AbstractZohoDao $dao
     */
    public function onInsert(array $data, AbstractZohoDao $dao);

    /**
     * Function call triggered when a new field has been updated.
     *
     * @param array $newData
     * @param array $oldData
     * @param AbstractZohoDao $dao
     */
    public function onUpdate(array $newData, array $oldData, AbstractZohoDao $dao);
}