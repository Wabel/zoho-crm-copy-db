<?php


namespace Wabel\Zoho\CRM\Copy;


class TestListener implements ZohoChangeListener
{
    private $insertCalled = false;
    private $updateCalled = false;

    /**
     * Function call triggered when a new field has been inserted.
     *
     * @param array $data
     * @param AbstractZohoDao $dao
     */
    public function onInsert(array $data, AbstractZohoDao $dao)
    {
        $this->insertCalled = true;
    }

    /**
     * Function call triggered when a new field has been updated.
     *
     * @param array $newData
     * @param array $oldData
     * @param AbstractZohoDao $dao
     */
    public function onUpdate(array $newData, array $oldData, AbstractZohoDao $dao)
    {
        $this->updateCalled = true;
    }

    /**
     * @return boolean
     */
    public function isInsertCalled()
    {
        return $this->insertCalled;
    }

    /**
     * @return boolean
     */
    public function isUpdateCalled()
    {
        return $this->updateCalled;
    }


}