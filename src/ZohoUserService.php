<?php


namespace Wabel\Zoho\CRM\Copy;

use Wabel\Zoho\CRM\Request\Response;
use Wabel\Zoho\CRM\ZohoClient;

/**
 * This class acts as a cache for fetching users from Zoho.
 */
class ZohoUserService
{

    /**
     * @var ZohoClient
     */
    private $zohoClient;

    public function __construct(ZohoClient $zohoClient)
    {
        $this->zohoClient = $zohoClient;
    }

    /**
     * @return \ZCRMUser[]
     */
    public function getUsers()
    {
        return $this->zohoClient->getUsers();
    }
}
