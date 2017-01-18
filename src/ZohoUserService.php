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

    /**
     * @var Response
     */
    private $usersResponse;

    public function __construct(ZohoClient $zohoClient)
    {
        $this->zohoClient = $zohoClient;
    }

    /**
     * @return Response
     */
    public function getUsers()
    {
        if ($this->usersResponse !== null) {
            return $this->usersResponse;
        }

        $this->usersResponse = $this->zohoClient->getUsers();
        return $this->usersResponse;
    }
}
