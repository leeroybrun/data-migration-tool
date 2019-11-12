<?php
namespace Migration\Handler\AheadworksBlog;

use Migration\ResourceModel\Adapter\Mysql;
use Migration\ResourceModel\Destination;
use Migration\ResourceModel\Record;
use Migration\Handler\AbstractHandler;
use Migration\Handler\HandlerInterface;
use Migration\Config;

/**
 * Handler for UrlKeyIsUnique
 */
class UrlKeyIsUnique extends AbstractHandler implements HandlerInterface
{
    /**
     * @var []
     */
    private $checkTables = [
        'aw_blog_post',
        'aw_blog_category'
    ];

    /**
     * @var []|null
     */
    private $urlKeys = null;

    /**
     * @var bool
     */
    private $blogUpdate;

    /**
     * @var Destination
     */
    private $destination;

    /**
     * @var Config
     */
    private $config;

    /**
     * @param Destination $destination
     * @param Config $config
     */
    public function __construct(
        Destination $destination,
        Config $config
    ) {
        $this->destination = $destination;
        $this->config = $config;
        $this->blogUpdate = (bool)$this->config->getOption('aw_blog_update');
        $this->getAllUrlKeyFromDb();
    }

    /**
     * {@inheritdoc}
     */
    public function handle(Record $recordToHandle, Record $oppositeRecord)
    {
        $this->validate($recordToHandle);
        $newUrlKey = $urlKey = $recordToHandle->getValue($this->field);
        $check = true;
        $counter = 1;
        do {
            if (in_array($newUrlKey, $this->urlKeys)) {
                $newUrlKey = $urlKey . '_' . $counter;
                $counter++;
            } else {
                $check = false;
                $this->urlKeys[] = $newUrlKey;
            }
        } while ($check);
        $recordToHandle->setValue($this->field, $newUrlKey);
    }

    /**
     * Retrieve url keys from db tables
     *
     * @return []
     */
    private function getAllUrlKeyFromDb()
    {
        if ($this->urlKeys == null) {
            $this->urlKeys = [];
            // If update blog
            if ($this->blogUpdate) {
                $adapter = $this->destination->getAdapter();
                foreach ($this->checkTables as $table) {
                    $query = $adapter->getSelect()
                        ->from($this->destination->addDocumentPrefix($table), ['url_key']);
                    $this->urlKeys = array_merge($this->urlKeys, $query->getAdapter()->fetchCol($query));
                }
            }
        }
        return $this->urlKeys;
    }
}
