<?php
namespace Migration\Step\AheadworksBlog;

use Migration\ResourceModel\Destination;
use Migration\Reader\GroupsFactory;
use Migration\ResourceModel\Record;
use Migration\Config;

/**
 * Class Helper
 */
class Helper
{
    /**
     * @var string
     */
    private $destinationTagTable = 'aw_blog_tag';

    /**
     * @var string
     */
    private $destinationPostTagTable = 'aw_blog_post_tag';

    /**
     * @var string
     */
    private $destinationUrlRewriteTable = 'url_rewrite';

    /**
     * @var []
     */
    private $tags = [];

    /**
     * @var []
     */
    private $documentsDuplicateOnUpdate = [];

    /**
     * @var []
     */
    private $collectOldNewData = [];

    /**
     * @var []
     */
    private $tableToCollectOldNewData = [];

    /**
     * @var []
     */
    private $urlKeys = [
        'default' => [
            0 => []
        ]
    ];

    /**
     * @var []|null
     */
    private $destinationStoreIds;

    /**
     * @var []
     */
    private $destinationCount;

    /**
     * @var Destination
     */
    private $destination;

    /**
     * @var Config
     */
    private $config;

    /**
     * @param GroupsFactory $groupsFactory
     * @param Destination $destination
     * @param Config $config
     */
    public function __construct(
        GroupsFactory $groupsFactory,
        Destination $destination,
        Config $config
    ) {
        $this->readerGroups = $groupsFactory->create('aw_blog_groups_file');
        $this->destination = $destination;
        $this->config = $config;
        $this->documentsDuplicateOnUpdate = $this->readerGroups->getGroup('destination_documents_update_on_duplicate');
        $this->tableToCollectOldNewData = $this->readerGroups->getGroup('collect_old_new_data');
    }

    /**
     * Clear destination tag tables
     * @return null
     */
    public function clearDestinationTagTables()
    {
        $this->destination->clearDocument($this->destinationTagTable);
        $this->destination->clearDocument($this->destinationPostTagTable);
    }

    /**
     * Get tag ids from destination tag table
     *
     * @param string $tags
     * @return []
     */
    public function getPostTagIds($tags)
    {
        $tagIds = [];
        $tagsName = explode(',', $tags);

        if (!is_array($tagsName)) {
            return $tagIds;
        }
        foreach ($tagsName as $tagName) {
            if (!empty($tagName) && !$this->isTagExists($tagName)) {
                $record = ['name' => $tagName];
                $this->destination->saveRecords($this->destinationTagTable, [$record]);
            }
            if (!empty($tagName) && $tagId = $this->isTagExists($tagName)) {
                $tagIds[$tagName] = $tagId;
            }
        }
        return $tagIds;
    }

    /**
     * Save post tag relations
     *
     * @param int $postId
     * @param [] $tagsIds
     * @return null
     */
    public function savePostTag($postId, $tagsIds)
    {
        $records = [];
        foreach ($tagsIds as $tagId) {
            $records[] = ['tag_id' => $tagId, 'post_id' => $postId];
        }
        if ($records) {
            $this->destination->saveRecords($this->destinationPostTagTable, $records);
        }
    }

    /**
     * Is exists tag name in destination tag table
     *
     * @param string $tagName
     * @return string
     */
    private function isTagExists($tagName)
    {
        if (!isset($this->tags[$tagName]) || (isset($this->tags[$tagName]) && !$this->tags[$tagName])) {
            /** @var Mysql $adapter */
            $adapter = $this->destination->getAdapter();
            $query = $adapter->getSelect()
                ->from($this->destination->addDocumentPrefix($this->destinationTagTable), ['id'])
                ->where('name = ?', $tagName);
            $this->tags[$tagName] = $query->getAdapter()->fetchOne($query);
        }
        return $this->tags[$tagName];
    }

    /**
     * Replace record data
     *
     * @param string $sourceDocName
     * @param Record $record
     * @return []|bool
     */
    public function replaceRecordData($sourceDocName, $record)
    {
        $tableSaveCollectedOldNewData = $this->readerGroups
            ->getGroup('save_collected_old_new_data_' . $sourceDocName);
        if ($tableSaveCollectedOldNewData) {
            // Replace old data on new
            foreach ($tableSaveCollectedOldNewData as $value => $field) {
                $oldValue = $record->getValue($field);
                $newValue = $value;
                if (isset($this->collectOldNewData[$value])) {
                    // In the AW Blog for M1, after the removal of the category, does not deleted the links
                    // between category and store, category and post. Therefore this check has been added
                    if (isset($this->collectOldNewData[$value][$oldValue])) {
                        $newValue = $this->collectOldNewData[$value][$oldValue];
                    } else {
                        return false;
                    }
                }
                $record->setValue($field, $newValue);
            }
        }
        return $record;
    }

    /**
     * Save old new data
     *
     * @param string $sourceDocName
     * @param Record $record
     * @param int $insertId
     * @return null
     */
    public function saveOldNewData($sourceDocName, $record, $insertId)
    {
        if (isset($this->tableToCollectOldNewData[$sourceDocName])) {
            $oldValue = $record->getValue($this->tableToCollectOldNewData[$sourceDocName]);
            $this->collectOldNewData[$sourceDocName][$oldValue] = $insertId;
        }
    }

    /**
     * Add url_key param to array
     *
     * @param string $destinationName
     * @param Record $record
     * @param Record $destRecord
     * @param int $insertId
     * @return null
     */
    public function addUrlKeyForRewrite($destinationName, $record, $destRecord, $insertId)
    {
        $tableNamesForUrlRewrite = ['aw_blog_post', 'aw_blog_category'];
        if (in_array($destinationName, $tableNamesForUrlRewrite)) {
            $this->urlKeys[$destinationName][$insertId] = [
                'old' => $record->getValue('identifier'),
                'new' => $destRecord->getValue('url_key')
            ];
        }
    }

    /**
     * Add tag name param to array
     *
     * @param int $insertId
     * @param [] $postTagIds
     * @return null
     * @SuppressWarnings(PHPMD.UnusedLocalVariable)
     */
    public function addTagUrlKeyForRewrite($insertId, $postTagIds)
    {
        foreach ($postTagIds as $tagName => $tagId) {
            $this->urlKeys['aw_blog_tag'][$insertId] = [
                'old' => $tagName,
                'new' => $tagName
            ];
        }
    }

    /**
     * Create url rewrite
     *
     * @return null
     */
    public function createUrlRewrite()
    {
        $records = [];
        foreach ($this->urlKeys as $tableName => $tableData) {
            foreach ($tableData as $entityId => $row) {
                $entityData = $this->prepareDataBeforeCreateUrlRewrite($tableName, $entityId, $row);
                if ($entityData) {
                    foreach ($entityData as $row) {
                        $records[] = [
                            'entity_type' => 'custom',
                            'entity_id' => 0,
                            'request_path' => $row['request_path'],
                            'target_path' => $row['target_path'],
                            'redirect_type' => 301,
                            'store_id' => $row['store_id'],
                            'is_autogenerated' => 1
                        ];
                    }
                }
            }
        }
        $fieldsUpdateOnDuplicate = $this->getFieldsUpdateOnDuplicate($this->destinationUrlRewriteTable);
        $this->destination->saveRecords($this->destinationUrlRewriteTable, $records, $fieldsUpdateOnDuplicate);
    }

    /**
     * Prepare data before create url rewrite
     *
     * @param string $tableName
     * @param int $entityId
     * @param [] $row
     * @return []
     * @SuppressWarnings(PHPMD.CyclomaticComplexity)
     * @SuppressWarnings(PHPMD.NPathComplexity)
     */
    private function prepareDataBeforeCreateUrlRewrite($tableName, $entityId, $row)
    {
        $entityData = [];
        $storeIds = [0];
        $sourceRoute = $this->config->getOption('aw_blog_source_route_to_blog');
        $destinationRoute = $this->config->getOption('aw_blog_destination_route_to_blog');
        $urlTempates = [
            'aw_blog_post' => [
                'old' => '%s/%s',
                'new' => '%s/%s/'
            ],
            'aw_blog_category' => [
                'old' => '%s/cat/%s',
                'new' => '%s/%s/'
            ],
            'aw_blog_tag' => [
                'old' => '%s/tag/%s',
                'new' => '%s/tag/%s/'
            ],
            'default' => [
                'old' => '%s',
                'new' => '%s/'
            ]
        ];

        switch ($tableName) {
            case 'aw_blog_post':
                if ($row['old'] == $row['new'] && $sourceRoute == $destinationRoute) {
                    return $entityData;
                }
                $postData = $this->getPostDataById($entityId);
                $storeIds = $postData['store_ids'];
                break;
            case 'aw_blog_category':
                $categoryData = $this->getCategoryDataById($entityId);
                $storeIds = $categoryData['store_ids'];
                break;
            case 'aw_blog_tag':
                if ($sourceRoute == $destinationRoute) {
                    return $entityData;
                }
                $postData = $this->getPostDataById($entityId);
                $storeIds = $postData['store_ids'];
                break;
            default:
                if ($sourceRoute == $destinationRoute) {
                    return $entityData;
                }
                $tableName = 'default';
        }
        if (in_array(0, $storeIds)) {
            $storeIds = $this->getAllDestinationStoreIds();
        }
        foreach ($storeIds as $storeId) {
            $entityData[] = [
                'request_path' => isset($row['old'])
                    ? sprintf($urlTempates[$tableName]['old'], $sourceRoute, $row['old'])
                    : sprintf($urlTempates[$tableName]['old'], $sourceRoute),
                'target_path' => isset($row['old'])
                    ? sprintf($urlTempates[$tableName]['new'], $destinationRoute, $row['new'])
                    : sprintf($urlTempates[$tableName]['new'], $destinationRoute),
                'store_id' => $storeId
            ];
        }

        return $entityData;
    }

    /**
     * Get post data by id
     *
     * @param int $id
     * @return []
     */
    private function getPostDataById($id)
    {
        $adapter = $this->destination->getAdapter();
        $query = $adapter->getSelect()
                        ->from(
                            ['post' => $this->destination->addDocumentPrefix('aw_blog_post')],
                            []
                        )->joinLeft(
                            ['post_store' => $this->destination->addDocumentPrefix('aw_blog_post_store')],
                            'post_store.post_id = post.id',
                            ['store_id']
                        )->where('post.id = ?', $id);
        $postData['store_ids'] = $query->getAdapter()->fetchCol($query);
        return $postData;
    }

    /**
     * Get category data by id
     *
     * @param int $id
     * @return []
     */
    private function getCategoryDataById($id)
    {
        $adapter = $this->destination->getAdapter();
        $query = $adapter->getSelect()
                        ->from(
                            ['category' => $this->destination->addDocumentPrefix('aw_blog_category')],
                            []
                        )->joinLeft(
                            ['category_store' => $this->destination->addDocumentPrefix('aw_blog_category_store')],
                            'category_store.category_id = category.id',
                            ['store_id']
                        )->where('category.id = ?', $id);
        $categoryData['store_ids'] = $query->getAdapter()->fetchCol($query);
        return $categoryData;
    }

    /**
     * Get store ids by destination store table
     *
     * @return []
     */
    private function getAllDestinationStoreIds()
    {
        if ($this->destinationStoreIds == null) {
            $adapter = $this->destination->getAdapter();
            $query = $adapter->getSelect()
                ->from($this->destination->addDocumentPrefix('store'), ['store_id'])
                ->where('code != "admin"');
            $this->destinationStoreIds = $query->getAdapter()->fetchCol($query);
        }
        return $this->destinationStoreIds;
    }

    /**
     * Get fields update on duplicate
     *
     * @param string $documentName
     * @return []|bool
     */
    public function getFieldsUpdateOnDuplicate($documentName)
    {
        $updateOnDuplicate = [];
        if (array_key_exists($documentName, $this->documentsDuplicateOnUpdate)) {
            $updateOnDuplicate = explode(',', $this->documentsDuplicateOnUpdate[$documentName]);
        }
        return $updateOnDuplicate;
    }

    /**
     * Set destination count for table name
     *
     * @param string $destinationName
     * @return null
     */
    public function setDestinationCount($destinationName)
    {
        $this->destinationCount[$destinationName] = $this->destination->getRecordsCount($destinationName);
    }

    /**
     * Get destination count for table name
     *
     * @param string $destinationName
     * @return null
     */
    public function getDestinationCount($destinationName)
    {
        return isset($this->destinationCount[$destinationName]) ? $this->destinationCount[$destinationName] : 0;
    }
}
