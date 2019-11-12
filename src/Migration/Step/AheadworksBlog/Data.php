<?php
namespace Migration\Step\AheadworksBlog;

use Migration\App\Step\StageInterface;
use Migration\Handler;
use Migration\Reader\MapInterface;
use Migration\Reader\GroupsFactory;
use Migration\Reader\Map;
use Migration\Reader\MapFactory;
use Migration\ResourceModel;
use Migration\ResourceModel\Record;
use Migration\App\ProgressBar;
use Migration\Logger\Manager as LogManager;
use Migration\Logger\Logger;
use Migration\Config;
use Migration\ResourceModel\Document;

/**
 * Class Data
 * @SuppressWarnings(PHPMD.CouplingBetweenObjects)
 */
class Data extends \Migration\Step\DatabaseStage implements StageInterface
{
    /**
     * @var ResourceModel\Source
     */
    private $source;

    /**
     * @var ResourceModel\Destination
     */
    private $destination;

    /**
     * @var ResourceModel\RecordFactory
     */
    private $recordFactory;

    /**
     * @var Map
     */
    private $map;

    /**
     * @var \Migration\RecordTransformerFactory
     */
    private $recordTransformerFactory;

    /**
     * @var ProgressBar\LogLevelProcessor
     */
    private $progress;

    /**
     * @var Logger
     */
    private $logger;

    /**
     * @var Helper
     */
    private $helper;

    /**
     * @var \Migration\Reader\Groups
     */
    private $readerGroups;

    /**
     * @var Config
     */
    private $config;

    /**
     * @var bool
     */
    private $blogUpdate;

    /**
     * @param \Migration\Config $config
     * @param ProgressBar\LogLevelProcessor $progress
     * @param ResourceModel\Source $source
     * @param ResourceModel\Destination $destination
     * @param ResourceModel\RecordFactory $recordFactory
     * @param \Migration\RecordTransformerFactory $recordTransformerFactory
     * @param MapFactory $mapFactory
     * @param GroupsFactory $groupsFactory
     * @param Logger $logger
     * @param Helper $helper
     * @param Config $config
     * @SuppressWarnings(PHPMD.ExcessiveParameterList)
     */
    public function __construct(
        \Migration\Config $config,
        ProgressBar\LogLevelProcessor $progress,
        ResourceModel\Source $source,
        ResourceModel\Destination $destination,
        ResourceModel\RecordFactory $recordFactory,
        \Migration\RecordTransformerFactory $recordTransformerFactory,
        MapFactory $mapFactory,
        GroupsFactory $groupsFactory,
        Logger $logger,
        Helper $helper,
        Config $config
    ) {
        $this->source = $source;
        $this->destination = $destination;
        $this->recordFactory = $recordFactory;
        $this->recordTransformerFactory = $recordTransformerFactory;
        $this->map = $mapFactory->create('aw_blog_map_file');
        $this->progress = $progress;
        $this->readerGroups = $groupsFactory->create('aw_blog_groups_file');
        $this->logger = $logger;
        $this->helper = $helper;
        $this->config = $config;
        $this->blogUpdate = (bool)$this->config->getOption('aw_blog_update');
        parent::__construct($config);
    }

    /**
     * {@inheritdoc}
     * @SuppressWarnings(PHPMD.CyclomaticComplexity)
     * @SuppressWarnings(PHPMD.NPathComplexity)
     */
    public function perform()
    {
        $destinationAdapter = $this->destination->getAdapter()->getSelect()->getAdapter();
        $sourceDocuments = array_keys($this->readerGroups->getGroup('source_documents'));
        if (!$this->blogUpdate) {
            $this->helper->clearDestinationTagTables();
        }

        $this->progress->start(count($sourceDocuments), LogManager::LOG_LEVEL_INFO);
        foreach ($sourceDocuments as $sourceDocName) {
            $sourceDocument = $this->source->getDocument($sourceDocName);
            $destinationName = $this->map->getDocumentMap($sourceDocName, MapInterface::TYPE_SOURCE);
            if (!$destinationName) {
                continue;
            }
            $destDocument = $this->destination->getDocument($destinationName);
            if (!$this->blogUpdate) {
                $this->destination->clearDocument($destinationName);
            }
            $this->helper->setDestinationCount($destinationName);
            $this->logger->debug('migrating', ['table' => $sourceDocName]);
            $recordTransformer = $this->getRecordTransformer($sourceDocument, $destDocument);
            $pageNumber = 0;
            $this->progress->start(
                ceil($this->source->getRecordsCount($sourceDocName) / $this->source->getPageSize($sourceDocName)),
                LogManager::LOG_LEVEL_DEBUG
            );
            while (!empty($items = $this->source->getRecords($sourceDocName, $pageNumber))) {
                $pageNumber++;
                foreach ($items as $recordData) {
                    $this->source->setLastLoadedRecord($sourceDocName, $recordData);
                    /** @var Record $record */
                    $record = $this->recordFactory->create(['document' => $sourceDocument, 'data' => $recordData]);
                    /** @var Record $destRecord */
                    $destRecord = $this->recordFactory->create(['document' => $destDocument]);

                    if ($this->blogUpdate) {
                        if (!$record = $this->helper->replaceRecordData($sourceDocName, $record)) {
                            continue;
                        }
                    }
                    $recordTransformer->transform($record, $destRecord);
                    if ($this->blogUpdate) {
                        if ($destRecord->getValue('id')) {
                            $destRecord->setValue('id', null);
                        }
                    }
                    $this->progress->advance(LogManager::LOG_LEVEL_DEBUG);
                    $fieldsUpdateOnDuplicate = $this->helper->getFieldsUpdateOnDuplicate($destinationName);
                    $destinationAdapter->insertOnDuplicate(
                        $this->destination->addDocumentPrefix($destinationName),
                        $destRecord->getData(),
                        $fieldsUpdateOnDuplicate
                    );
                    $insertId = $destinationAdapter->lastInsertId();

                    // For migrate tags
                    if ($sourceDocName == 'aw_blog') {
                        $postTagIds = $this->helper->getPostTagIds($record->getValue('tags'));
                        $this->helper->savePostTag($insertId, $postTagIds);
                        $this->helper->addTagUrlKeyForRewrite($insertId, $postTagIds);
                    }
                    $this->helper->addUrlKeyForRewrite($destinationName, $record, $destRecord, $insertId);

                    if ($this->blogUpdate) {
                        $this->helper->saveOldNewData($sourceDocName, $record, $insertId);
                    }
                }
            }
            $this->source->setLastLoadedRecord($sourceDocName, []);
            $this->progress->advance(LogManager::LOG_LEVEL_INFO);
            $this->progress->finish(LogManager::LOG_LEVEL_DEBUG);
        }
        $this->helper->createUrlRewrite();
        $this->progress->finish(LogManager::LOG_LEVEL_INFO);
        return true;
    }

    /**
     * Get record transformer
     *
     * @param Document $sourceDocument
     * @param Document $destDocument
     * @return \Migration\RecordTransformer
     */
    public function getRecordTransformer(Document $sourceDocument, Document $destDocument)
    {
        /** @var \Migration\RecordTransformer $recordTransformer */
        $recordTransformer = $this->recordTransformerFactory->create(
            [
                'sourceDocument' => $sourceDocument,
                'destDocument' => $destDocument,
                'mapReader' => $this->map
            ]
        );
        $recordTransformer->init();
        return $recordTransformer;
    }
}
