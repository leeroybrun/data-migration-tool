<?php
/**
 * Copyright © Magento, Inc. All rights reserved.
 * See COPYING.txt for license details.
 */
namespace Migration\Step\Eav;

use Migration\App\Step\StageInterface;
use Migration\App\Step\RollbackInterface;
use Migration\Reader\MapInterface;
use Migration\Reader\GroupsFactory;
use Migration\Reader\MapFactory;
use Migration\Reader\Map;
use Migration\App\ProgressBar;
use Migration\ResourceModel\Destination;
use Migration\ResourceModel\Record;
use Migration\ResourceModel\Document;
use Migration\ResourceModel\RecordFactory;
use Migration\ResourceModel\Source;
use Migration\Step\Eav\Model\IgnoredAttributes;
use Migration\Step\Eav\Model\Data as ModelData;

/**
 * Class Data
 * @SuppressWarnings(PHPMD.CyclomaticComplexity)
 * @SuppressWarnings(PHPMD.CouplingBetweenObjects)
 * @SuppressWarnings(PHPMD.TooManyFields)
 * @SuppressWarnings(PHPMD.ExcessiveClassComplexity)
 * @SuppressWarnings(PHPMD.NPathComplexity)
 * @codeCoverageIgnoreStart
 */
class Data implements StageInterface, RollbackInterface
{
    /**
     * @var array;
     */
    private $newAttributeSets = [];

    /**
     * @var array;
     */
    private $mapAttributeIdsDestOldNew = [];

    /**
     * @var array;
     */
    private $mapAttributeIdsSourceDest = [];

    /**
     * @var array;
     */
    private $mapAttributeSetIdsDestOldNew = [];

    /**
     * @var array;
     */
    private $mapEntityTypeIdsDestOldNew = [];

    /**
     * @var array;
     */
    private $mapEntityTypeIdsSourceDest = [];

    /**
     * @var array;
     */
    private $defaultAttributeSetIds = [];

    /**
     * @var array;
     */
    private $mapAttributeGroupIdsSourceDest = [];

    /**
     * @var Helper
     */
    private $helper;

    /**
     * @var Source
     */
    private $source;

    /**
     * @var Destination
     */
    private $destination;

    /**
     * @var Map
     */
    private $map;

    /**
     * @var RecordFactory
     */
    private $factory;

    /**
     * @var InitialData
     */
    private $initialData;

    /**
     * @var IgnoredAttributes
     */
    private $ignoredAttributes;

    /**
     * @var ProgressBar\LogLevelProcessor
     */
    private $progress;

    /**
     * @var \Migration\Reader\Groups
     */
    private $readerGroups;

    protected $attrIdsMigrated = [];
    protected $attrIdsKept = [];
    protected $sourceAttributesSetIgnored = array();
    protected $sourceAttributesGroupIgnored = array();
    protected $attributeSetIdsKept = array();
    protected $attributeGroupIdsKept = array();

    protected $mapAttrIdsMigrated = array();
    protected $mapAttrSetIdsMigrated = array();
    protected $mapAttrGroupIdsMigrated = array();
    protected $mapAttrIdsKept = array();
    protected $mapAttrSetIdsKept = array();
    protected $mapAttrGroupIdsKept = array();

    /**
     * @var ModelData
     */
    private $modelData;

    /**
     * @var array
     */
    private $mapProductAttributeGroupNamesSourceDest = [
        'General' => 'Product Details',
        'Prices' => 'Product Details',
        'Recurring Profile' => 'Product Details'
    ];

    protected $csvProcessor;
    protected $dir;

    /**
     * @param Source $source
     * @param Destination $destination
     * @param MapFactory $mapFactory
     * @param GroupsFactory $groupsFactory
     * @param Helper $helper
     * @param RecordFactory $factory
     * @param InitialData $initialData
     * @param IgnoredAttributes $ignoredAttributes
     * @param ProgressBar\LogLevelProcessor $progress
     * @param ModelData $modelData
     */
    public function __construct(
        Source $source,
        Destination $destination,
        MapFactory $mapFactory,
        GroupsFactory $groupsFactory,
        Helper $helper,
        RecordFactory $factory,
        InitialData $initialData,
        IgnoredAttributes $ignoredAttributes,
        ProgressBar\LogLevelProcessor $progress,
<<<<<<< HEAD
        \Magento\Framework\File\Csv $csvProcessor,
        \Magento\Framework\Filesystem\DirectoryList $dir
=======
        ModelData $modelData
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
    ) {
        $this->source = $source;
        $this->destination = $destination;
        $this->map = $mapFactory->create('eav_map_file');
        $this->readerGroups = $groupsFactory->create('eav_document_groups_file');
        $this->helper = $helper;
        $this->factory = $factory;
        $this->initialData = $initialData;
        $this->ignoredAttributes = $ignoredAttributes;
        $this->progress = $progress;
<<<<<<< HEAD
        $this->csvProcessor = $csvProcessor;
        $this->dir = $dir;
=======
        $this->modelData = $modelData;
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
    }

    /**
     * Entry point. Run migration of EAV structure.
     *
     * @return bool
     */
    public function perform()
    {
        $this->progress->start(7);
        $this->migrateEntityTypes();
        $this->migrateAttributeSets();
        $this->createProductAttributeSetStructures();
        $this->migrateCustomProductAttributeGroups();
        $this->migrateAttributes();
        $this->migrateAttributesExtended();
<<<<<<< HEAD
        $this->migrateEntityAttributes();
        $this->migrateOtherEntityAttributes();
=======
        $this->migrateCustomEntityAttributes();
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
        $this->progress->finish();
        return true;
    }

    /**
     * Migrate Entity Type table
     *
     * @return void
     */
    private function migrateEntityTypes()
    {
        $this->progress->advance();
        $documentName = 'eav_entity_type';
        $mappingField = 'entity_type_code';
        $sourceDocument = $this->source->getDocument($documentName);
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap($documentName, MapInterface::TYPE_SOURCE)
        );
        $this->destination->backupDocument($destinationDocument->getName());
        $destinationRecords = $this->helper->getDestinationRecords($documentName, [$mappingField]);
        $recordsToSave = $destinationDocument->getRecords();
        $recordTransformer = $this->helper->getRecordTransformer($sourceDocument, $destinationDocument);
        foreach ($this->helper->getSourceRecords($documentName) as $recordData) {
            /** @var Record $sourceRecord */
            $sourceRecord = $this->factory->create(['document' => $sourceDocument, 'data' => $recordData]);
            /** @var Record $destinationRecord */
            $destinationRecord = $this->factory->create(['document' => $destinationDocument]);
            $mappingValue = $sourceRecord->getValue($mappingField);
            if (isset($destinationRecords[$mappingValue])) {
                $destinationRecordData = $destinationRecords[$mappingValue];
                unset($destinationRecords[$mappingValue]);
            } else {
                $destinationRecordData = $destinationRecord->getDataDefault();
            }
            $destinationRecord->setData($destinationRecordData);
            $recordTransformer->transform($sourceRecord, $destinationRecord);
            $recordsToSave->addRecord($destinationRecord);
        }
        $this->destination->clearDocument($destinationDocument->getName());
        $this->saveRecords($destinationDocument, $recordsToSave);

        $recordsToSave = $destinationDocument->getRecords();
        foreach ($destinationRecords as $record) {
            $record['entity_type_id'] = null;
            $destinationRecord = $this->factory->create([
                'document' => $destinationDocument,
                'data' => $record
            ]);
            $recordsToSave->addRecord($destinationRecord);
        }
        $this->saveRecords($destinationDocument, $recordsToSave);
        $this->createMapEntityTypeIds();
    }

    /**
     * Migrate attribute set table
     *
     * @return void
     */
    private function migrateAttributeSets()
    {
<<<<<<< HEAD
        foreach (['eav_attribute_set', 'eav_attribute_group'] as $documentName) {
            $this->progress->advance();
            $sourceDocument = $this->source->getDocument($documentName);
            $destinationDocument = $this->destination->getDocument(
                $this->map->getDocumentMap($documentName, MapInterface::TYPE_SOURCE)
            );

            $this->destination->backupDocument($destinationDocument->getName());

            $sourceRecords = $this->source->getRecords($documentName, 0, $this->source->getRecordsCount($documentName));
            $recordsToSave = $destinationDocument->getRecords();
            $recordTransformer = $this->helper->getRecordTransformer($sourceDocument, $destinationDocument);

            // We keep attribute sets/groups for products from destination
            $dstRecords = $this->destination->getRecords($documentName, 0, $this->destination->getRecordsCount($documentName));
            foreach ($dstRecords as $recordData) {
                if($documentName == 'eav_attribute_set' && isset($recordData['entity_type_id']) && $recordData['entity_type_id'] == 4) {
                    $oldSetId = $recordData['attribute_set_id'];
                    //$recordData['attribute_set_id'] = null;
                    $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $recordData]);
                    $recordsToSave->addRecord($destinationRecord);
                    $this->attributeSetIdsKept[$oldSetId] = true;
                } else if($documentName == 'eav_attribute_group' && isset($this->attributeSetIdsKept[$recordData['attribute_set_id']])) {
                    $oldGroupId = $recordData['attribute_group_id'];
                    $recordData['attribute_group_id'] = null;
                    $recordData['attribute_set_id'] = $this->mapAttrSetIdsKept[$recordData['attribute_set_id']];
                    
                    $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $recordData]);
                    $recordsToSave->addRecord($destinationRecord);
                    $this->attributeGroupIdsKept[$oldGroupId] = true;
                }
            }

            // Migrate attribute sets/groups from source (without the ones used by products)
            foreach ($sourceRecords as $recordData) {
                if(($documentName == 'eav_attribute_set' && isset($recordData['entity_type_id']) && $recordData['entity_type_id'] == 4) 
                    || ($documentName == 'eav_attribute_group' && isset($recordData['attribute_set_id']) && isset($this->sourceAttributesSetIgnored[$recordData['attribute_set_id']]))) {
                        if($documentName == 'eav_attribute_set') {
                            $this->sourceAttributesSetIgnored[$recordData['attribute_set_id']] = true;
                        } else if($documentName == 'eav_attribute_group') {
                            $this->sourceAttributesGroupIgnored[$recordData['attribute_group_id']] = true;
                        }
                    continue;
                }

                // Reset ID
                if($documentName == 'eav_attribute_set') {
                    //$recordData['attribute_set_id'] = null;
                } else if($documentName == 'eav_attribute_group') {
                    $recordData['attribute_group_id'] = null;
                    $recordData['attribute_set_id'] = $this->mapAttrSetIdsMigrated[$recordData['attribute_set_id']];
                } 

                $sourceRecord = $this->factory->create(['document' => $sourceDocument, 'data' => $recordData]);

                $destinationRecord = $this->factory->create(['document' => $destinationDocument]);
                $recordTransformer->transform($sourceRecord, $destinationRecord);

                $recordsToSave->addRecord($destinationRecord);
            }

            /*/ Keep attributes set from destination (except the ones for products), but with a new ID
            if ($documentName == 'eav_attribute_set') {
                foreach ($this->initialData->getAttributeSets('dest') as $record) {
                    if($record['entity_type_id'] == 4 || $record['entity_type_id'] == "4") {
                        continue;
                    }

                    $record['attribute_set_id'] = null;
                    $record['entity_type_id'] = $this->mapEntityTypeIdsDestOldNew[$record['entity_type_id']];
                    $destinationRecord = $this->factory->create(
                        [
                            'document' => $destinationDocument,
                            'data' => $record
                        ]
                    );

                    $recordsToSave->addRecord($destinationRecord);
                }
            }

            // Keep attributes groups from destination (except the ones for products), but with a new ID
            if ($documentName == 'eav_attribute_group') {
                foreach ($this->initialData->getAttributeGroups('dest') as $record) {
                    $oldAttributeSet = $this->initialData->getAttributeSets('dest')[$record['attribute_set_id']];
                    if($oldAttributeSet['entity_type_id'] == 4 || $oldAttributeSet['entity_type_id'] == "4") {
                        continue;
                    }

                    $entityTypeId = $this->mapEntityTypeIdsDestOldNew[$oldAttributeSet['entity_type_id']];
                    $newAttributeSet = $this->newAttributeSets[
                        $entityTypeId . '-' . $oldAttributeSet['attribute_set_name']
                    ];
                    $record['attribute_set_id'] = $newAttributeSet['attribute_set_id'];

                    $record['attribute_group_id'] = null;
                    $destinationRecord = $this->factory->create(
                        [
                            'document' => $destinationDocument,
                            'data' => $record
                        ]
                    );

                    $recordsToSave->addRecord($destinationRecord);
                }
                $recordsToSave = $this->addAttributeGroups($recordsToSave, $documentName, $this->groupsDataToAdd);
            }*/

            $recordsToSave = $this->addAttributeGroups($recordsToSave, $documentName, $this->groupsDataToAdd);
=======
        $this->progress->advance();
        $documentName = 'eav_attribute_set';
        $sourceDocument = $this->source->getDocument($documentName);
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap($documentName, MapInterface::TYPE_SOURCE)
        );
        $this->destination->backupDocument($destinationDocument->getName());
        $destinationRecords = $this->helper->getDestinationRecords(
            $documentName,
            ['entity_type_id', 'attribute_set_name']
        );
        $sourceRecords = $this->source->getRecords($documentName, 0, $this->source->getRecordsCount($documentName));
        $recordsToSave = $destinationDocument->getRecords();
        $recordTransformer = $this->helper->getRecordTransformer($sourceDocument, $destinationDocument);
        foreach ($sourceRecords as $recordData) {
            $sourceRecord = $this->factory->create(['document' => $sourceDocument, 'data' => $recordData]);
            $destinationRecord = $this->factory->create(['document' => $destinationDocument]);
            $mappedKey = null;
            $entityTypeId = $sourceRecord->getValue('entity_type_id');
            if (isset($this->mapEntityTypeIdsSourceDest[$entityTypeId])) {
                $mappedId = $this->mapEntityTypeIdsSourceDest[$entityTypeId];
                $mappedKey = $mappedId . '-' . $sourceRecord->getValue('attribute_set_name');
            }
            if ($mappedKey && isset($destinationRecords[$mappedKey])) {
                unset($destinationRecords[$mappedKey]);
            }
            $destinationRecordData = $destinationRecord->getDataDefault();
            $destinationRecord->setData($destinationRecordData);
            $recordTransformer->transform($sourceRecord, $destinationRecord);
            $recordsToSave->addRecord($destinationRecord);
        }
        $this->destination->clearDocument($destinationDocument->getName());
        $this->saveRecords($destinationDocument, $recordsToSave);
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8

        $recordsToSave = $destinationDocument->getRecords();
        foreach ($destinationRecords as $recordData) {
            /** @var Record $destinationRecord */
            $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $recordData]);
            $destinationRecord->setValue('attribute_set_id', null);
            $destinationRecord->setValue(
                'entity_type_id',
                $this->mapEntityTypeIdsDestOldNew[$destinationRecord->getValue('entity_type_id')]
            );
            $recordsToSave->addRecord($destinationRecord);
        }
        $this->saveRecords($destinationDocument, $recordsToSave);
        $this->createMapAttributeSetIds();
    }

    /**
     * Take Default attribute set structure and duplicate it for all  attribute sets from Magento 1
     */
    private function createProductAttributeSetStructures()
    {
<<<<<<< HEAD
        $this->helper->setAddedGroups([]);
=======
        $this->progress->advance();
        $documentName = 'eav_attribute_group';
        $this->destination->backupDocument($documentName);
        $this->modelData->updateMappedKeys(
            $documentName,
            'attribute_set_id',
            $this->helper->getDestinationRecords($documentName),
            $this->mapAttributeSetIdsDestOldNew
        );
        // add default attribute groups from Magento 2 for each attribute set from Magento 1
        $prototypeProductAttributeGroups = $this->modelData->getDefaultProductAttributeGroups();
        $productAttributeSets = $this->modelData->getProductAttributeSets(
            ModelData::ATTRIBUTE_SETS_NONE_DEFAULT
        );
        foreach ($productAttributeSets as $attributeSet) {
            foreach ($prototypeProductAttributeGroups as &$prototypeAttributeGroup) {
                $prototypeAttributeGroup['attribute_set_id'] = $attributeSet['attribute_set_id'];
            }
            $this->saveRecords($documentName, $prototypeProductAttributeGroups);
        }
        // update mapped keys
        $entityAttributeDocument = 'eav_entity_attribute';
        $this->destination->backupDocument($documentName);
        $this->modelData->updateMappedKeys(
            $entityAttributeDocument,
            'attribute_set_id',
            $this->helper->getDestinationRecords($entityAttributeDocument),
            $this->mapAttributeSetIdsDestOldNew
        );
        // add default entity attributes from Magento 2 for each attribute set from Magento 1
        foreach ($productAttributeSets as $attributeSet) {
            $prototypeProductEntityAttributes = $this->modelData->getDefaultProductEntityAttributes();
            foreach ($prototypeProductEntityAttributes as &$prototypeEntityAttribute) {
                $attributeGroupId = $this->modelData->getAttributeGroupIdForAttributeSet(
                    $prototypeEntityAttribute['attribute_group_id'],
                    $attributeSet['attribute_set_id']
                );
                $prototypeEntityAttribute['attribute_set_id'] = $attributeSet['attribute_set_id'];
                $prototypeEntityAttribute['attribute_group_id'] = $attributeGroupId;
            }
            $this->saveRecords($entityAttributeDocument, $prototypeProductEntityAttributes);
        }
    }
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8

    /**
     * Migrate custom product attribute groups
     */
    public function migrateCustomProductAttributeGroups()
    {
        $this->progress->advance();
        $productAttributeSets = $this->modelData->getProductAttributeSets();
        foreach ($productAttributeSets as $productAttributeSet) {
            $attributeGroupIds = $this->modelData->getCustomProductAttributeGroups(
                $productAttributeSet['attribute_set_id']
            );
            if ($attributeGroupIds) {
                $this->migrateAttributeGroups($attributeGroupIds);
            }
        }
        $this->createMapProductAttributeGroupIds();
    }

    /**
     * Migrate attribute groups
     *
     * @param $attributeGroupIds
     */
    private function migrateAttributeGroups($attributeGroupIds)
    {
        $this->progress->advance();
        $documentName = 'eav_attribute_group';
        $sourceDocument = $this->source->getDocument($documentName);
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap($documentName, MapInterface::TYPE_SOURCE)
        );
        $sourceRecords = $this->source->getRecords(
            $documentName,
            0,
            $this->source->getRecordsCount($documentName),
            new \Zend_Db_Expr(sprintf('attribute_group_id IN (%s)', implode(',', $attributeGroupIds)))
        );
        $recordsToSave = $destinationDocument->getRecords();
<<<<<<< HEAD
        $entityTypesMigrated = $this->helper->getDestinationRecords($destinationDocument->getName());
        foreach ($entityTypesMigrated as $record) {
                if (isset($this->defaultAttributeSetIds[$record['entity_type_id']])
                    && !in_array($record['entity_type_code'], $exceptions)
                ) {
                    $record['default_attribute_set_id'] =
                        $this->defaultAttributeSetIds[$record['entity_type_id']];
                }
                $destinationRecord = $this->factory->create(
                    [
                        'document' => $destinationDocument,
                        'data' => $record
                    ]
                );
                $recordsToSave->addRecord($destinationRecord);
=======
        $recordTransformer = $this->helper->getRecordTransformer($sourceDocument, $destinationDocument);
        foreach ($sourceRecords as $recordData) {
            $recordData['attribute_group_id'] = null;
            $sourceRecord = $this->factory->create(['document' => $sourceDocument, 'data' => $recordData]);
            $destinationRecord = $this->factory->create(['document' => $destinationDocument]);
            $recordTransformer->transform($sourceRecord, $destinationRecord);
            $recordsToSave->addRecord($destinationRecord);
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
        }
        $this->saveRecords($destinationDocument, $recordsToSave);
    }

    /**
     * Migrate eav_attribute
     *
     * @return void
     */
    private function migrateAttributes()
    {
        $this->progress->advance();
        $sourceDocName = 'eav_attribute';
        $sourceDocument = $this->source->getDocument($sourceDocName);
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap($sourceDocName, MapInterface::TYPE_SOURCE)
        );
        $this->destination->backupDocument($destinationDocument->getName());
<<<<<<< HEAD
        $sourceRecords = $this->ignoredAttributes->clearIgnoredAttributes($this->initialData->getAttributes('source'));
        $destinationRecords = $this->initialData->getAttributes('dest');

        $migratedAttributesMap = array();

        // Migrate attributes from source (except the ones for products), with new IDs
=======
        $sourceRecords = $this->ignoredAttributes->clearIgnoredAttributes(
            $this->initialData->getAttributes(ModelData::TYPE_SOURCE)
        );
        $destinationRecords = $this->helper->getDestinationRecords(
            $sourceDocName,
            ['entity_type_id', 'attribute_code']
        );
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
        $recordsToSave = $destinationDocument->getRecords();
        $recordTransformer = $this->helper->getRecordTransformer($sourceDocument, $destinationDocument);
        foreach ($sourceRecords as $sourceRecordData) {
            /** @var Record $sourceRecord */
            $sourceRecord = $this->factory->create(['document' => $sourceDocument, 'data' => $sourceRecordData]);
            /** @var Record $destinationRecord */
            $destinationRecord = $this->factory->create(['document' => $destinationDocument]);
            $mappedKey = null;
            $entityTypeId = $sourceRecord->getValue('entity_type_id');
            if($entityTypeId == 4) {
                continue;
            }

            if (isset($this->mapEntityTypeIdsSourceDest[$entityTypeId])) {
                $mappedId = $this->mapEntityTypeIdsSourceDest[$entityTypeId];
                $mappedKey = $mappedId . '-' . $sourceRecord->getValue('attribute_code');
            }
            if ($mappedKey && isset($destinationRecords[$mappedKey])) {
                $destinationRecordData = $destinationRecords[$mappedKey];
                $destinationRecordData['attribute_id'] = $sourceRecordData['attribute_id'];
                $destinationRecordData['entity_type_id'] = $sourceRecordData['entity_type_id'];
                $destinationRecord->setData($destinationRecordData);
                unset($destinationRecords[$mappedKey]);
            } else {
                $destinationRecordData = $destinationRecord->getDataDefault();
                $destinationRecord->setData($destinationRecordData);
                $recordTransformer->transform($sourceRecord, $destinationRecord);
            }
<<<<<<< HEAD
            $oldAttrId = $sourceRecord->getValue('attribute_id');
            $destinationRecord->setData($destinationRecordData);

            $this->helper->getRecordTransformer($sourceDocument, $destinationDocument)
                ->transform($sourceRecord, $destinationRecord);
            
            if($mappedKey) {
                $migratedAttributesMap[$mappedKey] = true;
            } else {
                $migratedAttributesMap[$entityTypeId. '-' . $sourceRecord->getValue('attribute_code')] = true;
            }
            $destinationRecord->setValue('attribute_id', null);
            
            $this->attrIdsMigrated[$oldAttrId] = true;
=======
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
            $recordsToSave->addRecord($destinationRecord);
        }

        // Keep attributes for products in destination, but with new IDs
        $dstRecords = $this->destination->getRecords($sourceDocName, 0, $this->destination->getRecordsCount($sourceDocName));
        foreach ($dstRecords as $recordData) {
            if($recordData['entity_type_id'] == 4) {
                $oldAttrId = $recordData['attribute_id'];
                $recordData['attribute_id'] = null;
                $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $recordData]);
                
                $this->attrIdsKept[$oldAttrId] = true;
                $recordsToSave->addRecord($destinationRecord);
            }
        }

        // Keep other attributes from destination, except the ones for products or ones already migrated from source, but with new IDs
        foreach ($destinationRecords as $record) {
            /** @var Record $destinationRecord */
            $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $record]);
            if($destinationRecord->getValue('entity_type_id') == 4 || $destinationRecord->getValue('entity_type_id') == "4") {
                continue;
            }

            // If attribute was already migrated from source, skip it
            if(isset($migratedAttributesMap[$destinationRecord->getValue('entity_type_id'). '-' . $destinationRecord->getValue('attribute_code')])) {
                continue;
            }

            $oldAttrId = $record['attribute_id'];
            $destinationRecord->setValue('attribute_id', null);
            $destinationRecord->setValue(
                'entity_type_id',
                isset($this->mapEntityTypeIdsDestOldNew[$destinationRecord->getValue('entity_type_id')]) ? $this->mapEntityTypeIdsDestOldNew[$destinationRecord->getValue('entity_type_id')] : $destinationRecord->getValue('entity_type_id')
            );
            
            $this->attrIdsKept[$oldAttrId] = true;
            $recordsToSave->addRecord($destinationRecord);
        }
        $this->destination->clearDocument($destinationDocument->getName());
        $this->saveRecords($destinationDocument, $recordsToSave);

        $this->createMapAttributeIds();
        $mapAttributeIdsDestOldNew = [];
        foreach ($destinationRecords as $record) {
            if (isset($this->mapAttributeIdsDestOldNew[$record['attribute_id']])) {
                $mapAttributeIdsDestOldNew[$record['attribute_id']] =
                    $this->mapAttributeIdsDestOldNew[$record['attribute_id']];
            }
        }
        $mapAttributeIds = array_flip($this->mapAttributeIdsSourceDest);
        $mapAttributeIds = array_replace($mapAttributeIds, $mapAttributeIdsDestOldNew);
        $this->modelData->updateMappedKeys(
            'eav_entity_attribute',
            'attribute_id',
            $this->helper->getDestinationRecords('eav_entity_attribute'),
            $mapAttributeIds
        );
    }

    /**
<<<<<<< HEAD
     * Migrate eav_entity_attributes and other entity tables
     *
     * @return void
     */
    protected function migrateOtherEntityAttributes()
    {
        foreach ([
            'catalog_category_entity_datetime', 'catalog_category_entity_decimal', 'catalog_category_entity_int', 'catalog_category_entity_text', 'catalog_category_entity_varchar', 
            /*'catalog_eav_attribute',*/ 'eav_attribute_label', 'eav_attribute_option', 
            'customer_address_entity_datetime', 'customer_address_entity_decimal', 'customer_address_entity_int', 'customer_address_entity_text', 'customer_address_entity_varchar', 
            /*'customer_eav_attribute',*/ 'customer_eav_attribute_website', 'customer_entity_datetime', 'customer_entity_decimal', 'customer_entity_int', 'customer_entity_text', 'customer_entity_varchar', 'customer_form_attribute',
            'eav_entity_datetime', 'eav_entity_decimal', 'eav_entity_int', 'eav_entity_text', 'eav_entity_varchar', 'eav_form_element'
        ] as $sourceDocName) {
            $this->progress->advance();
            $sourceDocument = $this->source->getDocument($sourceDocName);
            $destinationDocument = $this->destination->getDocument(
                $this->map->getDocumentMap($sourceDocName, MapInterface::TYPE_SOURCE)
            );
            $this->destination->backupDocument($destinationDocument->getName());
            $recordsToSave = $destinationDocument->getRecords();

            $migratedAttrEntitiesMap = array();

            // Migrate attributes entities from source, but only the ones from attributes we already migrated 
            foreach ($this->helper->getSourceRecords($sourceDocName) as $sourceRecordData) {
                if(isset($this->attrIdsMigrated[$sourceRecordData['attribute_id']])) {

                    if (!isset($this->mapAttrIdsMigrated[$sourceRecordData['attribute_id']])) {
                        var_dump('ignore source attribute entity because not in maps', $sourceRecordData);
                        continue;
                    }
        
                    $oldAttrId = $sourceRecordData['attribute_id'];

                    $sourceRecord = $this->factory->create([
                        'document' => $sourceDocument,
                        'data' => $sourceRecordData
                    ]);

                    $destinationRecord = $this->factory->create(['document' => $destinationDocument]);
                    $this->helper->getRecordTransformer($sourceDocument, $destinationDocument)
                        ->transform($sourceRecord, $destinationRecord);

                    if(isset($sourceRecordData['attribute_id'])) {
                        $destinationRecord->setValue('attribute_id', $this->mapAttrIdsMigrated[$sourceRecordData['attribute_id']]);
                    }

                    if(isset($sourceRecordData['attribute_set_id'])) {
                        $destinationRecord->setValue('attribute_set_id', $this->mapAttrSetIdsMigrated[$sourceRecordData['attribute_set_id']]);
                    }

                    if(isset($sourceRecordData['attribute_group_id'])) {
                        $destinationRecord->setValue('attribute_group_id', $this->mapAttrGroupIdsMigrated[$sourceRecordData['attribute_group_id']]);
                    }

                    //$destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $sourceRecordData]);                    
                    
                    $recordsToSave->addRecord($destinationRecord);
                }
            }

            /*/ Keep attributes entities from destination for products
            $dstRecords = $this->destination->getRecords($sourceDocName, 0, $this->destination->getRecordsCount($sourceDocName));
            foreach ($dstRecords as $recordData) {
                if(isset($this->attrIdsKept[$recordData['attribute_id']])) {
                    if (!isset($this->mapAttrIdsKept[$recordData['attribute_id']])) {
                        var_dump('ignore dest product attribute entity because not in maps', $recordData);
                        var_dump(isset($this->mapAttrIdsKept[$recordData['attribute_id']]));
                        continue;
                    }
        
                    $oldAttrId = $recordData['attribute_id'];
        
                    if(isset($recordData['attribute_id'])) {
                        $recordData['attribute_id'] = $this->mapAttrIdsKept[$recordData['attribute_id']];
                    }
        
                    if(isset($recordData['attribute_set_id'])) {
                        $recordData['attribute_set_id'] = $this->mapAttrSetIdsKept[$recordData['attribute_set_id']];
                    }
        
                    if(isset($recordData['attribute_group_id'])) {
                        $recordData['attribute_group_id'] = $this->mapAttrGroupIdsKept[$recordData['attribute_group_id']];
                    }

                    $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $recordData]);
                    $recordsToSave->addRecord($destinationRecord);
                }
            }*/

            // Keep attributes entities from destination for other entity types (except products)
            foreach ($this->helper->getDestinationRecords($sourceDocName) as $record) {
                if(!isset($this->attrIdsKept[$record['attribute_id']])) {
                    continue;
                }
                
                if (!isset($this->mapAttrIdsKept[$record['attribute_id']])) {
                    var_dump('ignore dest attribute entity because not in maps', $record);
                    var_dump(isset($this->mapAttrIdsKept[$record['attribute_id']]));
                    continue;
                }

                $prevAttrId = $record['attribute_id'];

                if(isset($recordData['attribute_id'])) {
                    $record['attribute_id'] = $this->mapAttrIdsKept[$record['attribute_id']];
                }
        
                if(isset($recordData['attribute_set_id'])) {
                    $recordData['attribute_set_id'] = $this->mapAttrSetIdsKept[$recordData['attribute_set_id']];
                }
    
                if(isset($recordData['attribute_group_id'])) {
                    $recordData['attribute_group_id'] = $this->mapAttrGroupIdsKept[$recordData['attribute_group_id']];
                }

                $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $record]);
                
                $recordsToSave->addRecord($destinationRecord);
            }

            $this->destination->clearDocument($destinationDocument->getName());
            $this->saveRecords($destinationDocument, $recordsToSave);
        }
    }

    /**
     * Migrate eav_entity_attributes and other entity tables
     *
     * @return void
=======
     * Migrate custom entity attributes
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
     */
    private function migrateCustomEntityAttributes()
    {
        $this->progress->advance();
        $sourceDocName = 'eav_entity_attribute';
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap($sourceDocName, MapInterface::TYPE_SOURCE)
        );
        $recordsToSave = $destinationDocument->getRecords();
<<<<<<< HEAD

        $migratedAttrEntitiesMap = array();

        // Migrate attributes entities from source, but only the ones from attributes we already migrated and linked to sets/groups also migrated
        foreach ($this->helper->getSourceRecords($sourceDocName) as $sourceRecordData) {
            if(isset($this->attrIdsMigrated[$sourceRecordData['attribute_id']]) 
                /*&& (isset($this->attributeGroupIdsKept[$sourceRecordData['attribute_group_id']]) && isset($this->sourceAttributesGroupIgnored[$sourceRecordData['attribute_group_id']])
                    || !isset($this->sourceAttributesGroupIgnored[$sourceRecordData['attribute_group_id']]) )
                && (isset($this->attributeSetIdsKept[$sourceRecordData['attribute_set_id']]) && isset($this->sourceAttributesSetIgnored[$sourceRecordData['attribute_set_id']])
                    || !isset($this->sourceAttributesSetIgnored[$sourceRecordData['attribute_set_id']]) )*/) {

                if (!isset($this->mapAttrIdsMigrated[$sourceRecordData['attribute_id']])
                    || !isset($this->mapAttrSetIdsMigrated[$sourceRecordData['attribute_set_id']])
                    || !isset($this->mapAttrGroupIdsMigrated[$sourceRecordData['attribute_group_id']])
                    || !isset($this->mapEntityTypeIdsDestOldNew[$sourceRecordData['entity_type_id']])
                ) {
                    var_dump('ignore source attribute entity because not in maps', $sourceRecordData);
                    var_dump(
                        isset($this->mapAttrIdsMigrated[$sourceRecordData['attribute_id']]),
                        isset($this->mapAttrSetIdsMigrated[$sourceRecordData['attribute_set_id']]),
                        isset($this->mapAttrGroupIdsMigrated[$sourceRecordData['attribute_group_id']]),
                        isset($this->mapEntityTypeIdsDestOldNew[$sourceRecordData['entity_type_id']])
                    );
                    continue;
                }
    
                $oldAttrId = $sourceRecordData['attribute_id'];

                $migratedAttrEntitiesMap[$sourceRecordData['attribute_id'].'-'.$sourceRecordData['attribute_set_id'].'-'.$sourceRecordData['attribute_group_id'].'-'.$sourceRecordData['entity_type_id']] = true;

                $sourceRecord = $this->factory->create([
                    'document' => $sourceDocument,
                    'data' => $sourceRecordData
                ]);
                $destinationRecord = $this->factory->create(['document' => $destinationDocument]);
                $this->helper->getRecordTransformer($sourceDocument, $destinationDocument)
                    ->transform($sourceRecord, $destinationRecord);
                
                $destinationRecord->setValue('attribute_id', $this->mapAttrIdsMigrated[$sourceRecordData['attribute_id']]);
                $destinationRecord->setValue('attribute_set_id', $this->mapAttrSetIdsMigrated[$sourceRecordData['attribute_set_id']]);
                $destinationRecord->setValue('attribute_group_id', $this->mapAttrGroupIdsMigrated[$sourceRecordData['attribute_group_id']]);
                $destinationRecord->setValue('entity_type_id', $this->mapEntityTypeIdsDestOldNew[$sourceRecordData['entity_type_id']]);
    
                $sourceRecordData['entity_attribute_id'] = null;
                $recordsToSave->addRecord($destinationRecord);
            }
        }

        // Keep attributes entities from destination for products
        $dstRecords = $this->destination->getRecords($sourceDocName, 0, $this->destination->getRecordsCount($sourceDocName));
        foreach ($dstRecords as $recordData) {
            if($recordData['entity_type_id'] == 4 && isset($this->attrIdsKept[$recordData['attribute_id']])) {
                if (!isset($this->mapAttrIdsKept[$recordData['attribute_id']])
                    || !isset($this->mapAttrSetIdsKept[$recordData['attribute_set_id']])
                    || !isset($this->mapAttrGroupIdsKept[$recordData['attribute_group_id']])
                    || !isset($this->mapEntityTypeIdsDestOldNew[$recordData['entity_type_id']])
                ) {
                    var_dump('ignore dest product attribute entity because not in maps', $recordData);
                    var_dump(
                        isset($this->mapAttrIdsKept[$recordData['attribute_id']]),
                        isset($this->mapAttrSetIdsKept[$recordData['attribute_set_id']]),
                        isset($this->mapAttrGroupIdsKept[$recordData['attribute_group_id']]),
                        isset($this->mapEntityTypeIdsDestOldNew[$recordData['entity_type_id']])
                    );
                    continue;
                }
    
                $oldAttrId = $recordData['attribute_id'];
    
                $recordData['attribute_id'] = $this->mapAttrIdsKept[$recordData['attribute_id']];
                $recordData['attribute_set_id'] = $this->mapAttrSetIdsKept[$recordData['attribute_set_id']];
                $recordData['attribute_group_id'] = $this->mapAttrGroupIdsKept[$recordData['attribute_group_id']];
                $recordData['entity_type_id'] = $this->mapEntityTypeIdsDestOldNew[$recordData['entity_type_id']];
    
                $recordData['entity_attribute_id'] = null;

                $migratedAttrEntitiesMap[$recordData['attribute_id'].'-'.$recordData['attribute_set_id'].'-'.$recordData['attribute_group_id'].'-'.$recordData['entity_type_id']] = true;

                $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $recordData]);
                $recordsToSave->addRecord($destinationRecord);
            }
        }

        // Keep attributes entities from destination for other entity types (except products)
        foreach ($this->helper->getDestinationRecords('eav_entity_attribute') as $record) {
            if($record['entity_type_id'] == 4 || !isset($this->attrIdsKept[$record['attribute_id']]) || isset($migratedAttrEntitiesMap[$record['attribute_id'].'-'.$record['attribute_set_id'].'-'.$record['attribute_group_id'].'-'.$record['entity_type_id']])) {
                continue;
            }
            
            if (!isset($this->mapAttrIdsKept[$record['attribute_id']])
                || !isset($this->mapAttrSetIdsKept[$record['attribute_set_id']])
                || !isset($this->mapAttrGroupIdsKept[$record['attribute_group_id']])
                || !isset($this->mapEntityTypeIdsDestOldNew[$record['entity_type_id']])
            ) {
                var_dump('ignore dest attribute entity because not in maps', $record);
                var_dump(
                    isset($this->mapAttrIdsKept[$record['attribute_id']]),
                    isset($this->mapAttrSetIdsKept[$record['attribute_set_id']]),
                    isset($this->mapAttrGroupIdsKept[$record['attribute_group_id']]),
                    isset($this->mapEntityTypeIdsDestOldNew[$record['entity_type_id']])
                );
                continue;
            }

            $prevAttrId = $record['attribute_id'];

            $record['attribute_id'] = $this->mapAttrIdsKept[$record['attribute_id']];
            $record['attribute_set_id'] = $this->mapAttrSetIdsKept[$record['attribute_set_id']];
            $record['attribute_group_id'] = $this->mapAttrGroupIdsKept[$record['attribute_group_id']];
            $record['entity_type_id'] = $this->mapEntityTypeIdsDestOldNew[$record['entity_type_id']];

=======
        $customAttributeIds = $this->modelData->getCustomAttributeIds();
        if (empty($customAttributeIds)) {
            return;
        }
        $customEntityAttributes = $this->source->getRecords(
            $sourceDocName,
            0,
            $this->source->getRecordsCount($sourceDocName),
            new \Zend_Db_Expr(sprintf('attribute_id IN (%s)', implode(',', $customAttributeIds)))
        );
        foreach ($customEntityAttributes as $record) {
            if (!isset($this->mapAttributeGroupIdsSourceDest[$record['attribute_group_id']])) {
                continue;
            }
            $record['sort_order'] = $this->getCustomAttributeSortOrder($record);
            $record['attribute_group_id'] = $this->mapAttributeGroupIdsSourceDest[$record['attribute_group_id']];
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
            $record['entity_attribute_id'] = null;
            $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $record]);
            
            $recordsToSave->addRecord($destinationRecord);
        }
        $this->saveRecords($destinationDocument, $recordsToSave);
    }

    /**
     * Get sort order for custom attribute
     *
     * @param array $attribute
     * @return int
     */
    private function getCustomAttributeSortOrder(array $attribute)
    {
        $productEntityTypeId = $this->modelData->getEntityTypeIdByCode(ModelData::ENTITY_TYPE_PRODUCT_CODE);
        $groupName = $this->modelData->getSourceAttributeGroupNameFromId($attribute['attribute_group_id']);
        if ($attribute['entity_type_id'] == $productEntityTypeId
            && isset($this->mapProductAttributeGroupNamesSourceDest[$groupName])
        ) {
            return $attribute['sort_order'] + 200;
        }
        return $attribute['sort_order'];
    }

    /**
     * Migrate tables extended from eav_attribute
     */
    private function migrateAttributesExtended()
    {
        $this->progress->advance();
        $documents = $this->readerGroups->getGroup('documents_attribute_extended');
        foreach ($documents as $documentName => $mappingField) {
            $sourceDocument = $this->source->getDocument($documentName);
            $destinationDocument = $this->destination->getDocument(
                $this->map->getDocumentMap($documentName, MapInterface::TYPE_SOURCE)
            );
            
            $this->destination->backupDocument($destinationDocument->getName());
            $destinationRecords = $this->helper->getDestinationRecords($documentName, [$mappingField]);
            $recordsToSave = $destinationDocument->getRecords();
            $sourceRecords = $this->ignoredAttributes
                ->clearIgnoredAttributes($this->helper->getSourceRecords($documentName));
<<<<<<< HEAD
            foreach ($sourceRecords as $recordData) {
                // Migrate only records for attributes we migrated
                if(!isset($this->attrIdsMigrated[$recordData['attribute_id']])) {
                    continue;
                }

=======
            $recordTransformer = $this->helper->getRecordTransformer($sourceDocument, $destinationDocument);
            foreach ($sourceRecords as $sourceRecordData) {
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
                /** @var Record $sourceRecord */
                $sourceRecord = $this->factory->create(['document' => $sourceDocument, 'data' => $sourceRecordData]);
                /** @var Record $destinationRecord */
                $destinationRecord = $this->factory->create(['document' => $destinationDocument]);
                $mappedId = isset($this->mapAttributeIdsSourceDest[$sourceRecord->getValue($mappingField)])
                    ? $this->mapAttributeIdsSourceDest[$sourceRecord->getValue($mappingField)]
                    : null;
                if ($mappedId !== null && isset($destinationRecords[$mappedId])) {
                    $destinationRecordData = $destinationRecords[$mappedId];
                    $destinationRecordData['attribute_id'] = $sourceRecordData['attribute_id'];
                    $destinationRecord->setData($destinationRecordData);
                    unset($destinationRecords[$mappedId]);
                } else {
                    $destinationRecordData = $destinationRecord->getDataDefault();
                    $destinationRecord->setData($destinationRecordData);
                    $recordTransformer->transform($sourceRecord, $destinationRecord);
                }
<<<<<<< HEAD
                $destinationRecord->setData($destinationRecordData);
                $this->helper->getRecordTransformer($sourceDocument, $destinationDocument)
                    ->transform($sourceRecord, $destinationRecord);
                $destinationRecord->setValue('attribute_id', $this->mapAttrIdsMigrated[$recordData['attribute_id']]);
=======
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
                $recordsToSave->addRecord($destinationRecord);
            }
            $this->destination->clearDocument($destinationDocument->getName());
            $this->saveRecords($destinationDocument, $recordsToSave);

            $recordsToSave = $destinationDocument->getRecords();
            foreach ($destinationRecords as $record) {
                // Keep only records for attributes we kept
                if(!isset($this->attrIdsKept[$record['attribute_id']])) {
                    continue;
                }

                $record['attribute_id'] = $this->mapAttrIdsKept[$record['attribute_id']];
                $destinationRecord = $this->factory->create([
                    'document' => $destinationDocument,
                    'data' => $record
                ]);
                $recordsToSave->addRecord($destinationRecord);
            }
            $this->saveRecords($destinationDocument, $recordsToSave);
        }
    }

    /**
     * Save records
     *
     * @param Document|string $document
     * @param Record\Collection|array $recordsToSave
     * @return void
     */
    private function saveRecords($document, $recordsToSave)
    {
        if (is_object($document)) {
            $document = $document->getName();
        }
        $this->destination->saveRecords($document, $recordsToSave);
    }

    /**
     * Create mapping for entity type ids
     *
     * @return void
     */
    private function createMapEntityTypeIds()
    {
        $entityTypesMigrated = $this->helper->getDestinationRecords(
            'eav_entity_type',
            ['entity_type_code']
        );
        foreach ($this->initialData->getEntityTypes(ModelData::TYPE_DEST) as $entityTypeIdOld => $recordOld) {
            $entityTypeMigrated = $entityTypesMigrated[$recordOld['entity_type_code']];
            $this->mapEntityTypeIdsDestOldNew[$entityTypeIdOld] = $entityTypeMigrated['entity_type_id'];
        }
        foreach ($this->initialData->getEntityTypes(ModelData::TYPE_SOURCE) as $entityTypeIdSource => $recordSource) {
            foreach ($this->initialData->getEntityTypes(ModelData::TYPE_DEST) as $entityTypeIdDest => $recordDest) {
                if ($recordSource['entity_type_code'] == $recordDest['entity_type_code']) {
                    $this->mapEntityTypeIdsSourceDest[$entityTypeIdSource] = $entityTypeIdDest;
                }
            }
        }
    }

    /**
     * Create mapping for attribute set ids
     *
     * @return void
     */
    private function createMapAttributeSetIds()
    {
        $this->newAttributeSets = $this->helper->getDestinationRecords(
            'eav_attribute_set',
            ['entity_type_id', 'attribute_set_name']
        );
<<<<<<< HEAD
        
        foreach ($this->initialData->getAttributeSets('dest') as $attributeSetId => $record) {

=======
        foreach ($this->initialData->getAttributeSets(ModelData::TYPE_DEST) as $attributeSetId => $record) {
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
            $entityTypeId = $this->mapEntityTypeIdsDestOldNew[$record['entity_type_id']];

            $newKey = $entityTypeId . '-' . $record['attribute_set_name'];
            if(!isset($this->newAttributeSets[$newKey])) {
                $newKey = $entityTypeId . '-' . 'Migration_'.$record['attribute_set_name'];
            }

            $newAttributeSet = $this->newAttributeSets[$newKey];

            if($record['entity_type_id'] == 4) {
                $this->mapAttrSetIdsKept[$attributeSetId] = $newAttributeSet['attribute_set_id'];
                continue;
            }
            
            $this->mapAttributeSetIdsDestOldNew[$attributeSetId] = $newAttributeSet['attribute_set_id'];
            $this->defaultAttributeSetIds[$newAttributeSet['entity_type_id']] = $newAttributeSet['attribute_set_id'];
        }
        
        foreach ($this->initialData->getAttributeSets('source') as $sourceAttributeSetId => $sourceRecord) {
            if($sourceRecord['entity_type_id'] == 4) {
                continue;
            }

            $entityTypeId = $this->mapEntityTypeIdsDestOldNew[$sourceRecord['entity_type_id']];
            $newKey = $entityTypeId . '-' . $sourceRecord['attribute_set_name'];
            if(!isset($this->newAttributeSets[$newKey])) {
                $newKey = $entityTypeId . '-' . 'Migration_'.$sourceRecord['attribute_set_name'];
            }

            $newAttributeSet = $this->newAttributeSets[$newKey];

            $this->mapAttrSetIdsMigrated[$sourceAttributeSetId] = $newAttributeSet['attribute_set_id'];
        }
    }

    /**
<<<<<<< HEAD
     * Create mapping for attribute group ids
     *
     * @return void
     */
    protected function createMapAttributeGroupIds()
    {
        $newAttributeGroups = $this->helper->getDestinationRecords(
            'eav_attribute_group',
            ['attribute_set_id', 'attribute_group_name']
        );
        
        foreach ($this->initialData->getAttributeGroups('dest') as $record) {
            if(isset($this->mapAttrSetIdsKept[$record['attribute_set_id']])) {

                $newKey = $this->mapAttrSetIdsKept[$record['attribute_set_id']] . '-'
                    . $record['attribute_group_name'];

                if(!isset($newAttributeGroups[$newKey])) {
                    $newKey = $this->mapAttrSetIdsKept[$record['attribute_set_id']] . '-'
                    . 'Migration_'.$record['attribute_group_name'];
                }

                $newAttributeGroup = $newAttributeGroups[$newKey];
                $this->mapAttributeGroupIdsDestOldNew[$record['attribute_group_id']] =
                    $newAttributeGroup['attribute_group_id'];

                $this->mapAttrGroupIdsKept[$record['attribute_group_id']] = $newAttributeGroup['attribute_group_id'];
            }
        }
        
        foreach ($this->initialData->getAttributeGroups('source') as $record) {
            if(isset($this->mapAttrSetIdsMigrated[$record['attribute_set_id']])) {

                $newKey = $this->mapAttrSetIdsMigrated[$record['attribute_set_id']] . '-'
                    . $record['attribute_group_name'];

                if(!isset($newAttributeGroups[$newKey])) {
                    $newKey = $this->mapAttrSetIdsMigrated[$record['attribute_set_id']] . '-'
                    . 'Migration_'.$record['attribute_group_name'];
                }

                $newAttributeGroup = $newAttributeGroups[$newKey];

                $this->mapAttrGroupIdsMigrated[$record['attribute_group_id']] = $newAttributeGroup['attribute_group_id'];
            }
        }
    }

    /**
=======
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
     * Create mapping for attribute ids
     *
     * @return void
     */
    private function createMapAttributeIds()
    {
        $newAttributes = $this->helper->getDestinationRecords(
            'eav_attribute',
            ['entity_type_id', 'attribute_code']
        );
        foreach ($this->initialData->getAttributes(ModelData::TYPE_DEST) as $keyOld => $attributeOld) {
            $entityTypeId = $attributeOld['entity_type_id'];
            $attributeCode = $attributeOld['attribute_code'];
            $keyMapped = $this->mapEntityTypeIdsDestOldNew[$entityTypeId] . '-' . $attributeCode;
            $this->mapAttributeIdsDestOldNew[$attributeOld['attribute_id']] =
                $newAttributes[$keyMapped]['attribute_id'];
            
            $this->mapAttrIdsKept[$attributeOld['attribute_id']] = $newAttributes[$keyMapped]['attribute_id'];
        }
        foreach ($this->initialData->getAttributes('source') as $keyOld => $attributeOld) {
            //list($entityTypeId, $attributeCodeSource) = explode('-', $keyOld, 2);
            $entityTypeId = $attributeOld['entity_type_id'];
            $attributeCodeSource = $attributeOld['attribute_code'];
            $keyMapped = $this->mapEntityTypeIdsDestOldNew[$entityTypeId] . '-' . $attributeCodeSource;
            
            if(isset($newAttributes[$keyMapped])) {
                $this->mapAttrIdsMigrated[$attributeOld['attribute_id']] = $newAttributes[$keyMapped]['attribute_id'];
            }
        }
<<<<<<< HEAD
        /*foreach ($this->initialData->getAttributes('source') as $idSource => $attributeSource) {
            foreach ($this->initialData->getAttributes('dest') as $keyDest => $attributeDest) {
                list($entityTypeIdDest, $attributeCodeDest) = explode('-', $keyDest, 2);
                $keyDestMapped = $this->mapEntityTypeIdsDestOldNew[$entityTypeIdDest] . '-' . $attributeCodeDest;
                $keySource = $attributeSource['entity_type_id'] . '-' . $attributeSource['attribute_code'];
                if($attributeSource['attribute_id'] == 561) {
                    var_dump('attributeSource', $attributeSource);
                    var_dump('attributeDest', $attributeDest);
                    var_dump('keyDestMapped', $keyDestMapped);
                    var_dump('keySource', $keySource);
                }
                if ($keySource == $keyDestMapped) {
                    $this->mapAttributeIdsSourceDest[$idSource] = $attributeDest['attribute_id'];
                    //$this->mapAttrIdsMigrated[$idSource] = $attributeDest['attribute_id'];
=======
        foreach ($this->initialData->getAttributes(ModelData::TYPE_SOURCE) as $recordSourceId => $recordSource) {
            foreach ($this->initialData->getAttributes(ModelData::TYPE_DEST) as $recordDestId => $recordDest) {
                $sourceEntityTypeCode = $this->initialData->getEntityTypes(ModelData::TYPE_SOURCE)
                [$recordSource['entity_type_id']]['entity_type_code'];
                $destinationEntityTypeCode = $this->initialData->getEntityTypes(ModelData::TYPE_DEST)
                [$recordDest['entity_type_id']]['entity_type_code'];
                if ($recordSource['attribute_code'] == $recordDest['attribute_code']
                    && $sourceEntityTypeCode == $destinationEntityTypeCode
                ) {
                    $this->mapAttributeIdsSourceDest[$recordSourceId] = $recordDestId;
>>>>>>> e07ed628a60f5ee16f2806241c79fd01747b0cc8
                }
            }
        }*/

        $this->exportIdsMaps();
    }

    public function exportIdsMaps() {
        $attrIdsKeptArray = array();
        foreach ($this->mapAttrIdsKept as $key => $value) {
            $attrIdsKeptArray[] = array($key, $value);
        }

        $attrIdsMigratedArray = array();
        foreach ($this->mapAttrIdsMigrated as $key => $value) {
            $attrIdsMigratedArray[] = array($key, $value);
        }

        $this->csvProcessor->saveData($this->dir->getPath('var').'/migrationAttrIdsKept.csv', $attrIdsKeptArray);
        $this->csvProcessor->saveData($this->dir->getPath('var').'/migrationAttrIdsMigrated.csv', $attrIdsMigratedArray);
    }

    /**
     * Create mapping for product attribute group ids
     */
    private function createMapProductAttributeGroupIds()
    {
        $attributeGroupsDestination = $this->helper->getDestinationRecords(
            'eav_attribute_group',
            ['attribute_group_id']
        );
        $attributeGroupsSource = $this->helper->getSourceRecords(
            'eav_attribute_group',
            ['attribute_group_id']
        );
        $productAttributeSetIds = array_keys($this->modelData->getProductAttributeSets());
        foreach ($attributeGroupsSource as $idSource => $recordSource) {
            $sourceAttributeGroupName = $recordSource['attribute_group_name'];
            if (in_array($recordSource['attribute_set_id'], $productAttributeSetIds)) {
                $sourceAttributeGroupName = str_replace(
                    array_keys($this->mapProductAttributeGroupNamesSourceDest),
                    $this->mapProductAttributeGroupNamesSourceDest,
                    $recordSource['attribute_group_name']
                );
            }
            $sourceKey = $recordSource['attribute_set_id'] . ' ' . $sourceAttributeGroupName;
            foreach ($attributeGroupsDestination as $idDestination => $recordDestination) {
                $destinationKey = $recordDestination['attribute_set_id']
                    . ' '
                    . $recordDestination['attribute_group_name'];
                if ($sourceKey == $destinationKey) {
                    $this->mapAttributeGroupIdsSourceDest[$recordSource['attribute_group_id']] =
                        $recordDestination['attribute_group_id'];
                }
            }
        }
    }

    /**
     * Rollback backed up documents
     *
     * @return void
     */
    public function rollback()
    {
        foreach (array_keys($this->readerGroups->getGroup('documents')) as $documentName) {
            $destinationDocument = $this->destination->getDocument(
                $this->map->getDocumentMap($documentName, MapInterface::TYPE_SOURCE)
            );
            if ($destinationDocument !== false) {
                $this->destination->rollbackDocument($destinationDocument->getName());
            }
        }
    }
}
