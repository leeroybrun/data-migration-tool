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
    protected $newAttributeSets = [];

    /**
     * @var array;
     */
    protected $mapAttributeIdsDestOldNew = [];

    /**
     * @var array;
     */
    protected $mapAttributeIdsSourceDest = [];

    /**
     * @var array;
     */
    protected $mapAttributeSetIdsDestOldNew = [];

    /**
     * @var array;
     */
    protected $mapAttributeGroupIdsDestOldNew = [];

    /**
     * @var array;
     */
    protected $mapEntityTypeIdsDestOldNew = [];

    /**
     * @var array;
     */
    protected $mapEntityTypeIdsSourceDest = [];

    /**
     * @var array;
     */
    protected $defaultAttributeSetIds = [];

    /**
     * @var Helper
     */
    protected $helper;

    /**
     * @var Source
     */
    protected $source;

    /**
     * @var Destination
     */
    protected $destination;

    /**
     * @var Map
     */
    protected $map;

    /**
     * @var RecordFactory
     */
    protected $factory;

    /**
     * @var InitialData
     */
    protected $initialData;

    /**
     * @var IgnoredAttributes
     */
    protected $ignoredAttributes;

    /**
     * @var ProgressBar\LogLevelProcessor
     */
    protected $progress;

    /**
     * @var \Migration\Reader\Groups
     */
    protected $readerGroups;

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
     * @var array
     */
    protected $groupsDataToAdd = [
        [
            'attribute_group_name' => 'Schedule Design Update',
            'attribute_group_code' => 'schedule-design-update',
            'sort_order' => '55',
        ], [
            'attribute_group_name' => 'Bundle Items',
            'attribute_group_code' => 'bundle-items',
            'sort_order' => '16',
        ]
    ];

    /**
     * Attributes will be added to attribute group if not exist
     *
     * @var array
     */
    private $attributesGroupToAdd = [
        'category_ids' => 'product-details',
        'price_type' => 'product-details',
        'sku_type' => 'product-details',
        'weight_type' => 'product-details',
        'giftcard_type' => 'product-details',
        'quantity_and_stock_status' => 'product-details',
        'swatch_image' => 'image-management'
    ];

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
        ProgressBar\LogLevelProcessor $progress
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
    }

    /**
     * Entry point. Run migration of EAV structure.
     *
     * @return bool
     */
    public function perform()
    {
        $this->progress->start($this->getIterationsCount());
        $this->migrateEntityTypes();
        $this->migrateAttributeSetsAndGroups();
        $this->changeOldAttributeSetIdsInEntityTypes(['customer', 'customer_address']);
        $this->migrateAttributes();
        $this->migrateAttributesExtended();
        $this->migrateEntityAttributes();
        $this->migrateOtherEntityAttributes();
        $this->progress->finish();
        return true;
    }

    /**
     * Migrate Entity Type table
     *
     * @return void
     */
    protected function migrateEntityTypes()
    {
        $documentName = 'eav_entity_type';
        $mappingField = 'entity_type_code';

        $this->progress->advance();
        $sourceDocument = $this->source->getDocument($documentName);
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap($documentName, MapInterface::TYPE_SOURCE)
        );
        $this->destination->backupDocument($destinationDocument->getName());
        $destinationRecords = $this->helper->getDestinationRecords($documentName, [$mappingField]);
        $recordsToSave = $destinationDocument->getRecords();
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
            $this->helper->getRecordTransformer($sourceDocument, $destinationDocument)
                ->transform($sourceRecord, $destinationRecord);
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
     * Migrate eav_attribute_set and eav_attribute_group
     *
     * @return void
     */
    protected function migrateAttributeSetsAndGroups()
    {
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

            $this->destination->clearDocument($destinationDocument->getName());
            $this->saveRecords($destinationDocument, $recordsToSave);
            if ($documentName == 'eav_attribute_set') {
                $this->createMapAttributeSetIds();
            }
            if ($documentName == 'eav_attribute_group') {
                $this->createMapAttributeGroupIds();
            }
        }
    }

    /**
     * Add attribute groups to Magento 1 which are needed for Magento 2
     *
     * @param Record\Collection $recordsToSave
     * @param string $documentName
     * @param array $groupsData
     * @return Record\Collection
     */
    protected function addAttributeGroups($recordsToSave, $documentName, array $groupsData)
    {
        $this->helper->setAddedGroups([]);

        return $recordsToSave;
    }

    /**
     * Change old default attribute set ids in entity types
     *
     * @param array $exceptions
     * @return void
     */
    protected function changeOldAttributeSetIdsInEntityTypes(array $exceptions)
    {
        $documentName = 'eav_entity_type';
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap($documentName, MapInterface::TYPE_SOURCE)
        );
        $recordsToSave = $destinationDocument->getRecords();
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
        }
        $this->destination->clearDocument($destinationDocument->getName());
        $this->saveRecords($destinationDocument, $recordsToSave);
    }

    /**
     * Migrate eav_attribute
     *
     * @return void
     */
    protected function migrateAttributes()
    {
        $this->progress->advance();
        $sourceDocName = 'eav_attribute';
        $sourceDocument = $this->source->getDocument($sourceDocName);
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap($sourceDocName, MapInterface::TYPE_SOURCE)
        );
        $this->destination->backupDocument($destinationDocument->getName());
        $sourceRecords = $this->ignoredAttributes->clearIgnoredAttributes($this->initialData->getAttributes('source'));
        $destinationRecords = $this->initialData->getAttributes('dest');

        $migratedAttributesMap = array();

        // Migrate attributes from source (except the ones for products), with new IDs
        $recordsToSave = $destinationDocument->getRecords();
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
                unset($destinationRecords[$mappedKey]);
            } else {
                $destinationRecordData = $destinationRecord->getDataDefault();
            }
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
            $recordsToSave->addRecord($destinationRecord);
        }

        var_dump('attrIdsMigrated', $this->attrIdsMigrated);

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
                $this->mapEntityTypeIdsDestOldNew[$destinationRecord->getValue('entity_type_id')]
            );
            
            $this->attrIdsKept[$oldAttrId] = true;
            $recordsToSave->addRecord($destinationRecord);
        }
        $this->destination->clearDocument($destinationDocument->getName());
        $this->saveRecords($destinationDocument, $recordsToSave);

        $this->createMapAttributeIds();
    }

    /**
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
                        var_dump(isset($this->mapAttrIdsMigrated[$sourceRecordData['attribute_id']]));
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

            var_dump('saving', $sourceDocName);
            $this->destination->clearDocument($destinationDocument->getName());
            $this->saveRecords($destinationDocument, $recordsToSave);
        }
    }

    /**
     * Migrate eav_entity_attributes and other entity tables
     *
     * @return void
     */
    protected function migrateEntityAttributes()
    {
        $this->progress->advance();
        $sourceDocName = 'eav_entity_attribute';
        $sourceDocument = $this->source->getDocument($sourceDocName);
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap($sourceDocName, MapInterface::TYPE_SOURCE)
        );
        $this->destination->backupDocument($destinationDocument->getName());
        $recordsToSave = $destinationDocument->getRecords();

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

            $record['entity_attribute_id'] = null;
            $destinationRecord = $this->factory->create(['document' => $destinationDocument, 'data' => $record]);
            
            $recordsToSave->addRecord($destinationRecord);
        }

        $recordsToSave = $this->processDesignEntityAttributes($recordsToSave);
        $recordsToSave = $this->moveProductAttributes($recordsToSave);
        $recordsToSave = $this->addLackingAttributesToCustomerAttributeSet($recordsToSave);

        $this->destination->clearDocument($destinationDocument->getName());
        $this->saveRecords($destinationDocument, $recordsToSave);
    }

    /**
     * Move some fields to other attribute groups
     *
     * @param Record\Collection $recordsToSave
     * @return Record\Collection
     */
    private function moveProductAttributes($recordsToSave)
    {
        $this->moveProductAttributeToGroup($recordsToSave, 'price', 'product-details');
        $this->moveProductAttributeToGroup($recordsToSave, 'shipment_type', 'bundle-items');
        foreach ($this->attributesGroupToAdd as $attributeCode => $attributeGroupCode) {
            $this->addProductAttributeToGroup($recordsToSave, $attributeCode, $attributeGroupCode);
        }
        return $recordsToSave;
    }

    /**
     * Move attribute to other attribute group
     *
     * @param Record\Collection $recordsToSave
     * @param string $attributeCode
     * @param string $attributeGroupCode
     * @return Record\Collection
     */
    private function moveProductAttributeToGroup($recordsToSave, $attributeCode, $attributeGroupCode)
    {
        $productEntityType
            = $this->helper->getSourceRecords('eav_entity_type', ['entity_type_code'])['catalog_product'];
        $attributes = $this->helper->getDestinationRecords('eav_attribute', ['attribute_id']);
        $attributeGroups = $this->helper->getDestinationRecords('eav_attribute_group', ['attribute_group_id']);
        $attributeSetGroups = [];
        foreach ($attributeGroups as $attributeGroup) {
            if ($attributeGroup['attribute_group_code'] == $attributeGroupCode) {
                $attributeSetGroups[$attributeGroup['attribute_set_id']][$attributeGroupCode] =
                    $attributeGroup['attribute_group_id'];
            }
        }
        foreach ($recordsToSave as $record) {
            $attributeId = $record->getValue('attribute_id');
            $entityTypeId = $record->getValue('entity_type_id');
            $attributeSetId = $record->getValue('attribute_set_id');
            if (!isset($attributes[$attributeId])
                || $entityTypeId != $productEntityType['entity_type_id']
                || $attributeCode != $attributes[$attributeId]['attribute_code']
                || !array_key_exists($attributeSetId, $attributeSetGroups)
            ) {
                continue;
            }
            $record->setValue('attribute_group_id', $attributeSetGroups[$attributeSetId][$attributeGroupCode]);
        }
        return $recordsToSave;
    }

    /**
     * Add attribute to attribute group
     *
     * @param Record\Collection $recordsToSave
     * @param string $attributeCode
     * @param string $attributeGroupCode
     * @return Record\Collection
     */
    private function addProductAttributeToGroup($recordsToSave, $attributeCode, $attributeGroupCode)
    {
        $productEntityType
            = $this->helper->getSourceRecords('eav_entity_type', ['entity_type_code'])['catalog_product'];
        $productEntityTypeId = $productEntityType['entity_type_id'];
        $attributes = $this->helper->getDestinationRecords('eav_attribute', ['attribute_id']);
        $attributeGroups = $this->helper->getDestinationRecords('eav_attribute_group', ['attribute_group_id']);
        $attributeSets = $this->helper->getDestinationRecords('eav_attribute_set', ['attribute_set_id']);
        $attributeSetGroupsFound = [];
        $attribute = null;
        $destinationDocument = $this->destination->getDocument(
            $this->map->getDocumentMap('eav_entity_attribute', MapInterface::TYPE_SOURCE)
        );
        foreach ($recordsToSave as $record) {
            $attributeId = $record->getValue('attribute_id');
            $entityTypeId = $record->getValue('entity_type_id');
            if (isset($attributes[$attributeId])
                && $attributes[$attributeId]['attribute_code'] == $attributeCode
                && $entityTypeId == $productEntityTypeId
            ) {
                $attributeSetGroupsFound[$record->getValue('attribute_set_id')] =
                    $record->getValue('attribute_group_id');
                $attribute = $record->getData();
            }
        }
        if ($attribute === null) {
            return $recordsToSave;
        }
        foreach ($attributeGroups as $attributeGroup) {
            if ($attributeGroup['attribute_group_code'] == $attributeGroupCode
                && array_key_exists($attributeGroup['attribute_set_id'], $attributeSets)
                && $attributeSets[$attributeGroup['attribute_set_id']]['entity_type_id'] == $productEntityTypeId
                && !isset($attributeSetGroupsFound[$attributeGroup['attribute_set_id']])
            ) {
                $attribute['attribute_set_id'] = $attributeGroup['attribute_set_id'];
                $attribute['attribute_group_id'] = $attributeGroup['attribute_group_id'];
                $attribute['entity_attribute_id'] = null;
                $destinationRecord = $this->factory->create(
                    [
                        'document' => $destinationDocument,
                        'data' => $attribute
                    ]
                );
                $recordsToSave->addRecord($destinationRecord);
            }
        }
        return $recordsToSave;
    }

    /**
     * Move design attributes to schedule-design-update attribute groups
     *
     * @param Record\Collection $recordsToSave
     * @return Record\Collection
     * @throws \Migration\Exception
     */
    private function processDesignEntityAttributes($recordsToSave)
    {
        $data = $this->helper->getDesignAttributeAndGroupsData();
        $entityAttributeDocument = $this->destination->getDocument(
            $this->map->getDocumentMap('eav_entity_attribute', MapInterface::TYPE_SOURCE)
        );
        $recordsToSaveFiltered = $entityAttributeDocument->getRecords();
        foreach ($recordsToSave as $record) {
            /** @var Record $record */
            if (in_array($record->getValue('attribute_set_id'), $data['catalogProductSetIdsMigrated']) &&
                in_array($record->getValue('attribute_id'), [
                    $data['customDesignAttributeId'],
                    $data['customLayoutAttributeId']
                ])
            ) {
                continue;
            }
            $recordsToSaveFiltered->addRecord($record);
        }
        $recordsToSave = $recordsToSaveFiltered;

        foreach ($data['scheduleGroupsMigrated'] as $group) {
            if (isset($data['customDesignAttributeId']) && $data['customDesignAttributeId']) {
                $dataRecord = [
                    'entity_attribute_id' => null,
                    'entity_type_id' => $data['entityTypeIdCatalogProduct'],
                    'attribute_set_id' => $group['attribute_set_id'],
                    'attribute_group_id' => $group['attribute_group_id'],
                    'attribute_id' => $data['customDesignAttributeId'],
                    'sort_order' => 40,
                ];
                $destinationRecord = $this->factory->create([
                    'document' => $entityAttributeDocument,
                    'data' => $dataRecord
                ]);
                /** Adding custom_design */
                $recordsToSave->addRecord($destinationRecord);
            }

            if (isset($data['customLayoutAttributeId']) && $data['customLayoutAttributeId']) {
                $dataRecord = [
                    'entity_attribute_id' => null,
                    'entity_type_id' => $data['entityTypeIdCatalogProduct'],
                    'attribute_set_id' => $group['attribute_set_id'],
                    'attribute_group_id' => $group['attribute_group_id'],
                    'attribute_id' => $data['customLayoutAttributeId'],
                    'sort_order' => 50,
                ];
                $destinationRecord = $this->factory->create([
                    'document' => $entityAttributeDocument,
                    'data' => $dataRecord
                ]);
                /** Adding custom_layout */
                $recordsToSave->addRecord($destinationRecord);
            }
        }

        return $recordsToSave;
    }

    /**
     * There are attributes from destination customer attribute set
     * that do not exit in source customer attribute set.
     * The method adds the lacking attributes
     *
     * @param Record\Collection $recordsToSave
     * @return Record\Collection
     */
    private function addLackingAttributesToCustomerAttributeSet($recordsToSave)
    {
        $entityAttributeDocument = $this->destination->getDocument(
            $this->map->getDocumentMap('eav_entity_attribute', MapInterface::TYPE_SOURCE)
        );
        $customerEntityType
            = $this->helper->getSourceRecords('eav_entity_type', ['entity_type_code'])['customer'];
        $customerEntityTypeId = $customerEntityType['entity_type_id'];
        $attributeGroups = $this->helper->getDestinationRecords('eav_attribute_group', ['attribute_group_id']);
        $attributeSets = $this->helper->getDestinationRecords('eav_attribute_set', ['attribute_set_id']);
        $attributeSetNameCustomerSource = 'Migration_Default';
        $attributeSetNameCustomerDestination = 'Default';
        $attributeSetIdOfCustomerSource = null;
        $attributeSetIdOfCustomerDestination = null;
        $eavEntityAttributeOfCustomerSource = [];
        $eavEntityAttributeOfCustomerDestination = [];
        $attributeGroupIfOfCustomerSource = null;

        foreach ($attributeSets as $attributeSet) {
            if ($attributeSet['entity_type_id'] == $customerEntityTypeId
                && $attributeSet['attribute_set_name'] == $attributeSetNameCustomerSource
            ) {
                $attributeSetIdOfCustomerSource = $attributeSet['attribute_set_id'];
            } elseif ($attributeSet['entity_type_id'] == $customerEntityTypeId
                && $attributeSet['attribute_set_name'] == $attributeSetNameCustomerDestination
            ) {
                $attributeSetIdOfCustomerDestination = $attributeSet['attribute_set_id'];
            }
        }
        foreach ($attributeGroups as $attributeGroup) {
            if ($attributeGroup['attribute_set_id'] == $attributeSetIdOfCustomerSource) {
                $attributeGroupIfOfCustomerSource = $attributeGroup['attribute_group_id'];
            }
        }
        if ($attributeSetIdOfCustomerSource === null
            || $attributeSetIdOfCustomerDestination === null
            || $attributeGroupIfOfCustomerSource === null
        ) {
            return $recordsToSave;
        }

        foreach ($recordsToSave as $record) {
            $attributeId = $record->getValue('attribute_id');
            $attributeSetId = $record->getValue('attribute_set_id');
            if ($attributeSetId == $attributeSetIdOfCustomerSource) {
                $eavEntityAttributeOfCustomerSource[] = $attributeId;
            } else if ($attributeSetId == $attributeSetIdOfCustomerDestination) {
                $eavEntityAttributeOfCustomerDestination[] = $attributeId;
            }
        }
        $customerAttributeIdsToAdd = array_diff(
            $eavEntityAttributeOfCustomerDestination,
            $eavEntityAttributeOfCustomerSource
        );
        foreach ($customerAttributeIdsToAdd as $customerAttributeId) {
            $dataRecord = [
                'entity_attribute_id' => null,
                'entity_type_id' => $customerEntityTypeId,
                'attribute_set_id' => $attributeSetIdOfCustomerSource,
                'attribute_group_id' => $attributeGroupIfOfCustomerSource,
                'attribute_id' => $customerAttributeId,
                'sort_order' => 50,
            ];
            $destinationRecord = $this->factory->create([
                'document' => $entityAttributeDocument,
                'data' => $dataRecord
            ]);
            $recordsToSave->addRecord($destinationRecord);
        }

        return $recordsToSave;
    }

    /**
     * Migrate tables extended from eav_attribute
     *
     * @return void
     */
    protected function migrateAttributesExtended()
    {
        $documents = $this->readerGroups->getGroup('documents_attribute_extended');
        foreach ($documents as $documentName => $mappingField) {
            $this->progress->advance();
            $sourceDocument = $this->source->getDocument($documentName);
            $destinationDocument = $this->destination->getDocument(
                $this->map->getDocumentMap($documentName, MapInterface::TYPE_SOURCE)
            );
            
            $this->destination->backupDocument($destinationDocument->getName());
            $destinationRecords = $this->helper->getDestinationRecords($documentName, [$mappingField]);
            $recordsToSave = $destinationDocument->getRecords();
            $sourceRecords = $this->ignoredAttributes
                ->clearIgnoredAttributes($this->helper->getSourceRecords($documentName));
            foreach ($sourceRecords as $recordData) {
                // Migrate only records for attributes we migrated
                if(!isset($this->attrIdsMigrated[$recordData['attribute_id']])) {
                    continue;
                }

                /** @var Record $sourceRecord */
                $sourceRecord = $this->factory->create(['document' => $sourceDocument, 'data' => $recordData]);
                /** @var Record $destinationRecord */
                $destinationRecord = $this->factory->create(['document' => $destinationDocument]);
                $mappedId = isset($this->mapAttributeIdsSourceDest[$sourceRecord->getValue($mappingField)])
                    ? $this->mapAttributeIdsSourceDest[$sourceRecord->getValue($mappingField)]
                    : null;
                if ($mappedId !== null && isset($destinationRecords[$mappedId])) {
                    $destinationRecordData = $destinationRecords[$mappedId];
                    unset($destinationRecords[$mappedId]);
                } else {
                    $destinationRecordData = $destinationRecord->getDataDefault();
                }
                $destinationRecord->setData($destinationRecordData);
                $this->helper->getRecordTransformer($sourceDocument, $destinationDocument)
                    ->transform($sourceRecord, $destinationRecord);
                $destinationRecord->setValue('attribute_id', $this->mapAttrIdsMigrated[$recordData['attribute_id']]);
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
     * @param Document $document
     * @param Record\Collection $recordsToSave
     * @return void
     */
    protected function saveRecords(Document $document, Record\Collection $recordsToSave)
    {
        $this->destination->saveRecords($document->getName(), $recordsToSave);
    }

    /**
     * Create mapping for entity type ids
     *
     * @return void
     */
    protected function createMapEntityTypeIds()
    {
        $entityTypesMigrated = $this->helper->getDestinationRecords(
            'eav_entity_type',
            ['entity_type_code']
        );
        foreach ($this->initialData->getEntityTypes('dest') as $entityTypeIdOld => $recordOld) {
            $entityTypeMigrated = $entityTypesMigrated[$recordOld['entity_type_code']];
            $this->mapEntityTypeIdsDestOldNew[$entityTypeIdOld] = $entityTypeMigrated['entity_type_id'];
        }
        foreach ($this->initialData->getEntityTypes('source') as $entityTypeIdSource => $recordSource) {
            foreach ($this->initialData->getEntityTypes('dest') as $entityTypeIdDest => $recordDest) {
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
    protected function createMapAttributeSetIds()
    {
        $this->newAttributeSets = $this->helper->getDestinationRecords(
            'eav_attribute_set',
            ['entity_type_id', 'attribute_set_name']
        );
        
        foreach ($this->initialData->getAttributeSets('dest') as $attributeSetId => $record) {

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

        var_dump('mapAttrSetIdsKept', $this->mapAttrSetIdsKept);
        var_dump('mapAttrSetIdsMigrated', $this->mapAttrSetIdsMigrated);
    }

    /**
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
            var_dump('mapping group '.$record['attribute_group_id'].' from dest', $record);
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

                var_dump('new id', $newAttributeGroup['attribute_group_id']);

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

        var_dump('mapAttrGroupIdsKept', $this->mapAttrGroupIdsKept);
        var_dump('mapAttrGroupIdsMigrated', $this->mapAttrGroupIdsMigrated);
    }

    /**
     * Create mapping for attribute ids
     *
     * @return void
     */
    protected function createMapAttributeIds()
    {
        $newAttributes = $this->helper->getDestinationRecords(
            'eav_attribute',
            ['entity_type_id', 'attribute_code']
        );
        foreach ($this->initialData->getAttributes('dest') as $keyOld => $attributeOld) {
            list($entityTypeId, $attributeCodeDest) = explode('-', $keyOld, 2);
            $keyMapped = $this->mapEntityTypeIdsDestOldNew[$entityTypeId] . '-' . $attributeCodeDest;
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
                }
            }
        }*/

        var_dump('mapAttrIdsKept', $this->mapAttrIdsKept);
        var_dump('mapAttrIdsMigrated', $this->mapAttrIdsMigrated);
    }

    /**
     * Get iterations count
     *
     * @return int
     */
    public function getIterationsCount()
    {
        return count($this->readerGroups->getGroup('documents'));
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
