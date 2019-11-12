<?php
namespace Migration\Handler\AheadworksBlog;

use Migration\ResourceModel\Record;
use Migration\Handler\AbstractHandler;
use Migration\Handler\HandlerInterface;

/**
 * Handler to copy PublishDate field value to some other field
 */
class PostPublishDateCopy extends AbstractHandler implements HandlerInterface
{
    /**
     * {@inheritdoc}
     */
    public function handle(Record $recordToHandle, Record $oppositeRecord)
    {
        $this->validate($recordToHandle);
        $updatedAt = $recordToHandle->getValue('updated_at');
        $createdAt = $recordToHandle->getValue('created_at');

        $fieldCopyValue = $createdAt;
        if ($updatedAt && strtotime($createdAt) < strtotime($updatedAt)) {
            $fieldCopyValue = $updatedAt;
        }

        if ($fieldCopyValue && $recordToHandle->getValue('status') == 'publication') {
            $recordToHandle->setValue($this->field, $fieldCopyValue);
        } else {
            $recordToHandle->setValue($this->field, null);
        }
    }
}
