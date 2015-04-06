<?php
/**
 * Copyright © 2015 Magento. All rights reserved.
 * See COPYING.txt for license details.
 */

namespace Migration\App\Step;

/**
 * Class ProgressTest
 */
class ProgressTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \Magento\Framework\Filesystem\Driver\File|\PHPUnit_Framework_MockObject_MockObject
     */
    protected $filesystem;

    /**
     * @var \Migration\App\Step\Progress
     */
    protected $progress;

    public function setUp()
    {
        $this->filesystem = $this->getMockBuilder('\Magento\Framework\Filesystem\Driver\File')
            ->setMethods(['isExists', 'filePutContents', 'fileGetContents'])
            ->disableOriginalConstructor()
            ->getMock();
        $this->filesystem->expects($this->any())->method('filePutContents')->will($this->returnValue(true));
        $this->progress = new Progress($this->filesystem);
    }

    public function testIsCompleted()
    {
        $this->filesystem->expects($this->any())->method('isExists')->will($this->returnValue(true));
        $progress = sprintf('a:1:{s:18:"Migration\Step\Map";a:1:{s:9:"integrity";b:1;}}');
        $this->filesystem->expects($this->once())->method('fileGetContents')->will($this->returnValue($progress));
        $step = $this->getMock('\Migration\Step\Map', [], [], '', false);
        $this->progress->isCompleted($step, 'integrity');
    }

    public function testSaveResult()
    {
        $this->filesystem->expects($this->any())->method('isExists')->will($this->returnValue(true));
        $this->filesystem->expects($this->once())->method('filePutContents')->will($this->returnValue(1));
        $step = $this->getMock('\Migration\Step\Map', [], [], '', false);
        $this->progress->saveResult($step, 'integrity', 'true');
    }

    public function testClearLockFileExisting()
    {
        $this->filesystem->expects($this->any())->method('isExists')->will($this->returnValue(true));
        $this->filesystem->expects($this->once())->method('filePutContents')->will($this->returnValue(0));
        $this->assertEquals($this->progress, $this->progress->clearLockFile());
    }

    public function testClearLockFileAbsent()
    {
        $this->filesystem->expects($this->any())->method('isExists')->will($this->returnValue(false));
        $this->filesystem->expects($this->exactly(2))->method('filePutContents')->will($this->returnValue(0));
        $this->assertEquals($this->progress, $this->progress->clearLockFile());
    }

    public function testReset()
    {
        $step = $this->getMock('\Migration\Step\Map', [], [], '', false);
        $this->filesystem->expects($this->any())->method('isExists')->will($this->returnValue(true));
        $progress = sprintf('a:1:{s:%s:"%s";a:1:{s:9:"integrity";b:1;}}', strlen(get_class($step)), get_class($step));
        $this->filesystem->expects($this->once())->method('fileGetContents')->will($this->returnValue($progress));
        $this->filesystem->expects($this->once())->method('filePutContents');
        $this->progress->reset($step);
    }
}