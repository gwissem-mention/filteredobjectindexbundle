<?php
namespace Celltrak\FilteredObjectIndexBundle\Tests\Component\Set;

use Celltrak\FilteredObjectIndexBundle\Component\Set\PersistedSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\UnionSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\IntersectionSet;
use Celltrak\FilteredObjectIndexBundle\Tests\FilteredObjectIndexTestCase;
use Celltrak\RedisBundle\Component\Multi\Pipeline;



class PersistedSetTest extends FilteredObjectIndexTestCase
{

    protected function setUp()
    {
        parent::setUp();

        $this->redis->sAdd('planets', ...self::PLANETS);

        $this->set = new PersistedSet($this->redis, 'planets');
    }

    public function testCount()
    {
        $this->assertEquals(count(self::PLANETS), count($this->set));
    }

    public function testCountAfterLoading()
    {
        $this->set->getObjectIds();
        $this->assertEquals(count(self::PLANETS), count($this->set));
    }

    public function testGetObjectIds()
    {
        $objectIds = $this->set->getObjectIds();
        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(self::PLANETS, $objectIds);
    }

    public function testHasObject()
    {
        $this->assertTrue($this->set->hasObject('earth'));
        $this->assertFalse($this->set->hasObject('pluto'));
    }

    public function testHasObjectAfterLoading()
    {
        $this->set->getObjectIds();
        $this->assertTrue($this->set->hasObject('earth'));
        $this->assertFalse($this->set->hasObject('pluto'));
    }

    public function testInitializeForCalculation()
    {
        $pipelineMock = $this->getMockBuilder(Pipeline::class)
            ->disableOriginalConstructor()
            ->getMock();

        $this->assertEquals(
            'planets',
            $this->set->initializeForCalculation($pipelineMock)
        );
    }

    public function testIsPersisted()
    {
        $this->assertTrue($this->set->isPersisted());
    }

    public function testGetPersistedSetKey()
    {
        $this->assertEquals('planets', $this->set->getPersistedSetKey());
    }

    public function testIntersect()
    {
        $otherSet = new PersistedSet($this->redis, 'otherset');
        $inter = $this->set->intersect($otherSet);

        $this->assertInstanceOf(IntersectionSet::class, $inter);

        $includedSets = $inter->getIncludedSets();
        $this->assertCount(2, $includedSets);
        $this->assertEquals($this->set, $includedSets[0]);
        $this->assertEquals($otherSet, $includedSets[1]);
    }

    public function testUnion()
    {
        $otherSet = new PersistedSet($this->redis, 'otherset');
        $union = $this->set->union($otherSet);

        $this->assertInstanceOf(UnionSet::class, $union);

        $includedSets = $union->getIncludedSets();
        $this->assertCount(2, $includedSets);
        $this->assertEquals($this->set, $includedSets[0]);
        $this->assertEquals($otherSet, $includedSets[1]);
    }

    public function testIterator()
    {
        $objectIds = [];

        foreach ($this->set as $i => $objectId) {
            $objectIds[] = $objectId;
        }

        $this->assertEqualArrayValues(self::PLANETS, $objectIds);
    }

    public function testJsonSerialize()
    {
        $objectIds = json_decode(json_encode($this->set));

        $this->assertEqualArrayValues(self::PLANETS, $objectIds);
    }

    public function testDestruct()
    {
        unset($this->set);
        $this->assertTrue($this->redis->exists('planets'));
    }

}
