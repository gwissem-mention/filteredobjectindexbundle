<?php
namespace Celltrak\FilteredObjectIndexBundle\Tests\Component\Set;


use Celltrak\FilteredObjectIndexBundle\Component\Set\PersistedSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\TemporarySet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\CalculatedSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\UnionSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\IntersectionSet;
use Celltrak\FilteredObjectIndexBundle\Tests\FilteredObjectIndexTestCase;

class UnionSetTest extends FilteredObjectIndexTestCase
{

    protected function setUp()
    {
        parent::setUp();

        $this->redis->sAdd('planets', ...self::PLANETS);
        $this->redis->sAdd('astronomicalbodies', ...self::ASTRONOMICAL_BODIES);
        $this->redis->sAdd('romangods', ...self::ROMAN_GODS);

        $this->planets = new PersistedSet($this->redis, 'planets');
        $this->romanGods = new PersistedSet($this->redis, 'romangods');
        $this->emptySet = new PersistedSet($this->redis, 'empty');

        $this->astronomicalBodies = new PersistedSet(
            $this->redis,
            'astronomicalbodies'
        );


    }


    public function testCreateUnion()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);

        $this->assertInstanceOf(FilteredObjectIndexSet::class, $union);
        $this->assertInstanceOf(FilteredObjectIndexCalculatedSet::class, $union);

        $includedSets = $union->getIncludedSets();

        $this->assertInternalType('array', $includedSets);
        $this->assertCount(0, $includedSets);
    }

    public function testAddOneSet()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);

        $result = $union->addSet($this->planets);
        $this->assertEquals($union, $result);

        $includedSets = $union->getIncludedSets();

        $this->assertCount(1, $includedSets);
        $this->assertEquals($this->planets, $includedSets[0]);
    }

    public function testAddMultipleSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);

        $result = $union->addSet($this->planets, $this->romanGods);
        $this->assertEquals($union, $result);

        $includedSets = $union->getIncludedSets();

        $this->assertCount(2, $includedSets);
        $this->assertEquals($this->planets, $includedSets[0]);
        $this->assertEquals($this->romanGods, $includedSets[1]);
    }

    /**
     * @expectedException \InvalidArgumentException
     * @expectedExceptionMessage Set doesn't share index manager
     */
    public function testAddInvalidSet()
    {
        $loggerMock = $this->getMockBuilder(Logger::class)
            ->disableOriginalConstructor()
            ->getMock();

        $invalidIndexManager = new FilteredObjectIndexManager(
            $this->redis,
            $loggerMock,
            'test'
        );

        $invalidSet = new FilteredObjectIndexPersistedSet(
            $invalidIndexManager,
            'foo'
        );

        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $result = $union->addSet($this->planets, $invalidSet);
    }

    public function testUnion()
    {
        $u1 = new FilteredObjectIndexUnionSet($this->indexManager);
        $u1->addSet($this->planets, $this->romanGods);
        $u2 = $u1->union($this->astronomicalBodies);

        $this->assertInstanceOf(FilteredObjectIndexUnionSet::class, $u2);

        $includedSets = $u2->getIncludedSets();
        $this->assertEquals($u1, $includedSets[0]);
        $this->assertEquals($this->astronomicalBodies, $includedSets[1]);
    }

    public function testIntersect()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);
        $inter = $union->intersect($this->astronomicalBodies);

        $this->assertInstanceOf(FilteredObjectIndexIntersectionSet::class, $inter);

        $includedSets = $inter->getIncludedSets();
        $this->assertEquals($union, $includedSets[0]);
        $this->assertEquals($this->astronomicalBodies, $includedSets[1]);
    }

    /**
     * @expectedException CTLib\Component\FilteredObjectIndex\NoIncludedSetsException
     */
    public function testCountWithNoIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        count($union);
    }

    public function testCountWithOneIncludedSet()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets);

        $this->assertEquals(count(self::PLANETS), count($union));
    }

    public function testCountWithMultipleIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);

        $this->assertEquals(
            $this->getUniqueValueCount(self::PLANETS, self::ROMAN_GODS),
            count($union)
        );
    }

    public function testCountWithIncludedEmptySet()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods, $this->emptySet);

        $this->assertEquals(
            $this->getUniqueValueCount(self::PLANETS, self::ROMAN_GODS),
            count($union)
        );
    }

    /**
     * @expectedException CTLib\Component\FilteredObjectIndex\NoIncludedSetsException
     */
    public function testGetObjectIdsWithNoIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->getObjectIds();
    }

    public function testGetObjectIdsWithOneIncludedSet()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets);
        $objectIds = $union->getObjectIds();

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(self::PLANETS, $objectIds);
    }

    public function testGetObjectIdsWithMultipleIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);
        $objectIds = $union->getObjectIds();

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $objectIds
        );
    }

    public function testGetObjectIdsWithIncludedEmptySet()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods, $this->emptySet);
        $objectIds = $union->getObjectIds();

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $objectIds
        );
    }

    public function testGetObjectIdsWithNestedUnion()
    {
        $innerUnion = new FilteredObjectIndexUnionSet($this->indexManager);
        $innerUnion->addSet($this->planets, $this->romanGods);

        $outerUnion = $innerUnion->union($this->astronomicalBodies);
        $objectIds = $outerUnion->getObjectIds();

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(
            $this->getUniqueValues(
                self::PLANETS,
                self::ROMAN_GODS,
                self::ASTRONOMICAL_BODIES
            ),
            $objectIds
        );
    }

    public function testGetObjectIdsWithNestedIntersection()
    {
        $inter = new FilteredObjectIndexIntersectionSet($this->indexManager);
        $inter->addSet($this->planets, $this->romanGods);

        $union = $inter->union($this->astronomicalBodies);
        $objectIds = $union->getObjectIds();

        $this->assertInternalType('array', $objectIds);

        $expectedObjectIds = $this->getUniqueSharedValues(
            self::PLANETS,
            self::ROMAN_GODS
        );
        $expectedObjectIds = $this->getUniqueValues(
            $expectedObjectIds,
            self::ASTRONOMICAL_BODIES
        );

        $this->assertEqualArrayValues($expectedObjectIds, $objectIds);
    }

    /**
     * @expectedException CTLib\Component\FilteredObjectIndex\NoIncludedSetsException
     */
    public function testHasObjectWithNoIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->hasObject('foo');
    }

    public function testHasObjectWithOneIncludedSet()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets);

        $this->assertTrue($union->hasObject('venus'));
        $this->assertFalse($union->hasObject('foo'));
    }

    public function testHasObjectWithMultipleIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);

        $this->assertTrue($union->hasObject('venus'));
        $this->assertTrue($union->hasObject('pluto'));
        $this->assertFalse($union->hasObject('foo'));
    }

    public function testHasObjectWithIncludedEmptySet()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods, $this->emptySet);

        $this->assertTrue($union->hasObject('venus'));
        $this->assertTrue($union->hasObject('pluto'));
        $this->assertFalse($union->hasObject('foo'));
    }

    /**
     * @expectedException CTLib\Component\FilteredObjectIndex\NoIncludedSetsException
     */
    public function testInitializeForCalculationWithNoIncludedSets()
    {
        $pipeline = $this->redis->createPipeline();
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->initializeForCalculation($pipeline);
    }

    public function testInitializeForCalculationWithMultipleIncludedSets()
    {
        $pipeline = $this->redis->createPipeline();
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);
        $union->initializeForCalculation($pipeline);

        // Check that pipeline has
        //  sUnionStore <tmpKey> planets romangods
        //  expire <tmpKey> <ttl>
        $calls = $pipeline->getCalls();
        $this->assertCount(2, $calls);
        $this->assertEquals('sUnionStore', $calls[0]->getCommandName());
        $this->assertCount(3, $calls[0]->getParameters());
        $this->assertEquals('planets', $calls[0]->getParameters()[1]);
        $this->assertEquals('romangods', $calls[0]->getParameters()[2]);
        $this->assertEquals('expire', $calls[1]->getCommandName());
        $this->assertEquals(
            $calls[0]->getParameters()[0],
            $calls[1]->getParameters()[0]
        );
        $this->assertEquals(
            $calls[1]->getParameters()[1],
            FilteredObjectIndexCalculatedSet::TEMPORARY_KEY_TTL
        );
    }

    /**
     * @expectedException CTLib\Component\FilteredObjectIndex\NoIncludedSetsException
     */
    public function testPersistWithNoIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->persist();
    }

    public function testPersistWithMultipleIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);
        $persistedSet = $union->persist();

        $this->assertInstanceOf(
            FilteredObjectIndexPersistedSet::class,
            $persistedSet
        );

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $setKey = $persistedSet->getPersistedSetKey();
        unset($persistedSet);

        $persistedSet = new FilteredObjectIndexPersistedSet(
            $this->indexManager,
            $setKey
        );

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $this->assertEquals(-1, $this->redis->ttl($setKey));
    }

    public function testPersistWithIncludedEmptySet()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods, $this->emptySet);
        $persistedSet = $union->persist();

        $this->assertInstanceOf(
            FilteredObjectIndexPersistedSet::class,
            $persistedSet
        );

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $setKey = $persistedSet->getPersistedSetKey();
        unset($persistedSet);

        $persistedSet = new FilteredObjectIndexPersistedSet(
            $this->indexManager,
            $setKey
        );

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $this->assertEquals(-1, $this->redis->ttl($setKey));
    }


    public function testPersistWithExplicitName()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);
        $persistedSet = $union->persist('foo');

        $this->assertInstanceOf(
            FilteredObjectIndexPersistedSet::class,
            $persistedSet
        );

        $this->assertEquals('foo', $persistedSet->getPersistedSetKey());

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        unset($persistedSet);

        $persistedSet = new FilteredObjectIndexPersistedSet(
            $this->indexManager,
            'foo'
        );

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $this->assertEquals(-1, $this->redis->ttl('foo'));
    }

    /**
     * @expectedException CTLib\Component\FilteredObjectIndex\NoIncludedSetsException
     */
    public function testPersistTemporaryWithNoIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->persistTemporary();
    }

    public function testPersistTemporaryWithMultipleIncludedSets()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);
        $persistedSet = $union->persistTemporary();

        $this->assertInstanceOf(
            FilteredObjectIndexTemporarySet::class,
            $persistedSet
        );

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $setKey = $persistedSet->getPersistedSetKey();
        unset($persistedSet);

        $persistedSet = new FilteredObjectIndexPersistedSet(
            $this->indexManager,
            $setKey
        );

        $this->assertEquals([], $persistedSet->getObjectIds());
    }

    public function testPersistTemporaryWithIncludedEmptySet()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods, $this->emptySet);
        $persistedSet = $union->persistTemporary();

        $this->assertInstanceOf(
            FilteredObjectIndexTemporarySet::class,
            $persistedSet
        );

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $setKey = $persistedSet->getPersistedSetKey();
        unset($persistedSet);

        $persistedSet = new FilteredObjectIndexPersistedSet(
            $this->indexManager,
            $setKey
        );

        $this->assertEquals([], $persistedSet->getObjectIds());
    }

    public function testPersistTemporaryWithTtl()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);
        $persistedSet = $union->persistTemporary(null, 90);

        $this->assertInstanceOf(
            FilteredObjectIndexTemporarySet::class,
            $persistedSet
        );

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $setKey = $persistedSet->getPersistedSetKey();

        $this->assertLessThanOrEqual(90, $this->redis->ttl($setKey));
    }

    public function testIterator()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);

        $objectIds = [];

        foreach ($union as $i => $objectId) {
            $objectIds[] = $objectId;
        }

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $objectIds
        );
    }

    public function testJsonSerialize()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);
        $objectIds = json_decode(json_encode($union));

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $objectIds
        );
    }

    public function testDestruct()
    {
        $union = new FilteredObjectIndexUnionSet($this->indexManager);
        $union->addSet($this->planets, $this->romanGods);
        unset($union);

        $this->assertTrue($this->redis->exists('planets'));
        $this->assertTrue($this->redis->exists('romangods'));
    }



}
