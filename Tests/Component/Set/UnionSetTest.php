<?php
namespace Celltrak\FilteredObjectIndexBundle\Tests\Component\Set;


use Celltrak\FilteredObjectIndexBundle\Component\Set\BaseSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\PersistedSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\TemporarySet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\CalculatedSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\UnionSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\IntersectionSet;
use Celltrak\FilteredObjectIndexBundle\Tests\FilteredObjectIndexTestCase;
use Celltrak\RedisBundle\Component\Client\CelltrakRedis;


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
        $union = new UnionSet($this->redis);

        $this->assertInstanceOf(BaseSet::class, $union);
        $this->assertInstanceOf(CalculatedSet::class, $union);

        $includedSets = $union->getIncludedSets();

        $this->assertInternalType('array', $includedSets);
        $this->assertCount(0, $includedSets);
    }

    public function testAddOneSet()
    {
        $union = new UnionSet($this->redis);

        $result = $union->addSet($this->planets);
        $this->assertEquals($union, $result);

        $includedSets = $union->getIncludedSets();

        $this->assertCount(1, $includedSets);
        $this->assertEquals($this->planets, $includedSets[0]);
    }

    public function testAddMultipleSets()
    {
        $union = new UnionSet($this->redis);

        $result = $union->addSet($this->planets, $this->romanGods);
        $this->assertEquals($union, $result);

        $includedSets = $union->getIncludedSets();

        $this->assertCount(2, $includedSets);
        $this->assertEquals($this->planets, $includedSets[0]);
        $this->assertEquals($this->romanGods, $includedSets[1]);
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\InvalidSetException
     */
    public function testAddInvalidSet()
    {
        $redisMock = $this->getMockBuilder(CelltrakRedis::class)
            ->disableOriginalConstructor()
            ->getMock();

        $invalidSet = new PersistedSet($redisMock, 'foo');

        $union = new UnionSet($this->redis);
        $result = $union->addSet($this->planets, $invalidSet);
    }

    public function testUnion()
    {
        $u1 = new UnionSet($this->redis);
        $u1->addSet($this->planets, $this->romanGods);
        $u2 = $u1->union($this->astronomicalBodies);

        $this->assertInstanceOf(UnionSet::class, $u2);

        $includedSets = $u2->getIncludedSets();
        $this->assertEquals($u1, $includedSets[0]);
        $this->assertEquals($this->astronomicalBodies, $includedSets[1]);
    }

    public function testIntersect()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods);
        $inter = $union->intersect($this->astronomicalBodies);

        $this->assertInstanceOf(IntersectionSet::class, $inter);

        $includedSets = $inter->getIncludedSets();
        $this->assertEquals($union, $includedSets[0]);
        $this->assertEquals($this->astronomicalBodies, $includedSets[1]);
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testCountWithNoIncludedSets()
    {
        $union = new UnionSet($this->redis);
        count($union);
    }

    public function testCountWithOneIncludedSet()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets);

        $this->assertEquals(count(self::PLANETS), count($union));
    }

    public function testCountWithMultipleIncludedSets()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods);

        $this->assertEquals(
            $this->getUniqueValueCount(self::PLANETS, self::ROMAN_GODS),
            count($union)
        );
    }

    public function testCountWithIncludedEmptySet()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods, $this->emptySet);

        $this->assertEquals(
            $this->getUniqueValueCount(self::PLANETS, self::ROMAN_GODS),
            count($union)
        );
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testGetObjectIdsWithNoIncludedSets()
    {
        $union = new UnionSet($this->redis);
        $union->getObjectIds();
    }

    public function testGetObjectIdsWithOneIncludedSet()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets);
        $objectIds = $union->getObjectIds();

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(self::PLANETS, $objectIds);
    }

    public function testGetObjectIdsWithMultipleIncludedSets()
    {
        $union = new UnionSet($this->redis);
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
        $union = new UnionSet($this->redis);
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
        $innerUnion = new UnionSet($this->redis);
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
        $inter = new IntersectionSet($this->redis);
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
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testHasObjectWithNoIncludedSets()
    {
        $union = new UnionSet($this->redis);
        $union->hasObject('foo');
    }

    public function testHasObjectWithOneIncludedSet()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets);

        $this->assertTrue($union->hasObject('venus'));
        $this->assertFalse($union->hasObject('foo'));
    }

    public function testHasObjectWithMultipleIncludedSets()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods);

        $this->assertTrue($union->hasObject('venus'));
        $this->assertTrue($union->hasObject('pluto'));
        $this->assertFalse($union->hasObject('foo'));
    }

    public function testHasObjectWithIncludedEmptySet()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods, $this->emptySet);

        $this->assertTrue($union->hasObject('venus'));
        $this->assertTrue($union->hasObject('pluto'));
        $this->assertFalse($union->hasObject('foo'));
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testInitializeForCalculationWithNoIncludedSets()
    {
        $pipeline = $this->redis->createPipeline();
        $union = new UnionSet($this->redis);
        $union->initializeForCalculation($pipeline);
    }

    public function testInitializeForCalculationWithMultipleIncludedSets()
    {
        $pipeline = $this->redis->createPipeline();
        $union = new UnionSet($this->redis);
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
            CalculatedSet::TEMPORARY_KEY_TTL
        );
    }

    public function testIsPersisted()
    {
        $union = new UnionSet($this->redis);
        $this->assertFalse($union->isPersisted());
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testPersistWithNoIncludedSets()
    {
        $union = new UnionSet($this->redis);
        $union->persist();
    }

    public function testPersistWithMultipleIncludedSets()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods);
        $persistedSet = $union->persist();

        $this->assertInstanceOf(PersistedSet::class, $persistedSet);

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $setKey = $persistedSet->getPersistedSetKey();
        unset($persistedSet);

        $persistedSet = new PersistedSet($this->redis, $setKey);

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $this->assertEquals(-1, $this->redis->ttl($setKey));
    }

    public function testPersistWithIncludedEmptySet()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods, $this->emptySet);
        $persistedSet = $union->persist();

        $this->assertInstanceOf(PersistedSet::class, $persistedSet);

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $setKey = $persistedSet->getPersistedSetKey();
        unset($persistedSet);

        $persistedSet = new PersistedSet($this->redis, $setKey);

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $this->assertEquals(-1, $this->redis->ttl($setKey));
    }


    public function testPersistWithExplicitName()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods);
        $persistedSet = $union->persist('foo');

        $this->assertInstanceOf(PersistedSet::class, $persistedSet);

        $this->assertEquals('foo', $persistedSet->getPersistedSetKey());

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        unset($persistedSet);

        $persistedSet = new PersistedSet($this->redis, 'foo');

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $this->assertEquals(-1, $this->redis->ttl('foo'));
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testPersistTemporaryWithNoIncludedSets()
    {
        $union = new UnionSet($this->redis);
        $union->persistTemporary();
    }

    public function testPersistTemporaryWithMultipleIncludedSets()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods);
        $persistedSet = $union->persistTemporary();

        $this->assertInstanceOf(TemporarySet::class, $persistedSet);

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );
    }

    public function testPersistTemporaryWithIncludedEmptySet()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods, $this->emptySet);
        $persistedSet = $union->persistTemporary();

        $this->assertInstanceOf(TemporarySet::class, $persistedSet);

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );
    }

    public function testPersistTemporaryWithTtl()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods);
        $persistedSet = $union->persistTemporary(null, 90);

        $this->assertInstanceOf(TemporarySet::class, $persistedSet);

        $this->assertEqualArrayValues(
            $this->getUniqueValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $setKey = $persistedSet->getPersistedSetKey();

        $this->assertLessThanOrEqual(90, $this->redis->ttl($setKey));
    }

    public function testIterator()
    {
        $union = new UnionSet($this->redis);
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
        $union = new UnionSet($this->redis);
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
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods);
        unset($union);

        $this->assertTrue($this->redis->exists('planets'));
        $this->assertTrue($this->redis->exists('romangods'));
    }



}
