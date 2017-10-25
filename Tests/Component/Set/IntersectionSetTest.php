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


class IntersectionSetTest extends FilteredObjectIndexTestCase
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


    public function testCreateIntersection()
    {
        $inter = new IntersectionSet($this->redis);

        $this->assertInstanceOf(BaseSet::class, $inter);
        $this->assertInstanceOf(CalculatedSet::class, $inter);

        $includedSets = $inter->getIncludedSets();

        $this->assertInternalType('array', $includedSets);
        $this->assertCount(0, $includedSets);
    }

    public function testAddOneSet()
    {
        $inter = new IntersectionSet($this->redis);

        $result = $inter->addSet($this->planets);
        $this->assertEquals($inter, $result);

        $includedSets = $inter->getIncludedSets();

        $this->assertCount(1, $includedSets);
        $this->assertEquals($this->planets, $includedSets[0]);
    }

    public function testAddMultipleSets()
    {
        $inter = new IntersectionSet($this->redis);

        $result = $inter->addSet($this->planets, $this->romanGods);
        $this->assertEquals($inter, $result);

        $includedSets = $inter->getIncludedSets();

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

        $inter = new IntersectionSet($this->redis);
        $result = $inter->addSet($this->planets, $invalidSet);
    }

    public function testUnion()
    {
        $inter = new UnionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);
        $union = $inter->union($this->astronomicalBodies);

        $this->assertInstanceOf(UnionSet::class, $union);

        $includedSets = $union->getIncludedSets();
        $this->assertEquals($inter, $includedSets[0]);
        $this->assertEquals($this->astronomicalBodies, $includedSets[1]);
    }

    public function testIntersect()
    {
        $i1 = new IntersectionSet($this->redis);
        $i1->addSet($this->planets, $this->romanGods);
        $i2 = $i1->intersect($this->astronomicalBodies);

        $this->assertInstanceOf(IntersectionSet::class, $i2);

        $includedSets = $i2->getIncludedSets();
        $this->assertEquals($i1, $includedSets[0]);
        $this->assertEquals($this->astronomicalBodies, $includedSets[1]);
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testCountWithNoIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        count($inter);
    }

    public function testCountWithOneIncludedSet()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets);

        $this->assertEquals(count(self::PLANETS), count($inter));
    }

    public function testCountWithMultipleIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);

        $this->assertEquals(
            $this->getUniqueSharedValueCount(self::PLANETS, self::ROMAN_GODS),
            count($inter)
        );
    }

    public function testCountWithIncludedEmptySet()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->emptySet);

        $this->assertEquals(0, count($inter));
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testGetObjectIdsWithNoIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->getObjectIds();
    }

    public function testGetObjectIdsWithOneIncludedSet()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets);
        $objectIds = $inter->getObjectIds();

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(self::PLANETS, $objectIds);
    }

    public function testGetObjectIdsWithMultipleIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);
        $objectIds = $inter->getObjectIds();

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(
            $this->getUniqueSharedValues(self::PLANETS, self::ROMAN_GODS),
            $objectIds
        );
    }

    public function testGetObjectIdsWithIncludedEmptySet()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->emptySet);

        $this->assertEquals([], $inter->getObjectIds());
    }

    public function testGetObjectIdsWithNestedUnion()
    {
        $union = new UnionSet($this->redis);
        $union->addSet($this->planets, $this->romanGods);

        $inter = $union->intersect($this->astronomicalBodies);
        $objectIds = $inter->getObjectIds();

        $this->assertInternalType('array', $objectIds);

        $expectedObjectIds = $this->getUniqueValues(
            self::PLANETS,
            self::ROMAN_GODS
        );
        $expectedObjectIds = $this->getUniqueSharedValues(
            $expectedObjectIds,
            self::ASTRONOMICAL_BODIES
        );

        $this->assertEqualArrayValues($expectedObjectIds, $objectIds);
    }

    public function testGetObjectIdsWithNestedIntersection()
    {
        $innerInter = new IntersectionSet($this->redis);
        $innerInter->addSet($this->planets, $this->romanGods);

        $outerInter = $innerInter->intersect($this->astronomicalBodies);
        $objectIds = $outerInter->getObjectIds();

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(
            $this->getUniqueSharedValues(
                self::PLANETS,
                self::ROMAN_GODS,
                self::ASTRONOMICAL_BODIES
            ),
            $objectIds
        );
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testHasObjectWithNoIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->hasObject('foo');
    }

    public function testHasObjectWithOneIncludedSet()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets);

        $this->assertTrue($inter->hasObject('venus'));
        $this->assertFalse($inter->hasObject('pluto'));
        $this->assertFalse($inter->hasObject('foo'));
    }

    public function testHasObjectWithMultipleIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);

        $this->assertTrue($inter->hasObject('venus'));
        $this->assertFalse($inter->hasObject('pluto'));
        $this->assertFalse($inter->hasObject('foo'));
    }

    public function testHasObjectWithIncludedEmptySet()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->emptySet);

        $this->assertFalse($inter->hasObject('earth'));
        $this->assertFalse($inter->hasObject('foo'));
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testInitializeForCalculationWithNoIncludedSets()
    {
        $pipeline = $this->redis->createPipeline();
        $inter = new IntersectionSet($this->redis);
        $inter->initializeForCalculation($pipeline);
    }

    public function testInitializeForCalculationWithMultipleIncludedSets()
    {
        $pipeline = $this->redis->createPipeline();
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);
        $inter->initializeForCalculation($pipeline);

        // Check that pipeline has
        //  sInterStore <tmpKey> planets romangods
        //  expire <tmpKey> <ttl>
        $calls = $pipeline->getCalls();
        $this->assertCount(2, $calls);
        $this->assertEquals('sInterStore', $calls[0]->getCommandName());
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

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testPersistWithNoIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->persist();
    }

    public function testPersistWithMultipleIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);
        $persistedSet = $inter->persist();

        $this->assertInstanceOf(PersistedSet::class, $persistedSet);

        $this->assertEqualArrayValues(
            $this->getUniqueSharedValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );

        $setKey = $persistedSet->getPersistedSetKey();
        $this->assertEquals(-1, $this->redis->ttl($setKey));
    }

    public function testPersistWithIncludedEmptySet()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->emptySet);
        $persistedSet = $inter->persist('foo');

        $this->assertInstanceOf(PersistedSet::class, $persistedSet);
        $this->assertEquals([], $persistedSet->getObjectIds());
    }

    public function testPersistWithExplicitName()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);
        $persistedSet = $inter->persist('foo');

        $this->assertInstanceOf(PersistedSet::class, $persistedSet);
        $this->assertEquals('foo', $persistedSet->getPersistedSetKey());
        $this->assertEqualArrayValues(
            $this->getUniqueSharedValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );
        $this->assertEquals(-1, $this->redis->ttl('foo'));
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\NoIncludedSetsException
     */
    public function testPersistTemporaryWithNoIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->persistTemporary();
    }

    public function testPersistTemporaryWithMultipleIncludedSets()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);
        $persistedSet = $inter->persistTemporary();

        $this->assertInstanceOf(TemporarySet::class, $persistedSet);
        $this->assertEqualArrayValues(
            $this->getUniqueSharedValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );
    }

    public function testPersistTemporaryWithIncludedEmptySet()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->emptySet);
        $persistedSet = $inter->persistTemporary('foo');

        $this->assertInstanceOf(TemporarySet::class, $persistedSet);
        $this->assertEquals([], $persistedSet->getObjectIds());
    }

    public function testPersistTemporaryWithTtl()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);
        $persistedSet = $inter->persistTemporary(null, 90);

        $this->assertInstanceOf(TemporarySet::class, $persistedSet);
        $this->assertEqualArrayValues(
            $this->getUniqueSharedValues(self::PLANETS, self::ROMAN_GODS),
            $persistedSet->getObjectIds()
        );
        $setKey = $persistedSet->getPersistedSetKey();
        $this->assertLessThanOrEqual(90, $this->redis->ttl($setKey));
    }

    public function testIterator()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);

        $objectIds = [];

        foreach ($inter as $i => $objectId) {
            $objectIds[] = $objectId;
        }

        $this->assertEqualArrayValues(
            $this->getUniqueSharedValues(self::PLANETS, self::ROMAN_GODS),
            $objectIds
        );
    }

    public function testJsonSerialize()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);
        $objectIds = json_decode(json_encode($inter));

        $this->assertInternalType('array', $objectIds);
        $this->assertEqualArrayValues(
            $this->getUniqueSharedValues(self::PLANETS, self::ROMAN_GODS),
            $objectIds
        );
    }

    public function testDestruct()
    {
        $inter = new IntersectionSet($this->redis);
        $inter->addSet($this->planets, $this->romanGods);
        unset($inter);

        $this->assertTrue($this->redis->exists('planets'));
        $this->assertTrue($this->redis->exists('romangods'));
    }



}
