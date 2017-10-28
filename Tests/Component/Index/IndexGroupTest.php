<?php
namespace Celltrak\FilteredObjectIndexBundle\Tests\Component\Index;

use Celltrak\FilteredObjectIndexBundle\Tests\FilteredObjectIndexTestCase;
use Celltrak\FilteredObjectIndexBundle\Component\Index\IndexGroup;
use Celltrak\FilteredObjectIndexBundle\Component\Query\GroupedFilterQuery;
use Celltrak\FilteredObjectIndexBundle\Component\Set\PersistedSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\UnionSet;
use Celltrak\FilteredObjectIndexBundle\Component\Set\IntersectionSet;


class IndexGroupTest extends FilteredObjectIndexTestCase
{

    const KEY_OBJECT_SALT_LOCK = 'foi:food:obj:salt:lock';


    protected function setUp()
    {
        parent::setUp();

        $this->group = new IndexGroup('food', $this->redis);
    }


    public function testAddObjectToIndexWithNoFilters()
    {
        $this->group->addObjectToIndex('salt', 'recipe');

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertEqualArrayValues(
            ['recipe'],
            $this->group->getIndexesWithObject('salt')
        );
        $this->assertTrue(
            $this->group->getIndexGlobalSet('recipe')->hasObject('salt')
        );
    }

    public function testAddObjectToIndexWithFilters()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dessert', 'dinner']);

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertEqualArrayValues(
            ['recipe'],
            $this->group->getIndexesWithObject('salt')
        );
        $this->assertTrue(
            $this->group->getIndexGlobalSet('recipe')->hasObject('salt')
        );
        $this->assertTrue(
            $this->group->getIndexFilterSet('recipe', 'dessert')->hasObject('salt')
        );
        $this->assertTrue(
            $this->group->getIndexFilterSet('recipe', 'dinner')->hasObject('salt')
        );
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\LockedObjectException
     */
    public function testAddObjectExceedsLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1);
        $this->group->addObjectToIndex('salt', 'recipe', [], -1);
    }

    public function testAddObjectSuccessfulLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1, ['EX' => 2]);
        $this->group->addObjectToIndex('salt', 'recipe', [], 3);

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertEqualArrayValues(
            ['recipe'],
            $this->group->getIndexesWithObject('salt')
        );
        $this->assertTrue(
            $this->group->getIndexGlobalSet('recipe')->hasObject('salt')
        );
    }

    public function testRemoveObjectFromIndex()
    {
        $this->group->addObjectToIndex('salt', 'recipe');

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->removeObjectFromIndex('salt', 'recipe'));
        $this->assertFalse($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertFalse(
            $this->group->getIndexGlobalSet('recipe')->hasObject('salt')
        );
    }

    public function testRemoveObjectFromIndexWithFilters()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dinner', 'dessert']);

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->removeObjectFromIndex('salt', 'recipe'));
        $this->assertFalse($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertFalse(
            $this->group->getIndexGlobalSet('recipe')->hasObject('salt')
        );
        $this->assertFalse(
            $this->group->getIndexFilterSet('recipe', 'dinner')->hasObject('salt')
        );
        $this->assertFalse(
            $this->group->getIndexFilterSet('recipe', 'dessert')->hasObject('salt')
        );
    }

    public function testRemoveObjectNotExistingInIndex()
    {
        $this->assertFalse($this->group->removeObjectFromIndex('salt', 'dinner'));
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\LockedObjectException
     */
    public function testRemoveObjectFromIndexExceedsLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1);
        $this->group->removeObjectFromIndex('salt', 'recipe', -1);
    }

    public function testRemoveObjectFromIndexSuccessfulLockWaitTimeout()
    {
        $this->group->addObjectToIndex('salt', 'recipe');
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1, ['EX' => 2]);

        $this->assertTrue(
            $this->group->removeObjectFromIndex('salt', 'recipe', 3)
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertFalse($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertFalse(
            $this->group->getIndexGlobalSet('recipe')->hasObject('salt')
        );
    }

    public function testRemoveObjectFromAllIndexes()
    {
        $this->group->addObjectToIndex('salt', 'recipe');
        $this->group->addObjectToIndex('salt', 'cart');

        $this->assertEqualArrayValues(
            ['recipe', 'cart'],
            $this->group->removeObjectFromAllIndexes('salt')
        );

        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertFalse(
            $this->group->isObjectInIndex('salt', 'recipe')
        );
        $this->assertFalse(
            $this->group->getIndexGlobalSet('recipe')->hasObject('salt')
        );
        $this->assertFalse(
            $this->group->isObjectInIndex('salt', 'cart')
        );
        $this->assertFalse(
            $this->group->getIndexGlobalSet('cart')->hasObject('salt')
        );
    }

    public function testRemoveObjectNotInAnyIndex()
    {
        $this->assertEquals([], $this->group->removeObjectFromAllIndexes('salt'));
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
    }

    public function testMoveObjectToIndex()
    {
        $this->group->addObjectToIndex('salt', 'recipe');
        $this->assertEquals(
            ['recipe'],
            $this->group->moveObjectToIndex('salt', 'cart')
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'cart'));
        $this->assertTrue(
            $this->group->getIndexGlobalSet('cart')->hasObject('salt')
        );
        $this->assertFalse($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertFalse(
            $this->group->getIndexGlobalSet('recipe')->hasObject('salt')
        );
    }

    public function testMoveObjectToSameIndex()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dessert']);
        $this->assertEquals(
            [],
            $this->group->moveObjectToIndex('salt', 'recipe', ['dinner'])
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
        $this->assertTrue(
            $this->group->getIndexGlobalSet('recipe')->hasObject('salt')
        );
        $this->assertTrue(
            $this->group->getIndexFilterSet('recipe', 'dinner')->hasObject('salt')
        );
        $this->assertFalse(
            $this->group->getIndexFilterSet('recipe', 'dessert')->hasObject('salt')
        );
    }

    public function testMoveObjectNotInExistingIndex()
    {
        $this->assertEquals(
            [],
            $this->group->moveObjectToIndex('salt', 'recipe')
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
    }

    /**
     * @expectedException Celltrak\FilteredObjectIndexBundle\Exception\LockedObjectException
     */
    public function testMoveObjectExceedsLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1);
        $this->group->moveObjectToIndex('salt', 'recipe', [], -1);
    }

    public function testMoveObjectSuccessfulLockWaitTimeout()
    {
        $this->redis->set(self::KEY_OBJECT_SALT_LOCK, 1, ['EX' => 2]);
        $this->assertEquals(
            [],
            $this->group->moveObjectToIndex('salt', 'recipe', [], 3)
        );
        $this->assertFalse($this->redis->exists(self::KEY_OBJECT_SALT_LOCK));
        $this->assertTrue($this->group->isObjectInIndex('salt', 'recipe'));
    }

    public function testCreateUnionOfIndexFilters()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dinner','dessert']);
        $this->group->addObjectToIndex('sugar', 'recipe', ['dessert']);
        $this->group->addObjectToIndex('spam', 'recipe');

        $union = $this->group->createUnionOfIndexFilters(
            'recipe',
            ['dinner', 'dessert']
        );

        $this->assertEqualArrayValues(
            ['salt', 'sugar'],
            $union->getObjectIds()
        );
    }

    public function testCreateIntersectionOfIndexFilters()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dinner','dessert']);
        $this->group->addObjectToIndex('sugar', 'recipe', ['dessert']);
        $this->group->addObjectToIndex('spam', 'recipe');

        $inter = $this->group->createIntersectionOfIndexFilters(
            'recipe',
            ['dinner', 'dessert']
        );

        $this->assertEqualArrayValues(['salt'], $inter->getObjectIds());
    }

    public function testCreateUnion()
    {
        $union = $this->group->createUnion();

        $this->assertInstanceOf(UnionSet::class, $union);
    }

    public function testCreateIntersection()
    {
        $inter = $this->group->createIntersection();

        $this->assertInstanceOf(IntersectionSet::class, $inter);
    }

    public function testEmptyGroupedFilterQuery()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dinner','dessert']);
        $this->group->addObjectToIndex('sugar', 'recipe', ['dessert']);
        $this->group->addObjectToIndex('spam', 'recipe');

        $query = new GroupedFilterQuery;

        $resultSet = $this->group->createSetForIndexGroupedFilterQuery(
            'recipe',
            $query
        );

        $this->assertInstanceOf(PersistedSet::class, $resultSet);
        $this->assertEqualArrayValues(
            ['salt', 'sugar', 'spam'],
            $resultSet->getObjectIds()
        );
    }

    public function testSingleGroupedFilterQuery()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dinner','dessert']);
        $this->group->addObjectToIndex('sugar', 'recipe', ['dessert']);
        $this->group->addObjectToIndex('spam', 'recipe');

        $query = new GroupedFilterQuery;
        $query->addGroupedFilters('dessert', 'dinner');

        $resultSet = $this->group->createSetForIndexGroupedFilterQuery(
            'recipe',
            $query
        );

        $this->assertInstanceOf(UnionSet::class, $resultSet);
        $this->assertEqualArrayValues(
            ['salt', 'sugar'],
            $resultSet->getObjectIds()
        );
    }

    public function testMultipleGroupedFilterQuery()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dinner','dessert']);
        $this->group->addObjectToIndex('sugar', 'recipe', ['dessert']);
        $this->group->addObjectToIndex('apple', 'recipe', ['fruit', 'dessert']);
        $this->group->addObjectToIndex('carrot', 'recipe', ['vegetable','dinner']);
        $this->group->addObjectToIndex('kale', 'recipe', ['vegetable','dinner']);
        $this->group->addObjectToIndex('spam', 'recipe');

        $query = new GroupedFilterQuery;
        $query->addGroupedFilters('dinner', 'vegetable');
        $query->addGroupedFilters('dessert', 'fruit');

        $resultSet = $this->group->createSetForIndexGroupedFilterQuery(
            'recipe',
            $query
        );

        $this->assertInstanceOf(IntersectionSet::class, $resultSet);
        $this->assertEqualArrayValues(
            ['salt'],
            $resultSet->getObjectIds()
        );
    }

    public function testFlushGroup()
    {
        $this->group->addObjectToIndex('salt', 'recipe', ['dinner']);
        $this->group->addObjectToIndex('spam', 'recipe');
        $this->group->addObjectToIndex('fruit', 'produce');
        $this->group->flushGroup();

        $this->assertEmpty(
            $this->group->getIndexGlobalSet('recipe')->getObjectIds()
        );
        $this->assertEmpty(
            $this->group->getIndexGlobalSet('produce')->getObjectIds()
        );
        $this->assertEmpty($this->group->getIndexesWithObject('salt'));
        $this->assertEmpty($this->group->getIndexesWithObject('spam'));
        $this->assertEmpty($this->group->getIndexesWithObject('fruit'));
    }

}
